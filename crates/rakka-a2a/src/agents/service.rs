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
    load_agent_run_state, AgentConversationEntityReply, AgentConversationEntityStore,
    AgentConversationHistoryStore, AgentConversationScope, AgentConversationState,
    AgentCoordinationReplay, AgentCoordinationReplayError, AgentCoordinationSources,
    AgentDecisionEventSink, AgentEntityAddress, AgentEntityCommand, AgentEntityReply,
    AgentEntityState, AgentEntityStore, AgentGoalClaimSource, AgentGoalId, AgentGoalView, AgentId,
    AgentOperationId, AgentOperationKind, AgentRunScope, AgentRunState, AgentRunStatus,
    AgentSchemaPolicy, AgentScope, AgentTaskEntityCommand, AgentTaskEntityReply,
    AgentTaskEntityStore, AgentTaskHistoryStore, AgentTaskId, AgentTaskScope, AgentTaskSnapshot,
    AgentTaskState, AgentTeamEntityReply, AgentTeamEntityStore, AgentTeamHistoryStore,
    AgentTeamScope, AgentTeamState, TenantId, AGENT_GOAL_VIEW_MAX_TASKS,
};
use rakka_agent_workflow::{AgentTimestampMillis, PrincipalRef};
use rakka_core::{MetricsRecorder, NoopMetricsRecorder};
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
    agent_conversation_command, agent_task_cancel_command, agent_task_create_command,
    agent_task_handoff_command, agent_task_input, agent_task_result_command, agent_team_command,
    normalize_agent_cancel, normalize_agent_send, resolve_agent_target, resolve_agent_tenant,
    resolve_handoff_target, NormalizedAgentCommand,
};
use super::management::{
    is_management_message, management_provenance, management_response_message,
    parse_management_request, refusal_response, AgentManagementCommand, AgentManagementResponse,
    META_AUDIT_REF,
};
use super::sync::{project_agent_send, sync_agent_status};

/// A shared handle to one [`RakkaAgentA2AService`], spelled once.
///
/// The store-generic parameter list is wide by construction — one durable
/// store per entity family the service drives — so the executors and
/// transports that wrap a service name it through this alias.
pub type SharedRakkaAgentA2AService<
    Tasks,
    Agents,
    History,
    Runs,
    Teams,
    TeamHistory,
    Conversations,
    ConversationHistory,
> = Arc<
    RakkaAgentA2AService<
        Tasks,
        Agents,
        History,
        Runs,
        Teams,
        TeamHistory,
        Conversations,
        ConversationHistory,
    >,
>;

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
pub struct RakkaAgentA2AService<
    Tasks,
    Agents,
    History,
    Runs,
    Teams,
    TeamHistory,
    Conversations,
    ConversationHistory,
> where
    Tasks: DurableStateStore<AgentTaskState>,
    Agents: DurableStateStore<AgentEntityState>,
    History: AgentTaskHistoryStore + Clone,
    Runs: DurableStateStore<AgentRunState>,
    Teams: DurableStateStore<AgentTeamState>,
    TeamHistory: AgentTeamHistoryStore + Clone,
    Conversations: DurableStateStore<AgentConversationState>,
    ConversationHistory: AgentConversationHistoryStore + Clone,
{
    tasks: Tasks,
    agents: Agents,
    history: History,
    runs: Runs,
    teams: Teams,
    team_history: TeamHistory,
    conversations: Conversations,
    conversation_history: ConversationHistory,
    router: AgentExchangeRouter,
    catalog: Arc<dyn A2AAgentCatalog>,
    projections: Arc<dyn A2ATaskProjectionStore>,
    tenant_resolver: Arc<dyn A2ATenantResolver>,
    authorizer: Arc<dyn A2AAuthorizer>,
    clock: Arc<dyn A2AAgentClock>,
    default_tenant: Option<String>,
    metrics: Arc<dyn MetricsRecorder>,
    decision_events: Option<Arc<dyn AgentDecisionEventSink>>,
    goal_claims: Option<Arc<dyn AgentGoalClaimSource>>,
}

impl<Tasks, Agents, History, Runs, Teams, TeamHistory, Conversations, ConversationHistory>
    RakkaAgentA2AService<
        Tasks,
        Agents,
        History,
        Runs,
        Teams,
        TeamHistory,
        Conversations,
        ConversationHistory,
    >
where
    Tasks: DurableStateStore<AgentTaskState>,
    Agents: DurableStateStore<AgentEntityState>,
    History: AgentTaskHistoryStore + Clone,
    Runs: DurableStateStore<AgentRunState>,
    Teams: DurableStateStore<AgentTeamState>,
    TeamHistory: AgentTeamHistoryStore + Clone,
    Conversations: DurableStateStore<AgentConversationState>,
    ConversationHistory: AgentConversationHistoryStore + Clone,
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
        conversations: Conversations,
        conversation_history: ConversationHistory,
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
            conversations,
            conversation_history,
            router,
            catalog,
            projections,
            tenant_resolver,
            authorizer,
            clock: Arc::new(SystemA2AAgentClock),
            default_tenant: None,
            metrics: Arc::new(NoopMetricsRecorder),
            decision_events: None,
            goal_claims: None,
        }
    }

    /// Records the agent domain's bounded counters through this recorder.
    ///
    /// The service builds its own entity stores rather than routing through
    /// the sharded entities, so a store built without it records through the
    /// noop recorder and its counters stay at zero for the wire — which for
    /// the human submission (`rakka.agent.human.results`) and the turn
    /// protocol (`rakka.agent.moderation.turns`) is the only carrier they
    /// have. Every store this service builds that accepts a recorder is
    /// wired from here; forgetting one silences its counters with no other
    /// symptom, so a new store site must be wired at the same time.
    #[must_use]
    pub fn with_metrics(mut self, metrics: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics = metrics;
        self
    }

    /// The task entity facade, wired.
    ///
    /// Every task store this service builds comes from here, and that is the
    /// whole point of it existing. The service holds a recorder of its own,
    /// and the store constructors default to the noop one, so a store built
    /// directly records nothing and gives no other symptom — no error, no
    /// log, just a counter that never leaves zero. Three slices in a row
    /// shipped exactly that, each time by adding a `new` call *beside* the
    /// wiring instead of through it. Going through an accessor leaves nothing
    /// to forget: there is no second way to get a store.
    fn task_store(&self, scope: AgentTaskScope) -> AgentTaskEntityStore<Tasks, Agents, History> {
        AgentTaskEntityStore::new(
            scope,
            self.tasks.clone(),
            self.agents.clone(),
            self.history.clone(),
        )
        .with_metrics(self.metrics.clone())
    }

    /// The team entity facade, wired — see [`Self::task_store`].
    fn team_store(&self, scope: AgentTeamScope) -> AgentTeamEntityStore<Teams, TeamHistory> {
        AgentTeamEntityStore::new(scope, self.teams.clone(), self.team_history.clone())
            .with_metrics(self.metrics.clone())
    }

    /// The conversation entity facade, wired — see [`Self::task_store`].
    fn conversation_store(
        &self,
        scope: AgentConversationScope,
    ) -> AgentConversationEntityStore<Conversations, ConversationHistory> {
        AgentConversationEntityStore::new(
            scope,
            self.conversations.clone(),
            self.conversation_history.clone(),
        )
        .with_metrics(self.metrics.clone())
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

    /// Serves the run scope of [`Self::replay_coordination_events`] from this
    /// decision-event sink.
    ///
    /// A trait object rather than a generic: the sink is the deployment's, not
    /// the entity's, and a ninth store parameter would be paid for by every
    /// wiring that never replays a run. Left unwired, a run scope is refused
    /// explicitly — never answered with an empty page, which would claim the
    /// run decided nothing.
    #[must_use]
    pub fn with_decision_events(mut self, sink: Arc<dyn AgentDecisionEventSink>) -> Self {
        self.decision_events = Some(sink);
        self
    }

    /// Joins shared-knowledge claims into [`Self::agent_goal_view`].
    ///
    /// Left unwired, the view answers `claims_available: false` with no error
    /// code — the honest "nothing asked" that the degraded-source answer is
    /// deliberately distinct from.
    #[must_use]
    pub fn with_goal_claim_source(mut self, claims: Arc<dyn AgentGoalClaimSource>) -> Self {
        self.goal_claims = Some(claims);
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
        // either parses whole or fails the send closed, and the one merge
        // and normalization here serves the dispatch and the chosen branch
        // alike — the hot path never re-merges or re-parses.
        let normalized = self.normalized_send(params, request)?;
        if matches!(
            normalized.collaboration.as_ref(),
            Some(super::collaboration::AgentCollaborationEnvelope::Team(_))
        ) {
            return self
                .team_command_normalized(request, &normalized)
                .await
                .map(a2a::SendMessageResponse::Message);
        }
        // A conversation command likewise answers with an immediate message:
        // it drives a turn-protocol decision, never a task creation
        // (specification 8.11).
        if matches!(
            normalized.collaboration.as_ref(),
            Some(super::collaboration::AgentCollaborationEnvelope::Conversation(_))
        ) {
            return self
                .conversation_command_normalized(request, &normalized)
                .await
                .map(a2a::SendMessageResponse::Message);
        }
        self.send_message_normalized(request, &normalized)
            .await
            .map(a2a::SendMessageResponse::Task)
    }

    /// Merges the request- and message-level metadata and normalizes the
    /// send once: the shared chokepoint of every send-shaped entry point.
    fn normalized_send(
        &self,
        params: &ServiceParams,
        request: &SendMessageRequest,
    ) -> RakkaAgentA2AResult<NormalizedAgentCommand> {
        let metadata =
            merged_metadata(request.metadata.as_ref(), request.message.metadata.as_ref())
                .map_err(RakkaAgentA2AError::Mapping)?;
        normalize_agent_send(
            self.tenant_resolver.as_ref(),
            self.default_tenant.as_deref(),
            params,
            request.tenant.as_deref(),
            &request.message,
            &metadata,
        )
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
        let normalized = self.normalized_send(params, request)?;
        self.team_command_normalized(request, &normalized).await
    }

    /// The normalized half of [`Self::team_command`], shared with
    /// [`Self::send`]'s dispatch so the send is merged and parsed once.
    async fn team_command_normalized(
        &self,
        request: &SendMessageRequest,
        normalized: &NormalizedAgentCommand,
    ) -> RakkaAgentA2AResult<Message> {
        let now = self.clock.now();
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
        let authorization = A2AAuthorizationRequest::new(A2AOperation::TeamCommand)
            .with_tenant(normalized.tenant.as_str())
            .with_optional_task_id(cluster.task.as_deref())
            .with_principal(normalized.principal.as_ref())
            .with_team(crate::auth::A2ATeamClaim {
                team: &cluster.team,
                operation: cluster.operation.as_label(),
                member: cluster.member.as_deref(),
                task: cluster.task.as_deref(),
                target_member: cluster.target_member.as_deref(),
            });
        match self.authorizer.authorize(&authorization).await {
            A2AAuthorizationDecision::Allow => {}
            A2AAuthorizationDecision::Deny => return Err(RakkaAgentA2AError::Unauthorized),
        }

        let board_task = cluster.task.clone();
        let (scope, command) = agent_team_command(normalized, now)?;
        let mut store = self.team_store(scope.clone());
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
                    let mut tasks = self.task_store(task_scope);
                    let _ = tasks.settle_side_effects(&self.router, now).await;
                }
            }
        }
        let _ = store.settle_side_effects(&self.router, now).await;
        Ok(team_response_message(&request.message.message_id, &reply))
    }

    /// Serves one moderated-conversation turn-protocol command carried by
    /// the collaboration extension's conversation cluster
    /// ([specification 8.11](../../../../docs/plans/rakka-agent/spec.md)):
    /// authorizes it under its own operation class with the claimed
    /// conversation command bound in, durably applies it through the
    /// conversation entity's deduplicated inbox, drives the settle pass, and
    /// answers with an immediate response message.
    ///
    /// # Errors
    ///
    /// Fails closed on normalization, a missing required cluster field, an
    /// unauthenticated early end, or authorization denial. A domain refusal
    /// — a stale coordinate, a non-participant, an exhausted budget — is not
    /// an error: it answers as a structured refusal message so the caller
    /// can rebase on the current protocol state.
    pub async fn conversation_command(
        &self,
        params: &ServiceParams,
        request: &SendMessageRequest,
    ) -> RakkaAgentA2AResult<Message> {
        let normalized = self.normalized_send(params, request)?;
        self.conversation_command_normalized(request, &normalized)
            .await
    }

    /// The normalized half of [`Self::conversation_command`], shared with
    /// [`Self::send`]'s dispatch so the send is merged and parsed once.
    async fn conversation_command_normalized(
        &self,
        request: &SendMessageRequest,
        normalized: &NormalizedAgentCommand,
    ) -> RakkaAgentA2AResult<Message> {
        let now = self.clock.now();
        let Some(super::collaboration::AgentCollaborationEnvelope::Conversation(cluster)) =
            normalized.collaboration.as_ref()
        else {
            return Err(RakkaAgentA2AError::Unsupported {
                operation: "agent-collaboration",
                reason: "the send carries no conversation envelope",
            });
        };
        // The command is its own operation class at the authorization
        // boundary: the deployment authorizer sees `ConversationCommand`
        // with the cluster's claimed verb, speaker, and coordinate bound
        // into the request — never an undifferentiated send.
        let authorization = A2AAuthorizationRequest::new(A2AOperation::ConversationCommand)
            .with_tenant(normalized.tenant.as_str())
            .with_principal(normalized.principal.as_ref())
            .with_conversation(crate::auth::A2AConversationClaim {
                conversation: &cluster.conversation,
                operation: cluster.operation.as_label(),
                participant: cluster.participant.as_deref(),
                round: cluster.round,
                turn: cluster.turn,
            });
        match self.authorizer.authorize(&authorization).await {
            A2AAuthorizationDecision::Allow => {}
            A2AAuthorizationDecision::Deny => return Err(RakkaAgentA2AError::Unauthorized),
        }

        let (scope, command) = agent_conversation_command(normalized, now)?;
        let mut store = self.conversation_store(scope);
        let reply = match store.apply(command, &self.router, now).await {
            Ok(reply) => reply,
            // A domain refusal is a decision the caller rebases on, not a
            // transport failure; infrastructure faults stay errors.
            Err(error) if error.is_domain_refusal() => AgentConversationEntityReply::Rejected {
                code: error.code().to_string(),
                message: error.to_string(),
            },
            Err(error) => return Err(error.into()),
        };
        // One best-effort settle pass flushes the history the decision owed.
        // The conversation initiates no exchange this slice, so there is no
        // courier hop to any other entity — convergence never depends on
        // this call.
        let _ = store.settle_side_effects(&self.router, now).await;
        Ok(conversation_response_message(
            &request.message.message_id,
            &reply,
        ))
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
        let authorization = A2AAuthorizationRequest::new(operation)
            .with_tenant(tenant.as_str())
            .with_principal(principal.as_ref());
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
        let normalized = self.normalized_send(params, request)?;
        self.send_message_normalized(request, &normalized).await
    }

    /// The normalized half of [`Self::send_message`], shared with
    /// [`Self::send`]'s dispatch so the send is merged and parsed once.
    async fn send_message_normalized(
        &self,
        request: &SendMessageRequest,
        normalized: &NormalizedAgentCommand,
    ) -> RakkaAgentA2AResult<Task> {
        let now = self.clock.now();
        if matches!(
            normalized.intent,
            crate::mapping::A2ATaskIntent::ContinueTask
        ) {
            // A continuation carrying the handoff cluster is the same-task
            // transfer of specification 8.9: the deduplicated handoff command
            // records the transfer, and the inline assignment decision offers
            // the target its generation in the same compare-and-set. A plain
            // continuation is the typed-result submission of specification
            // 8.12, cleanly distinguished by the collaboration metadata.
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
                    normalized,
                    Some(crate::auth::A2AHandoffClaim {
                        handoff: &cluster.handoff,
                        source_agent: &cluster.source_agent,
                        source_run: &cluster.source_run,
                        source_generation: cluster.source_generation,
                        target_agent: &cluster.target_agent,
                    }),
                    None,
                )
                .await?;
                // The same catalog gate the creation path passes: the wire's
                // target must be an agent this surface serves, checked before
                // the state-mutating command can commit — or spend one of the
                // task's bounded handoffs on an unserved target.
                resolve_handoff_target(self.catalog.as_ref(), cluster)?;
                let command = agent_task_handoff_command(normalized)?;
                let snapshot = self.apply_task_command(normalized, command, now).await?;
                let run = self.current_run_status(normalized, &snapshot).await?;
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
                return self.public_task(normalized, None).await;
            }
            // A plain continuation is the authenticated typed-result
            // submission of specification 8.12 — the ingress door slice 1.12
            // deferred. Ownership stays the entity's decision: the wire
            // builds the deduplicated command for whatever task the send
            // names, and an agent-owned target answers the stable
            // `task-not-human-owned` refusal. The submission authorizes
            // under its own operation class with the claimed contract bound
            // in — never an undifferentiated send.
            self.authorize_claimed(
                A2AOperation::SubmitTaskResult,
                normalized,
                None,
                Some(crate::auth::A2ATaskResultClaim {
                    definition: normalized
                        .result
                        .as_ref()
                        .map(|binding| binding.definition.as_str()),
                    definition_version: normalized
                        .result
                        .as_ref()
                        .map(|binding| binding.definition_version),
                    result_schema: normalized
                        .result
                        .as_ref()
                        .map(|binding| binding.result_schema.as_str()),
                    result_schema_version: normalized
                        .result
                        .as_ref()
                        .map(|binding| binding.result_schema_version),
                    evidence_digest: normalized
                        .result
                        .as_ref()
                        .and_then(|binding| binding.evidence_digest.as_deref()),
                }),
            )
            .await?;
            let input = agent_task_input(&request.message)?;
            let command =
                agent_task_result_command(normalized, input, &request.message.message_id, now)?;
            // A non-committing entity refusal — unknown task, agent-owned
            // target, terminal or cancelling task, an unboundable record —
            // is a decision the caller rebases on, never a transport
            // failure; infrastructure faults stay errors.
            let snapshot = match self.apply_task_command(normalized, command, now).await {
                Ok(snapshot) => snapshot,
                Err(RakkaAgentA2AError::Task(error))
                    if matches!(
                        error,
                        rakka_agent::AgentTaskError::SubmissionRefused { .. }
                            | rakka_agent::AgentTaskError::NotCreated { .. }
                            | rakka_agent::AgentTaskError::Terminal { .. }
                            | rakka_agent::AgentTaskError::MaterializedStateTooLarge { .. }
                    ) =>
                {
                    return Err(RakkaAgentA2AError::Refused {
                        code: error.code().to_string(),
                        message: error.to_string(),
                    });
                }
                Err(error) => return Err(error),
            };
            // A validation rejection is a committed durable decision, so it
            // answers as the task view — never an error that claims nothing
            // happened; the rule code rides the projection's rejection echo.
            let run = self.current_run_status(normalized, &snapshot).await?;
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
            return self.public_task(normalized, None).await;
        }
        self.authorize(A2AOperation::SendMessage, normalized)
            .await?;
        let input = agent_task_input(&request.message)?;
        let target = resolve_agent_target(self.catalog.as_ref(), normalized)?;
        let command = agent_task_create_command(normalized, &target, input)?;

        let snapshot = self.apply_task_command(normalized, command, now).await?;
        let run = self.current_run_status(normalized, &snapshot).await?;
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
        self.public_task(normalized, None).await
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

    /// Replays one coordination scope's durable event log
    /// ([specification 17.13](../../../docs/plans/rakka-agent/spec.md);
    /// scenario 45).
    ///
    /// `scope` is an `AgentEntityAddress` key: `task/<tenant>/<id>`,
    /// `team/<tenant>/<id>`, `conversation/<tenant>/<id>`, or
    /// `run/<tenant>/<agent>/<id>`. Two fences run before any log is read — the
    /// scope's own tenant must be the authenticated one, and the deployment
    /// authorizer sees the addressed scope bound into its own
    /// [`A2AOperation::CoordinationEventRead`] class, never an undifferentiated
    /// read.
    ///
    /// An exhausted retention window is an *answer*, not an error: the reply's
    /// `WindowExpired` arm names the cursor to resume from once the caller has
    /// resynchronized from authoritative state.
    ///
    /// # Errors
    ///
    /// Fails when the tenant cannot be resolved, the scope does not parse or
    /// names another tenant, the authorizer denies, the class keeps no log, or a
    /// backing log faults.
    pub async fn replay_coordination_events(
        &self,
        params: &ServiceParams,
        request_tenant: Option<&str>,
        scope: &str,
        after_cursor: Option<&str>,
        limit: usize,
    ) -> RakkaAgentA2AResult<AgentCoordinationReplay> {
        let tenant = resolve_agent_tenant(
            self.tenant_resolver.as_ref(),
            self.default_tenant.as_deref(),
            params,
            request_tenant,
        )?;
        let address = AgentEntityAddress::parse(scope).map_err(|_| {
            RakkaAgentA2AError::Coordination(AgentCoordinationReplayError::MalformedCursor {
                cursor: scope.to_string(),
            })
        })?;
        // A scope key carries its own tenant. Trusting it would let an
        // authenticated caller read any tenant's coordination history by
        // spelling the key, so the authenticated tenant is the only one that
        // counts. This answers before the authorizer runs and reveals nothing:
        // the caller supplied the tenant it is being refused for.
        if address.tenant() != &tenant {
            return Err(RakkaAgentA2AError::Unauthorized);
        }
        let authorization = A2AAuthorizationRequest::new(A2AOperation::CoordinationEventRead)
            .with_tenant(tenant.as_str())
            .with_coordination(crate::auth::A2ACoordinationClaim {
                scope_class: address.class().as_label(),
                scope,
            });
        match self.authorizer.authorize(&authorization).await {
            A2AAuthorizationDecision::Allow => {}
            A2AAuthorizationDecision::Deny => return Err(RakkaAgentA2AError::Unauthorized),
        }

        // The durable drop count lives on the run record, not in the sink: the
        // sink cannot know what the outbox dropped before it arrived. Reading it
        // here is what lets a caller tell "resynchronize and you will have
        // everything" from "these decisions are gone".
        let mut sources = AgentCoordinationSources::new(
            &self.history,
            &self.team_history,
            &self.conversation_history,
        );
        if let Some(sink) = self.decision_events.as_ref() {
            sources = sources.with_run_events(sink.as_ref());
        }
        if let AgentEntityAddress::Run(run_scope) = &address {
            let losses = rakka_agent::agent_operational_snapshot(
                &self.runs,
                run_scope,
                &AgentSchemaPolicy::default(),
                self.clock.now(),
            )
            .await?
            .map_or(0, |snapshot| snapshot.decision_drops);
            sources = sources.with_run_losses(losses);
        }
        sources
            .replay(&address, after_cursor, limit)
            .await
            .map_err(Into::into)
    }

    /// Assembles the authorized goal view
    /// ([specification 17.18](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// `None` means the caller may not see it *or* it does not exist, and the
    /// two are byte-identical on purpose: the domain closed that existence
    /// oracle at the owner fence, and re-opening it at the wire would undo the
    /// work. A caller with no authenticated principal therefore gets `None` too,
    /// never an error naming the goal.
    ///
    /// # Errors
    ///
    /// Fails when the tenant cannot be resolved or a durable record the
    /// traversal reached is unreadable under the schema policy. Neither absence
    /// nor denial is a failure.
    pub async fn agent_goal_view(
        &self,
        params: &ServiceParams,
        request_tenant: Option<&str>,
        goal: &str,
        principal: Option<&PrincipalRef>,
        max_tasks: Option<usize>,
    ) -> RakkaAgentA2AResult<Option<AgentGoalView>> {
        let tenant = resolve_agent_tenant(
            self.tenant_resolver.as_ref(),
            self.default_tenant.as_deref(),
            params,
            request_tenant,
        )?;
        let authorization = A2AAuthorizationRequest::new(A2AOperation::GoalViewRead)
            .with_tenant(tenant.as_str())
            .with_principal(principal)
            .with_goal_view(crate::auth::A2AGoalViewClaim { goal, max_tasks });
        // A denial answers absent, not `Unauthorized`: the deny-is-absent
        // contract is the whole reason a non-owner cannot probe for a goal's
        // existence, and a distinguishable wire error would hand back exactly
        // that probe.
        if matches!(
            self.authorizer.authorize(&authorization).await,
            A2AAuthorizationDecision::Deny
        ) {
            return Ok(None);
        }
        let Some(principal) = principal else {
            return Ok(None);
        };
        let goal = AgentGoalId::new(goal)?;
        rakka_agent::authorized_agent_goal_view_bounded(
            &self.tasks,
            &self.runs,
            &tenant,
            &goal,
            principal,
            &AgentSchemaPolicy::default(),
            self.goal_claims.as_deref(),
            max_tasks.unwrap_or(AGENT_GOAL_VIEW_MAX_TASKS),
            self.clock.now(),
        )
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
        let mut store = self.task_store(scope);
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
        let mut store = self.task_store(scope);
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
        let mut store = self.task_store(scope);
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
        self.authorize_claimed(operation, normalized, None, None)
            .await
    }

    /// Runs the deployment authorizer for one operation, binding the claimed
    /// transfer into the request on a record-handoff check and the claimed
    /// result contract on a submit-task-result check.
    async fn authorize_claimed(
        &self,
        operation: A2AOperation,
        normalized: &NormalizedAgentCommand,
        handoff: Option<crate::auth::A2AHandoffClaim<'_>>,
        task_result: Option<crate::auth::A2ATaskResultClaim<'_>>,
    ) -> RakkaAgentA2AResult<()> {
        let mut request = A2AAuthorizationRequest::new(operation)
            .with_tenant(normalized.tenant.as_str())
            .with_task_id(normalized.task.as_str())
            .with_principal(normalized.principal.as_ref());
        request.handoff = handoff;
        request.task_result = task_result;
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

/// Builds the immediate response message one conversation command answers
/// with — the management-response precedent: a conversation command never
/// creates a task, so its outcome rides a message, not a task projection.
#[must_use]
pub fn conversation_response_message(
    request_message_id: &str,
    reply: &rakka_agent::AgentConversationEntityReply,
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
    message.message_id = format!("{request_message_id}::conversation-response");
    message
}
