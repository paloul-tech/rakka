//! Durable A2A request handler, service builder, and streaming.
//!
//! Public command paths acknowledge work only after the durable workflow
//! inbox accepts the command. Public read paths are served from the task
//! projection store (converging through the shard owner when routing is
//! configured). Streaming replays durable public task events; live updates
//! arrive through an [`A2ATaskEventWatcher`] with owner polling retained as
//! a fallback when no shared durable replay is available.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use a2a::{
    A2AError, AgentCard, CancelTaskRequest, DeleteTaskPushNotificationConfigRequest,
    GetExtendedAgentCardRequest, GetTaskPushNotificationConfigRequest, GetTaskRequest,
    ListTaskPushNotificationConfigsRequest, ListTaskPushNotificationConfigsResponse,
    ListTasksRequest, ListTasksResponse, SendMessageRequest, SendMessageResponse, StreamResponse,
    SubscribeToTaskRequest, Task, TaskArtifactUpdateEvent, TaskPushNotificationConfig, TaskStatus,
    TaskStatusUpdateEvent,
};
use a2a_server::{RequestHandler, ServiceParams};
use async_trait::async_trait;
use futures_util::stream::{self, BoxStream};
use rakka_agent_workflow::substrate::WorkflowError;
use rakka_agent_workflow::{
    agent_run_persistence_id, AgentInboxAcceptance, AgentInboxError, AgentRunEngineError,
    AgentRunId, AgentRunInbox, AgentRunState, AgentRunStatus, AgentRunTransition, AgentStepRunner,
    AgentTimestampMillis, AgentWorkflow, PrincipalRef,
};
use rakka_persistence::{DurableError, DurableStateStore};

use crate::auth::{
    A2AAuthorizationDecision, A2AAuthorizationRequest, A2AAuthorizer, A2AOperation,
    AllowAllAuthorizer,
};
use crate::catalog::A2AWorkflowCatalog;
use crate::error::RakkaA2AHandlerError;
use crate::mapping::{
    build_cancel_task_command_draft, build_send_message_command_draft, now_agent_timestamp,
    A2ACommandDraft, A2AHeaderTenantResolver, A2AMappingError, A2APayloadPolicy, A2ATenantResolver,
    A2AWorkflowSelection, DEFAULT_TENANT, META_WORKFLOW_ID,
};
use crate::projection::{
    A2ATaskEventSignal, A2ATaskEventSignalOutcome, A2ATaskEventWatcher, A2ATaskProjectionStore,
};
use crate::protocol::{
    A2AProjectionHints, A2ARunCommandMetadata, A2ARunFailureKind, A2ARunRequest, A2ARunRequestKind,
    A2ARunResponse, A2ARunResponseKind, A2ATimeoutPolicy, A2A_RUN_PROTOCOL_VERSION,
};
use crate::push::{schedule_push_effects_for_events, A2APushConfigStore};
use crate::routing::{A2ADrainGate, A2ARunRoute};
use crate::runsync::{
    artifact_refs, missing_run, project_send_result, projected_message, recover_context_id,
    run_is_terminal, run_tenant, snapshot_projection, sync_status_projection, validate_adopted_run,
    validate_send_lifecycle,
};
use crate::stores::{A2ARunStateStore, A2AWorkflowStateStore};
use crate::stream::{
    A2AStreamLease, A2AStreamLimitSettings, A2AStreamLimits, A2AStreamMetricsSnapshot,
};
use crate::task::{
    encode_replay_cursor, parse_replay_cursor, A2ATaskEvent, A2ATaskEventPayload,
    A2ATaskProjection, TaskProjectionError,
};

/// Stream frame metadata key carrying the public task-event kind.
pub const META_TASK_EVENT_KIND: &str = "io.rakka.task_event.kind";
/// Stream frame metadata key carrying the replay cursor to resume from.
pub const META_REPLAY_CURSOR: &str = "io.rakka.replay.cursor";
/// Stream frame metadata key carrying the event redaction label.
pub const META_REDACTION: &str = "io.rakka.redaction";
/// Stream frame metadata key marking synthetic stream events (heartbeats).
pub const META_STREAM_EVENT: &str = "io.rakka.stream.event";
/// Transport header carrying an explicit replay cursor on subscribe.
pub const REPLAY_CURSOR_HEADER: &str = "rakka-a2a-replay-cursor";
/// Standard SSE reconnect header honored as a replay cursor.
pub const LAST_EVENT_ID_HEADER: &str = "last-event-id";

/// Diagnostics hook invoked with the service parameters of every request.
type RequestObserver = Arc<dyn Fn(&ServiceParams) + Send + Sync>;

/// Bound on optimistic-concurrency re-drives for inbox accepts and run
/// transitions; each attempt requires a distinct concurrent writer, so the
/// bound is a livelock guard rather than a functional limit.
const MAX_CONFLICT_ATTEMPTS: usize = 3;
/// Consecutive owner-poll failures tolerated before a stream ends with
/// reconnect guidance; a single transient blip must not tear down every
/// subscriber for a task.
const MAX_STREAM_POLL_FAILURES: usize = 3;

/// Tunable service behavior with bounded, secure defaults.
#[derive(Debug, Clone, Copy)]
pub struct RakkaA2ASettings {
    /// Inline payload policy for accepted messages.
    ///
    /// Defaults to a bounded inline limit with **no** artifact strategy:
    /// oversized payloads are rejected until the application enables
    /// artifact references and persists their content.
    pub payload_policy: A2APayloadPolicy,
    /// Bounded streaming admission limits.
    pub stream_limits: A2AStreamLimitSettings,
    /// Ask timeout for owner-routed and local run-actor requests.
    pub run_ask_timeout: Duration,
    /// Idle heartbeat interval for open streams.
    pub stream_heartbeat_interval: Duration,
    /// Owner poll interval for streams served away from the shard owner
    /// when no shared durable replay is available.
    pub stream_owner_poll_interval: Duration,
}

impl Default for RakkaA2ASettings {
    fn default() -> Self {
        Self {
            payload_policy: A2APayloadPolicy::new().without_artifact_strategy(),
            stream_limits: A2AStreamLimitSettings::default(),
            run_ask_timeout: Duration::from_secs(3),
            stream_heartbeat_interval: Duration::from_secs(15),
            stream_owner_poll_interval: Duration::from_secs(2),
        }
    }
}

/// Tenant handling mode selected at build time.
#[derive(Debug, Clone)]
enum TenantMode {
    /// Single-tenant/local mode: commands without tenant input fall back to
    /// the default tenant; reads may stay unscoped when the store allows it.
    SingleTenant { default_tenant: String },
    /// Tenant-scoped production mode: every durable read and command carries
    /// a resolved tenant; unscoped reads are refused (DN-3).
    TenantScoped,
}

/// Build-time validation failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RakkaA2ABuildError {
    /// A required component was not supplied.
    Missing {
        /// Stable component name.
        component: &'static str,
    },
    /// Tenant-scoped mode requires a tenant-scoped projection store.
    UnscopedStoreInTenantScopedMode,
}

impl std::fmt::Display for RakkaA2ABuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Missing { component } => write!(f, "{component} is required"),
            Self::UnscopedStoreInTenantScopedMode => f.write_str(
                "tenant-scoped mode requires a projection store that refuses unscoped reads",
            ),
        }
    }
}

impl std::error::Error for RakkaA2ABuildError {}

/// Builder for [`RakkaA2AService`].
///
/// Secure defaults: raw push credentials are rejected by the push config
/// store unless the application overrides its policy, oversized payloads are
/// rejected until an artifact strategy exists, and tenant-scoped mode
/// refuses configurations that could issue unscoped reads.
pub struct RakkaA2AServiceBuilder {
    agent_card: Option<AgentCard>,
    catalog: Option<Arc<dyn A2AWorkflowCatalog>>,
    task_store: Option<Arc<dyn A2ATaskProjectionStore>>,
    task_event_watcher: Option<Arc<dyn A2ATaskEventWatcher>>,
    run_store: Option<A2ARunStateStore>,
    workflow_store: Option<A2AWorkflowStateStore>,
    push_configs: Option<A2APushConfigStore>,
    tenant_mode: TenantMode,
    tenant_resolver: Arc<dyn A2ATenantResolver>,
    authorizer: Arc<dyn A2AAuthorizer>,
    settings: RakkaA2ASettings,
    drain_gate: A2ADrainGate,
    router: Option<Arc<dyn A2ARunRoute>>,
    request_observer: Option<RequestObserver>,
}

impl std::fmt::Debug for RakkaA2AServiceBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RakkaA2AServiceBuilder")
            .finish_non_exhaustive()
    }
}

impl RakkaA2AServiceBuilder {
    /// Starts a builder in single-tenant mode with the crate default tenant.
    #[must_use]
    pub fn new() -> Self {
        Self {
            agent_card: None,
            catalog: None,
            task_store: None,
            task_event_watcher: None,
            run_store: None,
            workflow_store: None,
            push_configs: None,
            tenant_mode: TenantMode::SingleTenant {
                default_tenant: DEFAULT_TENANT.to_string(),
            },
            tenant_resolver: Arc::new(A2AHeaderTenantResolver),
            authorizer: Arc::new(AllowAllAuthorizer),
            settings: RakkaA2ASettings::default(),
            drain_gate: A2ADrainGate::new(),
            router: None,
            request_observer: None,
        }
    }

    /// Sets the public agent card served by this service.
    #[must_use]
    pub fn agent_card(mut self, card: AgentCard) -> Self {
        self.agent_card = Some(card);
        self
    }

    /// Sets the hosted workflow catalog.
    #[must_use]
    pub fn workflow_catalog(mut self, catalog: impl A2AWorkflowCatalog) -> Self {
        self.catalog = Some(Arc::new(catalog));
        self
    }

    /// Convenience for hosting exactly one workflow.
    #[must_use]
    pub fn single_workflow(self, workflow: AgentWorkflow) -> Self {
        self.workflow_catalog(crate::catalog::A2AStaticWorkflowCatalog::single(workflow))
    }

    /// Sets the task projection store.
    #[must_use]
    pub fn task_store(mut self, store: impl A2ATaskProjectionStore) -> Self {
        self.task_store = Some(Arc::new(store));
        self
    }

    /// Sets a task projection store that also serves as its event watcher
    /// (the in-memory store, for example).
    #[must_use]
    pub fn task_store_with_watcher<S>(mut self, store: S) -> Self
    where
        S: A2ATaskProjectionStore + A2ATaskEventWatcher + Clone,
    {
        self.task_event_watcher = Some(Arc::new(store.clone()));
        self.task_store = Some(Arc::new(store));
        self
    }

    /// Sets the durable task-event watcher used by streams.
    ///
    /// Optional: without a watcher, streams fall back to owner polling (when
    /// routing is configured) and heartbeats.
    #[must_use]
    pub fn task_event_watcher(mut self, watcher: impl A2ATaskEventWatcher) -> Self {
        self.task_event_watcher = Some(Arc::new(watcher));
        self
    }

    /// Sets the durable run state store.
    #[must_use]
    pub fn run_store(mut self, store: impl DurableStateStore<AgentRunState>) -> Self {
        self.run_store = Some(A2ARunStateStore::new(store));
        self
    }

    /// Sets the durable workflow inbox/outbox store.
    #[must_use]
    pub fn workflow_store(
        mut self,
        store: impl DurableStateStore<rakka_agent_workflow::substrate::WorkflowState>,
    ) -> Self {
        self.workflow_store = Some(A2AWorkflowStateStore::new(store));
        self
    }

    /// Sets the durable push config store.
    #[must_use]
    pub fn push_config_store(mut self, store: A2APushConfigStore) -> Self {
        self.push_configs = Some(store);
        self
    }

    /// Selects single-tenant mode with an explicit default tenant.
    #[must_use]
    pub fn single_tenant(mut self, default_tenant: impl Into<String>) -> Self {
        self.tenant_mode = TenantMode::SingleTenant {
            default_tenant: default_tenant.into(),
        };
        self
    }

    /// Selects tenant-scoped production mode with an explicit resolver.
    ///
    /// In this mode every durable read and command carries a resolved
    /// tenant; requests without tenant input are rejected, and the builder
    /// refuses projection stores that would permit unscoped reads.
    #[must_use]
    pub fn tenant_scoped(mut self, resolver: impl A2ATenantResolver) -> Self {
        self.tenant_mode = TenantMode::TenantScoped;
        self.tenant_resolver = Arc::new(resolver);
        self
    }

    /// Overrides the tenant resolver without changing the tenant mode.
    #[must_use]
    pub fn tenant_resolver(mut self, resolver: impl A2ATenantResolver) -> Self {
        self.tenant_resolver = Arc::new(resolver);
        self
    }

    /// Installs an authorization hook for public operations.
    #[must_use]
    pub fn authorizer(mut self, authorizer: impl A2AAuthorizer) -> Self {
        self.authorizer = Arc::new(authorizer);
        self
    }

    /// Overrides tunable service settings.
    #[must_use]
    pub fn settings(mut self, settings: RakkaA2ASettings) -> Self {
        self.settings = settings;
        self
    }

    /// Injects an externally owned drain gate (for Kubernetes readiness).
    #[must_use]
    pub fn drain_gate(mut self, gate: A2ADrainGate) -> Self {
        self.drain_gate = gate;
        self
    }

    /// Routes owner-only work through cluster sharding.
    #[must_use]
    pub fn router(mut self, router: impl A2ARunRoute) -> Self {
        self.router = Some(Arc::new(router));
        self
    }

    /// Observes the service parameters of every handled request
    /// (diagnostics hooks; must be cheap and non-blocking).
    #[must_use]
    pub fn request_observer(
        mut self,
        observer: impl Fn(&ServiceParams) + Send + Sync + 'static,
    ) -> Self {
        self.request_observer = Some(Arc::new(observer));
        self
    }

    /// Validates the configuration and builds the service.
    pub fn build(self) -> Result<RakkaA2AService, RakkaA2ABuildError> {
        let agent_card = self.agent_card.ok_or(RakkaA2ABuildError::Missing {
            component: "agent_card",
        })?;
        let catalog = self.catalog.ok_or(RakkaA2ABuildError::Missing {
            component: "workflow_catalog",
        })?;
        let task_store = self.task_store.ok_or(RakkaA2ABuildError::Missing {
            component: "task_store",
        })?;
        let run_store = self.run_store.ok_or(RakkaA2ABuildError::Missing {
            component: "run_store",
        })?;
        let workflow_store = self.workflow_store.ok_or(RakkaA2ABuildError::Missing {
            component: "workflow_store",
        })?;
        let push_configs = self.push_configs.ok_or(RakkaA2ABuildError::Missing {
            component: "push_config_store",
        })?;
        if matches!(self.tenant_mode, TenantMode::TenantScoped)
            && !task_store.requires_tenant_scope()
        {
            return Err(RakkaA2ABuildError::UnscopedStoreInTenantScopedMode);
        }

        let handler = Arc::new(RakkaA2ARequestHandler {
            agent_card: agent_card.clone(),
            catalog,
            task_store,
            task_event_watcher: self.task_event_watcher,
            run_store,
            workflow_store,
            push_configs,
            tenant_mode: self.tenant_mode,
            tenant_resolver: self.tenant_resolver,
            authorizer: self.authorizer,
            settings: self.settings,
            stream_limits: A2AStreamLimits::new(self.settings.stream_limits),
            drain_gate: self.drain_gate.clone(),
            router: self.router,
            request_observer: self.request_observer,
        });
        Ok(RakkaA2AService {
            handler,
            drain_gate: self.drain_gate,
            agent_card,
        })
    }
}

impl Default for RakkaA2AServiceBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// A built A2A service: the request handler plus its operational handles.
#[derive(Clone)]
pub struct RakkaA2AService {
    handler: Arc<RakkaA2ARequestHandler>,
    drain_gate: A2ADrainGate,
    agent_card: AgentCard,
}

impl std::fmt::Debug for RakkaA2AService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RakkaA2AService").finish_non_exhaustive()
    }
}

impl RakkaA2AService {
    /// Starts a service builder.
    #[must_use]
    pub fn builder() -> RakkaA2AServiceBuilder {
        RakkaA2AServiceBuilder::new()
    }

    /// The shared request handler implementing the A2A SDK `RequestHandler`.
    #[must_use]
    pub fn handler(&self) -> Arc<RakkaA2ARequestHandler> {
        Arc::clone(&self.handler)
    }

    /// The node drain gate wired into this service.
    #[must_use]
    pub fn drain_gate(&self) -> A2ADrainGate {
        self.drain_gate.clone()
    }

    /// The public agent card this service serves.
    #[must_use]
    pub fn agent_card(&self) -> &AgentCard {
        &self.agent_card
    }

    /// Bounded stream metrics for operational snapshots.
    #[must_use]
    pub fn stream_metrics(&self) -> A2AStreamMetricsSnapshot {
        self.handler.stream_limits.snapshot()
    }

    /// Aggregated operational snapshot for production review.
    ///
    /// `push_delivery` is supplied by the caller (the dispatcher coordinator
    /// lives out-of-band, driven by the durable dispatcher fleet), so pass
    /// its snapshot in when a dispatcher is configured.
    #[must_use]
    pub fn adapter_snapshot(
        &self,
        push_delivery: Option<crate::dispatch::A2APushDispatchSnapshot>,
    ) -> crate::observability::A2AAdapterSnapshot {
        crate::observability::A2AAdapterSnapshot {
            accepting_public_commands: self.drain_gate.accepts_public_commands(),
            streams: self.handler.stream_limits.snapshot(),
            push_delivery,
            projection_backend: self.handler.task_store.backend_name(),
        }
    }

    /// Rebuilds any missing task projections from durable run state.
    ///
    /// Run at boot after restarts that lost a node-local projection store;
    /// shared durable projection stores generally do not need it.
    pub async fn recover_task_projections(&self) -> Result<usize, RakkaA2AHandlerError> {
        self.handler.recover_task_projections().await
    }
}

/// A2A SDK `RequestHandler` backed by durable Rakka stores and optional
/// sharded owner routing.
pub struct RakkaA2ARequestHandler {
    agent_card: AgentCard,
    catalog: Arc<dyn A2AWorkflowCatalog>,
    task_store: Arc<dyn A2ATaskProjectionStore>,
    task_event_watcher: Option<Arc<dyn A2ATaskEventWatcher>>,
    run_store: A2ARunStateStore,
    workflow_store: A2AWorkflowStateStore,
    push_configs: A2APushConfigStore,
    tenant_mode: TenantMode,
    tenant_resolver: Arc<dyn A2ATenantResolver>,
    authorizer: Arc<dyn A2AAuthorizer>,
    settings: RakkaA2ASettings,
    stream_limits: A2AStreamLimits,
    drain_gate: A2ADrainGate,
    router: Option<Arc<dyn A2ARunRoute>>,
    request_observer: Option<RequestObserver>,
}

impl std::fmt::Debug for RakkaA2ARequestHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RakkaA2ARequestHandler")
            .finish_non_exhaustive()
    }
}

impl RakkaA2ARequestHandler {
    /// Returns whether mutating public ingress is still accepted.
    #[must_use]
    pub fn accepts_public_commands(&self) -> bool {
        self.drain_gate.accepts_public_commands()
    }

    /// Closes mutating public ingress on this node for graceful drain.
    pub fn begin_drain(&self) {
        self.drain_gate.begin_drain();
    }

    fn ensure_accepting_public_commands(&self) -> Result<(), RakkaA2AHandlerError> {
        if self.accepts_public_commands() {
            Ok(())
        } else {
            Err(RakkaA2AHandlerError::Draining)
        }
    }

    fn record(&self, params: &ServiceParams) {
        if let Some(observer) = &self.request_observer {
            observer(params);
        }
    }

    fn default_tenant(&self) -> Option<&str> {
        match &self.tenant_mode {
            TenantMode::SingleTenant { default_tenant } => Some(default_tenant.as_str()),
            TenantMode::TenantScoped => None,
        }
    }

    /// Resolves the tenant scope for a read per the crate's DN-3 rule.
    fn read_scope(
        &self,
        params: &ServiceParams,
        request_tenant: Option<&str>,
    ) -> Result<Option<String>, RakkaA2AHandlerError> {
        let resolved = self
            .tenant_resolver
            .resolve_read_tenant(params, request_tenant)
            .map_err(RakkaA2AHandlerError::Mapping)?;
        if resolved.is_some() {
            return Ok(resolved);
        }
        match &self.tenant_mode {
            TenantMode::SingleTenant { default_tenant } => {
                if self.task_store.requires_tenant_scope() {
                    Ok(Some(default_tenant.clone()))
                } else {
                    // Unscoped local-mode read; the store resolves the task's
                    // stored tenant.
                    Ok(None)
                }
            }
            TenantMode::TenantScoped => Err(RakkaA2AHandlerError::Mapping(
                A2AMappingError::TenantRequired,
            )),
        }
    }

    async fn authorize(
        &self,
        operation: A2AOperation,
        tenant: Option<&str>,
        task_id: Option<&str>,
        principal: Option<&PrincipalRef>,
    ) -> Result<(), RakkaA2AHandlerError> {
        let request = A2AAuthorizationRequest {
            operation,
            tenant,
            task_id,
            principal,
        };
        match self.authorizer.authorize(&request).await {
            A2AAuthorizationDecision::Allow => Ok(()),
            A2AAuthorizationDecision::Deny => Err(RakkaA2AHandlerError::NotAuthorized {
                task_id: task_id.map(str::to_string),
            }),
        }
    }

    /// Resolves the workflow of record for an existing run, when it exists.
    async fn workflow_of_record(
        &self,
        task_id: &str,
    ) -> Result<Option<AgentWorkflow>, RakkaA2AHandlerError> {
        let persistence_id = agent_run_persistence_id(&AgentRunId::new(task_id.to_string()));
        let Some(record) = self.run_store.load(&persistence_id).await? else {
            return Ok(None);
        };
        let state = record.state;
        // The run pins its workflow; prefer the exact definition version,
        // fall back to the id alone, then to the default so read paths keep
        // working when a definition version was retired from the catalog.
        let workflow = self
            .catalog
            .resolve_by_id(
                state.workflow_id.as_str(),
                Some(state.definition_version.as_str()),
            )
            .or_else(|| self.catalog.resolve_by_id(state.workflow_id.as_str(), None))
            .cloned()
            .unwrap_or_else(|| self.catalog.default_workflow().clone());
        Ok(Some(workflow))
    }

    /// Resolves the workflow for a send request per the catalog policy.
    async fn workflow_for_send(
        &self,
        req: &SendMessageRequest,
    ) -> Result<AgentWorkflow, RakkaA2AHandlerError> {
        if let Some(task_id) = req
            .message
            .task_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        {
            if let Some(workflow) = self.workflow_of_record(task_id).await? {
                return Ok(workflow);
            }
        }
        let selection = A2AWorkflowSelection::from_send_message_request(req)
            .map_err(RakkaA2AHandlerError::Mapping)?;
        self.catalog
            .resolve(&selection)
            .cloned()
            .ok_or_else(|| unknown_workflow_selection(&selection))
    }

    /// Resolves the workflow for a cancel request (workflow of record wins).
    async fn workflow_for_cancel(
        &self,
        req: &CancelTaskRequest,
    ) -> Result<AgentWorkflow, RakkaA2AHandlerError> {
        if let Some(workflow) = self.workflow_of_record(&req.id).await? {
            return Ok(workflow);
        }
        Ok(self.catalog.default_workflow().clone())
    }

    fn owner_timeout(&self) -> A2ATimeoutPolicy {
        A2ATimeoutPolicy::from_duration(self.settings.run_ask_timeout)
    }

    async fn route_for_projection(
        &self,
        router: &Arc<dyn A2ARunRoute>,
        request: A2ARunRequest,
    ) -> Result<A2ATaskProjection, RakkaA2AHandlerError> {
        let response = router.route(request).await?;
        let projection = projection_from_response(response)?;
        // Cache owner-served projections node-locally; shared durable stores
        // already observe the owner's writes, so skip the redundant upsert.
        if !self.task_store.supports_shared_replay() {
            self.task_store.upsert(projection.clone()).await?;
        }
        Ok(projection)
    }

    async fn authorized_task_projection(
        &self,
        params: &ServiceParams,
        task_id: &str,
        request_tenant: Option<&str>,
    ) -> Result<A2ATaskProjection, RakkaA2AHandlerError> {
        let tenant = self.read_scope(params, request_tenant)?;
        if let Some(router) = &self.router {
            let request = A2ARunRequest::new(
                task_id.to_string(),
                tenant,
                A2ARunCommandMetadata::query(),
                A2AProjectionHints::default(),
                self.owner_timeout(),
                A2ARunRequestKind::QueryTaskSnapshot,
            );
            self.route_for_projection(router, request).await
        } else {
            self.task_store
                .projection(tenant.as_deref(), task_id)
                .await
                .map_err(Into::into)
        }
    }

    async fn schedule_push_effects(
        &self,
        tenant: &str,
        task_id: &str,
        events: &[A2ATaskEvent],
    ) -> Result<(), RakkaA2AHandlerError> {
        schedule_push_effects_for_events(
            &self.workflow_store,
            &self.push_configs,
            self.task_store.as_ref(),
            tenant,
            task_id,
            events,
        )
        .await?;
        Ok(())
    }

    async fn save_request_push_config(
        &self,
        draft: &A2ACommandDraft,
        req: &SendMessageRequest,
    ) -> Result<(), RakkaA2AHandlerError> {
        let Some(config) = request_push_config(draft, req) else {
            return Ok(());
        };
        self.push_configs
            .save(draft.normalized.tenant.as_str(), config)
            .await?;
        Ok(())
    }

    async fn send_message_impl(
        &self,
        params: &ServiceParams,
        req: SendMessageRequest,
    ) -> Result<SendMessageResponse, RakkaA2AHandlerError> {
        self.ensure_accepting_public_commands()?;
        let received_at = now_agent_timestamp();
        let workflow = self.workflow_for_send(&req).await?;
        let draft = build_send_message_command_draft(
            self.tenant_resolver.as_ref(),
            self.default_tenant(),
            params,
            &req,
            &workflow,
            self.settings.payload_policy,
            received_at,
        )?;
        self.authorize(
            A2AOperation::SendMessage,
            Some(draft.normalized.tenant.as_str()),
            Some(&draft.normalized.task_id),
            draft.normalized.principal.as_ref(),
        )
        .await?;
        self.send_message_with_draft(req, workflow, draft, received_at)
            .await
    }

    /// Accepts a send whose command draft the caller already built, so the
    /// streaming path normalizes the request exactly once.
    async fn send_message_with_draft(
        &self,
        req: SendMessageRequest,
        workflow: AgentWorkflow,
        draft: A2ACommandDraft,
        received_at: AgentTimestampMillis,
    ) -> Result<SendMessageResponse, RakkaA2AHandlerError> {
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
                self.owner_timeout(),
                A2ARunRequestKind::AcceptMessage {
                    projected_message: Box::new(projected_message(&req.message, &draft)),
                    artifacts: artifact_refs(&draft.payload),
                    request_push_config: request_push_config(&draft, &req),
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
        let mut runner = self
            .recovered_runner(&workflow, draft.normalized.run_id())
            .await?;
        let existing_state = runner.state()?.cloned();
        validate_send_lifecycle(&draft, existing_state.as_ref())?;
        self.accept_draft(&draft, existing_state.is_some()).await?;
        let mut run_state = match existing_state {
            Some(state) => state,
            None => {
                let (state, adopted) = self
                    .start_run(&mut runner, &workflow, &draft, received_at)
                    .await?;
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
        self.save_request_push_config(&draft, &req).await?;
        let events = project_send_result(
            self.task_store.as_ref(),
            &draft,
            &projected_message,
            artifacts,
            &run_state,
            received_at,
        )
        .await?;
        // Runs even when this retry emitted nothing new: the scheduler works
        // from its watermark over the retained log, so a retry heals a push
        // schedule that failed after the original acceptance.
        self.schedule_push_effects(
            draft.normalized.tenant.as_str(),
            &draft.normalized.task_id,
            &events,
        )
        .await?;
        let task = self
            .task_store
            .get(
                Some(draft.normalized.tenant.as_str()),
                &draft.normalized.task_id,
                req.configuration
                    .as_ref()
                    .and_then(|config| config.history_length),
            )
            .await?;
        Ok(SendMessageResponse::Task(task))
    }

    async fn cancel_task_impl(
        &self,
        params: &ServiceParams,
        req: CancelTaskRequest,
    ) -> Result<Task, RakkaA2AHandlerError> {
        self.ensure_accepting_public_commands()?;
        let received_at = now_agent_timestamp();
        let workflow = self.workflow_for_cancel(&req).await?;
        let draft = build_cancel_task_command_draft(
            self.tenant_resolver.as_ref(),
            self.default_tenant(),
            params,
            &req,
            &workflow,
            received_at,
        )?;
        self.authorize(
            A2AOperation::CancelTask,
            Some(draft.normalized.tenant.as_str()),
            Some(&draft.normalized.task_id),
            draft.normalized.principal.as_ref(),
        )
        .await?;
        if let Some(router) = &self.router {
            let tenant = draft.normalized.tenant.as_str().to_string();
            let request = A2ARunRequest::new(
                draft.normalized.task_id.clone(),
                Some(tenant),
                A2ARunCommandMetadata::from_draft(&draft),
                A2AProjectionHints::default(),
                self.owner_timeout(),
                A2ARunRequestKind::CancelTask {
                    draft: Box::new(draft),
                    received_at,
                },
            );
            let projection = self.route_for_projection(router, request).await?;
            return Ok(projection.to_task(None, true));
        }
        let mut runner = self
            .recovered_runner(&workflow, draft.normalized.run_id())
            .await?;
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
            let events: Vec<A2ATaskEvent> = sync_status_projection(
                self.task_store.as_ref(),
                &run_state,
                &draft.normalized.context_id,
                received_at,
                None,
            )
            .await?
            .into_iter()
            .collect();
            self.schedule_push_effects(
                draft.normalized.tenant.as_str(),
                &draft.normalized.task_id,
                &events,
            )
            .await?;
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
        let events: Vec<A2ATaskEvent> = sync_status_projection(
            self.task_store.as_ref(),
            &run_state,
            &draft.normalized.context_id,
            received_at,
            None,
        )
        .await?
        .into_iter()
        .collect();
        self.schedule_push_effects(
            draft.normalized.tenant.as_str(),
            &draft.normalized.task_id,
            &events,
        )
        .await?;
        self.task_store
            .get(
                Some(draft.normalized.tenant.as_str()),
                &draft.normalized.task_id,
                None,
            )
            .await
            .map_err(Into::into)
    }

    async fn send_streaming_message_impl(
        &self,
        params: &ServiceParams,
        req: SendMessageRequest,
    ) -> Result<BoxStream<'static, Result<StreamResponse, A2AError>>, RakkaA2AHandlerError> {
        self.ensure_accepting_public_commands()?;
        let received_at = now_agent_timestamp();
        let workflow = self.workflow_for_send(&req).await?;
        let draft = build_send_message_command_draft(
            self.tenant_resolver.as_ref(),
            self.default_tenant(),
            params,
            &req,
            &workflow,
            self.settings.payload_policy,
            received_at,
        )?;
        self.authorize(
            A2AOperation::SendMessage,
            Some(draft.normalized.tenant.as_str()),
            Some(&draft.normalized.task_id),
            draft.normalized.principal.as_ref(),
        )
        .await?;
        let tenant = draft.normalized.tenant.as_str().to_string();
        let task_id = draft.normalized.task_id.clone();
        // Admission runs before the durable accept: a stream-limit rejection
        // must not leave the client told "retry" for a send that actually
        // committed. The lease is dropped (released) on any later error.
        let lease = self.stream_limits.acquire(&task_id).map_err(|error| {
            RakkaA2AHandlerError::StreamLimit {
                message: error.message().to_string(),
            }
        })?;
        let _response = self
            .send_message_with_draft(req, workflow, draft, received_at)
            .await?;
        let projection = self.task_store.projection(Some(&tenant), &task_id).await?;
        self.stream_task(tenant, task_id, None, true, projection, lease)
            .await
    }

    async fn subscribe_to_task_impl(
        &self,
        params: &ServiceParams,
        req: SubscribeToTaskRequest,
    ) -> Result<BoxStream<'static, Result<StreamResponse, A2AError>>, RakkaA2AHandlerError> {
        self.ensure_accepting_public_commands()?;
        let requested_tenant = self.read_scope(params, req.tenant.as_deref())?;
        self.authorize(
            A2AOperation::SubscribeToTask,
            requested_tenant.as_deref(),
            Some(&req.id),
            None,
        )
        .await?;
        // Admit before the owner round-trip so over-limit subscribes fail
        // fast; the lease is dropped (released) on any later error.
        let lease = self.stream_limits.acquire(&req.id).map_err(|error| {
            RakkaA2AHandlerError::StreamLimit {
                message: error.message().to_string(),
            }
        })?;
        let projection = if let Some(router) = &self.router {
            let request = A2ARunRequest::new(
                req.id.clone(),
                requested_tenant.clone(),
                A2ARunCommandMetadata::query(),
                A2AProjectionHints::default(),
                self.owner_timeout(),
                A2ARunRequestKind::QueryTaskSnapshot,
            );
            self.route_for_projection(router, request).await?
        } else {
            self.task_store
                .projection(requested_tenant.as_deref(), &req.id)
                .await?
        };
        let tenant = projection.tenant.clone();
        let task_id = projection.task_id.clone();
        self.stream_task(
            tenant,
            task_id,
            replay_cursor_from_params(params),
            false,
            projection,
            lease,
        )
        .await
    }

    async fn create_push_config_impl(
        &self,
        params: &ServiceParams,
        req: TaskPushNotificationConfig,
    ) -> Result<TaskPushNotificationConfig, RakkaA2AHandlerError> {
        self.ensure_accepting_public_commands()?;
        let projection = self
            .authorized_task_projection(params, &req.task_id, req.tenant.as_deref())
            .await?;
        self.authorize(
            A2AOperation::PushConfigWrite,
            Some(&projection.tenant),
            Some(&projection.task_id),
            None,
        )
        .await?;
        if let Some(router) = &self.router {
            let request = A2ARunRequest::new(
                projection.task_id.clone(),
                Some(projection.tenant.clone()),
                A2ARunCommandMetadata::query(),
                A2AProjectionHints::default(),
                self.owner_timeout(),
                A2ARunRequestKind::RecordPushConfig { config: req },
            );
            let response = router.route(request).await?;
            return push_config_from_response(response);
        }
        self.push_configs
            .save(&projection.tenant, req)
            .await
            .map_err(Into::into)
    }

    async fn get_push_config_impl(
        &self,
        params: &ServiceParams,
        req: GetTaskPushNotificationConfigRequest,
    ) -> Result<TaskPushNotificationConfig, RakkaA2AHandlerError> {
        let projection = self
            .authorized_task_projection(params, &req.task_id, req.tenant.as_deref())
            .await?;
        self.authorize(
            A2AOperation::PushConfigRead,
            Some(&projection.tenant),
            Some(&projection.task_id),
            None,
        )
        .await?;
        self.push_configs
            .get(&projection.tenant, &req.task_id, &req.id)
            .await
            .map_err(Into::into)
    }

    async fn list_push_configs_impl(
        &self,
        params: &ServiceParams,
        req: ListTaskPushNotificationConfigsRequest,
    ) -> Result<ListTaskPushNotificationConfigsResponse, RakkaA2AHandlerError> {
        let projection = self
            .authorized_task_projection(params, &req.task_id, req.tenant.as_deref())
            .await?;
        self.authorize(
            A2AOperation::PushConfigRead,
            Some(&projection.tenant),
            Some(&projection.task_id),
            None,
        )
        .await?;
        self.push_configs
            .list(&projection.tenant, &req)
            .await
            .map_err(Into::into)
    }

    async fn delete_push_config_impl(
        &self,
        params: &ServiceParams,
        req: DeleteTaskPushNotificationConfigRequest,
    ) -> Result<(), RakkaA2AHandlerError> {
        self.ensure_accepting_public_commands()?;
        let projection = self
            .authorized_task_projection(params, &req.task_id, req.tenant.as_deref())
            .await?;
        self.authorize(
            A2AOperation::PushConfigWrite,
            Some(&projection.tenant),
            Some(&projection.task_id),
            None,
        )
        .await?;
        if let Some(router) = &self.router {
            let request = A2ARunRequest::new(
                projection.task_id.clone(),
                Some(projection.tenant.clone()),
                A2ARunCommandMetadata::query(),
                A2AProjectionHints::default(),
                self.owner_timeout(),
                A2ARunRequestKind::DeletePushConfig { config_id: req.id },
            );
            let response = router.route(request).await?;
            return push_delete_from_response(response);
        }
        self.push_configs
            .delete(&projection.tenant, &req.task_id, &req.id)
            .await
            .map_err(Into::into)
    }

    /// Rebuilds any missing task projections from durable run state.
    pub async fn recover_task_projections(&self) -> Result<usize, RakkaA2AHandlerError> {
        let ids = self.run_store.persistence_ids().await?;
        let prefix = format!("{}:", rakka_agent_workflow::AGENT_RUN_PERSISTENCE_PREFIX);
        let mut recovered = 0;
        for persistence_id in ids {
            let Some(run_id) = persistence_id.as_str().strip_prefix(prefix.as_str()) else {
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
                .await
                .is_ok()
            {
                continue;
            }
            let context_id = recover_context_id(&self.workflow_store, &run_id)
                .await?
                .unwrap_or_else(|| state.run_id.as_str().to_string());
            // Boot recovery rebuilds caches for already-durable state; it is
            // not a new transition, so no push effects are scheduled.
            let _ = snapshot_projection(
                self.task_store.as_ref(),
                &state,
                &context_id,
                Vec::new(),
                Vec::new(),
                state.updated_at,
            )
            .await?;
            recovered += 1;
        }
        Ok(recovered)
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
        // instead of silently merging the two tasks.
        if run_exists
            && matches!(
                draft.normalized.intent,
                crate::mapping::A2ATaskIntent::NewTask
            )
            && !crate::runsync::known_command(state, draft)
        {
            return Err(RakkaA2AHandlerError::InvalidLifecycle {
                task_id: draft.normalized.task_id.clone(),
                reason: "generated task id collides with an existing task",
            });
        }
        // A concurrent request can win the inbox write; re-recover and retry
        // a bounded number of times — each retry dedupes against the winner's
        // entry or accepts cleanly at the new revision.
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
        runner: &mut AgentStepRunner<A2ARunStateStore>,
        workflow: &AgentWorkflow,
        draft: &A2ACommandDraft,
        now: AgentTimestampMillis,
    ) -> Result<(AgentRunState, bool), RakkaA2AHandlerError> {
        let initial = crate::runsync::initial_run_state(workflow, draft, now)?;
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
        runner: &mut AgentStepRunner<A2ARunStateStore>,
        now: AgentTimestampMillis,
    ) -> Result<AgentRunState, RakkaA2AHandlerError> {
        let begin_at = AgentTimestampMillis::new(now.as_millis().saturating_add(1));
        let result = runner.begin_step(begin_at).await;
        adopt_on_conflict(runner, result).await
    }

    /// Drives the run to the durable terminal `Cancelled` state.
    ///
    /// Nothing is in flight in this adapter phase, so a durably accepted
    /// cancellation completes immediately: `Cancelling` is transient and the
    /// public task state becomes the terminal `Canceled` instead of reading
    /// as `Working` forever. Once step execution and in-flight effects
    /// exist, completion must move to the executor, which has to drain
    /// outstanding work before calling `cancel`.
    async fn apply_cancellation(
        &self,
        runner: &mut AgentStepRunner<A2ARunStateStore>,
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
        workflow: &AgentWorkflow,
        run_id: AgentRunId,
    ) -> Result<AgentStepRunner<A2ARunStateStore>, RakkaA2AHandlerError> {
        let mut runner = AgentStepRunner::new(workflow.clone(), run_id, self.run_store.clone());
        runner.recover().await?;
        Ok(runner)
    }

    async fn stream_task(
        &self,
        tenant: String,
        task_id: String,
        after_cursor: Option<String>,
        snapshot_first: bool,
        projection: A2ATaskProjection,
        lease: A2AStreamLease,
    ) -> Result<BoxStream<'static, Result<StreamResponse, A2AError>>, RakkaA2AHandlerError> {
        // Subscribe before reading snapshot or replay state so an event
        // appended in between still wakes the stream; the durable replay from
        // the stream's own cursor drops the overlap.
        let signal = match &self.task_event_watcher {
            Some(watcher) => watcher.watch(&tenant, &task_id).await.ok(),
            None => None,
        };
        // Re-read after subscribing: the caller's projection copy may predate
        // events appended before the subscription existed.
        let projection = match self.task_store.projection(Some(&tenant), &task_id).await {
            Ok(current) => current,
            Err(TaskProjectionError::TaskNotFound { .. }) => projection,
            Err(error) => return Err(error.into()),
        };
        let shared_replay = self.task_store.supports_shared_replay();
        let started = Instant::now();
        let mut pending = VecDeque::new();
        let mut last_sequence = projection.projection_revision;
        let mut poll_owner_now = false;

        if let Some(cursor) = after_cursor.as_deref() {
            if self.router.is_some() && !shared_replay {
                // Without shared durable replay the owner holds the
                // authoritative event log; replay through it on the first
                // poll instead of trusting this node's local log, so a valid
                // cursor resumes without duplicate events.
                match parse_replay_cursor(cursor) {
                    Ok((cursor_task_id, sequence)) if cursor_task_id == task_id => {
                        last_sequence = sequence;
                        poll_owner_now = true;
                    }
                    _ => {
                        pending.push_back(Ok(StreamResponse::Task(projection.to_task(None, true))));
                        last_sequence = projection.projection_revision;
                    }
                }
            } else {
                match self
                    .task_store
                    .replay_events(&tenant, &task_id, Some(cursor))
                    .await
                {
                    Ok(events) => {
                        if let Some(last) = events.last() {
                            last_sequence = last.sequence;
                        }
                        pending.extend(events.into_iter().map(stream_response_from_event));
                    }
                    Err(TaskProjectionError::ReplayWindowExpired { .. })
                    | Err(TaskProjectionError::InvalidReplayCursor { .. }) => {
                        // The cursor fell out of the retained window: resync
                        // from the current snapshot instead of a silent gap.
                        pending.push_back(Ok(StreamResponse::Task(projection.to_task(None, true))));
                        last_sequence = projection.projection_revision;
                    }
                    Err(error) => return Err(error.into()),
                }
            }
        } else {
            pending.push_back(Ok(StreamResponse::Task(projection.to_task(None, true))));
            last_sequence = projection.projection_revision;
        }

        if snapshot_first && pending.is_empty() && !poll_owner_now {
            pending.push_back(Ok(StreamResponse::Task(projection.to_task(None, true))));
            last_sequence = projection.projection_revision;
        }

        self.stream_limits
            .record_replay(u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX));

        let status = TaskStatus {
            state: projection.status,
            message: None,
            timestamp: None,
        };
        let state = A2AStreamState {
            task_store: Arc::clone(&self.task_store),
            signal,
            pending,
            last_sequence,
            status,
            tenant,
            task_id,
            context_id: projection.context_id,
            limits: self.stream_limits.clone(),
            router: self.router.clone(),
            shared_replay,
            run_ask_timeout: self.settings.run_ask_timeout,
            heartbeat_interval: self.settings.stream_heartbeat_interval,
            owner_poll_interval: self.settings.stream_owner_poll_interval,
            poll_owner_now,
            poll_failures: 0,
            last_emitted: Instant::now(),
            _lease: lease,
            done: false,
        };
        Ok(Box::pin(stream::unfold(state, next_stream_item)))
    }
}

fn unknown_workflow_selection(selection: &A2AWorkflowSelection) -> RakkaA2AHandlerError {
    let actual = selection
        .workflow_id
        .clone()
        .or_else(|| selection.workflow_type.clone())
        .or_else(|| selection.definition_version.clone())
        .unwrap_or_default();
    RakkaA2AHandlerError::Mapping(A2AMappingError::InvalidWorkflowSelection {
        field: META_WORKFLOW_ID,
        expected: "a hosted workflow".to_string(),
        actual,
    })
}

struct A2AStreamState {
    task_store: Arc<dyn A2ATaskProjectionStore>,
    /// Durable event wake-ups; `None` when no watcher is configured or the
    /// watcher reported itself lost.
    signal: Option<A2ATaskEventSignal>,
    pending: VecDeque<Result<StreamResponse, A2AError>>,
    last_sequence: u64,
    status: TaskStatus,
    tenant: String,
    task_id: String,
    context_id: String,
    limits: A2AStreamLimits,
    /// Present in clustered mode: the owner-poll fallback path.
    router: Option<Arc<dyn A2ARunRoute>>,
    /// True when the projection store serves one shared durable event log,
    /// making owner polling unnecessary while the watcher lives.
    shared_replay: bool,
    run_ask_timeout: Duration,
    heartbeat_interval: Duration,
    owner_poll_interval: Duration,
    /// Poll the owner before waiting again (set on open with a client cursor
    /// and after wait timeouts when owner polling is the live path).
    poll_owner_now: bool,
    /// Consecutive owner-poll failures; reset on any successful poll.
    poll_failures: usize,
    /// When the last item was returned; paces heartbeats between polls.
    last_emitted: Instant,
    _lease: A2AStreamLease,
    done: bool,
}

impl A2AStreamState {
    /// True when owner polling is the live-update path for this stream.
    fn owner_polling_active(&self) -> bool {
        self.router.is_some() && (!self.shared_replay || self.signal.is_none())
    }
}

async fn next_stream_item(
    mut state: A2AStreamState,
) -> Option<(Result<StreamResponse, A2AError>, A2AStreamState)> {
    if state.done {
        return None;
    }
    if let Some(item) = state.pending.pop_front() {
        apply_stream_response_state(&mut state, &item);
        state.last_emitted = Instant::now();
        return Some((item, state));
    }

    loop {
        if state.poll_owner_now && state.owner_polling_active() {
            state.poll_owner_now = false;
            if let Some(item) = poll_owner_events(&mut state).await {
                apply_stream_response_state(&mut state, &item);
                state.last_emitted = Instant::now();
                return Some((item, state));
            }
        }
        state.poll_owner_now = false;

        let wait = if state.owner_polling_active() {
            state.owner_poll_interval
        } else {
            state.heartbeat_interval
        };
        let outcome = match state.signal.as_mut() {
            Some(signal) => tokio::time::timeout(wait, signal.changed()).await.ok(),
            None => {
                tokio::time::sleep(wait).await;
                None
            }
        };
        match outcome {
            Some(A2ATaskEventSignalOutcome::Notified { .. }) => {
                match drain_durable_events(&mut state).await {
                    Ok(true) => {
                        let item = state.pending.pop_front().expect("drained frames queued");
                        apply_stream_response_state(&mut state, &item);
                        state.last_emitted = Instant::now();
                        return Some((item, state));
                    }
                    Ok(false) => continue,
                    Err(error) => {
                        state.done = true;
                        return Some((Err(error.into_a2a_error()), state));
                    }
                }
            }
            Some(A2ATaskEventSignalOutcome::Lost) => {
                // Fall back to owner polling (when routed) or heartbeats.
                state.signal = None;
                continue;
            }
            None => {
                if state.owner_polling_active() {
                    // Owner-local streams are served by the local watcher;
                    // polling ourselves would only repeat what the watcher
                    // already delivered. Re-checked every interval so a
                    // rebalance that moves the shard away resumes polling
                    // within one interval.
                    let poll_owner = state
                        .router
                        .as_ref()
                        .is_some_and(|router| !router.local_node_owns(&state.task_id));
                    if poll_owner {
                        state.poll_owner_now = true;
                    }
                    if state.last_emitted.elapsed() < state.heartbeat_interval {
                        continue;
                    }
                }
                state.last_emitted = Instant::now();
                return Some((Ok(heartbeat_response(&state)), state));
            }
        }
    }
}

/// Replays durable events past the stream cursor into the pending queue.
///
/// Returns `Ok(true)` when at least one frame was queued. A cursor that fell
/// out of the retained window resyncs from the current projection snapshot
/// instead of leaving a silent gap.
async fn drain_durable_events(state: &mut A2AStreamState) -> Result<bool, RakkaA2AHandlerError> {
    let cursor = encode_replay_cursor(&state.task_id, state.last_sequence);
    match state
        .task_store
        .replay_events(&state.tenant, &state.task_id, Some(&cursor))
        .await
    {
        Ok(events) => {
            let mut queued = false;
            for event in events {
                if event.sequence <= state.last_sequence {
                    continue;
                }
                state.last_sequence = event.sequence;
                state.pending.push_back(stream_response_from_event(event));
                queued = true;
            }
            Ok(queued)
        }
        Err(
            TaskProjectionError::ReplayWindowExpired { .. }
            | TaskProjectionError::InvalidReplayCursor { .. },
        ) => {
            let projection = state
                .task_store
                .projection(Some(&state.tenant), &state.task_id)
                .await?;
            state.last_sequence = projection.projection_revision;
            state
                .pending
                .push_back(Ok(StreamResponse::Task(projection.to_task(None, true))));
            Ok(true)
        }
        Err(error) => Err(error.into()),
    }
}

/// Polls the shard owner for public events after the stream's cursor.
///
/// Returns the next item to emit, queueing any extra events on
/// `state.pending`; `None` means the owner reported nothing new.
async fn poll_owner_events(state: &mut A2AStreamState) -> Option<Result<StreamResponse, A2AError>> {
    let router = state.router.clone()?;
    let request = A2ARunRequest::new(
        state.task_id.clone(),
        Some(state.tenant.clone()),
        A2ARunCommandMetadata::query(),
        A2AProjectionHints::default(),
        A2ATimeoutPolicy::from_duration(state.run_ask_timeout),
        A2ARunRequestKind::OpenStreamCursor {
            after_cursor: Some(encode_replay_cursor(&state.task_id, state.last_sequence)),
        },
    );
    let outcome = match router.route(request).await {
        Ok(response) => stream_cursor_from_response(response),
        Err(error) => Err(error),
    };
    match outcome {
        Ok((projection, events, resync)) => {
            state.poll_failures = 0;
            if resync {
                // The owner cannot resume from this cursor (owner moved or
                // the window compacted); re-bootstrap from its snapshot.
                state.last_sequence = projection.projection_revision;
                return Some(Ok(StreamResponse::Task(projection.to_task(None, true))));
            }
            let mut first = None;
            for event in events {
                if event.sequence <= state.last_sequence {
                    continue;
                }
                state.last_sequence = event.sequence;
                let item = stream_response_from_event(event);
                if first.is_none() {
                    first = Some(item);
                } else {
                    state.pending.push_back(item);
                }
            }
            first
        }
        Err(_) => {
            state.poll_failures += 1;
            if state.poll_failures < MAX_STREAM_POLL_FAILURES {
                // Transient owner churn (rebalance, passivation, one ask
                // timeout); retry on the next poll interval instead of
                // ending the stream.
                return None;
            }
            state.limits.record_dropped();
            Some(Err(A2AError::internal(
                "a2a-stream-owner-unavailable: task owner is unavailable; \
                 reconnect with the last replay cursor",
            )))
        }
    }
}

fn stream_cursor_from_response(
    response: A2ARunResponse,
) -> Result<(A2ATaskProjection, Vec<A2ATaskEvent>, bool), RakkaA2AHandlerError> {
    let (task_id, outcome) = owner_response_outcome(response)?;
    match outcome {
        A2ARunResponseKind::StreamCursor {
            projection,
            events,
            resync,
        } => Ok((projection, events, resync)),
        _ => Err(unexpected_owner_response(task_id)),
    }
}

fn apply_stream_response_state(
    state: &mut A2AStreamState,
    item: &Result<StreamResponse, A2AError>,
) {
    match item {
        Ok(StreamResponse::Task(task)) => {
            state.status = task.status.clone();
            state.done = task.status.state.is_terminal();
        }
        Ok(StreamResponse::StatusUpdate(update)) => {
            state.status = update.status.clone();
            state.done = update.status.state.is_terminal();
        }
        Ok(StreamResponse::ArtifactUpdate(_)) | Ok(StreamResponse::Message(_)) => {}
        Err(_) => state.done = true,
    }
}

fn stream_response_from_event(event: A2ATaskEvent) -> Result<StreamResponse, A2AError> {
    let mut metadata = event.metadata.clone();
    metadata.insert(
        META_TASK_EVENT_KIND.to_string(),
        serde_json::Value::String(event.kind().as_label().to_string()),
    );
    metadata.insert(
        META_REPLAY_CURSOR.to_string(),
        serde_json::Value::String(event.replay_cursor()),
    );
    metadata.insert(
        META_REDACTION.to_string(),
        serde_json::Value::String(event.redaction.as_label().to_string()),
    );
    match event.payload {
        A2ATaskEventPayload::Snapshot(projection) => {
            Ok(StreamResponse::Task(projection.to_task(None, true)))
        }
        A2ATaskEventPayload::StatusUpdate { state } | A2ATaskEventPayload::Terminal { state } => {
            Ok(StreamResponse::StatusUpdate(TaskStatusUpdateEvent {
                task_id: event.task_id,
                context_id: event.context_id,
                status: TaskStatus {
                    state,
                    message: None,
                    timestamp: None,
                },
                metadata: Some(metadata),
            }))
        }
        A2ATaskEventPayload::ArtifactUpdate { artifact } => {
            Ok(StreamResponse::ArtifactUpdate(TaskArtifactUpdateEvent {
                task_id: event.task_id,
                context_id: event.context_id,
                artifact,
                append: Some(true),
                last_chunk: Some(true),
                metadata: Some(metadata),
            }))
        }
        A2ATaskEventPayload::MessageUpdate { mut message } => {
            // Message frames carry the replay cursor too; without it a
            // reconnecting client resumes from the preceding status event
            // and re-receives every message frame.
            message
                .metadata
                .get_or_insert_with(HashMap::new)
                .extend(metadata);
            Ok(StreamResponse::Message(message))
        }
    }
}

fn heartbeat_response(state: &A2AStreamState) -> StreamResponse {
    StreamResponse::StatusUpdate(TaskStatusUpdateEvent {
        task_id: state.task_id.clone(),
        context_id: state.context_id.clone(),
        status: state.status.clone(),
        metadata: Some(HashMap::from([
            (
                META_STREAM_EVENT.to_string(),
                serde_json::Value::String("heartbeat".to_string()),
            ),
            (
                META_REPLAY_CURSOR.to_string(),
                serde_json::Value::String(encode_replay_cursor(
                    &state.task_id,
                    state.last_sequence,
                )),
            ),
        ])),
    })
}

fn replay_cursor_from_params(params: &ServiceParams) -> Option<String> {
    params
        .get(REPLAY_CURSOR_HEADER)
        .or_else(|| params.get(LAST_EVENT_ID_HEADER))
        .and_then(|values| values.last())
        .cloned()
}

fn request_push_config(
    draft: &A2ACommandDraft,
    req: &SendMessageRequest,
) -> Option<TaskPushNotificationConfig> {
    let mut config = req
        .configuration
        .as_ref()
        .and_then(|configuration| configuration.task_push_notification_config.clone())?;
    config.task_id = draft.normalized.task_id.clone();
    config.tenant = Some(draft.normalized.tenant.as_str().to_string());
    Some(config)
}

fn projection_hints(history_length: Option<i32>) -> A2AProjectionHints {
    A2AProjectionHints::new(history_length, true)
}

fn return_immediately(req: &SendMessageRequest) -> bool {
    req.configuration
        .as_ref()
        .and_then(|config| config.return_immediately)
        .unwrap_or(false)
}

/// Validates the owner protocol version and maps owner-side failures,
/// returning the successful outcome for the caller to match. Shared by
/// every owner-response decoder so version and failure handling cannot
/// drift between response kinds.
fn owner_response_outcome(
    response: A2ARunResponse,
) -> Result<(String, A2ARunResponseKind), RakkaA2AHandlerError> {
    if response.version != A2A_RUN_PROTOCOL_VERSION {
        return Err(RakkaA2AHandlerError::InvalidLifecycle {
            task_id: response.task_id,
            reason: "owner response protocol version mismatch",
        });
    }
    match response.outcome {
        A2ARunResponseKind::Failure { failure } => Err(owner_failure_error(
            &response.task_id,
            failure.kind,
            failure.message,
        )),
        outcome => Ok((response.task_id, outcome)),
    }
}

fn unexpected_owner_response(task_id: String) -> RakkaA2AHandlerError {
    RakkaA2AHandlerError::InvalidLifecycle {
        task_id,
        reason: "owner returned an unexpected response kind",
    }
}

fn projection_from_response(
    response: A2ARunResponse,
) -> Result<A2ATaskProjection, RakkaA2AHandlerError> {
    let (task_id, outcome) = owner_response_outcome(response)?;
    match outcome {
        A2ARunResponseKind::TaskSnapshot { projection } => Ok(projection),
        _ => Err(unexpected_owner_response(task_id)),
    }
}

fn push_config_from_response(
    response: A2ARunResponse,
) -> Result<TaskPushNotificationConfig, RakkaA2AHandlerError> {
    let (task_id, outcome) = owner_response_outcome(response)?;
    match outcome {
        A2ARunResponseKind::PushConfigRecorded { config } => Ok(config),
        _ => Err(unexpected_owner_response(task_id)),
    }
}

fn push_delete_from_response(response: A2ARunResponse) -> Result<(), RakkaA2AHandlerError> {
    let (task_id, outcome) = owner_response_outcome(response)?;
    match outcome {
        A2ARunResponseKind::PushConfigDeleted => Ok(()),
        _ => Err(unexpected_owner_response(task_id)),
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

/// Resolves a run transition, adopting the concurrent winner's state when the
/// optimistic write lost a revision race.
async fn adopt_on_conflict(
    runner: &mut AgentStepRunner<A2ARunStateStore>,
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
    runner: &mut AgentStepRunner<A2ARunStateStore>,
) -> Result<AgentRunState, RakkaA2AHandlerError> {
    runner.recover().await?;
    let run_id = runner.run_id().as_str().to_string();
    runner.state()?.cloned().ok_or_else(|| missing_run(&run_id))
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
        self.send_streaming_message_impl(params, req)
            .await
            .map_err(RakkaA2AHandlerError::into_a2a_error)
    }

    async fn get_task(
        &self,
        params: &ServiceParams,
        req: GetTaskRequest,
    ) -> Result<Task, A2AError> {
        self.record(params);
        let result: Result<Task, RakkaA2AHandlerError> = async {
            let tenant = self.read_scope(params, req.tenant.as_deref())?;
            self.authorize(
                A2AOperation::GetTask,
                tenant.as_deref(),
                Some(&req.id),
                None,
            )
            .await?;
            if let Some(router) = &self.router {
                // A missing tenant stays `None` end-to-end in single-tenant
                // mode: the owner resolves the run's stored tenant, matching
                // the local unscoped read path.
                let request = A2ARunRequest::new(
                    req.id.clone(),
                    tenant,
                    A2ARunCommandMetadata::query(),
                    projection_hints(req.history_length),
                    self.owner_timeout(),
                    A2ARunRequestKind::QueryTaskSnapshot,
                );
                let projection = self.route_for_projection(router, request).await?;
                return Ok(projection.to_task(req.history_length, true));
            }
            self.task_store
                .get(tenant.as_deref(), &req.id, req.history_length)
                .await
                .map_err(Into::into)
        }
        .await;
        result.map_err(RakkaA2AHandlerError::into_a2a_error)
    }

    async fn list_tasks(
        &self,
        params: &ServiceParams,
        req: ListTasksRequest,
    ) -> Result<ListTasksResponse, A2AError> {
        self.record(params);
        let result: Result<ListTasksResponse, RakkaA2AHandlerError> = async {
            let tenant = self.read_scope(params, req.tenant.as_deref())?;
            self.authorize(A2AOperation::ListTasks, tenant.as_deref(), None, None)
                .await?;
            let req = ListTasksRequest { tenant, ..req };
            self.task_store.list(&req).await.map_err(Into::into)
        }
        .await;
        result.map_err(RakkaA2AHandlerError::into_a2a_error)
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
        req: SubscribeToTaskRequest,
    ) -> Result<BoxStream<'static, Result<StreamResponse, A2AError>>, A2AError> {
        self.record(params);
        self.subscribe_to_task_impl(params, req)
            .await
            .map_err(RakkaA2AHandlerError::into_a2a_error)
    }

    async fn create_push_config(
        &self,
        params: &ServiceParams,
        req: TaskPushNotificationConfig,
    ) -> Result<TaskPushNotificationConfig, A2AError> {
        self.record(params);
        self.create_push_config_impl(params, req)
            .await
            .map_err(RakkaA2AHandlerError::into_a2a_error)
    }

    async fn get_push_config(
        &self,
        params: &ServiceParams,
        req: GetTaskPushNotificationConfigRequest,
    ) -> Result<TaskPushNotificationConfig, A2AError> {
        self.record(params);
        self.get_push_config_impl(params, req)
            .await
            .map_err(RakkaA2AHandlerError::into_a2a_error)
    }

    async fn list_push_configs(
        &self,
        params: &ServiceParams,
        req: ListTaskPushNotificationConfigsRequest,
    ) -> Result<ListTaskPushNotificationConfigsResponse, A2AError> {
        self.record(params);
        self.list_push_configs_impl(params, req)
            .await
            .map_err(RakkaA2AHandlerError::into_a2a_error)
    }

    async fn delete_push_config(
        &self,
        params: &ServiceParams,
        req: DeleteTaskPushNotificationConfigRequest,
    ) -> Result<(), A2AError> {
        self.record(params);
        self.delete_push_config_impl(params, req)
            .await
            .map_err(RakkaA2AHandlerError::into_a2a_error)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::{A2AAuthorizationDecision, A2AAuthorizationRequest, A2AAuthorizer};
    use crate::catalog::A2AStaticWorkflowCatalog;
    use crate::projection::InMemoryA2ATaskProjectionStore;
    use crate::push::{A2APushConfigState, A2APushCredentialPolicy};
    use crate::testing::{fixture_agent_card, fixture_workflow};
    use a2a::TaskState;
    use futures_util::StreamExt;
    use rakka_agent_workflow::substrate::WorkflowState;
    use rakka_agent_workflow::{AgentEffectKind, AgentWorkflowId, WorkflowDefinitionVersion};
    use rakka_persistence::InMemoryDurableStateStore;

    struct TestContext {
        service: RakkaA2AService,
        task_store: InMemoryA2ATaskProjectionStore,
        run_store: InMemoryDurableStateStore<AgentRunState>,
        workflow_store: InMemoryDurableStateStore<WorkflowState>,
        push_store: InMemoryDurableStateStore<A2APushConfigState>,
        drain_gate: A2ADrainGate,
    }

    fn builder_with(ctx_stores: &TestContext) -> RakkaA2AServiceBuilder {
        RakkaA2AServiceBuilder::new()
            .agent_card(fixture_agent_card())
            .single_workflow(fixture_workflow())
            .task_store_with_watcher(ctx_stores.task_store.clone())
            .run_store(ctx_stores.run_store.clone())
            .workflow_store(ctx_stores.workflow_store.clone())
            .push_config_store(
                A2APushConfigStore::new(ctx_stores.push_store.clone())
                    .with_credential_policy(A2APushCredentialPolicy::RedactAndRecordPresence),
            )
    }

    fn test_context() -> TestContext {
        let task_store = InMemoryA2ATaskProjectionStore::local();
        let run_store = InMemoryDurableStateStore::<AgentRunState>::new();
        let workflow_store = InMemoryDurableStateStore::<WorkflowState>::new();
        let push_store = InMemoryDurableStateStore::<A2APushConfigState>::new();
        let drain_gate = A2ADrainGate::new();
        TestContext {
            service: RakkaA2AServiceBuilder::new()
                .agent_card(fixture_agent_card())
                .single_workflow(fixture_workflow())
                .task_store_with_watcher(task_store.clone())
                .run_store(run_store.clone())
                .workflow_store(workflow_store.clone())
                .push_config_store(
                    A2APushConfigStore::new(push_store.clone())
                        .with_credential_policy(A2APushCredentialPolicy::RedactAndRecordPresence),
                )
                .drain_gate(drain_gate.clone())
                .build()
                .expect("service"),
            task_store,
            run_store,
            workflow_store,
            push_store,
            drain_gate,
        }
    }

    fn params(tenant: &str) -> ServiceParams {
        ServiceParams::from([("x-rakka-tenant".to_string(), vec![tenant.to_string()])])
    }

    fn send_request(message_id: &str, immediate: bool) -> SendMessageRequest {
        serde_json::from_value(serde_json::json!({
            "message": {
                "messageId": message_id,
                "role": "ROLE_USER",
                "parts": [{"text": "hello"}]
            },
            "configuration": { "returnImmediately": immediate },
            "tenant": "tenant-a"
        }))
        .expect("send request")
    }

    fn continuation_request(
        message_id: &str,
        task_id: &str,
        immediate: bool,
    ) -> SendMessageRequest {
        serde_json::from_value(serde_json::json!({
            "message": {
                "messageId": message_id,
                "taskId": task_id,
                "role": "ROLE_USER",
                "parts": [{"text": "again"}]
            },
            "configuration": { "returnImmediately": immediate },
            "tenant": "tenant-a"
        }))
        .expect("continuation request")
    }

    fn task_of(response: SendMessageResponse) -> Task {
        match response {
            SendMessageResponse::Task(task) => task,
            other => panic!("expected task response, got {other:?}"),
        }
    }

    fn revision_of(task: &Task) -> u64 {
        task.metadata
            .as_ref()
            .and_then(|metadata| metadata.get(crate::task::META_PROJECTION_REVISION))
            .and_then(serde_json::Value::as_u64)
            .expect("projection revision")
    }

    #[tokio::test]
    async fn send_is_durable_and_deduplicated() {
        let ctx = test_context();
        let handler = ctx.service.handler();

        let first = task_of(
            handler
                .send_message(&params("tenant-a"), send_request("dedupe-message", true))
                .await
                .expect("first send"),
        );
        assert_eq!(first.status.state, TaskState::Submitted);

        let retry = task_of(
            handler
                .send_message(&params("tenant-a"), send_request("dedupe-message", true))
                .await
                .expect("retry send"),
        );
        assert_eq!(retry.id, first.id);
        assert_eq!(revision_of(&retry), revision_of(&first));
        assert_eq!(retry.history.expect("history").len(), 1);

        let listed = handler
            .list_tasks(
                &params("tenant-a"),
                ListTasksRequest {
                    tenant: Some("tenant-a".to_string()),
                    context_id: None,
                    status: None,
                    page_size: None,
                    page_token: None,
                    history_length: None,
                    status_timestamp_after: None,
                    include_artifacts: None,
                },
            )
            .await
            .expect("list");
        assert_eq!(listed.tasks.len(), 1);
    }

    #[tokio::test]
    async fn duplicate_send_still_begins_first_transition() {
        let ctx = test_context();
        let handler = ctx.service.handler();

        let first = task_of(
            handler
                .send_message(&params("tenant-a"), send_request("retry-message", true))
                .await
                .expect("first send"),
        );
        assert_eq!(first.status.state, TaskState::Submitted);

        // A retry of the same message is a durable duplicate, but a run still
        // waiting in `Accepted` must begin its first transition anyway.
        let second = task_of(
            handler
                .send_message(&params("tenant-a"), send_request("retry-message", false))
                .await
                .expect("retry send"),
        );
        assert_eq!(second.status.state, TaskState::Working);

        let mut runner = AgentStepRunner::new(
            fixture_workflow(),
            AgentRunId::new(first.id.clone()),
            ctx.run_store.clone(),
        );
        let state = runner.recover().await.unwrap().expect("run state");
        assert_eq!(state.status, AgentRunStatus::Running);
    }

    #[tokio::test]
    async fn continuation_send_reflects_run_status_in_projection() {
        let ctx = test_context();
        let handler = ctx.service.handler();

        let first = task_of(
            handler
                .send_message(&params("tenant-a"), send_request("continue-1", true))
                .await
                .expect("first send"),
        );
        let second = task_of(
            handler
                .send_message(
                    &params("tenant-a"),
                    continuation_request("continue-2", &first.id, false),
                )
                .await
                .expect("continuation"),
        );
        assert_eq!(second.status.state, TaskState::Working);
        assert_eq!(second.history.expect("history").len(), 2);
    }

    #[tokio::test]
    async fn cancel_completes_terminal_and_repeat_cancel_is_rejected() {
        let ctx = test_context();
        let handler = ctx.service.handler();

        let task = task_of(
            handler
                .send_message(&params("tenant-a"), send_request("cancel-message", true))
                .await
                .expect("send"),
        );
        let canceled = handler
            .cancel_task(
                &params("tenant-a"),
                CancelTaskRequest {
                    id: task.id.clone(),
                    metadata: None,
                    tenant: Some("tenant-a".to_string()),
                },
            )
            .await
            .expect("cancel");
        assert_eq!(canceled.status.state, TaskState::Canceled);

        let mut runner = AgentStepRunner::new(
            fixture_workflow(),
            AgentRunId::new(task.id.clone()),
            ctx.run_store.clone(),
        );
        let state = runner.recover().await.unwrap().expect("run state");
        assert_eq!(state.status, AgentRunStatus::Cancelled);

        let retry = handler
            .cancel_task(
                &params("tenant-a"),
                CancelTaskRequest {
                    id: task.id.clone(),
                    metadata: None,
                    tenant: Some("tenant-a".to_string()),
                },
            )
            .await
            .expect_err("repeat cancel");
        assert!(
            retry.message.contains("cannot be canceled"),
            "unexpected error: {retry:?}"
        );
    }

    #[tokio::test]
    async fn cancel_from_other_tenant_is_task_not_found_and_does_not_cancel() {
        let ctx = test_context();
        let handler = ctx.service.handler();

        let task = task_of(
            handler
                .send_message(&params("tenant-a"), send_request("cross-tenant", true))
                .await
                .expect("send"),
        );
        let denied = handler
            .cancel_task(
                &params("tenant-b"),
                CancelTaskRequest {
                    id: task.id.clone(),
                    metadata: None,
                    tenant: Some("tenant-b".to_string()),
                },
            )
            .await
            .expect_err("cross tenant cancel");
        assert!(
            denied.message.contains("task not found"),
            "unexpected: {denied:?}"
        );

        let mut runner = AgentStepRunner::new(
            fixture_workflow(),
            AgentRunId::new(task.id.clone()),
            ctx.run_store.clone(),
        );
        let state = runner.recover().await.unwrap().expect("run state");
        assert_eq!(state.status, AgentRunStatus::Accepted);
    }

    #[tokio::test]
    async fn send_to_terminal_task_is_rejected() {
        let ctx = test_context();
        let handler = ctx.service.handler();

        let task = task_of(
            handler
                .send_message(&params("tenant-a"), send_request("terminal-msg", true))
                .await
                .expect("send"),
        );
        handler
            .cancel_task(
                &params("tenant-a"),
                CancelTaskRequest {
                    id: task.id.clone(),
                    metadata: None,
                    tenant: Some("tenant-a".to_string()),
                },
            )
            .await
            .expect("cancel");

        let rejected = handler
            .send_message(
                &params("tenant-a"),
                continuation_request("terminal-msg-2", &task.id, true),
            )
            .await
            .expect_err("terminal continuation");
        assert!(format!("{rejected:?}").contains("invalid-command-lifecycle"));
    }

    #[tokio::test]
    async fn drain_closes_mutating_ingress_but_keeps_reads_available() {
        let ctx = test_context();
        let handler = ctx.service.handler();

        let task = task_of(
            handler
                .send_message(&params("tenant-a"), send_request("drain-before", true))
                .await
                .expect("send before drain"),
        );

        // The injectable gate flips ingress off without touching the handler.
        ctx.drain_gate.begin_drain();
        assert!(!handler.accepts_public_commands());

        let rejected = handler
            .send_message(&params("tenant-a"), send_request("drain-after", true))
            .await
            .expect_err("send during drain");
        assert!(format!("{rejected:?}").contains("a2a-agent-draining"));

        let read = handler
            .get_task(
                &params("tenant-a"),
                GetTaskRequest {
                    id: task.id.clone(),
                    history_length: None,
                    tenant: Some("tenant-a".to_string()),
                },
            )
            .await
            .expect("read during drain");
        assert_eq!(read.id, task.id);
    }

    #[tokio::test]
    async fn reads_are_scoped_by_tenant() {
        let ctx = test_context();
        let handler = ctx.service.handler();

        let task = task_of(
            handler
                .send_message(&params("tenant-a"), send_request("scoped-read", true))
                .await
                .expect("send"),
        );

        let cross = handler
            .get_task(
                &params("tenant-b"),
                GetTaskRequest {
                    id: task.id.clone(),
                    history_length: None,
                    tenant: Some("tenant-b".to_string()),
                },
            )
            .await
            .expect_err("cross tenant read");
        assert!(
            cross.message.contains("task not found"),
            "unexpected: {cross:?}"
        );

        let same = handler
            .get_task(
                &params("tenant-a"),
                GetTaskRequest {
                    id: task.id.clone(),
                    history_length: None,
                    tenant: Some("tenant-a".to_string()),
                },
            )
            .await
            .expect("same tenant read");
        assert_eq!(same.id, task.id);
    }

    #[tokio::test]
    async fn streaming_send_persists_push_config_and_schedules_notification_effect() {
        let ctx = test_context();
        let handler = ctx.service.handler();
        let request: SendMessageRequest = serde_json::from_value(serde_json::json!({
            "message": {
                "messageId": "streaming-push-message",
                "role": "ROLE_USER",
                "parts": [{"text": "hello stream"}]
            },
            "configuration": {
                "returnImmediately": true,
                "taskPushNotificationConfig": {
                    "id": "cfg-1",
                    "url": "https://example.com/a2a-push",
                    "token": "secret-token",
                    "authentication": { "scheme": "bearer", "credentials": "secret" }
                }
            },
            "tenant": "tenant-a"
        }))
        .expect("send request");

        let mut stream = handler
            .send_streaming_message(&params("tenant-a"), request)
            .await
            .expect("stream");
        let first = stream
            .next()
            .await
            .expect("first stream event")
            .expect("stream response");
        let task = match first {
            StreamResponse::Task(task) => task,
            other => panic!("expected task stream event, got {other:?}"),
        };
        assert_eq!(task.status.state, TaskState::Submitted);

        let saved = handler
            .get_push_config(
                &params("tenant-a"),
                GetTaskPushNotificationConfigRequest {
                    task_id: task.id.clone(),
                    id: "cfg-1".to_string(),
                    tenant: Some("tenant-a".to_string()),
                },
            )
            .await
            .expect("saved push config");
        assert_eq!(saved.url, "https://example.com/a2a-push");
        assert!(saved.token.is_none());
        assert!(saved
            .authentication
            .as_ref()
            .and_then(|auth| auth.credentials.as_ref())
            .is_none());

        let mut inbox =
            AgentRunInbox::new(AgentRunId::new(task.id.clone()), ctx.workflow_store.clone());
        inbox.recover().await.expect("recover workflow");
        let due = inbox.due_effects().expect("due effects");
        assert_eq!(due.len(), 1);
        let effect = &due[0].effect;
        assert_eq!(effect.kind, AgentEffectKind::Notification);
        assert_eq!(
            effect.target.address.as_deref(),
            Some("https://example.com/a2a-push")
        );
    }

    #[tokio::test]
    async fn stream_limit_rejection_precedes_durable_acceptance() {
        let ctx = test_context();
        let service = builder_with(&ctx)
            .settings(RakkaA2ASettings {
                stream_limits: A2AStreamLimitSettings {
                    max_node_streams: 0,
                    max_task_streams: 1,
                },
                ..RakkaA2ASettings::default()
            })
            .build()
            .expect("service");
        let handler = service.handler();

        let error = match handler
            .send_streaming_message(&params("tenant-a"), send_request("over-limit", true))
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("node stream limit must reject the stream"),
        };
        assert!(format!("{error:?}").contains("a2a-stream-limit"));

        // Admission ran before the durable accept: nothing was committed.
        assert!(
            ctx.run_store
                .persistence_ids()
                .await
                .expect("run ids")
                .is_empty(),
            "over-limit stream must not durably accept the send"
        );
        assert!(
            ctx.workflow_store
                .persistence_ids()
                .await
                .expect("workflow ids")
                .is_empty(),
            "over-limit stream must not write inbox state"
        );
    }

    #[tokio::test]
    async fn message_stream_frames_carry_replay_cursor() {
        let ctx = test_context();
        let handler = ctx.service.handler();

        let task = task_of(
            handler
                .send_message(&params("tenant-a"), send_request("cursor-1", true))
                .await
                .expect("send"),
        );

        let mut stream = handler
            .subscribe_to_task(
                &params("tenant-a"),
                SubscribeToTaskRequest {
                    id: task.id.clone(),
                    tenant: Some("tenant-a".to_string()),
                },
            )
            .await
            .expect("subscribe");
        let first = stream
            .next()
            .await
            .expect("initial snapshot")
            .expect("stream response");
        assert!(matches!(first, StreamResponse::Task(_)));

        // A continuation appends a MessageUpdate event; its stream frame
        // must carry the replay cursor a client resumes from on reconnect.
        task_of(
            handler
                .send_message(
                    &params("tenant-a"),
                    continuation_request("cursor-2", &task.id, true),
                )
                .await
                .expect("continuation"),
        );

        let item = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("message frame within deadline")
            .expect("stream open")
            .expect("stream response");
        match item {
            StreamResponse::Message(message) => {
                let metadata = message.metadata.expect("message frame metadata");
                assert!(
                    metadata.contains_key(META_REPLAY_CURSOR),
                    "message frames must carry the replay cursor"
                );
            }
            other => panic!("expected message frame, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn tenant_scoped_mode_requires_tenant_and_scoped_store() {
        let ctx = test_context();
        // A tenant-scoped configuration must refuse a store that would allow
        // unscoped reads (DN-3).
        let refused = builder_with(&ctx)
            .tenant_scoped(crate::mapping::A2AHeaderTenantResolver)
            .build();
        assert!(matches!(
            refused,
            Err(RakkaA2ABuildError::UnscopedStoreInTenantScopedMode)
        ));

        let scoped_store = InMemoryA2ATaskProjectionStore::tenant_scoped();
        let service = builder_with(&ctx)
            .task_store_with_watcher(scoped_store)
            .tenant_scoped(crate::mapping::A2AHeaderTenantResolver)
            .build()
            .expect("tenant-scoped service");
        let handler = service.handler();

        // Reads without tenant input are refused instead of unscoped.
        let error = handler
            .get_task(
                &ServiceParams::new(),
                GetTaskRequest {
                    id: "task-any".to_string(),
                    history_length: None,
                    tenant: None,
                },
            )
            .await
            .expect_err("unscoped read");
        assert!(format!("{error:?}").contains("tenant-required"));

        // Commands without tenant input are refused too.
        let mut request = send_request("scoped-command", true);
        request.tenant = None;
        let error = handler
            .send_message(&ServiceParams::new(), request)
            .await
            .expect_err("tenantless command");
        assert!(format!("{error:?}").contains("tenant-required"));
    }

    #[tokio::test]
    async fn workflow_catalog_selects_and_rejects_deterministically() {
        let ctx = test_context();
        let mut second = fixture_workflow();
        second.workflow_id = AgentWorkflowId::new("workflow-second");
        second.workflow_type = "second-type".to_string();
        second.definition_version = WorkflowDefinitionVersion::new("v2");
        let service = builder_with(&ctx)
            .workflow_catalog(
                A2AStaticWorkflowCatalog::new(vec![fixture_workflow(), second]).expect("catalog"),
            )
            .build()
            .expect("service");
        let handler = service.handler();

        // Selection metadata picks the second workflow.
        let request: SendMessageRequest = serde_json::from_value(serde_json::json!({
            "message": {
                "messageId": "catalog-select",
                "role": "ROLE_USER",
                "parts": [{"text": "hello"}],
                "metadata": { "io.rakka.workflow.type": "second-type" }
            },
            "configuration": { "returnImmediately": true },
            "tenant": "tenant-a"
        }))
        .expect("request");
        let task = task_of(
            handler
                .send_message(&params("tenant-a"), request)
                .await
                .expect("selected send"),
        );
        let metadata = task.metadata.expect("metadata");
        assert_eq!(
            metadata
                .get(crate::mapping::META_WORKFLOW_ID)
                .and_then(serde_json::Value::as_str),
            Some("workflow-second")
        );

        // A selection matching nothing is rejected before durable acceptance.
        let request: SendMessageRequest = serde_json::from_value(serde_json::json!({
            "message": {
                "messageId": "catalog-miss",
                "role": "ROLE_USER",
                "parts": [{"text": "hello"}],
                "metadata": { "io.rakka.workflow.type": "unknown-type" }
            },
            "tenant": "tenant-a"
        }))
        .expect("request");
        let error = handler
            .send_message(&params("tenant-a"), request)
            .await
            .expect_err("unknown selection");
        assert!(format!("{error:?}").contains("invalid-workflow-selection"));
    }

    #[tokio::test]
    async fn recovery_restores_original_context_id() {
        let ctx = test_context();
        let handler = ctx.service.handler();
        let request: SendMessageRequest = serde_json::from_value(serde_json::json!({
            "message": {
                "messageId": "context-message",
                "contextId": "ctx-original",
                "role": "ROLE_USER",
                "parts": [{"text": "hello"}]
            },
            "configuration": { "returnImmediately": true },
            "tenant": "tenant-a"
        }))
        .expect("request");
        let task = task_of(
            handler
                .send_message(&params("tenant-a"), request)
                .await
                .expect("send"),
        );
        assert_eq!(task.context_id, "ctx-original");

        // A fresh projection store simulates a restart that lost projections
        // while the durable run and inbox stores survived.
        let fresh_store = InMemoryA2ATaskProjectionStore::local();
        let recovery = builder_with(&ctx)
            .task_store_with_watcher(fresh_store.clone())
            .build()
            .expect("recovery service");
        let recovered = recovery
            .recover_task_projections()
            .await
            .expect("recover projections");
        assert_eq!(recovered, 1);
        let restored = fresh_store
            .get(Some("tenant-a"), &task.id, None)
            .await
            .expect("recovered task");
        assert_eq!(restored.context_id, "ctx-original");
    }

    #[tokio::test]
    async fn authorizer_denial_is_indistinguishable_from_missing_task() {
        struct DenyReads;
        #[async_trait]
        impl A2AAuthorizer for DenyReads {
            async fn authorize(
                &self,
                request: &A2AAuthorizationRequest<'_>,
            ) -> A2AAuthorizationDecision {
                if matches!(request.operation, A2AOperation::GetTask) {
                    A2AAuthorizationDecision::Deny
                } else {
                    A2AAuthorizationDecision::Allow
                }
            }
        }

        let ctx = test_context();
        let service = builder_with(&ctx)
            .authorizer(DenyReads)
            .build()
            .expect("service");
        let handler = service.handler();

        let task = task_of(
            handler
                .send_message(&params("tenant-a"), send_request("authz-message", true))
                .await
                .expect("send"),
        );
        let denied = handler
            .get_task(
                &params("tenant-a"),
                GetTaskRequest {
                    id: task.id.clone(),
                    history_length: None,
                    tenant: Some("tenant-a".to_string()),
                },
            )
            .await
            .expect_err("denied read");
        assert!(
            denied.message.contains("task not found"),
            "unexpected: {denied:?}"
        );
    }

    #[tokio::test]
    async fn builder_requires_every_durable_component() {
        let error = RakkaA2AServiceBuilder::new()
            .build()
            .expect_err("empty builder");
        assert!(matches!(error, RakkaA2ABuildError::Missing { .. }));
    }

    // --- Slice 7.6: streaming from durable events (DN-2) ---

    fn cursor_of(item: &StreamResponse) -> Option<String> {
        let metadata = match item {
            StreamResponse::StatusUpdate(update) => update.metadata.as_ref(),
            StreamResponse::Message(message) => message.metadata.as_ref(),
            StreamResponse::ArtifactUpdate(update) => update.metadata.as_ref(),
            StreamResponse::Task(_) => None,
        }?;
        metadata
            .get(META_REPLAY_CURSOR)
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
    }

    async fn append_message_event(
        store: &InMemoryA2ATaskProjectionStore,
        task_id: &str,
        message_id: &str,
        occurred: u64,
    ) -> A2ATaskEvent {
        let mut message = a2a::Message::new(a2a::Role::User, vec![a2a::Part::text("more")]);
        message.message_id = message_id.to_string();
        store
            .append_event_payload(
                "tenant-a",
                task_id,
                "ctx",
                AgentTimestampMillis::new(occurred),
                A2ATaskEventPayload::MessageUpdate { message },
            )
            .await
            .expect("append message event")
    }

    #[tokio::test]
    async fn reconnect_with_cursor_resumes_without_gap_or_duplicate() {
        let ctx = test_context();
        let handler = ctx.service.handler();

        let task = task_of(
            handler
                .send_message(&params("tenant-a"), send_request("stream-seed", true))
                .await
                .expect("send"),
        );
        // Durable events land at sequences 2, 3, 4 (snapshot is 1).
        for (index, occurred) in [(2u64, 20u64), (3, 21), (4, 22)] {
            append_message_event(&ctx.task_store, &task.id, &format!("m-{index}"), occurred).await;
        }

        // Subscribe replaying from sequence 1: the durable path yields events
        // 2, 3, 4 contiguously with no gap and no duplicate.
        let cursor_params = ServiceParams::from([
            ("x-rakka-tenant".to_string(), vec!["tenant-a".to_string()]),
            (
                REPLAY_CURSOR_HEADER.to_string(),
                vec![format!("{}:1", task.id)],
            ),
        ]);
        let mut stream = handler
            .subscribe_to_task(
                &cursor_params,
                SubscribeToTaskRequest {
                    id: task.id.clone(),
                    tenant: Some("tenant-a".to_string()),
                },
            )
            .await
            .expect("subscribe with cursor");

        let mut seen = Vec::new();
        for _ in 0..3 {
            let item = tokio::time::timeout(Duration::from_secs(5), stream.next())
                .await
                .expect("frame within deadline")
                .expect("stream open")
                .expect("stream response");
            if let Some(cursor) = cursor_of(&item) {
                seen.push(cursor);
            }
        }
        assert_eq!(
            seen,
            vec![
                format!("{}:2", task.id),
                format!("{}:3", task.id),
                format!("{}:4", task.id),
            ],
            "replay must be contiguous with no gap or duplicate"
        );

        // A new event past the last-served cursor wakes the live stream.
        append_message_event(&ctx.task_store, &task.id, "m-5", 23).await;
        let live = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("live frame within deadline")
            .expect("stream open")
            .expect("stream response");
        assert_eq!(
            cursor_of(&live).as_deref(),
            Some(format!("{}:5", task.id).as_str())
        );
    }

    #[tokio::test]
    async fn cursor_older_than_retained_window_yields_resync() {
        let ctx = test_context();
        // A tiny retention window so early events fall out of the log.
        let compacting = InMemoryA2ATaskProjectionStore::local()
            .with_retention(crate::projection::A2ATaskEventRetention::new(4));
        let service = builder_with(&ctx)
            .task_store_with_watcher(compacting.clone())
            .build()
            .expect("service");
        let handler = service.handler();

        let task = task_of(
            handler
                .send_message(&params("tenant-a"), send_request("compaction-seed", true))
                .await
                .expect("send"),
        );
        for index in 0..10u64 {
            append_message_event(&compacting, &task.id, &format!("m-{index}"), 20 + index).await;
        }

        // A cursor before the retained window must resync from the current
        // snapshot instead of silently skipping the dropped events.
        let cursor_params = ServiceParams::from([
            ("x-rakka-tenant".to_string(), vec!["tenant-a".to_string()]),
            (
                REPLAY_CURSOR_HEADER.to_string(),
                vec![format!("{}:2", task.id)],
            ),
        ]);
        let mut stream = handler
            .subscribe_to_task(
                &cursor_params,
                SubscribeToTaskRequest {
                    id: task.id.clone(),
                    tenant: Some("tenant-a".to_string()),
                },
            )
            .await
            .expect("subscribe with stale cursor");
        let first = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("first frame within deadline")
            .expect("stream open")
            .expect("stream response");
        assert!(
            matches!(first, StreamResponse::Task(_)),
            "a cursor older than the retained window must resync from the snapshot"
        );
    }

    #[tokio::test]
    async fn disconnect_does_not_cancel_run() {
        let ctx = test_context();
        let handler = ctx.service.handler();

        let task = task_of(
            handler
                .send_message(&params("tenant-a"), send_request("disconnect-msg", true))
                .await
                .expect("send"),
        );
        let mut stream = handler
            .subscribe_to_task(
                &params("tenant-a"),
                SubscribeToTaskRequest {
                    id: task.id.clone(),
                    tenant: Some("tenant-a".to_string()),
                },
            )
            .await
            .expect("subscribe");
        let _ = stream.next().await.expect("snapshot").expect("response");
        // Dropping the stream must not cancel or otherwise disturb the run.
        drop(stream);

        let mut runner = AgentStepRunner::new(
            fixture_workflow(),
            AgentRunId::new(task.id.clone()),
            ctx.run_store.clone(),
        );
        let state = runner.recover().await.unwrap().expect("run state");
        assert_eq!(state.status, AgentRunStatus::Accepted);
    }
}
