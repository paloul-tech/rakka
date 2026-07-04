//! Phase 1 A2A request handler.
//!
//! Public command paths validate and normalize into Rakka command drafts, but
//! still return an unsupported-operation error until Phase 2 adds durable inbox
//! acceptance. Read paths are backed by the Phase 1 task projection store.

use std::sync::{Arc, Mutex};

use a2a::{
    A2AError, AgentCard, CancelTaskRequest, DeleteTaskPushNotificationConfigRequest,
    GetExtendedAgentCardRequest, GetTaskPushNotificationConfigRequest, GetTaskRequest,
    ListTaskPushNotificationConfigsRequest, ListTaskPushNotificationConfigsResponse,
    ListTasksRequest, ListTasksResponse, SendMessageRequest, SendMessageResponse, StreamResponse,
    SubscribeToTaskRequest, Task, TaskPushNotificationConfig,
};
use a2a_server::{RequestHandler, ServiceParams};
use async_trait::async_trait;
use futures_util::stream::{self, BoxStream};
use rakka::agent_workflow::AgentWorkflow;

use crate::a2a_mapping::{
    build_cancel_task_command_draft, build_send_message_command_draft, canonical_read_tenant,
    now_agent_timestamp, A2AMappingError, A2APayloadPolicy,
};
use crate::task_projection::{empty_list, InMemoryA2ATaskProjectionStore, TaskProjectionError};

const PHASE1_UNIMPLEMENTED: &str =
    "A2A durable request handling is intentionally not implemented until Phase 2";

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

/// Phase 1 A2A handler implementation.
pub struct Phase1A2AHandler {
    agent_card: AgentCard,
    workflow: AgentWorkflow,
    task_store: InMemoryA2ATaskProjectionStore,
    header_observer: HeaderObserver,
}

impl Phase1A2AHandler {
    /// Creates a handler with a shared agent card and header observer.
    #[must_use]
    pub fn new(
        agent_card: AgentCard,
        workflow: AgentWorkflow,
        task_store: InMemoryA2ATaskProjectionStore,
        header_observer: HeaderObserver,
    ) -> Self {
        Self {
            agent_card,
            workflow,
            task_store,
            header_observer,
        }
    }

    fn record(&self, params: &ServiceParams) {
        self.header_observer.record(params);
    }

    fn unsupported() -> A2AError {
        A2AError::unsupported_operation(PHASE1_UNIMPLEMENTED)
    }
}

#[async_trait]
impl RequestHandler for Phase1A2AHandler {
    async fn send_message(
        &self,
        params: &ServiceParams,
        req: SendMessageRequest,
    ) -> Result<SendMessageResponse, A2AError> {
        self.record(params);
        build_send_message_command_draft(
            params,
            &req,
            &self.workflow,
            A2APayloadPolicy::default(),
            now_agent_timestamp(),
        )
        .map_err(mapping_error)?;
        Err(Self::unsupported())
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
            A2APayloadPolicy::default(),
            now_agent_timestamp(),
        )
        .map_err(mapping_error)?;
        Err(Self::unsupported())
    }

    async fn get_task(
        &self,
        params: &ServiceParams,
        req: GetTaskRequest,
    ) -> Result<Task, A2AError> {
        self.record(params);
        let tenant = canonical_read_tenant(params, req.tenant.as_deref()).map_err(mapping_error)?;
        self.task_store
            .get(tenant.as_deref(), &req.id, req.history_length)
            .map_err(projection_error)
    }

    async fn list_tasks(
        &self,
        params: &ServiceParams,
        req: ListTasksRequest,
    ) -> Result<ListTasksResponse, A2AError> {
        self.record(params);
        let tenant = canonical_read_tenant(params, req.tenant.as_deref()).map_err(mapping_error)?;
        let req = ListTasksRequest { tenant, ..req };
        match self.task_store.list(&req) {
            Ok(response) => Ok(response),
            Err(TaskProjectionError::TaskNotFound { .. }) => Ok(empty_list(req.page_size)),
            Err(error) => Err(projection_error(error)),
        }
    }

    async fn cancel_task(
        &self,
        params: &ServiceParams,
        req: CancelTaskRequest,
    ) -> Result<Task, A2AError> {
        self.record(params);
        build_cancel_task_command_draft(params, &req, &self.workflow, now_agent_timestamp())
            .map_err(mapping_error)?;
        Err(Self::unsupported())
    }

    async fn subscribe_to_task(
        &self,
        params: &ServiceParams,
        _req: SubscribeToTaskRequest,
    ) -> Result<BoxStream<'static, Result<StreamResponse, A2AError>>, A2AError> {
        self.record(params);
        Ok(Box::pin(stream::once(async { Err(Self::unsupported()) })))
    }

    async fn create_push_config(
        &self,
        params: &ServiceParams,
        _req: TaskPushNotificationConfig,
    ) -> Result<TaskPushNotificationConfig, A2AError> {
        self.record(params);
        Err(Self::unsupported())
    }

    async fn get_push_config(
        &self,
        params: &ServiceParams,
        _req: GetTaskPushNotificationConfigRequest,
    ) -> Result<TaskPushNotificationConfig, A2AError> {
        self.record(params);
        Err(Self::unsupported())
    }

    async fn list_push_configs(
        &self,
        params: &ServiceParams,
        _req: ListTaskPushNotificationConfigsRequest,
    ) -> Result<ListTaskPushNotificationConfigsResponse, A2AError> {
        self.record(params);
        Err(Self::unsupported())
    }

    async fn delete_push_config(
        &self,
        params: &ServiceParams,
        _req: DeleteTaskPushNotificationConfigRequest,
    ) -> Result<(), A2AError> {
        self.record(params);
        Err(Self::unsupported())
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

fn mapping_error(error: A2AMappingError) -> A2AError {
    A2AError::invalid_params(format!("{}: {error}", error.code()))
}

fn projection_error(error: TaskProjectionError) -> A2AError {
    match error {
        TaskProjectionError::TaskNotFound { task_id } => A2AError::task_not_found(&task_id),
        error => A2AError::invalid_params(format!("{}: {error}", error.code())),
    }
}
