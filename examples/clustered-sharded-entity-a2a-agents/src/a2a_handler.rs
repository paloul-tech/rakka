//! Placeholder A2A request handler.
//!
//! The handler is intentionally protocol-shaped but non-mutating: every public
//! A2A command returns a stable unsupported-operation error until later phases
//! add durable inbox acceptance, task projection, streaming, and push handling.

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

use crate::task_projection::Phase0TaskProjection;

const PHASE0_UNIMPLEMENTED: &str =
    "A2A durable request handling is intentionally not implemented in Phase 0";

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

/// Phase 0 A2A handler implementation.
pub struct Phase0A2AHandler {
    agent_card: AgentCard,
    header_observer: HeaderObserver,
}

impl Phase0A2AHandler {
    /// Creates a handler with a shared agent card and header observer.
    #[must_use]
    pub fn new(agent_card: AgentCard, header_observer: HeaderObserver) -> Self {
        Self {
            agent_card,
            header_observer,
        }
    }

    fn record(&self, params: &ServiceParams) {
        self.header_observer.record(params);
    }

    fn unsupported() -> A2AError {
        A2AError::unsupported_operation(PHASE0_UNIMPLEMENTED)
    }
}

#[async_trait]
impl RequestHandler for Phase0A2AHandler {
    async fn send_message(
        &self,
        params: &ServiceParams,
        _req: SendMessageRequest,
    ) -> Result<SendMessageResponse, A2AError> {
        self.record(params);
        Err(Self::unsupported())
    }

    async fn send_streaming_message(
        &self,
        params: &ServiceParams,
        _req: SendMessageRequest,
    ) -> Result<BoxStream<'static, Result<StreamResponse, A2AError>>, A2AError> {
        self.record(params);
        Err(Self::unsupported())
    }

    async fn get_task(
        &self,
        params: &ServiceParams,
        _req: GetTaskRequest,
    ) -> Result<Task, A2AError> {
        self.record(params);
        Err(Self::unsupported())
    }

    async fn list_tasks(
        &self,
        params: &ServiceParams,
        req: ListTasksRequest,
    ) -> Result<ListTasksResponse, A2AError> {
        self.record(params);
        Ok(Phase0TaskProjection::empty_list(req.page_size))
    }

    async fn cancel_task(
        &self,
        params: &ServiceParams,
        _req: CancelTaskRequest,
    ) -> Result<Task, A2AError> {
        self.record(params);
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
