//! Opt-in axum route composition for the A2A surface.
//!
//! Composes the agent-card, REST, and JSON-RPC routers from the A2A SDK over
//! a [`RakkaA2AService`]'s request handler. Applications own their outer
//! router (health, readiness, drain, discovery) and merge these in.

use std::sync::Arc;

use a2a::AgentCard;
use a2a_server::agent_card::agent_card_router;
use a2a_server::jsonrpc::jsonrpc_router;
use a2a_server::rest::rest_router;
use a2a_server::StaticAgentCard;
use axum::Router;

use crate::handler::{RakkaA2ARequestHandler, RakkaA2AService};

/// Nest paths for the composed A2A routers.
#[derive(Debug, Clone)]
pub struct A2ARoutePaths {
    /// Nest path for the REST transport.
    pub rest: String,
    /// Nest path for the JSON-RPC transport.
    pub jsonrpc: String,
}

impl Default for A2ARoutePaths {
    fn default() -> Self {
        Self {
            rest: "/a2a".to_string(),
            jsonrpc: "/a2a/jsonrpc".to_string(),
        }
    }
}

/// Builds the agent-card router serving the well-known card endpoint.
pub fn agent_card_routes(card: AgentCard) -> Router {
    agent_card_router(Arc::new(StaticAgentCard::new(card)))
}

/// Builds the REST + JSON-RPC routers over a handler at the default paths.
pub fn a2a_transport_routes(handler: Arc<RakkaA2ARequestHandler>) -> Router {
    a2a_transport_routes_at(handler, &A2ARoutePaths::default())
}

/// Builds the REST + JSON-RPC routers over a handler at custom paths.
pub fn a2a_transport_routes_at(
    handler: Arc<RakkaA2ARequestHandler>,
    paths: &A2ARoutePaths,
) -> Router {
    Router::new()
        .nest(&paths.rest, rest_router(handler.clone()))
        .nest(&paths.jsonrpc, jsonrpc_router(handler))
}

/// Composes the full A2A router (agent card + REST + JSON-RPC) for a service.
///
/// The agent card served is the service's own card; transport routers are
/// mounted at the default `/a2a` and `/a2a/jsonrpc` paths.
pub fn a2a_routes(service: &RakkaA2AService) -> Router {
    a2a_routes_at(service, &A2ARoutePaths::default())
}

/// Composes the full A2A router at custom transport paths.
pub fn a2a_routes_at(service: &RakkaA2AService, paths: &A2ARoutePaths) -> Router {
    agent_card_routes(service.agent_card().clone())
        .merge(a2a_transport_routes_at(service.handler(), paths))
}

#[cfg(all(test, feature = "testkit"))]
mod tests {
    use super::*;
    use crate::handler::RakkaA2AServiceBuilder;
    use crate::projection::InMemoryA2ATaskProjectionStore;
    use crate::push::{A2APushConfigState, A2APushConfigStore, A2APushCredentialPolicy};
    use crate::testing::{fixture_agent_card, fixture_workflow};
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use rakka_agent_workflow::substrate::WorkflowState;
    use rakka_agent_workflow::AgentRunState;
    use rakka_persistence::InMemoryDurableStateStore;
    use tower::ServiceExt;

    fn service() -> RakkaA2AService {
        let task_store = InMemoryA2ATaskProjectionStore::local();
        RakkaA2AServiceBuilder::new()
            .agent_card(fixture_agent_card())
            .single_workflow(fixture_workflow())
            .task_store_with_watcher(task_store)
            .run_store(InMemoryDurableStateStore::<AgentRunState>::new())
            .workflow_store(InMemoryDurableStateStore::<WorkflowState>::new())
            .push_config_store(
                A2APushConfigStore::new(InMemoryDurableStateStore::<A2APushConfigState>::new())
                    .with_credential_policy(A2APushCredentialPolicy::RedactAndRecordPresence),
            )
            .build()
            .expect("service")
    }

    #[tokio::test]
    async fn composed_router_serves_card_and_rest_send() {
        let app = a2a_routes(&service());

        let card = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/.well-known/agent-card.json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(card.status(), StatusCode::OK);

        let body = serde_json::json!({
            "message": {
                "messageId": "routes-send",
                "role": "ROLE_USER",
                "parts": [{"text": "hello"}]
            },
            "configuration": { "returnImmediately": true },
            "tenant": "tenant-a"
        });
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/a2a/message:send")
                    .header("content-type", "application/json")
                    .header("x-rakka-tenant", "tenant-a")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let payload: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(payload["task"]["status"]["state"], "TASK_STATE_SUBMITTED");
    }
}
