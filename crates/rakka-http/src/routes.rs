//! Unary HTTP route adapters for services, actors, and entities.

use std::error::Error;
use std::future::Future;

use axum::body::{to_bytes, Body, Bytes};
use axum::http::header::{HeaderValue, CONTENT_TYPE};
use axum::http::{Request, Response, StatusCode};
use axum::response::IntoResponse;
use axum::routing::post;
use axum::Router;
use rakka_core::{ActorRef, Message, ReplyTo};
use rakka_sharding::{EntityRef, ShardRegion};
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::{HttpError, HttpResult, HttpRouteConfig};

/// Type alias for Axum routers returned by Rakka HTTP adapters.
pub type HttpRouter = Router;

/// JSON response emitted by tell adapters after accepting a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct HttpAccepted {
    /// Whether the message was accepted for delivery.
    pub accepted: bool,
}

/// Creates a POST route for a JSON unary service handler.
pub fn json_service_route<Req, Resp, F, Fut>(
    path: &'static str,
    config: HttpRouteConfig,
    handler: F,
) -> HttpRouter
where
    Req: DeserializeOwned + Send + 'static,
    Resp: Serialize + Send + 'static,
    F: Fn(Req) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = HttpResult<Resp>> + Send + 'static,
{
    Router::new().route(
        path,
        post(move |request: Request<Body>| {
            let handler = handler.clone();
            async move {
                let payload = read_json(request, config).await?;
                let response = run_with_timeout(handler(payload), config).await?;
                json_response(&response)
            }
        }),
    )
}

/// Creates a POST route for a binary unary service handler.
pub fn binary_service_route<F, Fut>(
    path: &'static str,
    config: HttpRouteConfig,
    handler: F,
) -> HttpRouter
where
    F: Fn(Bytes) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = HttpResult<Bytes>> + Send + 'static,
{
    Router::new().route(
        path,
        post(move |request: Request<Body>| {
            let handler = handler.clone();
            async move {
                let payload = read_body(request, config).await?;
                let response = run_with_timeout(handler(payload), config).await?;
                Ok::<_, HttpError>(binary_response(response))
            }
        }),
    )
}

/// Creates a POST route that decodes JSON and sends an actor `ask`.
pub fn json_actor_ask_route<Req, M, Resp, B>(
    path: &'static str,
    config: HttpRouteConfig,
    actor: ActorRef<M>,
    build: B,
) -> HttpRouter
where
    Req: DeserializeOwned + Send + 'static,
    M: Message,
    Resp: Serialize + Send + 'static,
    B: Fn(Req, ReplyTo<Resp>) -> M + Clone + Send + Sync + 'static,
{
    Router::new().route(
        path,
        post(move |request: Request<Body>| {
            let actor = actor.clone();
            let build = build.clone();
            async move {
                let payload = read_json(request, config).await?;
                let timeout = config.request_timeout_value();
                let response = actor
                    .ask(|reply_to| build(payload, reply_to), timeout)
                    .await
                    .map_err(HttpError::from_actor_ask)?;
                json_response(&response)
            }
        }),
    )
}

/// Creates a POST route that decodes JSON and sends an actor `tell`.
pub fn json_actor_tell_route<Req, M, B>(
    path: &'static str,
    config: HttpRouteConfig,
    actor: ActorRef<M>,
    build: B,
) -> HttpRouter
where
    Req: DeserializeOwned + Send + 'static,
    M: Message,
    B: Fn(Req) -> M + Clone + Send + Sync + 'static,
{
    Router::new().route(
        path,
        post(move |request: Request<Body>| {
            let actor = actor.clone();
            let build = build.clone();
            async move {
                let payload = read_json(request, config).await?;
                actor
                    .tell(build(payload))
                    .map_err(HttpError::from_actor_tell)?;
                accepted_response()
            }
        }),
    )
}

/// Creates a POST route that decodes JSON and sends an entity `ask`.
pub fn json_entity_ask_route<Req, M, Resp, B>(
    path: &'static str,
    config: HttpRouteConfig,
    region: ShardRegion<M>,
    entity: EntityRef<M>,
    build: B,
) -> HttpRouter
where
    Req: DeserializeOwned + Send + 'static,
    M: Message,
    Resp: Serialize + Send + 'static,
    B: Fn(Req, ReplyTo<Resp>) -> M + Clone + Send + Sync + 'static,
{
    Router::new().route(
        path,
        post(move |request: Request<Body>| {
            let region = region.clone();
            let entity = entity.clone();
            let build = build.clone();
            async move {
                let payload = read_json(request, config).await?;
                let timeout = config.request_timeout_value();
                let response = region
                    .ask(&entity, |reply_to| build(payload, reply_to), timeout)
                    .await
                    .map_err(HttpError::from_entity_ask)?;
                json_response(&response)
            }
        }),
    )
}

/// Creates a POST route that decodes JSON and sends an entity `tell`.
pub fn json_entity_tell_route<Req, M, B>(
    path: &'static str,
    config: HttpRouteConfig,
    region: ShardRegion<M>,
    entity: EntityRef<M>,
    build: B,
) -> HttpRouter
where
    Req: DeserializeOwned + Send + 'static,
    M: Message,
    B: Fn(Req) -> M + Clone + Send + Sync + 'static,
{
    Router::new().route(
        path,
        post(move |request: Request<Body>| {
            let region = region.clone();
            let entity = entity.clone();
            let build = build.clone();
            async move {
                let payload = read_json(request, config).await?;
                region
                    .tell(&entity, build(payload))
                    .map_err(HttpError::from_entity_tell)?;
                accepted_response()
            }
        }),
    )
}

async fn run_with_timeout<T>(
    future: impl Future<Output = HttpResult<T>> + Send,
    config: HttpRouteConfig,
) -> HttpResult<T> {
    tokio::time::timeout(config.request_timeout_value(), future)
        .await
        .map_err(|_elapsed| HttpError::ServiceTimeout {
            timeout: config.request_timeout_value(),
        })?
}

async fn read_json<T>(request: Request<Body>, config: HttpRouteConfig) -> HttpResult<T>
where
    T: DeserializeOwned,
{
    let bytes = read_body(request, config).await?;
    serde_json::from_slice(&bytes).map_err(|error| HttpError::JsonDecode {
        message: error.to_string(),
    })
}

async fn read_body(request: Request<Body>, config: HttpRouteConfig) -> HttpResult<Bytes> {
    let limit = config.max_payload_bytes_value();
    to_bytes(request.into_body(), limit).await.map_err(|error| {
        if Error::source(&error)
            .is_some_and(|source| source.is::<http_body_util::LengthLimitError>())
        {
            HttpError::PayloadTooLarge { limit }
        } else {
            HttpError::BodyRead {
                message: error.to_string(),
            }
        }
    })
}

fn json_response<T>(value: &T) -> HttpResult<Response<Body>>
where
    T: Serialize,
{
    let bytes = serde_json::to_vec(value).map_err(|error| HttpError::JsonEncode {
        message: error.to_string(),
    })?;
    let mut response = Response::new(Body::from(bytes));
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/json; charset=utf-8"),
    );
    Ok(response)
}

fn binary_response(bytes: Bytes) -> Response<Body> {
    let mut response = Response::new(Body::from(bytes));
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    response
}

fn accepted_response() -> HttpResult<Response<Body>> {
    let mut response = json_response(&HttpAccepted { accepted: true })?;
    *response.status_mut() = StatusCode::ACCEPTED;
    Ok(response)
}

/// Converts any `IntoResponse` value into an HTTP response.
#[must_use]
pub fn into_response(response: impl IntoResponse) -> Response<Body> {
    response.into_response()
}
