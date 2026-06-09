//! Unary gRPC adapters for services, actors, and entities.
#![allow(clippy::result_large_err)]

use std::future::Future;
use std::time::Duration;

use rakka_core::{ActorRef, Message, ReplyTo};
use rakka_sharding::{EntityRef, ShardRegion};
use tonic::metadata::MetadataMap;
use tonic::{Request, Response};

use crate::{GrpcError, GrpcResult, GrpcUnaryConfig};

/// Trait implemented by unary service handlers accepted by Rakka gRPC adapters.
pub trait GrpcUnaryService<Req, Resp>: Send {
    /// Future returned by the handler.
    type Future: Future<Output = GrpcResult<Resp>> + Send;

    /// Calls the unary handler with a decoded protobuf request.
    fn call(self, request: Req) -> Self::Future;
}

impl<Req, Resp, F, Fut> GrpcUnaryService<Req, Resp> for F
where
    F: FnOnce(Req) -> Fut + Send,
    Fut: Future<Output = GrpcResult<Resp>> + Send,
{
    type Future = Fut;

    fn call(self, request: Req) -> Self::Future {
        self(request)
    }
}

/// Calls a unary service handler from a tonic generated service method.
pub async fn unary_service<Req, Resp, S>(
    request: Request<Req>,
    config: GrpcUnaryConfig,
    service: S,
) -> GrpcResult<Response<Resp>>
where
    S: GrpcUnaryService<Req, Resp>,
{
    let timeout = effective_request_timeout(&request, config);
    let payload = request.into_inner();
    let response = run_with_timeout(service.call(payload), timeout).await?;
    Ok(Response::new(response))
}

/// Sends an actor ask from a tonic generated unary service method.
///
/// If the RPC future is dropped by tonic because the client cancels the call,
/// the adapter drops the pending reply receiver instead of spawning detached
/// wait work.
pub async fn unary_actor_ask<Req, M, Resp, B>(
    request: Request<Req>,
    config: GrpcUnaryConfig,
    actor: &ActorRef<M>,
    build: B,
) -> GrpcResult<Response<Resp>>
where
    M: Message,
    Resp: Send + 'static,
    B: FnOnce(Req, ReplyTo<Resp>) -> M + Send,
{
    let timeout = effective_request_timeout(&request, config);
    let payload = request.into_inner();
    let response = actor
        .ask(|reply_to| build(payload, reply_to), timeout)
        .await
        .map_err(|error| GrpcError::from_actor_ask(error).into_status())?;
    Ok(Response::new(response))
}

/// Sends an actor tell from a tonic generated unary service method.
pub fn unary_actor_tell<Req, M, Resp, B, A>(
    request: Request<Req>,
    actor: &ActorRef<M>,
    build: B,
    accepted: A,
) -> GrpcResult<Response<Resp>>
where
    M: Message,
    B: FnOnce(Req) -> M,
    A: FnOnce() -> Resp,
{
    actor
        .tell(build(request.into_inner()))
        .map_err(|error| GrpcError::from_actor_tell(error).into_status())?;
    Ok(Response::new(accepted()))
}

/// Sends an entity ask through a shard region from a tonic generated unary service method.
///
/// If the RPC future is dropped by tonic because the client cancels the call,
/// the adapter drops the pending reply receiver instead of spawning detached
/// wait work.
pub async fn unary_entity_ask<Req, M, Resp, B>(
    request: Request<Req>,
    config: GrpcUnaryConfig,
    region: &ShardRegion<M>,
    entity: &EntityRef<M>,
    build: B,
) -> GrpcResult<Response<Resp>>
where
    M: Message,
    Resp: Send + 'static,
    B: FnOnce(Req, ReplyTo<Resp>) -> M + Send,
{
    let timeout = effective_request_timeout(&request, config);
    let payload = request.into_inner();
    let response = region
        .ask(entity, |reply_to| build(payload, reply_to), timeout)
        .await
        .map_err(|error| GrpcError::from_entity_ask(error).into_status())?;
    Ok(Response::new(response))
}

/// Sends an entity tell through a shard region from a tonic generated unary service method.
pub fn unary_entity_tell<Req, M, Resp, B, A>(
    request: Request<Req>,
    region: &ShardRegion<M>,
    entity: &EntityRef<M>,
    build: B,
    accepted: A,
) -> GrpcResult<Response<Resp>>
where
    M: Message,
    B: FnOnce(Req) -> M,
    A: FnOnce() -> Resp,
{
    region
        .tell(entity, build(request.into_inner()))
        .map_err(|error| GrpcError::from_entity_tell(error).into_status())?;
    Ok(Response::new(accepted()))
}

/// Computes the timeout for a unary request from config and `grpc-timeout` metadata.
#[must_use]
pub fn effective_request_timeout<T>(request: &Request<T>, config: GrpcUnaryConfig) -> Duration {
    request_timeout_from_metadata(request.metadata())
        .map(|deadline| deadline.min(config.request_timeout_value()))
        .unwrap_or_else(|| config.request_timeout_value())
}

/// Parses tonic `grpc-timeout` metadata into a duration.
#[must_use]
pub fn request_timeout_from_metadata(metadata: &MetadataMap) -> Option<Duration> {
    metadata
        .get("grpc-timeout")
        .and_then(|value| value.to_str().ok())
        .and_then(parse_grpc_timeout)
}

async fn run_with_timeout<T>(
    future: impl Future<Output = GrpcResult<T>> + Send,
    timeout: Duration,
) -> GrpcResult<T> {
    tokio::time::timeout(timeout, future)
        .await
        .map_err(|_elapsed| GrpcError::ServiceTimeout { timeout }.into_status())?
}

fn parse_grpc_timeout(value: &str) -> Option<Duration> {
    let (digits, unit) = value.split_at(value.len().checked_sub(1)?);
    if digits.is_empty() || digits.len() > 8 || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }

    let amount = digits.parse::<u64>().ok()?;
    match unit {
        "H" => amount
            .checked_mul(60)?
            .checked_mul(60)
            .map(Duration::from_secs),
        "M" => amount.checked_mul(60).map(Duration::from_secs),
        "S" => Some(Duration::from_secs(amount)),
        "m" => Some(Duration::from_millis(amount)),
        "u" => Some(Duration::from_micros(amount)),
        "n" => Some(Duration::from_nanos(amount)),
        _ => None,
    }
}
