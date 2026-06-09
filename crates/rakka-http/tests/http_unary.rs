//! Unary HTTP adapter behavior tests.

use std::time::Duration;

use axum::body::{to_bytes, Body, Bytes};
use axum::http::header::CONTENT_TYPE;
use axum::http::{Request, StatusCode};
use rakka_cluster::{
    ClusterMembership, ClusterNode, DiscoverySnapshot, MembershipConfig, NodeAddress, NodeId,
};
use rakka_core::{actor_future, Actor, ActorAction, ActorContext, ActorSystem, ReplyTo};
use rakka_http::{
    binary_service_route, json_actor_ask_route, json_actor_tell_route, json_entity_ask_route,
    json_entity_tell_route, json_service_route, HttpRouteConfig,
};
use rakka_sharding::{
    EntityTellError, EntityType, RoutedEntityMessage, ShardCoordinator, ShardRegion, ShardingConfig,
};
use serde::{Deserialize, Serialize};
use tower::ServiceExt;

#[tokio::test]
async fn json_actor_route_calls_actor_and_returns_json_response() {
    let system = ActorSystem::new("http-actor-route-test");
    let actor = system
        .spawn_actor("counter", CounterActor { value: 0 })
        .expect("counter actor should spawn");
    let router = json_actor_ask_route(
        "/counter/add",
        HttpRouteConfig::default(),
        actor,
        |request: AddRequest, reply_to| CounterCommand::Add {
            amount: request.amount,
            reply_to,
        },
    );

    let response = post_json(router, "/counter/add", &AddRequest { amount: 7 }).await;

    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<AddResponse>(&response.body).expect("json response"),
        AddResponse { value: 7 }
    );
}

#[tokio::test]
async fn json_entity_route_calls_sharded_entity_and_returns_json_response() {
    let entity_type = EntityType::new("Cart");
    let config = ShardingConfig::new(4).expect("valid sharding config");
    let mut coordinator = ShardCoordinator::new(entity_type.clone(), config.clone());
    let membership = membership_with_up_nodes(vec![node("rakka-0", "uid-a")]);
    coordinator.reconcile(&membership);

    let region = ShardRegion::from_snapshot(
        entity_type,
        config,
        &coordinator.snapshot(),
        |message: RoutedEntityMessage<CartCommand>| {
            match message.into_message() {
                CartCommand::Add { sku, reply_to } => {
                    let _sent = reply_to.reply(CartReply {
                        accepted: true,
                        sku,
                    });
                }
                CartCommand::Mark { sku, reply_to } => {
                    let _sent = reply_to.reply(CartReply {
                        accepted: true,
                        sku,
                    });
                }
            }
            Ok(())
        },
    )
    .expect("region should accept ownership snapshot");
    let entity = region.entity_ref("cart-1");
    let router = json_entity_ask_route(
        "/cart/add",
        HttpRouteConfig::default(),
        region,
        entity,
        |request: CartRequest, reply_to| CartCommand::Add {
            sku: request.sku,
            reply_to,
        },
    );

    let response = post_json(
        router,
        "/cart/add",
        &CartRequest {
            sku: "book".to_owned(),
        },
    )
    .await;

    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<CartReply>(&response.body).expect("json response"),
        CartReply {
            accepted: true,
            sku: "book".to_owned()
        }
    );
}

#[tokio::test]
async fn binary_service_route_round_trips_binary_payload() {
    let router = binary_service_route(
        "/echo",
        HttpRouteConfig::default(),
        |payload: Bytes| async move { Ok(Bytes::from([payload.as_ref(), b":ok"].concat())) },
    );

    let response = post_bytes(
        router,
        "/echo",
        b"payload".to_vec(),
        "application/octet-stream",
    )
    .await;

    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(response.body.as_ref(), b"payload:ok");
}

#[tokio::test]
async fn actor_timeout_maps_to_gateway_timeout() {
    let system = ActorSystem::new("http-actor-timeout-test");
    let actor = system
        .spawn_actor("slow", SlowActor)
        .expect("slow actor should spawn");
    let router = json_actor_ask_route(
        "/slow",
        HttpRouteConfig::default().request_timeout(Duration::from_millis(5)),
        actor,
        |request: AddRequest, reply_to| CounterCommand::Slow {
            amount: request.amount,
            reply_to,
        },
    );

    let response = post_json(router, "/slow", &AddRequest { amount: 1 }).await;

    assert_eq!(response.status, StatusCode::GATEWAY_TIMEOUT);
    assert_error_code(&response.body, "actor-timeout");
}

#[tokio::test]
async fn payload_limit_failure_maps_to_typed_http_error() {
    let router = json_service_route(
        "/tiny",
        HttpRouteConfig::default().max_payload_bytes(4),
        |_request: AddRequest| async { Ok(AddResponse { value: 0 }) },
    );

    let response = post_bytes(
        router,
        "/tiny",
        br#"{"amount":100}"#.to_vec(),
        "application/json",
    )
    .await;

    assert_eq!(response.status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_error_code(&response.body, "payload-too-large");
}

#[tokio::test]
async fn actor_and_entity_tell_routes_accept_json_messages() {
    let system = ActorSystem::new("http-tell-route-test");
    let actor = system
        .spawn_actor("collector", TellActor { seen: Vec::new() })
        .expect("tell actor should spawn");
    let actor_router = json_actor_tell_route(
        "/actor/tell",
        HttpRouteConfig::default(),
        actor,
        |request: AddRequest| TellCommand::Record(request.amount),
    );

    let actor_response = post_json(actor_router, "/actor/tell", &AddRequest { amount: 3 }).await;
    assert_eq!(actor_response.status, StatusCode::ACCEPTED);

    let entity_type = EntityType::new("Cart");
    let config = ShardingConfig::new(4).expect("valid sharding config");
    let mut coordinator = ShardCoordinator::new(entity_type.clone(), config.clone());
    let membership = membership_with_up_nodes(vec![node("rakka-0", "uid-a")]);
    coordinator.reconcile(&membership);

    let region = ShardRegion::from_snapshot(
        entity_type,
        config,
        &coordinator.snapshot(),
        |message: RoutedEntityMessage<CartCommand>| match message.into_message() {
            CartCommand::Mark { reply_to, .. } => {
                let _sent = reply_to.reply(CartReply {
                    accepted: true,
                    sku: "marked".to_owned(),
                });
                Ok(())
            }
            CartCommand::Add { sku, reply_to } => Err(EntityTellError::NoRoute {
                message: CartCommand::Add { sku, reply_to },
                error: rakka_sharding::ShardingError::InvalidShardCount,
            }),
        },
    )
    .expect("region should accept ownership snapshot");
    let entity = region.entity_ref("cart-1");
    let entity_router = json_entity_tell_route(
        "/entity/tell",
        HttpRouteConfig::default(),
        region,
        entity,
        |request: CartRequest| CartCommand::Mark {
            sku: request.sku,
            reply_to: ReplyTo::new(tokio::sync::oneshot::channel().0),
        },
    );

    let entity_response = post_json(
        entity_router,
        "/entity/tell",
        &CartRequest {
            sku: "book".to_owned(),
        },
    )
    .await;
    assert_eq!(entity_response.status, StatusCode::ACCEPTED);
}

struct HttpTestResponse {
    status: StatusCode,
    body: Bytes,
}

async fn post_json<T>(router: axum::Router, path: &str, payload: &T) -> HttpTestResponse
where
    T: Serialize,
{
    let body = serde_json::to_vec(payload).expect("request should encode");
    post_bytes(router, path, body, "application/json").await
}

async fn post_bytes(
    router: axum::Router,
    path: &str,
    body: Vec<u8>,
    content_type: &'static str,
) -> HttpTestResponse {
    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(path)
                .header(CONTENT_TYPE, content_type)
                .body(Body::from(body))
                .expect("request should build"),
        )
        .await
        .expect("router should respond");
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body should collect");
    HttpTestResponse { status, body }
}

fn assert_error_code(body: &[u8], expected: &str) {
    let error =
        serde_json::from_slice::<serde_json::Value>(body).expect("error body should be json");
    assert_eq!(error["code"], expected);
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AddRequest {
    amount: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AddResponse {
    value: i64,
}

enum CounterCommand {
    Add {
        amount: i64,
        reply_to: ReplyTo<AddResponse>,
    },
    Slow {
        amount: i64,
        reply_to: ReplyTo<AddResponse>,
    },
}

struct CounterActor {
    value: i64,
}

impl Actor for CounterActor {
    type Msg = CounterCommand;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> rakka_core::ActorFuture<'a> {
        actor_future(async move {
            match msg {
                CounterCommand::Add { amount, reply_to } => {
                    self.value += amount;
                    let _sent = reply_to.reply(AddResponse { value: self.value });
                }
                CounterCommand::Slow { amount, reply_to } => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    let _sent = reply_to.reply(AddResponse { value: amount });
                }
            }
            Ok(ActorAction::Continue)
        })
    }
}

struct SlowActor;

impl Actor for SlowActor {
    type Msg = CounterCommand;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> rakka_core::ActorFuture<'a> {
        actor_future(async move {
            if let CounterCommand::Slow { amount, reply_to } = msg {
                tokio::time::sleep(Duration::from_millis(50)).await;
                let _sent = reply_to.reply(AddResponse { value: amount });
            }
            Ok(ActorAction::Continue)
        })
    }
}

enum TellCommand {
    Record(i64),
}

struct TellActor {
    seen: Vec<i64>,
}

impl Actor for TellActor {
    type Msg = TellCommand;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> rakka_core::ActorFuture<'a> {
        actor_future(async move {
            let TellCommand::Record(value) = msg;
            self.seen.push(value);
            Ok(ActorAction::Continue)
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CartRequest {
    sku: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CartReply {
    accepted: bool,
    sku: String,
}

enum CartCommand {
    Add {
        sku: String,
        reply_to: ReplyTo<CartReply>,
    },
    Mark {
        sku: String,
        reply_to: ReplyTo<CartReply>,
    },
}

fn node(logical_id: &str, incarnation: &str) -> ClusterNode {
    ClusterNode::new(
        NodeId::new(logical_id, incarnation),
        NodeAddress::new(format!("{logical_id}.rakka.default.svc"), 2552),
    )
}

fn membership_with_up_nodes(nodes: Vec<ClusterNode>) -> ClusterMembership {
    let local = nodes[0].clone();
    let mut membership = ClusterMembership::new(
        local,
        MembershipConfig::new(1, Duration::from_millis(50), Duration::from_millis(100)),
    );

    membership
        .record_discovery(DiscoverySnapshot::new("test", 1, nodes))
        .expect("discovery should be accepted");

    for member in membership
        .members()
        .map(|member| member.node().id().clone())
        .collect::<Vec<_>>()
    {
        membership
            .mark_up(&member, 2)
            .expect("member should transition up");
    }

    membership
}
