#![forbid(unsafe_code)]

//! End-to-end Phase 5 gateway example.

use std::error::Error;
use std::io::{BufRead, Write};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::stream;
use rakka_cluster::{
    ClusterMembership, ClusterNode, DiscoverySnapshot, MembershipConfig, NodeAddress, NodeId,
};
use rakka_core::{
    actor_future, Actor, ActorAction, ActorContext, ActorFuture, ActorSystem,
    InMemoryMetricsRecorder, METRIC_GRPC_REQUEST_LATENCY_MS, METRIC_HTTP_REQUEST_LATENCY_MS,
};
use rakka_grpc::{
    bidi_streaming_service_from_stream, record_grpc_request_metrics, unary_actor_ask,
    unary_entity_ask, GrpcStreamConfig, GrpcUnaryConfig,
};
use rakka_http::{
    json_actor_ask_route, json_entity_ask_route, json_service_route, record_http_request_metrics,
    HttpError, HttpRouteConfig,
};
use rakka_k8s::{KubernetesDrainController, KubernetesDrainOutcome, KubernetesNodeHealth};
use rakka_process::{
    spawn_stdio_actor, ExecutableAllowlist, LineJsonCodec, ProcessSpec, ProcessStdio, StdioCommand,
    StdioProtocolConfig,
};
use rakka_sharding::{
    EntityRef, EntityType, RoutedEntityMessage, ShardCoordinator, ShardRegion, ShardingConfig,
};
use rakka_stream::{bounded_channel, StreamSink};
use rakka_testkit::{
    assert_drain_complete, assert_http_status, assert_metric_attribute, assert_probe_passed,
    collect_grpc_stream, expect_grpc_unary_ok, expect_metric_observation, http_post_json,
};
use serde::{Deserialize, Serialize};
use tonic::{Request, Status};

const CHILD_FLAG: &str = "--legacy-child";
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, prost::Message)]
struct AddRequest {
    #[prost(int64, tag = "1")]
    amount: i64,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, prost::Message)]
struct AddReply {
    #[prost(int64, tag = "1")]
    value: i64,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, prost::Message)]
struct CartRequest {
    #[prost(string, tag = "1")]
    sku: String,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, prost::Message)]
struct CartReply {
    #[prost(bool, tag = "1")]
    accepted: bool,
    #[prost(string, tag = "2")]
    entity_id: String,
    #[prost(string, tag = "3")]
    sku: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LegacyRequest {
    command: String,
    value: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct LegacyReply {
    service: String,
    result: u64,
}

enum CounterCommand {
    Add {
        amount: i64,
        reply_to: rakka_core::ReplyTo<AddReply>,
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
    ) -> ActorFuture<'a> {
        match msg {
            CounterCommand::Add { amount, reply_to } => {
                self.value += amount;
                let value = self.value;
                actor_future(async move {
                    let _sent = reply_to.reply(AddReply { value });
                    Ok(ActorAction::Continue)
                })
            }
        }
    }
}

enum CartCommand {
    Add {
        sku: String,
        reply_to: rakka_core::ReplyTo<CartReply>,
    },
    Record {
        sku: String,
    },
}

type CartEvents = Arc<Mutex<Vec<String>>>;
type CartRegionParts = (ShardRegion<CartCommand>, EntityRef<CartCommand>, CartEvents);

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    if std::env::args().any(|arg| arg == CHILD_FLAG) {
        run_legacy_child()?;
        return Ok(());
    }

    let system = ActorSystem::new("edge-gateway-example");
    let counter = system.spawn_actor("counter", CounterActor { value: 0 })?;
    let (cart_region, cart_entity, cart_events) = cart_region()?;
    let legacy = spawn_legacy_actor(&system)?;
    let recorder = InMemoryMetricsRecorder::new();

    let http_router = json_actor_ask_route(
        "/counter/add",
        HttpRouteConfig::default(),
        counter.clone(),
        |request: AddRequest, reply_to| CounterCommand::Add {
            amount: request.amount,
            reply_to,
        },
    )
    .merge(json_entity_ask_route(
        "/cart/add",
        HttpRouteConfig::default(),
        cart_region.clone(),
        cart_entity.clone(),
        |request: CartRequest, reply_to| CartCommand::Add {
            sku: request.sku,
            reply_to,
        },
    ))
    .merge(json_service_route(
        "/legacy/increment",
        HttpRouteConfig::default(),
        {
            let legacy = legacy.clone();
            move |request: LegacyRequest| {
                let legacy = legacy.clone();
                async move {
                    legacy
                        .ask(
                            |reply_to| StdioCommand::Request { request, reply_to },
                            DEFAULT_TIMEOUT,
                        )
                        .await
                        .map_err(|error| HttpError::service(error.to_string()))?
                        .map_err(|error| HttpError::service(error.to_string()))
                }
            }
        },
    ));

    let http_counter = record_http_request_metrics(&recorder, "POST", "/counter/add", async {
        Ok::<_, HttpError>(
            http_post_json(
                http_router.clone(),
                "/counter/add",
                &AddRequest { amount: 7 },
            )
            .await,
        )
    })
    .await?;
    assert_http_status(&http_counter, axum::http::StatusCode::OK);
    let http_counter: AddReply = http_counter.json();

    let http_cart = http_post_json(
        http_router.clone(),
        "/cart/add",
        &CartRequest {
            sku: "book".to_owned(),
        },
    )
    .await;
    assert_http_status(&http_cart, axum::http::StatusCode::OK);
    let http_cart: CartReply = http_cart.json();

    let http_legacy = http_post_json(
        http_router,
        "/legacy/increment",
        &LegacyRequest {
            command: "increment".to_owned(),
            value: 41,
        },
    )
    .await;
    assert_http_status(&http_legacy, axum::http::StatusCode::OK);
    let http_legacy: LegacyReply = http_legacy.json();

    let grpc_counter = record_grpc_request_metrics(
        &recorder,
        "rakka.example.CounterService",
        "Add",
        "unary",
        unary_actor_ask(
            Request::new(AddRequest { amount: 3 }),
            GrpcUnaryConfig::default(),
            &counter,
            |request, reply_to| CounterCommand::Add {
                amount: request.amount,
                reply_to,
            },
        ),
    )
    .await?
    .into_inner();
    let grpc_cart = expect_grpc_unary_ok(unary_entity_ask(
        Request::new(CartRequest {
            sku: "pencil".to_owned(),
        }),
        GrpcUnaryConfig::default(),
        &cart_region,
        &cart_entity,
        |request, reply_to| CartCommand::Add {
            sku: request.sku,
            reply_to,
        },
    ))
    .await;

    let grpc_stream = bidi_streaming_service_from_stream(
        stream::iter([
            Ok(CartRequest {
                sku: "eraser".to_owned(),
            }),
            Ok(CartRequest {
                sku: "ruler".to_owned(),
            }),
        ]),
        GrpcStreamConfig::default().buffer_capacity(2),
        {
            let cart_region = cart_region.clone();
            let cart_entity = cart_entity.clone();
            move |inbound, outbound| async move {
                while let Some(request) = inbound.next().await.map_err(stream_status)? {
                    let reply = cart_region
                        .ask(
                            &cart_entity,
                            |reply_to| CartCommand::Add {
                                sku: request.sku,
                                reply_to,
                            },
                            DEFAULT_TIMEOUT,
                        )
                        .await
                        .map_err(|error| Status::internal(error.to_string()))?;
                    outbound
                        .send(reply)
                        .await
                        .map_err(|error| Status::internal(error.to_string()))?;
                }
                Ok(())
            }
        },
    )?;
    let grpc_stream_replies = collect_grpc_stream(grpc_stream.into_inner()).await?;

    let ingested = run_streaming_ingestion(cart_region.clone(), cart_entity.clone()).await?;
    let health = KubernetesNodeHealth::from_membership(&membership_with_up_node(example_node())?);
    health.accept_compatibility();
    health.require_service("http");
    health.require_service("grpc");
    health.register_service("http");
    health.register_service("grpc");
    assert_probe_passed(&health.readiness_probe());
    let mut drain = KubernetesDrainController::new(health.clone());
    let (drain_sink, _drain_source) = bounded_channel::<String>(1)?;
    drain.add_stream_sink("gateway-streams", drain_sink);
    let drain_report = drain.drain(DEFAULT_TIMEOUT).await;
    assert_drain_complete(&drain_report);
    assert_eq!(drain_report.outcome(), KubernetesDrainOutcome::Complete);

    let metrics = recorder.snapshot();
    let http_metric = expect_metric_observation(
        &metrics,
        METRIC_HTTP_REQUEST_LATENCY_MS,
        rakka_core::MetricKind::Histogram,
    );
    assert_metric_attribute(&http_metric, "route", "/counter/add");
    let grpc_metric = expect_metric_observation(
        &metrics,
        METRIC_GRPC_REQUEST_LATENCY_MS,
        rakka_core::MetricKind::Histogram,
    );
    assert_metric_attribute(&grpc_metric, "method", "Add");

    println!(
        "HTTP actor gateway returned counter value {}.",
        http_counter.value
    );
    println!(
        "HTTP entity gateway accepted {} for {}.",
        http_cart.sku, http_cart.entity_id
    );
    println!(
        "HTTP process-backed legacy service returned {}.",
        http_legacy.result
    );
    println!(
        "gRPC unary actor value {} and entity SKU {}.",
        grpc_counter.value, grpc_cart.sku
    );
    println!(
        "gRPC bidirectional stream routed {} cart updates.",
        grpc_stream_replies.len()
    );
    println!(
        "Streaming ingestion transformed {} items into entity commands.",
        ingested
    );
    println!(
        "Kubernetes readiness passed before drain; drain outcome {:?}.",
        drain_report.outcome()
    );
    println!(
        "Metrics captured HTTP route {} and gRPC method {}.",
        http_metric.attribute("route").unwrap_or("unknown"),
        grpc_metric.attribute("method").unwrap_or("unknown")
    );

    legacy.stop()?;
    system.shutdown();
    let events = cart_events
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert!(events.iter().any(|event| event.contains("http:book")));
    Ok(())
}

async fn run_streaming_ingestion(
    cart_region: ShardRegion<CartCommand>,
    cart_entity: EntityRef<CartCommand>,
) -> Result<usize, Box<dyn Error>> {
    let (sink, source) = bounded_channel::<CartRequest>(1)?;
    let worker = tokio::spawn(async move {
        let mut count = 0usize;
        while let Some(request) = source.next().await.map_err(|error| error.to_string())? {
            let normalized = request.sku.to_ascii_uppercase();
            cart_region
                .tell(&cart_entity, CartCommand::Record { sku: normalized })
                .map_err(|_error| "entity tell failed".to_owned())?;
            count = count.saturating_add(1);
        }
        Ok::<usize, String>(count)
    });

    send_ingest(&sink, "paper").await?;
    send_ingest(&sink, "folder").await?;
    sink.drain()
        .map_err(|error| example_error(error.to_string()))?;
    worker
        .await
        .map_err(|error| example_error(error.to_string()))?
        .map_err(example_error)
        .map_err(Into::into)
}

async fn send_ingest(sink: &StreamSink<CartRequest>, sku: &str) -> Result<(), Box<dyn Error>> {
    sink.send(CartRequest {
        sku: sku.to_owned(),
    })
    .await
    .map_err(|error| example_error(error.to_string()).into())
}

fn cart_region() -> Result<CartRegionParts, Box<dyn Error>> {
    let node = example_node();
    let membership = membership_with_up_node(node.clone())?;
    let entity_type = EntityType::new("Cart");
    let sharding = ShardingConfig::new(8)?;
    let mut coordinator = ShardCoordinator::new(entity_type.clone(), sharding.clone());
    coordinator.reconcile(&membership);
    let events = Arc::new(Mutex::new(Vec::new()));
    let events_for_route = Arc::clone(&events);
    let region = ShardRegion::from_snapshot(
        entity_type,
        sharding,
        &coordinator.snapshot(),
        move |routed: RoutedEntityMessage<CartCommand>| {
            let entity_id = routed.entity_id().as_str().to_owned();
            match routed.into_message() {
                CartCommand::Add { sku, reply_to } => {
                    events_for_route
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push(format!("http:{sku}"));
                    let _sent = reply_to.reply(CartReply {
                        accepted: true,
                        entity_id,
                        sku,
                    });
                }
                CartCommand::Record { sku } => {
                    events_for_route
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push(format!("stream:{sku}"));
                }
            }
            Ok(())
        },
    )?;
    let entity = region.entity_ref("cart-1");
    Ok((region, entity, events))
}

fn example_node() -> ClusterNode {
    ClusterNode::new(
        NodeId::new("rakka-0", "uid-a"),
        NodeAddress::new("rakka-0.rakka.default.svc.cluster.local", 2552),
    )
    .with_role("gateway")
}

fn membership_with_up_node(node: ClusterNode) -> Result<ClusterMembership, Box<dyn Error>> {
    let mut membership = ClusterMembership::new(
        node.clone(),
        MembershipConfig::new(1, Duration::from_millis(50), Duration::from_millis(100)),
    );
    membership.record_discovery(DiscoverySnapshot::new("example", 1, [node.clone()]))?;
    membership.mark_up(node.id(), 2)?;
    Ok(membership)
}

fn spawn_legacy_actor(
    system: &ActorSystem,
) -> Result<rakka_core::ActorRef<StdioCommand<LegacyRequest, LegacyReply>>, Box<dyn Error>> {
    let executable = std::env::current_exe()?;
    let allowlist = ExecutableAllowlist::from_exact_paths([executable.clone()]);
    let spec = ProcessSpec::new(executable)
        .arg(CHILD_FLAG)
        .stdin(ProcessStdio::Piped)
        .stdout(ProcessStdio::Piped)
        .stderr(ProcessStdio::Piped)
        .shutdown_timeout(Duration::from_secs(1));

    Ok(spawn_stdio_actor(
        system,
        "legacy-calculator",
        spec,
        allowlist,
        LineJsonCodec::<LegacyRequest, LegacyReply>::new(),
        StdioProtocolConfig::new().default_request_timeout(DEFAULT_TIMEOUT),
    )?)
}

fn run_legacy_child() -> Result<(), Box<dyn Error>> {
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    for line in stdin.lock().lines() {
        let line = line?;
        let frame: serde_json::Value = serde_json::from_str(&line)?;
        let request_id = frame
            .get("id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| example_error("line-json request is missing id"))?;
        let request: LegacyRequest = serde_json::from_value(
            frame
                .get("payload")
                .cloned()
                .ok_or_else(|| example_error("line-json request is missing payload"))?,
        )?;
        let response = serde_json::json!({
            "id": request_id,
            "payload": {
                "service": "legacy-calculator",
                "result": request.value + 1,
            },
        });
        writeln!(stdout, "{response}")?;
        stdout.flush()?;
    }
    Ok(())
}

fn stream_status(error: rakka_stream::StreamError) -> Status {
    Status::internal(error.to_string())
}

fn example_error(message: impl Into<String>) -> std::io::Error {
    std::io::Error::other(message.into())
}
