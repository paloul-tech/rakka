//! Unary gRPC adapter behavior tests.

use std::time::Duration;

use rakka_cluster::{
    ClusterMembership, ClusterNode, DiscoverySnapshot, MembershipConfig, NodeAddress, NodeId,
};
use rakka_core::{
    actor_future, Actor, ActorAction, ActorContext, ActorFuture, ActorSystem, ReplyTo,
};
use rakka_grpc::{
    decode_status, unary_actor_ask, unary_actor_tell, unary_entity_ask, unary_entity_tell,
    unary_service, validation_status, GrpcResult, GrpcUnaryConfig, RAKKA_GRPC_ERROR_CODE_METADATA,
};
use rakka_sharding::{
    EntityType, RoutedEntityMessage, ShardCoordinator, ShardRegion, ShardingConfig,
};
use tokio::sync::{mpsc, oneshot};
use tonic::{Code, Request, Response, Status};

#[tokio::test]
async fn unary_service_calls_handler_and_returns_protobuf_response() {
    let response = unary_service(
        Request::new(AddRequest { amount: 4 }),
        GrpcUnaryConfig::default(),
        |request: AddRequest| async move {
            Ok(AddReply {
                value: request.amount + 1,
            })
        },
    )
    .await
    .expect("service adapter should respond");

    assert_eq!(response.into_inner().value, 5);
}

#[tokio::test]
async fn validation_failure_maps_to_invalid_argument_status() {
    let status = expect_status(
        unary_service(
            Request::new(AddRequest { amount: -1 }),
            GrpcUnaryConfig::default(),
            |request: AddRequest| async move {
                if request.amount < 0 {
                    return Err(validation_status("amount must be non-negative"));
                }

                Ok(AddReply {
                    value: request.amount,
                })
            },
        )
        .await,
    );

    assert_eq!(status.code(), Code::InvalidArgument);
    assert_status_error_code(&status, "validation-error");

    let decode = decode_status("invalid protobuf payload");
    assert_eq!(decode.code(), Code::InvalidArgument);
    assert_status_error_code(&decode, "decode-error");
}

#[tokio::test]
async fn unary_actor_ask_calls_actor_and_returns_protobuf_response() {
    let system = ActorSystem::new("grpc-actor-ask-test");
    let actor = system
        .spawn_actor("counter", CounterActor { value: 0 })
        .expect("counter actor should spawn");

    let response = unary_actor_ask(
        Request::new(AddRequest { amount: 7 }),
        GrpcUnaryConfig::default(),
        &actor,
        |request, reply_to| CounterCommand::Add {
            amount: request.amount,
            reply_to,
        },
    )
    .await
    .expect("actor adapter should respond");

    assert_eq!(response.into_inner().value, 7);
    system.shutdown();
}

#[tokio::test]
async fn unary_entity_ask_calls_sharded_entity_and_returns_protobuf_response() {
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
                CartCommand::Slow { sku, reply_to } => {
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        let _sent = reply_to.reply(CartReply {
                            accepted: true,
                            sku,
                        });
                    });
                }
                CartCommand::Mark { sku, seen } => {
                    let _sent = seen.send(sku);
                }
            }
            Ok(())
        },
    )
    .expect("region should accept ownership snapshot");
    let entity = region.entity_ref("cart-1");

    let response = unary_entity_ask(
        Request::new(CartRequest {
            sku: "book".to_owned(),
        }),
        GrpcUnaryConfig::default(),
        &region,
        &entity,
        |request, reply_to| CartCommand::Add {
            sku: request.sku,
            reply_to,
        },
    )
    .await
    .expect("entity adapter should respond");

    assert_eq!(response.into_inner().sku, "book");
}

#[tokio::test]
async fn actor_timeout_maps_to_deadline_exceeded_status() {
    let system = ActorSystem::new("grpc-actor-timeout-test");
    let actor = system
        .spawn_actor("slow", CounterActor { value: 0 })
        .expect("slow actor should spawn");

    let status = expect_status(
        unary_actor_ask(
            Request::new(AddRequest { amount: 1 }),
            GrpcUnaryConfig::default().request_timeout(Duration::from_millis(5)),
            &actor,
            |request, reply_to| CounterCommand::Slow {
                amount: request.amount,
                reply_to,
            },
        )
        .await,
    );

    assert_eq!(status.code(), Code::DeadlineExceeded);
    assert_status_error_code(&status, "actor-timeout");
    system.shutdown();
}

#[tokio::test]
async fn entity_timeout_respects_grpc_timeout_metadata() {
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
            if let CartCommand::Slow { sku, reply_to } = message.into_message() {
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    let _sent = reply_to.reply(CartReply {
                        accepted: true,
                        sku,
                    });
                });
            }
            Ok(())
        },
    )
    .expect("region should accept ownership snapshot");
    let entity = region.entity_ref("cart-1");
    let mut request = Request::new(CartRequest {
        sku: "book".to_owned(),
    });
    request.set_timeout(Duration::from_millis(5));

    let status = expect_status(
        unary_entity_ask(
            request,
            GrpcUnaryConfig::default().request_timeout(Duration::from_secs(1)),
            &region,
            &entity,
            |request, reply_to| CartCommand::Slow {
                sku: request.sku,
                reply_to,
            },
        )
        .await,
    );

    assert_eq!(status.code(), Code::DeadlineExceeded);
    assert_status_error_code(&status, "entity-timeout");
}

#[tokio::test]
async fn cancelling_unary_actor_ask_drops_pending_reply_receiver() {
    let system = ActorSystem::new("grpc-actor-cancel-test");
    let actor = system
        .spawn_actor("slow", CounterActor { value: 0 })
        .expect("slow actor should spawn");
    let (observed_tx, observed_rx) = oneshot::channel();
    let actor_for_task = actor.clone();

    let handle = tokio::spawn(async move {
        unary_actor_ask(
            Request::new(AddRequest { amount: 9 }),
            GrpcUnaryConfig::default().request_timeout(Duration::from_secs(1)),
            &actor_for_task,
            |request, reply_to| CounterCommand::SlowCancellation {
                amount: request.amount,
                reply_to,
                observed: observed_tx,
            },
        )
        .await
    });

    tokio::time::sleep(Duration::from_millis(5)).await;
    handle.abort();

    match handle.await {
        Err(error) => assert!(error.is_cancelled()),
        Ok(_) => panic!("aborted gRPC adapter task should not complete normally"),
    }

    let reply_was_dropped = tokio::time::timeout(Duration::from_secs(1), observed_rx)
        .await
        .expect("actor should observe cancelled reply receiver")
        .expect("actor should report reply state");
    assert!(reply_was_dropped);
    system.shutdown();
}

#[tokio::test]
async fn actor_and_entity_tell_adapters_accept_protobuf_requests() {
    let system = ActorSystem::new("grpc-tell-test");
    let actor = system
        .spawn_actor("collector", TellActor)
        .expect("tell actor should spawn");
    let (actor_tx, mut actor_rx) = mpsc::unbounded_channel();

    let response = unary_actor_tell(
        Request::new(AddRequest { amount: 3 }),
        &actor,
        |request| TellCommand::Record {
            amount: request.amount,
            seen: actor_tx,
        },
        || AckReply { accepted: true },
    )
    .expect("actor tell should be accepted");

    assert!(response.into_inner().accepted);
    assert_eq!(actor_rx.recv().await, Some(3));

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
            if let CartCommand::Mark { sku, seen } = message.into_message() {
                let _sent = seen.send(sku);
            }
            Ok(())
        },
    )
    .expect("region should accept ownership snapshot");
    let entity = region.entity_ref("cart-1");
    let (entity_tx, mut entity_rx) = mpsc::unbounded_channel();

    let response = unary_entity_tell(
        Request::new(CartRequest {
            sku: "book".to_owned(),
        }),
        &region,
        &entity,
        |request| CartCommand::Mark {
            sku: request.sku,
            seen: entity_tx,
        },
        || AckReply { accepted: true },
    )
    .expect("entity tell should be accepted");

    assert!(response.into_inner().accepted);
    assert_eq!(entity_rx.recv().await.as_deref(), Some("book"));
    system.shutdown();
}

fn expect_status<T>(result: GrpcResult<Response<T>>) -> Status {
    match result {
        Ok(_) => panic!("expected gRPC status error"),
        Err(status) => status,
    }
}

fn assert_status_error_code(status: &Status, expected: &str) {
    let code = status
        .metadata()
        .get(RAKKA_GRPC_ERROR_CODE_METADATA)
        .expect("status should include Rakka error code")
        .to_str()
        .expect("error code should be ASCII metadata");
    assert_eq!(code, expected);
}

#[derive(Clone, PartialEq, prost::Message)]
struct AddRequest {
    #[prost(int64, tag = "1")]
    amount: i64,
}

#[derive(Clone, PartialEq, prost::Message)]
struct AddReply {
    #[prost(int64, tag = "1")]
    value: i64,
}

#[derive(Clone, PartialEq, prost::Message)]
struct AckReply {
    #[prost(bool, tag = "1")]
    accepted: bool,
}

#[derive(Clone, PartialEq, prost::Message)]
struct CartRequest {
    #[prost(string, tag = "1")]
    sku: String,
}

#[derive(Clone, PartialEq, prost::Message)]
struct CartReply {
    #[prost(bool, tag = "1")]
    accepted: bool,
    #[prost(string, tag = "2")]
    sku: String,
}

enum CounterCommand {
    Add {
        amount: i64,
        reply_to: ReplyTo<AddReply>,
    },
    Slow {
        amount: i64,
        reply_to: ReplyTo<AddReply>,
    },
    SlowCancellation {
        amount: i64,
        reply_to: ReplyTo<AddReply>,
        observed: oneshot::Sender<bool>,
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
        actor_future(async move {
            match msg {
                CounterCommand::Add { amount, reply_to } => {
                    self.value += amount;
                    let _sent = reply_to.reply(AddReply { value: self.value });
                }
                CounterCommand::Slow { amount, reply_to } => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    let _sent = reply_to.reply(AddReply { value: amount });
                }
                CounterCommand::SlowCancellation {
                    amount,
                    reply_to,
                    observed,
                } => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    let reply_was_dropped = reply_to.reply(AddReply { value: amount }).is_err();
                    let _sent = observed.send(reply_was_dropped);
                }
            }
            Ok(ActorAction::Continue)
        })
    }
}

enum TellCommand {
    Record {
        amount: i64,
        seen: mpsc::UnboundedSender<i64>,
    },
}

struct TellActor;

impl Actor for TellActor {
    type Msg = TellCommand;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        actor_future(async move {
            let TellCommand::Record { amount, seen } = msg;
            let _sent = seen.send(amount);
            Ok(ActorAction::Continue)
        })
    }
}

enum CartCommand {
    Add {
        sku: String,
        reply_to: ReplyTo<CartReply>,
    },
    Slow {
        sku: String,
        reply_to: ReplyTo<CartReply>,
    },
    Mark {
        sku: String,
        seen: mpsc::UnboundedSender<String>,
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
