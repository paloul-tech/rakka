//! Integration tests for the remote envelope and serialization boundary.

use prost::Message;
use rakka_cluster::NodeId;
use rakka_core::{
    actor_fn, actor_future, Actor, ActorAction, ActorContext, ActorFuture, ActorOptions, ActorPath,
    ActorUid, Receptionist, SerializedActorRef, ServiceKey,
};
use rakka_remote::{
    EncodedPayload, InMemoryRemoteTransport, ProtobufEnvelopeCodec, RemoteActorRef,
    RemoteActorRefInbound, RemoteDestination, RemoteEndpoint, RemoteEndpointError, RemoteEnvelope,
    RemoteEnvelopeMetadata, RemoteError, RemoteReceptionistListing, RemoteRequestError,
    RemoteRequestRegistry, RemoteServiceRoutee, RemoteTransport, RemoteTransportError,
    SchemaCompatibilityPolicy, SerializationRegistry, TcpRemoteTransportConfig,
    DEFAULT_REMOTE_ENVELOPE_VERSION, DEFAULT_TCP_REMOTE_BIND_ADDR,
    DEFAULT_TCP_REMOTE_CONNECT_TIMEOUT, DEFAULT_TCP_REMOTE_IDLE_TIMEOUT,
    DEFAULT_TCP_REMOTE_MAX_FRAME_BYTES, DEFAULT_TCP_REMOTE_OUTBOUND_QUEUE_CAPACITY,
    DEFAULT_TCP_REMOTE_RECONNECT_BACKOFF, TCP_REMOTE_REQUIRES_REGISTERED_PEERS,
};
use std::any::type_name;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

#[derive(Clone, PartialEq, Message)]
struct Ping {
    #[prost(string, tag = "1")]
    value: String,
}

#[derive(Clone, PartialEq, Message)]
struct Pong {
    #[prost(string, tag = "1")]
    value: String,
}

#[derive(Clone, PartialEq, Message)]
struct CompatPingV1 {
    #[prost(string, tag = "1")]
    value: String,
}

#[derive(Clone, PartialEq, Message)]
struct CompatPingV2 {
    #[prost(string, tag = "1")]
    value: String,
    #[prost(string, tag = "2")]
    suffix: String,
}

#[test]
fn tcp_remote_defaults_are_loopback_bounded_and_known_peer_only() {
    let config = TcpRemoteTransportConfig::default();

    assert_eq!(config.bind_addr_value(), DEFAULT_TCP_REMOTE_BIND_ADDR);
    assert_eq!(
        config.outbound_queue_capacity_value(),
        DEFAULT_TCP_REMOTE_OUTBOUND_QUEUE_CAPACITY
    );
    assert_eq!(
        config.connect_timeout_value(),
        DEFAULT_TCP_REMOTE_CONNECT_TIMEOUT
    );
    assert_eq!(
        config.reconnect_backoff_value(),
        DEFAULT_TCP_REMOTE_RECONNECT_BACKOFF
    );
    assert_eq!(config.idle_timeout_value(), DEFAULT_TCP_REMOTE_IDLE_TIMEOUT);
    assert_eq!(
        config.max_frame_bytes_value(),
        DEFAULT_TCP_REMOTE_MAX_FRAME_BYTES
    );
    assert_eq!(
        config.envelope_version_value(),
        DEFAULT_REMOTE_ENVELOPE_VERSION
    );
    assert_eq!(
        config.requires_registered_peers(),
        TCP_REMOTE_REQUIRES_REGISTERED_PEERS
    );
}

#[test]
fn protobuf_registry_round_trips_a_payload() {
    let mut registry = SerializationRegistry::new();
    registry
        .register_protobuf::<Ping>("rakka.test.Ping", 1)
        .unwrap();

    let encoded = registry
        .encode(&Ping {
            value: "hello".to_string(),
        })
        .unwrap();
    let decoded: Ping = registry.decode(&encoded).unwrap();

    assert_eq!(decoded.value, "hello");
    assert_eq!(encoded.metadata.codec_id, "protobuf");
    assert_eq!(encoded.metadata.message_type_id, "rakka.test.Ping");
    assert_eq!(encoded.metadata.schema_version, 1);
}

#[test]
fn protobuf_envelope_codec_round_trips_route_metadata() {
    let mut registry = SerializationRegistry::new();
    registry
        .register_protobuf::<Ping>("rakka.test.Ping", 1)
        .unwrap();
    let encoded = registry
        .encode(&Ping {
            value: "hello".to_string(),
        })
        .unwrap();
    let envelope = RemoteEnvelope::new(
        RemoteDestination::Entity {
            entity_type: "cart".to_string(),
            entity_id: "cart-42".to_string(),
        },
        encoded,
    )
    .with_source("/system/gateway")
    .with_trace_context("traceparent=abc")
    .with_request_id("request-1");

    let wire = ProtobufEnvelopeCodec::encode(&envelope).unwrap();
    let decoded_envelope = ProtobufEnvelopeCodec::decode(&wire).unwrap();
    let decoded_payload: Ping = registry.decode_envelope(&decoded_envelope).unwrap();

    assert_eq!(decoded_envelope, envelope);
    assert_eq!(decoded_payload.value, "hello");
}

#[test]
fn all_destination_variants_round_trip_through_envelope_codec() {
    let destinations = [
        RemoteDestination::ActorPath {
            path: "/user/worker".to_string(),
        },
        RemoteDestination::actor_ref(test_remote_actor_ref()),
        RemoteDestination::Entity {
            entity_type: "cart".to_string(),
            entity_id: "cart-42".to_string(),
        },
        RemoteDestination::Service {
            service_key: "payments".to_string(),
        },
        RemoteDestination::RouteKey {
            route_key: "tenant-a/orders".to_string(),
        },
        RemoteDestination::Reply {
            request_id: "request-1".to_string(),
        },
    ];

    for destination in destinations {
        let envelope = RemoteEnvelope::new(
            destination,
            EncodedPayload::new(
                RemoteEnvelopeMetadata::protobuf("rakka.test.Ping", 1),
                vec![1, 2, 3],
            ),
        );

        let wire = ProtobufEnvelopeCodec::encode(&envelope).unwrap();
        let decoded = ProtobufEnvelopeCodec::decode(&wire).unwrap();

        assert_eq!(decoded, envelope);
    }
}

#[test]
fn remote_actor_ref_destination_round_trips_serialized_identity() {
    let serialized = SerializedActorRef::new(
        "orders",
        ActorPath::new("rakka://local/orders/user/worker"),
        ActorUid::new(42),
        "rakka.test.Ping",
    );
    let actor_ref =
        RemoteActorRef::from_serialized(NodeId::new("rakka-0", "uid-a"), &serialized).unwrap();
    let envelope = RemoteEnvelope::new(
        RemoteDestination::actor_ref(actor_ref.clone()),
        EncodedPayload::new(
            RemoteEnvelopeMetadata::protobuf("rakka.test.Ping", 1),
            vec![1, 2, 3],
        ),
    );

    let wire = ProtobufEnvelopeCodec::encode(&envelope).unwrap();
    let decoded = ProtobufEnvelopeCodec::decode(&wire).unwrap();

    assert_eq!(decoded, envelope);
    assert_eq!(actor_ref.node_id(), &NodeId::new("rakka-0", "uid-a"));
    assert_eq!(actor_ref.to_serialized_ref(), serialized);
}

#[tokio::test]
async fn remote_receptionist_listing_from_local_listing_converts_routees() {
    let node_id = NodeId::new("rakka-0", "uid-a");
    let system = rakka_core::ActorSystem::new("remote-listing-converts-routees");
    let key = ServiceKey::<Ping>::new("workers");
    let receptionist = Receptionist::get(&system);
    let (_delivered_a, _received_a, actor_a) = spawn_recording_ping_actor(&system, "worker-a");
    let (_delivered_b, _received_b, actor_b) = spawn_recording_ping_actor(&system, "worker-b");
    let _registration_a = receptionist.register(&key, actor_a.clone()).unwrap();
    let _registration_b = receptionist.register(&key, actor_b.clone()).unwrap();
    let listing = receptionist.find_local(&key).unwrap();

    let remote = RemoteReceptionistListing::from_listing(
        node_id.clone(),
        &system.actor_ref_resolver(),
        &listing,
        99,
    )
    .unwrap();

    assert_eq!(remote.source_node(), &node_id);
    assert_eq!(remote.service_id(), "workers");
    assert_eq!(remote.service_message_type(), type_name::<Ping>());
    assert_eq!(remote.version(), listing.revision());
    assert_eq!(remote.observed_at_millis(), 99);
    assert_eq!(remote.len(), 2);
    assert!(!remote.is_empty());
    assert!(remote.routees().iter().all(|routee| {
        routee.actor_ref().node_id() == &node_id && routee.message_type() == type_name::<Ping>()
    }));

    let mut remote_paths = remote
        .routees()
        .iter()
        .map(|routee| routee.actor_ref().path().to_string())
        .collect::<Vec<_>>();
    let mut local_paths = vec![actor_a.path().to_string(), actor_b.path().to_string()];
    remote_paths.sort();
    local_paths.sort();
    assert_eq!(remote_paths, local_paths);

    system.terminate().await.unwrap();
}

#[tokio::test]
async fn remote_receptionist_listing_allows_empty_listing_for_deregistration() {
    let node_id = NodeId::new("rakka-0", "uid-a");
    let system = rakka_core::ActorSystem::new("remote-listing-empty");
    let key = ServiceKey::<Ping>::new("workers");
    let receptionist = Receptionist::get(&system);
    let listing = receptionist.find_local(&key).unwrap();

    let remote = RemoteReceptionistListing::from_listing(
        node_id.clone(),
        &system.actor_ref_resolver(),
        &listing,
        100,
    )
    .unwrap();

    assert_eq!(remote.source_node(), &node_id);
    assert_eq!(remote.service_id(), "workers");
    assert_eq!(remote.service_message_type(), type_name::<Ping>());
    assert_eq!(remote.version(), listing.revision());
    assert_eq!(remote.observed_at_millis(), 100);
    assert!(remote.is_empty());

    system.terminate().await.unwrap();
}

#[test]
fn remote_receptionist_listing_rejects_invalid_inputs() {
    let source_node = NodeId::new("rakka-0", "uid-a");
    let routee = RemoteServiceRoutee::new(test_remote_actor_ref());

    assert!(matches!(
        RemoteReceptionistListing::new(
            NodeId::new("", "uid-a"),
            "workers",
            type_name::<Ping>(),
            Vec::new(),
            0,
            1,
        ),
        Err(RemoteError::InvalidEnvelope { .. })
    ));
    assert!(matches!(
        RemoteReceptionistListing::new(
            source_node.clone(),
            "",
            type_name::<Ping>(),
            Vec::new(),
            0,
            1,
        ),
        Err(RemoteError::InvalidEnvelope { .. })
    ));
    assert!(matches!(
        RemoteReceptionistListing::new(source_node.clone(), "workers", "", Vec::new(), 0, 1,),
        Err(RemoteError::InvalidEnvelope { .. })
    ));

    let wrong_node_routee = RemoteServiceRoutee::new(
        RemoteActorRef::new(
            NodeId::new("rakka-1", "uid-b"),
            "orders",
            ActorPath::new("rakka://local/orders/user/worker"),
            ActorUid::new(42),
            type_name::<Ping>(),
        )
        .unwrap(),
    );
    assert!(matches!(
        RemoteReceptionistListing::new(
            source_node.clone(),
            "workers",
            type_name::<Ping>(),
            vec![wrong_node_routee],
            1,
            1,
        ),
        Err(RemoteError::InvalidEnvelope { .. })
    ));

    let wrong_type_routee = RemoteServiceRoutee::new(
        RemoteActorRef::new(
            source_node.clone(),
            "orders",
            ActorPath::new("rakka://local/orders/user/worker"),
            ActorUid::new(42),
            type_name::<Pong>(),
        )
        .unwrap(),
    );
    assert!(matches!(
        RemoteReceptionistListing::new(
            source_node.clone(),
            "workers",
            type_name::<Ping>(),
            vec![wrong_type_routee],
            1,
            1,
        ),
        Err(RemoteError::InvalidEnvelope { .. })
    ));
    assert!(matches!(
        RemoteReceptionistListing::new_with_max_routees(
            source_node,
            "workers",
            type_name::<Ping>(),
            vec![routee],
            1,
            1,
            Some(0),
        ),
        Err(RemoteError::InvalidEnvelope { .. })
    ));
}

#[tokio::test]
async fn remote_receptionist_listing_from_local_listing_enforces_max_routees() {
    let node_id = NodeId::new("rakka-0", "uid-a");
    let system = rakka_core::ActorSystem::new("remote-listing-max-routees");
    let key = ServiceKey::<Ping>::new("workers");
    let receptionist = Receptionist::get(&system);
    let (_delivered, _received, actor) = spawn_recording_ping_actor(&system, "worker");
    let _registration = receptionist.register(&key, actor).unwrap();
    let listing = receptionist.find_local(&key).unwrap();

    let error = RemoteReceptionistListing::from_listing_with_max_routees(
        node_id,
        &system.actor_ref_resolver(),
        &listing,
        101,
        Some(0),
    )
    .unwrap_err();

    assert!(matches!(error, RemoteError::InvalidEnvelope { .. }));
    system.terminate().await.unwrap();
}

#[test]
fn duplicate_codec_registration_fails_closed() {
    let mut registry = SerializationRegistry::new();
    registry
        .register_protobuf::<Ping>("rakka.test.Ping", 1)
        .unwrap();

    let error = registry
        .register_protobuf::<Ping>("rakka.test.Ping", 1)
        .unwrap_err();

    assert_eq!(
        error,
        RemoteError::DuplicateCodec {
            codec_id: "protobuf".to_string(),
            message_type_id: "rakka.test.Ping".to_string(),
            schema_version: 1,
        }
    );
}

#[test]
fn unknown_message_type_and_unknown_codec_fail_closed() {
    let registry = SerializationRegistry::new();

    let unknown_message = registry
        .encode(&Ping {
            value: "hello".to_string(),
        })
        .unwrap_err();
    assert!(matches!(
        unknown_message,
        RemoteError::UnknownMessageType { .. }
    ));

    let unknown_codec = registry
        .decode::<Ping>(&EncodedPayload::new(
            RemoteEnvelopeMetadata::protobuf("rakka.test.Ping", 1),
            Vec::new(),
        ))
        .unwrap_err();
    assert_eq!(
        unknown_codec,
        RemoteError::UnknownCodec {
            codec_id: "protobuf".to_string(),
            message_type_id: "rakka.test.Ping".to_string(),
            schema_version: 1,
        }
    );
}

#[test]
fn wrong_rust_type_for_registered_codec_fails_closed() {
    let mut registry = SerializationRegistry::new();
    registry
        .register_protobuf::<Ping>("rakka.test.Ping", 1)
        .unwrap();
    let encoded = registry
        .encode(&Ping {
            value: "hello".to_string(),
        })
        .unwrap();

    let error = registry.decode::<Pong>(&encoded).unwrap_err();

    assert!(matches!(
        error,
        RemoteError::CodecTypeMismatch {
            message_type_id,
            expected,
        } if message_type_id == "rakka.test.Ping" && expected.ends_with("Pong")
    ));
}

#[test]
fn schema_policy_accepts_additive_n_plus_one_versions() {
    let mut old_registry = SerializationRegistry::new();
    old_registry
        .register_protobuf::<CompatPingV1>("rakka.test.CompatPing", 1)
        .unwrap();
    let old_payload = old_registry
        .encode(&CompatPingV1 {
            value: "hello".to_string(),
        })
        .unwrap();
    let mut new_registry = SerializationRegistry::new();
    let policy = SchemaCompatibilityPolicy::n_plus_one(2);

    new_registry
        .register_protobuf_compatible::<CompatPingV2>("rakka.test.CompatPing", 2, policy)
        .unwrap();

    let decoded_old: CompatPingV2 = new_registry.decode(&old_payload).unwrap();
    let encoded_new = new_registry
        .encode(&CompatPingV2 {
            value: "hello".to_string(),
            suffix: "new".to_string(),
        })
        .unwrap();

    assert_eq!(policy.min_supported(), 1);
    assert_eq!(policy.max_supported(), 2);
    assert!(policy.supports(1));
    assert!(policy.supports(2));
    assert!(!policy.supports(3));
    assert_eq!(decoded_old.value, "hello");
    assert_eq!(decoded_old.suffix, "");
    assert_eq!(encoded_new.metadata.schema_version, 2);
}

#[test]
fn exact_schema_policy_rejects_old_schema_versions() {
    let mut old_registry = SerializationRegistry::new();
    old_registry
        .register_protobuf::<CompatPingV1>("rakka.test.CompatPing", 1)
        .unwrap();
    let old_payload = old_registry
        .encode(&CompatPingV1 {
            value: "hello".to_string(),
        })
        .unwrap();
    let mut registry = SerializationRegistry::new();

    registry
        .register_protobuf_compatible::<CompatPingV2>(
            "rakka.test.CompatPing",
            2,
            SchemaCompatibilityPolicy::exact(2),
        )
        .unwrap();

    let error = registry.decode::<CompatPingV2>(&old_payload).unwrap_err();

    assert_eq!(
        error,
        RemoteError::UnknownCodec {
            codec_id: "protobuf".to_string(),
            message_type_id: "rakka.test.CompatPing".to_string(),
            schema_version: 1,
        }
    );
}

#[test]
fn unsupported_schema_versions_fail_closed_with_typed_errors() {
    let mut registry = SerializationRegistry::new();
    registry
        .register_protobuf_compatible::<CompatPingV2>(
            "rakka.test.CompatPing",
            2,
            SchemaCompatibilityPolicy::additive_window(1, 2).unwrap(),
        )
        .unwrap();
    let unsupported = EncodedPayload::new(
        RemoteEnvelopeMetadata::protobuf("rakka.test.CompatPing", 3),
        Vec::new(),
    );

    let error = registry.decode::<CompatPingV2>(&unsupported).unwrap_err();

    assert_eq!(
        error,
        RemoteError::UnknownCodec {
            codec_id: "protobuf".to_string(),
            message_type_id: "rakka.test.CompatPing".to_string(),
            schema_version: 3,
        }
    );
}

#[test]
fn invalid_schema_policy_is_rejected_before_registration() {
    let error = SchemaCompatibilityPolicy::additive_window(2, 1).unwrap_err();
    assert_eq!(
        error,
        RemoteError::InvalidSchemaCompatibilityPolicy {
            min_supported: 2,
            max_supported: 1,
            current: 1,
        }
    );

    let mut registry = SerializationRegistry::new();
    let error = registry
        .register_protobuf_compatible::<CompatPingV2>(
            "rakka.test.CompatPing",
            3,
            SchemaCompatibilityPolicy::additive_window(1, 2).unwrap(),
        )
        .unwrap_err();

    assert_eq!(
        error,
        RemoteError::InvalidSchemaCompatibilityPolicy {
            min_supported: 1,
            max_supported: 2,
            current: 3,
        }
    );
}

#[test]
fn invalid_envelope_bytes_fail_as_decode_error() {
    let error = ProtobufEnvelopeCodec::decode(&[255, 255, 255]).unwrap_err();

    assert!(matches!(error, RemoteError::Decode { .. }));
}

#[test]
fn endpoint_dispatches_entity_envelope_received_over_in_memory_transport() {
    let mut registry = SerializationRegistry::new();
    registry
        .register_protobuf::<Ping>("rakka.test.Ping", 1)
        .unwrap();
    let received = Arc::new(Mutex::new(Vec::new()));
    let received_for_handler = received.clone();
    let registry_for_handler = registry.clone();
    let endpoint = RemoteEndpoint::new(NodeId::new("rakka-1", "uid-b"));
    endpoint
        .register_entity_handler("cart", move |envelope: RemoteEnvelope| {
            let ping: Ping = registry_for_handler.decode_envelope(&envelope).unwrap();
            received_for_handler
                .lock()
                .expect("received mutex poisoned")
                .push(ping.value);
            Ok(())
        })
        .unwrap();
    let transport = InMemoryRemoteTransport::new();
    transport.register_endpoint(endpoint).unwrap();
    let envelope = RemoteEnvelope::new(
        RemoteDestination::Entity {
            entity_type: "cart".to_string(),
            entity_id: "cart-42".to_string(),
        },
        registry
            .encode(&Ping {
                value: "hello".to_string(),
            })
            .unwrap(),
    );

    transport
        .send(&NodeId::new("rakka-1", "uid-b"), envelope)
        .unwrap();

    assert_eq!(
        *received.lock().expect("received mutex poisoned"),
        vec!["hello".to_string()]
    );
}

#[tokio::test]
async fn endpoint_dispatches_actor_ref_envelope_received_over_in_memory_transport() {
    let node_id = NodeId::new("rakka-0", "uid-a");
    let system = rakka_core::ActorSystem::new("remote-actor-ref-delivery");
    let mut registry = SerializationRegistry::new();
    registry
        .register_protobuf::<Ping>("rakka.test.Ping", 1)
        .unwrap();
    let (delivered, mut received) = tokio::sync::mpsc::unbounded_channel();
    let actor = system
        .spawn(
            "worker",
            actor_fn(move |_ctx: &mut ActorContext<Ping>, msg: Ping| {
                let _sent = delivered.send(msg.value);
                Ok(ActorAction::Continue)
            }),
        )
        .unwrap();
    let actor_ref =
        RemoteActorRef::from_serialized(node_id.clone(), &actor.to_serialized_ref()).unwrap();
    let endpoint = RemoteEndpoint::new(node_id.clone());
    endpoint
        .register_actor_ref_handler::<Ping>(RemoteActorRefInbound::<Ping>::new(
            node_id.clone(),
            system.clone(),
            registry.clone(),
        ))
        .unwrap();
    let transport = InMemoryRemoteTransport::new();
    transport.register_endpoint(endpoint).unwrap();
    let envelope = RemoteEnvelope::new(
        RemoteDestination::actor_ref(actor_ref),
        registry
            .encode(&Ping {
                value: "hello actor".to_string(),
            })
            .unwrap(),
    );

    transport.send(&node_id, envelope).unwrap();

    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), received.recv())
            .await
            .unwrap(),
        Some("hello actor".to_string())
    );
    system.terminate().await.unwrap();
}

#[test]
fn endpoint_fails_closed_when_actor_ref_handler_is_missing() {
    let endpoint = RemoteEndpoint::new(NodeId::new("rakka-0", "uid-a"));
    let error = endpoint
        .receive_envelope(RemoteEnvelope::new(
            RemoteDestination::actor_ref(test_remote_actor_ref()),
            EncodedPayload::new(
                RemoteEnvelopeMetadata::protobuf("rakka.test.Ping", 1),
                Vec::new(),
            ),
        ))
        .unwrap_err();

    assert!(matches!(
        error,
        RemoteEndpointError::UnregisteredActorRefHandler { message_type }
            if message_type == type_name::<Ping>()
    ));
}

#[tokio::test]
async fn actor_ref_inbound_rejects_stale_uid_before_delivery() {
    let node_id = NodeId::new("rakka-0", "uid-a");
    let system = rakka_core::ActorSystem::new("remote-stale-uid");
    let registry = ping_registry();
    let (_delivered, mut received, actor) = spawn_recording_ping_actor(&system, "worker");
    let actor_ref = RemoteActorRef::new(
        node_id.clone(),
        actor.to_serialized_ref().system_name(),
        actor.path().clone(),
        ActorUid::new(actor.uid().value() + 1000),
        type_name::<Ping>(),
    )
    .unwrap();
    let endpoint = actor_ref_endpoint(&node_id, &system, &registry);
    let error = endpoint
        .receive_envelope(ping_actor_ref_envelope(actor_ref, &registry, "stale"))
        .unwrap_err();

    assert!(matches!(
        error,
        RemoteEndpointError::HandlerRejected { message, .. }
            if message.contains("remote actor-ref resolve failed")
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(25), received.recv())
            .await
            .is_err()
    );
    system.terminate().await.unwrap();
}

#[tokio::test]
async fn actor_ref_inbound_rejects_wrong_message_type_before_delivery() {
    let node_id = NodeId::new("rakka-0", "uid-a");
    let system = rakka_core::ActorSystem::new("remote-wrong-type");
    let registry = ping_registry();
    let (_delivered, mut received, actor) = spawn_recording_ping_actor(&system, "worker");
    let actor_ref = RemoteActorRef::new(
        node_id.clone(),
        actor.to_serialized_ref().system_name(),
        actor.path().clone(),
        actor.uid(),
        type_name::<Pong>(),
    )
    .unwrap();
    let endpoint = RemoteEndpoint::new(node_id.clone());
    endpoint
        .register_actor_ref_handler::<Pong>(RemoteActorRefInbound::<Pong>::new(
            node_id.clone(),
            system.clone(),
            registry.clone(),
        ))
        .unwrap();
    let error = endpoint
        .receive_envelope(ping_actor_ref_envelope(actor_ref, &registry, "wrong-type"))
        .unwrap_err();

    assert!(matches!(
        error,
        RemoteEndpointError::HandlerRejected { message, .. }
            if message.contains("remote actor-ref resolve failed")
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(25), received.recv())
            .await
            .is_err()
    );
    system.terminate().await.unwrap();
}

#[tokio::test]
async fn actor_ref_inbound_rejects_missing_codec_before_delivery() {
    let node_id = NodeId::new("rakka-0", "uid-a");
    let system = rakka_core::ActorSystem::new("remote-missing-codec");
    let empty_registry = SerializationRegistry::new();
    let encode_registry = ping_registry();
    let (_delivered, mut received, actor) = spawn_recording_ping_actor(&system, "worker");
    let actor_ref =
        RemoteActorRef::from_serialized(node_id.clone(), &actor.to_serialized_ref()).unwrap();
    let endpoint = actor_ref_endpoint(&node_id, &system, &empty_registry);
    let error = endpoint
        .receive_envelope(ping_actor_ref_envelope(
            actor_ref,
            &encode_registry,
            "missing-codec",
        ))
        .unwrap_err();

    assert!(matches!(
        error,
        RemoteEndpointError::HandlerRejected { message, .. }
            if message.contains("remote actor-ref decode failed")
                && message.contains("unknown codec")
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(25), received.recv())
            .await
            .is_err()
    );
    system.terminate().await.unwrap();
}

#[tokio::test]
async fn actor_ref_inbound_rejects_node_mismatch_before_delivery() {
    let node_id = NodeId::new("rakka-0", "uid-a");
    let wrong_node = NodeId::new("rakka-1", "uid-b");
    let system = rakka_core::ActorSystem::new("remote-node-mismatch");
    let registry = ping_registry();
    let (_delivered, mut received, actor) = spawn_recording_ping_actor(&system, "worker");
    let actor_ref =
        RemoteActorRef::from_serialized(wrong_node.clone(), &actor.to_serialized_ref()).unwrap();
    let endpoint = actor_ref_endpoint(&node_id, &system, &registry);
    let error = endpoint
        .receive_envelope(ping_actor_ref_envelope(actor_ref, &registry, "wrong-node"))
        .unwrap_err();

    assert!(matches!(
        error,
        RemoteEndpointError::HandlerRejected { message, .. }
            if message.contains("cannot be handled by local node")
                && message.contains(&wrong_node.to_string())
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(25), received.recv())
            .await
            .is_err()
    );
    system.terminate().await.unwrap();
}

#[tokio::test]
async fn actor_ref_inbound_reports_full_mailbox() {
    let node_id = NodeId::new("rakka-0", "uid-a");
    let system = rakka_core::ActorSystem::new("remote-mailbox-full");
    let registry = ping_registry();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let entered_for_actor = entered.clone();
    let release_for_actor = release.clone();
    let actor = system
        .spawn_actor_with_options(
            "worker",
            move || BlockingPingActor {
                entered: entered_for_actor.clone(),
                release: release_for_actor.clone(),
            },
            ActorOptions::default().with_mailbox_capacity(1),
        )
        .unwrap();
    actor
        .tell(Ping {
            value: "block".to_string(),
        })
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), entered.notified())
        .await
        .unwrap();
    actor
        .tell(Ping {
            value: "queued".to_string(),
        })
        .unwrap();
    let actor_ref =
        RemoteActorRef::from_serialized(node_id.clone(), &actor.to_serialized_ref()).unwrap();
    let endpoint = actor_ref_endpoint(&node_id, &system, &registry);
    let error = endpoint
        .receive_envelope(ping_actor_ref_envelope(actor_ref, &registry, "full"))
        .unwrap_err();

    assert!(matches!(
        error,
        RemoteEndpointError::HandlerRejected { message, .. }
            if message.contains("mailbox was full")
    ));
    release.notify_waiters();
    wait_for_mailbox_depth(&actor, 0).await;
    system.terminate().await.unwrap();
}

#[test]
fn in_memory_transport_reports_unknown_destination_node() {
    let transport = InMemoryRemoteTransport::new();
    let envelope = RemoteEnvelope::new(
        RemoteDestination::Entity {
            entity_type: "cart".to_string(),
            entity_id: "cart-42".to_string(),
        },
        EncodedPayload::new(
            RemoteEnvelopeMetadata::protobuf("rakka.test.Ping", 1),
            Vec::new(),
        ),
    );

    let error = transport
        .send(&NodeId::new("rakka-missing", "uid-z"), envelope)
        .unwrap_err();

    assert!(matches!(
        error,
        RemoteTransportError::UnknownNode { node_id }
            if node_id == NodeId::new("rakka-missing", "uid-z")
    ));
}

#[test]
fn endpoint_fails_closed_for_unhandled_destination_and_entity_type() {
    let endpoint = RemoteEndpoint::new(NodeId::new("rakka-1", "uid-b"));
    let payload = EncodedPayload::new(
        RemoteEnvelopeMetadata::protobuf("rakka.test.Ping", 1),
        Vec::new(),
    );
    let service_error = endpoint
        .receive_envelope(RemoteEnvelope::new(
            RemoteDestination::Service {
                service_key: "payments".to_string(),
            },
            payload.clone(),
        ))
        .unwrap_err();
    assert!(matches!(
        service_error,
        RemoteEndpointError::UnexpectedDestination {
            destination: RemoteDestination::Service { service_key },
        } if service_key == "payments"
    ));

    let entity_error = endpoint
        .receive_envelope(RemoteEnvelope::new(
            RemoteDestination::Entity {
                entity_type: "cart".to_string(),
                entity_id: "cart-42".to_string(),
            },
            payload,
        ))
        .unwrap_err();
    assert!(matches!(
        entity_error,
        RemoteEndpointError::UnregisteredEntityType { entity_type }
            if entity_type == "cart"
    ));
}

#[tokio::test]
async fn request_registry_routes_reply_to_pending_waiter() {
    let mut registry = SerializationRegistry::new();
    registry
        .register_protobuf::<Pong>("rakka.test.Pong", 1)
        .unwrap();
    let requests = RemoteRequestRegistry::new(registry.clone());
    let endpoint = RemoteEndpoint::new(NodeId::new("rakka-0", "uid-a"));
    endpoint.register_reply_handler(requests.clone());
    let pending = requests.register::<Pong>("request-1").unwrap();
    let reply = RemoteEnvelope::new(
        RemoteDestination::Reply {
            request_id: "request-1".to_string(),
        },
        registry
            .encode(&Pong {
                value: "world".to_string(),
            })
            .unwrap(),
    )
    .with_request_id("request-1");

    endpoint.receive_envelope(reply).unwrap();

    let received = pending
        .wait(std::time::Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(received.value, "world");
    assert_eq!(requests.pending_count(), 0);
}

#[tokio::test]
async fn request_registry_timeout_removes_pending_request_and_rejects_late_reply() {
    let mut registry = SerializationRegistry::new();
    registry
        .register_protobuf::<Pong>("rakka.test.Pong", 1)
        .unwrap();
    let requests = RemoteRequestRegistry::new(registry.clone());
    let endpoint = RemoteEndpoint::new(NodeId::new("rakka-0", "uid-a"));
    endpoint.register_reply_handler(requests.clone());
    let pending = requests.register::<Pong>("request-timeout").unwrap();

    let error = pending
        .wait(std::time::Duration::from_millis(1))
        .await
        .unwrap_err();
    assert_eq!(error, RemoteRequestError::Timeout);
    assert_eq!(requests.pending_count(), 0);

    let late_reply = RemoteEnvelope::new(
        RemoteDestination::Reply {
            request_id: "request-timeout".to_string(),
        },
        registry
            .encode(&Pong {
                value: "late".to_string(),
            })
            .unwrap(),
    )
    .with_request_id("request-timeout");
    let error = endpoint.receive_envelope(late_reply).unwrap_err();

    assert!(matches!(
        error,
        RemoteEndpointError::HandlerRejected { message, .. }
            if message.contains("request-timeout") && message.contains("not pending")
    ));
}

#[tokio::test]
async fn request_registry_rejects_duplicate_reply_after_completion() {
    let mut registry = SerializationRegistry::new();
    registry
        .register_protobuf::<Pong>("rakka.test.Pong", 1)
        .unwrap();
    let requests = RemoteRequestRegistry::new(registry.clone());
    let endpoint = RemoteEndpoint::new(NodeId::new("rakka-0", "uid-a"));
    endpoint.register_reply_handler(requests.clone());
    let pending = requests.register::<Pong>("request-duplicate").unwrap();
    let reply = RemoteEnvelope::new(
        RemoteDestination::Reply {
            request_id: "request-duplicate".to_string(),
        },
        registry
            .encode(&Pong {
                value: "first".to_string(),
            })
            .unwrap(),
    )
    .with_request_id("request-duplicate");

    endpoint.receive_envelope(reply.clone()).unwrap();
    assert_eq!(
        pending
            .wait(std::time::Duration::from_secs(1))
            .await
            .unwrap()
            .value,
        "first"
    );

    let error = endpoint.receive_envelope(reply).unwrap_err();
    assert!(matches!(
        error,
        RemoteEndpointError::HandlerRejected { message, .. }
            if message.contains("request-duplicate") && message.contains("not pending")
    ));
}

#[test]
fn endpoint_fails_closed_when_reply_handler_is_missing() {
    let endpoint = RemoteEndpoint::new(NodeId::new("rakka-0", "uid-a"));
    let reply = RemoteEnvelope::new(
        RemoteDestination::Reply {
            request_id: "request-1".to_string(),
        },
        EncodedPayload::new(
            RemoteEnvelopeMetadata::protobuf("rakka.test.Pong", 1),
            Vec::new(),
        ),
    )
    .with_request_id("request-1");

    let error = endpoint.receive_envelope(reply).unwrap_err();

    assert!(matches!(
        error,
        RemoteEndpointError::UnregisteredReplyHandler { request_id }
            if request_id == "request-1"
    ));
}

fn test_remote_actor_ref() -> RemoteActorRef {
    RemoteActorRef::new(
        NodeId::new("rakka-0", "uid-a"),
        "orders",
        ActorPath::new("rakka://local/orders/user/worker"),
        ActorUid::new(42),
        type_name::<Ping>(),
    )
    .expect("remote actor ref should be valid")
}

fn ping_registry() -> SerializationRegistry {
    let mut registry = SerializationRegistry::new();
    registry
        .register_protobuf::<Ping>("rakka.test.Ping", 1)
        .unwrap();
    registry
}

fn actor_ref_endpoint(
    node_id: &NodeId,
    system: &rakka_core::ActorSystem,
    registry: &SerializationRegistry,
) -> RemoteEndpoint {
    let endpoint = RemoteEndpoint::new(node_id.clone());
    endpoint
        .register_actor_ref_handler::<Ping>(RemoteActorRefInbound::<Ping>::new(
            node_id.clone(),
            system.clone(),
            registry.clone(),
        ))
        .unwrap();
    endpoint
}

fn ping_actor_ref_envelope(
    actor_ref: RemoteActorRef,
    registry: &SerializationRegistry,
    value: &str,
) -> RemoteEnvelope {
    RemoteEnvelope::new(
        RemoteDestination::actor_ref(actor_ref),
        registry
            .encode(&Ping {
                value: value.to_string(),
            })
            .unwrap(),
    )
}

fn spawn_recording_ping_actor(
    system: &rakka_core::ActorSystem,
    name: &str,
) -> (
    tokio::sync::mpsc::UnboundedSender<String>,
    tokio::sync::mpsc::UnboundedReceiver<String>,
    rakka_core::ActorRef<Ping>,
) {
    let (delivered, received) = tokio::sync::mpsc::unbounded_channel();
    let actor = system
        .spawn(
            name,
            actor_fn({
                let delivered = delivered.clone();
                move |_ctx: &mut ActorContext<Ping>, msg: Ping| {
                    let _sent = delivered.send(msg.value);
                    Ok(ActorAction::Continue)
                }
            }),
        )
        .unwrap();
    (delivered, received, actor)
}

struct BlockingPingActor {
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

impl Actor for BlockingPingActor {
    type Msg = Ping;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        let entered = self.entered.clone();
        let release = self.release.clone();
        actor_future(async move {
            if msg.value == "block" {
                entered.notify_waiters();
                release.notified().await;
            }
            Ok(ActorAction::Continue)
        })
    }
}

async fn wait_for_mailbox_depth<M>(actor: &rakka_core::ActorRef<M>, expected: usize)
where
    M: rakka_core::Message,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    loop {
        if actor.mailbox_depth() == expected {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "mailbox depth stayed at {}, expected {expected}",
            actor.mailbox_depth()
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}
