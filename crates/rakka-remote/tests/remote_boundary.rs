//! Integration tests for the remote envelope and serialization boundary.

use prost::Message;
use rakka_cluster::NodeId;
use rakka_remote::{
    EncodedPayload, InMemoryRemoteTransport, ProtobufEnvelopeCodec, RemoteDestination,
    RemoteEndpoint, RemoteEndpointError, RemoteEnvelope, RemoteEnvelopeMetadata, RemoteError,
    RemoteRequestError, RemoteRequestRegistry, RemoteTransport, RemoteTransportError,
    SchemaCompatibilityPolicy, SerializationRegistry, TcpRemoteTransportConfig,
    DEFAULT_REMOTE_ENVELOPE_VERSION, DEFAULT_TCP_REMOTE_BIND_ADDR,
    DEFAULT_TCP_REMOTE_CONNECT_TIMEOUT, DEFAULT_TCP_REMOTE_IDLE_TIMEOUT,
    DEFAULT_TCP_REMOTE_MAX_FRAME_BYTES, DEFAULT_TCP_REMOTE_OUTBOUND_QUEUE_CAPACITY,
    DEFAULT_TCP_REMOTE_RECONNECT_BACKOFF, TCP_REMOTE_REQUIRES_REGISTERED_PEERS,
};
use std::sync::{Arc, Mutex};

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
