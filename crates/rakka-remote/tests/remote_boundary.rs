//! Integration tests for the remote envelope and serialization boundary.

use prost::Message;
use rakka_cluster::NodeId;
use rakka_remote::{
    EncodedPayload, InMemoryRemoteTransport, ProtobufEnvelopeCodec, RemoteDestination,
    RemoteEndpoint, RemoteEndpointError, RemoteEnvelope, RemoteEnvelopeMetadata, RemoteError,
    RemoteTransport, RemoteTransportError, SerializationRegistry,
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
