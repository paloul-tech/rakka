//! Integration tests for the remote envelope and serialization boundary.

use prost::Message;
use rakka_remote::{
    EncodedPayload, ProtobufEnvelopeCodec, RemoteDestination, RemoteEnvelope,
    RemoteEnvelopeMetadata, RemoteError, SerializationRegistry,
};

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
