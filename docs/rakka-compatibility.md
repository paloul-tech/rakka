# Rakka Compatibility Policy

This document defines the v1 compatibility rules for cluster protocol admission and remote Protobuf message schemas.

## Cluster Protocol

Rakka nodes advertise a `ClusterProtocol` with a concrete `ProtocolVersion` and an inclusive `CompatibilityRange`.

- Nodes may join only when compatibility is mutual: each node's range must include the other node's version.
- Kubernetes rolling updates should use an N/N+1 minor-version window, for example `1.0..=1.1`.
- Incompatible protocol changes should use an exact policy and must not allow mixed-version membership.
- Major-version changes are treated as incompatible unless a dedicated compatibility bridge is implemented.

The helper APIs are:

- `CompatibilityRange::n_to_n_plus_one(major, minor)` for rolling-update windows.
- `ClusterProtocol::n_to_n_plus_one(version, base_minor)` for nodes participating in that window.
- `CompatibilityRange::exact(version)` and `ClusterProtocol::exact(version)` for incompatible migrations.

## Remote Protobuf Schemas

Remote envelopes carry `codec_id`, `message_type_id`, and `schema_version`. Protobuf is the default codec, but schema compatibility is still explicit.

- During rolling updates, application message changes must be additive inside the active schema window.
- Additive changes may add new fields with new tags. Existing field numbers and meanings must remain stable.
- Unknown fields may be ignored by older nodes, and missing new fields must have safe defaults on newer nodes.
- Removed fields, changed field types, reused field numbers, changed semantic meaning, or changed required interpretation are incompatible.
- Incompatible migrations must use an exact schema policy, a staged drain, a bridge, or a separate cluster.
- Unsupported schema versions fail closed with typed `RemoteError::UnknownCodec`.

The helper APIs are:

- `SchemaCompatibilityPolicy::n_plus_one(current_schema_version)` for standard old/current schema coexistence.
- `SchemaCompatibilityPolicy::additive_window(min, max)` for explicit additive compatibility windows.
- `SchemaCompatibilityPolicy::exact(schema_version)` for incompatible migrations.
- `SerializationRegistry::register_protobuf_compatible::<M>(type_id, current_version, policy)` to encode with the current version while accepting every schema version in the policy window.

Rakka does not automatically diff Protobuf descriptors in v1. Schema compatibility is enforced by the explicit registry policy and by tests owned by the application.
