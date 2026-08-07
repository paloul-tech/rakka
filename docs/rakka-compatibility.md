# Rakka Compatibility Policy

This document defines the v1 compatibility rules for Rakka rolling updates. The goal is narrow and explicit: a Kubernetes cluster running release N must be able to roll to release N+1 while routing supported messages, and incompatible nodes must fail closed before they acquire shard ownership.

## Compatibility Dimensions

The v1 compatibility matrix tracks these dimensions together:

| Dimension | Contract |
| --- | --- |
| Crate version | Release artifacts should identify the Rakka crate/package version used by the node. |
| Cluster protocol version | Nodes advertise `ClusterProtocol { version, compatible_with }`; membership admission requires mutual compatibility. |
| Remote envelope version | Network remoting handshakes include the remote envelope wire version. |
| Message schema version | Remote payloads carry `codec_id`, `message_type_id`, and `schema_version`; Protobuf is the default payload codec. |
| Kubernetes manifest version | Manifests carry compatibility metadata through env vars and `rakka.rs/*` annotations. |
| Generated API version | HTTP and gRPC adapter contracts expose stable generated/public API version constants. |

The shared fixture lives in `rakka_testkit::compatibility::V1CompatibilityFixture`.

## Allowed Skew

Rakka v1 supports one rolling-update window at a time:

- `N` and `N+1` minor versions may coexist when both nodes advertise a mutual compatibility range, for example protocol `1.0` with range `1.0..=1.1`.
- `N+2`, older-than-N, and cross-major nodes are incompatible unless a dedicated bridge is implemented.
- Incompatible nodes must fail membership admission, fail Kubernetes readiness through `compatibility-not-accepted`, and must not receive shard ownership.
- Message schemas inside a rolling update must be additive. Exact schema policy is reserved for incompatible migrations that require a staged drain, bridge, or separate cluster.

Rakka does not promise arbitrary multi-version clusters in v1.

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

## A2A Adapter Surface

`rakka-a2a` carries several compatibility commitments beyond the remoting envelope:

- The owner remote protocol is versioned. The message type ids `rakka.a2a.A2ARunRequest` and `rakka.a2a.A2ARunResponse`, the JSON codec id `rakka-a2a-json`, the remote schema version, and the protocol version (`A2A_RUN_PROTOCOL_VERSION`) must agree across adjacent node versions; a mismatched protocol version fails closed with a `version-mismatch` owner failure, and an unknown type id or schema version fails closed with `RemoteError::UnknownCodec`.
- The `io.rakka.*` A2A metadata keys (workflow selection, command id/kind, dedup key, causation/correlation, principal, trace context, projection revision, replay cursor, redaction) are public compatibility commitments consumed by clients.
- The agent-collaboration extension is versioned by URI (`urn:rakka:a2a-extension:collaboration:v1`) with the envelope schema number inside it, and its `io.rakka.collaboration` metadata key is a public compatibility commitment. A message that engages the extension half-formed — an unserved version, the declaration without the metadata object, the reserved key without the declaration, or an envelope that does not parse under the served schema — fails closed as `unsupported-operation`; a client that never engages it is untouched. The stable delegation refusal codes (`delegation-skill-unknown`, `delegation-skill-not-authorized`, `delegation-target-ambiguous`, `delegation-skill-not-allowed`, `delegation-invalid-arguments`, `delegation-limit-exceeded`, `delegation-child-conflict`, `delegation-child-mismatch`, `collaboration-version-unsupported`, `a2a-send-executor-missing`, `coordination-tool-not-intercepted`, `goal-tool-not-allowed`) are compatibility commitments surfaced in tool results, effect records, and A2A errors. The envelope carries logical credential-binding references at most, never resolved credentials.
- The replay cursor shape `<task-id>:<sequence>` and the stable mapping/projection/handler error code strings are compatibility commitments surfaced in A2A errors and bounded metrics.
- The PostgreSQL A2A schema evolves additively within a release: new tables, new nullable/defaulted columns, and new indexes only, applied by an idempotent, advisory-lock-guarded migration so N and N+1 pods share the schema during rolling updates.

## Kubernetes Rolling Updates

Recommended sequence:

1. Release N with a declared N/N+1 protocol window, additive schema policy, and manifest/API metadata.
2. Build release N+1 without removing fields, changing field meanings, or changing required defaults used by N.
3. Deploy N+1 through a Kubernetes rolling update with readiness gates enabled.
4. Let incompatible nodes fail readiness instead of joining the cluster or acquiring shards.
5. Drain and replace pods one at a time, respecting the PodDisruptionBudget.
6. After every pod is on N+1, the next release may advance the window to N+1/N+2.

The example manifest includes:

- `RAKKA_PROTOCOL_VERSION`
- `RAKKA_COMPAT_MIN`
- `RAKKA_COMPAT_MAX`
- `RAKKA_COMPAT_POLICY`
- `RAKKA_MANIFEST_VERSION`
- `RAKKA_GENERATED_API_VERSION`
- `rakka.rs/protocol-version`
- `rakka.rs/compatible-min`
- `rakka.rs/compatible-max`
- `rakka.rs/manifest-version`
- `rakka.rs/generated-api-version`

## Observability

Compatibility failures must be observable through multiple surfaces:

- Typed errors: incompatible discovery returns `ClusterError::IncompatibleNode`, wrapped as `ClusterShardingError::Cluster` at the sharding runtime boundary.
- Readiness: Kubernetes readiness includes `compatibility-not-accepted`.
- Metrics: `rakka.k8s.compatibility` records `state=accepted|rejected`, and `rakka.k8s.readiness` records `outcome=ready|not-ready`.
- Remote delivery: unsupported schemas fail through the serialization registry before delivery.

## Tests

Run the default compatibility matrix:

```sh
cargo test -p rakka-testkit --test compatibility_matrix -- --nocapture
```

Run the Kubernetes manifest contract tests:

```sh
cargo test -p rakka-k8s --test kubernetes_manifests
```

Run the gated multi-process loopback compatibility check:

```sh
RAKKA_RUN_MULTI_PROCESS_COMPATIBILITY=1 cargo test -p rakka-testkit --test compatibility_matrix optional_multi_process_compatibility_example_is_gated -- --nocapture
```
