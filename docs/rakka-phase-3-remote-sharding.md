# Rakka Phase 3 Remote Sharding

This document explains the current Phase 3 remote entity routing path and the boundary between production-ready foundations and deterministic test scaffolding.

## Remote Entity Routing Flow

The current remote sharding path follows the same shape as Akka Cluster Sharding, but keeps each boundary explicit and typed in Rust.

1. Application code holds an `EntityRef<M>` for an entity type and entity id.
2. `ShardRegion<M>` computes the entity shard id and resolves the shard owner from its `ShardOwnerCache`.
3. `LocalEntityRoute` accepts the message only when the resolved owner is the local node. Otherwise it returns `EntityDeliveryFailure::NotLocal { owner }`.
4. `RemoteEntityRoute` wraps the local route. When local delivery reports `NotLocal`, it encodes the typed message through `SerializationRegistry` and builds a `RemoteEnvelope` with `RemoteDestination::Entity`.
5. `RemoteTransportEntityOutbound` sends the envelope to the owning `NodeId` through a `RemoteTransport`.
6. `RemoteEndpoint` receives encoded bytes, decodes the envelope, and dispatches by `RemoteDestination`.
7. `RemoteEntityInbound` validates the entity type, decodes the Protobuf payload back to `M`, reconstructs the typed `EntityRef<M>`, and hands it to the owning node's local `ShardRegion`.
8. The owning `LocalEntityRoute` starts or reuses the local entity actor and delivers the message.

The runnable example in `examples/multi-node-sharding` demonstrates this path inside one process with two logical nodes and an in-memory remote transport:

```bash
cargo run -p rakka-example-multi-node-sharding
```

Expected output includes:

```text
Rakka multi-node sharding routed add-apple to cart-N on rakka-1#uid-b.
node-a local entity count: 0
node-b local entity count: 1
```

The specific entity id may vary if the shard hashing configuration changes, but node A should not spawn the remote entity locally and node B should receive it.

The same example also has V1 hardening modes that exercise real Tokio TCP remoting:

```bash
cargo run -p rakka-example-multi-node-sharding -- --networked-loopback
cargo run -p rakka-example-multi-node-sharding -- --networked-processes
```

`--networked-loopback` runs two networked node runtimes in one process. `--networked-processes` launches two child Rakka node processes on loopback ports and routes a sharded entity message from node A to node B over TCP.

## Ownership Refresh

`ClusterShardingRuntime` is the local runtime facade that connects discovery snapshots, membership transitions, coordinator reconciliation, and registered shard regions.

- Discovery updates feed `ClusterMembership`.
- Membership state decides which nodes are routable.
- `ShardCoordinator` computes deterministic shard ownership.
- Registered regions refresh their owner caches from the coordinator snapshot.
- Graceful leave can begin shard handoff, reject new local activations while draining, complete ownership transfer, and let the new owner acquire the shard.

This establishes the internal coordination model for Rakka-owned clustering. It is intentionally deterministic so the semantics can be tested before adding distributed consensus and production networking.

Phase 4D adds durable coordinator stores and recovery-after-movement examples on
top of this model. See
`docs/rakka-akka-parity-phase-4d6-recovery-after-movement.md` for the sharded
cart example that writes event-sourced state on node A, moves the owning shard
to node B, and proves node B recovers the entity by `PersistenceId`.

## Durability and Lifecycle

Phase 3 sharding is compatible with the durable-state foundation from Phase 2, but it does not force every entity to be durable. Entity actors are normal typed actors; an application can choose a durable actor implementation for stateful entities.

Coordinator durability and entity persistence remain separate concerns:
coordinator stores recover shard ownership decisions, while typed persistence
stores recover the entity's domain state.

Local entity lifecycle support now includes:

- activation on first message;
- explicit passivation;
- idle timeout passivation;
- actor registry cleanup after termination;
- recreation on the next routed message.

Core actor delivery remains at-most-once. Durable inbox/outbox, retries, deduplication, and workflow-level reliability belong in separate modules.

## Compatibility

Cluster admission and remote payload decoding fail closed by default.

- Cluster nodes advertise `ClusterProtocol` with mutual compatibility checks.
- The default v1 policy permits N/N+1 minor-version coexistence during Kubernetes rolling updates.
- Remote Protobuf payloads carry `codec_id`, `message_type_id`, and `schema_version`.
- `SerializationRegistry` can accept exact schemas or additive compatibility windows.

See `docs/rakka-compatibility.md` for the detailed compatibility policy.

## Production Foundations

The following Phase 3 pieces are usable as production-oriented foundations:

- typed actor boundaries and local actor lifecycle;
- cluster node identity, address, roles, discovery snapshots, and membership state machine;
- mutual cluster protocol compatibility checks;
- Protobuf remote envelope and payload codec registry;
- typed remote endpoint errors and fail-closed dispatch;
- deterministic shard identity, owner cache, coordinator, and rebalance decisions;
- `ShardRegion`, `EntityRef<M>`, local entity activation, passivation, and handoff states.

## Test Scaffolding and Known Gaps

The following pieces are intentionally deterministic scaffolding or incomplete production surfaces:

- `InMemoryRemoteTransport` is for deterministic tests and local examples.
- `TcpRemoteTransport` provides the current Tokio TCP remoting foundation for local multi-process and Kubernetes pod-to-pod hardening.
- The default multi-node example still runs multiple logical nodes in one process; use `--networked-processes` for a local multi-process TCP demonstration.
- There is no HTTP/2, TLS, mTLS, or QUIC transport yet.
- There is no full distributed consensus, gossip, or split-brain resolution yet.
- Kubernetes DNS discovery exists as a foundation, but the full production runtime loop still needs deployment hardening.
- Graceful handoff is modeled locally and deterministically; distributed acknowledgement and retry behavior remain future work.
- Exactly-once delivery, durable inbox/outbox, retries, and deduplication are out of scope for core actors.

## Phase 3 Completion Status

Phase 3 is complete as a foundation slice: Rakka can demonstrate end-to-end sharded entity routing across simulated cluster nodes, membership-driven ownership, in-memory remote dispatch, remote ask/reply correlation, ownership refresh, graceful shard handoff, entity passivation, and compatibility hardening.

The next major work should move from deterministic foundations to productionization: real remote transport, Kubernetes runtime integration, distributed coordination hardening, and end-to-end examples that combine sharding with durable state.
