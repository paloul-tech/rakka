# Rakka Phase 3 Continuation Plan

## Purpose

This file is the working slice outline for finishing Phase 3: remote, cluster, and sharding. The larger v1 plan defines the destination; this file defines the order of the remaining slices and the acceptance criteria for each one.

Working note: as we move forward, follow this slice outline from this file. Before starting a Phase 3 slice, read this file, implement only the next agreed slice, then update this file if scope or status changes.

## Current Baseline

The following Phase 3 foundations are already in place:

- `rakka-remote` has remote envelopes, destination metadata, Protobuf envelope encoding, Protobuf payload codecs, and a pluggable serialization registry.
- `rakka-cluster` has node identity, protocol compatibility advertisement, static/local discovery, membership state, heartbeat/failure detection, graceful leave state, and compatibility checks.
- `rakka-k8s` has Kubernetes DNS discovery foundations for headless service pod discovery.
- `rakka-sharding` has entity identity, shard id hashing, owner cache, shard coordinator, rebalance/failover decisions, `ShardRegion`, `EntityRef<M>`, and local entity spawning.
- Remote-aware entity routing now covers both directions at the envelope boundary:
  - Outbound: `LocalEntityRoute` returning `NotLocal` can be wrapped by `RemoteEntityRoute` and sent as a `rakka-remote` envelope.
  - Inbound: `RemoteEntityInbound` can validate `RemoteDestination::Entity`, decode the payload, reconstruct `EntityRef<M>`, and deliver through the local `ShardRegion`.

## Phase 3 Done Definition

Phase 3 should be considered complete when Rakka can demonstrate end-to-end entity routing across simulated cluster nodes, including membership-driven ownership, remote transport dispatch, graceful ownership movement, compatibility checks, and local entity lifecycle behavior.

Minimum completion criteria:

- A concrete test transport can move encoded `RemoteEnvelope` messages between logical nodes.
- A remote endpoint can dispatch inbound envelopes by destination kind, including sharded entities.
- A multi-node test proves `EntityRef<M>::tell` routes from one node to the owning node and reaches the local entity actor.
- Remote `ask` has request ids, reply routing, timeout behavior, and reply cleanup.
- Shard ownership changes can be applied to running regions and tested across join, leave, and failure scenarios.
- Graceful shard handoff has an explicit runtime protocol, not only coordinator decisions.
- Entity passivation removes idle local actors and recreates them on the next message.
- N/N+1 compatibility is enforced at node admission and covered by remote message/schema tests.

## Slice 3A: In-Memory Remote Transport and Endpoint Router

Goal: turn the outbound/inbound envelope boundary into a working end-to-end remote entity path inside tests.

Scope:

- Add a remote transport abstraction for sending encoded or structured `RemoteEnvelope` messages to a `NodeId`.
- Add an in-memory transport implementation suitable for deterministic multi-node tests.
- Add a remote endpoint/router that can register destination handlers.
- Register entity inbound handlers by entity type.
- Connect `RemoteEntityRoute` outbound sends to the in-memory transport.
- Decode/dispatch inbound envelopes through `RemoteEntityInbound`.

Acceptance criteria:

- Unknown destination node returns a typed remote send error.
- Unknown destination kind or unregistered entity type fails closed with a typed endpoint error.
- A two-node integration test sends an entity message from node A to an entity owned by node B.
- The test proves node A does not spawn the remote entity locally and node B does spawn and receive the message.
- Existing focused and workspace tests remain green.

Out of scope:

- TCP, HTTP/2, TLS, mTLS, or Kubernetes networking.
- Remote `ask` reply routing.
- Real cluster gossip or bootstrap loops.

Review commands:

```bash
cargo fmt --all -- --check
cargo test -p rakka-remote
cargo test -p rakka-sharding
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

## Slice 3B: Remote Ask and Reply Correlation

Goal: make remote `ask` work across nodes instead of only remote `tell`.

Scope:

- Use `RemoteEnvelope::request_id` for ask correlation.
- Define how reply envelopes address the original requester.
- Add a pending request registry with timeout cleanup.
- Route decoded replies back to the original `ReplyTo`/oneshot waiter.
- Ensure late replies are observable and safely dropped.

Acceptance criteria:

- Remote ask from node A to entity on node B returns a reply.
- Timeout removes pending request state.
- Late or duplicate replies do not panic and are reported as dropped/unknown.
- Missing reply route fails with a typed error.

Out of scope:

- Durable ask/retry semantics.
- Workflow reliability; that remains a `rakka-workflow` concern.

## Slice 3C: Cluster Runtime Loop and Region Ownership Refresh

Goal: connect discovery, membership, coordinator reconciliation, and shard-region owner refresh into a small runtime surface.

Scope:

- Add a cluster/sharding runtime facade that periodically applies discovery snapshots.
- Reconcile shard ownership after membership changes.
- Publish ownership snapshots to registered shard regions.
- Add deterministic tests for join, leave, and failure-triggered refreshes.

Acceptance criteria:

- A joining node causes ownership to rebalance and regions observe a new revision.
- A leaving node causes affected shards to move away from it.
- A down/unreachable node causes failover decisions and region cache refresh.
- Incompatible nodes are rejected and do not receive shard ownership.

Out of scope:

- Full distributed consensus.
- Split-brain resolution beyond fail-closed/local deterministic foundations.

## Slice 3D: Graceful Shard Handoff Protocol

Goal: make shard movement an explicit runtime flow instead of only a coordinator decision.

Scope:

- Model shard handoff states: owning, draining, transferring, acquired.
- Prevent new local activations while a shard is draining.
- Decide whether in-flight messages are rejected, forwarded, or temporarily buffered for v1.
- Stop/passivate local entities after handoff completes.
- Apply handoff during graceful leave.

Acceptance criteria:

- Graceful leave triggers shard drain before ownership moves.
- Messages to a draining shard have deterministic behavior.
- After handoff, the old owner no longer hosts entities for the shard.
- The new owner can activate entities for the shard.

Out of scope:

- Exactly-once handoff.
- Durable inbox/outbox integration.

## Slice 3E: Entity Passivation

Goal: allow idle local sharded entities to stop and be recreated on demand.

Scope:

- Add local entity registry removal when an entity stops.
- Add explicit passivation command/API.
- Add idle timeout passivation using Tokio timers.
- Ensure next message recreates the entity actor.

Acceptance criteria:

- An entity can passivate itself or be passivated by the route.
- The local route removes terminated/passivated actors from its registry.
- A later message respawns the entity with the same identity context.
- Passivation does not affect entities on unrelated shards.

Out of scope:

- Persisted passivation state.
- Cluster-wide passivation policies.

## Slice 3F: Compatibility Hardening

Goal: make rolling-update compatibility enforceable and testable beyond node admission.

Scope:

- Add compatibility tests for N/N+1 cluster node protocol admission.
- Add remote message schema compatibility tests for accepted and rejected schema versions.
- Define policy helpers for additive schema windows and incompatible migrations.
- Document the expected app-level Protobuf evolution rules.

Acceptance criteria:

- Compatible N and N+1 nodes can coexist.
- Incompatible nodes are rejected before membership admission.
- Unsupported remote message schema versions fail closed with typed errors.
- Compatibility rules are documented in the spec or implementation docs.

Out of scope:

- Automated Protobuf schema diffing.
- Cross-language compatibility tooling.

## Slice 3G: Phase 3 Examples and Documentation

Goal: make the Phase 3 behavior reviewable by humans, not only tests.

Scope:

- Add a minimal multi-node in-memory cluster example.
- Document the remote entity routing flow.
- Document which Phase 3 pieces are production-ready foundations and which are test-only scaffolding.
- Update this continuation plan with final status.

Acceptance criteria:

- Example runs with `cargo run`.
- README or docs explain remote entity routing in the current architecture.
- Phase 3 completion status is clear.

## Suggested Next Slice

Start with Slice 3A: in-memory remote transport and endpoint router. It is the shortest path from the current envelope boundary to a real end-to-end remote entity routing test.
