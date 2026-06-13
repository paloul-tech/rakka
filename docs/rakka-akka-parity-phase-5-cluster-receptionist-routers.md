# Rakka Akka Parity Phase 5: Cluster, Receptionist, And Routers

Status: implemented, with TCP clustered receptionist propagation still tracked
as a follow-up.
Date: 2026-06-13

Phase 5 adds the Akka-style cluster extension, typed receptionist, local
routers, clustered receptionist propagation, and reusable testkit helpers. The
goal is a smaller first-path API for service discovery and routing without
requiring applications to wire low-level membership, route, or coordinator
types.

## What To Use

Use the cluster extension when code needs membership lifecycle:

```rust
let cluster = Cluster::get(&system);
cluster.manager().join_self()?;
let state = cluster.state();
```

Use the receptionist when actors are interchangeable service instances:

```rust
let key = ServiceKey::<WorkerCommand>::new("workers");
let receptionist = Receptionist::get(&system);
let _registration = receptionist.register(&key, worker)?;
let listing = receptionist.find(&key)?;
```

Use routers when a caller should send to one routee from a set:

```rust
let pool = Routers::pool("worker", 4, Worker::new)
    .with_round_robin()
    .spawn(&system)?;

let group = Routers::group(key)
    .with_round_robin()
    .spawn(&system, "worker-group")?;
```

Use sharding when the key is durable entity identity, when messages must route
to the current owner of an entity, or when passivation, handoff buffering,
coordinator durability, remembered entities, and recovery after movement are
part of the correctness story.

## Cluster Extension

`Cluster` is the facade for membership state and events. It supports:

- `Cluster::get(&system)` for a local cluster facade.
- `Cluster::for_local_node(node, config)` for explicit node identity.
- `cluster.manager().join_self()`, `join`, `join_seed_nodes`, `leave`, and
  `down`.
- `cluster.state()` and `cluster.self_member()`.
- `cluster.subscriptions().subscribe(...)` with current-state, initial-event,
  or live-only replay.

`ClusterRuntime` keeps runtime hooks explicit. Discovery polling, failure
detection, and downing are driven by application or platform loops instead of
hidden background IO. The default failure detector and downing strategy are
timeout based, and callers can provide their own `FailureDetector` and
`DowningStrategy` implementations.

## Local Receptionist

`Receptionist` stores typed service registrations behind `ServiceKey<M>`.
Registration returns a drop-safe lease, explicit deregistration is available,
and actor termination removes the service from future listings.

Subscriptions deliver the initial listing and future changes:

```rust
let mut subscription = receptionist.subscribe(&key)?;
let listing = subscription.recv().await?;
```

`Listing<M>` includes the service key, routees, a monotonic revision, `len`,
`is_empty`, and `contains` helpers. The revision is used by clustered
receptionist propagation to reject stale updates.

## Pool Routers

Pool routers spawn their own local routees:

- `with_round_robin()` for deterministic fan-out.
- `with_random()` for pseudo-random live routee selection.
- `with_consistent_hash(|message| key)` for key-sticky routing.
- `with_consistent_hash_virtual_nodes(count)` for distribution tuning.
- `with_spawn_options(options)` for routee mailbox, supervision, dispatcher,
  and instrumentation settings.

Pool router sends are explicit and message-preserving. No-routee, full-mailbox,
and closed-routee errors return the original message.

## Group Routers

Group routers route over receptionist listings instead of spawning routees.
They refresh synchronously before sends and keep a background receptionist
subscription for normal listing changes.

Group routers support the same round-robin, random, and consistent-hash
strategies as pool routers. They also expose `with_fail_fast_no_routees()` and
`with_drop_when_no_routees()` so empty-listing behavior is explicit.

## Clustered Receptionist

`ClusteredReceptionist` publishes local-only listings and applies listings from
other cluster members. The deterministic API is intentionally direct:

```rust
let source = ClusteredReceptionist::get(&system_a, cluster_a);
let destination = ClusteredReceptionist::get(&system_b, cluster_b);
source.propagate_to(&destination, &key, 1)?;
```

The propagated listing model includes:

- enabled/disabled settings;
- publication interval metadata;
- remote listing TTL;
- optional maximum routees per listing;
- source-node and service-key scoping;
- stale source-revision rejection;
- non-`Up` source pruning.

Today, clustered receptionist propagation is production-shaped but
deterministic and in-process. TCP propagation remains pending because remote
service routees need a transport-serializable service-reference representation
instead of cloned in-memory `ActorRef<M>` values.

## Testkit Helpers

`rakka-testkit` now includes reusable Phase 5 assertions:

- `assert_receptionist_listing_count`
- `assert_receptionist_listing_contains`
- `expect_receptionist_listing_count`
- `assert_pool_routee_count`
- `assert_group_routee_count`
- `assert_group_router_snapshot_routee_count`
- `expect_cluster_event`
- `expect_cluster_event_matching`
- `expect_cluster_member_up`
- `assert_cluster_event_node`

These helpers keep tests focused on behavior rather than repeated subscription
timeout and listing boilerplate.

## Examples

Run the Phase 5 examples from the workspace root:

```bash
cargo run -p rakka-example-local-receptionist-router
cargo run -p rakka-example-pool-router
cargo run -p rakka-example-clustered-receptionist
```

Use `local-receptionist-router` to review service registration and
receptionist-backed group routing. Use `pool-router` to review local worker
farm routing. Use `clustered-receptionist` to review deterministic two-node
listing propagation and routing over a propagated remote listing.
