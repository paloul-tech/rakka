# Rakka Akka Parity Phase 5 Detailed Plan

Status: In progress; Slices 5A, 5B, 5C, 5D, and 5E implemented; 5F deterministic propagation implemented
Date: 2026-06-12

## Purpose

This plan expands Phase 5 from `docs/plans/rakka-akka-parity-implementation-plan.md`
into implementation slices for Akka-parity cluster ergonomics:

- `Cluster` extension facade.
- Typed local and clustered receptionist.
- Pool and group routers.
- Optional consistent-hash routing.

This plan is separate from the older v1 Phase 5 continuation plan, which covered
streams, HTTP/gRPC, and Kubernetes operations. The scope here is Akka Typed
parity for cluster extension, receptionist, and routers.

## Evaluation

The current Phase 5 description is directionally correct, but too compressed for
implementation. It combines three large user-facing surfaces:

- a cluster extension API over the current membership and node-runtime
  foundations;
- a typed receptionist for local and eventually cluster-wide service discovery;
- typed routers over local pools and receptionist-backed service groups.

The dependency order should be:

1. Cluster facade and event subscription model.
2. Runtime/discovery/failure hooks behind the facade.
3. Local receptionist.
4. Local pool routers.
5. Local receptionist-backed group routers.
6. Clustered receptionist propagation.
7. Clustered group routers and optional consistent-hash routing.

Group routers need receptionist listings. Clustered group routers need clustered
receptionist propagation. Consistent-hash routing should stay behind the simpler
pool/group routing APIs until those are stable.

## Target Outcome

Rakka should support Akka-like application code without exposing low-level
membership, route, or registry internals:

```rust
let cluster = Cluster::get(&system);
cluster.manager().join_seed_nodes(seed_nodes).await?;

let receptionist = Receptionist::get(&system);
let workers = ServiceKey::<WorkCommand>::new("workers");
receptionist.register(&workers, worker_ref.clone()).await?;

let router = Routers::group(workers.clone())
    .with_round_robin()
    .spawn(&system, "worker-router")?;

router.tell(WorkCommand::Process(job))?;
```

The final API does not need to match this sketch exactly. The important outcome
is that users can work through compact cluster, receptionist, and router
facades while foundation crates keep the explicit Rust-native contracts.

## Non-goals

- Full Akka Distributed Data.
- Cluster singleton.
- Distributed pub-sub.
- Reliable delivery.
- Replicated event sourcing.
- Transparent remote deployment of arbitrary closures.
- General service mesh behavior.
- HTTP/gRPC/Kubernetes work from the older v1 Phase 5 plan.

## Guiding Decisions

- Keep `ClusterMembership` as the internal membership truth, but expose mutation
  through `ClusterManager`.
- Make cluster event replay explicit so subscribers can choose initial snapshot,
  initial event replay, or live-only mode.
- Keep local receptionist semantics correct before clustering them.
- Keep sharding and receptionist separate:
  - sharding owns stateful entity identity and persistence uniqueness;
  - receptionist owns dynamic stateless service discovery.
- Routers should preserve message ownership on fail-fast errors whenever the
  message type can be returned.
- Add clustered receptionist propagation only after routee cleanup and local
  subscription semantics are proven.
- Use deterministic in-memory tests before TCP or Kubernetes demonstrations.

## Slice 5A: Cluster Extension Facade

Goal: expose a small Akka-like `Cluster` API over `rakka-cluster` and existing
node-runtime foundations.

Status: implemented.

Scope:

- Add a cluster extension type, likely in `rakka-cluster`.
- Add top-level facade re-exports through `rakka::cluster` and curated prelude
  entries only if the surface is stable enough.
- Add `Cluster::get(&system)` for local defaults.
- Add explicit constructors for configured local nodes and membership settings.
- Add `ClusterManager` with:
  - `join`;
  - `join_seed_nodes`;
  - `leave`;
  - `down`.
- Add query APIs:
  - `cluster.state()`;
  - `cluster.self_member()`;
  - `cluster.members()`;
  - `cluster.is_terminated()` or equivalent lifecycle view if useful.
- Add `ClusterState` and `SelfMember` snapshots that are stable for tests and
  docs.
- Add `ClusterEvent` covering discovered, up, leaving, unreachable, reachable,
  down, removed, and local-state changes.
- Add `ClusterSubscriptions` with replay modes:
  - initial full state snapshot;
  - initial event replay;
  - live-only subscription.

Acceptance criteria:

- Users can drive local membership transitions without touching
  `ClusterMembership` directly.
- Subscription replay order is deterministic.
- Invalid transitions return typed errors.
- The facade can be used by future sharding and receptionist integrations
  without creating a dependency cycle.

Implementation status:

- Added `Cluster`, `ClusterManager`, `ClusterSubscriptions`,
  `ClusterSubscription`, `ClusterSubscriptionReplay`, `ClusterState`,
  `SelfMember`, `ClusterUpdate`, and `ClusterEvent` in `rakka-cluster`.
- Added `Cluster::get(&ActorSystem)`, `Cluster::for_local_node`, and
  `Cluster::from_membership`.
- Added manager commands for `join_self`, `join`, `join_seed_nodes`, `leave`,
  and `down`.
- Added replayable subscriptions with initial-state, initial-events, and
  live-only modes.
- Added top-level `rakka::prelude` exports for the stable cluster facade types.
- Added tests for local state, join/leave/down, seed joins, subscription replay
  modes, and invalid transition failures.

Tests:

- `Cluster::get` produces a usable local cluster extension.
- `join`, `leave`, and `down` produce expected state transitions.
- `join_seed_nodes` records discovered seed nodes and marks eligible nodes up
  according to membership configuration.
- Subscribers can receive initial snapshot plus live events.
- Live-only subscribers do not receive historical events.
- Invalid transitions fail closed.

Review commands:

```bash
cargo fmt --all -- --check
cargo test -p rakka-cluster
cargo clippy -p rakka-cluster --all-targets -- -D warnings
cargo doc -p rakka-cluster --no-deps
```

## Slice 5B: Runtime, Discovery, Failure, And Downing Hooks

Goal: make the cluster extension useful in running systems, not only direct
unit tests.

Status: implemented.

Scope:

- Add `ClusterSettings` covering:
  - local node descriptor;
  - seed nodes;
  - minimum contact points;
  - discovery polling interval;
  - failure timeout;
  - down-after-unreachable timeout.
- Add a cluster runtime loop that polls `DiscoveryProvider`.
- Keep runtime startup explicit so examples and tests can control scheduling.
- Add trait hooks:
  - `FailureDetector`;
  - `DowningStrategy`;
  - optionally `SplitBrainPolicy` as a named placeholder with conservative
    defaults.
- Add a conservative default failure/downing policy that mirrors existing
  membership behavior.
- Add adapter APIs so Kubernetes discovery can plug into the cluster extension
  without making `rakka-cluster` depend on `rakka-k8s`.
- Add bridge APIs so `ClusterNodeRuntime` can share or mirror cluster extension
  state without divergence.

Acceptance criteria:

- Static discovery can form a local cluster through the facade.
- Local discovery changes emit cluster events.
- Failure detection and downing are configurable through typed settings.
- `ClusterNodeRuntime` and `Cluster` facade state remain consistent after
  discovery updates.

Implementation status:

- Added `ClusterSettings` with local node, seed nodes, membership config,
  discovery poll interval, failure tick interval, and convenience builders for
  minimum contact points, failure timeout, and down-after-unreachable timeout.
- Added `ClusterRuntime` as an explicit, deterministic facade runtime with
  `join_seed_nodes`, `poll_discovery`, and `tick` operations.
- Added `FailureDetector` and `DowningStrategy` hooks, with default timeout
  implementations and a `NoDowningStrategy` for callers that disable automatic
  downing.
- Added `ClusterManager::apply_discovery` for direct snapshot application when
  callers already own discovery scheduling.
- Kept discovery provider integration in `rakka-cluster`, so
  `rakka-k8s::KubernetesDnsDiscovery` can plug in through `DiscoveryProvider`
  without a crate dependency cycle.
- Kept discovery disappearance conservative: missing nodes are not immediately
  removed by discovery polling; they become unreachable/down through the
  configured failure detector and downing strategy.
- Added `ClusterNodeRuntime::apply_cluster_state` and
  `apply_cluster_state_async` bridge APIs so sharding/remoting runtimes can
  mirror the high-level cluster facade state.
- Exported the new runtime, settings, and policy hook types from
  `rakka-cluster` and the top-level `rakka::prelude`.

Tests:

- Static discovery joins nodes deterministically.
- Local discovery additions update state and subscriptions.
- Unreachable/down transitions fire at configured times.
- Custom downing strategy can prevent automatic downing.
- Node runtime and cluster facade do not diverge after shared discovery updates.

Review commands:

```bash
cargo fmt --all -- --check
cargo test -p rakka-cluster
cargo test -p rakka-sharding
cargo clippy -p rakka-cluster -p rakka-sharding --all-targets -- -D warnings
```

## Slice 5C: Local Receptionist

Goal: implement Akka-style typed service discovery for local actor refs.

Status: implemented.

Proposed API:

```rust
let receptionist = Receptionist::get(&system);
let key = ServiceKey::<WorkerCommand>::new("workers");

let registration = receptionist.register(&key, worker_ref.clone())?;
let listing = receptionist.find(&key)?;
let subscription = receptionist.subscribe(&key)?;
```

Scope:

- Add `ServiceKey<M>`.
- Add `Receptionist::get(&system)`.
- Add:
  - `register`;
  - `deregister`;
  - `find`;
  - `subscribe`.
- Add `Listing<M>` carrying typed routees and key metadata.
- Add registration handles with explicit deregistration.
- Add automatic cleanup when registered actors terminate.
- Add subscription updates for register, deregister, and termination cleanup.
- Add typed mismatch protection so a key cannot be read as another message
  protocol.

Acceptance criteria:

- Local actors can register under a typed service key.
- Listings are deterministic and typed.
- Actor termination removes routees without manual cleanup.
- Subscribers receive initial listing and subsequent changes.
- Duplicate registration is idempotent.

Implementation status:

- Added `ServiceKey<M>`, `Receptionist`, `Listing<M>`,
  `ReceptionistRegistration<M>`, `ReceptionistSubscription<M>`,
  `ReceptionistError`, and `ReceptionistResult` in `rakka-core`.
- Added `Receptionist::get(&ActorSystem)` backed by a per-system local
  registry.
- Added synchronous local `register`, `deregister`, `find`, and `subscribe`
  operations; local calls remain deterministic and do not need async scheduling.
- Added registration leases that deregister on drop and can be explicitly
  released with `ReceptionistRegistration::deregister`.
- Added duplicate-registration lease counting so repeated registration of the
  same actor/key produces one listing entry until all leases are released.
- Added actor-termination cleanup through DeathWatch-backed registration tasks,
  with subscriber notification after cleanup.
- Added typed service-id protection: once a service id is associated with a
  message protocol, using that id through another `ServiceKey<M>` fails closed.
- Added `rakka::prelude` exports for the stable local receptionist facade.

Tests:

- Register, find, deregister.
- Duplicate registration remains one listing entry.
- Actor termination removes registration.
- Subscription emits initial listing and updates.
- Type mismatch fails closed.
- Dropping or closing registration handle deregisters if that policy is chosen.

Review commands:

```bash
cargo fmt --all -- --check
cargo test -p rakka-core
cargo clippy -p rakka-core --all-targets -- -D warnings
cargo doc -p rakka-core --no-deps
```

## Slice 5D: Pool Routers

Goal: support local worker pools without forcing users into sharding.

Status: implemented.

Proposed API:

```rust
let router = Routers::pool("workers", 8, || WorkerActor)
    .with_round_robin()
    .spawn(&system)?;

router.tell(WorkCommand::Process(job))?;
```

Scope:

- Add `Routers` facade namespace.
- Add `Routers::pool`.
- Add routee construction from an actor factory.
- Add routing strategies:
  - round-robin;
  - random.
- Add routee child lifecycle management.
- Add router actor or router ref abstraction that accepts `M`.
- Add routee supervision options consistent with `ActorOptions`.
- Add no-routee behavior with typed errors.
- Decide whether terminated routees are removed, replaced, or configurable.

Acceptance criteria:

- Pool routers spawn and route to local children.
- Round-robin distribution is deterministic in tests.
- Random routing never selects terminated routees.
- Routee termination behavior is explicit.
- Failures preserve message ownership where possible.

Implementation status:

- Added the `Routers` facade namespace in `rakka-core`.
- Added `Routers::pool(name, size, factory)` with routee actors spawned as
  named local actors using the supplied factory.
- Added `PoolRouterBuilder` with `with_round_robin`, `with_random`,
  `with_strategy`, `with_options`, and `with_spawn_options`.
- Added `PoolRouter<M>` with `tell`, `routees`, `routee_count`, `is_empty`,
  `strategy`, and `stop_routees`.
- Added `PoolRoutingStrategy` with round-robin and pseudo-random routing.
- Added `PoolRouterTellError<M>` with message-preserving `NoRoutees`, `Full`,
  and `Closed` failures.
- Chose explicit routee termination semantics for this slice: terminated
  routees are removed from the pool on the next router observation or send;
  they are not automatically replaced yet.
- Added `rakka::prelude` exports for the stable local pool-router facade.

Tests:

- Pool spawns configured number of routees.
- Round-robin fairness over a fixed message count.
- Random strategy routes only to live routees.
- Terminated routees are removed or replaced according to settings.
- No-routee errors are typed and documented.

Review commands:

```bash
cargo fmt --all -- --check
cargo test -p rakka-core
cargo test -p rakka-testkit
cargo clippy -p rakka-core -p rakka-testkit --all-targets -- -D warnings
```

## Slice 5E: Group Routers Over Local Receptionist

Goal: route to dynamically discovered actors registered under `ServiceKey<M>`.

Proposed API:

```rust
let router = Routers::group(ServiceKey::<WorkCommand>::new("workers"))
    .with_round_robin()
    .spawn(&system, "workers-group")?;
```

Scope:

- Add `Routers::group(service_key)`.
- Subscribe to receptionist listings.
- Refresh routees when local listings change.
- Support round-robin and random group strategies.
- Add no-routee policy:
  - fail fast;
  - drop;
  - bounded buffer if useful and consistent with sharding buffers.
- Add observable router state for tests.

Acceptance criteria:

- Group routers discover routees through receptionist.
- Registering a service updates routing without restarting the router.
- Deregistering or terminating a service removes it from routing.
- No-routee behavior is explicit and tested.

Implementation status:

- Added `Routers::group(ServiceKey<M>)` as the local
  receptionist-backed group-router entry point.
- Added `GroupRouterBuilder<M>` with `with_round_robin`, `with_random`,
  `with_strategy`, `with_fail_fast_no_routees`,
  `with_drop_when_no_routees`, and `with_no_routee_behavior`.
- Added `GroupRouter<M>` with `tell`, `refresh`, `routees`,
  `routee_count`, `is_empty`, `snapshot`, `strategy`,
  `service_key`, and `no_routee_behavior`.
- Added `GroupRoutingStrategy`, `GroupNoRouteeBehavior`,
  `GroupRouterSnapshot`, and message-preserving
  `GroupRouterTellError<M>`.
- Group routers subscribe to local receptionist listing updates and also
  refresh synchronously before sends so newly registered routees can be used
  without restarting the router.
- Round-robin and pseudo-random routing are supported over current local
  receptionist routees.
- Deregistered routees and terminated actors are removed from routing on
  listing refresh, router observation, or send.
- No-routee behavior is explicit: fail-fast is the default and preserves the
  message; drop is opt-in and reports success after consuming the message.
- Bounded no-routee buffering is intentionally deferred until it can reuse or
  align with the existing sharding buffer model rather than adding a separate
  router-only queue policy.
- Added `rakka::prelude` exports for the stable local group-router facade.

Tests:

- Routee appears after registration.
- Routee removed after deregistration.
- Routee removed after actor termination.
- Router refreshes without restart.
- No-routee policy works as configured.

Review commands:

```bash
cargo fmt --all -- --check
cargo test -p rakka-core
cargo test -p rakka-testkit
cargo clippy -p rakka-core -p rakka-testkit --all-targets -- -D warnings
```

## Slice 5F: Clustered Receptionist Propagation

Goal: extend receptionist semantics across cluster members after local behavior
is stable.

Scope:

- Add clustered receptionist settings:
  - enabled/disabled;
  - publish interval;
  - remote listing TTL;
  - maximum services/listing size if needed.
- Add remote listing state keyed by node id and service key.
- Add listing versioning or timestamps to avoid stale overwrites.
- Add reachability filtering so down/removed nodes disappear from listings.
- Propagate listings over existing remoting/node-runtime surfaces.
- Keep deterministic in-memory propagation tests before TCP tests.
- Add clustered group router integration once remote listings are stable.

Acceptance criteria:

- Service registered on node A appears in node B listing.
- Deregister on node A removes it from node B listing.
- Down or removed node clears remote routees.
- Stale remote listings expire.
- Clustered group router refreshes from propagated listings.

Implementation status:

- Added additive core receptionist support for propagated listings:
  `Listing::revision`, `Receptionist::find_local`,
  `Receptionist::install_remote_listing`,
  `Receptionist::remove_remote_node`, and
  `Receptionist::expire_remote_listings`.
- Local receptionist registrations and propagated remote node snapshots are
  stored separately, then merged by normal `Receptionist::find` so existing
  group routers can discover propagated routees without a new router API.
- Added `ClusteredReceptionistSettings` with enabled/disabled propagation,
  publish interval metadata, remote listing TTL, and optional maximum routees
  per propagated listing.
- Added `ClusteredReceptionistListing<M>` as the versioned publication envelope
  and `ClusteredReceptionist` as the propagation facade.
- Added explicit deterministic propagation APIs:
  `publish_local`, `apply_remote`, and `propagate_to`.
- Remote listings are keyed by source node and service id, reject lower source
  revisions, refresh TTL on equal-version publications, expire by TTL, and are
  pruned when the source member is no longer `Up`.
- Added `ClusterSettings::clustered_receptionist` and
  `with_clustered_receptionist` so runtime wiring can use the same settings.
- Added `rakka-cluster` and `rakka::prelude` exports for the clustered
  receptionist facade.
- Added deterministic in-memory tests for registration propagation,
  deregistration propagation, down-node pruning, TTL expiry, same-version TTL
  refresh, stale-version rejection, listing-size limits, and group-router
  routing over propagated listings.
- TCP loopback propagation remains a follow-up inside 5F/5H because transport
  propagation needs a serializable remote service-reference representation
  rather than in-memory `ActorRef<M>` clones.

Tests:

- Two logical nodes propagate registration.
- Deregistration propagates.
- Node down removes routees.
- TTL expiry removes stale listings.
- Clustered group router routes after propagation.
- TCP loopback propagation test after deterministic tests pass.

Review commands:

```bash
cargo fmt --all -- --check
cargo test -p rakka-core
cargo test -p rakka-cluster
cargo test -p rakka-remote
cargo test -p rakka-sharding
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

## Slice 5G: Consistent Hash Routing

Goal: add key-sticky routing where users need stable distribution but not
sharding ownership.

Scope:

- Add consistent-hash strategy for pool routers.
- Add consistent-hash strategy for group routers.
- Add hash mapper closure or trait, for example `Fn(&M) -> String`.
- Add configurable virtual nodes if useful.
- Ensure routee changes remap as little as practical.
- Document that consistent-hash routing is not a replacement for sharding when
  the service has durable entity identity.

Acceptance criteria:

- Same key routes to the same live routee while routees are unchanged.
- Different keys spread across routees.
- Removed routee remaps affected keys.
- Added routee changes distribution predictably.

Tests:

- Stable route for same key.
- Distribution across routees.
- Routee removal remaps keys away from removed routee.
- Group consistent hash refreshes after receptionist listing changes.

Review commands:

```bash
cargo fmt --all -- --check
cargo test -p rakka-core
cargo test -p rakka-testkit
cargo clippy -p rakka-core -p rakka-testkit --all-targets -- -D warnings
```

## Slice 5H: Docs, Examples, And Testkit

Goal: make Phase 5 behavior reviewable by humans and reusable in tests.

Scope:

- Add `docs/rakka-akka-parity-phase-5-cluster-receptionist-routers.md`.
- Update `README.md` with Phase 5 examples.
- Update `docs/rakka-akka-parity-migration-notes.md`.
- Add examples:
  - local receptionist and group router;
  - pool router worker farm;
  - clustered receptionist with two logical nodes.
- Add testkit helpers for:
  - receptionist listing assertions;
  - router routee-count assertions;
  - cluster event subscription assertions.
- Update the main Akka parity implementation plan with Phase 5 status.

Acceptance criteria:

- Examples compile and run locally.
- Docs explain when to choose receptionist, router, or sharding.
- Testkit helpers remove repeated subscription/listing boilerplate.
- Phase 5 completion status is clear.

Review commands:

```bash
cargo fmt --all -- --check
cargo test -p rakka-testkit
cargo run -p rakka-example-local-receptionist-router
cargo run -p rakka-example-pool-router
cargo run -p rakka-example-clustered-receptionist
cargo doc --workspace --all-features --no-deps
```

## Cross-cutting Test Matrix

| Concern | Required coverage |
| --- | --- |
| Cluster facade | join, seed join, leave, down, invalid transitions |
| Cluster subscriptions | initial snapshot, initial events, live-only updates |
| Discovery runtime | static discovery, local discovery changes, failure timeout |
| Downing hooks | default policy, custom no-down policy, unreachable transitions |
| Local receptionist | register, deregister, find, subscribe, actor termination cleanup |
| Type safety | service-key message mismatch fails closed |
| Pool routers | routee spawn, fairness, random live routee selection, termination |
| Group routers | listing refresh, no-routee policy, deregistration cleanup |
| Clustered receptionist | propagation, stale listing expiry, down-node filtering |
| Consistent hash | stable key routing, routee change remapping |
| Docs/API | examples compile, rustdoc builds, migration notes updated |

## Validation Gate

Use this as the full Phase 5 review gate:

```bash
cargo fmt --all -- --check
cargo test -p rakka-core
cargo test -p rakka-cluster
cargo test -p rakka-remote
cargo test -p rakka-sharding
cargo test -p rakka-testkit
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo doc --workspace --all-features --no-deps
```

## Recommended Implementation Order

1. Slice 5A: Cluster extension facade.
2. Slice 5B: Runtime, discovery, failure, and downing hooks.
3. Slice 5C: Local receptionist.
4. Slice 5D: Pool routers.
5. Slice 5E: Group routers over local receptionist.
6. Slice 5F: Clustered receptionist propagation.
7. Slice 5G: Consistent-hash routing.
8. Slice 5H: Docs, examples, and testkit.

Start with Slice 5A. It gives the rest of Phase 5 a stable lifecycle and event
source, and it should make receptionist and clustered router APIs cleaner.

## Risks And Mitigations

- API sprawl: keep the first cluster facade small and use explicit settings
  builders for advanced behavior.
- Cluster/sharding divergence: share membership updates or provide a clear
  bridge so `ClusterNodeRuntime` and `Cluster` do not publish conflicting state.
- Receptionist cardinality: document that receptionist is for stateless service
  discovery, not entity identity.
- Stale clustered listings: include TTL and membership-state filtering from the
  first clustered receptionist slice.
- Router message loss surprises: make no-routee and routee-termination behavior
  explicit in builder settings and errors.
- Dependency cycles: keep cluster/receptionist/router foundations in crates that
  do not require persistence or adapter crates.
