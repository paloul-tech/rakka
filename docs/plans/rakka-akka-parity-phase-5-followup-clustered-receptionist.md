# Rakka Akka Parity Phase 5 Follow-up: TCP Clustered Receptionist

Status: complete; Slices 5R-A through 5R-H implemented
Date: 2026-06-13

## Purpose

Phase 5 implemented deterministic clustered receptionist propagation with
in-process `ActorRef<M>` routees. This follow-up completes the remaining TCP
clustered receptionist work by adding a transport-serializable remote service
reference model and routing through local proxy actors.

The goal is to preserve the user-facing API:

```rust
let key = ServiceKey::<WorkerCommand>::new("workers");
let router = Routers::group(key).spawn(&system, "workers")?;
router.tell(command)?;
```

Applications should not need to know whether a receptionist routee is local,
deterministically propagated, or reached over TCP.

## Architecture Decision

Implement the transport integration in `rakka-remote`, not `rakka-cluster`.
`rakka-cluster` should remain transport-agnostic. `rakka-remote` already owns
envelopes, endpoints, TCP transport, remote request/reply behavior, and payload
serialization.

Do not serialize `ActorRef<M>` directly. Instead, publish wire descriptors for
concrete actor incarnations, materialize typed local proxy actors on the
receiving node, and install those proxies into the existing local receptionist.
That lets `Routers::group(ServiceKey<M>)` continue to route over normal typed
`ActorRef<M>` values.

## Slice 5R-A: Remote Actor-Ref Wire Identity

Goal: make remote envelopes able to address one concrete actor incarnation.

Status: implemented.

Scope:

- Add a transport-facing actor-reference destination or descriptor that carries:
  - source `NodeId`;
  - actor system name;
  - logical actor path;
  - actor uid;
  - message type.
- Extend the Protobuf envelope destination fields without breaking existing
  entity, service, route-key, and reply destinations.
- Add conversion tests for encode/decode round trips.
- Keep path-only `ActorPath` destinations if existing callers rely on them, but
  do not use path-only routing for clustered receptionist routees.

Acceptance criteria:

- Wire identity round-trips through `ProtobufEnvelopeCodec`.
- Missing path, uid, system name, or message type fails closed.
- Existing remote envelope tests continue to pass.

Implementation status:

- Added `RemoteActorRef` as the transport-serializable descriptor for a
  concrete actor incarnation.
- Added `RemoteDestination::ActorRef` while preserving the existing
  path-only, entity, service, route-key, and reply destination variants.
- Extended the Protobuf destination shape with actor node id, actor system
  name, actor uid, and actor message type fields.
- Added codec round-trip coverage and malformed private-proto tests for
  missing actor-ref identity fields.

Review commands:

```bash
cargo test -p rakka-remote --test remote_boundary
```

## Slice 5R-B: Typed Remote Actor-Ref Inbound Handler

Goal: deliver remote envelopes to local concrete actor refs safely.

Status: implemented.

Scope:

- Add a typed inbound handler, for example `RemoteActorRefInbound<M>`.
- Decode payloads with `SerializationRegistry`.
- Resolve the serialized actor reference through `ActorRefResolver`.
- Send the decoded message to the resolved local actor.
- Map stale uid, wrong message type, missing codec, full mailbox, and closed
  actor into stable remote endpoint errors.
- Add `RemoteEndpoint` dispatch support for the concrete actor-ref destination.

Acceptance criteria:

- A remote envelope addressed to a live local actor is decoded and delivered.
- A stale uid at the same path is rejected.
- Wrong message type and missing codec fail closed before delivery.
- Mailbox full and closed actor errors are observable.

Implementation status:

- Added `RemoteActorRefInbound<M>` as the typed inbound actor-ref handler.
- Added `RemoteActorRefInboundError` for destination mismatch, node mismatch,
  actor-ref resolution, decode, full mailbox, and closed mailbox failures.
- Added `RemoteEndpoint::register_actor_ref_handler::<M>` and actor-ref
  dispatch keyed by the Rust message type carried in `RemoteActorRef`.
- Added remote boundary coverage for successful in-memory delivery, missing
  handler, stale uid, wrong message type, missing codec, node mismatch, and
  full mailbox failures.

Review commands:

```bash
cargo test -p rakka-core --test local_actor_runtime
cargo test -p rakka-remote
```

## Slice 5R-C: Remote Receptionist Wire Listing Model

Goal: represent receptionist listings without in-memory `ActorRef<M>` values.

Status: implemented.

Scope:

- Add remote wire-listing types in `rakka-remote`, for example:
  - `RemoteReceptionistListing`;
  - `RemoteServiceRoutee`.
- Include source node, service id, routee descriptors, listing version, and
  observed timestamp.
- Convert local-only `Listing<M>` snapshots into wire listings using
  `ActorRefResolver`.
- Validate service id, source node, routee count, and routee descriptor fields.
- Preserve Phase 5F semantics: source revision, same-version refresh, TTL, and
  listing-size limits.

Acceptance criteria:

- Local listings can be converted into remote wire listings.
- Empty listings propagate deregistration.
- Invalid or oversized wire listings fail closed.
- Deterministic `ClusteredReceptionistListing<M>` remains available for tests.

Implementation status:

- Added `RemoteReceptionistListing` and `RemoteServiceRoutee` as
  transport-facing descriptors for one source-node service snapshot.
- Included source node, service id, service message type, routee descriptors,
  source listing version, and observation timestamp.
- Added conversion from local `Listing<M>` snapshots through
  `ActorRefResolver`, preserving empty listings for deregistration.
- Added fail-closed validation for source node identity, service id, service
  message type, routee source node, routee message type, and optional maximum
  routee count.
- Added remote boundary coverage for non-empty local listing conversion, empty
  deregistration listings, invalid input rejection, and oversized listing
  rejection.

Review commands:

```bash
cargo test -p rakka-cluster --test clustered_receptionist
cargo test -p rakka-remote
```

## Slice 5R-D: Proxy Materialization And Lifecycle

Goal: turn remote service routee descriptors into local typed proxy actors.

Status: implemented.

Scope:

- Add a proxy actor, for example `RemoteServiceProxy<M>`.
- Proxy accepts `M`, encodes it with `SerializationRegistry`, and sends a
  remote envelope to the source node.
- Add a proxy registry keyed by source node, service id, actor path, uid, and
  message type.
- Reuse proxies across equal-version TTL refreshes.
- Stop and remove proxies when routees disappear, listings expire, or source
  nodes leave/down.
- Install materialized proxy actor refs through the existing
  `Receptionist::install_remote_listing` path.

Acceptance criteria:

- A remote wire listing materializes local proxy routees.
- `Routers::group(ServiceKey<M>)` discovers and routes to those proxy routees
  without new router APIs.
- Equal-version refreshes do not create duplicate proxies.
- Deregistration, TTL expiry, and node down remove proxy routees.

Implementation status:

- Added `RemoteServiceProxy<M>` as a local actor that encodes `M`, wraps it in
  `RemoteDestination::ActorRef`, and sends it through `RemoteTransport`.
- Added `RemoteServiceProxyRegistry` to materialize `RemoteReceptionistListing`
  routees into local anonymous proxy actors and install them through
  `Receptionist::install_remote_listing`.
- Added stable `RemoteServiceRouteeKey` tracking by source node, service id,
  actor path, actor uid, and message type.
- Reused existing proxies for same-version listing refreshes while refreshing
  the core receptionist timestamp.
- Added lifecycle cleanup for empty listings, stale-listing expiry, and source
  node removal, stopping proxy actors as their routees leave the remote
  listing.
- Added remote boundary coverage proving group routers discover local proxies
  and deliver over the in-memory remote transport to the source node actor.

Review commands:

```bash
cargo test -p rakka-core --test receptionist
cargo test -p rakka-core --test routers
cargo test -p rakka-remote
```

## Slice 5R-E: Explicit Runtime Helper

Goal: provide a reviewable integration surface before adding background loops.

Status: implemented.

Scope:

- Add a helper such as `RemoteClusteredReceptionist` in `rakka-remote`.
- Wire together:
  - `ActorSystem`;
  - `Cluster`;
  - `RemoteEndpoint`;
  - `RemoteTransport`;
  - `SerializationRegistry`;
  - `ClusteredReceptionistSettings`.
- Expose explicit methods first:
  - `publish_once(&ServiceKey<M>, observed_at_millis)`;
  - `apply_wire_listing::<M>(listing)`;
  - `prune_unreachable_members`;
  - `expire_stale_listings`.
- Add optional interval-driven publication task helpers only after the explicit
  methods are covered.
- Keep any background task cancellable and owned by a returned handle.

Acceptance criteria:

- Users can drive one publish/apply cycle explicitly in tests.
- Runtime helper does not hide IO or create unbounded background work.
- Settings match `ClusteredReceptionistSettings`.

Implementation status:

- Added `RemoteClusteredReceptionist` as an explicit helper that wires
  `ActorSystem`, `Cluster`, `RemoteEndpoint`, `RemoteTransport`,
  `SerializationRegistry`, `Receptionist`, `RemoteServiceProxyRegistry`, and
  `ClusteredReceptionistSettings`.
- Added `register_actor_ref_handler::<M>` for reviewable inbound actor-ref
  registration on the owned endpoint.
- Added `publish_once` to convert local-only `ServiceKey<M>` listings into
  remote wire listings without sending them implicitly.
- Added `apply_wire_listing::<M>` to validate source membership, enforce routee
  limits, and materialize remote service proxies through the proxy registry.
- Added explicit `prune_unreachable_members` and `expire_stale_listings`
  lifecycle methods using the same `Up` membership and TTL semantics as the
  deterministic clustered receptionist facade.
- Added remote boundary coverage for an explicit publish/apply cycle, disabled
  propagation, routee-limit errors, pruning, and TTL expiry. No background loop
  helper was added in this slice.

Review commands:

```bash
cargo test -p rakka-remote
cargo test -p rakka-cluster
```

## Slice 5R-F: In-Memory Remote Transport Validation

Goal: prove the transport-facing model without TCP timing noise.

Status: implemented.

Scope:

- Build a two-node in-memory remote transport test fixture.
- Register a local service actor on node A.
- Publish a remote receptionist listing from node A to node B.
- Materialize proxies on node B.
- Route from a node B group router to the node A service actor.
- Cover deregistration, actor termination, stale uid, missing codec, and node
  down.

Acceptance criteria:

- Group router on node B delivers to a service actor on node A.
- Stale or invalid routee descriptors never deliver.
- Listing lifecycle cleanup removes proxy routees.
- The deterministic Phase 5F tests remain green.

Implementation status:

- Added explicit two-node in-memory remote clustered receptionist validation
  through `RemoteClusteredReceptionist` and `InMemoryRemoteTransport`.
- Covered node B group-router delivery to a node A service actor through a
  materialized proxy routee.
- Covered deregistration and actor-termination publication of empty listings,
  verifying proxy cleanup on node B.
- Covered stale-uid remote routees by restarting the source actor at the same
  path and asserting the stale proxy does not deliver.
- Covered missing outbound codec on the proxying node, asserting the routee
  never delivers while the source node remains valid.
- Reused the 5R-E node-down pruning coverage and kept deterministic clustered
  receptionist tests green.

Review commands:

```bash
cargo test -p rakka-remote
cargo test -p rakka-cluster --test clustered_receptionist
```

## Slice 5R-G: TCP Loopback Integration

Goal: complete the production-shaped TCP propagation story.

Status: implemented.

Scope:

- Add TCP loopback tests for remote clustered receptionist propagation.
- Use real `TcpRemoteTransport` with registered peers.
- Register typed payload codecs for the service command protocol.
- Publish listings over TCP.
- Route group-router messages over TCP to the remote service actor.
- Assert metrics or endpoint snapshots where available.

Acceptance criteria:

- Node B discovers node A's service through TCP propagation.
- A group router on node B delivers to node A's service actor over TCP.
- Missing peer or unknown destination node fails closed.
- Existing sharding TCP tests remain unaffected.

Implementation status:

- Added `RemoteEndpoint::register_service_handler` and fail-closed service-key
  dispatch so remote receptionist listings can arrive as service-addressed
  envelopes over TCP.
- Added `RemoteReceptionistListingCodec` for registering typed listing payloads
  with `SerializationRegistry`.
- Added `RemoteClusteredReceptionist::register_receptionist_listing_handler`
  and `publish_once_to` so callers can explicitly receive and send wire
  listings through the configured `RemoteTransport`.
- Added TCP loopback coverage that publishes node A's service listing to node
  B, materializes a node B proxy routee, routes a node B group-router message
  back to node A's service actor over TCP, and asserts transport snapshots for
  inbound and outbound envelope flow.
- Added missing-peer fail-closed coverage for listing publication through the
  TCP-backed helper.

Review commands:

```bash
cargo test -p rakka-remote
cargo test -p rakka-sharding --test network_runtime
```

## Slice 5R-H: Docs, Example, And Testkit Polish

Goal: make the TCP receptionist behavior reviewable and reusable.

Status: implemented.

Scope:

- Update `docs/rakka-akka-parity-phase-5-cluster-receptionist-routers.md`.
- Add a runnable TCP loopback example or extend
  `rakka-example-clustered-receptionist` with `--tcp-loopback`.
- Add `rakka-testkit` helpers for remote receptionist propagation assertions if
  test code repeats wire-listing or proxy-count boilerplate.
- Update this plan and the main Akka parity implementation plan with final
  completion notes.

Acceptance criteria:

- Docs explain deterministic propagation versus TCP propagation.
- Example runs locally without external services.
- Testkit helper additions are covered by `rakka-testkit` tests.

Implementation status:

- Updated the Phase 5 cluster, receptionist, and routers guide to explain the
  deterministic in-process propagation model separately from TCP propagation
  through `rakka-remote`.
- Extended `rakka-example-clustered-receptionist` with `--tcp-loopback`, which
  binds two loopback `TcpRemoteTransport` instances, registers service command
  and listing codecs, publishes a remote listing over TCP, materializes the
  proxy routee, and routes through a normal group router.
- Added reusable `rakka-testkit` helpers for remote receptionist wire-listing
  assertions, remote proxy/listing snapshot counts, and waiting for proxy
  registry convergence.
- Covered the new testkit helpers in `rakka-testkit` integration tests.
- Updated the main Akka parity plan and migration notes to remove the stale
  "TCP propagation pending" wording.

Review commands:

```bash
cargo fmt --all -- --check
cargo test -p rakka-testkit
cargo run -p rakka-example-clustered-receptionist -- --tcp-loopback
cargo doc --workspace --all-features --no-deps
```

## Full Validation Gate

Use this gate before marking the follow-up complete:

```bash
cargo fmt --all -- --check
cargo test -p rakka-core
cargo test -p rakka-cluster
cargo test -p rakka-remote
cargo test -p rakka-sharding
cargo test -p rakka-testkit
cargo check --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo doc --workspace --all-features --no-deps
```

## Risks And Mitigations

- Public API sprawl: keep TCP integration in `rakka-remote` and expose explicit
  helpers before background runtime tasks.
- Stale actor delivery: require actor uid and message type in the routee
  descriptor, and reject path-only service routees for TCP receptionist.
- Duplicate proxies: key proxies by source node, service id, path, uid, and
  message type.
- Hidden resource growth: enforce listing-size limits, bounded proxy lifecycle,
  and TTL expiry.
- Dependency cycles: do not move transport types into `rakka-cluster`.
