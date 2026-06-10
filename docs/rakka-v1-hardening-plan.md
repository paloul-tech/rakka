# Rakka V1 Hardening Plan

## Purpose

This file is the working slice outline for V1 hardening after Phase 5. The v1 foundations now demonstrate typed actors, durable state, cluster membership, sharding, remote envelopes, workflows, process actors, streams, HTTP/gRPC adapters, Kubernetes health/drain, metrics, examples, and testkit helpers. This plan defines the remaining work needed to turn those foundations into a reviewable v1 release candidate.

Working note: as we move forward, follow this slice outline from this file. Before starting a V1 hardening slice, read this file, implement only the next agreed slice, then update this file if scope or status changes.

## Current Baseline

The following foundations are already in place:

- `rakka-core` provides typed actors, bounded mailboxes, `tell`, `ask`, timers, child spawning, stopping, dead letters, watching, supervision, and in-memory metrics.
- `rakka-persistence` and `rakka-persistence-postgres` provide durable state, revision fencing, in-memory storage, and PostgreSQL storage.
- `rakka-remote` provides Protobuf remote envelopes, serialization registry behavior, schema compatibility policy checks, endpoint routing, reply correlation, and deterministic in-memory transport.
- `rakka-cluster` provides membership, discovery snapshots, failure detection state, protocol compatibility policy, and operational snapshots.
- `rakka-sharding` provides shard ownership, entity refs, local entity routing, remote-aware routing, inbound remote entity delivery, graceful handoff, passivation, runtime ownership refresh, and metrics snapshots.
- `rakka-process` provides supervised child process ownership, stdio/line-json protocols, one-shot/file-watch/socket/local-gRPC modes, process-backed entities, process health, and process testkit helpers.
- `rakka-workflow` provides durable inbox/outbox reliability, deduplication, retries, recovery, and telemetry.
- `rakka-stream`, `rakka-http`, `rakka-grpc`, and `rakka-k8s` provide bounded streams, public HTTP/gRPC adapter foundations, Kubernetes health/drain hooks, example manifests, and metrics surfaces.
- `rakka-testkit` provides reusable local assertions for actors, HTTP, gRPC, streams, Kubernetes health/drain, and metrics.
- Reviewable examples exist for minimal actors, durable counters, durable workflows, multi-node sharding with deterministic in-memory transport, external binary wrapping, edge gateway integration, and Kubernetes manifests.

## V1 Hardening Done Definition

V1 hardening should be considered complete when Rakka can be reviewed as a coherent release candidate rather than only as a set of foundations.

Minimum completion criteria:

- Remote actor/entity routing can run across real networked Rakka nodes using a Tokio transport, not only deterministic in-memory transport.
- Cluster membership, sharding, and remoting can be exercised by multi-process local tests and gated local Kubernetes scenarios.
- N/N+1 rolling-update compatibility is validated across protocol versions, schema versions, manifests, and example deployments.
- Public APIs are reviewed for crate boundaries, naming, error stability, feature flags, documentation, and v1 draft expectations.
- Generated gRPC examples demonstrate how application proto contracts call actors, entities, streams, workflows, and process-backed services.
- Metrics and tracing can be exported through production-oriented adapters while keeping the in-memory recorder for tests.
- Kubernetes manifests and examples show readiness, liveness, drain, remoting, HTTP/gRPC ingress, rolling update, and graceful shard handoff boundaries.
- CI/release checks cover formatting, tests, clippy, docs, MSRV, examples, packaging, and optional gated integration suites.
- V1 docs clearly explain reliability boundaries: core actor at-most-once delivery, opt-in durable workflow reliability, internal remoting trust boundary, process ownership constraints, and Kubernetes operation assumptions.

## Phase-Level Out of Scope

- Exactly-once actor delivery or exactly-once external side effects.
- Non-Tokio runtimes.
- Public exposure of internal Rakka remote envelopes as a client API.
- Full service mesh behavior, API gateway policy, authN/authZ platform, or certificate lifecycle management.
- Per-actor Kubernetes sidecar containers.
- Cloud-specific operator/controller implementation.
- Full Helm chart lifecycle beyond reviewable examples and optional packaging notes.
- Distributed consensus implementation from scratch unless required by a specific hardening blocker.

## Slice V1A: Network Remoting Transport Foundation

Status: implemented.

Goal: replace deterministic-only remoting with a real Tokio network transport suitable for local multi-process and Kubernetes scenarios.

Scope:

- Add a Tokio TCP remote transport for Rakka envelope bytes.
- Use length-delimited Protobuf remote envelopes with bounded inbound and outbound queues.
- Add connection lifecycle states: connecting, ready, backoff, draining, closed, and failed.
- Add node handshake metadata: node id, protocol version, compatibility range, supported envelope version, and optional capabilities.
- Enforce compatibility before accepting remote delivery.
- Add reconnect and backoff behavior for transient connection failures.
- Add idle timeout, graceful close, and forced close behavior.
- Add metrics and tracing for connection state, sends, receives, decode failures, back-pressure, reconnects, and compatibility rejection.
- Keep deterministic in-memory transport for tests and single-process examples.

Acceptance criteria:

- Two local `ActorSystem`s in separate Tokio tasks can exchange remote entity envelopes over loopback TCP.
- Unknown nodes, incompatible protocol versions, bad envelope bytes, and missing handlers fail closed with typed errors.
- Bounded queues apply back-pressure instead of growing unbounded memory.
- Dropping or draining a connection wakes pending sends with typed failures.
- Unit tests cover handshake, reconnect, compatibility rejection, decode failure, queue saturation, graceful close, and forced close.

Out of scope:

- Public internet exposure of remoting.
- TLS/mTLS certificate lifecycle.
- QUIC or multi-transport negotiation.

Implementation notes:

- Added `rakka_remote::network` with `TcpRemoteTransport`, `TcpRemoteTransportConfig`, `TcpRemoteHandshake`, lifecycle snapshots, transport snapshots, and TCP-specific metric names.
- Kept `InMemoryRemoteTransport` unchanged for deterministic single-process tests and examples.
- Implemented length-delimited TCP frames with explicit frame kinds for handshake, envelope, and graceful close.
- Added handshake metadata for node id, cluster protocol version, compatibility range, envelope wire version, and capabilities.
- Enforced registered-peer checks, expected-peer checks, envelope-version checks, and mutual `ClusterProtocol` compatibility before accepting inbound delivery or completing outbound connection setup.
- Implemented bounded outbound per-peer queues through the existing synchronous `RemoteTransport::send` API, returning typed queue-full, draining, closed, and unknown-node failures.
- Added fail-fast validation for invalid TCP transport queue and frame-size settings.
- Added outbound worker lifecycle states for connecting, ready, backoff, draining, closed, and failed; added reconnect-on-write-failure with backoff, idle close, graceful drain, and force close.
- Added TCP remoting counters/gauges for connection state, sends, receives, reconnects, and failures, plus tracing events for state transitions and inbound failures.
- Added unit coverage for loopback delivery, unknown inbound node rejection, incompatible protocol rejection, malformed inbound envelope decode failure, bounded queue back-pressure, graceful drain, force-close reconnect, idle timeout, and failure metrics.
- TCP tests self-skip when the host denies local loopback bind; unsandboxed loopback execution was also verified.

## Slice V1B: Cluster Runtime Networking Integration

Status: implemented.

Goal: wire network remoting into cluster membership and sharding so routed entities can move across real process boundaries.

Scope:

- Add a cluster node runtime builder that combines membership, discovery, network remoting, sharding, and health.
- Wire `RemoteTransportEntityOutbound` to the network transport for non-local owners.
- Register inbound `RemoteEntityInbound` handlers from configured entity types.
- Refresh shard ownership after membership and discovery changes.
- Ensure leaving/draining nodes stop accepting new local ownership before handoff completes.
- Add process-level local examples that launch multiple Rakka node processes on loopback ports.
- Preserve the current in-memory multi-node sharding example as deterministic documentation.

Acceptance criteria:

- A multi-process local example routes a sharded entity message from node A to node B through network remoting.
- A node leaving event triggers ownership refresh and graceful handoff across real transport.
- Remote ask replies route back to the requester over network remoting.
- Tests cover node join, remote tell, remote ask, owner refresh, leaving handoff, and unreachable remote delivery.

Out of scope:

- Kubernetes manifests beyond consuming this runtime in later slices.
- Durable shard coordinator storage unless needed to satisfy V1 reliability criteria.

Implementation notes:

- Added `rakka_sharding::ClusterNodeRuntime` and `ClusterNodeRuntimeBuilder` as the V1 networked node facade over membership, discovery, TCP remoting, remote endpoint dispatch, ask reply correlation, and `ClusterShardingRuntime`.
- The builder binds `TcpRemoteTransport`, registers a reply handler backed by `RemoteRequestRegistry`, supports shared `SerializationRegistry`, accepts a metrics recorder, and can advertise the actual bound loopback address for local port-0 tests and examples.
- Runtime discovery application now refreshes cluster/sharding ownership and registers non-local discovered members as TCP peers before callers route traffic.
- Added helpers for TCP-backed remote-aware entity routes, remote ask clients, default inbound tell handlers, custom inbound handlers, and inbound ask handlers.
- Added networked sharding integration tests for node join/peer registration, remote tell over TCP, remote ask/reply over TCP, graceful leaving handoff after remote delivery, ownership refresh, and unreachable peer failure snapshots.
- Kept the deterministic in-memory multi-node sharding example as the default path.
- Extended `rakka-example-multi-node-sharding` with `--networked-loopback` for two TCP node runtimes in one process and `--networked-processes` for a parent process that launches two child Rakka node processes on loopback ports.
- Updated README and Phase 3 remote-sharding docs with deterministic, TCP loopback, and multi-process example commands.

## Slice V1C: Compatibility Matrix and Rolling Update Hardening

Status: implemented.

Goal: make N/N+1 compatibility a tested release property rather than only a policy object.

Scope:

- Define compatibility dimensions: crate version, protocol version, envelope version, schema version, manifest version, and generated API version.
- Add compatibility fixtures for N, N+1, incompatible-old, incompatible-new, additive-schema, and exact-schema cases.
- Add matrix tests for remote handshake, serialization registry policy, sharding ownership, HTTP/gRPC adapters, and Kubernetes manifest env vars.
- Validate that a mixed N/N+1 cluster can route supported messages during rolling updates.
- Validate that incompatible nodes fail readiness and do not acquire shard ownership.
- Document compatibility promises and explicit non-promises in `docs/rakka-compatibility.md`.

Acceptance criteria:

- Compatibility matrix tests run in normal workspace tests without external services.
- Gated multi-process compatibility tests can run over loopback network remoting.
- Rolling update docs specify the exact allowed version skew and operational sequence.
- Incompatibility is observable through readiness reasons, metrics, and typed errors.

Out of scope:

- Supporting arbitrary multi-version clusters beyond N/N+1.
- Silent best-effort delivery across unknown schema changes.

Implementation notes:

- Added `rakka_testkit::compatibility` fixtures for the six v1 dimensions: crate version, cluster protocol, remote envelope, message schema, Kubernetes manifest, and generated API.
- Added standard cases for current N, next N+1, incompatible-old, incompatible-new, additive-schema, and exact-schema compatibility checks.
- Added `crates/rakka-testkit/tests/compatibility_matrix.rs` covering protocol handshake/admission, additive and exact schema policy, mixed N/N+1 remote entity routing over `InMemoryRemoteTransport`, incompatible-node readiness/metrics/ownership rejection, and HTTP/gRPC/Kubernetes metadata alignment.
- Added Kubernetes compatibility/readiness metrics (`rakka.k8s.compatibility` and `rakka.k8s.readiness`) through `KubernetesNodeHealth::record_metrics`.
- Added public HTTP/gRPC API compatibility version constants and manifest/API metadata to the Kubernetes example.
- Expanded `docs/rakka-compatibility.md` with allowed skew, rolling-update sequence, explicit non-promises, observability surfaces, and compatibility test commands.

## Slice V1D: Public API Review and Crate Boundary Hardening

Status: implemented.

Goal: make the public crate surfaces understandable, stable enough for v1 draft review, and internally consistent.

Scope:

- Audit public exports in every crate for naming, visibility, docs, and ownership boundaries.
- Split test-only helpers, examples, and production APIs cleanly.
- Review error types for stable codes, display messages, and conversion boundaries.
- Add feature flags where optional integrations should not be mandatory in minimal builds.
- Confirm crate target names, docs.rs output, README links, and rustdoc examples.
- Add API review notes for actor, persistence, remote, cluster, sharding, workflow, stream, process, HTTP, gRPC, Kubernetes, and testkit crates.
- Prefer additive changes; record any breaking changes explicitly before implementation.

Acceptance criteria:

- `cargo doc --workspace --all-features --no-deps` produces useful public docs.
- Public APIs have docs or intentional hidden/internal visibility.
- Core crates can be compiled without optional HTTP/gRPC/Kubernetes/process integrations when feature flags are introduced.
- Error code docs distinguish user-facing adapter errors from internal remoting errors.
- The README has a concise crate map and links to deeper docs.

Out of scope:

- Promising final semver stability before the v1 release candidate review is complete.
- Large rewrites of already working slices without a concrete API or safety issue.

Implementation notes:

- Added `docs/rakka-v1-api-review.md` with stability tiers, a crate map, feature-boundary notes, public error-code policy, crate-by-crate review notes, and remaining open review questions.
- Added a concise README crate map and linked the API review document from the repository entry point.
- Made stable `code()` accessors public on `ClusterError`, `DurableError`, `ShardingError`, and `WorkflowError`.
- Added `ProcessError::code()` and `ProcessError::into_rakka_error()` so process actor failures follow the same `RakkaError` conversion convention as the rest of the runtime crates.
- Split `rakka-stream` adapter dependencies behind default features: `adapters` for actor/entity helpers and `process-io` for process pipe adapters. Stream core now compiles with `--no-default-features`.
- Set `rakka-http`, `rakka-grpc`, `rakka-k8s`, and `rakka-testkit` to depend on stream core with `default-features = false`.
- Feature-gated `rakka-process::testkit` behind a default-enabled `testkit` feature so production/minimal builds can disable it while existing tests remain compatible.

## Slice V1E: Generated gRPC and HTTP Contract Examples

Status: implemented.

Goal: show how real application contracts use Rakka adapters without relying only on hand-written test structs.

Scope:

- Add generated tonic gRPC example services from `.proto` files.
- Demonstrate unary actor ask, unary entity ask, server streaming, client streaming, and bidirectional streaming.
- Add HTTP examples that mirror the same domain commands for JSON and binary payloads.
- Include a workflow-backed endpoint using durable inbox/outbox reliability.
- Include a process-backed endpoint that wraps a legacy child process through the actor cluster.
- Add contract tests that call the generated clients in-process or over loopback.

Acceptance criteria:

- Examples run with `cargo run` or documented test commands.
- Generated code is reproducible through workspace build steps.
- Tests prove generated gRPC services call actors/entities/streams/workflows/process-backed services.
- Docs explain where generated service code ends and Rakka adapter code begins.

Out of scope:

- Full application template generator.
- OpenAPI generation unless needed for an example.

Implementation notes:

- Added `rakka-example-generated-contracts`, a workspace example package with a `.proto` contract, `tonic-build` build script, generated tonic clients/servers, and mirrored HTTP JSON/binary routes.
- The generated contract demonstrates unary actor ask, unary entity ask, server streaming, client streaming, bidirectional streaming, durable workflow inbox acceptance, and a process-backed line-json legacy service.
- Added a dedicated legacy child binary for deterministic process-backed tests.
- Added `docs/rakka-v1-generated-contracts.md` explaining where generated service code ends and Rakka adapter glue begins.
- Added an integration test that calls generated tonic clients over loopback and exercises mirrored in-process HTTP routes.
- Updated README validation and example instructions with generated-contract commands and expected output.

## Slice V1F: Production Observability Exporters

Status: implemented.

Goal: connect backend-neutral metrics and tracing to production-oriented observability without forcing a single backend.

Scope:

- Add a Prometheus exporter or adapter for stable metric names and labels.
- Add an OpenTelemetry-oriented adapter or documented bridge for metrics/traces.
- Add operational snapshot endpoints or helper routes for actor system, membership, sharding, process, stream, HTTP, and gRPC state.
- Add examples showing metrics scraping and JSON snapshots.
- Add tests for label stability, metric kind mapping, histogram/counter/gauge behavior, and exporter output.
- Document cardinality guidance for entity ids, actor paths, routes, and process names.

Acceptance criteria:

- A local example exposes metrics in a production-consumable format.
- Exported metric names match the stable constants in runtime crates.
- Tests cover HTTP/gRPC latency, stream pressure, process exits, remote failures, membership counts, shard ownership, and actor mailbox depth.
- Docs explain how to keep labels bounded.

Out of scope:

- Hosted observability dashboards.
- Vendor-specific agents beyond generic Prometheus/OpenTelemetry integration.

Implementation notes:

- Added Prometheus text exposition helpers in `rakka-core` that aggregate `MetricsSnapshot` counters, gauges, and histograms while deterministically mapping canonical Rakka metric names to Prometheus-safe identifiers.
- Added an OpenTelemetry-oriented serializable bridge model in `rakka-core` with resource attributes, instrument kind, cumulative temporality, scalar data points, and histogram count/sum data points without forcing a concrete OpenTelemetry SDK dependency.
- Added `rakka-http` observability routes for Prometheus metrics, OpenTelemetry bridge JSON, generic JSON snapshots, and a named `OperationalSnapshotRegistry`.
- Added exporter tests covering stable label ordering, metric kind mapping, HTTP/gRPC latency, stream pressure, process exits, remote failures, membership counts, shard ownership, and actor mailbox depth.
- Added HTTP route tests for `/metrics`, `/otel/metrics`, individual snapshots, and named snapshot registry output.
- Updated the edge gateway example to expose `/metrics`, `/otel/metrics`, and `/snapshots` through the in-process router and assert those routes during the example run.
- Added `docs/rakka-v1-observability-exporters.md` with exporter boundary, OpenTelemetry bridge, snapshot registry, trace integration, and label cardinality guidance.

## Slice V1G: Kubernetes Multi-Node End-to-End Scenario

Status: planned.

Goal: prove the Kubernetes manifests can run real network remoting, shard routing, readiness, drain, and rolling updates in a local cluster.

Scope:

- Build or document a local Rakka node image for kind/minikube.
- Update Kubernetes examples to use network remoting ports and discovery config.
- Add a gated local cluster scenario that deploys three nodes, waits for readiness, routes HTTP/gRPC requests, checks metrics, drains one pod, deletes one pod, and validates replacement.
- Add an optional N/N+1 rolling update scenario using two image tags.
- Keep destructive or cluster-mutating tests gated by explicit env vars.
- Record expected output and troubleshooting notes.

Acceptance criteria:

- Dry-run scenario remains safe and default.
- Gated local-cluster scenario verifies real pod-to-pod routing through Rakka remoting.
- Readiness fails during drain and recovers on replacement.
- Rolling update scenario validates N/N+1 compatibility before all pods are updated.
- Kubernetes docs explain internal remoting service versus public HTTP/gRPC service.

Out of scope:

- Cloud-provider-specific ingress, load balancers, autoscaling, and managed identity.
- Production Helm release automation unless handled in release packaging.

## Slice V1H: Security and Operational Defaults

Status: planned.

Goal: make trust boundaries, unsafe defaults, and operator responsibilities explicit and testable.

Scope:

- Document internal remoting as trusted-cluster traffic and fail closed outside known nodes.
- Add allowlist and bind-address defaults for remoting.
- Review process actor defaults for environment inheritance, executable allowlists, working directories, stdio capture limits, and shutdown behavior.
- Add operational timeout defaults for actor ask, remote send, stream drain, process shutdown, and Kubernetes pre-stop.
- Add tests for unsafe process specs, unknown remote peers, incompatible handshakes, and drain timeouts.
- Add configuration examples for development, local cluster, and production-like deployment.

Acceptance criteria:

- Defaults are conservative for process execution and remote node acceptance.
- Security and operations docs state what Rakka protects, what Kubernetes protects, and what the application/operator must protect.
- Tests cover the major fail-closed paths.

Out of scope:

- Complete authN/authZ platform.
- Certificate provisioning, rotation, or service-mesh integration.

## Slice V1I: CI, Release Packaging, and Repository Hygiene

Status: planned.

Goal: make the repo releasable and keep the validation surface repeatable.

Scope:

- Add or update CI jobs for format, clippy, workspace tests, docs, MSRV, examples, and package checks.
- Add optional CI jobs for PostgreSQL and gated local-cluster scenarios where feasible.
- Validate `cargo package` for publishable crates and `publish = false` examples.
- Add release profile guidance and binary image build notes.
- Add changelog/release-notes draft structure.
- Audit licenses, README links, docs links, crate metadata, rust-toolchain, and `.gitignore`.

Acceptance criteria:

- A clean checkout can run the documented validation commands.
- Publishable crates pass `cargo package --list` and package checks.
- Examples remain excluded from publishing where appropriate.
- CI distinguishes required checks from optional/gated integration checks.

Out of scope:

- Actual crates.io publish.
- Production image registry publishing.

## Slice V1J: Final V1 Release Candidate Review

Status: planned.

Goal: assemble the hardening work into a final reviewable v1 release candidate.

Scope:

- Run the full validation suite and record command output expectations.
- Review all docs for stale phase language and inconsistent terminology.
- Confirm examples cover minimal actor usage, durable state, workflow reliability, process ownership, remote sharding, edge integration, observability, and Kubernetes operation.
- Create a v1 reliability boundaries document.
- Create a migration/upgrade note for N/N+1 rolling updates.
- Record remaining known limitations and post-v1 roadmap items.

Acceptance criteria:

- V1 docs clearly separate implemented behavior, gated examples, future extensions, and non-goals.
- All required validation commands pass locally.
- Optional gated checks have documented prerequisites.
- The release candidate has a concise review checklist.

Out of scope:

- Implementing new runtime features not already covered by the hardening slices.

## Suggested Next Slice

Continue with Slice V1F: Production Observability Exporters. Generated contract examples now exercise the adapter APIs from `.proto` service definitions through gRPC, HTTP, workflow, and process-backed paths; the next step is connecting the stable metrics/tracing surfaces to production-consumable exporters.
