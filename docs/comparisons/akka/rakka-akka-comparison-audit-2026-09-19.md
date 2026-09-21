# Rakka vs Akka Comparison Audit

- Status: read-only audit; no Rakka code, docs, or tests were changed
- Audit date: 2026-09-19
- Rakka snapshot: `a757ad20c59ed3bee414b37b67c1dad3cd737b13` (branch `rakka-agents`,
  2026-09-15, merge of PR #76 "gap slice 3")
- Akka baseline: Akka libraries BOM `25.10.16` (akka-core `2.10.22`), Akka SDK
  `3.6.3`, Akka Edge Rust `0.8.0` (incubating), as published on
  <https://doc.akka.io/libraries/akka-dependencies/current/> and
  <https://github.com/akka/akka-core> on the audit date
- Predecessors this audit updates:
  - [`docs/rakka-akka-core-gap-report.md`](../../rakka-akka-core-gap-report.md)
    (2026-06-11, Akka Core only, never revised after parity phases 0–7 landed)
  - [`rakka-akka-technical-comparison.md`](rakka-akka-technical-comparison.md)
    (2026-07-09, Akka Libraries plus Akka SDK, written before any agent crate existed)

## 1. Purpose and method

Rakka was designed as a Rust overhaul of Akka. This audit answers one question:
as of the snapshot above, what does Rakka provide, and what does it not, against
what Akka provides today — both the Akka Core libraries and the Akka SDK's
agent layer.

The method was:

1. Inventory Akka from its current documentation: every module in the
   `akka/akka-core` repository, every library in the current dependencies BOM,
   and the Akka SDK reference pages for agents, autonomous agents, memory,
   guardrails, MCP, protocols, and workflows, plus the release notes since
   2026-07-01.
2. Audit every Rakka crate against that inventory by reading the code, not the
   docs: public items, doc comments, and the test or example that exercises each
   capability. Three independent read-only passes covered (a) core, cluster,
   remote, sharding, and the facade; (b) persistence, workflow, streams, process,
   HTTP, gRPC, Kubernetes, testkit, and observability; (c) the six agent-domain
   crates against the Akka SDK.
3. Spot-check every headline claim with a direct grep or read before it entered
   this document. Every claim below that names a file was verified at the
   snapshot; line numbers are from the files at `a757ad2`.

No `cargo` command was run. Status vocabulary throughout:

| Status | Meaning |
| --- | --- |
| Implemented | Parity for the common case, with a test or example naming it |
| Partial | Present, with named gaps |
| Absent | Nothing in the code |
| Different-by-design | Rakka deliberately diverges and documents why |

## 2. Executive summary

**Since the last audit, Rakka has closed the developer-facing Akka Core gaps
the June report called P0, and has built the agent domain the July report said
was missing.** Of the fifteen capability rows in the June gap report, three were
"Absent" or "Needs redesign"; all three are now Implemented (receptionist,
routers, path/uid identity), and the two the June report called "too low-level"
(sharding, persistence) now have Akka-shaped facades. Of the twenty-three rows in
the July agent capability matrix, nineteen were "No" or "traits only"; thirteen
are now Implemented, six Partial, and three remain Absent (MCP client, MCP
server, ACP).

**What Akka still has that Rakka does not.** On the core side the durable gaps
are cluster internals Rakka chose not to build (gossip, split-brain resolver,
Distributed Data, cluster singleton, distributed pub-sub, ShardedDaemonProcess),
actor-runtime depth (stash, dispatchers, behavior decorators, restart windows
and jitter, extension registry, event stream), TLS/mTLS remoting, reliable
delivery controllers and Projections, stream operator breadth and the graph DSL,
persistence event adapters, replication, live and slice queries, the multi-node
test harness, and the Kubernetes operations layer (API discovery, bootstrap,
lease, pod-deletion cost, Helm/operator). On the agent side the gaps are
packaging, not durability: the agents A2A surface is not mounted on any HTTP or
JSON-RPC route, no model provider is wired, MCP is an enum label, the model
response guardrail boundary has no evaluation point, there is no streaming, no
composition root, and the agent crates are neither publishable nor licensed.

**What Rakka has that Akka does not.** A typed task entity distinct from the
run, a ten-dimension escrow budget with settlement exchanges, indeterminate
effect reconciliation as a durable wait, autonomy admission, finite and
continuous goals with a wake controller, communal knowledge claims with a trust
lattice, immutable context snapshots before every model call, cursor-based
replay with explicit window expiry, a 61-scenario recovery roster held to the
tree by a test, two-fidelity fault injection, supervised child-process actors,
and a coordinated shutdown that returns a structured per-task report.

**Defects and drift found while auditing.** Two persistence defects (a query
helper that hangs on result sets over its buffer size; stash and unhandled
directives silently discarded), one untested public supervision strategy, and
seven places where a document or a name promises more than the code does. They
are listed in section 8; none was fixed, per the audit's scope.

Size of the workspace at the snapshot, for scale:

| Measure | Value |
| --- | --- |
| Crates / examples | 22 / 26 |
| Source lines (all crates) | ~223,000 |
| Source lines in the six agent-domain crates | ~167,600 (75%) |
| Integration test files / test functions | 201 / 2,202 |
| Publishable crates | 15 (no agent-domain crate is publishable) |
| Repository license | All rights reserved; no release license declared |

## 3. Akka baseline

### 3.1 Akka libraries BOM 25.10.16

| Library | Version | Rakka counterpart |
| --- | --- | --- |
| Akka core (actors, cluster, persistence, streams) | 2.10.22 | `rakka-core`, `rakka-cluster`, `rakka-remote`, `rakka-sharding`, `rakka-persistence`, `rakka-stream` |
| Akka HTTP | 10.7.5 | `rakka-http` (axum adapters) |
| Akka gRPC | 2.5.12 | `rakka-grpc` (tonic adapters) |
| Akka Persistence R2DBC / JDBC / Cassandra / DynamoDB | 1.3.17 / 5.5.5 / 1.3.5 / 2.0.14 | `rakka-persistence-postgres` only |
| Akka Projections | 1.6.23 | none |
| Akka Management (bootstrap, health, cluster HTTP, k8s lease, rolling updates) | 1.6.5 | `rakka-k8s` (health, drain), `rakka-discovery-etcd` |
| Akka Diagnostics | 2.2.3 | none |
| Alpakka / Alpakka Kafka | 10.0.4 / 8.0.2 | none |
| Akka Edge Rust (incubating) | 0.8.0 | not comparable; see 3.3 |
| Akka Insights | 2.22 | `rakka-core` metrics + OTel bridge |

Akka is cross-built for Scala 2.13 and 3.3.7 under Business Source License 1.1,
with artifacts behind a tokenized repository.

### 3.2 akka-core modules mapped to Rakka crates

| akka-core module | Rakka crate | Coverage |
| --- | --- | --- |
| akka-actor, akka-actor-typed | rakka-core | Partial (section 4.1) |
| akka-actor-testkit-typed, akka-testkit | rakka-testkit | Partial (4.11) |
| akka-cluster, akka-cluster-typed | rakka-cluster | Partial / Different-by-design (4.3) |
| akka-cluster-sharding, -typed | rakka-sharding, rakka-sharding-postgres | Implemented facade, Partial internals (4.5) |
| akka-cluster-tools (singleton, pub-sub, reliable delivery) | none | Absent |
| akka-cluster-metrics | rakka-cluster (member counts only) | Partial |
| akka-distributed-data | none | Absent by design |
| akka-coordination (leases) | rakka-sharding coordinator lease, rakka-discovery-etcd | Partial |
| akka-discovery | rakka-cluster, rakka-k8s, rakka-discovery-etcd | Partial (4.10) |
| akka-remote | rakka-remote | Partial (4.4) |
| akka-serialization-jackson, akka-protobuf-v3 | rakka-remote registry (Protobuf) | Different-by-design |
| akka-persistence, -typed, -query | rakka-persistence | Implemented with defects (4.6) |
| akka-persistence-testkit, -tck | rakka-testkit | Partial |
| akka-stream, akka-stream-typed | rakka-stream | Partial (4.8) |
| akka-stream-testkit | rakka-testkit | Implemented |
| akka-multi-node-testkit | none | Absent |
| akka-pki, akka-slf4j | none / `tracing` | Absent / Partial |

### 3.3 Akka SDK since the July audit

Akka SDK is at 3.6.3. The release notes from 2026-07-01 to the audit date list
3.6.1 (function tools may return images, PDFs, and multi-content results;
generic-array JSON schema; TestKit gRPC endpoint generation), 3.6.2 and 3.6.3
(dependency and error-reporting maintenance), and a series of CLI releases
adding "Akka Specify", a spec-driven agent development workflow. No new
runtime agent capability was introduced after July. The AutonomousAgent contract
the July report described stands: typed task definitions and results, task
rules and templates, dependencies with automatic cancellation, human-driven
tasks, delegation, handoff, team leadership, and moderation with per-pattern
concurrency knobs, iteration limits, dynamic setup, attachments, a push-style
notification stream, session memory with limited-window, read-only, write-only,
filtered, and custom providers plus interception and compaction, request and
response guardrails with a category taxonomy and a built-in similarity guard,
and native MCP (server with tools, resources, prompts; client), A2A, and ACP.

Akka's only Rust offering is Akka Edge Rust 0.8.0 (last released 2025-02-01,
incubating): event sourcing, a file-log persistence adapter, projections over
gRPC, and an offset store for edge devices. It has no actors, no cluster, and
no HTTP server. It is not a Rust Akka and is not a competitor to Rakka's scope.

## 4. Akka Core parity by area

### 4.1 Typed actor runtime

| Akka capability | Rakka | Status |
| --- | --- | --- |
| Behaviors DSL (setup, receive, same, stopped) | `Actor` trait, `Behavior<M>` closure trait, `actor_fn`, `setup`, `ActorAction::{Continue, Stop}` (`crates/rakka-core/src/actor.rs`) | Partial: no behavior transitions, no `unhandled` routing to dead letters, `actor_fn` is synchronous by decision |
| withTimers / withStash / intercept / monitor / logMessages / withMdc | none | Absent |
| ActorSystem builder, terminate, when_terminated | `ActorSystem::builder`, `terminate`, `terminate_with_report`, `when_terminated` (`system.rs`) | Implemented |
| Guardian / root behavior, system actors hierarchy | none; the system is a flat spawn registry | Absent |
| Config / settings | `ActorSystemRuntimeSettings`, `ActorSystemShutdownConfig`, operational defaults | Partial: no file config; the `RAKKA_*` env vars in the security doc are examples no crate parses |
| Extensions registry | none; `Cluster::get` and `ClusterSharding::get` build a fresh instance per call (`crates/rakka-cluster/src/facade.rs:45`) | Absent |
| Event stream | none; only `subscribe_dead_letters` and cluster event broadcast | Absent |
| Dead letters | `DeadLetter` on MailboxFull / MailboxClosed, lossy broadcast | Partial: no unhandled-message dead letters |
| ActorRefResolver | `ActorRefResolver` for live local refs | Implemented (local only) |
| Logical path vs uid, unique child names | `ActorPath` + `ActorUid`, duplicate live names rejected | Implemented (the June P0) |
| ActorContext: children, child, spawn, spawn_anonymous, stop, watch, watch_with, unwatch, receive timeout, message_adapter, ask, ask_with_status, pipe_to_self | all present (`actor.rs:839–1186`) | Implemented (the June P0); children are paths not refs, and are not stopped on parent restart |
| Timers | `schedule_once`, keyed `start_timer_once`, `cancel_timer` | Partial: no fixed-delay or fixed-rate timers; timers survive restart, cancelled only on stop |
| Mailboxes | bounded `mpsc` only, `tell` returns `TellError::Full` | Different-by-design; no unbounded, priority, or control-aware mailboxes |
| Dispatchers / blocking pools | `DispatcherHint::Blocking` recorded, never consumed | Absent (declared) |
| Stash | none in rakka-core | Absent |
| Supervision | Resume, Restart, Stop, Escalate, RestartWithBackoff (`supervision.rs`) | Partial: Escalate stops instead of consulting the parent; no restart window, jitter, reset-after, or per-error strategy; `RestartWithBackoff` has no test anywhere in the workspace |
| DeathWatch, ask with timeout | local watch, `ActorRef::ask` with `ReplyTo` oneshot | Implemented (local); no remote watch; `ReplyTo` cannot cross the wire |
| Scheduler / executor access | none | Absent |

### 4.2 Receptionist and routers

| Akka capability | Rakka | Status |
| --- | --- | --- |
| ServiceKey, register, deregister, find, subscribe | `Receptionist::get`, typed `ServiceKey<M>`, drop-deregistering handles, subscriptions with initial listing (`crates/rakka-core/src/receptionist.rs`) | Implemented |
| Clustered receptionist | `ClusteredReceptionist` model plus TCP propagation with remote service proxies (`rakka-cluster`, `rakka-remote`) | Partial: the application drives publish, propagate, prune, and expire on its own timer; listing wire format is JSON |
| Pool and group routers | `Routers::pool`, `Routers::group`, round-robin, random, consistent hash with virtual nodes (`routers.rs`) | Implemented |
| Broadcast, smallest-mailbox, scatter-gather, resize | none | Absent |
| Router as actor (registrable, watchable) | routers are handles, not actors | Different-by-design |

### 4.3 Cluster

| Akka capability | Rakka | Status |
| --- | --- | --- |
| Cluster extension: join, leave, down, self member, state, subscriptions with replay | `Cluster::get`, `ClusterManager`, `ClusterSubscriptions` with InitialState / InitialEvents / LiveOnly (`crates/rakka-cluster/src/facade.rs`) | Implemented; not a per-system singleton, and the sharding runtime keeps its own membership table that must be mirrored by hand |
| Gossip membership, convergence, leader | deterministic per-node membership from discovery snapshots | Different-by-design (strategy doc "will not build") |
| Failure detector | `TimeoutFailureDetector` fed only by discovery or explicit heartbeat; the TCP transport has no heartbeat frame | Partial: no phi-accrual, no transport-derived liveness |
| Reachability table, WeaklyUp, Exiting, PreparingForShutdown states | folded into member state; states absent | Partial |
| Roles | `NodeRole` stored, no consumer anywhere | Partial (data only) |
| min-nr-of-members | `min_contact_points` counts Joining members too | Partial |
| Split-brain resolver / downing | `TimeoutDowningStrategy` default, `NoDowningStrategy` | Different-by-design; note the shipped default is timeout auto-down, which Akka removed as unsafe, and the crates do not enforce the external-arbiter contract that makes it safe |
| Leases | etcd registration lease; Postgres and in-memory shard coordinator lease with fencing token | Partial: no general `Lease` API, no Kubernetes Lease object backend |
| Cluster singleton | none | Absent |
| Distributed pub-sub | none | Absent |
| Distributed Data / CRDTs | none | Absent by design |
| Cluster metrics | member-count gauges | Partial |
| Discovery | `StaticDiscovery`, `LocalDiscovery`, `EtcdDiscovery`, `KubernetesDnsDiscovery` | Partial: the Kubernetes provider performs no DNS or API lookup, it formats hostnames for a caller-supplied pod list (`crates/rakka-k8s/src/lib.rs:179–235`); no Kubernetes API provider |
| Cluster bootstrap | none; seed join plus discovery polling driven by the application | Absent |
| Peer self-fencing (Rakka's documented substitute) | `SelfFenceDetector` policy core and etcd lease revoke exist; the transport-health signal, shutdown and readiness hooks, and fail-fast remote routing exist only in example code and `rakka-a2a` | Partial; the strategy doc's "implementation status" overstates this |

### 4.4 Remoting

| Akka capability | Rakka | Status |
| --- | --- | --- |
| Transport | TCP, length-prefixed frames, handshake with protocol and envelope version, per-peer worker (`crates/rakka-remote/src/network.rs`) | Implemented; no Aeron, no lanes, no compression |
| TLS / mTLS | no TLS dependency in any manifest | Absent (documented limitation) |
| Serialization | explicit `SerializationRegistry`, Protobuf codecs, schema compatibility policy (exact, additive window, N+1) | Implemented, Different-by-design versus config-bound serializers |
| Envelope versioning | `RemoteEnvelope` metadata, exact envelope-version handshake | Implemented |
| Quarantine / association lifecycle | connection lifecycle states and snapshots; no quarantine, no system-message resend buffer | Partial |
| Remote deployment | none | Absent by design |
| Remote watch | none; `ActorTerminated` never crosses the wire | Absent |
| Frame size limit, bounded send queues | 16 MiB frames, 1024-deep per-peer queue, fail-fast `QueueFull` | Implemented |
| Security posture | known-peer admission, loopback default bind, protocol handshake | Partial: no authentication beyond the plaintext node id, no TLS |
| Remote actor-ref / service delivery | `RemoteDestination::{Entity, Service, Reply, ActorRef}` dispatched; `ActorPath` and `RouteKey` destinations exist on the wire but fail closed | Partial |

### 4.5 Cluster sharding

| Akka capability | Rakka | Status |
| --- | --- | --- |
| ClusterSharding facade, init, EntityTypeKey, Entity, EntityContext, entity_ref_for, EntityRef, ShardRegion | all present (`crates/rakka-sharding/src/facade.rs`), local and networked variants | Implemented (the June P0); networked use still needs `ClusterNodeRuntime` plus a registry and transport config, across nineteen constructors rather than one settings object |
| Coordinator | per-node deterministic `ShardCoordinator` over a durable revision-CAS store (in-memory, PostgreSQL) with an optional fencing lease | Different-by-design: no elected coordinator actor; with a lease configured a non-holder cannot host regions (single-coordinator topology) |
| Allocation strategies | deterministic modulo, least-shard, custom trait | Partial: no slice-range or external allocation; least-shard is safe only under a single coordinator |
| Rebalance and graceful handoff | membership-driven rebalance, draining/transferring/acquired handoff states, per-shard buffering with overflow policy | Implemented; handoff is executed by the leaving node, no coordinator-mediated ack barrier (by decision) |
| Passivation | idle timeout, explicit `Passivate`, self-stop, buffering during passivation | Partial: no active-entity-limit strategies (LRU, MRU, LFU) or hill-climbing |
| Remembered entities | store plus replay-on-acquire, in-memory and PostgreSQL | Implemented (decision record explains the divergence from event-sourced shard state) |
| Proxy-only mode | `init_proxy` | Implemented |
| Shard stats / queries | per-node `ClusterShardingState` | Partial: no cluster-wide aggregation |
| ShardedDaemonProcess | none | Absent |
| Sharding delivery | at-most-once, typed `EntityTellError` / `EntityAskError` | Different-by-design (documented) |
| Multi-node example | `examples/multi-node-sharding` still uses component crates and low-level route, region, and coordinator types rather than the facade; `clustered-counter-http` and `-grpc` are the facade-first examples | Implemented |

### 4.6 Persistence

| Akka capability | Rakka | Status |
| --- | --- | --- |
| PersistenceId::of with separator validation | `PersistenceId::of` (`crates/rakka-persistence/src/store.rs:44`) | Implemented |
| EventSourcedBehavior with command and event handlers | builder with synchronous handlers plus an async `EventSourcedActor` trait (`behavior.rs`, `event_sourced.rs`) | Implemented |
| Effect API: persist, persist_all, none, unhandled, stash, unstash_all, then_reply, then_run, then_stop, reply, no_reply | all constructors exist (`event_sourced.rs:249–379`) | Partial with a defect: the runtime discards the stash and unhandled directives (`event_sourced.rs:665`); a stashed command is dropped, and unhandled is indistinguishable from none |
| Snapshots and retention | `RetentionCriteria::snapshot_every / keep_snapshots / delete_events_on_snapshot`, explicit `then_snapshot` | Implemented; count-based only, no predicate snapshotting, save runs inline on the command path |
| Recovery customisation | `RecoveryOptions` (snapshot selection, replay range) | Partial: no disabled recovery, no `RecoveryFailed` signal, whole replay range loaded into memory |
| Event tagging | per-effect tags, Postgres GIN index | Implemented; no behavior-level tagger, no offsets |
| Event adapters / schema evolution | none; opaque byte codecs only, no manifest or version column | Absent |
| Persist failure backoff | fixed-delay `PersistFailureBackoff` | Partial: no exponential backoff or jitter; no failing-store test exists |
| Signals | RecoveryStarted/Completed, EventsPersisted, PersistFailed, SnapshotSaved/Failed, PreRestart, PostStop | Implemented; no RecoveryFailed or delete-completed signals; no test asserts signal order |
| DurableStateBehavior | builder, `DurableEffect`, revision CAS, delete | Implemented |
| Replicated Event Sourcing | none | Absent |
| Persistence query | `current_events_by_persistence_id`, `current_events_by_tag`, `current_persistence_ids`, `current_durable_state_by_id` as bounded stream sources (`query.rs`) | Partial with a defect: the `events_by_*` and `persistence_ids` names are aliases of the current-only helpers, not live queries; the helpers push the whole result into a 1024-item bounded channel before returning the source, and the sink blocks on full, so any result set over 1024 items never returns (`query.rs:121–141`, `crates/rakka-stream/src/lib.rs:446–467`) |
| eventsBySlices, durableStateChanges | none; the Postgres `slice` column is never populated | Absent |
| Backends | in-memory and PostgreSQL journal, snapshot, durable state | Implemented (two backends; Akka lists four production plugins) |
| Persistence testkit | `PersistenceTestKit`, `EventSourcedBehaviorTestKit`, `DurableStateBehaviorTestKit` | Partial: no failure or rejection injection policy |
| Migrations | idempotent advisory-locked DDL | Partial: no versioned migration history |
| Recovery after shard movement | `examples/sharded-cart-persistence` | Implemented |

### 4.7 Reliable delivery and projections

| Akka capability | Rakka | Status |
| --- | --- | --- |
| Point-to-point / work-pulling / sharding reliable delivery controllers | none; `DurableInbox` acceptance with dedup keys (`crates/rakka-workflow`) | Different-by-design: durable acceptance plus idempotency key, no sequence numbers, resend, or demand window |
| Durable producer queue | durable outbox with retry policy, jitter, scheduled dispatch, `Dispatching` persisted before the effect | Partial: one revision-CAS blob per workflow, not an append-only queue |
| Delivery to actors and entities | `OutboxTarget::{actor, entity}` are descriptive; the application implements `OutboxDispatcher` | Absent (routing) |
| Akka Projections (source providers, offset stores, exactly-once handlers) | none; the only projection is the A2A task read model | Absent |
| Broker connectors (Alpakka, Kafka) | none | Absent |
| Manual clock, compaction, telemetry | `ManualWorkflowClock`, `WorkflowStateCompactionPolicy`, `WorkflowTelemetryEvent` | Implemented |

### 4.8 Streams

Rakka's facade offers nine operators on `Source` (`map`, `map_async`, `filter`,
`take`, `via`, `merge`, `merge_all`, `broadcast`, `to`) and a single-transform
`Flow` (`identity`, `from_fn`, `from_async_fn`) that cannot be composed with
another `Flow`. Akka's operator index lists roughly two hundred operators across
seventeen families.

| Akka capability | Rakka | Status |
| --- | --- | --- |
| Source / Flow / Sink with linear operators | facade above (`crates/rakka-stream/src/facade.rs`) | Partial |
| Materialized values, Keep, viaMat, toMat | sink-side mat value only (`collect`, `fold`, counts) | Partial |
| run_with / run_collect / run_foreach | present | Implemented |
| Backpressure | bounded buffers, `send` awaits space, `try_send` fails fast, pressure metrics | Implemented (buffer-based, not demand-signal based) |
| KillSwitch / cancellation | per-handle `cancel`, `take` cancels upstream | Partial: no shared kill switch |
| Restart / supervision / recover | none; errors are fail-stop | Absent |
| Graph DSL, zip, balance, partition, concat, interleave | none | Absent (scoped out by the phase 6 decision) |
| throttle, buffer overflow strategies, delay, grouped, scan, and the rest of the operator families | none | Absent |
| Actor interop with ack | `Sink::actor_ref`, `Sink::actor_ref_with_ack`, `Source::actor_ref`, `Source::actor_ref_with_ack` | Implemented |
| Entity sink | `Sink::entity_ref`, `Sink::sharded_entity_ref` | Implemented (no Akka equivalent) |
| Reactive Streams / `futures::Stream` interop | none in core; one-way adapters in `rakka-grpc` and `rakka-http` | Absent (core) |
| StreamRefs | none | Absent |
| Process stdio adapters | `Source::process_stdout`, `ProcessInputSink` | Implemented (no Akka equivalent) |
| Stream testkit | source, sink, and demand probes | Implemented |

### 4.9 Coordinated shutdown

This is the one area where Rakka is at or ahead of parity.

| Akka capability | Rakka | Status |
| --- | --- | --- |
| Built-in phases with a configurable DAG | nine Rakka-native phases, custom phases before/after, cycle rejection (`crates/rakka-core/src/coordinated_shutdown.rs`) | Implemented |
| add_task with per-task timeout and failure policy | present | Implemented; no cancellable task handle |
| Reasons | actor-system terminate, Kubernetes pre-stop, user request | Partial: no cluster-originated reasons; downing or incompatible config do not trigger shutdown |
| Idempotent run, shared result, structured report | `CoordinatedShutdownReport` with per-task status | Implemented (Akka returns `Future[Done]`) |
| ActorSystem.terminate runs shutdown | yes | Implemented |
| JVM hook / exit | opt-in OS-signal helpers in `rakka-k8s` | Different-by-design |
| Kubernetes pre-stop | `KubernetesDrainController`, `/drain` route, manifest wiring | Implemented |
| Adapter tasks (HTTP, gRPC, streams, persistence, process, sharding) | registration helpers in every adapter crate | Implemented |
| Metrics and testkit | shutdown metrics, `CoordinatedShutdownTestKit` | Implemented |

### 4.10 HTTP, gRPC, management, discovery, Kubernetes

| Akka capability | Rakka | Status |
| --- | --- | --- |
| Akka HTTP routing DSL, marshalling, client | axum re-export with actor and entity route builders, JSON and bytes | Different-by-design; no client |
| Streaming (chunked, SSE, WebSocket, request body) | all four | Implemented |
| Auth, TLS, CORS, rate limiting | none | Absent (declared) |
| Akka gRPC codegen, streaming, health, reflection | tonic helpers for unary and all three streaming shapes | Different-by-design; no health service, reflection, or TLS helpers |
| Akka Management health checks | `KubernetesNodeHealth` readiness and liveness with compatibility and drain gating | Implemented; fixed fields, not pluggable named checks |
| Cluster HTTP management | none | Absent |
| Cluster bootstrap | none | Absent |
| Kubernetes lease | none in `rakka-k8s` | Absent |
| Rolling updates (pod deletion cost, app version) | compatibility-gated readiness, N/N+1 policy, StatefulSet manifest | Partial |
| Pre-stop drain | implemented | Implemented |
| Manifests, Helm, operator | reviewable YAML and a gated script; no Helm or operator | Partial (declared) |
| Metrics export routes | Prometheus text, OTel bridge JSON, operational snapshots | Implemented |

### 4.11 Testkit

| Akka capability | Rakka | Status |
| --- | --- | --- |
| ActorTestKit owning a system | none; tests build `ActorSystem` directly | Partial |
| TestProbe: expect_message, expect_no_message, expect_terminated | present | Implemented |
| expect_message_type, receive_messages, await_assert, fish_for_message, within | none | Absent |
| Manual time | none for actor timers (`ManualWorkflowClock` covers retry clocks only) | Absent |
| BehaviorTestKit (synchronous effect capture) | persistence behavior kits only | Partial |
| LoggingTestKit | none | Absent |
| Serialization testkit | compatibility fixture over the registry | Partial |
| Stream, persistence, shutdown, HTTP, gRPC kits | present | Implemented (persistence lacks failure injection) |
| Multi-node testkit (roles, barriers, test conductor, partition injection) | in-process mixed-version cluster tests; a gated multi-process example run | Absent as a harness |

### 4.12 Observability

| Akka capability | Rakka | Status |
| --- | --- | --- |
| Metrics instrumentation across subsystems | backend-neutral `MetricsRecorder`, stable metric names in core, cluster, sharding, persistence, streams, HTTP, gRPC, k8s, shutdown | Implemented (smaller catalogue; `rakka-workflow` records none) |
| Prometheus exposition | `export_prometheus_text` | Implemented (summary-style histograms) |
| OTLP export | SDK-free bridge model; no published crate ships an exporter | Partial (by decision) |
| Logging with MDC / span propagation | `tracing` at HTTP, gRPC, and stream boundaries only; persistence, workflow, process, k8s, and testkit crates emit no `tracing` events | Partial |
| Diagnostics (starvation detector, config checker) | none | Absent |

### 4.13 Process actors (Rakka only)

`rakka-process` has no Akka counterpart: supervised child processes with
readiness, health checks, restart budgets with jitter, and operational
snapshots; process-backed sharded entities whose child moves with the shard;
stdio protocol actors with line and JSON codecs, one-shot execution with output
caps, file-watch sandboxes, and local-endpoint readiness; conservative security
defaults (absolute executables on an allowlist, no environment inheritance,
null stdio); coordinated shutdown and Kubernetes drain integration; and an
in-crate testkit with a fixture binary. Declared limits: children run inside the
node container, no per-actor sidecars, not an OS sandbox.

## 5. Rakka Agents versus the Akka SDK

### 5.1 The July matrix, re-scored

| Agent capability | Akka SDK 3.6 | July 2026 | Now |
| --- | --- | --- | --- |
| Request-based model agent | Yes | Adapter traits only | Different-by-design: every model call is a durable effect on a sharded run; no ephemeral single-turn path |
| Durable autonomous loop | Yes | No | Implemented |
| Typed task definitions and results | Yes | No | Implemented (schema references plus six deterministic rule kinds, not a JSON Schema engine) |
| Task dependencies | Yes | Graph edges only | Implemented, with dependent cancellation |
| Durable task and agent state | Yes | Run state only | Implemented: five sharded entity classes |
| Delegation | Built in | A2A peer effect | Implemented in-fabric |
| Handoff | Built in | Modelable | Implemented |
| Team / shared backlog | Built in | No | Implemented; claims arrive by A2A or typed client only, the model-tool hook is dormant |
| Moderated conversation | Built in | No | Implemented; turns are submitted through the surface, nothing auto-drives a participant's model turn |
| Human approval | Yes | Checkpoints | Implemented: effect checkpoints plus human-owned tasks |
| Iteration budgets | Yes | Generic policy | Implemented: ten-dimension escrow; run-level wall clock is inherited but never enforced |
| Token / cost / delegation budgets | Iteration and token controls | Generic | Implemented, richer than Akka |
| Model providers | Nine plus custom | None | Partial: `RigModelAdapter` over `rig-core 0.37`; no provider is constructed anywhere in the repository; every example uses the deterministic adapter |
| Function tools | Yes | Traits | Partial: registry, bindings, authority, and a dispatch trait exist; the model request carries no tool list, so the model is never told which tools exist beyond the result tool; no derive or schema generation |
| MCP client | Yes | No | Absent (an enum label only) |
| MCP server | Yes (tools, resources, prompts) | No | Absent |
| A2A | Client and platform patterns | Durable server | Partial: the workflow surface is fully mounted with streaming, push, and replay; the agents surface exists only in-process behind SDK types with no HTTP or JSON-RPC route binding and no outbound network client |
| ACP | Client | No | Absent |
| Session memory | Event-sourced, configurable modes | No | Implemented per run scope; no read-only, write-only, or filtered modes; no shipped compactor |
| Long-term / shared memory | Entities plus external vector stores | No | Implemented write side: pgvector private memory; communal knowledge graph whose read path into a model context is absent |
| Guardrails | Model and MCP request and response | Policy substrate | Partial: chains evaluate at model request, tool request, tool response, and memory ingress; the model response, A2A ingress, and A2A egress boundaries have no evaluation point |
| Attachments | URI and object loaders | Artifact refs | Partial: references only; binary and URL message parts refuse on the wire |
| Agent registry | Yes | No | Partial: static application-supplied catalog; no dynamic registry or enumeration |
| Notifications | Live, non-replayable stream | Runtime events plus A2A replay | Implemented replay with cursors and explicit window expiry (a differentiator); no push or stream on the agents surface |
| Testkit | SDK testkit | Workflow and A2A only | Implemented: deterministic model, scripted dispatcher, crash sweeps, kill windows, conformance suites |
| Unified client | ComponentClient | No | Partial: `RakkaAgentClient` covers tasks, management, and reads; teams, conversations, checkpoints, admission, and memory commands go through the service or entity commands |

### 5.2 Structure of the agent domain

Five sharded entity classes (`Agent`, `Task`, `Run`, `Team`, `Conversation`),
eighteen exchange kinds held to `AgentExchangeKind::ALL` by a test, fourteen
validated identity types, forty operation kinds, eight effect kinds, fifteen run
statuses, ten budget dimensions of which eight are conserved, twenty-four
telemetry segment classes, thirty-seven catalogued metrics, and sixty-one
rostered recovery scenarios each bound to a test. The definition is a durable
versioned record whose settings and setup revisions can only narrow authority,
with timing classes and compare-and-set. Admission verifies eight requirements
before unattended work. Every model call is preceded by an immutable context
snapshot, and every effect carries a safety class, generation, and external
idempotency key, with indeterminate outcomes parked for reconciliation rather
than retried.

### 5.3 Agent-domain gaps in detail

| Area | Gap | Evidence |
| --- | --- | --- |
| A2A mounting | `RakkaAgentA2AService` is not referenced by the request handler, routes, or router; the only `impl RequestHandler` is the workflow surface's | `crates/rakka-a2a/src/handler.rs:1922`; plan line "the agents surface has no route binding at all, deferred since 1.12" |
| A2A federation | no outbound network client; delegation and handoff executors run in-process over the same service core | no `impl AgentA2APeerAdapter` in crates or examples |
| Streaming | no model-token streaming; the Rig fake refuses `stream`; no SSE on the agents surface | `crates/rakka-agent/src/rig.rs:463–470` |
| Providers | `rig-core` pinned at 0.37 with default features off; no `providers::` use anywhere | grep across crates and examples |
| Tool visibility | `AgentToolRegistry::model_visible` is called only from a unit test; `AgentModelRequest` has no tool list | `crates/rakka-agent/src/tools.rs:3167` |
| MCP | `AgentToolKind::RemoteMcp` label only | four `mcp` hits, all in `tools.rs` |
| Guardrails | `AGENT_EVALUATED_GUARDRAIL_BOUNDARIES` names four of the seven spec boundaries | `crates/rakka-agent/src/tools.rs:128–133` |
| Run suspend and timer | `AgentRunStatus::Suspended`, `WaitingForTimer`, and `AgentLoopState::pending_timer` have no writer | `run.rs:309, 322`; `loop_runtime.rs:519` |
| Wall clock | `max_wall_clock_millis` appears in no run, dispatch, or loop file | grep |
| Goal evaluation | the `VerificationWorkflow` method answers `evaluation-workflow-deferred` | `crates/rakka-agent/src/dispatch.rs:3738–3744` |
| Communal read | `MemoryContextSnapshot::communal_claims` is permanently empty | security matrix "Owed" |
| Knowledge graph retention | no retention, tombstone, or deletion on any backend | security matrix "Owed" |
| Process tools | the substrate's `ProcessFileWatchToolAdapter` is on a different seam from `AgentDispatchToolExecutor`; `rakka-agent` never references it | grep |
| PostgreSQL for entities | no test or example runs the five entities on `PostgresDurableStateStore`; the multi-pod soak uses a shared directory | fault matrix "Production interpretation" |
| Kubernetes | no autoscaling signal; `rakka-agent` has no drain hook of its own | grep |
| Composition | wiring a deployment means hand-building the eight-store service, five sharding registrations, dispatcher fleet, wake scanner, courier, memory bundle, authority, and executors | `examples/*/src/wiring.rs` |
| Telemetry | thirteen segment classes have no production call site; five audit families have durable state and no event; decision vocabulary mostly unwritten | telemetry matrix "Owed" |
| Publishing | all six agent crates are `publish = false`; the repository declares no license | `Cargo.toml`, `LICENSE` |

### 5.4 The July recommendations, re-scored

| July recommendation | Status now |
| --- | --- |
| 1. First-class `rakka-agent` crate with definition, typed tasks, budgets, guardrail chains, clients, deterministic testkit | Done, except the ergonomic composition root |
| 2. Durable Rig loop as a bounded state machine | Done |
| 3. Delegation, handoff, team, moderation as first-class capabilities | Done in-fabric; the A2A carrier for federation is unbuilt |
| 4. Typed tasks above A2A projections | Done |
| 5. Event-sourced session memory with windows, modes, interceptor, compaction, custom store | Partial: windows, interceptor, and custom store done; read-only, write-only, and filtered modes and a shipped compactor are not |
| 6. Policy into runtime guardrails | Partial: four of seven boundaries |
| 7. Preserve the replay advantage | Done |
| 8. Unified component client | Partial |
| 9. Production Kubernetes package (Helm or operator, mTLS, arbiter, PDB, autoscaling, Collector) | Not done; Collector topology and manifests exist, the rest does not |
| Phase 5, enterprise operations: license, releases, mTLS, self-fencing, chaos and upgrade evidence | Not done |

## 6. Movement since the June 2026 Akka Core gap report

| June row | June status | Now |
| --- | --- | --- |
| Typed actor behavior | Partial | Partial (closure behaviors, `actor_fn`, `setup`; no transitions or decorators) |
| Actor context | Partial | Implemented (periodic timers and log context still missing) |
| Actor identity and paths | Needs redesign | Implemented |
| Actor system lifecycle | Partial | Partial (builder and terminate done; guardian, extensions, event stream, config not) |
| Supervision | Partial | Partial (unchanged; backoff strategy untested) |
| Timers and scheduling | Partial | Partial (keyed once-timers; not restart-managed) |
| Receptionist and service discovery | Absent | Implemented locally, Partial clustered |
| Routers | Absent | Implemented (no broadcast) |
| Cluster membership API | Partial | Partial (facade done; gossip, SBR, singleton, pub-sub, DData absent, mostly by design) |
| Cluster sharding | Partial, too low-level | Implemented facade; Partial passivation strategies and allocation |
| Persistence | Durable-state subset | Implemented event sourcing and durable state; Partial query; two runtime defects |
| Streams | Minimal subset | Partial facade (nine operators) |
| Remoting and serialization | Partial | Partial (unchanged; TLS, quarantine, remote watch absent) |
| Testkit | Partial | Partial (manual time, sync behavior kit, log capture, multi-node absent) |
| Distributed tools | Absent | Absent |

Of the seven "first implementation slices" the June report proposed, all seven
landed: identity cleanup, context ergonomics, sharding facade, prelude crate,
testkit expansion, persistence ergonomics, and receptionist plus routers.

## 7. Capabilities each side lacks

### 7.1 Akka has, Rakka does not

Cluster: gossip membership, phi-accrual failure detection, split-brain resolver
strategies, cluster singleton, distributed pub-sub, Distributed Data and CRDTs,
roles as a routing input, leader events, cluster bootstrap, Kubernetes API
discovery, Kubernetes lease, cluster HTTP management, pod-deletion cost.
Actors: stash, dispatchers and pinned or blocking pools, priority and
control-aware mailboxes, behavior decorators, restart windows and jitter,
per-error supervision, true parent escalation, extension registry, event
stream, periodic timers, remote watch, remote deployment. Remoting: TLS and
mTLS with certificate rotation, quarantine, large-message lanes, Aeron.
Sharding: active-entity-limit passivation, slice-range and external allocation,
ShardedDaemonProcess, an elected coordinator that decouples coordination from
hosting. Persistence: event adapters and manifests, replicated event sourcing,
live and slice queries, durable-state change streams, Cassandra, DynamoDB, and
JDBC backends, persistence failure injection. Delivery: reliable delivery
controllers, durable producer queue as an append-only log, Projections with
offset stores, Kafka and Alpakka connectors. Streams: the graph DSL and roughly
one hundred ninety operators including throttle, buffer strategies, zip,
balance, partition, restart wrappers, StreamRefs, Reactive Streams interop.
Testing: multi-node testkit with barriers and partition injection, manual time,
synchronous behavior testing, log capture. Agents: MCP client and server, ACP,
a mounted network A2A surface for agents, wired model providers, token
streaming, response guardrails with a built-in similarity guard, memory modes,
a compactor, a dynamic agent registry, per-agent cards, attachments on the
wire, a push notification stream, a builder-style workflow DSL, a composition
root. Product: a release license, published artifacts, commercial support,
managed operations, multi-region.

### 7.2 Rakka has, Akka does not

A typed task entity separate from the run with assignment generations, a
dependency graph with failure policy, human ownership, and cursor-paged history.
A ten-dimension escrow budget with allocation, settlement, and return exchanges
and top-up parking. Indeterminate-effect reconciliation as a durable wait with
dispatcher kill windows. Autonomy admission. Finite and continuous goals, a wake
controller with derived wake ids, coalescing, backoff, windows, stagnation
detection, and five evaluation methods. Cancellation propagation with subtree
quiescence. Immutable context snapshots before every model call. A communal
knowledge graph with provenance, a trust lattice, a promotion gate, a portable
store interface, and a conformance harness. Narrowing-only settings and setup
revisions with wait invalidation while parked. Cursor replay with explicit
window expiry. A sixty-one scenario recovery roster held by a test, in-process
crash sweeps at every write, and real-process abort with shard takeover.
Struggle signals and operational snapshots that stay correct with telemetry
down. Catalogue-as-data telemetry with a source scan and an OTLP acceptance
binary. Supervised child-process actors and process-backed entities. Coordinated
shutdown with a structured per-task report. Explicit `Result`-returning sends on
bounded mailboxes, explicit Protobuf schema compatibility windows, and an
external-arbiter cluster design aimed at Kubernetes.

## 8. Defects and documentation drift found

None of these was changed; they are recorded for follow-up.

1. Persistence query helpers hang on large results. `stream_items` sends every
   record into a 1024-capacity bounded channel before returning the source, and
   `StreamSink::send` waits for a reader that does not exist yet
   (`crates/rakka-persistence/src/query.rs:121–141`,
   `crates/rakka-stream/src/lib.rs:446–467`). No test exceeds the buffer.
2. Event-sourced stash and unhandled are inert. `apply_effect` destructures
   the directives into `_stash` and `_unhandled` and never reads them
   (`crates/rakka-persistence/src/event_sourced.rs:665`). The migration notes
   advertise `EventSourcedEffect::stash()`.
3. Live query names are aliases. `events_by_persistence_id`, `events_by_tag`,
   and `persistence_ids` call their `current_*` counterparts
   (`query.rs:27–82`).
4. `SupervisionStrategy::RestartWithBackoff` is public and documented and has
   no test in the workspace.
5. `Cluster::get(&system)` builds a fresh, unshared membership on every call
   (`crates/rakka-cluster/src/facade.rs:45`); phase 5 documentation presents it
   as an Akka-style extension entry point.
6. `KubernetesDnsDiscovery` resolves nothing; it formats hostnames for a
   caller-supplied pod list (`crates/rakka-k8s/src/lib.rs:179–235`). The
   cluster coordination strategy document also promises a Kubernetes API
   variant that does not exist, and its "implementation status" section counts
   as landed three actions that exist only in example code.
7. `docs/rakka-agents.md:260–262` says `rakka-a2a` "serves the entities as A2A
   agents"; the agents service has no route or request-handler binding.
   `docs/rakka-agents.md:348` cites `examples/clustered-sharded-entity-a2a-agents`
   as the adapter over agent entities; that example enables only the workflow
   surface features.
8. `AgentRunStatus::{Suspended, WaitingForTimer}` and `AgentLoopState::pending_timer`
   are declared with no writer, so the status vocabulary overstates run
   capabilities.
9. The telemetry matrix counts twenty-five segment classes; the enum has
   twenty-four variants.
10. `docs/rakka-akka-core-gap-report.md` still lists receptionist, routers, the
    cluster extension, and the sharding facade as absent or too low-level; it
    was never revised after phases 2–7.
11. `docs/rakka-v1-security-operational-defaults.md` lists `RAKKA_REMOTING_*`
    and timeout environment variables that no crate parses; the document says
    so, but readers of the manifests may not notice.
12. `docs/rakka-v1-known-limitations-roadmap.md` says the repository does not
    declare a license; `LICENSE` is an all-rights-reserved notice. Both are
    true and the tension is unresolved, as the July report also noted.

The agent domain's own "Owed" sections (security, telemetry, fault-injection
matrices, recovery roster, and the implementation plan's "Owed onward" lines)
remain accurate and are not repeated here; section 5.3 draws on them.

## 9. Decisions Rakka has made that are not gaps

- Bounded mailboxes with explicit `Result` sends instead of silent
  fire-and-forget.
- Actors as Rust traits with boxed async handlers rather than pure behavior
  values; `Behavior<M>` is a closure trait, not a state machine.
- `ReplyTo` as a typed oneshot rather than an `ActorRef`, with a separate
  remote request registry.
- Deterministic per-node membership from a consistent external arbiter, with
  peer self-fencing, instead of gossip, phi-accrual, and a split-brain resolver.
- Shard ownership as a pure function of membership serialized by durable
  revision compare-and-set and an optional fencing lease, instead of a
  singleton coordinator actor and a stop-the-world handoff barrier.
- Receptionist and routers as system-owned handles rather than actors.
- Explicit Protobuf registry with per-message schema windows instead of
  config-bound serializers; no remote deployment; trusted-cluster remoting.
- Persistence stores passed explicitly at spawn; synchronous behavior builders
  with an async trait beneath; persist awaited inline.
- Reliable delivery as a durable inbox and outbox with idempotency keys rather
  than producer and consumer controllers.
- Streams as bounded, pull-based, single-task pipelines with fail-stop errors.
- Metrics recorded through a neutral trait and exported as data; no OTel SDK
  in any published crate.
- Every agent model call durable and sharded; no ephemeral request agent.
- Session memory scoped per run; goals carried by the root task rather than a
  sixth entity; no per-agent task queue, concurrency bounded by budgets.

## 10. Bottom line

Against Akka Core, Rakka now presents the Akka-shaped surface an application
developer touches every day: system builder, typed refs and paths, a full
actor context, receptionist, routers, `ClusterSharding` with entities and
passivation, event-sourced and durable-state behaviors, a stream facade, and a
coordinated shutdown that is better instrumented than Akka's. The July
assessment that Rakka had "reached meaningful parity at the distributed
execution substrate" holds and has widened. What Rakka does not have is Akka's
cluster machinery, which it declined to build, and the long tail of runtime
depth (stash, dispatchers, supervision windows, stream operators, persistence
query and adapters, multi-node testing, TLS) that Akka accumulated over a
decade. Two of the persistence gaps are defects rather than omissions.

Against the Akka SDK, Rakka has gone from no agent layer to one whose
durability and coordination contracts are stricter than Akka's: typed tasks,
escrowed budgets, reconciliation, admission, goals, five coordinated entity
classes, knowledge claims, and a recovery roster the tree is held to. Akka
remains ahead where a developer first meets the product: a mounted agent
endpoint, wired model providers, MCP and ACP, response guardrails, streaming,
memory modes, a registry, a builder DSL, a composition root, published
artifacts, a license, and years of operations. Those are the gaps the next
phase should close, and none of them requires weakening the boundaries Rakka
has built.

## 11. Validation

No Rakka code, documentation, or test was changed, and no `cargo` command was
run. Spot-checks performed directly against the snapshot, beyond the three audit
passes: absence of any `RestartWithBackoff` use outside its definition; the
body of `Cluster::get`; absence of any TLS crate in every manifest; absence of
singleton, pub-sub, and CRDT code; the `KubernetesDnsDiscovery` dependencies and
`discover` body; the persistence query and stream sink bodies; the event-sourced
`apply_effect` destructuring; the agents service's absence from the A2A handler,
routes, and router; the single `impl RequestHandler`; absence of any Rig
`providers::` use; absence of `max_wall_clock_millis` from run, dispatch, and
loop files; the four `mcp` occurrences; the absent `AgentRunStatus::Suspended`
writer; the evaluated guardrail boundary constant; the sharded-agents example's
feature list; and the callers of `model_visible`.

## 12. Sources

Akka:

- <https://github.com/akka/akka-core>
- <https://doc.akka.io/libraries/akka-dependencies/current/>
- <https://doc.akka.io/libraries/akka-core/current/> (typed actors, fault
  tolerance, stash, interaction patterns, lifecycle, mailboxes, dispatchers,
  extending, cluster, membership, split-brain resolver, singleton, pub-sub,
  distributed data, coordination, discovery, sharding, sharding concepts,
  sharded daemon process, reliable delivery, persistence, snapshots, durable
  state, replicated event sourcing, persistence query, persistence plugins,
  persistence testing, streams, operators index, graphs, error handling,
  stream refs, stream testkit, actor interop, Artery remoting, remote security,
  serialization, Jackson, async and sync testing, multi-node testing,
  coordinated shutdown)
- <https://doc.akka.io/libraries/akka-management/current/>
- <https://doc.akka.io/libraries/akka-projection/current/>
- <https://doc.akka.io/libraries/akka-http/current/>
- <https://doc.akka.io/libraries/akka-grpc/current/>
- <https://doc.akka.io/libraries/akka-edge/current/>
- <https://doc.akka.io/reference/release-notes.html>
- <https://doc.akka.io/sdk/autonomous-agents/defining.html>
- <https://doc.akka.io/sdk/autonomous-agents/client.html>
- <https://doc.akka.io/sdk/agents/memory.html>
- <https://doc.akka.io/sdk/agents/guardrails.html>
- <https://doc.akka.io/sdk/mcp-endpoints.html>
- <https://doc.akka.io/sdk/integrations/apis-and-protocols.html>
- <https://doc.akka.io/sdk/workflows.html>

Rakka (all at `a757ad2`): every crate under `crates/`, every example under
`examples/`, `docs/rakka-agents.md`, the four agent validation matrices, the
recovery roster, `docs/rakka-compatibility.md`,
`docs/rakka-v1-known-limitations-roadmap.md`,
`docs/rakka-v1-reliability-boundaries.md`,
`docs/rakka-cluster-coordination-strategy.md`,
`docs/rakka-akka-parity-*.md`, `docs/rakka-akka-parity-migration-notes.md`,
`docs/rakka-api-boundary-inventory.md`, `docs/rakka-v1-release-packaging.md`,
`docs/plans/rakka-agent/spec.md`, and
`docs/plans/rakka-agent/implementation-plan.md`.
