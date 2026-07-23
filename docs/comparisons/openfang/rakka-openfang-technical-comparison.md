# Rakka and OpenFang Technical Comparison

Status: research evaluation  
Evaluation date: 2026-07-09  
Rakka snapshot: `f59f6d4722362617a8da8b84f4ccd6e4834763bb` (2026-07-08)  
OpenFang snapshot: repository `main` at workspace version `0.6.9`; latest
reviewed tag `v0.6.9` (2026-05-12)

## Purpose

This document compares Rakka with OpenFang, with emphasis on:

1. OpenFang Protocol (OFP) versus Rakka multi-node clustering;
2. `openfang-kernel` versus Rakka orchestration, reliability, and durability;
3. the two workflow models;
4. enterprise and Kubernetes readiness;
5. porting `openfang-runtime`'s agent loop into Rakka;
6. porting `openfang-memory` into a multi-node Rakka deployment; and
7. opportunities for building a distributed cluster of autonomous agents from
   the two projects.

The evaluation is based on OpenFang's public repository and documentation plus
the Rakka implementation and design documents present at the snapshots above.
OpenFang evolves quickly, so source-level conclusions should be revalidated
against the exact tag selected for an integration.

## Executive Summary

Rakka and OpenFang are complementary rather than direct substitutes.

- OpenFang is a batteries-included agent application/runtime. It supplies LLM
  loops, model drivers, tools, sessions, semantic memory, knowledge graphs,
  channel adapters, autonomous Hands, capability enforcement, workflows, and
  user-facing APIs.
- Rakka is a distributed execution substrate. It supplies typed actors,
  membership, remoting, logical entity placement, sharding, revision-fenced
  durable state, deterministic graph execution, durable inbox/outbox,
  recovery, and Kubernetes lifecycle behavior.

The recommended combined architecture is:

> Rakka owns durable identity, placement, workflow state, recovery, and effect
> delivery. OpenFang supplies the agent loop, model drivers, tools, channels,
> memory semantics, sandboxing, and autonomous behavior behind Rakka dispatcher
> adapters.

`openfang-kernel` should not be embedded wholesale inside every Rakka entity.
Selected OpenFang subsystems should be adapted behind Rakka's durable effect
boundary.

## Version and Maturity Baseline

At the evaluation date, OpenFang's workspace manifest and latest tag identify
version `0.6.9`. The repository README still says that OpenFang is pre-1.0 and
may introduce breaking changes between minor versions, while the documentation
homepage displays `v1.0.0`. This evaluation treats the tagged source and Cargo
manifest as authoritative and recommends pinning an exact tag or commit.

Rakka's workspace crates are currently version `0.1.0`, and the repository
describes itself as a v1 release-candidate foundation. Neither project should
be treated as a semver-stable, turnkey enterprise platform without additional
validation and operational ownership.

## Architectural Comparison

| Area | OpenFang | Rakka |
| --- | --- | --- |
| Primary abstraction | Autonomous agent OS/application | Distributed actor and durable workflow framework |
| Agent execution | Complete LLM/tool loop | Product-neutral model/tool effects and dispatchers |
| Networking | OFP peer links between independent kernels | Membership, private remoting, receptionist, sharding, and ownership |
| Persistence | SQLite agent memory, sessions, semantic memory, knowledge graph, and usage | Revision-fenced state, event sourcing, PostgreSQL adapters, and durable inbox/outbox |
| Workflows | User-facing prompt pipelines | Durable compiled graph execution runtime |
| Security focus | Capabilities, sandboxing, taint tracking, signing, RBAC, and audit | Explicit reliability boundaries, bounded execution, process allowlists, and credential-reference discipline |
| Kubernetes | Containerizable single daemon | Cluster-aware readiness, drain, shard movement, compatibility, and multi-replica reference topology |
| Best fit | Making agents useful | Keeping distributed, long-running work correct |

## 1. OFP Versus a Rakka Multi-Node Cluster

OFP and Rakka remoting overlap at the transport level, but OFP is not a cluster
runtime.

### What OFP Provides

The `openfang-wire` crate implements:

- a TCP listener and outbound peer connections;
- four-byte length-prefixed JSON request/response frames;
- node identity and an exact protocol version;
- exchange and discovery of locally hosted agents;
- remote agent prompt/response forwarding;
- ping/pong;
- HMAC-SHA256 mutual authentication;
- nonce replay protection; and
- per-session message integrity.

Its core request surface is `Handshake`, `Discover`, `AgentMessage`, and `Ping`.
The peer registry tracks connected or disconnected peers and their advertised
agents.

OFP does not itself provide:

- a strongly consistent membership view;
- distributed placement or ownership for an agent identity;
- shard allocation;
- single-active-owner fencing;
- state transfer or durable recovery after owner failure;
- automatic rescheduling of an agent on another node;
- split-brain prevention;
- durable message acceptance; or
- a rolling-update schema compatibility window comparable to Rakka's N/N+1
  policy.

Connecting OpenFang kernels through OFP therefore creates a federation of
independently stateful daemons. It does not turn their SQLite databases,
workflow engines, or schedulers into one highly available cluster.

### What Rakka Adds

Rakka's cluster layer tracks joining, up, leaving, unreachable, down, and
removed states. Node identities include process or pod incarnation. Membership
admission checks mutual protocol compatibility before a node can acquire shard
ownership.

Rakka remoting adds bounded per-peer queues, versioned Protobuf envelopes,
registered-peer admission, reconnect behavior, message schema policies, and
fail-closed compatibility handling. Sharding maps stable entity identities to
cluster owners and supports handoff, passivation, recovery, and optional
PostgreSQL-backed coordinator leases and fencing.

Rakka can use etcd as a strongly consistent external membership arbiter.
Leased registrations disappear after crashes or scale-in, and every node
derives the same ownership view from the same up-set. See:

- [`rakka-cluster` membership](../../../crates/rakka-cluster/src/membership.rs)
- [Rakka reliability boundaries](../../rakka-v1-reliability-boundaries.md)
- [`rakka-sharding-postgres`](../../../crates/rakka-sharding-postgres/src/lib.rs)
- [`rakka-discovery-etcd`](../../../crates/rakka-discovery-etcd/src/lib.rs)

### Security Boundary

OFP's HMAC authenticates peers and protects frame integrity. Because its
implementation uses plain TCP rather than TLS, payload confidentiality must be
provided by a private network, VPN, mTLS proxy, or service mesh.

Rakka's internal remoting similarly does not include built-in TLS/mTLS or
certificate lifecycle management in v1. Both transports should be treated as
private infrastructure traffic rather than internet-facing client protocols.

### Recommended Combined Use

- Use Rakka membership, remoting, receptionist, and sharding within one Rakka
  deployment or failure domain.
- Use OFP as a gateway for federation with standalone OpenFang installations or
  other OpenFang-backed clusters.
- Prefer [`rakka-a2a`](../../../crates/rakka-a2a/src/lib.rs) for standards-based
  public agent interoperability. It maps public A2A requests to durable Rakka
  inbox/outbox and optionally sharded run ownership.
- Do not advertise every sharded entity from every Rakka pod through OFP. That
  would create a second, eventually stale location registry. Advertise stable
  gateway-level services instead.

## 2. `openfang-kernel` Versus Rakka Orchestration

This is not a one-to-one comparison.

`OpenFangKernel` is a product composition root. It assembles the agent registry,
scheduler, memory, supervisor, workflow engine, triggers, channels, RBAC,
metering, model drivers, skills, background execution, and OFP networking.

Rakka distributes the closest equivalents across specialized crates:

| OpenFang component | Closest Rakka component | Important difference |
| --- | --- | --- |
| `AgentRegistry` | Receptionist plus sharded entity registry | Rakka resolves logical entities independently of their current host |
| `AgentScheduler` | Graph scheduler plus dispatcher fleet | Rakka focuses on durable transitions, leases, bounded concurrency, and recovery; product quotas remain application policy |
| `Supervisor` | Actor supervision and process actors | Rakka supervision participates in the actor ownership model |
| `WorkflowEngine` | `rakka-agent-workflow` graph runtime | Rakka state is designed for crash, passivation, and shard-movement recovery |
| `EventBus` | Runtime events and projections | Rakka explicitly keeps events and projections outside the correctness boundary |
| `MemorySubstrate` | No direct equivalent | OpenFang memory is agent-domain data; Rakka persistence is correctness-oriented actor/workflow state |
| RBAC, auth, and metering | Application-owned | Rakka deliberately does not become an auth, billing, or tenant platform |
| LLMs, tools, channels, and Hands | Application/effect adapters | These are the highest-value OpenFang capabilities to reuse |

Rakka's [compiled execution specification](../../plans/compiled_execution_with_graph_schdlr/compiled-execution-with-graph-scheduler-spec.md)
makes this ownership split explicit:

- the application owns product DSL, credentials, policy, prompts, adapters, and
  user-facing APIs; and
- Rakka owns durable graph state, deterministic scheduling, durable effects,
  sharding, recovery, passivation, drain, and operational state.

OpenFang has greater agent-product breadth. Rakka has deeper distributed
ownership and failure semantics. OpenFang's normal supervision and retry paths
operate within a live daemon; Rakka's durable paths explicitly address process
death, pod movement, stale ownership, and redelivery boundaries.

## 3. Workflow Comparison

OpenFang workflows are convenient agent pipelines. Rakka workflows are durable
distributed state machines.

| Capability | OpenFang | Rakka |
| --- | --- | --- |
| Authoring | JSON, API, and CLI prompt pipeline | Compiled product-neutral execution plan |
| Control flow | Sequential, fan-out, collect, substring conditional, bounded loop | DAG dependencies, fan-out/fan-in, branches, bounded iterators, waiting states, cancellation, and terminal transitions |
| Execution | Calls named OpenFang agents through closures | Pure transitions locally; external work through durable effects |
| State | `Arc<RwLock<HashMap<...>>>` | Durable graph run and node state |
| Restart recovery | No durable recovery mechanism visible in `WorkflowEngine` | Designed for recovery after acceptance, effect scheduling, callbacks, passivation, and shard movement |
| Retries | Per-step timeout and retry count | Durable attempt state, retry scheduling, dispatcher leases, exhaustion, and callback deduplication |
| Retention | Up to 200 completed or failed runs retained in memory | Durable retention, compaction, query projections, and artifact references |
| Long waits | Primarily live execution | Durable timers and human checkpoints without retaining a live task |
| Distribution | Within one kernel, although agents can call peers | Sharded run ownership across cluster nodes |
| External effects | Executed as part of workflow calls | Intent persisted before dispatch; idempotency and compensation boundaries are explicit |

OpenFang's workflow engine is useful for short-running prompt pipelines. It
supports fan-out, conditionals, loops, timeouts, retries, APIs, and CLI tooling.
Its source stores definitions and runs in in-memory hash maps and evicts the
oldest terminal runs after the retained count exceeds 200.

### Workflow Compiler Opportunity

OpenFang's workflow definition could become an application-facing DSL that
compiles into Rakka's durable graph IR:

- `sequential` becomes a normal graph dependency;
- consecutive `fan_out` steps become parallel downstream nodes;
- `collect` becomes a fan-in/join node;
- `conditional` becomes a branch decision supplied by an application adapter;
- `loop` becomes a bounded iterator node;
- `Retry` becomes Rakka durable effect retry policy; and
- `{{input}}` and variables become artifact references and bounded node outputs.

This preserves OpenFang's approachable authoring model while replacing its
in-memory execution engine with Rakka's durable scheduler.

## 4. Enterprise and Kubernetes Readiness

### OpenFang Assessment

OpenFang contains enterprise-relevant security and operational features, but
its current repository does not demonstrate an enterprise-ready, horizontally
scalable control plane.

Positive indicators include:

- a sizable Rust workspace and test suite;
- capability enforcement and RBAC;
- rate limiting and metering;
- WASM and subprocess isolation controls;
- SSRF protection and secret zeroization;
- signed manifests;
- Merkle-chain audit logging;
- API authentication and security headers; and
- frequent tagged releases, including security dependency updates.

Important distributed and Kubernetes gaps include:

- the `deploy` directory currently contains a systemd unit, not Kubernetes
  manifests, Helm, or an operator;
- Docker Compose defines one daemon and one local data volume;
- the supplied image does not declare a non-root `USER` or container-level
  health check;
- SQLite is the primary state store;
- workflow definitions and runs are maintained in memory;
- OFP does not replicate kernel state;
- no documented leader election or distributed workflow ownership;
- no shard placement or handoff;
- no documented multi-version rolling compatibility policy; and
- no provided PDB, pre-stop ownership handoff, or multi-replica readiness
  model.

OpenFang is suitable for a single Kubernetes pod or StatefulSet replica with a
PVC, explicit health probes, externalized secrets, private OFP networking, a
restricted security context, and operator-supplied NetworkPolicy. It is not
safe to obtain HA merely by setting `replicas: 3` against one SQLite-backed
installation.

### Rakka Assessment

Rakka is more Kubernetes-shaped. Its
[agent workflow reference topology](../../plans/agentic-workflow/kubernetes-reference-topology.md)
includes:

- three replicas;
- a headless internal remoting service;
- separate public service exposure;
- startup, readiness, liveness, and drain hooks;
- a PodDisruptionBudget;
- PostgreSQL-backed state;
- rolling compatibility metadata;
- a non-root, read-only-root-filesystem policy; and
- default-deny NetworkPolicy guidance.

Rakka is also not a turnkey enterprise platform. Its documented limitations
include no built-in remoting TLS/mTLS, no Helm/operator lifecycle, no internal
consensus implementation, and pre-stable APIs. See
[Rakka v1 known limitations](../../rakka-v1-known-limitations-roadmap.md).

## 5. Porting the OpenFang Agent Loop Into Rakka

This is feasible and is likely the highest-value integration. It should be done
through an adapter and targeted refactoring rather than by calling the complete
agent loop from an actor handler.

OpenFang is dual-licensed under Apache-2.0 or MIT. Its runtime manifest is
workspace-oriented and directly depends on `openfang-types`, `openfang-memory`,
and `openfang-skills`. An initial integration should therefore pin an exact Git
tag or vendor the relevant crates.

The current `run_agent_loop` interface accepts, among other inputs:

- an `AgentManifest`;
- a mutable `Session`;
- the concrete `MemorySubstrate`;
- an `Arc<dyn LlmDriver>`;
- tool definitions;
- an optional `KernelHandle`; and
- optional skills, MCP, web, and browser contexts.

The loop provides valuable functionality: context repair, retry handling,
context budgeting, loop detection, tool execution, streaming, session
persistence, and model-provider abstraction.

### Recommended Execution Boundary

```text
A2A / HTTP / channel ingress
        |
        v
Rakka sharded run owner ----> durable run and graph state
        |
        v
durable outbox effect
        |
        v
Rakka dispatcher worker ----> OpenFang agent-loop adapter
                                    |         |
                                    |         +--> distributed agent memory
                                    +------------> LLMs, tools, MCP, sandbox
        ^
        |
durable completion command
```

The run actor should serialize durable state transitions. Dispatcher workers
should perform slow LLM and tool work.

### Critical Failure Granularity

If a complete OpenFang loop of up to many LLM/tool iterations is represented as
one opaque Rakka effect, a crash late in the loop can replay tool calls that
already executed.

That behavior is safe only for read-only or externally idempotent tools. For
effectful tools, OpenFang's tool execution path should be refactored so each
tool call becomes a Rakka durable outbox effect with a deterministic
idempotency key. The loop should resume after the durable tool result is
accepted.

### Suggested Implementation Sequence

1. Wrap a pinned OpenFang loop as a Rakka model effect with read-only tools.
2. Abstract `MemorySubstrate` and session access behind traits.
3. Replace direct effectful tool execution with a durable
   `ToolCallRequested`/`ToolCallCompleted` boundary.
4. Add dispatcher lease, cancellation, deadline, and timeout propagation.
5. Persist bounded output or artifact references before advancing graph state.
6. Test crashes before and after model response, tool dispatch, tool result, and
   session commit.
7. Test pod loss and shard movement during an active loop.

## 6. Porting `openfang-memory` Into a Multi-Node Rakka Deployment

The OpenFang memory domain model and algorithms are reusable, but the current
storage architecture should not become the multi-node source of truth.

`MemorySubstrate` combines structured memory, semantic search, knowledge graph,
sessions, consolidation, and usage tracking around a shared
`Arc<Mutex<rusqlite::Connection>>`.

This is appropriate for a single daemon, local development, and tests. It is
not an appropriate foundation for:

- multiple concurrent pods;
- shard movement;
- cross-node session recovery;
- shared-volume multi-writer access;
- revision-fenced writes; or
- HA failover and rolling data migration.

### Recommended State Division

Keep correctness state in Rakka's durable stores:

- run status and current graph node;
- accepted command ids;
- effect ids and retry state;
- timers and human checkpoints;
- plan fingerprint and state schema version;
- bounded artifact references; and
- session or memory revision references.

Move OpenFang application memory into shared domain stores:

- sessions and structured KV to PostgreSQL using tenant, agent, and session keys
  plus revision compare-and-set;
- semantic memory to PostgreSQL plus `pgvector`, or an external vector store;
- knowledge graph data to PostgreSQL tables initially, or a graph backend when
  query requirements justify it;
- large transcripts and tool outputs to object storage referenced from durable
  state; and
- usage and metering to append-only or idempotent PostgreSQL records.

Local SQLite can remain an optional cache or developer-mode implementation.

The sharded Rakka entity should normally be the logical writer for an agent or
session, but database revision checks must still reject stale owners. Rakka's
single-writer safety depends on revision CAS plus idempotent effects, not on
topology alone.

One SQLite PVC shared across replicas is not a solution. A StatefulSet with one
PVC per pod would also strand memory on the old pod when Rakka moves an entity.

## Proposed Combined Architecture

The system should have four explicit planes.

### Control and Correctness Plane: Rakka

- cluster membership and compatibility admission;
- logical agent and run identity;
- sharded single-owner execution;
- durable graph state;
- durable inbox/outbox;
- timers, checkpoints, retries, and cancellation;
- dispatcher leases and fencing;
- recovery, passivation, drain, and shard movement; and
- A2A task projection and public status.

### Agent Execution Plane: OpenFang Runtime

- LLM model drivers and fallback logic;
- agent loop and session repair;
- context budgeting and compaction;
- loop guards;
- tool implementations;
- MCP integrations;
- WASM and subprocess sandboxing;
- channel adapters; and
- Hands and agent manifests.

### Shared Data Plane

- PostgreSQL for durable state, inbox/outbox, sessions, usage, and structured
  memory;
- vector storage for semantic memory;
- object storage for large artifacts; and
- a secret manager for resolved credentials.

Resolved credentials should exist only in memory during one dispatch attempt.
Durable plans and effects should contain logical credential binding references,
consistent with Rakka's existing credential boundary.

### Interoperability Plane

- A2A and HTTP/gRPC for public application-facing protocols;
- OFP for OpenFang-specific federation;
- private Rakka remoting for cluster internals; and
- optional mTLS, VPN, or service mesh for transport confidentiality.

## Highest-Value Opportunities

### 1. Durable OpenFang Hands

Run Hands as Rakka sharded durable entities. Rakka owns schedules,
checkpoints, retries, recovery, and ownership. OpenFang supplies the operational
playbook, tools, model loop, and reporting integrations.

### 2. OpenFang Workflow Compiler

Keep OpenFang's approachable JSON/API authoring model and compile it into
Rakka's durable graph IR.

### 3. `rakka-openfang-runtime` Adapter

Implement Rakka model/tool dispatcher traits using OpenFang drivers, tools,
skills, MCP, sandboxing, loop guards, and session repair.

### 4. Distributed OpenFang Memory API

Preserve OpenFang's semantic, knowledge, session, and usage model while
extracting storage traits and adding PostgreSQL/pgvector implementations.

### 5. Regional Rakka Clusters With OFP Federation

Use Rakka within a reliable failure domain and OFP or A2A between regions or
independent clusters. This avoids stretching one actor cluster across WAN
failure modes.

### 6. Security Composition

Combine OpenFang's sandbox, taint tracking, signed manifests, and capability
gates with Rakka's durable intent, idempotency, credential references, and
single-owner execution.

### 7. Standards-Based A2A Surface

Expose durable combined agents through Rakka's A2A adapter. Retain OFP for
OpenFang-specific federation rather than making it the only public protocol.

## Recommended First Milestone

The first production-quality milestone should be one sharded Rakka agent whose
OpenFang loop survives dispatcher restart, pod loss, and shard movement without
duplicating a non-idempotent tool effect.

The milestone should demonstrate:

1. durable command acceptance before acknowledgement;
2. stable `AgentRunId` to sharded entity identity;
3. a PostgreSQL-backed session with revision CAS;
4. an OpenFang model turn executed by a Rakka dispatcher worker;
5. every effectful tool call crossing a durable outbox boundary;
6. idempotent result acceptance;
7. recovery after killing the owner pod at each effect boundary;
8. cancellation and deadline propagation;
9. bounded artifacts, metrics, and logs without prompt or credential leakage;
   and
10. a public A2A task projection independent of the current owner pod.

Once this boundary is proven, workflow compilation, Hands, channels, memory
features, and OFP federation become incremental additions rather than
architectural risks.

## Conclusion

The two projects fit together well when their ownership boundaries remain
clear:

- Rakka should be the control and correctness plane.
- OpenFang runtime workers should be the agent execution plane.
- PostgreSQL, vector storage, object storage, and secret management should form
  the shared data plane.
- A2A, HTTP/gRPC, and OFP gateways should form the interoperability plane.

Porting the OpenFang agent loop is practical with moderate refactoring. Porting
the current SQLite persistence implementation directly into a multi-node Rakka
cluster is not. The memory interfaces and semantics should be retained while
the backend is replaced with shared, revision-aware storage.

## OpenFang Sources

- [OpenFang repository](https://github.com/RightNow-AI/openfang)
- [OpenFang website](https://www.openfang.sh/)
- [OpenFang documentation](https://www.openfang.sh/docs)
- [Architecture](https://www.openfang.sh/docs/architecture)
- [Workflow documentation](https://www.openfang.sh/docs/workflows)
- [Security documentation](https://www.openfang.sh/docs/security)
- [MCP and A2A documentation](https://www.openfang.sh/docs/mcp-a2a)
- [Configuration](https://www.openfang.sh/docs/configuration)
- [`openfang-wire`](https://github.com/RightNow-AI/openfang/tree/main/crates/openfang-wire)
- [OFP peer implementation](https://github.com/RightNow-AI/openfang/blob/main/crates/openfang-wire/src/peer.rs)
- [OFP wire messages](https://github.com/RightNow-AI/openfang/blob/main/crates/openfang-wire/src/message.rs)
- [`openfang-kernel`](https://github.com/RightNow-AI/openfang/tree/main/crates/openfang-kernel)
- [Workflow engine source](https://github.com/RightNow-AI/openfang/blob/main/crates/openfang-kernel/src/workflow.rs)
- [Agent loop source](https://github.com/RightNow-AI/openfang/blob/main/crates/openfang-runtime/src/agent_loop.rs)
- [Memory substrate source](https://github.com/RightNow-AI/openfang/blob/main/crates/openfang-memory/src/substrate.rs)
- [Docker Compose](https://github.com/RightNow-AI/openfang/blob/main/docker-compose.yml)
- [Dockerfile](https://github.com/RightNow-AI/openfang/blob/main/Dockerfile)
- [Release tags](https://github.com/RightNow-AI/openfang/tags)

## Rakka References

- [Rakka README](../../../README.md)
- [Rakka reliability boundaries](../../rakka-v1-reliability-boundaries.md)
- [Rakka compatibility policy](../../rakka-compatibility.md)
- [Rakka known limitations](../../rakka-v1-known-limitations-roadmap.md)
- [Agent workflow specification](../../plans/agentic-workflow/agentic-workflow-spec.md)
- [Compiled graph specification](../../plans/compiled_execution_with_graph_schdlr/compiled-execution-with-graph-scheduler-spec.md)
- [Kubernetes reference topology](../../plans/agentic-workflow/kubernetes-reference-topology.md)
- [Clustered sharded A2A agent specification](../../plans/clustered-sharded-entity-a2a-agents/spec.md)
- [`rakka-agent-workflow`](../../../crates/rakka-agent-workflow/src/lib.rs)
- [`rakka-a2a`](../../../crates/rakka-a2a/src/lib.rs)
- [`rakka-discovery-etcd`](../../../crates/rakka-discovery-etcd/src/lib.rs)
- [`rakka-sharding-postgres`](../../../crates/rakka-sharding-postgres/src/lib.rs)
