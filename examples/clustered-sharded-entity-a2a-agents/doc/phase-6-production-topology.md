# Phase 6 Production Topology

This guide is the production-candidate operating contract for
`rakka-example-clustered-sharded-entity-a2a-agents`. It hardens the Phase 4/5
A2A example for Kubernetes-style deployment without changing the core
reliability boundary: accepted A2A work is durable only after Rakka durable
state/inbox/outbox writes succeed, and external side effects remain
at-least-once unless the target participates in idempotency.

## Traffic And Ownership Paths

Public traffic enters through the load-balanced `rakka-a2a-public` Service:

```text
A2A client
  -> LoadBalancer / Ingress
  -> public A2A HTTP route on any pod
  -> RakkaA2ARequestHandler
  -> sharded owner request over private Rakka remoting when needed
  -> owning A2ARunEntity
  -> durable run state, workflow inbox/outbox, task projection, push configs
```

Private Rakka remoting uses the headless `rakka-a2a-internal` Service and pod
DNS/pod IPs. It is trusted cluster traffic, not a public protocol. Do not expose
the remoting port through the public Service or an internet-facing ingress.

etcd provides dynamic membership. Pods register under
`RAKKA_ETCD_PREFIX` with a lease; scale-out adds members when new pods publish
their keys, and scale-in/removal drops members after graceful lease revoke or
lease expiry.

PostgreSQL is the shared durable store for:

- `AgentRunState`
- workflow inbox/outbox state
- A2A push notification configs

The example rebuilds process-local A2A task projections from durable run and
inbox state on restart. A future extracted `rakka-a2a` crate should promote the
A2A task-event projection to a shared PostgreSQL query/event table so every node
can serve replay without owner polling.

## Local Developer Topology

Local development defaults to:

- `RAKKA_DISCOVERY_PROVIDER=file`
- `RAKKA_PERSISTENCE=file`
- `RAKKA_STATE_DIR=/tmp/rakka-clustered-sharded-entity-a2a-agents/state`
- agent cards advertising the per-node local HTTP URL

This is useful for two local processes on one host, but it is not a production
multi-pod durability story. A PersistentVolume is not enough when ownership can
move to a different pod; production-like multi-pod recovery requires
`RAKKA_PERSISTENCE=postgres`.

## Kubernetes Objects

The manifest set in `examples/clustered-sharded-entity-a2a-agents/k8s/`
provides:

- `Namespace` and `ServiceAccount`
- `ConfigMap` for discovery, ports, public URL, self-fencing, and telemetry
- `Secret` for the PostgreSQL DSN
- public `LoadBalancer` Service for A2A HTTP/JSON and JSON-RPC
- headless private Service for Rakka remoting
- `StatefulSet` with downward-API pod identity/address
- readiness, liveness, startup, and preStop drain hooks
- `PodDisruptionBudget`
- `HorizontalPodAutoscaler`
- demo-grade single-node etcd and PostgreSQL deployments

Production operators should replace demo etcd/PostgreSQL with managed or HA
services, configure network policy so only app pods can reach remoting, etcd,
and PostgreSQL, and set `RAKKA_A2A_PUBLIC_URL` to the external HTTPS load
balancer URL. The agent card must point to that public URL, not pod-local
addresses.

## PostgreSQL And Migrations

Build the image with the `postgres` feature:

```sh
docker build -f examples/clustered-sharded-entity-a2a-agents/Dockerfile \
  -t rakka-clustered-a2a-agents:0.1.0 .
```

Run with:

```sh
RAKKA_PERSISTENCE=postgres
RAKKA_POSTGRES_DSN='host=rakka-a2a-postgres port=5432 user=postgres password=postgres dbname=postgres'
```

The example self-applies the `rakka-persistence-postgres` durable-state
migration on startup for the run, workflow, and push-config stores. The
underlying migration creates the durable-state, event-journal, and snapshot
tables. For a reusable `rakka-a2a` extraction, add a dedicated PostgreSQL A2A
task-event projection migration and wire it with the
`PostgresAgentWorkflowQueryIndex` runtime-event tables.

Backup and restore expectations:

- back up PostgreSQL before deploying schema changes;
- restore durable state, inbox/outbox state, and push configs together;
- restore to a point before accepting public traffic;
- after restore, let each node rebuild task projections before becoming ready;
- validate duplicate retry behavior with the same A2A `messageId`.

## Discovery, Membership, And Self-Fencing

Set:

```sh
RAKKA_DISCOVERY_PROVIDER=etcd
RAKKA_ETCD_ENDPOINTS=http://rakka-a2a-etcd:2379
RAKKA_ETCD_PREFIX=/rakka/examples/a2a-agents/members/
RAKKA_ETCD_LEASE_TTL_SECONDS=10
RAKKA_SELF_FENCE=true
```

Each pod registers its Rakka node address under the etcd prefix and refreshes
the lease. The discovery loop applies membership snapshots to
`ClusterNodeRuntime`. When sustained remote ask failures show peer
unreachability, self-fencing revokes the etcd lease and shuts the node down.

Partial partitions can still produce at-most-once remoting loss. Correctness
comes from durable state plus idempotent retry, not from remoting delivery.
Load-balancer health remains independent from shard ownership: a pod is ready
only while accepting public ingress, but a healthy pod may route a request to a
different owner.

## Drain And Shutdown

The app exposes:

- `GET /healthz` for liveness;
- `GET /readyz` for public-ingress readiness;
- `GET /drain` or `POST /drain` for preStop drain.

`/drain` closes mutating public A2A ingress on that pod. New
`message:send`, `message:stream`, `subscribe_to_task`, `cancel_task`, and
push-config writes receive the stable `a2a-agent-draining` error and should be
retried against another Service endpoint. Existing `get_task`, `list_tasks`,
and push-config reads remain available while the process is still running.
After drain, `/readyz` returns HTTP 503 with `ready=false` so Kubernetes removes
the pod from Service endpoints; `/healthz` remains HTTP 200 because the process
is live and completing graceful shutdown.

Graceful drain reduces rollout disruption but is not the correctness boundary.
Abrupt pod kill after durable acceptance still recovers because the run and
workflow stores are shared.

## Observability And Snapshots

Use existing Rakka/agent-workflow metric and snapshot surfaces for:

- A2A ingress request counts, latency, error codes, and stream disconnects;
- stream opened/closed/over-limit/lagged/dropped/replay metrics from
  `stream_limits.rs`;
- durable acceptance, duplicate, conflict, and rejection counts;
- dispatcher backlog, in-flight dispatches, retry/exhaustion counts, and due
  outbox effects;
- shard ownership, route failures, passivation, and owner movement;
- push scheduling and retry/exhaustion counts;
- recovery errors from runtime snapshots.

Keep hot metric labels bounded. Do not use task ids, actor paths, prompts,
payloads, callback URLs, command args, temp paths, full errors, or resolved
credentials as labels.

OpenTelemetry guidance:

- propagate W3C `traceparent`/`tracestate` from A2A HTTP headers into durable
  command metadata;
- use span links when a dispatcher attempt or callback resumes work after a
  durable boundary;
- send metrics/traces/logs through an OpenTelemetry Collector sidecar or gateway;
- add Kubernetes resource attributes such as namespace, pod name, pod uid, node
  name, deployment name, container name, and Rakka node id.

Operational snapshots for production review should include runtime, outbox,
recovery, human checkpoints, shards, streams, task projection, and push
delivery. The example has local runtime/stream/push state; a reusable crate
should register these snapshots through the shared HTTP snapshot registry.

## Autoscaling Signals

Scale-out signals:

- active public streams;
- pending inbox commands;
- due outbox effects;
- dispatcher backlog;
- in-flight dispatches;
- stream lag or replay latency;
- A2A request latency.

Alert-only signals:

- recovery errors;
- self-fence events;
- push retry exhaustion;
- shard ownership skew;
- Postgres latency or migration lock contention;
- sustained stream disconnect rate.

Do not scale directly on high-cardinality labels. Use bounded dimensions such as
workflow type, target class, status, operation, transport, and outcome.

## Failure Injection

Run these before treating a deployment as production-candidate:

1. Kill the owner pod after durable A2A acceptance and retry the same
   `messageId`.
2. Kill the owner after push/effect scheduling and confirm the due outbox entry
   remains.
3. Kill a dispatcher worker during external effect execution and confirm retry
   reuses the same idempotency key.
4. Kill a public ingress pod during an SSE stream; reconnect through the Service
   with the latest replay cursor.
5. Route a stream reconnect through a different pod and confirm `get_task`
   returns the durable task projection.
6. Restart PostgreSQL connections during retryable operations.
7. Scale the StatefulSet up and down while tasks are active.

Expected outcomes:

- runs recover or reach a durable, explainable terminal state;
- stream disconnects never cancel runs;
- duplicate retries remain idempotent;
- snapshots and logs expose the failure and recovery path.

## Production-Candidate Review

Before extracting into `rakka-a2a`, review:

- API stability of metadata keys, task-event projection schema, and replay
  cursor shape;
- security and tenancy assumptions;
- migration and retention policy;
- whether task-event projection needs a shared PostgreSQL table before broad
  reuse;
- whether the push dispatcher is product-owned or crate-owned;
- compatibility of agent-card capabilities and transport URLs;
- documentation for local, Kubernetes, backup/restore, drain, and failure
  injection operation.

Current candidate scope: production-shaped example deployment and recovery
validation, not a reusable public `rakka-a2a` API and not exactly-once external
side effects.
