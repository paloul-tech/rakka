# Kubernetes Deployment with etcd Service Discovery

> **Status: implemented.** This document is the design rationale; the example now
> ships the etcd discovery provider (`src/etcd_discovery.rs`), provider selection
> (`RAKKA_DISCOVERY_PROVIDER`), the file/PostgreSQL durable-store seam
> (`src/persistence.rs`, `postgres` feature), Kubernetes-friendly config
> (downward-API pod identity, `0.0.0.0` bind), SIGTERM-aware graceful shutdown
> with etcd lease revoke, a `Dockerfile`, and `k8s/` manifests (etcd, PostgreSQL,
> a StatefulSet, headless + public Services, a PodDisruptionBudget, and a
> HorizontalPodAutoscaler). See the example README for usage. The remaining
> production hardening called out below — a shared, fenced shard coordinator
> store/lease — is documented but not wired.

## Requirement: dynamic autoscaling and downscaling

Running in Kubernetes, this workload **must** support dynamic horizontal
autoscaling: the deployment scales **out** under load and scales **in** to
reclaim resources when they are not fully used. Membership discovery therefore
has a hard requirement — it must reflect pods **joining and leaving at runtime**.
A static, fixed-size pod list (for example, enumerating StatefulSet ordinals
`0..replicas`) does **not** satisfy this and is explicitly out of scope.

Consequences that the rest of this document assumes:

- **Discovery must be dynamic.** New pods must appear in membership without a
  manifest or config change, and removed pods must disappear promptly. This is
  the reason the example targets **etcd** (register + lease + watch) rather than
  the static-list DNS provider — see Parts 3 and 4.
- **Scaling can be driven by Rakka's own signals.** `rakka-agent-workflow`
  publishes bounded autoscaling-signal metrics (`AGENT_WORKFLOW_AUTOSCALING_SIGNALS`,
  `agent_autoscaling_signal` — e.g. active runs, mailbox depth, due outbox
  effects, human-waiting runs, dispatcher backlog) suitable for a Horizontal Pod
  Autoscaler or a KEDA `ScaledObject`, rather than CPU alone. See
  `docs/plans/agentic-workflow/kubernetes-autoscaling-signals.md`.
- **Scale-in must be safe.** Removing a pod moves the shards (and runs) it owned
  to survivors, so runs must be recoverable from shared durable storage and
  removed pods must drain before exit (Part 4).

## The one seam that matters: `DiscoveryProvider` → `DiscoverySnapshot`

Rakka's membership is **discovery-snapshot driven**, and the source is fully
pluggable. Everything downstream — membership, failure detection, shard
ownership via `region().resolve(...)`, `remote_ask` routing, and the run entity —
consumes a `DiscoverySnapshot` and is agnostic to where it came from:

- `rakka::cluster::DiscoveryProvider` is a trait with one method:
  `discover(observed_at_millis) -> ClusterResult<DiscoverySnapshot>`, where a
  snapshot is just `(provider_name, observed_at, Vec<ClusterNode>)`.
- `ClusterNodeRuntime::poll_discovery(&provider, now)` (and `_async`) call the
  provider and apply the result. The example today does the equivalent manually
  in `src/discovery.rs` (`runtime.apply_discovery(snapshot)` + `runtime.tick(now)`
  in a loop fed by the shared file directory).

So the **file directory is just one `DiscoveryProvider`**. Swapping it for etcd
(or Kubernetes DNS) touches only the discovery source; the sharded run
execution, the HTTP/gRPC ingress, and the durable model are unchanged. That is
why this is a small, contained change.

## What Rakka already provides for Kubernetes (and what it doesn't)

`rakka-k8s` and `rakka-agent-workflow`'s `k8s` feature provide the native pieces.
**etcd is not one of them** — but the discovery seam makes it easy to add.

- **DNS discovery**: `KubernetesDnsDiscovery` (a `DiscoveryProvider`),
  `KubernetesPodIdentity` (pod name → logical id, pod uid → incarnation), and
  `KubernetesDnsDiscoveryConfig::pod_host()` building
  `pod.svc.ns.svc.cluster.local`. **It does not meet our requirement:** it is
  built from a **static pod list** (you supply the StatefulSet ordinals
  `0..replicas`), so it only fits a fixed-size StatefulSet. Because we require
  dynamic autoscaling and downscaling (see above), the static-list DNS provider
  is unsuitable, and the example uses a dynamic **etcd** provider instead.
- **Health / probes**: `readiness_probe_hook`, `liveness_probe_hook`,
  `KubernetesNodeHealth`, `KubernetesProbeHook`.
- **Drain / preStop**: `KubernetesDrainController`, `kubernetes_drain_route`,
  `run_kubernetes_prestop_shutdown_on_os_signal`.
- **Agent-workflow startup / drain gate**: `AgentWorkflowKubernetesStartup`
  (ordered startup steps), `AgentWorkflowIngressGate`
  (`begin_drain` / `accepts_public_commands` / `ensure_accepting`),
  `register_agent_workflow_ingress_stop_task`.
- **A reference manifest** at `examples/kubernetes/rakka-node.yaml` (Namespace,
  ConfigMap, headless `rakka-internal` Service for remoting, public
  `rakka-public` Service, PodDisruptionBudget, StatefulSet with downward-API pod
  identity, readiness/liveness/preStop). It already standardizes a
  `RAKKA_DISCOVERY_PROVIDER` env knob — the intended place to select a provider.

**etcd is therefore a small custom `DiscoveryProvider`** the example would
implement, slotting into the exact same seam DNS uses.

## Part 1 — Containerizing the example

1. **Multi-stage Dockerfile.** The builder stage needs Rust + `protoc` (the gRPC
   contract is generated at build time by `tonic-build`); the runtime stage is
   slim/distroless carrying only the compiled
   `rakka-example-clustered-agent-workflow-http-grpc` binary (plus CA certs if
   etcd uses TLS). `protoc` is build-only.
2. **No code change for config** — the binary already takes the ingress as a CLI
   argument (`http` / `grpc`) and reads all addressing from env. In-container:
   - `RAKKA_BIND_HOST=0.0.0.0` (bind all interfaces).
   - `RAKKA_ADVERTISE_HOST` = the pod's routable address (pod IP via downward API
     `status.podIP`, or the stable headless-service pod DNS name).
   - `RAKKA_NODE_LOGICAL_ID` = pod name (`metadata.name`) for **stable** identity
     across restarts; `RAKKA_NODE_INCARNATION` = pod uid (`metadata.uid`) for a
     fresh incarnation per pod (matches `KubernetesPodIdentity`).
   - `RAKKA_PORT` (remoting), and `RAKKA_HTTP_PORT` / `RAKKA_GRPC_PORT` for the
     chosen ingress.
3. **Container command** selects the ingress: `args: ["grpc"]` (or `["http"]`).
   The public Service targets that port.

## Part 2 — Kubernetes objects (mirror `rakka-node.yaml`)

- **StatefulSet** for stable per-pod identity/DNS, downward-API env
  (`metadata.name`, `metadata.uid`, `status.podIP`, `metadata.namespace`),
  `terminationGracePeriodSeconds`, rolling updates.
- **Headless Service** (`clusterIP: None`, `publishNotReadyAddresses: true`)
  exposing the **remoting** port — this is the pod-to-pod path that
  `rakka-remote` uses (the efficient inter-node channel; the ingress is never
  used pod-to-pod).
- **Public Service** (ClusterIP / LoadBalancer) targeting the chosen ingress
  port (HTTP or gRPC).
- **PodDisruptionBudget** for safe rollouts.
- **Probes & preStop**: readiness/liveness + a preStop `/drain` that runs
  coordinated shutdown (`run_kubernetes_prestop_shutdown_on_os_signal` + the
  agent-workflow ingress gate). Nuance: Rakka's probe hooks are **HTTP**; if a
  pod serves the **gRPC** ingress, expose a small dedicated HTTP admin/health
  port for probes (the reference manifest separates probe HTTP `8080` from public
  gRPC `50051`), or use `grpc_health_probe`.

## Part 3 — etcd-backed discovery

etcd is the provider that satisfies the dynamic autoscaling requirement: pods
register themselves at runtime and are removed automatically when they go away,
with no replica count or ordinal list hardcoded anywhere. It maps cleanly onto
the same model the file loop uses today — register / lease / watch instead of
write-file / read-dir:

1. **Add an etcd client** (e.g. the `etcd-client` crate) and an
   `EtcdDiscoveryConfig` (endpoints, key prefix such as
   `/rakka/<cluster>/members/`, lease TTL, optional auth/TLS) read from env
   (`RAKKA_ETCD_ENDPOINTS`, …).
2. **Register self**: `PUT /rakka/<cluster>/members/<logical-id>` = JSON of the
   `ClusterNode` (id, `advertise_host:rakka_port`, role), attached to a **lease**
   with a TTL of a few poll intervals; run lease **keepalive**. etcd auto-deletes
   the key when the lease lapses — this replaces the file's manual TTL/expiry and
   gives crisper liveness.
3. **Observe peers**: either **watch** the prefix (event-driven) or periodically
   **range-get** it; map the member values to `Vec<ClusterNode>` and build a
   `DiscoverySnapshot`. etcd's monotonic store revision is a natural value for the
   snapshot's `observed_at` / ordering.
4. **Feed the runtime**: the etcd watch task calls
   `runtime.apply_discovery(snapshot)` + `runtime.tick(now)` — exactly the calls
   `src/discovery.rs` makes today.
5. **Clean leave on shutdown**: revoke the lease / delete the key (so peers drop
   you immediately) alongside the existing `runtime.leave_local(...)`.

**How this satisfies autoscaling.** A scaled-**out** pod registers its key and
appears in the next snapshot, so it joins membership automatically. A
scaled-**in** or crashed pod's key disappears when its lease lapses (or is
revoked on graceful shutdown), so it drops out within roughly the lease TTL.
Shard ownership then rebalances across the surviving members, and runs owned by a
departed pod resume on their new owners (from shared durable storage; Part 4). No
replica count is configured in the discovery layer. `DiscoveryProvider::discover()` is synchronous,
but etcd is async. Implement it like Rakka's own `LocalDiscovery`: a background
async task watches etcd and maintains an `Arc<RwLock<Vec<ClusterNode>>>`, and
`discover()` returns the cached snapshot — or skip the trait entirely and have
the watch task call `apply_discovery` / `tick` directly (the path the example
already uses). Both are clean; the latter is the smaller diff.

Compared to Rakka's native `KubernetesDnsDiscovery`, etcd is **dynamic** (handles
scale up/down and churn without a hardcoded ordinal list), at the cost of running
and operating an etcd cluster and adding the client dependency.

## Part 4 — Correctness items required for multi-pod (beyond discovery)

These are what make the *current* example single-host-only; deploying without
addressing them would break run recovery:

- **Durable store must be shared across pods.** The example's
  `FileDurableStateStore` writes to a local directory. For sharding to move a run
  to a new owner pod and recover it, run/workflow state must live in a **shared**
  `DurableStateStore` — the **PostgreSQL persistence plugin** (`rakka-*-postgres`,
  via the facade `postgres` feature). A `PersistentVolume` does not help because a
  *different* pod becomes the owner. This is the single most important change.
- **Shard coordination under churn.** The example resolves ownership from the
  region/coordinator fed by discovery. For production rolling updates and
  failover, use a **shared, fenced shard coordinator store + lease** (the Postgres
  sharding plugin) so there is one ownership authority during membership changes.
- **Advertise address** must be the pod's cluster-routable host (not
  `127.0.0.1`), and **bind** `0.0.0.0`.
- **Probe port** for gRPC-only pods (see Part 2).
- **Scale-in must drain.** Because downscaling is a requirement, a terminating
  pod must run its preStop `/drain` (coordinated shutdown + `leave_local` + etcd
  lease revoke) so the shards/runs it owned hand off cleanly and survivors recover
  them from the shared store. A PodDisruptionBudget bounds how many pods leave at
  once. Without the shared durable store, scale-in would lose in-flight runs.

## Concrete changes the example would need

- `Cargo.toml`: enable the facade `k8s` (and `postgres`) features; add
  `etcd-client`.
- `src/config.rs`: add `RAKKA_DISCOVERY_PROVIDER` (`file` | `etcd` |
  `kubernetes-dns`), etcd endpoints/prefix/TTL, and derive bind/advertise/identity
  from the downward-API env.
- New `src/etcd_discovery.rs`: the register / lease / keepalive / watch task +
  snapshot builder (or an `EtcdDiscovery: DiscoveryProvider`).
- `src/server.rs` `boot()`: select the discovery source by env (keep `file` for
  local dev, add `etcd` / `kubernetes-dns`); wire the rakka-k8s health/drain hooks
  and the agent-workflow ingress gate; swap the file store for the Postgres store
  when configured.
- New `Dockerfile` and a `k8s/` manifest set (StatefulSet + headless + public
  Service + PDB + ConfigMap), modeled on `examples/kubernetes/rakka-node.yaml`,
  with `args: ["http" | "grpc"]` and `RAKKA_DISCOVERY_PROVIDER=etcd`.

## Summary

Dynamic autoscaling and downscaling are a hard requirement, so a static pod
list is not an option; etcd is **required** (not merely convenient) because it
expresses membership that grows and shrinks at runtime. The etcd integration
itself is genuinely small because it reuses the existing discovery seam
(`DiscoveryProvider` / `DiscoverySnapshot` → `apply_discovery` / `tick`). The
larger work for a *correct* Kubernetes deployment is swapping the local file
durable store for a shared one (Postgres) so sharded runs can recover on a new
owner pod when the cluster scales in or a pod fails, plus the standard k8s wiring
(StatefulSet, headless + public Services, probes, preStop drain) that Rakka's
`rakka-k8s` and `rakka-agent-workflow` k8s helpers already support.

## References

- Example discovery seam: `src/discovery.rs`, `src/server.rs` (`boot`).
- Rakka discovery: `rakka::cluster::{DiscoveryProvider, DiscoverySnapshot,
  StaticDiscovery, LocalDiscovery}`.
- Rakka Kubernetes: `rakka-k8s` (`KubernetesDnsDiscovery`,
  `KubernetesPodIdentity`, probe/drain helpers); `rakka-agent-workflow` k8s
  feature (`AgentWorkflowKubernetesStartup`, `AgentWorkflowIngressGate`).
- Reference manifest: `examples/kubernetes/rakka-node.yaml`.
- Topology/operations docs: `docs/plans/agentic-workflow/kubernetes-*.md`.
