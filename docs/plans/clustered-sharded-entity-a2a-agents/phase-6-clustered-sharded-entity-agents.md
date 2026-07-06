# Phase 6 Clustered Sharded Entity A2A Agents

Status: implemented
Source spec: `docs/plans/clustered-sharded-entity-a2a-agents/spec.md`

## Goal

Harden the A2A agent runtime for production-style Kubernetes deployment:
load-balanced ingress, private Rakka remoting, external discovery, PostgreSQL
persistence, operational telemetry, graceful drain, autoscaling, and
failure-injection coverage.

## Slices

### Slice 6.1: Production Topology Document

Status: implemented

Work:

- Document public load-balanced A2A ingress.
- Document private Rakka remoting between pods.
- Document external discovery using etcd or an equivalent provider.
- Document shared PostgreSQL persistence for run state, workflow state,
  projections, push configs, dispatcher state, timers, and coordinator data
  where applicable.
- Document local developer topology separately from production topology.

Acceptance:

- Operators can distinguish public traffic, private remoting, discovery, and
  persistence paths.
- The agent card points to the load balancer URL, not pod-local URLs.

### Slice 6.2: Kubernetes Manifests Or Template

Status: implemented

Work:

- Provide namespace, service account, config, secret references, services,
  StatefulSet or Deployment, PodDisruptionBudget, and HorizontalPodAutoscaler
  guidance.
- Expose public A2A HTTP port through a Service.
- Expose private Rakka remoting through pod DNS or a headless Service.
- Configure readiness, liveness, startup, and drain hooks.
- Configure environment variables for public URL, remoting address, discovery,
  persistence, and telemetry.

Acceptance:

- A local Kubernetes cluster can start multiple nodes.
- Public A2A traffic can hit any pod through the Service.
- Pods can discover each other and route task ownership internally.

### Slice 6.3: PostgreSQL Persistence And Migrations

Status: implemented

Work:

- Use PostgreSQL durable state for run and workflow state.
- Use PostgreSQL query indexes and A2A task event projection in production
  mode.
- Apply migrations for durable state, runtime event projection, task
  projection, push configs, timers, dispatcher state, and audit indexes.
- Use migration locks where available.
- Document backup and restore expectations.

Acceptance:

- A pod restart recovers accepted tasks.
- A new owner recovers run state after old owner removal.
- Projection and push config data survive process restart.

### Slice 6.4: Discovery, Membership, And Self-Fencing

Status: implemented

Work:

- Wire etcd or equivalent discovery for dynamic pod membership.
- Configure member lease TTL, poll/watch interval, and graceful lease revoke.
- Feed peer reachability into self-fencing when supported.
- Document partial partition behavior.
- Keep load balancer health independent from internal shard ownership.

Acceptance:

- Scaling up adds routable nodes.
- Scaling down removes nodes and moves ownership.
- Sustained peer unreachability can self-fence the affected node when enabled.

### Slice 6.5: Drain And Shutdown

Status: implemented

Work:

- Stop accepting new public A2A ingress during drain.
- Keep `get_task` and current projection reads available when safe.
- Notify discovery provider that the node is leaving.
- Let in-flight handlers finish within a bounded grace period.
- Do not rely on graceful drain for correctness.
- Ensure abrupt pod kill is covered by durable recovery tests.

Acceptance:

- Graceful termination reduces failed requests during rollout.
- Abrupt termination still recovers through durable state.
- Streams disconnect cleanly and clients can reconnect to another node.

### Slice 6.6: Observability And Operational Snapshots

Status: implemented

Work:

- Export A2A ingress metrics, stream metrics, durable acceptance metrics,
  projection metrics, dispatcher metrics, shard metrics, and push metrics.
- Propagate trace context through A2A ingress, durable commands, outbox effects,
  dispatcher attempts, peer A2A calls, and callbacks.
- Register operational snapshots for runtime, outbox, recovery, human
  checkpoints, shards, streams, task projection, and push delivery.
- Provide Prometheus and OTLP guidance.
- Keep high-cardinality identifiers out of hot metric labels.

Acceptance:

- Operators can see active runs, due effects, stream counts, push retry counts,
  shard ownership, and recovery errors.
- Trace context survives durable boundaries using span links where appropriate.

### Slice 6.7: Autoscaling Signals

Status: implemented

Work:

- Define autoscaling signals for active streams, pending inbox commands, due
  outbox effects, dispatcher backlog, in-flight dispatches, stream lag, and
  request latency.
- Document which signals are scale-out versus alert-only.
- Avoid scaling directly on high-cardinality labels.
- Add dashboards or snapshot examples for local validation.

Acceptance:

- HPA guidance uses bounded metrics.
- Operators can reason about read-heavy streaming load separately from
  write/dispatch backlog.

### Slice 6.8: Production Failure Injection

Status: implemented

Work:

- Kill owner pod after durable acceptance.
- Kill owner pod after effect scheduling.
- Kill dispatcher worker during external effect execution.
- Kill public ingress pod during SSE stream.
- Route stream reconnect through a different pod.
- Restart Postgres connection during retryable operations.
- Scale cluster up and down while tasks are active.

Acceptance:

- Runs recover or fail with durable, explainable terminal state.
- Stream disconnects do not cancel runs.
- Duplicate retries remain idempotent.
- Operational snapshots expose the failure and recovery path.

### Slice 6.9: Production Candidate Review

Status: implemented

Work:

- Review API stability for extraction into `rakka-a2a`.
- Review security and tenancy assumptions.
- Review migration and retention policy.
- Review backward compatibility of metadata keys and task event projection.
- Review documentation for local and production operation.

Acceptance:

- The team can decide whether to promote example code into a reusable crate.
- Remaining risks are documented with owners or follow-up plans.

## Exit Criteria

- Multi-node deployment works behind a load balancer.
- Production mode uses shared durable stores and external discovery.
- Streaming reconnect and owner movement are failure-tested.
- Operational telemetry and runbooks are sufficient for a production-candidate
  review.

## Implementation Summary

- Slice 6.1 is implemented in
  `examples/clustered-sharded-entity-a2a-agents/doc/phase-6-production-topology.md`
  and the example README. The topology guide separates public A2A ingress,
  private Rakka remoting, etcd membership, PostgreSQL persistence, and local
  developer file mode. `build_agent_card` now advertises
  `RAKKA_A2A_PUBLIC_URL` and test coverage verifies the card uses the
  load-balanced URL.
- Slice 6.2 is implemented by
  `examples/clustered-sharded-entity-a2a-agents/Dockerfile` and
  `examples/clustered-sharded-entity-a2a-agents/k8s/`. The manifest set defines
  namespace, service account, config, Secret-backed PostgreSQL DSN, public
  A2A Service, private headless remoting Service, StatefulSet, readiness,
  liveness, startup, preStop drain, PodDisruptionBudget, HPA, and demo-grade
  etcd/PostgreSQL services.
- Slice 6.3 is implemented for shared durable run/workflow/push-config state by
  the optional `postgres` feature in the A2A example. `RAKKA_PERSISTENCE` now
  selects `file` or `postgres`; Postgres mode connects three
  `PostgresDurableStateStore` instances and self-applies the persistence
  migration. The Phase 6 guide documents backup/restore expectations and calls
  out the remaining extraction follow-up for a shared PostgreSQL A2A
  task-event projection table.
- Slice 6.4 builds on the existing etcd discovery and reachability
  self-fencing modules. The Kubernetes config selects etcd, the topology guide
  documents lease TTL, membership refresh, lease revoke, partial partitions,
  and the separation between load-balancer health and shard ownership.
- Slice 6.5 is implemented by the new A2A ingress drain gate. `/drain` flips
  readiness to HTTP 503 with `ready=false`, keeps liveness HTTP 200, and
  rejects new mutating public A2A calls and new streams with the stable
  retryable `a2a-agent-draining` code while safe reads remain available.
  Existing shutdown still notifies discovery and leaves the local runtime.
- Slice 6.6 is documented in the Phase 6 topology guide and backed by existing
  stream-limit metrics, durable acceptance/outbox state, push scheduling,
  Rakka agent-workflow metrics/snapshots, and OTLP guidance. The doc repeats
  the bounded-label policy and names the runtime, outbox, recovery, human,
  shard, stream, task projection, and push snapshots needed for review.
- Slice 6.7 is documented in the Phase 6 topology guide and referenced by the
  HPA manifest. The HPA starts with CPU as a portable baseline and points
  operators at bounded scale-out signals: active streams, pending inbox
  commands, due outbox effects, dispatcher backlog, in-flight dispatches,
  stream lag, and A2A request latency.
- Slice 6.8 is implemented as a production failure-injection runbook in the
  Phase 6 topology guide, with existing cluster tests covering owner movement,
  duplicate retries, lazy recovery, stream reconnect behavior, and push effect
  scheduling. The runbook adds pod kill, dispatcher kill, ingress stream kill,
  Postgres restart, and scale up/down drills for real clusters.
- Slice 6.9 is captured in the Production-Candidate Review section of the
  Phase 6 guide. It scopes the current result as a production-shaped example,
  not a reusable `rakka-a2a` crate, and records review items for API stability,
  security/tenancy, migration/retention, task-event projection durability,
  push dispatch ownership, and metadata compatibility.

Validation added:

- `phase6_kubernetes_manifest_covers_public_private_and_persistence_paths`
  keeps the manifest shape tied to public ingress, private remoting, etcd,
  PostgreSQL, probes, drain, PDB, and HPA.
- `phase6_docs_cover_exit_criteria_and_known_boundaries` keeps the README and
  Phase 6 guide tied to the exit criteria and known production-candidate
  boundaries.
- `drain_closes_mutating_ingress_but_keeps_reads_available` verifies readiness
  flips to HTTP 503 while liveness stays HTTP 200, new sends are rejected with
  retryable `a2a-agent-draining`, and accepted task reads still work.
- `card_advertises_load_balancer_url_and_implemented_features` verifies
  streaming, intentionally disabled push advertisement, and load-balanced
  agent-card URLs.

## References

- `docs/plans/clustered-sharded-entity-a2a-agents/spec.md`
- `examples/clustered-sharded-entity-a2a-agents/doc/phase-6-production-topology.md`
- `examples/clustered-sharded-entity-a2a-agents/k8s/`
- `examples/clustered-sharded-entity-a2a-agents/Dockerfile`
- `examples/clustered-sharded-entity-a2a-agents/src/config.rs`
- `examples/clustered-sharded-entity-a2a-agents/src/durable_stores.rs`
- `examples/clustered-sharded-entity-a2a-agents/src/server.rs`
- `examples/clustered-sharded-entity-a2a-agents/src/a2a_handler.rs`
- `examples/clustered-sharded-entity-a2a-agents/src/agent_card.rs`
- `examples/clustered-agent-workflow-http-grpc/k8s/`
- `examples/clustered-agent-workflow-http-grpc/doc/kubernetes-etcd-discovery.md`
- `examples/clustered-agent-workflow-http-grpc/src/server.rs`
- `crates/rakka-k8s/src/health.rs`
- `crates/rakka-k8s/src/drain.rs`
- `crates/rakka-discovery-etcd/src/lib.rs`
- `crates/rakka-persistence-postgres/src/lib.rs`
- `crates/rakka-sharding-postgres/src/lib.rs`
- `crates/rakka-agent-workflow/src/kubernetes.rs`
- `crates/rakka-agent-workflow/src/snapshots.rs`
- `crates/rakka-agent-workflow/src/otlp.rs`
