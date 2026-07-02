# Phase 6 Clustered Sharded Entity A2A Agents

Status: planning draft
Source spec: `docs/plans/clustered-sharded-entity-a2a-agents/spec.md`

## Goal

Harden the A2A agent runtime for production-style Kubernetes deployment:
load-balanced ingress, private Rakka remoting, external discovery, PostgreSQL
persistence, operational telemetry, graceful drain, autoscaling, and
failure-injection coverage.

## Slices

### Slice 6.1: Production Topology Document

Status: planned

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

Status: planned

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

Status: planned

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

Status: planned

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

Status: planned

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

Status: planned

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

Status: planned

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

Status: planned

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

Status: planned

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

## References

- `docs/plans/clustered-sharded-entity-a2a-agents/spec.md`
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
