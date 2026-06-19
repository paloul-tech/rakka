# Rakka Agent Workflow Kubernetes Reference Topology

Status: Slice 6.1 reference artifact.

This topology is the first production-shaped deployment contract for Rakka
agent workflows. It is written as raw Kubernetes YAML so it can be reviewed and
validated today, while keeping names and values stable enough to become
Helm-style templates later.

## Local Defaults

- Kubernetes context: Docker Desktop.
- Namespace: `rakka-system`.
- PostgreSQL: existing local Docker container published as
  `0.0.0.0:5432->5432/tcp`.
- Cluster-to-host PostgreSQL route:
  `rakka-postgres.rakka-system.svc.cluster.local` as an `ExternalName` Service
  pointing to `host.docker.internal`.
- Local PostgreSQL DSN:
  `postgres://postgres:postgres@rakka-postgres.rakka-system.svc.cluster.local:5432/postgres`.
- OTLP endpoint:
  `http://rakka-otel-collector.rakka-system.svc.cluster.local:4317`.
- Default app image placeholder:
  `ghcr.io/rakka-rs/rakka-agent-workflow:0.1.0`.

## Resource Shape

The reference manifest defines:

- `Namespace/rakka-system`.
- `ServiceAccount/rakka-agent-workflow`.
- `ConfigMap/rakka-agent-workflow-config` for runtime, compatibility,
  remoting, endpoint, PostgreSQL, artifact-store, and OpenTelemetry defaults.
- `Secret/rakka-postgres-credentials` for the local PostgreSQL DSN and
  credentials.
- `Secret/rakka-artifact-store-credentials` for object-store credentials.
- `Service/rakka-postgres`, an `ExternalName` to `host.docker.internal`.
- `Service/rakka-agent-internal`, a headless internal remoting service.
- `Service/rakka-agent-public`, a public HTTP/gRPC ClusterIP service.
- `PodDisruptionBudget/rakka-agent-workflow`.
- `Deployment/rakka-agent-workflow` with three replicas, rolling update,
  readiness, liveness, startup, and pre-stop drain hooks.

## Runtime Contract

The application image should expose:

- HTTP readiness at `GET /ready`.
- HTTP liveness at `GET /live`.
- HTTP pre-stop drain at `GET /drain`.
- Prometheus metrics at `GET /metrics`.
- JSON operational snapshots at `GET /snapshots`.
- Public gRPC on port `50051`, when the application enables gRPC.
- Internal Rakka remoting on port `2552`.

Readiness should stay false until the telemetry resource, OTLP exporter,
PostgreSQL stores, durable state, query indexes, artifact-store configuration,
actor system, remoting, sharding, workflow registry, snapshots, and
compatibility policy are ready. The pre-stop drain endpoint should mark
readiness false before stopping ingress or handing off workflow work. The
complete startup order is captured in `kubernetes-startup-readiness.md`, and
the drain/shutdown order is captured in `kubernetes-drain-shutdown.md`.
Autoscaling metric signals are captured in
`kubernetes-autoscaling-signals.md`.

## Service Boundaries

`rakka-agent-public` is the only service intended for user-facing HTTP/gRPC
traffic. It exposes the `http` and `grpc` ports only.

`rakka-agent-internal` is headless and exposes only the `remoting` port. It is
for Rakka node-to-node traffic and Kubernetes DNS discovery. It should not be
exposed through public ingress.

`rakka-postgres` is local-development wiring only. In a production cluster,
replace it with a managed PostgreSQL endpoint, an internal service, or a
cloud-provider connection method. The application should continue to consume
`RAKKA_POSTGRES_DSN` from `rakka-postgres-credentials`.

## Object Storage

The manifest models object storage as S3-compatible configuration:

- `RAKKA_ARTIFACT_STORE_KIND=s3-compatible`.
- `RAKKA_ARTIFACT_ENDPOINT=http://rakka-object-store.rakka-system.svc.cluster.local:9000`.
- `RAKKA_ARTIFACT_BUCKET=rakka-agent-artifacts`.

The endpoint is a placeholder for local MinIO or a production object store.
Phase 6.6 should lock down credentials, network policy, and tool-access
boundaries.

## Compatibility And Migration

The deployment carries N/N+1 compatibility metadata:

- `RAKKA_PROTOCOL_VERSION=1.0`.
- `RAKKA_COMPAT_MIN=1.0`.
- `RAKKA_COMPAT_MAX=1.1`.
- `RAKKA_AGENT_WORKFLOW_COMPAT_POLICY=n-to-n-plus-one`.
- `RAKKA_AGENT_WORKFLOW_CURRENT_STATE_SCHEMA_VERSION=1`.
- `RAKKA_AGENT_WORKFLOW_CURRENT_INDEX_SCHEMA_VERSION=1`.

The application should fail readiness if durable state, index schema, or
workflow definition compatibility checks reject the current deployment.

## Local Validation

Create the namespace and validate the manifest shape:

```sh
kubectl apply --dry-run=client -f docs/plans/agentic-workflow/kubernetes-reference-topology.yaml
```

Check that pods can reach the local Docker PostgreSQL container after applying
the topology:

```sh
kubectl -n rakka-system run pg-check \
  --rm -it --restart=Never \
  --image=postgres:16-alpine \
  --env PGPASSWORD=postgres \
  -- psql -h rakka-postgres -U postgres -d postgres -c "select 1"
```

## Helm Path

The current names should become chart values:

- `namespaceOverride`.
- `image.repository`, `image.tag`, `image.pullPolicy`.
- `replicaCount`.
- `postgres.externalName`, `postgres.port`, `postgres.secretName`.
- `artifactStore.kind`, `artifactStore.endpoint`, `artifactStore.bucket`.
- `otel.endpoint`, `otel.protocol`.
- `service.public.name`, `service.internal.name`.
- `ports.http`, `ports.grpc`, `ports.remoting`.
- `compat.protocolVersion`, `compat.min`, `compat.max`.
- `workflow.stateSchemaVersion`, `workflow.indexSchemaVersion`.

Keeping the raw manifest deterministic first gives us stable contract tests
before introducing template rendering.
