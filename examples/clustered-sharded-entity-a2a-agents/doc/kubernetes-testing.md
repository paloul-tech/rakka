# Docker Image And Kubernetes Testing

This guide walks through building the container image for
`rakka-example-clustered-sharded-entity-a2a-agents` and exercising the full
clustered stack on a real Kubernetes cluster. It covers the image build, how to
get the image onto the cluster, applying the manifests, driving the public A2A
API, and the distributed behaviors worth testing (routing, drain, failover,
durable recovery).

The manifests referenced here live in
[`../k8s`](../k8s): `agent-a2a.yaml` (namespace, config, services, StatefulSet,
PodDisruptionBudget, HorizontalPodAutoscaler), `etcd.yaml`, and `postgres.yaml`.
See [`phase-6-production-topology.md`](phase-6-production-topology.md) for the
operating contract these manifests implement.

## What Gets Deployed

- A **2-replica `StatefulSet`** of the agent. Each pod serves private Rakka
  remoting on `25580` (headless `rakka-a2a-internal` Service) and public A2A
  HTTP on `35580` (behind the `rakka-a2a-public` `LoadBalancer` Service on port
  `80`).
- **etcd** for dynamic cluster membership discovery.
- **PostgreSQL** as the shared durable store for `AgentRunState`, workflow
  inbox/outbox state, and A2A push notification configs. The example runs the
  crate-owned migrations on startup (`PostgresA2ATaskProjectionStore::migrate`),
  so the `rakka_a2a_*` tables are created automatically.
- A `PodDisruptionBudget` (`minAvailable: 1`), a `HorizontalPodAutoscaler`
  (2→8 on CPU), readiness/liveness/startup probes, and a `preStop` `/drain`
  hook for graceful shutdown.

The demo workflow is a single-workflow catalog: id `workflow-a2a-phase-2-demo`,
type `a2a-phase-2-demo`. Because it is the only hosted workflow, `message:send`
requests do not need any workflow-selection metadata.

> The bundled etcd and PostgreSQL are **demo-grade**: single replica, no
> auth/TLS, ephemeral storage. Production should use HA/managed services,
> external secret management, network policy around the private
> remoting/etcd/PostgreSQL paths, and an HTTPS load balancer.

## Prerequisites

- A Kubernetes cluster and a `kubectl` context (kind, minikube, k3d, or a cloud
  cluster).
- `docker` for the image build, and `jq` for the example commands below.

## 1. Build The Docker Image

The [`Dockerfile`](../Dockerfile) is a multi-stage build. It compiles the
example with the optional `postgres` feature (so pods can share PostgreSQL
durable state) and produces a slim runtime image exposing the private remoting
port `25580` and the public A2A HTTP port `35580`.

Build from the **workspace root** (the build context is the whole workspace):

```sh
docker build -f examples/clustered-sharded-entity-a2a-agents/Dockerfile \
  -t rakka-clustered-a2a-agents:0.1.0 .
```

The resulting image runs the agent binary as its entrypoint; all configuration
is supplied through environment variables (see the ConfigMap and Secret in the
manifests).

## 2. Make The Image Visible To The Cluster

The StatefulSet uses `imagePullPolicy: IfNotPresent` with the local tag
`rakka-clustered-a2a-agents:0.1.0`, so the image must exist on the nodes. Pick
the path that matches your cluster:

- **kind:** `kind load docker-image rakka-clustered-a2a-agents:0.1.0`
- **minikube:** `minikube image load rakka-clustered-a2a-agents:0.1.0`
- **k3d:** `k3d image import rakka-clustered-a2a-agents:0.1.0 -c <cluster>`
- **Cloud cluster:** retag and push to a registry the cluster can pull from,
  then update `image:` in `k8s/agent-a2a.yaml`.

A pod stuck in `ImagePullBackOff` almost always means this step was skipped.

## 3. (Optional) Validate The Manifests

```sh
kubectl apply --dry-run=server -f examples/clustered-sharded-entity-a2a-agents/k8s/
```

## 4. Apply And Wait

```sh
kubectl apply -f examples/clustered-sharded-entity-a2a-agents/k8s/

# etcd + PostgreSQL come up first; the agent pods CrashLoop-retry until the
# durable store and discovery are Ready, which is expected.
kubectl -n rakka-a2a-agents rollout status deploy/rakka-a2a-etcd
kubectl -n rakka-a2a-agents rollout status deploy/rakka-a2a-postgres
kubectl -n rakka-a2a-agents rollout status statefulset/rakka-a2a-agent

kubectl -n rakka-a2a-agents get pods -o wide
```

## 5. Reach The Public API

On kind/minikube a `LoadBalancer` Service stays `<pending>` without extra setup
(MetalLB, `minikube tunnel`, or a cloud LB). The simplest path for testing is a
port-forward:

```sh
kubectl -n rakka-a2a-agents port-forward svc/rakka-a2a-public 8080:80
```

For real external access, point a cloud/ingress LB at `rakka-a2a-public` and set
`RAKKA_A2A_PUBLIC_URL` in the `rakka-a2a-agent-config` ConfigMap to the
externally reachable HTTPS URL so the agent card advertises it correctly.

Useful routes served on the public port:

- `GET /healthz`, `GET /readyz`, `GET /cluster`
- `GET /.well-known/agent-card.json`
- `POST /a2a/message:send`, `POST /a2a/message:stream`
- `GET|POST /a2a/tasks/{id}/subscribe`
- `POST|GET /a2a/tasks/{id}/pushNotificationConfigs`
- `GET|DELETE /a2a/tasks/{id}/pushNotificationConfigs/{config_id}`
- `POST /a2a/jsonrpc`

## 6. Exercise The A2A Surface

The example resolves the tenant from the `x-rakka-tenant` header (and request
body); the commands below use `tenant-a`.

```sh
# Agent card
curl -s localhost:8080/.well-known/agent-card.json | jq

# message:send (new task)
TASK=$(curl -s -X POST localhost:8080/a2a/message:send \
  -H 'content-type: application/json' -H 'x-rakka-tenant: tenant-a' \
  -d '{
    "message": { "messageId": "msg-1", "contextId": "ctx-1", "role": "ROLE_USER",
                 "parts": [{ "text": "hello from k8s" }] },
    "tenant": "tenant-a"
  }' | jq -r '.task.id // .id')
echo "task=$TASK"

# Read the task back (routes to the shard owner wherever it lives)
curl -s "localhost:8080/a2a/tasks/$TASK" -H 'x-rakka-tenant: tenant-a' | jq

# Stream public task events (SSE); reconnect with the last replay cursor is safe
curl -N -H 'x-rakka-tenant: tenant-a' "localhost:8080/a2a/tasks/$TASK/subscribe"

# Push notification config CRUD (durable, credential-redacted)
curl -s -X POST "localhost:8080/a2a/tasks/$TASK/pushNotificationConfigs" \
  -H 'content-type: application/json' -H 'x-rakka-tenant: tenant-a' \
  -d '{"pushNotificationConfig":{"url":"https://example.com/hook"}}' | jq
curl -s "localhost:8080/a2a/tasks/$TASK/pushNotificationConfigs" \
  -H 'x-rakka-tenant: tenant-a' | jq
```

## Automated Smoke Test

For a repeatable, one-command version of steps 4–7, use
[`../scripts/k8s-smoke-test.sh`](../scripts/k8s-smoke-test.sh). It applies the
stack, waits for readiness, sends a task, registers a push config, force-deletes
an agent pod, and asserts the task and its push config still resolve — proving
failover and durable recovery. It needs `kubectl`, `curl`, and `jq`, and the
image already loaded into the cluster (steps 1–2).

```sh
# Preview the plan without touching the cluster
SMOKE_DRY_RUN=1 examples/clustered-sharded-entity-a2a-agents/scripts/k8s-smoke-test.sh

# Run it (add SMOKE_CLEANUP=1 to delete the stack on success)
examples/clustered-sharded-entity-a2a-agents/scripts/k8s-smoke-test.sh
```

The manual steps below explain what the script automates and cover behaviors it
does not assert (scaling, drain, cross-node reads).

## 7. Test The Distributed Behaviors

These are the behaviors that only appear on a real multi-pod cluster:

- **Cross-node routing.** `kubectl exec` into each pod and hit its local
  `/a2a/tasks/$TASK`. Every pod answers: a non-owner routes the request to the
  shard owner over private remoting and reads the shared PostgreSQL projection.
- **Scale and rebalance.**
  `kubectl -n rakka-a2a-agents scale statefulset/rakka-a2a-agent --replicas=4`,
  send more tasks, and confirm ownership spreads. The HPA also scales on CPU.
- **Graceful drain / rolling update.**
  `kubectl -n rakka-a2a-agents rollout restart statefulset/rakka-a2a-agent`.
  The `preStop` hook calls `/drain`, which closes mutating ingress
  (`send`/`cancel`) while reads and streams stay available; open streams
  reconnect to the new owner with their replay cursor (no gap, no duplicate).
- **Owner failover and recovery.**
  `kubectl -n rakka-a2a-agents delete pod rakka-a2a-agent-0 --grace-period=0 --force`,
  then re-read `$TASK`. It recovers from durable run + inbox state on whichever
  node picks up the shard.
- **Durable survival.** Confirm the task and its push config survive pod
  restarts — they live in PostgreSQL, not pod memory.

## 8. Observe

```sh
# Structured tracing events (spawn/stop, undecodable inbox entries, deferred
# push scheduling, PostgreSQL connection close) surface here.
kubectl -n rakka-a2a-agents logs statefulset/rakka-a2a-agent -f

# Cluster membership and readiness
curl -s localhost:8080/cluster | jq
kubectl -n rakka-a2a-agents get hpa,pdb

# Inspect the durable model directly
kubectl -n rakka-a2a-agents exec deploy/rakka-a2a-postgres -- \
  psql -U postgres -c '\dt' -c 'select task_id, status from rakka_a2a_tasks;'
```

## 9. Cleanup

```sh
kubectl delete -f examples/clustered-sharded-entity-a2a-agents/k8s/
```

## Gotchas

- **Image visibility** is the most common failure — `ImagePullBackOff` means the
  `kind`/`minikube`/`k3d` image-load step was skipped or a registry is needed.
- **`LoadBalancer` stays `<pending>`** on bare local clusters — use a
  port-forward or a tunnel; this is expected, not a failure.
- **OpenTelemetry:** the ConfigMap points `OTEL_EXPORTER_OTLP_ENDPOINT` at a
  collector (`rakka-otel-collector.rakka-system…:4317`) that these manifests do
  not include. If you are not running a collector, ignore the export-connection
  warnings or remove the `OTEL_*` keys from `rakka-a2a-agent-config`.
- **Ephemeral demo storage:** restarting the PostgreSQL or etcd pod discards its
  state. For a real durability test, back PostgreSQL with a `PersistentVolume`
  or use a managed instance.
- **Self-fencing:** `RAKKA_SELF_FENCE` is enabled; a pod that cannot reach peers
  through etcd removes itself. With `replicas: 2` and the PDB `minAvailable: 1`,
  a network-partitioned pod fencing itself is expected behavior.
