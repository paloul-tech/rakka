# Clustered Agent Workflow HTTP + gRPC Example

This example runs one Rakka node per process. The nodes discover each other through a shared directory and form a cluster, then host **durable, compiled agent workflows** sharded across the cluster. Each node exposes **one public ingress — HTTP or gRPC — chosen by a CLI argument**, accepting a compiled workflow definition (a graph of nodes and edges). Rakka compiles it into an `AgentCompiledExecutionPlan`, routes it to the cluster node that owns the run **over `rakka-remote` TCP**, and executes it there with the deterministic graph scheduler and durable run state.

HTTP and gRPC are public **ingress** only, and a process exposes exactly one of them at a time. Node-to-node communication always uses `rakka-remote`, Rakka's native transport. Because clustering is independent of the ingress, nodes serving different ingresses still form one cluster and route runs to each other. All Rakka APIs are imported through the top-level `rakka` facade crate (`rakka::prelude`, `rakka::cluster`, `rakka::sharding`, `rakka::remote`, `rakka::agent_workflow`, `rakka::http`, `rakka::grpc`, `rakka::persistence`).

## What it demonstrates

- **Cluster membership** from a tiny file-based discovery directory feeding a `ClusterNodeRuntime` (Rakka TCP remoting).
- **Sharded ownership** of runs: a run's id is the sharded entity id, and the sharding region decides which node owns it. Every node agrees on the owner for a given run id.
- **Native inter-node routing**: a non-owning node sends the (serializable) compiled plan to the owner with `ShardedEntityRef::remote_ask` over `rakka-remote` TCP — one round trip, regardless of which ingress received the request.
- **Selectable ingress**: a protocol-neutral ingress core is shared by thin HTTP (`axum` via `rakka::http`) and gRPC (`tonic` + `rakka::grpc`) adapters; `main` runs exactly one.
- **Distributed, durable execution**: the owning node hosts the run as a sharded `RunEntity` backed by a durable `AgentRunActor`, driven `StartGraph -> { MarkGraphReady -> StartGraphNode -> CompleteGraphNode }` to a terminal status. Run/workflow state lives in shared directories, so a run can recover on a new owner after shard movement or a restart.

## Run

Start two or more nodes in separate terminals. Give each a different `RAKKA_PORT`, point them at the **same** discovery and state directories, and pass the ingress to run (`http` or `grpc`):

```sh
# Terminal 1 (HTTP ingress)
RAKKA_DISCOVERY_DIR=/tmp/rakka-agent-demo/disc \
  RAKKA_STATE_DIR=/tmp/rakka-agent-demo/state \
  RAKKA_PORT=25530 \
  cargo run -p rakka-example-clustered-agent-workflow-http-grpc -- http

# Terminal 2 (HTTP ingress)
RAKKA_DISCOVERY_DIR=/tmp/rakka-agent-demo/disc \
  RAKKA_STATE_DIR=/tmp/rakka-agent-demo/state \
  RAKKA_PORT=25531 \
  cargo run -p rakka-example-clustered-agent-workflow-http-grpc -- grpc
```

Pass `grpc` instead of `http` to expose the gRPC ingress; no argument defaults to `http`. The HTTP port defaults to `RAKKA_PORT + 10000` and the gRPC port to `RAKKA_PORT + 20000`, so node one listens on HTTP `127.0.0.1:35530` / gRPC `127.0.0.1:45530`, and node two on HTTP `127.0.0.1:35531` / gRPC `127.0.0.1:45531`.

## HTTP ingress

A compiled workflow definition is a graph: a list of `nodes` and the directed `edges` between them. Submit it to **any** node; Rakka executes it on the node that owns the run. A ready-to-run definition is included at [`sample-workflow.json`](sample-workflow.json):

```sh
curl -s http://127.0.0.1:35530/cluster
curl -s -X POST http://127.0.0.1:35530/workflows \
  -H 'content-type: application/json' \
  --data-binary @examples/clustered-agent-workflow-http-grpc/sample-workflow.json
curl -s http://127.0.0.1:35530/workflows/sample-research-1
```

An inline diamond (fan-out / fan-in) graph runs the two middle nodes in parallel and joins them:

```sh
curl -s -X POST http://127.0.0.1:35530/workflows \
  -H 'content-type: application/json' \
  -d '{
        "run_id": "diamond-1",
        "nodes": [{"id":"input"},{"id":"a"},{"id":"b"},{"id":"join"},{"id":"done"}],
        "edges": [
          {"from":"input","to":"a"}, {"from":"input","to":"b"},
          {"from":"a","to":"join"}, {"from":"b","to":"join"},
          {"from":"join","to":"done"}
        ]
      }'
```

A response looks like:

```json
{
  "run_id": "diamond-1",
  "owner_node": "agent-node-25531#uid-25531-...",
  "executed_locally": false,
  "served_by": "agent-node-25530#uid-25530-...",
  "status": "completed",
  "plan_id": "plan-diamond-1",
  "plan_fingerprint": "fp1:8e6ce55e2881eda5",
  "node_count": 5,
  "completed_node_count": 4,
  "terminal_node_count": 1,
  "nodes": [ /* per-node status */ ]
}
```

- `owner_node` is the cluster node that actually executed the run.
- `served_by` is the node that received your request.
- `executed_locally` is `served_by == owner_node`. When `false`, the request was routed to the owner over `rakka-remote`.

The HTTP routes are `POST /workflows`, `GET /workflows/:run_id`, `GET /cluster`, and `GET /health`.

## gRPC ingress

The gRPC service `AgentWorkflowIngress` mirrors the HTTP routes (`SubmitWorkflow`, `GetWorkflow`, `GetCluster`). The contract is [`proto/rakka/examples/agent_workflow/v1/agent_workflow.proto`](proto/rakka/examples/agent_workflow/v1/agent_workflow.proto). Using `grpcurl` from the repo root:

```sh
PROTO_DIR=examples/clustered-agent-workflow-http-grpc/proto
PROTO=rakka/examples/agent_workflow/v1/agent_workflow.proto
SVC=rakka.examples.agent_workflow.v1.AgentWorkflowIngress

grpcurl -plaintext -import-path $PROTO_DIR -proto $PROTO \
  -d '{}' 127.0.0.1:45530 $SVC/GetCluster

grpcurl -plaintext -import-path $PROTO_DIR -proto $PROTO \
  -d '{"run_id":"research-1","nodes":[{"id":"input"},{"id":"summarize"},{"id":"done"}],"edges":[{"from":"input","to":"summarize"},{"from":"summarize","to":"done"}]}' \
  127.0.0.1:45530 $SVC/SubmitWorkflow

grpcurl -plaintext -import-path $PROTO_DIR -proto $PROTO \
  -d '{"run_id":"research-1"}' 127.0.0.1:45531 $SVC/GetWorkflow
```

Errors map to gRPC status codes: an invalid graph returns `InvalidArgument`, an unknown run returns `NotFound`, and an unreachable owner returns `Unavailable`.

## Request schema

| Field | Required | Meaning |
| --- | --- | --- |
| `nodes` | yes | Array of `{ "id": "...", "kind": "input\|transform\|terminal" }`. `kind` is optional. |
| `edges` | no | Array of `{ "from": "node-id", "to": "node-id" }`. |
| `run_id` | no | Stable run id (and sharded entity id). Generated when omitted. |
| `plan_id` | no | Diagnostic plan id. Generated when omitted. |
| `entry_nodes` | no | Nodes the scheduler starts first. Defaults to every node with no incoming edge. |

When `kind` is omitted it is inferred from the node's position: no incoming edges → `input`, no outgoing edges → `terminal`, otherwise `transform`. The plan is validated with `rakka::agent_workflow::validate_compiled_execution_plan`, so cycles, missing terminals, and unreachable required inputs are rejected (HTTP `400` / gRPC `InvalidArgument`).

## How distribution and durability work

- **Ownership** is decided by the sharding region: the run id hashes to a shard, and the cluster coordinator (deterministic-modulo allocation, fed by discovery) maps shards to up members. Every node resolves the same owner for a given run id via `region().resolve(...)`.
- **Routing**: a non-owner node sends the serializable compiled plan to the owner with `remote_ask` over `rakka-remote` TCP — one round trip carrying the plan. The owner runs the chatty `MarkGraphReady`/`StartGraphNode`/`CompleteGraphNode` loop locally against its child run actor, so that loop never crosses the network.
- **Single writer**: only the owner ever drives a given run, so there is exactly one active run actor for a run id across the cluster.
- **Failover**: run and workflow state live in the shared `RAKKA_STATE_DIR`. If the owner exits, its discovery record expires (after the discovery TTL) and ownership moves to a live node, which recovers the run from the shared store on first reference. During the detection window, requests for runs owned by the exited node may be unavailable.

## Discovery providers

Cluster membership discovery is pluggable via `RAKKA_DISCOVERY_PROVIDER`:

- `file` (default): a shared directory; good for local multi-terminal runs.
- `etcd`: dynamic register/lease/watch; pods join and leave at runtime, so it fits Kubernetes autoscaling. Each node registers `<RAKKA_ETCD_PREFIX><node-id>` under a lease, renews it every poll, and lists the prefix to learn peers; a crashed or scaled-in node's key disappears when its lease lapses (or is revoked on graceful shutdown). Both providers feed the same `apply_discovery`/`tick` path, so routing and execution are unchanged.

Run two nodes against a local etcd (Docker):

```sh
docker run -d --name etcd -p 2379:2379 \
  -e ALLOW_NONE_AUTHENTICATION=yes \
  -e ETCD_ADVERTISE_CLIENT_URLS=http://0.0.0.0:2379 \
  -e ETCD_LISTEN_CLIENT_URLS=http://0.0.0.0:2379 \
  quay.io/coreos/etcd:v3.5.17

RAKKA_DISCOVERY_PROVIDER=etcd RAKKA_ETCD_ENDPOINTS=http://127.0.0.1:2379 \
  RAKKA_STATE_DIR=/tmp/rakka-agent-demo/state RAKKA_PORT=25530 \
  cargo run -p rakka-example-clustered-agent-workflow-http-grpc -- http

RAKKA_DISCOVERY_PROVIDER=etcd RAKKA_ETCD_ENDPOINTS=http://127.0.0.1:2379 \
  RAKKA_STATE_DIR=/tmp/rakka-agent-demo/state RAKKA_PORT=25531 \
  cargo run -p rakka-example-clustered-agent-workflow-http-grpc -- grpc
```

Nodes serving different ingresses still form one cluster (clustering is over `rakka-remote`, independent of the ingress).

## Durable persistence

`RAKKA_PERSISTENCE` selects the durable store: `file` (default, single host) or `postgres` (shared, required for multi-pod run recovery). The PostgreSQL store is behind the `postgres` build feature and self-migrates its tables on startup:

```sh
RAKKA_PERSISTENCE=postgres \
  RAKKA_POSTGRES_DSN="host=127.0.0.1 user=postgres password=postgres dbname=postgres" \
  cargo run -p rakka-example-clustered-agent-workflow-http-grpc --features postgres -- http
```

## Kubernetes deployment (with etcd + PostgreSQL)

The manifests in [`k8s/`](k8s/) deploy the full stack: a single-node **etcd** for dynamic membership discovery, a single-node **PostgreSQL** for the shared durable store, and a horizontally autoscaled **StatefulSet** of app nodes wired to both. (etcd and Postgres are deployed as demo-grade single replicas with ephemeral storage; production should use an HA etcd and a managed/HA PostgreSQL.)

**1. Build the image** from the workspace root (the builder stage needs `protoc`; the `postgres` feature is compiled in). On Docker Desktop or a registry-less local cluster the local image is used directly:

```sh
docker build -f examples/clustered-agent-workflow-http-grpc/Dockerfile \
  -t rakka-agent-workflow-http-grpc:0.1.0 .
```

**2. Apply the manifests** (creates the `rakka-agent-workflow` namespace with etcd, PostgreSQL, the app StatefulSet, Services, a PodDisruptionBudget, and a HorizontalPodAutoscaler) and wait for the pods:

```sh
kubectl apply -f examples/clustered-agent-workflow-http-grpc/k8s/
kubectl -n rakka-agent-workflow rollout status statefulset/agent-workflow --timeout=180s
kubectl -n rakka-agent-workflow get pods
```

**3. Submit a workflow and inspect the cluster** through the public HTTP service:

```sh
kubectl -n rakka-agent-workflow port-forward svc/agent-workflow-http 18080:80 &
curl -s http://127.0.0.1:18080/cluster
curl -s -X POST http://127.0.0.1:18080/workflows -H 'content-type: application/json' \
  --data-binary @examples/clustered-agent-workflow-http-grpc/sample-workflow.json
curl -s http://127.0.0.1:18080/workflows/sample-research-1
```

**4. Watch dynamic membership** as you scale — etcd registers new members at runtime and drops them on scale-in (their lease is revoked on the preStop drain):

```sh
ETCD=$(kubectl -n rakka-agent-workflow get pod -l app.kubernetes.io/name=rakka-etcd -o jsonpath='{.items[0].metadata.name}')
kubectl -n rakka-agent-workflow scale statefulset agent-workflow --replicas=4
kubectl -n rakka-agent-workflow exec "$ETCD" -- etcdctl get --prefix /rakka/agent-workflow/members/ --keys-only
kubectl -n rakka-agent-workflow scale statefulset agent-workflow --replicas=2
```

Verify durable state landed in PostgreSQL:

```sh
PG=$(kubectl -n rakka-agent-workflow get pod -l app.kubernetes.io/name=rakka-postgres -o jsonpath='{.items[0].metadata.name}')
kubectl -n rakka-agent-workflow exec "$PG" -- psql -U postgres -d postgres \
  -c "select persistence_id, revision from rakka_durable_state order by 1;"
```

Tear down with `kubectl delete namespace rakka-agent-workflow`.

The app nodes:

- discover each other dynamically through etcd (`RAKKA_DISCOVERY_PROVIDER=etcd`),
- share durable state in PostgreSQL (`RAKKA_PERSISTENCE=postgres`) so runs recover on a new owner during scale-in or pod failure,
- talk pod-to-pod over `rakka-remote` via a headless Service,
- derive identity/address from the downward API (`RAKKA_POD_NAME` / `RAKKA_POD_UID` / `RAKKA_POD_IP`), and
- scale via a `HorizontalPodAutoscaler` (CPU metrics require a metrics-server), with a `PodDisruptionBudget` and a preStop drain that revokes the etcd lease and leaves the cluster cleanly.

Switch a node to the gRPC ingress by setting its container `args` to `["grpc"]` and exposing the gRPC port. The full design rationale is in [`doc/kubernetes-etcd-discovery.md`](doc/kubernetes-etcd-discovery.md), and an Akka↔Rakka review of the sharding/coordination/rebalancing/split‑brain model is in [`doc/akka-comparison.md`](doc/akka-comparison.md).

## Environment variables

- `RAKKA_PORT`: Rakka TCP remoting port for inter-node communication; also the source of the stable logical node id. Defaults to `25530`.
- `RAKKA_HTTP_PORT`: public HTTP ingress port (used in `http` mode). Defaults to `RAKKA_PORT + 10000`.
- `RAKKA_GRPC_PORT`: public gRPC ingress port (used in `grpc` mode). Defaults to `RAKKA_PORT + 20000`.
- `RAKKA_DISCOVERY_DIR`: shared directory used by file discovery. Share it across processes to form one cluster.
- `RAKKA_STATE_DIR`: shared base directory for durable run and workflow state. Share it so runs recover across owners.
- `RAKKA_BIND_HOST`: local IP to bind. Defaults to `127.0.0.1`.
- `RAKKA_ADVERTISE_HOST`: host written into discovery records. Defaults to `RAKKA_BIND_HOST`.
- `RAKKA_NODE_LOGICAL_ID`: override the stable logical node id. Defaults to `RAKKA_POD_NAME`, else `agent-node-<RAKKA_PORT>`.
- `RAKKA_NODE_INCARNATION`: override the per-process incarnation. Defaults to `RAKKA_POD_UID`, else a fresh value each start.
- `RAKKA_POD_NAME` / `RAKKA_POD_UID` / `RAKKA_POD_IP`: Kubernetes downward-API pod identity/address; used as the logical id / incarnation / advertise host when the explicit overrides above are unset.
- `RAKKA_DISCOVERY_PROVIDER`: `file` (default) or `etcd`.
- `RAKKA_ETCD_ENDPOINTS`: comma-separated etcd endpoints (etcd mode). Defaults to `http://127.0.0.1:2379`.
- `RAKKA_ETCD_PREFIX`: etcd key prefix for member registration. Defaults to `/rakka/agent-workflow/members/`.
- `RAKKA_ETCD_LEASE_TTL_SECONDS`: member lease TTL. Defaults to `10`.
- `RAKKA_PERSISTENCE`: `file` (default) or `postgres` (requires the `postgres` build feature).
- `RAKKA_POSTGRES_DSN`: PostgreSQL connection string (required when `RAKKA_PERSISTENCE=postgres`).

## Scope and simplifications

This example focuses on the cluster, sharding, durable run, inter-node remoting, and selectable HTTP/gRPC ingress wiring. To keep it self-contained:

- Nodes are executed as deterministic local work (`input`, `transform`, `terminal`). Effect-producing kinds (`model-call`, `tool-call`, ...), branches, joins-with-conditions, iterators, timers, and human checkpoints are part of `rakka-agent-workflow` but are out of scope here; see that crate's tests and `docs/plans/compiled_execution_with_graph_schdlr/` for those.
- HTTP and gRPC are public ingress only; inter-node communication uses `rakka-remote`. The rich `AgentRunActorCommand` protocol carries an `Arc<plan>` and reply channels, so it is process-local and not directly sendable over the wire. The example therefore wraps the (serializable) compiled plan in a small `RunRequest` and registers a `RunEntity` with `init_remote_with_ask`; Rakka serializes the request to the owner, which maps it to a local command and drives a child `AgentRunActor`.
- File discovery and the file store are the local-dev defaults; for Kubernetes use the `etcd` discovery provider and the `postgres` durable store (above). Ownership uses a per-node deterministic-modulo coordinator (every node computes the same owners), which suits this symmetric topology. A shared, fenced PostgreSQL shard coordinator (store + lease) was evaluated and is intentionally not used — it requires a single-coordinator topology incompatible with symmetric per-node hosting; see [`doc/kubernetes-etcd-discovery.md`](doc/kubernetes-etcd-discovery.md).
- Ownership is eventually consistent during membership changes; a request that arrives mid-change may briefly route to the previous owner.
- `protoc` (the Protocol Buffers compiler) must be installed to build the example, because the gRPC contract is generated at build time.
