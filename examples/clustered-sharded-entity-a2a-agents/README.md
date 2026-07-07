# Clustered Sharded Entity A2A Agents

Phase 6 runnable example for exposing durable Rakka agent runs through the A2A
Rust SDK with clustered sharded run ownership. This example is the incubator for
a future `rakka-a2a` crate; no reusable public Rakka A2A API is introduced here.

## SDK Choice

- `a2a-lf = 0.3.0` is imported as the `a2a` library crate.
- `a2a-server-lf = 0.4.0` is imported as the `a2a_server` library crate.
- `a2a-server-lf` is used with `default-features = false`, so this skeleton does
  not pull TLS server helpers. A2A gRPC and SLIMRPC crates are not enabled.
- The current server SDK still pulls `a2a-pb` and `tonic` transitively for
  ProtoJSON conversion even though this example does not mount A2A gRPC.
- The published A2A crates currently require Rust 1.85, so the Rakka workspace
  MSRV is Rust 1.85.

## Run

```sh
cargo run -p rakka-example-clustered-sharded-entity-a2a-agents
```

Useful routes:

- `GET /healthz`
- `GET /readyz`
- `GET /cluster`
- `GET /.well-known/agent-card.json`
- `POST /a2a/message:send`
- `POST /a2a/message:stream`
- `GET|POST /a2a/tasks/{id}/subscribe`
- `POST|GET /a2a/tasks/{id}/pushNotificationConfigs`
- `GET|DELETE /a2a/tasks/{id}/pushNotificationConfigs/{config_id}`
- `POST /a2a/jsonrpc`

Run two local nodes with shared file discovery and shared example file state:

```sh
RAKKA_DISCOVERY_DIR=/tmp/rakka-a2a-discovery RAKKA_STATE_DIR=/tmp/rakka-a2a-state \
  RAKKA_PORT=25580 RAKKA_HTTP_PORT=35580 \
  cargo run -p rakka-example-clustered-sharded-entity-a2a-agents

RAKKA_DISCOVERY_DIR=/tmp/rakka-a2a-discovery RAKKA_STATE_DIR=/tmp/rakka-a2a-state \
  RAKKA_PORT=25581 RAKKA_HTTP_PORT=35581 \
  cargo run -p rakka-example-clustered-sharded-entity-a2a-agents
```

Run against etcd-backed discovery for production-like membership testing:

```sh
RAKKA_DISCOVERY_PROVIDER=etcd RAKKA_ETCD_ENDPOINTS=http://127.0.0.1:2379 \
  RAKKA_STATE_DIR=/tmp/rakka-a2a-state RAKKA_PORT=25580 RAKKA_HTTP_PORT=35580 \
  cargo run -p rakka-example-clustered-sharded-entity-a2a-agents
```

Set `RAKKA_A2A_PUBLIC_URL=https://example.com/agents/demo` when the agent card
should advertise a load-balanced public URL. Without it, developer mode
advertises the local HTTP address.

Rakka remoting stays private to node-to-node communication on `RAKKA_PORT`.
Public A2A clients should use the HTTP address or a load-balanced
`RAKKA_A2A_PUBLIC_URL`, not the Rakka remoting address.

## Production-Like Mode

`RAKKA_PERSISTENCE` selects the durable store:

- `file` (default): one-host local development only.
- `postgres`: shared durable state for multi-pod recovery, built with
  `--features postgres`.

```sh
RAKKA_DISCOVERY_PROVIDER=etcd \
RAKKA_ETCD_ENDPOINTS=http://127.0.0.1:2379 \
RAKKA_PERSISTENCE=postgres \
RAKKA_POSTGRES_DSN="host=127.0.0.1 user=postgres password=postgres dbname=postgres" \
RAKKA_A2A_PUBLIC_URL=https://agents.example.test/rakka-a2a \
  cargo run -p rakka-example-clustered-sharded-entity-a2a-agents --features postgres
```

Build the Kubernetes image and apply the demo stack:

```sh
docker build -f examples/clustered-sharded-entity-a2a-agents/Dockerfile \
  -t rakka-clustered-a2a-agents:0.1.0 .

kubectl apply -f examples/clustered-sharded-entity-a2a-agents/k8s/
```

The manifests include demo-grade single-node etcd and PostgreSQL. Production
deployments should use HA/managed services, external secret management, network
policy around private remoting/etcd/PostgreSQL, and an HTTPS load balancer whose
URL is supplied through `RAKKA_A2A_PUBLIC_URL`.

See [`doc/phase-6-production-topology.md`](doc/phase-6-production-topology.md)
for the Phase 6 topology, drain, telemetry, autoscaling, failure-injection, and
production-candidate review contract.

## Phase 6 Boundary

Implemented:

- A real `ActorSystem`, `ClusterNodeRuntime`, `ClusterSharding`, demo
  `AgentWorkflow`, and clustered sharded A2A run entity registration.
- Local file discovery for one or more developer-mode nodes.
- Etcd-backed discovery with peer-reachability self-fencing for
  production-like testing.
- Shared example file-backed durable stores for run state and workflow
  inbox/outbox recovery across local nodes.
- A static A2A agent card with REST/HTTP+JSON and JSON-RPC interfaces.
- A2A REST and JSON-RPC routers mounted beside Rakka health/cluster routes.
- A2A identity and `io.rakka.*` metadata normalization into Rakka command
  drafts.
- A2A message/part conversion into bounded inline payloads or artifact
  references.
- A versioned remote-safe `A2ARunRequest`/`A2ARunResponse` protocol registered
  in the Rakka serialization registry.
- An `A2ARunEntity` owner shell registered with
  `ClusterSharding::init_remote_with_ask`; it hosts a local `AgentRunActor` and
  maps remote-safe requests to local `AgentRunActorCommand` messages.
- Cluster routing for `send_message`, `get_task`, and `cancel_task`; any public
  node can route those requests to the shard owner by `Task.id`.
- Durable `AgentRunInbox` acceptance for `message:send` and `cancel_task`.
- Idempotent duplicate handling for repeated A2A `messageId`/deduplication keys.
- Owner-local `AgentRunState` creation for new tasks, and durable cancellation that
  completes accepted cancels to the terminal `Cancelled` state (nothing is in
  flight in this phase, so `Cancelling` is transient).
- A task projection model, public task-event model, replay cursors, and a local
  in-memory task projection store. Owner responses carry projection snapshots so
  ingress nodes can answer routed requests and cache the projection locally.
- Projection-backed `send_message`, `get_task`, `list_tasks`, and `cancel_task`.
- A2A `send_streaming_message` and `subscribe_to_task` served from public task
  events, with current-task snapshots, bounded replay cursors, live projection
  watchers, heartbeats, and terminal stream completion. Streams opened on a
  non-owner public node receive live updates by polling the shard owner's
  event log through the sharded run protocol (`OpenStreamCursor`).
- Bounded stream admission with per-node and per-task limits plus bounded
  metrics for opened, closed, over-limit, lagged, dropped, and replay work.
- Durable A2A push notification config create/get/list/delete keyed by tenant,
  task id, and config id. Stored configs redact tokens and credentials while
  retaining secret-presence audit metadata.
- Request-level push configs from `message:send` and `message:stream`.
- Push notification work scheduled as `AgentEffectKind::Notification` durable
  outbox effects after public task events are emitted. Request handlers do not
  call external webhook URLs directly.
- Optional PostgreSQL durable-state mode for run state, workflow inbox/outbox
  state, and A2A push notification configs.
- Kubernetes manifests for public load-balanced A2A HTTP, private Rakka
  remoting, etcd discovery, PostgreSQL persistence, readiness, liveness,
  startup, drain, PodDisruptionBudget, and HorizontalPodAutoscaler guidance.
- `/drain` closes mutating public A2A ingress with the stable
  `a2a-agent-draining` code while keeping safe reads available until shutdown.
- Agent cards advertise streaming support and use `RAKKA_A2A_PUBLIC_URL` for
  load-balanced production URLs.

Not implemented in Phase 6:

- A production HTTP webhook dispatcher for the scheduled A2A push outbox
  effects. The durable effect record carries the callback target and bounded
  labels; an adapter-owned worker should resolve any credential binding and send
  the webhook. The agent card keeps `push_notifications=false` until delivery
  exists.
- Step execution to completion or peer A2A calls.
- A shared PostgreSQL A2A task-event projection table. The example rebuilds
  current task projections from durable run/inbox state and uses owner polling
  for cross-node replay; a reusable `rakka-a2a` crate should promote task-event
  replay to a shared durable projection.
- A reusable `rakka-a2a` crate or top-level `rakka` facade feature.

## SSE And Load Balancers

- Sticky sessions are optional for correctness. Any public node can authorize a
  task and open a stream; when the current node is not the shard owner it
  routes the snapshot query to the owner and then polls the owner's event log
  (every 2 seconds) for live updates, so non-owner streams observe status,
  message, and terminal events with at most one poll interval of added
  latency. Owner-node streams deliver events immediately.
- Reconnect clients with the latest replay cursor. The handler accepts
  `rakka-a2a-replay-cursor` or `last-event-id` service params when the selected
  binding forwards them. In clustered mode a valid cursor is replayed through
  the shard owner, so reconnecting to a different node does not re-send
  already acknowledged events.
- If a cursor is missing, invalid, or compacted out of the bounded replay log,
  the handler returns the current task snapshot and then resumes live updates.
- Configure ingress/proxies for SSE: disable response buffering, allow long
  request durations, and set idle timeouts above the heartbeat interval. The
  example emits heartbeat status events every 15 seconds while a stream is idle.
- Slow clients are disconnected rather than buffered indefinitely. Over-limit
  responses are protocol-shaped errors and should be retried with backoff.

## Future Extraction Map

Candidate future `rakka-a2a` modules:

- `handler`: durable `RequestHandler` implementation.
- `task_mapping`: A2A task/message ids to Rakka run/command ids.
- `projection`: durable A2A task and task-event read models.
- `agent_card`: dynamic card producer from workflow registrations.
- `streaming`: SSE snapshot/replay over durable task events.
- `push`: durable push config store and outbox-backed push dispatcher.

Example-owned pieces that should stay local:

- Demo workflow definition and skill labels.
- Local environment configuration.
- Local file discovery mode.
- Example file persistence mode.
- Route/debug helpers used only to prove example wiring.
