# Clustered Sharded Entity A2A Agents

Phase 3 runnable example for exposing durable Rakka agent runs through the A2A
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

## Phase 3 Boundary

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

Not implemented in Phase 3:

- Streaming, push notification delivery, step execution to completion, or peer
  A2A calls.
- Production PostgreSQL persistence in this example. Shared file state is for
  local multi-process demos only.
- A reusable `rakka-a2a` crate or top-level `rakka` facade feature.

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
