# Clustered Sharded Entity A2A Agents

Phase 2 runnable example for exposing durable local Rakka agent runs through the
A2A Rust SDK. This example is the incubator for a future `rakka-a2a` crate; no
reusable public Rakka A2A API is introduced here.

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

Run two local nodes with shared file discovery:

```sh
RAKKA_DISCOVERY_DIR=/tmp/rakka-a2a-discovery RAKKA_PORT=25580 RAKKA_HTTP_PORT=35580 \
  cargo run -p rakka-example-clustered-sharded-entity-a2a-agents

RAKKA_DISCOVERY_DIR=/tmp/rakka-a2a-discovery RAKKA_PORT=25581 RAKKA_HTTP_PORT=35581 \
  cargo run -p rakka-example-clustered-sharded-entity-a2a-agents
```

Set `RAKKA_A2A_PUBLIC_URL=https://example.com/agents/demo` when the agent card
should advertise a load-balanced public URL. Without it, developer mode
advertises the local HTTP address.

## Phase 2 Boundary

Implemented:

- A real `ActorSystem`, `ClusterNodeRuntime`, `ClusterSharding`, demo
  `AgentWorkflow`, and sharded agent-run entity registration.
- Local file discovery for one or more developer-mode nodes.
- Shared in-memory durable stores for local command acceptance, run state, and
  task projection recovery.
- A static A2A agent card with REST/HTTP+JSON and JSON-RPC interfaces.
- A2A REST and JSON-RPC routers mounted beside Rakka health/cluster routes.
- A2A identity and `io.rakka.*` metadata normalization into Rakka command
  drafts.
- A2A message/part conversion into bounded inline payloads or artifact
  references.
- Durable `AgentRunInbox` acceptance for `message:send` and `cancel_task`.
- Idempotent duplicate handling for repeated A2A `messageId`/deduplication keys.
- Local `AgentRunState` creation for new tasks and cancellation requests for
  active tasks.
- A task projection model, public task-event model, replay cursors, and a local
  in-memory task projection store.
- Projection-backed `send_message`, `get_task`, `list_tasks`, and `cancel_task`.

Not implemented in Phase 2:

- Cluster-routed A2A command delivery to remote shard owners.
- Streaming, push notification delivery, terminal execution, or peer A2A calls.
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
- In-memory persistence mode.
- Route/debug helpers used only to prove example wiring.
