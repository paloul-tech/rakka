# Clustered Sharded Entity A2A Agents

Phase 0 runnable skeleton for exposing durable Rakka agent runs through the A2A
Rust SDK. This example is the incubator for a future `rakka-a2a` crate; no
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

## Phase 0 Boundary

Implemented:

- A real `ActorSystem`, `ClusterNodeRuntime`, `ClusterSharding`, demo
  `AgentWorkflow`, and sharded agent-run entity registration.
- Local file discovery for one or more developer-mode nodes.
- In-memory stores for local boot only.
- A static A2A agent card with REST/HTTP+JSON and JSON-RPC interfaces.
- A2A REST and JSON-RPC routers mounted beside Rakka health/cluster routes.
- A placeholder A2A `RequestHandler` that returns protocol-shaped
  unsupported-operation errors for command paths.

Not implemented in Phase 0:

- Durable A2A command acceptance.
- A2A task reads from durable projections.
- Streaming, push notification delivery, cancellation, or peer A2A calls.
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
- Route/debug helpers used only to prove Phase 0 wiring.
