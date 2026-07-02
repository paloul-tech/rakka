# Phase 0 Clustered Sharded Entity A2A Agents

Status: planning draft
Source spec: `docs/plans/clustered-sharded-entity-a2a-agents/spec.md`

## Goal

Lock the first implementation boundary and prove the A2A Rust SDK can be
mounted beside a Rakka runtime without changing Rakka's actor, sharding,
remoting, or durable workflow semantics.

This phase should produce a runnable example skeleton, not a reusable public
crate. The implementation should make later extraction into `rakka-a2a`
straightforward by keeping A2A-specific code in clear modules.

## Slices

### Slice 0.1: Dependency And Feature Spike

Status: planned

Work:

- Add the A2A SDK dependencies to the selected example crate only.
- Use the current SDK package names and Rust library imports consistently.
- Keep A2A support out of the top-level `rakka` facade in this phase.
- Record the chosen SDK version, crate names, and feature flags in the example
  README or phase notes.
- Verify the dependency tree does not force TLS, gRPC, or SLIMRPC unless the
  example explicitly enables them.

Acceptance:

- `cargo check` succeeds for the new example target.
- The example can import `a2a`, `a2a_server`, and the selected A2A router
  modules.
- No reusable Rakka public API is committed before the example proves the
  shape.

### Slice 0.2: Example Skeleton And Module Boundary

Status: planned

Work:

- Create a new runnable example modeled after
  `clustered-agent-workflow-http-grpc`.
- Split modules by concern: configuration, server boot, A2A handler, agent
  card, task projection, sharded run entity, durable stores, codec, discovery,
  and support.
- Keep example-local A2A adapter code separable from example-local demo
  workflow code.
- Add a minimal README with local run commands and a note that the example is
  the incubator for a future `rakka-a2a` crate.

Acceptance:

- The example starts one node with no A2A behavior beyond health and agent-card
  serving.
- The module structure makes it obvious what can later move into a library
  crate.

### Slice 0.3: Runtime Boot Without Public A2A Commands

Status: planned

Work:

- Boot `ActorSystem`, `ClusterNodeRuntime`, `ClusterSharding`, and the demo
  `AgentWorkflow`.
- Reuse the local file discovery pattern for developer mode.
- Reuse file or in-memory durable stores for local mode.
- Register remoting codecs required by any example-local inter-node payloads.
- Leave durable command acceptance unimplemented until Phase 2.

Acceptance:

- One node starts and shuts down cleanly.
- Two nodes can discover each other in local file discovery mode.
- No public A2A request can mutate durable state yet.

### Slice 0.4: Agent Card Router

Status: planned

Work:

- Build a static or lightly dynamic `AgentCardProducer`.
- Publish only the load-balanced public URL when configured, and local URLs in
  developer mode.
- Advertise REST/HTTP+JSON and JSON-RPC as planned first transports.
- Set `streaming` according to implemented behavior. In Phase 0 this should be
  false unless the handler can serve a stream endpoint safely.
- Add one demo `AgentSkill` derived from the demo workflow.

Acceptance:

- `GET /.well-known/agent-card.json` returns a valid A2A `AgentCard`.
- The card does not advertise transports or capabilities that are not wired.
- The card can be served through the same HTTP server as Rakka health routes.

### Slice 0.5: Router Composition Spike

Status: planned

Work:

- Mount A2A agent-card, REST, and JSON-RPC routers in one axum/Rakka HTTP
  server.
- Add a placeholder `RequestHandler` that returns protocol-valid
  not-implemented errors.
- Verify route paths do not collide with health, cluster, or operational
  snapshot routes.
- Confirm request headers are visible through A2A `ServiceParams`.

Acceptance:

- The HTTP server can serve health, agent card, and A2A routes together.
- Placeholder A2A calls fail predictably with protocol-shaped errors.
- Header propagation can be observed in a unit or integration test.

### Slice 0.6: API Boundary Note

Status: planned

Work:

- Write a short note in the example README or this plan directory that lists
  candidate future `rakka-a2a` modules.
- Identify what remains example-owned: demo workflow, local configuration,
  local persistence mode, and local discovery mode.
- Identify what can graduate later: task mapping, durable request handler,
  task projection, push config store, stream projection, and agent card
  producer.

Acceptance:

- Reviewers can tell which code is product API exploration and which code is
  demo scaffolding.

## Exit Criteria

- A runnable example skeleton exists.
- A valid A2A agent card is served.
- A2A routers can coexist with Rakka HTTP routes.
- No durable mutation path exists before Phase 2.
- The future extraction boundary is documented.

## References

- `docs/plans/clustered-sharded-entity-a2a-agents/spec.md`
- `examples/clustered-agent-workflow-http-grpc/src/server.rs`
- `examples/clustered-agent-workflow-http-grpc/src/config.rs`
- `examples/clustered-agent-workflow-http-grpc/src/codec.rs`
- `crates/rakka-http/src/server.rs`
- `crates/rakka-http/src/routes.rs`
- `https://github.com/a2aproject/a2a-rs/blob/main/a2a-server/src/agent_card.rs`
- `https://github.com/a2aproject/a2a-rs/blob/main/a2a-server/src/rest.rs`
- `https://github.com/a2aproject/a2a-rs/blob/main/a2a-server/src/jsonrpc.rs`
