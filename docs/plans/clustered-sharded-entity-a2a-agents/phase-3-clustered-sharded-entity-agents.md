# Phase 3 Clustered Sharded Entity A2A Agents

Status: planning draft
Source spec: `docs/plans/clustered-sharded-entity-a2a-agents/spec.md`

## Goal

Make the durable A2A handler work across a cluster. Any public node should be
able to accept A2A traffic, resolve the owning sharded run entity, and route
owner-only commands through Rakka remoting.

## Slices

### Slice 3.1: Remote-Safe Run Protocol

Status: planned

Work:

- Define `A2ARunRequest` and `A2ARunResponse` as serializable inter-node
  payloads.
- Include variants for accept message, query task snapshot, cancel task, open
  stream cursor, record push config, and delete push config.
- Keep process-local types such as `ReplyTo`, `Arc`, actor refs, and store
  handles out of the remote protocol.
- Include task id, tenant, command metadata, projection hints, and timeout
  policy in request payloads where needed.
- Version the remote protocol payloads.

Acceptance:

- Payloads serialize and deserialize through the selected Rakka remote codec.
- The remote protocol can evolve without changing A2A wire types.
- No `AgentRunActorCommand` is serialized directly over the wire.

### Slice 3.2: Codec And Registry Wiring

Status: planned

Work:

- Register codecs for `A2ARunRequest` and `A2ARunResponse` in
  `SerializationRegistry`.
- Reuse the example JSON codec initially, unless protobuf contracts are needed
  for compatibility.
- Add tests for unknown payload type, schema version mismatch, and decode
  failure.
- Ensure the same registry is used by every node.

Acceptance:

- Two local nodes can exchange the remote-safe A2A run payloads.
- Decode failures map to A2A unavailable or internal errors without panics.

### Slice 3.3: Clustered Run Entity

Status: planned

Work:

- Implement a sharded `A2ARunEntity`.
- Spawn or host a local `AgentRunActor` for the owning run id.
- Map `A2ARunRequest` variants to local `AgentRunActorCommand` values and
  projection operations.
- Keep the entity as a routing shell; durable stores remain the source of
  truth.
- Stop local child actors when the entity stops or passivates.

Acceptance:

- First reference to a task id starts the entity on the owner node.
- Owner-local commands are serialized through the run actor.
- Stopping an entity does not lose accepted work.

### Slice 3.4: Cluster Routing Helper

Status: planned

Work:

- Implement a helper that resolves `Task.id` to `ShardedEntityRef`.
- Determine local vs remote ownership from the sharding region.
- Use local `ask` for owner-local commands.
- Use `remote_ask` for non-owner commands.
- Record peer reachability outcomes for self-fencing input.
- Map `EntityAskError` and `RemoteEntityAskError` to A2A errors.

Acceptance:

- Any node can handle `send_message`, `get_task`, and `cancel_task` for any
  task id.
- Remote timeouts are surfaced as retryable/unavailable A2A errors.
- Reachability recording ignores validation and codec errors.

### Slice 3.5: Cluster Boot Modes

Status: planned

Work:

- Keep single-node local mode as the default.
- Add multi-node file discovery mode for local testing.
- Add etcd or equivalent external discovery mode for production-like testing.
- Keep Rakka remoting private to node-to-node communication.
- Document the load-balanced public endpoint separately from remoting address.

Acceptance:

- One node runs without discovery configuration.
- Two or more local nodes can route tasks by id.
- Public A2A URLs do not expose Rakka remoting addresses.

### Slice 3.6: Owner Movement And Recovery Tests

Status: planned

Work:

- Test task acceptance on one node and read/cancel from another.
- Test owner node shutdown and recovery on a new owner after membership update.
- Test shard handoff while a command is in flight.
- Test passivation and lazy recovery on next task reference.
- Test duplicate message retry after owner movement.

Acceptance:

- Only one owner drives a run at a time under healthy membership.
- Durable state recovers after owner movement.
- Client retries remain idempotent across owner movement.

### Slice 3.7: Cluster Documentation

Status: planned

Work:

- Document local multi-node run commands.
- Document public A2A endpoint versus private Rakka remoting endpoint.
- Document the expected detection window for failed owners.
- Document load balancer behavior at a high level.

Acceptance:

- A reviewer can run a two-node cluster and send A2A requests to either node.

## Exit Criteria

- Any node can accept A2A requests for any task id.
- The owner-only run actor remains the only writer for run execution.
- Durable recovery works after owner movement.
- The remote protocol is separate from A2A public wire types.

## References

- `docs/plans/clustered-sharded-entity-a2a-agents/spec.md`
- `examples/clustered-agent-workflow-http-grpc/src/ingress.rs`
- `examples/clustered-agent-workflow-http-grpc/src/run_entity.rs`
- `examples/clustered-agent-workflow-http-grpc/src/server.rs`
- `examples/clustered-counter-http/src/api.rs`
- `examples/clustered-counter-http/src/counter.rs`
- `crates/rakka-sharding/src/facade.rs`
- `crates/rakka-sharding/src/node_runtime.rs`
- `crates/rakka-remote/src/registry.rs`
- `crates/rakka-remote/src/request.rs`
