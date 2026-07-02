# Phase 2 Clustered Sharded Entity A2A Agents

Status: planning draft
Source spec: `docs/plans/clustered-sharded-entity-a2a-agents/spec.md`

## Goal

Implement the durable A2A request handler for single-node operation. This phase
establishes the most important correctness rule: public A2A work is
acknowledged only after durable inbox acceptance succeeds.

## Slices

### Slice 2.1: Handler State And Error Model

Status: planned

Work:

- Implement `RakkaA2ARequestHandler` over A2A `RequestHandler`.
- Store workflow registry, task projection reader/writer, durable stores,
  clock, metrics recorder, and optional sharding handle in handler state.
- Define adapter-local error types and map them to `A2AError`.
- Preserve protocol-specific details such as `ServiceParams`, tenant, and
  request metadata.
- Add helpers for validation, not-found, unavailable, conflict, duplicate, and
  internal errors.

Acceptance:

- The handler compiles without cluster routing.
- Error mapping is deterministic and covered by unit tests.
- Handler construction does not require networked remoting.

### Slice 2.2: `send_message` Durable Acceptance

Status: planned

Work:

- Normalize identity and metadata using Phase 1 helpers.
- Build an `AgentCommand`.
- Accept the command through `AgentRunInbox`.
- For a new run, persist initial `AgentRunState` with `AgentStepRunner` or
  `AgentRunActorCommand::Start` in local actor mode.
- Write or update the A2A task projection.
- Honor `return_immediately` by returning after durable acceptance and initial
  projection.
- For non-immediate requests, wait only for a bounded first transition or a
  terminal result.

Acceptance:

- A crash after durable acceptance but before response can be retried without a
  duplicate run.
- A duplicate `Message.message_id` returns the existing task projection.
- New and continuation messages use the same canonical task id behavior.

### Slice 2.3: `get_task` From Durable Projection

Status: planned

Work:

- Implement `get_task` by reading the durable A2A task projection.
- Apply tenant authorization before returning task data.
- Support `history_length`.
- Support artifact inclusion policy if the SDK request exposes it through the
  selected binding.
- Return task-not-found without probing live actors.

Acceptance:

- `get_task` works after handler restart.
- `history_length` truncates history deterministically.
- Tenant mismatch returns an authorization or not-found response according to
  configured policy.

### Slice 2.4: `list_tasks` From Durable Projection

Status: planned

Work:

- Implement filtering by tenant, context id, status, and status timestamp.
- Implement stable page tokens.
- Return bounded task histories and artifacts.
- Avoid scanning live actors or shard state.
- Add tests for empty pages, final pages, invalid page tokens, and status
  filters.

Acceptance:

- `list_tasks` works with no active run actors.
- Pagination is deterministic across handler restart.
- Query filters match the A2A request contract.

### Slice 2.5: `cancel_task` Durable Command

Status: planned

Work:

- Normalize cancellation metadata.
- Build `AgentCommandKind::CancelRun`.
- Durably accept the cancellation command.
- Persist `RequestCancellation` locally when the run is active.
- Return the latest task projection.
- Make cancellation idempotent for terminal and already-cancelling runs.

Acceptance:

- Cancelling a running task moves it toward cancellation durably.
- Cancelling an already terminal task returns the terminal projection without a
  duplicate transition.
- Retry of the same cancellation is deduplicated.

### Slice 2.6: Projection Writes And Runtime Events

Status: planned

Work:

- Write task projection updates after command acceptance, run start, status
  transition, cancellation request, and terminal transition.
- Emit A2A task events for each public projection change.
- Ensure projection writes are idempotent by task id and sequence or revision.
- Add a recovery pass that can rebuild projection records from durable Rakka
  state for local mode.

Acceptance:

- Projection can recover after process restart.
- Public task events do not expose internal-only runtime details.
- Projection writes can be replayed without duplicating public events.

### Slice 2.7: A2A CLI Smoke Tests

Status: planned

Work:

- Add a single-node smoke path using `a2acli` or equivalent SDK client calls.
- Exercise agent card, `send_message`, `get_task`, `list_tasks`, and
  `cancel_task`.
- Include duplicate retry scenarios.
- Keep the smoke test optional if it requires an external binary.

Acceptance:

- The example can be driven by an A2A client against one node.
- The documented commands produce stable task ids and status transitions.

## Exit Criteria

- A single node accepts A2A messages durably.
- Task read/list/cancel work from durable projections.
- Duplicate client retries do not create duplicate runs.
- No cluster routing is required for correctness in this phase.

## References

- `docs/plans/clustered-sharded-entity-a2a-agents/spec.md`
- `crates/rakka-agent-workflow/src/inbox.rs`
- `crates/rakka-agent-workflow/src/runner.rs`
- `crates/rakka-agent-workflow/src/runtime.rs`
- `crates/rakka-agent-workflow/src/query.rs`
- `crates/rakka-workflow/src/inbox.rs`
- `https://github.com/a2aproject/a2a-rs/blob/main/a2a-server/src/handler.rs`
- `https://github.com/a2aproject/a2a-rs/blob/main/a2a-server/src/rest.rs`
- `https://github.com/a2aproject/a2a-rs/blob/main/a2a/src/types.rs`
