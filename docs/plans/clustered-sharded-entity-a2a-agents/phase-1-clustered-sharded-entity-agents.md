# Phase 1 Clustered Sharded Entity A2A Agents

Status: implemented
Source spec: `docs/plans/clustered-sharded-entity-a2a-agents/spec.md`

## Goal

Define the durable A2A task read model and the conversion boundary between A2A
wire types and Rakka agent workflow domain types. This phase should not yet
accept public commands through the durable inbox; it prepares the data contract
that Phase 2 uses.

## Slices

### Slice 1.1: Identity And Metadata Normalization

Status: implemented

Work:

- Implement helpers that derive `AgentRunId` from A2A `Task.id` or
  `Message.task_id`.
- Generate a new task/run id when a new A2A message does not include a task id.
- Treat A2A `Message.message_id` as the default `AgentCommandId`.
- Define `io.rakka.*` metadata parsing for workflow id, workflow type,
  definition version, deduplication key, causation id, correlation id,
  principal ref, and trace fallback fields.
- Prefer W3C trace headers from `ServiceParams` over metadata fallback.
- Reject ambiguous requests where A2A first-class fields conflict with
  `io.rakka.*` metadata.

Acceptance:

- A new request always produces one canonical `AgentRunId`.
- A continuation request always targets the supplied `Message.task_id`.
- Duplicate metadata conflicts fail validation before durable acceptance.
- The canonical tenant source is explicit and test-covered.

### Slice 1.2: Message And Part Conversion

Status: implemented

Work:

- Convert A2A `Message` into an `AgentCommand` draft.
- Map text parts to small inline payloads or artifact references according to
  size policy.
- Map raw, URL, and data parts to `ArtifactRef` or application payload
  references.
- Preserve bounded message metadata needed for audit and task projection.
- Reject payloads that exceed inline limits without an artifact strategy.
- Keep application-specific part interpretation behind a trait or example-local
  function.

Acceptance:

- Text-only user messages convert into durable command drafts.
- Non-text parts can be represented without embedding unbounded data in hot
  state.
- Conversion tests cover missing task id, existing task id, multiple parts,
  metadata conflicts, and oversize payloads.

### Slice 1.3: Agent Command Construction

Status: implemented

Work:

- Build `AgentCommandMetadata` from normalized A2A inputs.
- Map a new A2A task to `AgentCommandKind::StartRun`.
- Map continuation messages to `AgentCommandKind::SubmitSignal` unless a more
  specific command type is supplied by metadata.
- Map A2A cancellation to `AgentCommandKind::CancelRun`.
- Use stable deduplication keys derived from message id, task id, tenant, and
  optional metadata.
- Attach principal and telemetry context.

Acceptance:

- Constructed commands pass `validate_command`.
- Deduplication keys are stable across client retry.
- Command construction does not require a live actor or cluster runtime.

### Slice 1.4: A2A Task Projection Model

Status: implemented

Work:

- Define a durable A2A task projection type.
- Include task id, context id, tenant, status, status timestamp, bounded
  history, bounded artifacts, metadata, and projection revision.
- Map Rakka run states to A2A `TaskState`.
- Store low-cardinality projection fields separately from large history or
  artifact records.
- Define projection compaction and redaction hooks.

Acceptance:

- Every `AgentRunStatus` maps to a valid A2A task status.
- Terminal states only project after durable Rakka state is terminal.
- Projection serialization round-trips.
- Large artifact payloads are referenced, not embedded.

### Slice 1.5: A2A Task Event Projection

Status: implemented

Work:

- Define a public task event shape for snapshot, status update, artifact
  update, message update, and terminal events.
- Assign monotonic per-task sequence numbers.
- Define replay cursors for `subscribe_to_task` and stream reconnect.
- Project from Rakka runtime events into public A2A task events.
- Keep internal runtime event details out of the public projection.

Acceptance:

- A task event can be replayed in order by task id and tenant.
- A current task snapshot can be reconstructed from projected events.
- Projection can distinguish public status updates from internal runtime
  bookkeeping.

### Slice 1.6: Query Index Integration

Status: implemented

Work:

- Decide where the example stores task projections in local mode.
- Integrate production projection with the existing agent workflow query index
  or a new example-local table.
- Support filtering by tenant, context id, status, and status timestamp.
- Support page size and page token.
- Keep `list_tasks` independent of live actor enumeration.

Acceptance:

- Projection reads work when no run actor is active.
- Pagination is deterministic.
- Tenant filters are mandatory for tenant-scoped deployments.

### Slice 1.7: Conversion Test Matrix

Status: implemented

Work:

- Add table-driven tests for identity, metadata, command construction, status
  mapping, artifact projection, history limits, and replay cursor generation.
- Include negative tests for metadata conflicts, invalid workflow selection,
  forbidden hot labels, and oversized inline payloads.
- Add compatibility fixtures with representative A2A JSON payloads.

Acceptance:

- Tests exercise conversion without starting a cluster.
- Fixtures are small enough to review.
- Failures return stable validation codes or messages.

## Exit Criteria

- A2A wire inputs can be normalized into Rakka command drafts.
- Rakka run state can be projected into A2A tasks.
- A public A2A task event projection is specified and test-covered.
- No public command is acknowledged before Phase 2 durable acceptance exists.

## Implementation Notes

- `examples/clustered-sharded-entity-a2a-agents/src/a2a_mapping.rs` owns
  identity normalization, `io.rakka.*` metadata parsing, tenant-source
  selection, trace extraction, part conversion, and `AgentCommand` draft
  construction. Command drafts are built through `AgentTriggerCommandBuilder`
  with the `api` trigger source, so A2A commands carry the same normalized
  trigger-source attributes as other trigger paths.
- `examples/clustered-sharded-entity-a2a-agents/src/task_projection.rs` owns
  the A2A task projection, public task events, runtime-event projection mapping,
  replay cursors, and the local in-memory projection store.
- The `Phase1A2AHandler` validates `send_message`,
  `send_streaming_message`, and `cancel_task` into command drafts before
  returning unsupported-operation errors. Durable inbox acceptance remains a
  Phase 2 boundary.
- `get_task` and `list_tasks` are projection-backed and do not enumerate live
  actors. Local mode uses the in-memory projection store; the module also
  exposes tenant-scoped behavior that requires tenant filters. Read paths
  resolve their tenant scope through the same header-first precedence and
  conflict policy as command paths.
- Oversized message parts convert to artifact drafts that pair each reference
  with its source content. Phase 2 must persist that content behind the
  synthetic `a2a-message://` URIs (computing real checksums at that point)
  before accepting the command durably; only the reference may reach durable
  state.
- Conversion fixtures live under
  `examples/clustered-sharded-entity-a2a-agents/tests/fixtures/`.

## References

- `docs/plans/clustered-sharded-entity-a2a-agents/spec.md`
- `crates/rakka-agent-workflow/src/domain.rs`
- `crates/rakka-agent-workflow/src/facade.rs`
- `crates/rakka-agent-workflow/src/artifacts.rs`
- `crates/rakka-agent-workflow/src/runtime_events.rs`
- `crates/rakka-agent-workflow/src/query.rs`
- `crates/rakka-agent-workflow/src/postgres_query.rs`
- `crates/rakka-agent-workflow/src/trace_context.rs`
- `https://github.com/a2aproject/a2a-rs/blob/main/a2a/src/types.rs`
- `https://github.com/a2aproject/a2a-rs/blob/main/a2a/src/agent_card.rs`
