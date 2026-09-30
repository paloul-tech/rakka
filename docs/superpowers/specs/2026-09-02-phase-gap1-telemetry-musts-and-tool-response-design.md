# Phase gap 1 — the two telemetry MUSTs and the `ToolResponse` guardrail point

Status: design, 2026-09-02. Branch `rakka-agents-phase-gap1`.

Closes three items the Phase 6 matrices record as owed:

- spec 17.11 — the resolution/resume span MUST link the parked span and the
  incoming request span;
- spec 17.9 — the indeterminate transition MUST link the ambiguous dispatch
  attempt and the later reconciliation decision;
- the `ToolResponse` guardrail evaluation point (security matrix, "Owed").

## The obstacle both MUSTs share

Every exported span id is derived at export time — `segment_span` derives it
from the record's fields and `AgentGenAiSpanExporter` re-derives it with its
emission ordinal. No component can therefore name a span it did not export
itself, and a link needs a name. Both MUSTs are link requirements between
spans closed by *different* components (the run entity parks; the dispatcher
attempts; a human resolves), often on different nodes.

## Decision 1 — durable span identity, derived from durable facts

A segment may carry an explicit `span_id`. The id is derived, not drawn:
`agent_derived_span_id(trace_id, parent_span_id, material)` over the trace
context a durable record already holds plus the record's durable identity.
Two components holding the same record derive the same id without
communicating, which is exactly what a cross-component link needs.

- `AgentTelemetrySegment::span_id: Option<String>` (builder `.span_id()`).
- `observability::agent_durable_span_identity(context, material) ->
  Option<AgentTraceContext>`: a child context whose span id is the derived
  one; usable as a link (`to_span_link`) or as a stored context
  (`to_telemetry_context`). `None` when the context has no trace parent —
  telemetry never fails a transition.
- `segment_span` uses an explicit id verbatim and does not re-derive; the
  exporter skips its ordinal re-derivation for such a segment. A durable
  identity is by definition already distinguished from its siblings.
- Derivation material, all relative to the **effect's** context
  (`AgentRunEffect::telemetry`, which the dispatcher receives verbatim as the
  intent):
  - `checkpoint-open`: `["checkpoint-open", checkpoint_id]`
  - `checkpoint-resolve`: `["checkpoint-resolve", checkpoint_id]`
  - dispatch attempt: `["effect-dispatch", effect_id, generation, attempt]`
- Link kinds (`rakka.agent.link.kind`), catalogued as data next to
  `superseded-generation` and held by a source-scan test both ways:
  `parked-checkpoint`, `resume-request`, `ambiguous-attempt`,
  `reconciliation-decision`.

## Decision 2 — 17.11: the checkpoint record stores the parked span

- `open_effect_checkpoint` stores the parked identity's context on
  `AgentCheckpoint::telemetry` (the field's doc already claims this role).
  Old records decode to the run context, as today.
- The `checkpoint-open` segment closes with `.span_id(parked)`. Opened
  checkpoints are closed at two sites: `advance` (approval family, as today)
  and the `RecordEffectResult` command (the reconciliation park, which closed
  no segment before).
- New segment class `checkpoint-resolve` (`rakka.agent.checkpoint.resolve`,
  INTERNAL, attribute `rakka.agent.checkpoint.kind`). Closed by
  `ResolveCheckpoint` and `ResolveIndeterminateEffect` when the transition
  committed and the checkpoint was open before it. Its own id is the
  pre-derived resolve identity; its links: `parked-checkpoint` → the stored
  checkpoint context, `resume-request` → the command's context.
- `ResolveCheckpoint` and `ResolveIndeterminateEffect` gain
  `#[serde(default)] telemetry: AgentTelemetryContext` — the incoming request
  span. Commands are JSON-encoded, so absent decodes to the empty context.
- The `run-resume` segment closed in the same `apply` carries the same links.

## Decision 3 — 17.9: two segments carry the links, the decision is pre-named

- The dispatcher's attempt segment closes with `.span_id(attempt identity)`.
- The dispatcher's indeterminate park segment links `ambiguous-attempt` → the
  attempt identity and `reconciliation-decision` → the resolve identity of
  the reconciliation checkpoint, whose id is a pure function of effect id and
  generation (`{effect}#ck-reconcile-g{n}`; the derivation moves to
  `checkpoints.rs` so both sides call one function).
- The run's reconciliation `checkpoint-open` segment (new close site) closes
  with error status and `rakka.agent.effect.status=indeterminate`, carrying
  the same two links. Retention selects it by checkpoint kind and by effect
  status, as the catalogue already documents.

## Decision 4 — `ToolResponse` evaluates in the dispatcher, before delivery

- `AgentToolAuthority::review_tool_response(scope, intent, call, content)`
  evaluates the configured chain at `ToolResponse` with the tool in context
  and the result content as the value (inline value, or the artifact
  reference for artifact content — the memory-ingress precedent), bounded at
  `AGENT_TOOL_RESULT_MAX_BYTES`.
- Dispositions: `Allowed` passes; `Transform` on inline content replaces the
  delivered content (refused `guardrail-transform-invalid` if it does not
  form a bounded inline result; `guardrail-transform-unsupported` on artifact
  content); `Blocked` → `guardrail-blocked`; `CheckpointRequired` →
  `checkpoint-required`, fail closed — no checkpoint can gate a response that
  already exists.
- Called inside `AgentAgentDispatcher::invoke` for `Tool` and `Compensation`
  outcomes, after execution and before the outcome is constructed. A refusal
  becomes `AgentRunEffectOutcome::Failed { code }` — a determinate failure of
  an effect that did run. It is delivered once, the effect fails, and the run
  winds down under the stable code, exactly as a blocked request does. A
  transformed result is what is delivered, so a redelivery carries the same
  content and a retry never re-evaluates. Nothing blocked reaches the run,
  session memory, or a later context snapshot.
- `ToolResponse` joins both evaluated-boundary constants (4 of 7 at the
  authority; 4 of 7 overall become 5 once attested). The existing "unevaluated
  boundary fails closed" test moves to `ModelResponse`.

## Decision 5 — what stays owed

Wait segments for timer/child waits (no parked span exists), token usage on
the chat span, the decision vocabulary, and the other three guardrail points.
The matrices are updated to say so.

## Testing

- Unit: derived identity is stable and context-relative; explicit id survives
  export and the exporter's ordinal; link-kind vocabulary bijection.
- Runtime: scenario 22 re-pointed at runtime-produced segments — the
  `checkpoint-open` segment's id equals the stored checkpoint context's span,
  and the `checkpoint-resolve` segment links it and the request span.
- Runtime: an ambiguous non-idempotent loss — the park segment's
  `ambiguous-attempt` link equals the attempt segment's id; the
  `reconciliation-decision` link equals the later `checkpoint-resolve` id.
- Guardrail: coverage satisfied by a `ToolResponse`-only mandatory stage;
  block → one invocation, terminal `guardrail-blocked`, no tool-result session
  entry; transform → the recorded result is the transformed content;
  checkpoint-required → fail closed; chain-consistency arithmetic holds.
- `scripts/validate.sh` green; doc-currency tests updated with the docs.
