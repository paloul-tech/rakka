# Dispatch-path outcomes — definitive refusals and unknown MCP outcomes

Status: implemented, 2026-10-11. Branch `dispatch-path-outcomes` off `e2e7a29`.
Brief: issue #85 (rakka.host A16), asks 7, 14, and 15, drafted against
`96a18b0`; the code was read at `e2e7a29`.

Three gaps on the dispatch path, each with a host workaround in place:

- **Ask 14.** A collaborator cannot say a failure is definitive.
  `AgentDispatchError::Collaborator` is always retried until the entry's
  retry policy is spent, so an MCP egress refusal the host declared
  `retryable: false` is retried up to `maxAttempts` times, and its reason
  reaches the run only after the last attempt.
- **Ask 15.** An artifact store cannot say a refusal is definitive. The MCP
  executor maps every `AgentArtifactError` to a retryable collaborator
  failure, so a store that will never accept a result (too large) has the
  remote tool **invoked again** on each attempt.
- **Ask 7.** The MCP executor maps every elapsed deadline, and every
  transport or protocol failure, to `Invocation { mcp-transport-failed }`, a
  retryable failure, whether or not `tools/call` was already sent. A
  non-idempotent effect has one attempt, so it exhausts and the run reads
  "failed" when the server may have acted. Specification 11.5 requires an
  `Indeterminate` outcome once invocation may have occurred.

## Decision 1 — two new `AgentDispatchError` variants

```rust
/// A collaborator refused, and retrying cannot change its answer.
Definitive { code: String, message: String },
/// The attempt may have invoked the target and its outcome is unknown.
OutcomeUnknown { code: &'static str, message: String },
```

with constructors `AgentDispatchError::definitive(code, message)` and
`AgentDispatchError::outcome_unknown(code, message)`. `AgentDispatchError` is
`#[non_exhaustive]`, so both are additive. Any executor of any effect kind
may return them, and the dispatcher honors them where an invocation's result
is decided (Decisions 2 and 4); the MCP executor is the first to return
them.

- `Definitive::code()` is `dispatch-collaborator-failed`, the same code
  `Collaborator` answers. The collaborator's own code rides the bounded
  detail and the failure reason exactly as a `Collaborator` code does today
  (`collaborator_code` and `collaborator_reason` treat both variants alike).
  So no recorded code changes meaning: a definitive refusal ends in the
  state a retryable one reaches once its budget is spent, only sooner.
- `OutcomeUnknown::code()` is its own code, as `Invocation`'s is.
- Its `Display` text becomes durable state under the same contract every
  `AgentDispatchError` text already carries: no credential, argument, or
  content material.

**Rejected.** A richer return type on `AgentDispatchToolExecutor::execute`
(an outcome enum, like the goal-evaluation executor's finding) changes a
trait every executor implements, the host's included. Flags on the existing
`Collaborator` and `Invocation` variants break every pattern and literal that
names their fields.

## Decision 2 — a definitive failure exhausts its entry on that attempt

When an invocation answers `Definitive`, `record_attempt_failure` records the
attempt through a new `InboxWrite::Exhausted { line }`, which goes through
`write_outbox` (and so through issue #81's in-pass retry) to the substrate's
new `record_outbox_exhausted` (Decision 3). The substrate answers
`OutboxDispatchExhausted`, and the existing branch of `record_attempt_failure`
does the rest unchanged:

- the fleet index entry is marked `Exhausted` with the bounded line
  (`record_claim_failure`'s existing arm);
- the run receives `AgentRunEffectOutcome::Exhausted` with code
  `dispatch-collaborator-failed`, the bounded detail, and the collaborator's
  code as its failure reason.

That is the end state a `Collaborator` failure reaches after `maxAttempts`
attempts today; the outbox row's attempt count is this attempt's. Nothing
else invokes the target again.

**Where it is honored:** the invocation result path, `attempt_invocation`'s
error arm, which serves every effect kind's executor. The credential
resolver's failure path maps any resolver error to
`credential-resolution-failed` and is unchanged; a definitive resolver
refusal is out of scope.

## Decision 3 — the substrate's exhausting write

`rakka-workflow` gains

```rust
pub async fn record_outbox_exhausted(
    &mut self,
    message_id: &OutboxMessageId,
    message: impl Into<String>,
) -> WorkflowResult<WorkflowTelemetryEvent>
```

on `DurableInbox`, backed by an `OutboxEntry` transition that records the
failure with `RetryAttempt::record_exhaustion` and sets
`OutboxStatus::Exhausted` whatever budget remains. It mirrors
`record_outbox_failure` in every other respect — the same recovered-record
compare-and-set, the same `OutboxEntryNotFound` refusal — and answers
`OutboxDispatchExhausted { attempts, message, .. }`. Additive.

## Decision 4 — an unknown outcome follows specification 11.5's table

When an invocation answers `OutcomeUnknown`, the dispatcher applies the same
by-safety-class rule it applies to an attempt whose worker died after
`Started` (`recover_ambiguous`), with the executor's code and message in
place of `dispatcher-lost-after-started`:

| Safety class | What the dispatcher does |
| --- | --- |
| `ReadOnly`, `Idempotent` | Records a failed attempt, as an `Invocation` failure is recorded today: retried after backoff under the budget, with the same external idempotency key. Only the code differs. |
| `Reconcileable` | Queries the reconciler now (`reconcile_ambiguous`). With no protocol or no reconciler configured, that path already parks the run. |
| `NonIdempotent` | `park_indeterminate`: one durable `Indeterminate` with the executor's code and message, the run in `WaitingForReconciliation` with a reconciliation checkpoint, and no automatic re-invocation. |

`attempt_invocation` runs only for a run that is not winding down (or for the
kinds exempt from the fence), so the worker-loss path's wind-down arm does not
apply here.

## Decision 5 — `AgentArtifactError::Refused`, and `#[non_exhaustive]`

```rust
/// The store refused the write, and will refuse it again: retrying cannot
/// help. A store that may accept the same write later answers `Store`.
Refused { message: String },
```

with code `artifact-store-refused`. The enum gains `#[non_exhaustive]` in the
same change.

**This is a breaking change** for any exhaustive `match` on
`AgentArtifactError` outside the crate: a new variant breaks one already, and
`#[non_exhaustive]` makes it the last such break. Nothing in this workspace
outside the defining crate matches the enum exhaustively, and the host only
constructs `Store`. The compatibility document records it as source-breaking.

## Decision 6 — the MCP executor's mappings

- **Egress (ask 14).** `dispatch_error` maps
  `McpClientError::Egress { retryable: false, .. }` to
  `AgentDispatchError::definitive(code, message)`, under the host's own code.
  `retryable: true` stays a `Collaborator`. The message text is unchanged
  (ask 13 is out of scope).
- **Artifact sink (ask 15).** A sink answering `AgentArtifactError::Refused`
  maps to `definitive("artifact-store-refused", ..)`. Every other sink
  error stays a `Collaborator`, as today.
- **Two windows (ask 7).** The attempt keeps one deadline, spent across two
  windows split at the send:
  - **before the send** — the handshake (`open`) and the descriptor recheck's
    listing. An elapsed deadline, or a transport or protocol failure, stays
    `Invocation { mcp-transport-failed }`: the call was never sent, so the
    attempt is retryable;
  - **from the send** — `call_tool_once` and the mapping of its answer. An
    elapsed deadline, and every `ServiceError` from `call_tool_once` except
    `ServiceError::McpError`, maps to
    `AgentDispatchError::outcome_unknown("mcp-outcome-unknown", ..)`, a new
    stable MCP code. That covers `TransportSend`, `TransportClosed`,
    `UnexpectedResponse`, `Cancelled`, `Timeout`, `SubscriptionLagged`,
    `InputRequiredRoundsExceeded`, and any variant rmcp adds.
  - **`McpError` stays a known outcome.** It is a JSON-RPC error the server
    answered with, so the server reported what it did; it keeps today's
    mapping. A tool result the server marked `isError` is likewise a known
    outcome and is unchanged.

  The message names the window and, for a service error, rmcp's variant name
  (as `service_error` does today) — never server-chosen text.

## What this does not change

- Ask 6 (cancelling an in-flight call), ask 13 (the egress message naming
  the server), and every other ask of #85.
- No other MCP refusal code becomes definitive (`mcp-tool-unbound`,
  `tool-descriptor-revision-mismatch`, and the rest stay retryable); any of
  them, or a credential resolver, can adopt `Definitive` later.
- No schema version, no durable field, no existing code's meaning.

## Proof

Each test is written to fail first against `e2e7a29`.

- **Dispatcher** (`rakka-agent`, a scripted tool executor through the real
  pipeline):
  - `Definitive` under a budget of three: one invocation, the outbox row and
    fleet entry `Exhausted` after the first attempt, the run's outcome
    `Exhausted` with the collaborator's code as its reason.
  - `OutcomeUnknown` once per safety class: `NonIdempotent` parks
    `WaitingForReconciliation` with the executor's code and no second
    invocation; `Idempotent` and `ReadOnly` record a retryable failure and
    re-invoke on the next claim; `Reconcileable` asks the reconciler in the
    same pass.
- **Substrate** (`rakka-workflow`): `record_outbox_exhausted` exhausts an
  entry with budget left, and refuses an unknown message id.
- **MCP executor** (`rakka-agent-mcp`, against the testkit's fake server):
  - an egress refusal with `retryable: false` exhausts on the first attempt
    under a multi-attempt budget and opens no connection;
  - an artifact sink answering `Refused` invokes the remote tool exactly once
    across the passes that would otherwise retry it;
  - a `tools/call` answer slower than the deadline parks a non-idempotent run
    `WaitingForReconciliation` under `mcp-outcome-unknown`;
  - a `tools/call` the server fails at the transport does the same;
  - a JSON-RPC error answer, and a slow listing (before the send), still
    record a retryable failure.

  The slow call uses the fake server's existing `FakeToolBehaviour::Sleep`;
  the fake gains a transport failure after the call runs
  (`with_call_transport_failure`) and a JSON-RPC error answer
  (`FakeToolBehaviour::RpcError`).

Documentation: specification 11.5 (an executor-reported unknown outcome
follows the table), `docs/rakka-compatibility.md` (the two variants, the
substrate method, the artifact variant and its `#[non_exhaustive]` break,
`mcp-outcome-unknown`, and the behaviour change for non-idempotent MCP
tools), the MCP section of `docs/rakka-agents.md`, `CHANGELOG.md`, and a
status comment on #85.
