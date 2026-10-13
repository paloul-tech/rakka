# Unknown-outcome evidence: what a parked MCP effect knows about its call

Status: design approved in conversation, 2026-10-13. Branch `run-evidence-unknown-outcome` off `2c9e919`. It lands after #101 (batch A), whose `AgentPendingEffectView` it extends.

Brief: issue #100 (the rakka.host run-evidence brief), ask 5, drafted against `2c9e919`. Batch A (asks 1 and 2) is #101 and batch B (asks 3 and 4) is #102.

Owner rulings (2026-10-13):
- The send time is stamped by the dispatcher, from an offset the executor reports. The MCP executor holds no clock (`rakka-agent-mcp/src/executor.rs:723`), and this keeps it that way.
- A call that provably never left is **recorded, not fixed**: it gets its own reason code and still parks. The behaviour fix is a separate follow-up issue.
- The approach is structured evidence end to end (below), not codes alone and not persisted message text.

## The gap

A non-idempotent MCP call whose answer was lost parks the run `WaitingForReconciliation`. A person then decides "not executed", "abandon" or "escalate". What reaches them is the effect's pipeline code, `mcp-outcome-unknown`, and nothing else:

- **Why it is unknown.** Every arm after the send maps to the one code (`executor.rs:926-951`). The message is dropped, because `failure_reason()` answers nothing for `Indeterminate` (`effect.rs:2406-2411`).
- **When it was sent.** `dispatched_at` is when the effect was handed to the sink, not when the call left.
- **Whether the server was told to stop.** At a deadline the executor cancels the call (`executor.rs:454-460`, #95), and the result is discarded (`client.rs:396-416`).

## What the code actually distinguishes

The brief names two arms: a deadline after the send, and a transport error after the send. The code has four situations behind them, because `within_after_send` wraps the whole of `call()` (`executor.rs:432`).

That covers four steps:
1. handing the request to rmcp;
2. waiting for the answer;
3. mapping the result;
4. the artifact write.

`send_cancellable_request` returns once the request is queued on rmcp's service channel (`rmcp-3.4.0/src/service.rs:919-937`). The channel send is cancel-safe, and `TransportClosed` there means the request was never queued.

| Where the call was | Did the server get it? | Reason code |
| --- | --- | --- |
| The deadline fired, or the transport closed, before rmcp accepted the request | **No.** It provably never left | `mcp-call-not-handed-over` |
| The deadline fired while waiting for the answer | Maybe | `mcp-deadline-awaiting-answer` |
| The transport failed while waiting, with no answer read | Maybe | `mcp-transport-awaiting-answer` |
| The answer arrived, and mapping or storing it overran the deadline | **Yes.** The server answered | `mcp-deadline-after-answer` |

The first row parks a non-idempotent effect today although nothing was sent. That predates this work, and it is the follow-up the owner approved. It is not changed here.

## Decision 1: the executor tracks how far the call got

The MCP executor's `in_flight: Option<RequestId>` becomes a private `CallProgress`:
- `NotHandedOver`;
- `AwaitingAnswer { id, handed_over: Instant }`;
- `Answered { handed_over: Instant }`.

`McpClientSession::call_tool` moves it forward:
- to `AwaitingAnswer` when `send_cancellable_request` returns;
- to `Answered` when the answer is read, whatever the answer is.

`execute` records `tokio::time::Instant::now()` when the attempt starts, beside the deadline it already computes (`attempt_deadline`, `executor.rs:887`). The offset reported is the hand-off instant minus that start. Both are monotonic, so the executor still reads no wall clock.

The reason code comes from the progress state, never from which wrapper fired:
- **`within_after_send`'s deadline arm:** `NotHandedOver` maps to `mcp-call-not-handed-over`, `AwaitingAnswer` to `mcp-deadline-awaiting-answer`, and `Answered` to `mcp-deadline-after-answer`.
- **`sent_call_error`:** a failure while `NotHandedOver` maps to `mcp-call-not-handed-over`, and any other non-`McpError` failure to `mcp-transport-awaiting-answer`. A JSON-RPC error is the server's own answer, and stays the ordinary failure it is today.

The mapping is one pure function of the progress state and the cause, so it can be tested without a server.

**"Sent" means handed over:** queued on rmcp's service channel. That is the closest point the executor can observe, and the spec and the docs say so. The wire write follows on rmcp's worker task.

### The cancellation

`cancel_and_close` returns whether the cancellation was **issued**. Issued means rmcp accepted `notify_cancelled` within the close bound (`SESSION_CLOSE_BOUND`). It does not mean delivered, and it does not mean honoured.
- On a legacy session (2025-11-25 and earlier) or a child process's stdio, rmcp sends `notifications/cancelled`.
- On a modern HTTP session, rmcp's worker turns the notice into an abort of the request's own HTTP exchange (`streamable_http_client.rs`, `is_control_message`). No message is sent.

The close runs after the attempt's outcome is computed (`executor.rs:449-461`), so `execute` sets the flag on the report afterwards. Only `mcp-deadline-awaiting-answer` can carry an issued cancellation: a transport failure has no session to cancel on, and an answered call has nothing in flight.

## Decision 2: a report on the error, stamped by the dispatcher

- **`AgentUnknownOutcomeReport`** is new, public and not durable, in `rakka-agent`'s dispatch module. It is `#[non_exhaustive]`, built with `AgentUnknownOutcomeReport::new()` and builder methods, and carries:
  - `reason: Option<AgentFailureReason>`, built with `AgentFailureReason::new`, so a blank code is no reason;
  - `handed_over_after: Option<Duration>`;
  - `cancellation_issued: bool`.
- **`AgentDispatchError::OutcomeUnknown`** gains `report: Option<Box<AgentUnknownOutcomeReport>>`, set through a new `AgentDispatchError::with_report(self, report)`. That method leaves every other variant unchanged, as `AgentRunEffectOutcome::with_reason` does. The variant becomes `#[non_exhaustive]`, so a later field is not a source break. `outcome_unknown(code, message)` is unchanged.
- **The attempt's start** is the instant the dispatcher already reads before `invoke`: `self.now()`, opening the attempt timer at `dispatch.rs:3120`. It is passed into `settle_unknown_outcome`.
- **Only the non-idempotent park uses the report.** It builds the durable evidence with `sent_at = attempt start + handed_over_after`, in whole milliseconds and saturating. The reason travels separately.
- **The other arms drop the report**, and are otherwise unchanged:
  - `ReadOnly` and `Idempotent` retry under the budget, and the next attempt replaces whatever this one knew.
  - `Reconcileable` asks the reconciler, and a park there records the reconciler's code (`reconciliation-unknown`, `reconciliation-exhausted`).

## Decision 3: the outcome, the record and the view

- **`AgentRunEffectOutcome::Indeterminate`** gains two fields, both `#[serde(default, skip_serializing_if = "Option::is_none")]`:
  - `reason: Option<AgentFailureReason>`;
  - `evidence: Option<AgentUnknownOutcomeEvidence>`.

  The variant becomes `#[non_exhaustive]`. It gains a constructor, `AgentRunEffectOutcome::indeterminate(code, message)`, and `with_evidence`. `with_reason` and `failure_reason()` now cover it. `park_indeterminate` builds through the constructor. Every literal in this workspace moves to it.
- **`AgentUnknownOutcomeEvidence`** is a new durable type, `Default`, with two fields, each left out while unset:
  - `sent_at: Option<AgentTimestampMillis>`;
  - `cancellation_issued: bool`.
- **`AgentRunEffect`** gains `unknown_outcome: Option<AgentUnknownOutcomeEvidence>`, which is `#[serde(default, skip_serializing_if = "Option::is_none")]`.
  - The run's indeterminate arm (`run.rs:4735-4746`) already copies `outcome.failure_reason()` into `last_error_reason`. It now also copies the evidence into `unknown_outcome`.
  - A new generation clears it, beside `last_error_code` and `last_error_reason` (`effect.rs:2012-2014`).
  - A park with no report records neither: a lost worker, the reconciler's codes, or an executor that attaches none.
- **`AgentPendingEffectView`**, as #101 extends it, gains `unknown_outcome`, left out while unset.
- **The run-state growth-reserve test** (`run.rs`, the maximal working set) puts maximal evidence on every effect.

## Durable and wire compatibility

- **No schema version moves.** Every new field is serde-defaulted and left out while unset. A record or outcome without evidence encodes byte-identically to before.
- **Older peers degrade, never mislead.** An older peer that receives an `Indeterminate` carrying the new fields ignores them; nothing on that path uses `deny_unknown_fields`. An older peer that rewrites a record drops them. Either way the answer falls back to today's, never to a wrong one.
- **The snapshot body** gains one optional member per pending effect, so the change is additive for Studio.
- **Reason codes, not refusal codes.** The four reason codes are registered in `docs/rakka-compatibility.md`. The pipeline code stays `mcp-outcome-unknown`. As with every failure reason, none of them is ever a metric label.
- **Source changes:**
  - `AgentDispatchError::OutcomeUnknown` and `AgentRunEffectOutcome::Indeterminate` become `#[non_exhaustive]`. An external pattern needs `..`, and an external literal needs the constructor.
  - `AgentDispatchError::OutcomeUnknown` gains a field.

## What this does not change

- Which MCP failures are unknown outcomes. The never-sent row still parks; that is the approved follow-up.
- How any unknown outcome settles, for any safety class.
- What any dispatch decision reads. Every new field is observability.
- The five reconciliation decisions, their semantics, and the idempotency key a redispatch reuses.

## What rakka.host changes at the pin bump

- **One unit-test literal:** `AgentRunEffectOutcome::Indeterminate { code, message }` at `crates/rakka-host-runtime/src/fleet.rs:2368` moves to `AgentRunEffectOutcome::indeterminate(code, message)`. Nothing else in the host builds or matches either widened variant.
- **No host code.** The snapshot route serves the view verbatim. The handoff tells Studio about the four reason codes and `unknown_outcome`, and what "issued" does and does not mean.

## Proof

Each new test is watched failing before the code exists. A falsification pass then breaks each behaviour and names the test that catches it.

**`rakka-agent-mcp`**, against `FakeMcpServer`:
- **A deadline while waiting, on a legacy session** (`with_legacy_only`): the reason is `mcp-deadline-awaiting-answer`, there is an offset, the cancellation was issued, and `cancellations()` holds the notice.
- **The same on a modern session:** the cancellation was issued, and `cancellations()` is empty (the HTTP exchange was aborted instead).
- **A transport failure while waiting** (`with_call_transport_failure`): the reason is `mcp-transport-awaiting-answer`, there is an offset, and no cancellation was issued.
- **The existing slow-artifact-sink staging** (`tests/artifact_sink.rs`): the reason is `mcp-deadline-after-answer`, there is an offset, and no cancellation was issued.
- **The classifier**, as an inline unit test: every progress state against every cause, including both never-sent cases. Those are not reliably stageable end to end.

**`rakka-agent`**, with a scripted executor:
- **A non-idempotent park records the evidence.** The executor reports a reason, a 250 ms offset and an issued cancellation. The effect record holds the reason in `last_error_reason`, and `sent_at` equals the attempt's start plus 250, checked against a literal under a pinned clock. The flag is set, and the pending-effect view reports all three.
- **An idempotent effect keeps none.** The same report on a retried effect leaves no evidence on the record.
- **A redispatch clears it.** A `ConfirmedNotExecuted` redispatch clears `unknown_outcome` on the new generation.
- **Record shape** (the `failure_reason_records.rs` pattern, with explicit checks): the outcome's new fields, the effect record's field and the view's field are each absent when unset, absent on decode of an older encoding, and round-trip when set.
- **The growth-reserve test**, updated.

**Validation:** `scripts/validate.sh` exits 0.

**Delivery:** one PR against `main` once #101 has merged. The changelog and compatibility entries go with it. The follow-up issue for the never-sent case is filed before the PR, so the PR can link to it.
