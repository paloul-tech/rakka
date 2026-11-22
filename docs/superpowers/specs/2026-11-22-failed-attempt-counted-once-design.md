# A reconcileable call's failed attempt is counted once (issue #195)

2026-11-22. Short design. The owner ruled on the two open questions before the
build; my one call is listed. The owner reviews at the pull request.

## 1. What was wrong

A reconcileable call is asked about before every retry. When its attempt fails
with a retryable failure, the outbox row counts the attempt and is left
`Failed`, and the next claim asks the reconciler whether the call executed
after all (`execute_claim` treats such a row as ambiguous).

That claim ran the recovery for a `Dispatching` row: an attempt whose worker
was lost after its durable `Started`, which the row has not counted. It handed
the recovery the row's count plus one as the attempt in question, and every
arm took that number as an attempt not yet counted. For a `Failed` row the
attempt in question was counted when it failed.

| The reconciler's answer | What the recovery did | What it should do |
|---|---|---|
| Not executed | recorded one more failed attempt, then invoked | invoke, with no new failure |
| Executed | delivered the outcome with the row's count plus one | the row's count |
| `Unknown` or mismatched, on a winding-down run; no reconciler wired | parked with the row's count plus one | the row's count |

Measured: a budget of six bought three calls, and the run failed under
`dispatcher-lost-after-started` with `attempts: Some(6)`, though no worker was
lost.

## 2. The owner's rulings (2026-11-22)

1. **A finding on a `Failed` row goes straight to the next attempt**, with no
   new failure, and a finding that ends the recovery is delivered or parked
   under the attempts that started. Each `Unknown` finding on a live run still
   burns one attempt (the tested policy that keeps the queries inside the
   budget).
2. **The run's charged attempts and the row's count are checked in tests
   only.** They differ by design: the row counts failures (a success adds
   nothing, a burn adds one), and they sit in two stores written in different
   orders. Every new test reads the row's count, the run's record of the call
   and what the run charged side by side.

## 3. The change

All in `crates/rakka-agent/src/dispatch.rs`. Issue #182 had already computed
`started` in `execute_claim` (the row's count for a `Failed` row, one more for
a `Dispatching` one) and handed it to the recovery, where only the two
cancellations read it. Now every arm that ends the recovery without another
attempt reads it.

| Where | Before | Now |
|---|---|---|
| `reconcile_ambiguous`, `NotExecuted` on a live run | `retry_ambiguous`: burn, then the next attempt | where `started < attempt` (the row counted the attempt asked about), `ClaimStep::Invoke` with `attempt` and nothing written; otherwise unchanged |
| `reconcile_ambiguous`, `Executed` | delivered with `attempt` | `started` |
| `reconcile_ambiguous`, no reconciler wired | parked with `attempt` | `started` |
| `reconcile_ambiguous`, no protocol on the record | parked with `attempt` | `started` (unreachable: the spec validation refuses such a record) |
| `settle_unknown_finding`, winding-down park | `attempt` | `started`, a new argument |
| `settle_unknown_finding`, park after a burn that spent the budget | `attempt` | unchanged: after the burn the row counts `attempt` |

`started == attempt` for a `Dispatching` row and for an attempt this claim
made itself (`settle_unknown_outcome` passes both as `attempt`), so nothing
changes on those paths. The branch has to stay off those paths: the burn is
what moves the row's count past an attempt it has not counted. A break of the
fix that took the branch whenever the finding was "not executed" handed back
the same attempt number, and a claim whose own attempt ended unknown made
that attempt again without end inside its `ClaimStep` loop: a test of
`dispatch_failure_outcomes.rs` spun for 27 minutes before I stopped it. The lease check of issue #171 runs only for an attempt
made under the claim, which the row has not counted, so it never applies to
the new branch. The next attempt after a `Failed` row is within the budget,
since a `Failed` row is one with a retry left.

## 4. My call

**Records already charged are not repaired.** The row keeps only its last
failure line, so an extra failure recorded this way cannot be told from a
failed call after the fact, and a run's charge was settled when it was told.
A row still waiting for its retry goes on from the count it has. Yours to
reverse.

## 5. Mixed builds and rollback

Nothing durable is added. The binary that runs the claim decides; an earlier
one counts as before.

## 6. Tests

In `crates/rakka-agent/tests/effect_dispatch.rs`, with the real dispatcher and
a reconcileable tool call:

- A failed call found not executed is retried without another failed attempt:
  budget six, every call fails, six calls, the row `Failed` 1 to 5 and then
  `Exhausted` 6, six charged, the run failing under the tool's own code.
- A lost call found `Unknown` then not executed is charged one attempt per
  call: two calls, two charged.
- A failed call found executed is charged the attempt it made.
- A failed call parked for a missing reconciler, and one found `Unknown` or
  mismatched on a cancelled run, are each charged the attempt it made. The
  mismatched finding reaches its park from another call site.
- Two guards against fixing too much: an attempt lost after `Started` and
  found not executed is still counted before its retry; an `Unknown` finding
  on a failed call still counts an attempt, and the park is charged what the
  row counts.

The first six fail on `main` with the issue's numbers; the two guards pass
there.

Twelve breaks of the fix were run one at a time against `effect_dispatch.rs`,
`dispatch_owed_outcome.rs`, `dispatch_attempt_lease.rs` and
`dispatch_failure_outcomes.rs`. Eleven are caught:

| Break | Caught by |
|---|---|
| the burn restored on a `Failed` row | both "not executed" tests |
| the burn skipped for every "not executed" finding | the lost-attempt guard, ten lease tests, and a hang in `dispatch_failure_outcomes.rs` |
| "executed" delivered with the next attempt's number | the "executed" test |
| the missing-reconciler park with the next attempt's number | its test |
| the mismatch call site passing the next attempt's number | its test |
| the `Unknown` call site passing it | its test |
| the winding-down park in `settle_unknown_finding` taking it | both winding-down tests |
| the park after a spending burn charged `started` | the `Unknown` guard |
| the retry made under the counted attempt's number | both "not executed" tests |
| a `Failed` row's next attempt counted as started | six new tests and one of #182's |
| a `Dispatching` row's lost attempt not counted as started | the lost-attempt guard, two of #182's, four lease tests |

The one that survives is the park for a record with no reconciliation
protocol, which the spec validation makes unreachable (section 7).

## 7. Not done

- The park for a record with no reconciliation protocol is unreachable and is
  held by no test.
- Issue #196 (a dispatch refused before `Started` is charged one attempt) is
  a separate cause and needs its own ruling.
