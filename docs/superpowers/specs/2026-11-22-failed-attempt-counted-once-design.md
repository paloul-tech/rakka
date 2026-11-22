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
   orders. Every new test reads the row's count and what the run charged side
   by side, and the run's record of the call where the run still holds it (a
   call that succeeded is gone from the run once its turn moves on).

Two more, after the second review (section 7):

3. **After a failed attempt, the dispatch authority is asked before the
   reconciler.** A retry the authority defers then asks the reconciler
   nothing. Without it, every dispatcher pass asked the reconciler while an
   agent was suspended: the dispatcher's deferral backoff is 0.
4. **The first attempt a claim makes after the reconciler's answer needs a
   current lease.** As first approved, the lease had to cover the attempt, as
   issue #171 asks of a later attempt. I then found that this never retries an
   effect whose answer and call together outlast the lease, since every claim
   on this path starts with the question (#171's
   `a_bound_longer_than_the_lease_means_one_attempt_to_a_claim` is such an
   effect). Asked again, the owner chose "the lease is still current", with
   the docs saying this path's lease must cover the answer and the call.

## 3. The change

All in `crates/rakka-agent/src/dispatch.rs`. Issue #182 had already computed
`started` in `execute_claim` (the row's count for a `Failed` row, one more for
a `Dispatching` one) and handed it to the recovery, where only the two
cancellations read it. Now every arm that ends the recovery without another
attempt reads it.

| Where | Before | Now |
|---|---|---|
| `execute_claim`, a `Failed` row of a reconcileable call the fence does not cancel | asks the reconciler | asks the authority first (`defer_before_reconciling`): a transient refusal defers there; anything else goes on to the reconciler (ruling 3) |
| `reconcile_ambiguous`, `NotExecuted` on a live run, an attempt follows that is the claim's first | made whatever the lease | made only if `is_current_claim_for(claim, 0)`; otherwise `OutOfLease`, nothing written (ruling 4) |
| `reconcile_ambiguous`, `NotExecuted` on a live run | `retry_ambiguous`: burn, then the next attempt | where `started < attempt` (the row counted the attempt asked about), `retry_counted_attempt`: the row is read again. Still `Failed` at `started`: `ClaimStep::Invoke` with `attempt`, nothing written. Gone, put back, or finished: the conclusion a claim that read it first reaches. Otherwise `OutOfLease`, nothing written. Where the row did not count it, unchanged |
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
`dispatch_failure_outcomes.rs` spun for 27 minutes before I stopped it. The
next attempt after a `Failed` row is within the budget, since a `Failed` row
is one with a retry left.

The lease rule of ruling 4 asks length 0 of a claim's first attempt and keeps
#171's cover check (`timeout_ms`) for a later one. A first attempt follows a
finding about an attempt that was not this claim's: always where the row
counted it, and where it did not, when the burn leaves budget.

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
protocol, which the spec validation makes unreachable (section 8).

In `crates/rakka-agent/tests/dispatch_failed_attempt_recovery.rs`, the cases
the two reviews found (section 7), on the shared `AuthorityFixture`:

- A retry the authority defers after a failed call spends nothing and asks
  nothing: five passes, four of them back to back, each deferred, the
  reconciler asked nothing, the row still `Failed` 1, nothing charged;
  resumed, the budget of three buys three calls and two questions.
- A cancelled run's failed call is cancelled on the finding while its agent
  is suspended: under the fence the authority is not asked first.
- A claim whose ticket moved while the reconciler answered (a rival made
  attempt two) makes no attempt, and the three calls carry attempts 1, 2, 3.
- A claim whose lease lapsed while the reconciler answered makes no
  attempt, though the row had not moved; the next claim makes it.
- The same for a lost attempt: the late claim burns nothing, and the next
  counts the lost attempt and makes the next.
- Under a current lease, while a worker on a lapsed lease of its own writes
  the row as the reconciler answers: a row in flight at the same count gets
  no attempt beside it; a row failed again at a new count gets none; a row
  exhausted is told to the run in the same pass.
- A park after a failed call names attempt one and links its span, in the
  dispatcher's park segment and the run's `checkpoint-open`.

Against `main`'s `dispatch.rs` (swapped in, the rest of the branch kept) the
first three of the first review's cases failed. Against the dispatcher of the
answer to the first review (`fd0f662d`), four fail: the deferral, both lease
cases and the exhausted row. The two moved-row cases pass there, as that
version already read the row again.

Ten breaks of the answer to the second review were run one at a time
against `dispatch_failed_attempt_recovery.rs` and the five dispatch test
files above (with `dispatch_finished_row.rs`). All ten are caught:

| Break | Caught by |
|---|---|
| no authority before the reconciler | the deferral test |
| the authority asked first under the fence too | the cancelled-run test |
| no lease check at all | both lease tests here, and thirteen of #171's |
| a first attempt follows only when the burn leaves budget | the failed-row lease test, once it ran on the call's last attempt (it first ran at a budget of six, where the break survived) |
| a first attempt follows only where the row counted the attempt | the lost-attempt lease test |
| a first attempt must be covered for its bound | #171's bound-longer-than-the-lease test |
| a finished row ends out of lease | the exhausted-row test |
| the row check without its status half | the in-flight test |
| the row check without its count half | the failed-meanwhile test |
| no row check | both of those |

## 7. The reviews

### The first review

A fresh reviewer read the branch and ran six probes of its own, on the
branch and on `main`. What changed in answer:

- **A retry the authority defers** (a suspended agent) spent the budget on
  `main`: each claim burned an attempt for the finding before the deferral,
  and the run failed under `dispatcher-lost-after-started` after one call.
  The fix ends that, which nothing had said or held. The reconciler is now
  asked on every claim while the retry is deferred, bounded by the claims
  and not the budget, so my sentence "the queries stay inside the budget"
  was true only of `Unknown`. Documented and tested; flagged to the owner.
- **The no-burn branch handed back the attempt number of the claim's first
  read.** A reconciler slower than the lease let a rival worker take the
  ticket and make attempt two, and the late claim then made a second call
  as attempt two, under the same derived span identity. `main` made the
  second call too, under a later number. The claim now reads the row again
  and makes no attempt if the row moved: it ends out of lease, writes
  nothing, and the next claim takes the ticket up from the row. My call:
  this goes further than restoring the number. I wrote that it removes the
  second call in that race; the second review showed it does not where the
  rival claims after the re-read, which ruling 4 closes.
- **The park's telemetry changed** (it names and links the failed attempt,
  not a span no segment exported): documented and tested.
- **Sentences corrected:** "called as many times as its budget allows" holds
  only when every finding is "not executed"; "asked once before each retry"
  is "on every claim that would make a retry"; the tests do not all read the
  run's record of the call.
- The reviewer also said a deferral charges the attempt that never started.
  By reading, `defer_dispatch` writes only the fleet entry and tells the run
  nothing; the test above holds that nothing is charged. That is issue
  #196's refusal path (`refuse_dispatch`), not the deferral.

### The second review

A second fresh reviewer read the answer to the first (`fd0f662d`) and ran
seven probes. What changed in answer:

- **The deferral asked the reconciler on every pass**, not on every backoff:
  the dispatcher's deferral backoff is 0 and has no setter. Brought to the
  owner (ruling 3).
- **The re-read was a read, not a fence.** A rival that claimed after it,
  while the late claim was being authorized, called beside it under the same
  number; the late claim's lease had lapsed during the question. Brought to
  the owner (ruling 4), with the correction about the cover rule.
- **A finished row ended the claim out of lease**, so a run whose row a stale
  worker exhausted heard only a lease later. A finished, missing or replaced
  row now takes the path a claim that read it first would.
- **The status half of the check had no test**, and my record said none
  could tell; a row in flight at the same count does. Tested now.
- **Sentences corrected:** "paced by the retry backoff"; "reads the row
  again" without saying it is after a failed attempt only; "another worker
  moved it" where a deleted or replaced row also fails the check; the
  `lapsed` docs that said every such claim ran out of lease; "a reconciler
  that cannot establish the outcome" where an `Unknown` one was meant.

## 8. Not done

- The park for a record with no reconciliation protocol is unreachable and is
  held by no test.
- The re-read's gone and put-back rows are held by no test; their paths
  are the ones a claim that read them first takes, by reading.
- A park after an `Unknown` burn that spends the budget still names the
  attempt the burn recorded, which no call made, and links that attempt's
  span. Unchanged, by reading; it follows from ruling 2.
- The lease is checked at the answer, not fenced: a lease that runs out
  during the authority's answer or the call itself can still let a rival in,
  as for any claim's first attempt.
- A reconciler that answers with an error ends the pass and burns nothing,
  so a reconciler that keeps failing is asked once per lease without bound,
  as on `main` (by reading, from the second review).
- Issue #196 (a dispatch refused before `Started` is charged one attempt) is
  a separate cause and needs its own ruling.
