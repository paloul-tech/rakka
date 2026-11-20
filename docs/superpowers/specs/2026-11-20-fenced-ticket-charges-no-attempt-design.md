# A call the dispatcher cancels before it starts is charged no attempt (issue #182)

2026-11-20. Short design, with my calls listed. The owner reviews at the pull
request. Stacked on the fixes for issues #179 and #181.

## 1. What was wrong

When a run is winding down, the dispatcher's fence cancels a ticket whose next
attempt has not started, and tells the run `Cancelled`. It told the run with
the number of the attempt its claim was about to make: the row's count of
attempts, plus one. The run keeps the largest attempt number it is told
(`record_attempt`) and, when the call resolves, charges that many attempts and
gives back the rest of the reservation.

So a call that was sent, never claimed and then cancelled was recorded with
one attempt made and charged one effect attempt, though its executor was never
invoked. The run's consumption goes to its task with its settlement, and an
epoch's to its goal.

The comment where the run settles attempts already says what was meant: "A
generation that never dispatched (one the dispatch layer cancelled) reports
zero attempts and so consumes none".

## 2. Approaches

1. **The dispatcher reports the attempts that started.** Each place that
   cancels a ticket for a run that is winding down tells the run that count.
2. **The run does not charge a `Cancelled` outcome's attempt.**

I chose the first. The run cannot tell from `Cancelled` alone whether the
attempt it is told of started: the dispatcher also cancels a call after an
attempt that did start and was lost, and that attempt is charged on purpose.
The claim can tell, from the status its row was left in. The fix is one number
at each place that knows.

## 3. The change

The dispatcher cancels a ticket for a winding-down run in three places, and
replays the first of them in two more. Each now tells the run the attempts
that started.

| Where (`dispatch.rs`) | The row it finds | Told before | Told now |
|---|---|---|---|
| The fence, in `execute_claim` | `Pending`, or `Failed` with a retry due | The row's count plus one | The row's count |
| The fence replayed by the next claim (`owed_by_finished_row`): the worker was lost before its fleet write | `Cancelled` | The row's count plus one | The row's count |
| The fence replayed by the per-run sweep: the worker was lost after its fleet write | `Cancelled` | The run's own count | The row's count |
| A reconcileable call found not executed (`reconcile_ambiguous`) | `Failed`: the attempt asked about ended and was counted | The row's count plus one | The row's count |
| The same, and a read-only call (`recover_ambiguous`) | `Dispatching`: an attempt started and was lost, uncounted | The row's count plus one | Unchanged: that is the attempt that started |

The row counts an attempt when it ends, in a failure. So a `Failed` row has
counted every attempt that started, and a `Dispatching` row has one more that
it has not. `execute_claim` works that out once (`started`) and hands it to
the recovery.

The sweep's replay now reads its count from `owed_by_finished_row`, the same
function the claim's replay uses, so the two cannot drift.

Nothing in the run changes.

## 4. My calls

1. **Fixed in the dispatcher** (section 2). Yours to reverse.
2. **The comment was right and the number was wrong.** The issue left open
   which. The run's budget rule is that an attempt which reached a durable
   `Started` is charged, and a ticket the fence cancels has no such attempt
   beyond those its row counts.
3. **The replay takes the row's count.** A row left `Cancelled` does not say
   whether its last attempt started. Most such rows are the plain fence's.
   The cost, which the reviewer reproduced: a ticket cancelled after an
   attempt that started and was lost, whose second worker was then lost too
   before it told the run, is told with one attempt fewer than started.
4. **The sweep's replay can now charge more than it did.** It told the run
   the run's own count, and the run is never told of a failed attempt that
   is retried. So a call with failed attempts, cancelled by a worker that
   died before telling the run, was charged none of them. It is now charged
   what its row counts.
5. **The cancellation after a failed attempt of a reconcileable call is in.**
   The reviewer reproduced it: one attempt made, two charged. It is the
   symptom this issue names. **The other outcomes of that same recovery are
   not in.** A finding of "executed", a park, and the retry on a live run
   are each handed the same number, one more than the attempts made, and the
   live retry also records a failed attempt on the row. That is the cause an
   earlier unfiled draft describes, and what it needs is a decision about the
   retry's accounting, not a number. Yours to decide.
6. **Records are not repaired.** A run that was already told keeps the
   attempt it was charged.

## 5. Mixed builds and rollback

Nothing durable is added. A dispatcher of an earlier build reports the earlier
numbers, and a run of either build charges what it is told.

## 6. Tests

In `crates/rakka-agent/tests/effect_dispatch.rs`, with the real dispatcher and
a tool call whose ticket is out when the run is cancelled. Each reads the
call's status and attempts, the run's spent effect attempts and its reserved
attempts, and how often the tool was invoked.

- A call the fence cancels before it starts is charged no attempt.
- A call the fence cancels before its retry is charged the one attempt it
  made.
- A read-only call cancelled after its attempt started and was lost is
  charged that attempt.
- A reconcileable call found not executed after its attempt started and was
  lost is charged that attempt.
- A reconcileable call cancelled after a failed attempt is charged that
  attempt alone.

In `dispatch_owed_outcome.rs`, with a call whose first attempt failed, so that
the row counts one attempt and the run was told of none:

- The fence tells the run the row's count.
- A fence lost before its fleet write is replayed by the next claim, with the
  same count.
- A fence lost before it told the run is replayed by the sweep, with the same
  count.

The first, the fifth and the last fail on the commits below their fixes.

Nine breaks of the fix as it now stands were run, one at a time, against the
two test files. Each is caught by a test:

- the fence reporting the attempt it was about to make, and reporting none
  at all;
- the claim's replay reporting one more than the row counts, and reporting
  the run's own count;
- the sweep's replay reporting the run's own count;
- a failed row's recovery counting the next attempt as started, and a lost
  attempt not counted as started;
- the read-only cancellation reporting one attempt fewer, and the reconciled
  cancellation reporting the claim's next attempt.

The whole crate passes: 1,845 tests.

## 7. The review

A fresh reviewer read the first two commits and ran probes and breaks of its
own. What changed in answer:

- **The sweep's replay** told the run's own count. Reproduced; fixed (call 4).
- **The cancellation after a failed attempt of a reconcileable call** charged
  an attempt that never started. Reproduced; fixed (call 5). The record had
  called that path "unchanged and right".
- **No test held the number the two cancellations after a started attempt
  report.** The record declared it; the reviewer wrote the tests, and they
  are kept.
- **The fence's test could not tell the row's count from zero,** and the
  replay's could not tell it from the run's count. Both now run over a row
  that counts one attempt.
- **Sentences corrected:** the fence is in `execute_claim`; the test pumps
  the dispatcher rather than running one pass; "and no other" overstated the
  budget rule, which a refused dispatch also departs from.

## 8. Not done

- Records already charged (call 6).
- The double fault of call 3.
- The other outcomes of a reconcileable call's recovery from a failed
  attempt (call 5). Drafted as an issue with the earlier draft, not filed.
- **A dispatch refused before its attempt starts is charged one attempt.**
  Found by the reviewer and reproduced. It is not a cancellation, and the
  number is a public field of the run's `EffectFailed` reason. Drafted as an
  issue, not filed.
