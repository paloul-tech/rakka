# A finished ticket whose run was never told (issue #134)

Status: approved by the owner on 2026-11-16 ("1b. approved"), with decision
1 settled as 1b: the row's last failure line is followed for every safety
class (section 8 has how that came about). Built (section 9). Size: a
project (two code paths, one of them new).

## 1. Intent

A dispatch ticket can be finished at the outbox, or at the outbox and the
fleet, while the run that owns the effect still waits on it. Three paths
that spend a ticket's budget write the outbox row (`Exhausted`), then the
fleet entry, then tell the run: `record_attempt_failure`, `retry_ambiguous`
and `conclude_on_finished_row` (fleet, then run). A fourth,
`settle_unknown_finding`, writes the row, then tells the run, then ends the
ticket. A worker can be lost between any two.

- **Lost before the fleet write.** The row is `Exhausted`, the entry is
  `Claimed`. The lease lapses, the entry is claimed again, and
  `execute_claim` starts an attempt: the row is neither `Dispatching` nor
  `Failed`, so it is not read as ambiguous, and the `Dispatching` mark sets
  the status whatever the row held. The target is invoked again, past its
  attempt bound, a reconcileable effect included. This is #134 as filed.
- **Lost after the fleet write.** The row and the entry are both
  `Exhausted` and the effect is `Ready`. Nothing claims a finished entry and
  nothing else looks. The run waits for good.
- **A finished entry whose row was then lost**, on a live run. The run puts
  a row back, registration leaves a finished entry alone, and nothing claims
  it. PR #155 covers a winding-down run and a claimable entry, not this.

All three are one broken rule: **a ticket that is finished while its run
still waits must end with the run told, and never with another
invocation.**

## 2. Goals, non-goals, anti-goals

**Goals**

- No attempt is ever started on a finished row.
- A run that still waits on an effect whose ticket is finished is told,
  whether or not a claim will come for the ticket.
- What the run is told is true of what the durable records hold.

**Non-goals**

- Changing the write order of the exhausting paths. The fleet settle before
  the delivery is what establishes that the worker still holds its claim; a
  late worker must not deliver a failure for a ticket another worker is
  running.
- Fencing every outbox write on the claim (#146 left it as a larger change).
- `rakka-workflow`: the substrate's `mark_outbox_dispatching` is not
  changed. The guard is the dispatcher's.
- The workflow tier's dispatcher, which already checks the row before the
  mark.

**Anti-goals**

- Telling a run an outcome while a worker that may still hold the ticket
  could deliver a different one.
- A repair that reads the shared fleet record on an ordinary pass.

## 3. Constraints

- A finished row is never rewritten by a settle write (#146). So what a
  finished row says stays true, whoever reads it.
- An `Exhausted` row keeps its attempt count and its last failure line. A
  failed attempt's line is `code: detail`; an ambiguous burn's line is the
  ambiguity's own text. The collaborator's failure reason, with its
  definitive marker, is on no durable record.
- A lease bounds how long a claim's worker may still be acting. A lapsed
  lease is the system's only statement that a worker is gone.
- "One outcome, one code path": an indeterminate park goes through
  `tell_indeterminate`.

## 4. Approaches

**Decision 1, the owner's: what a run is told for an `Exhausted` row nobody
delivered.**

1. **Follow the row (chosen, as 1b).** The row's last failure line is the
   line of the write that exhausted it, and it says which path that was. The
   run is told what that path's worker tells it: `Exhausted` with the row's
   code, or `Indeterminate` under the finding's code, for every safety
   class. A crash changes who speaks and not what is said, less the
   collaborator's reason, which no durable record keeps; the run's own
   readers of the reason err to the cautious side (a handoff send parks).
2. **Reconcileable always `Indeterminate`, the rest from the row** (1a, the
   issue's suggestion, and the owner's first pick). An uncrashed worker
   tells a reconcileable effect `Exhausted` at two of its three exhausting
   sites, so the same exhaustion would end differently crashed and not.
3. **Always `Indeterminate`.** The lost-row rule. An operator decision for
   every crash in this window, including a read-only failure the row
   records in full.
4. **Always `Exhausted`.** Calls a reconciliation that established nothing
   a failure.

**Decision 2, mine: who tells a run when no claim will come.**

1. **The per-run half of a pass (chosen).** `register_run` and `pump_run`
   already read the run and its outbox. They look at each effect the run
   waits on, and read the fleet record only when a row is finished or
   absent, or when registration read it anyway.
2. **Keep the entry claimable until the run is told**, with an extra fleet
   write that proves the claim before the delivery. One more write to the
   hottest shared record on every exhaustion, and it does nothing for a
   finished entry whose row was lost.
3. **Deliver before the fleet settle.** Removes the window, and lets a late
   worker deliver a failure while the claim holder's call is in flight.

## 5. Design

All in `crates/rakka-agent/src/dispatch.rs`.

**The claim path.** In `execute_claim`, after the stale, resolved and
replaced-row checks and before anything reads the row as startable: a row
that is `Dispatched`, `Exhausted` or `Cancelled`, with the intent still
outstanding, is concluded and never started. The entry is settled to follow
the row first, and the run is told second, and only when the settle
succeeded.

| Row | The run is told | The ticket |
|---|---|---|
| `Exhausted` | what the row's line rebuilds (below), attempts from the row | the entry follows the row (`settle_claim_from_row`) |
| `Cancelled`, and the wind-down fence applies (run winding down, kind not exempt) | `Cancelled`, as today | as today |
| `Cancelled` otherwise | `Indeterminate` | the entry follows the row |
| `Dispatched` | `Indeterminate`: the row says the call succeeded, and its result reached nobody | the entry follows the row |

**Reading an `Exhausted` row.** The row's last failure line is the line of
the write that exhausted it, and each exhausting site writes a known shape:

| Site | Line on the row | What its worker tells the run | What is rebuilt |
|---|---|---|---|
| `record_attempt_failure` | `code: detail` | `Exhausted(code, detail)` with the collaborator's reason | `Exhausted(code, detail)`, no reason |
| `retry_ambiguous`, worker lost | `dispatcher lost after started` | `Exhausted(dispatcher-lost-after-started, "the retry budget was spent recovering ambiguous attempts")` | the same |
| `retry_ambiguous`, after an executor's unknown outcome was found not executed | `code: message` | `Exhausted(code, "the retry budget was spent…")` | `Exhausted(code, message)`: the code is the same, the detail is the row's |
| `settle_unknown_finding` | `reconciliation outcome unknown`, or the mismatch line | `Indeterminate` under `reconciliation-exhausted`, or `effect-outcome-mismatch` | the same |

So, in this order: a finding line rebuilds its `Indeterminate`; the
worker-lost line rebuilds its `Exhausted`; any other line is split at its
first `": "` into code and detail when the part before it is non-empty and
has no whitespace, and rebuilds `Exhausted`. A line of no known shape, or
no line, says the budget is spent and not how: a reconcileable effect parks
under `reconciliation-exhausted`, and any other is told `Exhausted` under
`ticket-row-settled` with the line as detail. The finding lines and the
worker-lost line are named constants paired with their codes, shared by the
site that writes each and the reader, as `WORKER_LOST` already is. The
mismatch line itself has the `code: detail` shape, so it is matched before
the split.

**Fleet first, run second**, the order `conclude_on_finished_row` keeps,
and for its reason. The settle is fenced on the claim, so it is what
establishes that this worker still holds the ticket. A worker whose lease
lapsed while it worked through its batch is answered `claim-fenced` and
tells the run nothing: the worker that took the ticket over may be between
its own fleet write and its own delivery, and would be beaten to the run by
a rebuilt outcome. A crash between this settle and the delivery leaves the
entry finished and the run untold, which the per-run half below repairs.

A row can be exhausted by a late worker while the claim holder's call is
still in flight. That holder is not affected: it delivers a success before
it touches the row, and a failure through `conclude_on_finished_row`. Only
if its lease also lapses mid-call does another worker reach this arm and
tell the run `Exhausted` ahead of it. A lapsed lease is the system's only
statement that a worker is gone, and today that other worker invokes the
target again.

**The mark.** `InboxWrite::Dispatching` goes through the same finished-row
check the settle writes use. A row finished between the claim's read and the
mark ends the claim through the table above instead of being invoked.

**The per-run half.** One sweep in `register_run_into`, for every run, live
or winding down, over each effect the run still waits on that has reached
`Ready`. It replaces the lost-row part of `fence_run` and keeps the fence's
own two cases.

| Row | Fleet entry | What happens |
|---|---|---|
| live | a worker holds or can claim it, or none | nothing |
| finished | a worker holds or can claim it | nothing: the claim path owns it |
| finished | finished for at least one lease | the run is told by the claim-path table |
| finished | absent, and the row finished at least one lease ago | the same |
| absent or live | finished, or fenced by a cancellation request | parked `ticket-row-missing`; a live row is cancelled |
| absent | a worker can claim it | nothing: the claim parks it (#155) |
| absent | none, run winding down | the fence's tombstone and `Cancelled`, as today |
| absent | none, run live | nothing: the run's own flush lands or is retried |

The fleet record is read only for a finished or absent row, and used
without a read when registration left it current. A live row under a
finished entry is therefore seen in the pass that registers it: a row the
run puts back is `Pending` and due, so registration reads the fleet record
for it.

**A live row under a finished entry is always a row put back**, whatever
the entry's stamp says. An entry is finished only after its row is
finished or gone, and nothing makes a finished row live again once the mark
is guarded. So this arm does not ask `indexes_row`, and it reaches an entry
registered before #155, which has no stamp.

**The wait of one lease.** A worker that has just finished the entry is
about to deliver, with the collaborator's reason the row does not keep. A
sweep that told the run first would win by microseconds and lose that
reason in a flow where nothing crashed. An entry finished for a full lease
has no worker behind it. An entry that is absent was finished and pruned;
the row's own finishing time stands in for it.

### Calls I made, for you to check

- **Fleet first, run second** on the claim path. An earlier draft had the
  reverse; the review of this document showed a worker with a lapsed claim
  would then speak over the claim's holder.
- **A live row under a finished entry is parked without asking the stamp.**
- **The wait for an absent entry is measured from the row.** A prune with a
  horizon shorter than a lease can still let the sweep speak a moment
  before a worker that is about to.
- **The code of an indeterminate park from a finished row.** `Dispatched`
  and live `Cancelled` rows park under `ticket-row-settled`, the fleet's
  existing word for "the entry follows a finished row", new as an effect's
  indeterminate code. A reconcileable `Exhausted` row whose line has no
  known shape parks under `reconciliation-exhausted`.
- **How the exhausted code is read from the row's line** (the table in
  section 5), and the fallback code `ticket-row-settled` for a line of no
  known shape.
- **No reason is invented.** The repaired outcome carries none.
- **A lost row on a live run gets no wait.** Nothing delivers for it.

## 6. Tests

`crates/rakka-agent/tests/dispatch_owed_outcome.rs` (the two reproductions
become the first two) and the pinned #134 test in
`dispatch_finished_row.rs`:

- an `Exhausted` row under a re-claim is not invoked; the run is told what
  the row's line rebuilds; the entry follows the row;
- the same through the reconciler's `Unknown` arm with a reconcileable
  effect, which the issue has only by reading, and through its mismatch
  arm, under that arm's code;
- `record_attempt_failure` and `retry_ambiguous`, each crashed after its
  outbox write and after its fleet write, and `settle_unknown_finding`
  crashed after its outbox write (it has no later window): the run is told
  what an uncrashed worker tells it, less the reason and, in one row of
  section 5's table, the detail, for a reconcileable effect as for any
  other;
- a row whose line has no known shape, for a reconcileable effect and for
  another;
- a worker whose claim lapsed before it reached a finished row tells the
  run nothing and its pass does not fail;
- a finished entry is not swept before a lease has passed, and a live
  worker's own delivery wins;
- a live row under a finished entry with no stamp is parked;
- a row finished between the claim's read and the mark is not invoked;
- a live run whose finished ticket lost its row is parked, and the row it
  put back is cancelled;
- `Dispatched` and `Cancelled` rows with an outstanding intent;
- an ordinary pass reads the fleet record no more often than before.

Each guard is falsified once before the work is called done.

## 7. Documentation

`docs/rakka-compatibility.md` (a dated entry), `docs/rakka-agents.md` (the
dispatcher section), `CHANGELOG.md`.

## 8. How decision 1 was settled

The owner first chose "Option 1. By safety class, code from the row.", put
to them as the option closest to what the lost worker would have said, with
reconcileable effects always `Indeterminate`. The review of this document
showed that description was wrong for reconcileable effects: an uncrashed
worker tells them `Exhausted` at two of the three sites that spend their
budget. Put back to the owner as 1a (keep the pick), 1b (follow the row for
every class) and 1c (make the uncrashed sites park too, outside this
issue), the owner chose 1b.

## 9. As built

Where the build moved from the sections above, and what it chose where they
left a choice. Plan: `docs/superpowers/plans/2026-11-16-finished-ticket-untold-run.md`.

- **Names.** The reader is `owed_by_exhausted_row`, in the private inline
  module `dispatch::owed_outcome`, with the lines the exhausting sites write
  (`WORKER_LOST_LINE`, `RECONCILIATION_UNKNOWN`, `OUTCOME_MISMATCH`) beside
  it. It was a file of its own first; the crate's shape test holds every
  source file to a public module, and its items are the dispatcher's own.
  `owed_by_finished_row` reads any finished row into a `RowVerdict`
  (`NeverStarted`, `Exhausted`, `Unknown`); `tell_owed` delivers it;
  `conclude_on_untold_row` is the claim-path arm; `mark_dispatching` is the
  guarded mark; `settle_waited_effects` is the per-run sweep, and `fence_run`
  is gone into it.
- **A rebuilt `Exhausted` closes no segment.** The lost worker's attempt
  segments were closed, or never opened, when it was lost. An `Indeterminate`
  goes through `tell_indeterminate`, which closes the park's segment and
  counts it, as every park does.
- **The sweep looks twice.** First at the outbox rows registration already
  read, and at the fleet entries when registration read those too; that look
  reads nothing. Only an effect whose row is finished or absent there, or
  live under a finished entry, starts the second look, which reads the run's
  outbox again for each effect (telling a live run makes it flush, which
  changes the next effect's row) and the fleet record once unless it is
  current. A winding-down run's pass reads less than it did: the fence read
  the outbox once for each ready effect.
- **A live row is parked only under a finished entry**, not under one a
  cancellation request fenced (section 5's table had both). The request is
  the first step of a cancellation another worker is carrying out, and that
  worker settles the row itself. A row that is absent or finished under a
  fenced entry is handled as the table says.
- **A live row under a finished entry is cancelled first and the run told
  second.** Told first, a crash would leave a `Pending` row due for good
  under an entry nothing claims, and every pass of that run would read the
  fleet record for it. Cancelled first, a crash leaves a finished row, which
  the sweep tells the run from after a lease, under `ticket-row-settled`
  where an uncrashed sweep says `ticket-row-missing`.
- **The wait is measured from the entry's `updated_at`**, which every finish
  and every cancellation request sets.
- **A winding-down run with an `Exhausted` row is told what the row says.**
  The claim's wind-down branch used to cancel that ticket as never started.
- **The lapsed-claim test** uses one ticket and a store hook that moves the
  clock past the lease at the claim's first look at the row. Section 6's
  mid-batch shape cannot be built without a second worker: a first claim
  that outlives its lease fails the pass at its own fleet write, which is
  the behaviour before this change and is not touched.
- **No read-count test for a run with nothing due.** A `Dispatching` row is
  due, so registration reads the fleet record for an in-flight ticket; the
  cost test counts both stores on a healthy due ticket instead, and measured
  two outbox reads and one fleet read before the sweep existed and after.
- **No stack change was needed.** The whole `rakka-agent` suite ran with all
  features without an overflow.

**Falsified**, one mutation at a time against the reader's unit tests,
`dispatch_owed_outcome`, `dispatch_finished_row` and `dispatch_missing_row`.
Three of the first eighteen survived and each got a test.

| Mutation | Fails |
|---|---|
| The claim path starts a finished row | four, among them `an_ambiguous_budget_lost_before_the_fleet_write_is_told` |
| The run is told without the fleet settle | seven, among them `a_worker_whose_claim_lapsed_tells_the_run_nothing` |
| `claim-fenced` fails the pass | `a_worker_whose_claim_lapsed_tells_the_run_nothing` |
| The mark is unguarded | `a_row_finished_between_the_claims_read_and_the_mark_is_not_invoked` |
| The reader does not know a finding line | two unit tests and `the_same_through_the_mismatch_arm_parks_under_its_code` |
| The reader does not know the worker-lost line | a unit test and both ambiguous-budget tests |
| The reader takes a code with whitespace, or an empty one | the two unit tests of lines with no known shape |
| The sweep does not wait a lease | `a_finished_entry_is_not_swept_before_a_lease_has_passed`, `a_finished_row_whose_entry_was_pruned_is_repaired_from_the_row` |
| The sweep does not wait for a pruned entry | survived: the fixture's clock sat below one lease. `a_finished_row_whose_entry_was_pruned_is_repaired_from_the_row` now starts ten leases in |
| The sweep leaves a live row under a finished entry, in either look | `a_live_run_whose_finished_ticket_lost_its_row_is_parked`, `the_same_for_an_entry_with_no_stamp` |
| The sweep always takes its second look | `a_healthy_due_ticket_costs_the_per_run_half_no_more_reads` |
| A `Dispatched` row is read as exhausted | `a_dispatched_row_nobody_delivered_is_parked` |
| A `Cancelled` row is never-started without the fence | `a_cancelled_row_on_a_live_run_is_parked` |
| The sweep speaks under a claim | `a_finished_row_under_a_claim_is_left_to_the_claim_path`, written for this mutation before it was run |
| A live row is parked under a cancellation-fenced entry | survived in each look alone: the two looks hold the same test, and one ticket cannot show either without the other. Mutated together, `a_live_row_under_a_cancellation_in_progress_is_left_alone` fails |
| A live row is parked and not cancelled | `a_live_run_whose_finished_ticket_lost_its_row_is_parked`, `the_same_for_an_entry_with_no_stamp` |
| A live row's cancellation is not counted | survived; `a_live_run_whose_finished_ticket_lost_its_row_is_parked` now asserts the count |
