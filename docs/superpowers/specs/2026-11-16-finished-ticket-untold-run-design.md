# A finished ticket whose run was never told (issue #134)

Status: written 2026-11-16, awaiting the owner's approval. The owner chose
option 1 of decision 1 the same day ("Option 1. By safety class, code from
the row."). Nothing is built beyond the reproductions. Size: a project (two
code paths, one of them new).

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

1. **By safety class, with the row's own code (chosen by the owner).**
   Read-only, idempotent and non-idempotent effects are told `Exhausted`
   with the code and detail read from the row's last failure line; a
   reconcileable effect is told `Indeterminate`, as the issue suggests. For
   the first three classes this is what the lost worker would have said,
   less its reason, so the run's own readers of the reason err to the
   cautious side (a handoff send parks). **For a reconcileable effect it is
   not always what the lost worker would have said**: only
   `settle_unknown_finding` parks. `record_attempt_failure` and
   `retry_ambiguous` tell a reconcileable effect `Exhausted` like any other.
   So under this option the same exhaustion ends `Exhausted` when nothing
   crashed and `Indeterminate` when the worker was lost. The option was put
   to the owner as "closest to what the lost worker would have said" without
   that exception; section 8 holds the question it leaves.
2. **Always `Indeterminate`.** One rule, the lost-row rule. Costs an
   operator decision for every crash in this window, including a read-only
   failure the row records in full.
3. **Always `Exhausted`.** One rule, and it calls a reconcileable effect
   failed where its own doctrine says unknown.

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
| `Exhausted`, read-only, idempotent or non-idempotent effect | `Exhausted`, code and detail from the row (below), attempts from the row | the entry follows the row (`settle_claim_from_row`) |
| `Exhausted`, reconcileable effect | `Indeterminate` | the entry follows the row |
| `Cancelled`, and the wind-down fence applies (run winding down, kind not exempt) | `Cancelled`, as today | as today |
| `Cancelled` otherwise | `Indeterminate` | the entry follows the row |
| `Dispatched` | `Indeterminate`: the row says the call succeeded, and its result reached nobody | the entry follows the row |

**Reading an `Exhausted` row.** The row's last failure line is the line of
the write that exhausted it, and each exhausting site writes a known shape:

| Site | Line on the row | What its worker tells the run | What is rebuilt |
|---|---|---|---|
| `record_attempt_failure` | `code: detail` | `Exhausted(code, detail)` with the collaborator's reason | `Exhausted(code, detail)`, no reason |
| `retry_ambiguous`, worker lost | `dispatcher lost after started` | `Exhausted(dispatcher-lost-after-started, "the retry budget was spent recovering ambiguous attempts")` | the same |
| `retry_ambiguous`, after a reconciler's "not executed" | `code: message` | `Exhausted(code, …)` | reconcileable only: `Indeterminate` |
| `settle_unknown_finding` | the finding's line (`reconciliation outcome unknown`, or the mismatch line) | `Indeterminate` under `reconciliation-exhausted`, or `effect-outcome-mismatch` | reconcileable only: `Indeterminate` under the same code |

So: the worker-lost line maps to its code and message; any other line is
split at its first `": "` into code and detail when the part before it is
non-empty and has no whitespace; a line of neither shape, or no line, is
told under `ticket-row-settled` with the line as detail. The two finding
lines become named constants paired with their codes, shared by the site
that writes them and the reader, as `WORKER_LOST` already is. A
reconcileable effect whose row carries neither finding line parks under
`reconciliation-exhausted`. For a reconcileable effect the rebuilt answer
differs from the lost worker's in the first three rows (section 8).

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
- **The code of an indeterminate park from a finished row.** The
  reconcileable `Exhausted` row parks under `reconciliation-exhausted`, the
  code a spent reconciliation budget already parks under. `Dispatched` and
  live `Cancelled` rows park under `ticket-row-settled`, the fleet's
  existing word for "the entry follows a finished row", new as an effect's
  indeterminate code.
- **How the exhausted code is read from the row's line** (the table in
  section 5), and the fallback code `ticket-row-settled` for a line of no
  known shape.
- **No reason is invented.** The repaired outcome carries none.
- **A lost row on a live run gets no wait.** Nothing delivers for it.

## 6. Tests

`crates/rakka-agent/tests/dispatch_owed_outcome.rs` (the two reproductions
become the first two) and the pinned #134 test in
`dispatch_finished_row.rs`:

- an `Exhausted` row under a re-claim is not invoked; the run is told, per
  class; the entry follows the row;
- the same through the reconciler's `Unknown` arm with a reconcileable
  effect, which the issue has only by reading, and through its mismatch
  arm, under that arm's code;
- `record_attempt_failure` and `retry_ambiguous`, each crashed after its
  outbox write and after its fleet write, and `settle_unknown_finding`
  crashed after its outbox write (it has no later window): the run is told
  what section 5's table rebuilds;
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

## 8. Open with the owner

Decision 1 was chosen on a description that left out one thing: an
uncrashed worker tells a reconcileable effect `Exhausted` at two of the
three sites that spend its budget. Under the rule as chosen, a reconcileable
exhaustion ends `Indeterminate` only when the worker was lost.

- **1a, as chosen.** Reconcileable: always `Indeterminate` from a finished
  row. The issue's suggestion, and the cautious side. Crashed and uncrashed
  differ.
- **1b, follow the row for every class.** A `code: detail` line or the
  worker-lost line rebuilds `Exhausted`; a finding line rebuilds
  `Indeterminate` under its code. Crashed and uncrashed agree at every
  site, less the reason. A reconcileable row whose line has no known shape
  still parks.
- **1c, make the uncrashed sites park a reconcileable effect too.** A
  behaviour change where nothing crashed, and outside this issue.

Sections 5 and 6 are written for 1a and change in the two tables'
reconcileable rows under 1b.
