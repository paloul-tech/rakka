# Fenced live rows: the workflow tier leaves a witness, and the agent tier settles them (issues #211 and #209)

Status: design record for issues #211 and #209, for the owner's approval.
A builder check was run. Its two questions are answered here: the owner's
second ruling in §2, and my call 9 in §4.

## 1. What is wrong

Both issues meet at the same place: a fleet entry that a cancellation
request fenced (`AgentDispatcherFleet::request_run_cancellation`), which is
not finished, is held by no active lease, and whose outbox row is still
live (`Pending`, `Dispatching` or `Failed`).

**#211: an agent-only deployment never settles that entry.**
- A fenced entry is never claimable again.
- The agent dispatcher's per-run sweep (`settle_waited_effects` in
  `crates/rakka-agent/src/dispatch.rs`) acts on a live row only under a
  *finished* entry.
- Only the workflow tier settles it, through
  `AgentDispatcherWorker::refresh_run` or another `cancel_run_dispatches`,
  and `AgentRunEffectDispatcher` calls neither.
- So the run stalls: `Cancelling`, with its call `Ready`. This holds both
  for a failed attempt fenced mid-flight (shape A) and for a worker lost
  after `Started` under a lease that was fenced (shape B).
- A run whose dispatches are fenced without the run being cancelled stalls
  the same way.

**#209: the workflow tier settles the entry, but loses the attempt.**
- `settle_cancelled_effects` (`crates/rakka-agent-workflow/src/dispatcher.rs`)
  cancels every cancellable row, a `Dispatching` row included: an attempt
  whose worker was lost after `Started`, uncounted.
- The row becomes `Cancelled, 0`. A cancelled row keeps no reason, and a
  finished fleet entry is eventually pruned, so nothing durable says an
  attempt had started.
- The agent's sweep reads the row as "never started" and tells the run
  `Cancelled`, charged nothing, for every safety class. That includes a
  non-idempotent call, which specification 8.7 says must park
  (`WaitingForReconciliation`).

## 2. The owner's ruling (2026-11-23)

**A witness, and the agent tier settles.**
- The workflow tier keeps cancelling, but a `Dispatching` row it cancels
  keeps a durable mark that an attempt started.
- The agent tier reads the mark and parks the call for a decision.
  Read-only calls are cancelled, charged the attempt.
- The agent's per-run sweep runs that same settlement for a fenced entry no
  worker holds, so an agent-only deployment no longer needs `refresh_run`.
- **A second ruling, from the builder check:** a reconcileable call whose
  last attempt failed (a `Failed` row with attempts counted), cancelled
  under the fleet's fence on a cancelled run, **parks** too. The claim path
  treats that attempt as possibly executed and asks the reconciler first,
  but a fenced entry never reaches the claim path. Today such a row reads
  as "never started" and the run is told `Cancelled`. That includes a row
  whose last attempt was a reconciler `Unknown` burned on a live run.
- Rejected:
  - **The workflow tier leaving `Dispatching` rows alone:** a deployment
    using its cancellation without the agent tier would never settle them.
  - **Documenting `refresh_run` as a requirement:** an agent-only
    deployment that missed the rule would still stall, silently.

## 3. The change

### 3.1 The witness (#209): `rakka-workflow` and `rakka-agent-workflow`

- **A new inbox write in `rakka-workflow`:**
  `WorkflowInbox::record_outbox_cancelled_in_flight(&mut self, message_id, line) -> WorkflowResult<Option<WorkflowTelemetryEvent>>`.
  - For a `Dispatching` row, it counts the attempt in flight and marks the
    row `Cancelled`, in one write: the row's attempt count goes up by one
    and its last failure line becomes `line`. No retry is scheduled, and
    the budget plays no part: the row ends `Cancelled` even if the count
    reaches the budget.
  - For any other row it writes nothing and answers `None`, as
    `record_outbox_cancelled` does for a finished row.
  - It produces the same telemetry event as a cancel
    (`OutboxDispatchCancelled`).
- **A stable line in `rakka-agent-workflow`:** a public, documented
  constant `DISPATCH_CANCELLED_IN_FLIGHT_LINE`, with the value
  `"cancelled while an attempt was in flight"`. Both tiers read it, and it
  becomes durable on rows.
- **`AgentDispatcherWorker::settle_cancelled_effects`** cancels a
  `Dispatching` row through the new write, with that line. `Pending` and
  `Failed` rows are cancelled as before. Its two callers,
  `cancel_run_dispatches` and `refresh_run`, both get the witness.

### 3.2 The reader (#209): `rakka-agent`

In `owed_by_finished_row`, a `Cancelled` row whose last failure line is the
witness line means that an attempt started and its outcome is unknown.

- **Under the wind-down fence** (`fence_applies`), a cancelled row parks
  `Indeterminate` under `run-cancelled-after-attempt`, charged the row's
  count, when either holds:
  - **The row carries the witness.** This applies to an idempotent call
    (it cannot be retried once fenced), a non-idempotent call and a
    reconcileable call.
  - **The row counts attempts, and the call is idempotent (#206, as today)
    or reconcileable** (the owner's second ruling). A reconcileable call's
    counted attempt is one the claim path would have asked the reconciler
    about.
- **Every other cancelled row under the fence is told `Cancelled`**,
  charged the count. That covers a read-only call, a call that produces
  only its result (a model call or a goal evaluation, whatever the row
  says: the owner's #206 ruling), and a row that counts nothing.
- **Not under the fence**, the row is read as any cancelled row is today:
  parked `ticket-row-settled`.

In code, this widens `the_wind_down_parks(intent, started)` to take the
witness. It parks when the call does not produce only its result, and
either the witness is there and the class is not read-only, or
`started > 0` and the class is idempotent or reconcileable. Its two
callers, the claim's fence and the reader, keep agreeing. The table in
`owed_by_finished_row`'s doc gains the rows.

The claim's own fence never cancels a reconcileable row with counted
attempts: it asks the reconciler first. So on the claim path the new
reconcileable rule meets only one row: one the fence cancelled after a
"not executed", where the worker was lost before it told the run (§4,
call 8).

### 3.3 The agent sweep settles a fenced live row (#211): `rakka-agent`

In `settle_waited_effects`, take a waited ticket whose entry is fenced
(`cancellation_requested`), not finished, held by no active lease at `now`,
and whose row is live. The sweep settles it as `refresh_run` would:
1. A `Dispatching` row is cancelled through the witnessed write. A
   `Pending` or `Failed` row is cancelled with the reason
   `dispatch-cancelled`, `refresh_run`'s.
2. The entry is finished:
   `self.fleet.finalize_run_cancellations(&run_id, &[effect_id])`.
3. The row and the entry are read again, and the existing arms tell the run
   in the same pass. On a winding-down run that is the fenced-`Cancelled`
   arm, which reads the row through §3.2 at once. On a live run it is the
   general arm, which waits a lease after the entry finished, as it does
   today.

To support this:
- **The cheap look** at the top of `settle_waited_effects` counts such a
  ticket as owed. Today it returns early for every live row under an
  unfinished entry.
- **`WaitedTicket`** gains what the sweep needs, either the entry's
  `awaits_cancellation_settlement(now)` or a `held` flag. Which one is the
  builder's choice.
- **An entry under an active lease** is left to its worker, as the workflow
  tier leaves it.
- **A row the run put back is not the ticket's history** (#153's rule; §4,
  call 9). When the entry does not index the row
  (`AgentDispatchEntry::indexes_row`), the row was scheduled again after
  the ticket's own row was lost. The sweep cancels it with nothing
  witnessed, finishes the entry, and parks the call `ticket-row-missing`,
  as the claim path's `conclude_on_replaced_row` does. Reading such a row's
  count of 0 as "never started" would tell a non-idempotent call
  `Cancelled` though its lost row may have been `Dispatching`.
- **The agent dispatcher's inbox writes** go through `write_outbox`, with
  its re-application on a lost compare-and-set (#81). The witnessed cancel
  becomes a new `InboxWrite` variant.

## 4. Calls made in the design (each the owner's to reverse)

1. **The witness is on the row, not the fleet entry.** The row is the
   authority on an effect, and a finished fleet entry is pruned (#145).
2. **The witness counts the attempt in flight, with a line, rather than
   adding a field.** No schema change. The count is what the run is
   charged, and a line is how the agent tier already recognizes what a
   finished row means (`owed_by_exhausted_row`).
3. **One write for the witness**, so no crash can leave the count without
   the cancel or the reverse.
4. **A witnessed lost attempt parks for every consequential class,
   idempotent and reconcileable included.** An idempotent call cannot be
   retried once the fleet has fenced it, and the sweep does not ask the
   reconciler, so a person decides. #206's code
   (`run-cancelled-after-attempt`) is reused, so no new error code is added.
5. **The sweep settles with no grace period**, as `refresh_run` does: no
   active lease is enough. A worker whose call outlived its lease may still
   finish. Its writes then find the row finished, as they do after
   `refresh_run` today.
6. **On a live run fenced at the fleet without a run cancel**, the settled
   row is read as today's general arm reads a cancelled row: after a lease,
   parked `ticket-row-settled`. Today such a run stalls.
7. **The sweep settles per waited ticket,** not for the whole run as
   `refresh_run` does. A ticket the run no longer waits on is left to the
   workflow tier.
8. **One crash window now parks where it used to cancel.** On a
   winding-down run, a claim told "not executed" about a reconcileable call
   cancels the row and then tells the run `Cancelled`. A worker lost
   between the two leaves a cancelled row that counts attempts. The
   reader, under the owner's second ruling, parks it, where it used to tell
   `Cancelled`. Telling the two apart would need the fleet entry's code,
   and a finished entry is pruned. Parking is the conservative reading. It
   costs a person's decision about a call the reconciler had found not
   executed.
9. **The sweep keeps #153's rule** that a row the run put back is not the
   ticket's history (§3.3), rather than matching `refresh_run`, which
   ignores the row's scheduling stamp. This came from the builder check.
   The claim path parks such a row `ticket-row-missing`, and so does the
   sweep.

## 5. What a deployment sees

**#211, an agent-only deployment that fences at the fleet:** the run ends
without `refresh_run`, with the outcomes `refresh_run` gives today, and
with #209's fix for a lost attempt.
- **Shape A** (a failed attempt):
  - idempotent or reconcileable: `Indeterminate`,
    `run-cancelled-after-attempt`, charged 1;
  - read-only: `Cancelled`, charged 1, and the run is `Cancelled`.
- **Shape B** (a lost attempt):
  - non-idempotent, idempotent or reconcileable: parked
    `Indeterminate`, `run-cancelled-after-attempt`, charged 1, and the run
    rests at `WaitingForReconciliation`;
  - read-only: `Cancelled`, charged 1.

**#209, through `cancel_run_dispatches` or `refresh_run`:** the lost attempt
gets the shape B outcomes. Today it is `Cancelled`, charged 0, for every
class. A reconcileable call with a failed attempt, cancelled the same way,
parks as in shape A. Today it is `Cancelled`.

**A row the run put back under a fenced entry** is parked
`ticket-row-missing`.

**Durable:**
- A `Cancelled` outbox row can now carry the witness line as its last
  failure, with a count that includes the attempt that was in flight.
- No field, status or schema version is added. One stable line string is
  added.
- A deployment reading a cancelled row's count sees the attempt in flight
  counted.

## 6. Mixed builds and rollback

- **An earlier workflow tier with this agent tier:** no witness is written,
  so its cancellation still reads as "never started" (#209 stays for rows
  it cancels).
- **This workflow tier with an earlier agent tier:** a witnessed row counts
  1.
  - An earlier reader parks an idempotent call (#206).
  - It tells other classes `Cancelled`, charged 1. That is still short of
    specification 8.7 for a non-idempotent call, but better than charged 0.
  - It tells a reconcileable call with a failed attempt `Cancelled`, as
    today.
- **Rolled back:** witnessed rows stay, and an earlier build reads them as
  above.

## 7. Tests

- **`rakka-workflow`:** the new write.
  - A `Dispatching` row becomes `Cancelled`, with one more attempt and the
    line.
  - `Pending`, `Failed` and finished rows are untouched, and the write
    answers `None` for them.
  - A count at the budget still ends `Cancelled`, not `Exhausted`.
- **`rakka-agent-workflow`:**
  - `cancel_run_dispatches` over a `Dispatching` row writes the witness;
    `refresh_run` over a fenced, lapsed `Dispatching` row does too.
  - `Pending` and `Failed` rows are cancelled as before.
- **`rakka-agent`**, in a new file `dispatch_fenced_live_row.rs`:
  - #211 shapes A and B on the agent tier alone: one pass ends the wait,
    for each class above.
  - #209 through `cancel_run_dispatches`, then a pass: idempotent and
    non-idempotent park, and read-only is cancelled, charged 1.
  - A model call's lost attempt under the witness is cancelled.
  - A reconcileable call with a failed attempt, fenced at the fleet on a
    cancelled run, parks. This holds through the sweep, and through
    `cancel_run_dispatches` followed by a pass.
  - A row the run put back under a fenced entry is parked
    `ticket-row-missing`.
  - An entry under an active lease is left alone.
  - A live run fenced without a cancel parks after a lease.
  - The crash window of §4, call 8: the reconciler says "not executed" on a
    winding-down run, and the worker is lost after the cancel and before
    the tell. The call parks.
- **Breaks:**
  - no witness written;
  - the reader ignores the witness;
  - the reader drops the reconcileable rule;
  - the sweep never settles;
  - the sweep settles under an active lease;
  - the sweep ignores the scheduling stamp;
  - the cheap look skips the ticket.

## 8. Documents

- `docs/rakka-agents.md`: the fleet cancellation passage (around `:337`)
  and the wind-down fence bullets:
  - an agent-only deployment needs no `refresh_run` for a fenced run;
  - a lost attempt under a fleet cancellation parks.
- `docs/rakka-compatibility.md`: one entry for both issues, after #212's.
  It covers the new line, the counted attempt, and mixed builds.
- `CHANGELOG.md`: a bullet under Fixed.
- Rustdoc:
  - `settle_cancelled_effects`, `cancel_run_dispatches`, `refresh_run` and
    `request_run_cancellation`: a `Dispatching` row is cancelled with the
    witness;
  - the new write and the line constant.

## 9. Left to the builder

- Where the sweep's new arm sits among the existing ones, and how it reads
  the row and entry again after settling.
- The `WaitedTicket` field (§3.3).
- The new `InboxWrite` variant's name.
- The test fixtures for fencing at the fleet: the review's probe in the
  ignored `tmp/2026-11-22-issue-206-review/zz_probe_fenced_live_row.rs` is
  a starting point.
