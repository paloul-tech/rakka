# A lost outbox row gets one answer, however it is reached (issues #153, #154)

Status: owner said "dive in with your picks" on 2026-11-16; the picks are
sections 4 and 5. Built (section 8). Size: a small change (one durable field, one check in the
claim path, one read in the wind-down fence, tests, three doc entries).

## 1. Intent

Issue #150 (PR #152) parks a run's effect as `Indeterminate` under
`ticket-row-missing` when a claim finds the ticket's outbox row gone. Its
review found two doors by which a lost row still gets another answer:

- **#153.** The park is a command the run handles, and a run that handles a
  command flushes every `Ready` effect again. A sibling's lost row comes back
  as a new `Pending` row with no attempts, and its claim runs it as a first
  attempt, a non-idempotent effect included.
- **#154.** `pump_run` and `register_run` fence a winding-down run before
  anything is claimed. The fence reads a `Ready` effect with no row as one
  whose flush never landed and tells the run `Cancelled`, which says nothing
  was invoked. For a lost row nobody can say that.

Both come from the same gap: nothing durable tells "this ticket once had a
row that a worker could have claimed" from "this ticket never had one". The
fleet entry is that witness. It exists only because a registration read a due
row, and it outlives the row.

## 2. Goals, non-goals, anti-goals

**Goals**

- A claim that finds a row other than the one its fleet entry was registered
  from treats the ticket as #150 treats a missing row: the run's effect is
  parked `Indeterminate` under `ticket-row-missing`, nothing is invoked, and
  the ticket is finished at both layers.
- The wind-down fence does not say `Cancelled` for a row-less `Ready` effect
  whose ticket has a fleet entry.
- A ticket with no fleet entry behaves exactly as today on both paths: a
  recreated row runs as a first attempt (nothing could have claimed the lost
  one), and the fence plants its tombstone and says `Cancelled`.

**Non-goals**

- A row that was put back to an older state of itself (a restore to a
  snapshot that already held the row). Its stamp is unchanged.
- The workflow tier's dispatcher. It shares the fleet entry and records the
  new field, and does not read it.
- Issue #134, and #150's deferred minors.

**Anti-goals**

- Parking a healthy ticket. The check must have no false positive from clock
  skew between nodes.
- A run left waiting on an effect nothing will ever claim.

## 3. Constraints

- The fleet record is one record every worker reads and writes whole.
  Mixed builds must not confuse each other, and a rollback must be clean.
- An outbox row carries no identity beyond its message id except
  `scheduled_at`, written once when the row is created and never changed by
  a retry.
- "One outcome, one code path": every indeterminate park goes through the
  park's delivery, segment and counter.

## 4. Approaches

**#153, telling a recreated row from the lost one**

1. **Stamp the fleet entry (chosen).** Registration records the row's
   `scheduled_at` on the entry; a claim compares it with the row it finds.
   Works across workers and batches. Equality of one value with itself, so
   clock skew cannot produce a false positive. Misses a row recreated in the
   same millisecond as the original was scheduled, and says nothing for an
   entry written before the upgrade.
2. **An in-pass guard.** Note a run's other row-less effects before telling
   it, and conclude their claims in the same pass. No durable change, and it
   closes only the same-batch case.
3. **The run stops flushing an effect it flushed once.** Needs a run write
   per flush to record that the sink accepted it, and a lost row that was
   never registered would then never come back: a silent wait where today
   the row heals itself.

Rejected early: comparing the row's `scheduled_at` with the entry's
`created_at` needs no new field, but the two are stamped by different nodes'
clocks, and skew would park healthy tickets.

**#154, the fence**

One sensible way, the issue's own: the fence reads the fleet entry for a
row-less `Ready` effect. The open point was whether a read-only effect
should still be cancelled, as the ordinary path does for an ambiguous
read-only attempt under a wind-down. Chosen: no. #150's park is the same for
every safety class, and the point of this change is that both halves of a
pass give the same answer.

## 5. Design

**The field.** `AgentDispatchEntry::row_scheduled_at:
Option<AgentTimestampMillis>` in `rakka-agent-workflow`, `serde(default)` and
skipped when `None`. `from_due_effect` sets it from the row. Nothing else
writes it. `AgentDispatchEntry::indexes_row(&OutboxEntry) -> bool` answers
whether a row is the one the entry was registered from: true when the entry
has no stamp, or the stamps are equal.

**Registration.** `upsert_from_due_effect` leaves an entry untouched when the
due row is not the one it indexes. The entry's attempt count is the last
thing the fleet learned from the lost row; a recreated row's zero must not
overwrite it.

**The claim path** (`execute_claim` in `crates/rakka-agent/src/dispatch.rs`).
After the stale-ticket and already-resolved checks, and before the ambiguity
and wind-down branches: if the fleet entry does not index the row, park the
effect through `park_indeterminate_with` under `ticket-row-missing`, with
the attempt count #150 uses (the larger of the run's and the entry's), and
end the ticket at both layers under the same code. The recreated row becomes
a cancelled row, so a later flush lands on a finished row.

**The fence** (`fence_run`). For a `Ready`, non-exempt effect with no row it
reads the fleet record (once per fence, only in this case) and looks up the
ticket's entry:

- no entry: today's tombstone and `Cancelled`;
- an entry that is not finished: nothing. The claim path owns it and parks
  it;
- a finished entry: no claim will come, so the fence parks the effect itself
  under `ticket-row-missing`.

### Calls I made, for you to check

- **No exemption for a ticket nobody ever claimed.** An entry whose
  `last_fencing_token` is 0 proves no worker held the ticket, so a recreated
  row could run safely. #150 parks a missing row without asking that, and
  the fleet record may itself be the store that was restored. Same rule
  everywhere: a ticket with an entry and without its row is parked.
- **A stamp is never adopted later.** An entry written before this build has
  none and stays that way until it finishes; such entries behave as today.
- **The same code at the fleet.** The entry of a replaced row is finished
  `Cancelled` with `last_error_code` `ticket-row-missing`, like a missing
  row's. An operator reads one code for one cause.
- **A finished entry is parked, whatever it finished as.** An entry finished
  `Cancelled` under `run-cancelled` records that the ticket was fenced, so
  `Cancelled` would be true for it. One rule is simpler than a table of
  entry codes, and the cost is an operator decision in a case that needs a
  crash and a store loss together.
- **An entry that was finished and pruned** is absent and reads as never
  registered. Nothing durable is left to say otherwise.

## 6. Tests

`crates/rakka-agent-workflow/tests/dispatcher_fleet_rows.rs` (or a
neighbour): registration records the stamp; a registration from a different
row leaves the entry as it was; an entry without the field decodes and
behaves as before. The pinned record bytes in `dispatcher_fleet_prune.rs`
gain the field.

`crates/rakka-agent/tests/dispatch_missing_row.rs`:

- two lost rows of one run, one pass: both parked, nothing invoked (replaces
  the pinned test);
- a row the run put back before the claim arrived is parked, through
  `claim_and_execute` and through `pump_run`, and the attempt count survives
  the re-registration;
- a lost row that was never registered comes back and runs as a first
  attempt;
- an entry with no stamp behaves as before;
- a crash between telling the run and ending the replaced ticket is repaired
  on the next lease;
- a cancelling run that is pumped is parked, not cancelled (replaces the
  pinned test); with `register_run` alone it is left for the claim;
- a cancelling run whose flush never landed is still cancelled by the fence;
- a cancelling run whose ticket's entry is finished is parked by the fence.

Each guard is falsified once before the work is called done.

## 7. Documentation

`docs/rakka-compatibility.md` (a dated entry; the #150 entry's "does not
cover" paragraph and table rows point to it), `docs/rakka-agents.md` (the
dispatcher section), `CHANGELOG.md`.

## 8. As built

Where the build moved from the sections above, and what it chose where they
left a choice.

- **What the stamp is.** A row's `scheduled_at` is the instant it was
  written only when its effect names no due time; an effect that names one
  is stamped with that time, whenever the row is written, so a second row
  for it carries the same stamp. Every agent-tier ticket is scheduled with
  no due time (`AgentRunEffect::to_workflow_effect`), so the check holds
  where it is read. Workflow-tier effects name one; that tier records the
  field and does not read it. Found when the first fleet test, written over
  the shared helper's dated effect, could not tell the rows apart. Pinned by
  `a_row_scheduled_for_a_stated_time_is_not_told_from_its_replacement`,
  which also stands in for the same-millisecond case: the agent fixture's
  clock cannot be set back to build that one directly.
- **Names.** `conclude_on_replaced_row` is the claim-path arm,
  `claim_indexes_row` its check, `attempts_counted` the attempt count both
  lost-row arms and the fence share. `ParkedTicket` gained `ReplacedRow`
  (both layers, under `ticket-row-missing`). `park_indeterminate_with` was
  split: `tell_indeterminate` is the delivery, the segment and the count,
  and the park under a claim ends its ticket afterwards. The fence calls
  `tell_indeterminate` directly, so its park is the same outcome, segment
  and count as every other.
- **An entry fenced by a cancellation request is treated as finished** by
  the fence. `request_run_cancellation` makes an entry never claimable
  again without finishing it, so "an entry that is not finished" (section
  5) was the wrong test for "a claim will come". The agent tier never makes
  that request itself; a deployment can, through the workflow tier's
  worker. Without this the fence would have left such a run waiting where
  it used to end it.
- **The fence reuses the registration's read.** `fence_run` takes
  `fleet_current`: when the registration just before it read the fleet
  record, the fence reads nothing more.
- **A crash between telling the run and finishing the replaced ticket** is
  repaired by the ordinary path, not by this arm: the next lease finds the
  effect resolved and settles the ticket `intent-already-resolved`.
- **A claim whose entry is no longer in the record** (finished and pruned
  under a lapsed claim) reads as indexing its row and takes the ordinary
  path, whose settle is answered `claim-fenced`. No test builds it.
- **Tests.** The fleet tests are a new file,
  `crates/rakka-agent-workflow/tests/dispatcher_fleet_row_stamp.rs`. The
  record's bytes are pinned twice in `dispatcher_fleet_prune.rs`: what this
  build writes, and that the record `ff9a495` wrote is read and written
  back byte for byte. `dispatch_missing_row.rs` has the cases of section 6
  and one more, the cancellation-fenced entry. The two-rows test needs no
  push of the clock: the fixture's time moves on its own between the first
  flush and the second.
- **Documents.** As section 7. The issue #150 entry in
  `docs/rakka-compatibility.md` keeps its text and gains one sentence that
  points to the new entry.

**Falsified**, one mutation at a time, each caught by the tests named
(`scratchpad` runner, both test files, 13 of 13 caught):

| Mutation | Fails |
|---|---|
| Registration records no stamp | twelve tests, the fleet's `a_registration_records_when_its_row_was_scheduled` first |
| A registration from another row overwrites the entry | `a_registration_from_another_row_leaves_the_entry_as_it_was`, `a_pumped_run_whose_row_was_put_back_keeps_the_attempts_the_fleet_counted` |
| `indexes_row` true for every row | six, among them `of_two_lost_rows_of_one_run_both_are_parked` |
| An entry with no stamp indexes nothing | `an_entry_with_no_stamp_indexes_any_row_and_registers_as_before`, `an_entry_registered_before_the_stamp_runs_the_row_put_back_as_before` |
| The claim does not check the row | five, among them `a_row_the_run_put_back_before_the_claim_is_parked_not_run` |
| The check yields to the wind-down branch | `a_row_put_back_before_a_cancellation_is_parked_not_cancelled` |
| The replaced row's ticket settled under `indeterminate` | four, at the fleet entry's code |
| The replaced row left unfinished | `a_row_the_run_put_back_before_the_claim_is_parked_not_run`, `of_two_lost_rows_of_one_run_both_are_parked` |
| The fence cancels a lost row a worker can still claim | `a_cancelling_run_that_is_pumped_is_parked_too`, `the_fence_alone_leaves_a_lost_row_to_the_claim` |
| The fence tells nothing for an entry no claim will come for | both fence-park tests |
| The fence reads a never-registered ticket as lost | `a_cancelling_run_whose_ticket_was_never_registered_is_still_cancelled` |
| The fence's park ignores the fleet's attempt count | `a_cancelling_run_whose_ticket_is_finished_at_the_fleet_is_parked_by_the_fence` |
| The fence ignores a cancellation-fenced entry | `a_cancelling_run_whose_ticket_is_fenced_at_the_fleet_is_parked_by_the_fence` |
