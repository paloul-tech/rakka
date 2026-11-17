# A dispatch ticket whose outbox row is gone is settled, and its run is told (issue #150)

Status: awaiting approval. Size: a small change (one arm of one function, one
helper's settle step, one test file, three doc entries).

## 1. Intent

In the agent tier's dispatcher (`rakka_agent::AgentRunEffectDispatcher`), a
fleet entry whose outbox row no longer exists is never settled. Every pass
that claims it fails with `outbox-entry-not-found`, the claims behind it in
the batch are left unexecuted, and the entry is claimed again on every lease,
for good. Issue #150 holds the reproduction.

The entry has to finish. And a run that is still alive and waiting on that
effect has to hear something, or fixing the loop only turns a loud failure
into a silent wait.

## 2. Goals, non-goals, anti-goals

**Goals**

- A claim on an absent row ends the claim: the fleet entry becomes
  `Cancelled` with `last_error_code` `ticket-row-missing`, the pass answers
  `Ok`, and the rest of the batch is executed.
- A run that still holds the effect unresolved, at the ticket's generation,
  is parked on it as indeterminate with the code `ticket-row-missing`, so an
  operator decides through the reconciliation checkpoint the run already
  opens for any indeterminate effect.
- Nothing is invoked and nothing is written to the missing row.

**Non-goals**

- Putting the row back. A run that handles a command before the claim arrives
  re-flushes its `Ready` effects and so recreates the row itself, as
  `Pending` with no attempt history; the claim then finds a row and runs it
  as a first attempt. That is today's behaviour and it is not changed here.
- The other callers of `settle_ticket_cancelled`. Each runs after the row was
  read in the same claim; a row deleted between that read and the write still
  fails the pass. Not reproduced, not in this change.
- The workflow tier, which already settles this case (issue #145).
- Issue #134 (a claim on an `Exhausted` row with an unresolved intent).

**Anti-goals**

- Telling the run the effect was cancelled. `Cancelled` means "fenced before
  any invocation could have happened", and the lost row was the only record
  of whether an attempt started.
- Re-invoking anything on the strength of a missing row.
- A run left waiting with nothing recorded on it.

## 3. Constraints

- Delivery is at-most-once below the outbox; the dispatcher's own passes are
  the retry. Whatever this arm does must be safe to repeat after a crash.
- A fleet settle is fenced on the claim and can answer `claim-fenced`. That
  is an expected answer here, as it is in `conclude_on_finished_row`, and not
  a failure of the pass.
- "One outcome, one code path": the comment on the non-idempotent arm of
  `recover_ambiguous` records why every indeterminate park goes through
  `park_indeterminate_with` (the segment, the links, the counter). This park
  goes through it too.
- `ticket-row-missing` is already a documented value of a fleet entry's
  `last_error_code` for this tier (`docs/rakka-compatibility.md`, the issue
  #81 entry). It becomes true; it is not a new string there. As an effect's
  indeterminate code on a run record it is new.

## 4. Approaches

1. **Settle the fleet entry and tell nobody.** The issue's suggestion and
   what the workflow tier does. Smallest. A live run keeps waiting on a
   `Ready` effect; if it later re-flushes the row, the row sits unclaimed
   behind the finished entry until an operator prune, and then runs as a
   first attempt.
2. **Settle the entry and park the effect as indeterminate.** Chosen by the
   owner on 2026-11-16. True for every safety class, since nobody can say
   whether an attempt started. Costs an operator decision even for a
   read-only effect.
3. **Put the row back from the run's intent and carry on.** Read-only and
   idempotent effects heal themselves, but a non-idempotent effect could be
   invoked a second time, so it would need the park of approach 2 as well.
   The most code, for a case that needs store damage to reach.

## 5. Design

All of it is in `crates/rakka-agent/src/dispatch.rs`.

**The absent-row arm of `execute_claim`** stops calling
`settle_ticket_cancelled`. Instead:

1. Read the run. Find the effect whose `dispatch_ticket_id()` equals the
   claim's `effect_id`. The ticket id carries the generation, so a match is
   the generation this ticket was for. (The row's payload, which the ordinary
   path uses to find the intent, is gone.)
2. **No run, no such effect, or the effect is not outstanding:** settle the
   fleet entry only, with `self.fleet.settle_claim_from_row(&claim, None)`,
   and count `pass.cancelled`. There is nobody to tell.
3. **The effect is outstanding:** park it through `park_indeterminate_with`,
   code `ticket-row-missing`, with a fixed message saying the ticket's row is
   gone and whether an attempt invoked the target cannot be established. No
   failure reason, no evidence. The park delivers, closes its segment and
   counts `pass.parked_indeterminate` as it does today.

**`park_indeterminate_with` learns to settle the ticket at the fleet only.**
Its last step is `settle_ticket_cancelled(.., "indeterminate", ..)`, which
writes the outbox row. For this caller that step is the fleet-only settle of
step 2 instead. How that is expressed (a parameter, a small enum, a split
into two functions) is the builder's choice; the existing callers must behave
exactly as they do now.

**The fleet-only settle** is one small helper used by steps 2 and 3:
`settle_claim_from_row(&claim, None)`, `claim-fenced` taken as "the claim is
over", any other error returned, `pass.cancelled += 1` when it settled.

**What the run does** is unchanged: `apply_effect_outcome` already parks any
effect kind on an `Indeterminate` outcome and opens the reconciliation
checkpoint in the same transition. A run that has ended refuses the result
(`Terminal`, or a stale-result refusal), and `deliver_outcome` already takes
those as the fence doing its job.

### Calls I made, for you to check

- **The run is told first and the entry is settled second**, the order the
  park already keeps. A crash between the two leaves the entry `Claimed`;
  the next lease finds the row still missing, the delivery is refused as
  stale, and the entry is settled. The other order would strand exactly the
  run this change exists to tell. The price: a worker whose lease lapsed can
  park an effect that another worker is running, if the run recreated the
  row in between. The run then holds "unknown, an operator decides" for an
  effect that ran, which is recoverable, where a silent wait is not.
- **The attempt number the park reports** is the count the fleet entry
  holds, which is the last the fleet learned from the row, falling back to
  the run's own count if the entry cannot be read there. The run keeps the
  larger of that and its own. It may undercount an attempt that was in
  flight when the row was lost; nothing left can say.
- **A winding-down run gets the same answer** as a live one. The wind-down
  fence (`fence_run`) cancels a `Ready` effect with no row when it runs
  first; whichever reaches the run first resolves the effect and the other
  is refused. No special case in this arm.
- **The entry's `last_error_code` is `ticket-row-missing`** in both branches,
  not `indeterminate`, so a parked and an unparked orphan read the same in
  the fleet and the park is read from the run.

## 6. Tests

A new file, `crates/rakka-agent/tests/dispatch_missing_row.rs`, over
`AuthorityFixture`, in the style of `dispatch_finished_row.rs`. The
reproduction is: a run with one tool ticket, `register_run`, the run's
workflow record deleted from the store, then `claim_and_execute`.

- **A live run is parked.** The first pass answers `Ok`; the entry is
  `Cancelled` with `ticket-row-missing`; the run's effect is `Indeterminate`
  with the code `ticket-row-missing` and the reconciliation checkpoint is
  open; the pass counts one cancelled, one parked, one delivered; the tool
  was never called; a second pass after the lease would have lapsed claims
  nothing.
- **The batch goes on.** A claim that follows the orphan in the same batch is
  executed in that pass.
- **No run, nobody told.** With the run's own record gone as well, the pass
  answers `Ok`, the entry is settled, and nothing is delivered.
- **A crash between the delivery and the fleet settle is repaired.** The
  fleet write refused once: the pass fails, the run is already parked, the
  entry is still `Claimed`. The next pass after the lease settles the entry
  and delivers nothing more.

Each guard is falsified once before the work is called done: with the fix
reverted the first test must fail with the issue's own error.

## 7. Documentation

- `docs/rakka-compatibility.md`: a dated entry. The pass no longer fails on
  an orphan; `ticket-row-missing` is now also an indeterminate code on a
  run's effect; nothing else moves, no schema version changes.
- `docs/rakka-agent-observability-catalogue.md`, wherever the effect
  indeterminate codes are listed, if they are.
- `CHANGELOG.md`.

## 8. Left to the builder

- The shape of the fleet-only variant of the park (section 5).
- How the "batch goes on" test puts two claims in one batch, and in which
  order the fleet hands them out.
- Whether the fleet entry's attempt count is readable from the dispatcher
  without a new public method; if it is not, use the run's own count and say
  so in the "as built" note.
- The exact message text of the indeterminate outcome.
