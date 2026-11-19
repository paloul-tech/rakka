# A call the run cancels before it is sent: its gate and its attempts

Issues #133 and #132. Both were found while building the fix for #126 and
#127 (PR #129), and both are about the same few lines.

## 1. What was wrong

A call that is still `Pending` has provably not reached the outbox, so a run
that stops dispatching cancels it in place. That is the wind-down fence
(`AgentLoopState::fence_unsent_effects`). It runs when a call fails
definitively, on a cancel, when an operator schedules a compensation, and
when the task accepts a handoff.

The fence set the call `Cancelled`, settled its delegation, handoff or
workflow cell, and did nothing else.

**The gate (#133).** A call that waits behind an approval or an
authorization is `Pending` until it is granted, so it is exactly what the
fence cancels. Its gate stayed waiting. Only a cancel closed it, because a
cancel closes every such gate.

**The attempts (#132).** Every call reserves its attempt bound when it is
committed, and the reservation is settled where the call's outcome is
applied. The fence is the one place a call is resolved anywhere else, and it
settled nothing.

**No fence at all.** A run that ends without winding down (its result
accepted, its rejections or its budget spent) ran no fence. Neither issue
names this; it is the same defect by another door.

The ways in. Rows 1, 2, 3 and 5 fail as tests on `main`. Row 4 is a unit
test that fails when the three lines it covers are put back as `main` has
them.

| | What happens | What is left |
|---|---|---|
| 1 | One turn calls a gated tool and a tool that fails. The failure cancels the gated call. | The run ends `Failed` with the call's approval `Open`, and the call's attempts reserved. #133 as filed. |
| 2 | The same, with a third call still out when the failure lands. | The run winds down with the approval open. A grant is accepted and stored for the cancelled call. A denial is accepted and rewrites the cancelled call as `Failed`; an expiry takes the same path. |
| 3 | An ungated call's outcome is unknown, and an operator schedules its compensation while a gated call of the same turn waits. | The gated call is cancelled and its approval stays open through the wind-down and past the end. |
| 4 | A handoff send is re-sent after `ConfirmedNotExecuted`, the send is gated, and the task's acceptance arrives first. | The run ends `HandedOff` holding the re-send's fresh approval. |
| 5 | A review of the goal is open when the run proposes its result, and the task accepts it. A turn that makes no call is not held for an evaluation. | The run ends `Completed` holding the evaluation `Pending`, which an ended run never sends, behind a review nothing can answer. |

On an ended run nothing answers such a gate. `ResolveCheckpoint` is refused
`run-terminal` unless the gate is bound to a memory promotion or a claim
append, the timer sweep skips it, and `Cancel` is refused.

For the attempts, I measured where the counter goes wrong, and did not rely
on reading. With a check at the point every run write is sealed, across the
`rakka-agent` suite (1,668 tests on `main`) the reserved attempts stop
matching the calls still out in one place only: the write in which the
fence cancels a call, by exactly that call's bound.

The leak matters only under an `EffectAttempts` limit. Reproduced on `main`:
under a limit of four, a run that consumed three and holds one leaked is
refused the one attempt its compensation needs (`run-redispatch-unaffordable`).

## 2. Approaches

For the gate:

1. **Close it where the fence cancels its call, and run the fence at every
   end.** The gate never outlives the call it gates.
2. **Close it when the run ends.** One place, but the gate still waits
   through the wind-down, where a decision is applied to a call that was
   cancelled (row 2).
3. **Let an ended run answer it,** as it answers a promotion's gate. The
   decision would be for a call that was never sent and never will be.
4. **Leave the record and filter the point query.** One reader stops seeing
   it. The record, and every other reader, still says a person is being
   waited for.

Chosen: 1.

For the attempts, the issue suggests the fix: the fence settles each call it
cancels as nothing made, everything released. That is what the outcome path
does for a cancelled call that never dispatched. I considered sending each
fenced call through the outcome path itself; it reaches the same arithmetic
through a function that also moves the run's status and phase, for no gain.

## 3. The change

`crates/rakka-agent/src/loop_runtime.rs`:

- `fence_unsent_effects` settles each call it cancels: its whole
  reservation is released and none of it is consumed.
- `strands` says whether a waiting approval or authorization can still have
  a decision applied to it. It cannot when the call it is bound to is held
  and is no longer `Pending`, or when the run has ended and the call is not
  a memory promotion or a claim append. A reconciliation checkpoint is never
  asked.
- `close_stranded_gates` closes every gate `strands` names, as a cancel
  closes one: status `Cancelled`, kept on the record.

`crates/rakka-agent/src/run.rs`:

- `fence_unsent` runs the fence and the close together and records each
  close. A failed call, a scheduled compensation and an accepted handoff
  call it where they called the bare fence.
- `terminate` calls it. Every end now cancels what the run never sent and
  closes what the ended run could not answer.
- `cancel` is unchanged in what it does to gates and in what it records. It
  still closes every approval and authorization itself. It gains the
  release, because it runs the same fence.
- `schedule_compensation` fences before it reserves.
- `AgentRun::strands` asks the same question of a run that is winding down
  or has ended. Four readers share it: the settle pass's repair
  (`needs_gate_close`, `close_what_nothing_can_answer`), the decision door,
  the timer sweep and the snapshot's `next_wake`.

No durable field, status, error code, event kind or metric is added.

## 4. My calls

1. **The gate closes in the transition that cancels its call,** not at the
   end. That answers the issue's question about a grant in the window: there
   is no window. A decision that arrives after is refused
   `checkpoint-already-resolved` while the run winds down and `run-terminal`
   once it has ended, which is what a decision gets after a cancel.
2. **The close is recorded as a cancel's is:** `resolve-checkpoint`, outcome
   and reason `cancelled`, source `deterministic-policy`, before the run's
   end event. That is under the `Version2` vocabulary; `Version1`, the
   default, writes no `resolve-checkpoint` at all. The product doc said such
   a gate "gets no close event; the run's end event closes it for readers".
3. **Every end runs the fence.** This is new behaviour beyond the two
   issues: a run that completes, or ends on its budget, no longer keeps a
   goal evaluation `Pending`. It is `Cancelled`, its review is closed and
   its attempts are released. The code already said so in two comments
   ("everything else was fenced at the terminal transition"); it was not
   true. The four kinds the fence exempts are still sent after an end, as
   before.
4. **What is closed is asked of the record, not of the fence.** The fence
   cancels; a separate question, asked right after, finds the gates that now
   gate nothing. The same question is the repair's test, so the two cannot
   disagree.
5. **A record an older binary wrote is repaired, for the gate.** The settle
   pass closes it once. On a run still winding down the close is recorded.
   On a run that has ended the pass corrects the record and nothing else:
   no event is added after the end, and `updated_at` is not moved. I did
   not add a filter to the point query: until a pass touches the record,
   the snapshot lists the gate, because the record holds it.
6. **Until that pass, nothing acts on such a gate.** A command's own
   transition runs before the pass that follows it, so a decision or a
   timer can reach an older record first. The decision is refused as one
   for a closed checkpoint, and the timer sweep and `next_wake` leave the
   gate out. Neither writes anything, so a refusal still changes nothing.
7. **A leaked reservation in a record an older binary wrote is not
   repaired.** Nothing marks a cancelled call as fenced, so a repair would
   have to recompute the counter from the calls still out and tie the two
   together for good, and it would have to run in every door that reserves
   as well as in the settle pass, for the reason in call 6. Nothing is
   stuck. What it costs: under an `EffectAttempts` limit such a record can
   still be refused a reservation it could afford. That is a compensation
   or a workflow cancel while it winds down, a memory promotion or a claim
   append inside the post-terminal window, and the re-send of a parked
   promotion or claim append, which can come at any time after the end.
   **This one is yours to reverse.** The measurement in section 1 says the
   recompute is sound today.
8. **A compensation fences before it reserves,** so it can spend what the
   calls it cancels had reserved. The fence alone would not have helped the
   decision that starts the wind-down, which reserved first. A compensation
   the run still cannot afford is refused as before, and the refusal changes
   nothing.
9. **The repair never asks a live run that is not winding down.** Nothing
   leaves such a gate on one, and its gate is its wait.

One consequence I did not choose and want named: of two gated calls whose
gates expire in the same timer sweep, the first expires and fails its call,
and that failure cancels the second call and closes its gate as cancelled.
Before, both expired and both calls were failed.

## 5. What a reader sees

| State | Before | Now |
|---|---|---|
| A failed call cancels a gated call | the gate waits, through the wind-down and on the ended run | the gate is `Cancelled` in that transition, with a close event under `Version2` |
| A grant, a denial or an expiry of that gate while the run winds down | applied: a grant stored for a cancelled call, or the call rewritten `Failed` | refused `checkpoint-already-resolved`; the timer acts on nothing |
| A compensation or an accepted handoff cancels a gated call | as the first row | as the first row |
| A run ends at once with a goal evaluation waiting behind its gate | the run ends holding the evaluation `Pending` and the gate open | the evaluation is `Cancelled` and the gate closed, before the end event |
| Two gated calls whose gates expire in one sweep | both expire, both calls `Failed` | the first expires; the second call is `Cancelled` and its gate closed |
| The attempts of a call the fence cancels | reserved for the rest of the run's life | released; none consumed |
| A compensation scheduled while unsent calls hold attempts | may be refused though the run can afford it | can spend what those calls reserved |
| A cancel | closes every approval and authorization | the same, in the same order; and releases the attempts |
| The gate of a memory promotion or a claim append | not touched by a wind-down that is not a cancel; answered on an ended run | unchanged |
| A gate while its call is `Pending` on a live run | its wait | unchanged |
| A record an older binary left with such a gate | as the first two rows, for good | closed by the first settle pass; until then refused and not swept |

## 6. Tests

`crates/rakka-agent/tests/wind_down_fence.rs`, new, sixteen tests. A test
that reaches its record through commands alone ends by checking that the
attempts the run holds reserved are those of the calls it still waits on.

- A failed call closes the gate of the call it cancels, for an approval and
  for an authorization; the close is recorded once, before the end.
- The same with another call still out: the gate closes in the failure's own
  transition, before the wind-down's wait event, and a grant and a denial
  are each refused and change nothing.
- A compensation closes the gate of the call it cancels, in the decision's
  own transition.
- Two gates due in one sweep: one call expires, the other is cancelled.
- A run that completes cancels its unsent evaluation and closes its review,
  before the end event; a late decision is refused `run-terminal`.
- A cancelled call's attempts are free for the compensation that follows,
  under a limit that refuses it on `main`.
- A compensation can spend what its own wind-down releases.
- A compensation the run cannot afford changes nothing.
- A cancel releases the attempts of the call it cancels.
- Three records as an older binary left them (ended with a cancelled call's
  approval, winding down with one, completed with an unsent review): each is
  closed by one settle pass, once. On the two ended ones the end, the
  decision stream and `updated_at` do not move.
- A decision that reaches such a record before any pass is refused and
  changes nothing; an expiry that reaches it first changes nothing and
  reports no wake.
- Ninety-six forged records: each standing of the run, each kind of gate,
  seven of the gated call's eight statuses (all but `indeterminate`, which
  is the reconciliation park's) or no call at all, the gate open or
  escalated. All are at rest in two passes; a live run's gate is never
  closed for it; an ended run is left with no waiting gate and no unsent
  call.
- The step that closes a gate measures no turn.

One unit test in `run.rs`, for the handoff: an accepted notice closes the
gate of a re-send it cancels, with nothing else out and with another call
still out.

Sixteen of the seventeen fail on `main`: fifteen when the file is run
against `main`'s two source files, and the unit test when the three lines
it covers are put back. The one that passes there, the refused
compensation, holds what must not change.

One existing test changed. `an_ended_runs_next_wake_reads_only_what_its_sweep_acts_on`
built the defect through commands. It now starts from a rewritten record,
and rewrites it once more after the promotion's own pass has closed the
approval, so it still holds what it was written for: with a skipped
approval and a swept gate both waiting, `next_wake` is the gate's deadline
and not the approval's earlier one.

Twenty-five deliberate breaks of the final code were run against these
tests and the promotion, claim, checkpoint, goal and query tests.
Twenty-four each fail at least one test. The one that does not cannot be
caught by any record a binary writes: the end passing "not ended" to the
close. The fence that runs just before has already cancelled every unsent
call, so the two answers only differ for a gate whose call the record does
not hold. The breaks that close too much (every gate at a wind-down, a
promotion's gate at the end) are caught by tests that were already there.

The first run, before the review, caught thirteen of eighteen. The five
that survived showed where a test could not tell my change from the repair
that follows it one pass later, and four of them are caught now: the
failed call's and the compensation's close are tied to their own
transition, the forged records include a gate with no call, and the
repair's step is checked against the turn metrics.

## 7. The review

A fresh reviewer read the first build alone and found no defect on a record
this binary writes. What it did find, each with a test that showed it:

- **The repair re-dated an ended record.** It set `updated_at`, and its
  recorded close moved it again when the outbox was flushed. A record with
  no terminal stamp is backfilled from that clock, so the repair could
  reopen its memory window. The repair of an ended record now writes no
  event and leaves the clock alone (call 5).
- **A decision or a timer could reach an older record before the repair**
  and be applied as on `main`. My first answer closed the gate inside the
  two doors; that broke two existing tests which hold that a refused
  decision changes nothing and that a timer leaves an ended run's other
  checkpoints alone. The doors now refuse and skip without writing (call 6).
- **My rewrite of the one existing test no longer tested its subject.** Once
  the promotion's pass closed the approval, nothing held that `next_wake`
  leaves out a skipped earlier deadline. Restored, and held by two breaks.
- **One line had no test:** the run's pending checkpoint after a close.
- **Statements that were not true.** That an ended run reserves only inside
  its memory window, which was my reason for call 7 (corrected there). That
  only an older binary leaves an ended run a waiting checkpoint the timer
  skips (see section 8). That every end cancels every unsent call (four
  kinds are exempt). That a late decision is always
  `checkpoint-already-resolved` (it is `run-terminal` after the end). That
  the close is in the stream (only under `Version2`). And two counts.

## 8. Found, not done

- **An ended run can hold a goal evaluation `Ready` for good.** An ungated
  evaluation that was sent before the run ended at once is not the fence's
  to cancel: it may have been invoked. The dispatcher then cancels it, and
  the ended run refuses that result `run-terminal`, because an ended run
  takes results only for the four exempt kinds. The record lists the call
  among its pending effects for good and keeps its reserved attempts.
  Reproduced on `main` and on this branch.
- **An ended run can hold a goal evaluation's reconciliation checkpoint
  that nothing answers.** The evaluation's outcome is unknown while the
  run's proposal is with the task, and the task accepts. The run ends
  `Completed` with the checkpoint open, a decision for it is refused
  `run-terminal`, and the timer skips it. The reviewer's finding;
  reproduced on `main` and on this branch. It is #133's title with another
  kind of checkpoint, and #123's family (a run that ends at once with
  something outstanding).
- **A gated compensation or workflow cancel never opens its gate** (#128,
  open). Unchanged.
- A leaked reservation in a record an older binary wrote (call 7).

The first two are drafted for the owner and not filed.
