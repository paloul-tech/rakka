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
1,671 tests of `rakka-agent` the reserved attempts stop matching the calls
still out in one place only: the write in which the fence cancels a call,
by exactly that call's bound.

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
- `close_stranded_gates` closes every approval and authorization that still
  waits and that no decision can be applied to, as a cancel closes one:
  status `Cancelled`, kept on the record. A gate is stranded when the call
  it is bound to is held and is no longer `Pending`, or when the run has
  ended and the call is not a memory promotion or a claim append. A
  reconciliation checkpoint is never one.

`crates/rakka-agent/src/run.rs`:

- `fence_unsent` runs the two together and records each close. A failed
  call, a scheduled compensation and an accepted handoff call it where they
  called the bare fence.
- `terminate` calls it. Every end now cancels what the run never sent and
  closes what the ended run could not answer.
- `cancel` is unchanged in what it does to gates and in what it records. It
  still closes every approval and authorization itself. It gains the
  release, because it runs the same fence.
- `schedule_compensation` fences before it reserves.
- `AgentRun::needs_gate_close` and `close_what_nothing_can_answer` repair a
  record an older binary wrote, on the first settle pass that touches it.

No durable field, status, error code, event kind or metric is added.

## 4. My calls

1. **The gate closes in the transition that cancels its call,** not at the
   end. That answers the issue's question about a grant in the window: there
   is no window. A decision that arrives after is refused
   `checkpoint-already-resolved` while the run winds down and `run-terminal`
   once it has ended, which is what a decision gets after a cancel.
2. **The close is recorded as a cancel's is:** `resolve-checkpoint`, outcome
   and reason `cancelled`, source `deterministic-policy`, before the run's
   end event. The product doc said such a gate "gets no close event; the
   run's end event closes it for readers". It gets one now.
3. **Every end runs the fence.** This is new behaviour beyond the two
   issues: a run that completes, or ends on its budget, no longer keeps a
   goal evaluation `Pending`. It is `Cancelled`, its review is closed and
   its attempts are released. The code already said so in two comments
   ("everything else was fenced at the terminal transition"); it was not
   true.
4. **What is closed is asked of the record, not of the fence.** The fence
   cancels; a separate question, asked right after, finds the gates that now
   gate nothing. The same question is the repair's test, so the two cannot
   disagree, and a gate an older binary left is closed by the next wind-down
   transition or by the end as well as by the repair.
5. **A record an older binary wrote is repaired, for the gate.** The settle
   pass closes it once; the terminal status, reason and stamp do not move. I
   did not add a filter to the point query: until a pass touches the record,
   the snapshot lists the gate, because the record holds it.
6. **A leaked reservation in a record an older binary wrote is not
   repaired.** Nothing marks a cancelled call as fenced, so a repair would
   have to recompute the counter from the calls still out and tie the two
   together for good. Nothing is stuck. An ended run reserves only inside
   its post-terminal memory window, ten minutes by default, so the records
   it could still matter for are runs that were winding down across the
   upgrade under an attempts limit. If you want it, the measurement in
   section 1 says the recompute is sound today, and it is one predicate.
7. **A compensation fences before it reserves,** so it can spend what the
   calls it cancels had reserved. The fence alone would not have helped the
   decision that starts the wind-down, which reserved first. A compensation
   the run still cannot afford is refused as before, and the refusal changes
   nothing.
8. **The repair never asks a live run that is not winding down.** Nothing
   leaves such a gate on one, and its gate is its wait.

## 5. What a reader sees

| State | Before | Now |
|---|---|---|
| A failed call cancels a gated call | the gate waits, through the wind-down and on the ended run | the gate is `Cancelled` in that transition, with a close event |
| A grant, a denial or an expiry of that gate while the run winds down | applied: a grant stored for a cancelled call, or the call rewritten `Failed` | refused `checkpoint-already-resolved`; the timer acts on nothing |
| A compensation or an accepted handoff cancels a gated call | as the first row | as the first row |
| A run ends at once with a goal evaluation waiting behind its gate | the run ends holding the evaluation `Pending` and the gate open | the evaluation is `Cancelled` and the gate closed, before the end event |
| The attempts of a call the fence cancels | reserved for the rest of the run's life | released; none consumed |
| A compensation scheduled while unsent calls hold attempts | may be refused though the run can afford it | can spend what those calls reserved |
| A cancel | closes every approval and authorization | the same, in the same order; and releases the attempts |
| The gate of a memory promotion or a claim append | not touched by a wind-down that is not a cancel; answered on an ended run | unchanged |
| A gate while its call is `Pending` on a live run | its wait | unchanged |

## 6. Tests

`crates/rakka-agent/tests/wind_down_fence.rs`, new, thirteen tests. All but
the forged ones end by checking that the attempts the run holds reserved
are those of the calls it still waits on.

- A failed call closes the gate of the call it cancels, for an approval and
  for an authorization; the close is recorded once, before the end.
- The same with another call still out: the gate closes in the failure's own
  transition, before the wind-down's wait event, and a grant and a denial
  are each refused and change nothing.
- A compensation closes the gate of the call it cancels, in the decision's
  own transition.
- A run that completes cancels its unsent evaluation and closes its review,
  before the end event; a late decision is refused `run-terminal`.
- A cancelled call's attempts are free for the compensation that follows,
  under a limit that refuses it on `main`.
- A compensation can spend what its own wind-down releases.
- A compensation the run cannot afford changes nothing.
- A cancel releases the attempts of the call it cancels.
- Three records as an older binary left them (ended with a cancelled call's
  approval, winding down with one, completed with an unsent review): each is
  closed by one settle pass, once, and the end does not move.
- Ninety-six forged records: each standing of the run, each kind of gate,
  each status of the gated call or no call at all, the gate open or
  escalated. All are at rest in two passes; a live run's gate is never
  closed for it; an ended run is left with no waiting gate and no unsent
  call.
- The step that closes a gate measures no turn.

One unit test in `run.rs`, for the handoff: an accepted notice closes the
gate of a re-send it cancels, with nothing else out and with another call
still out.

Thirteen of the fourteen fail on `main`: twelve when the file is run
against `main`'s source, and the unit test when the three lines it covers
are put back. The one that passes there, the refused compensation, holds
what must not change.

One existing test changed. `an_ended_runs_next_wake_reads_only_what_its_sweep_acts_on`
built the defect through commands and asserted the approval was "exactly as
the run ended". It now starts from a rewritten record, reads the snapshot
before anything touches it, and then checks the first settle pass closes the
approval.

Eighteen deliberate breaks of the change were run against these tests and
the promotion, claim, checkpoint and goal tests. Seventeen each fail at
least one test. The first run caught thirteen; the five that survived
showed where a test could not tell my change from the repair that follows
it one pass later, and four of them are now caught: the failed call's and
the compensation's close are tied to their own transition, the forged
records include a gate with no call, and the repair's step is checked
against the turn metrics. The eighteenth cannot be caught by any record a
binary writes: the end passing "not ended" to the close. The fence that
runs just before has already cancelled every unsent call, so the two
answers only differ for a gate whose call the record does not hold.

The breaks that close too much (every gate at a wind-down, a promotion's
gate at the end) are caught by tests that were already there.

## 7. Found, not done

- **An ended run can hold a goal evaluation `Ready` for good.** An ungated
  evaluation that was sent before the run ended at once is not the fence's
  to cancel: it may have been invoked. The dispatcher then cancels it, and
  the ended run refuses that result `run-terminal`, because an ended run
  takes results only for the four exempt kinds. The record lists the call
  among its pending effects for good and keeps its reserved attempts.
  Reproduced on `main` and on this branch. Drafted, not filed:
  `tmp/2026-11-18-issue-draft-ended-run-ready-evaluation.md`.
- **A gated compensation or workflow cancel never opens its gate** (#128,
  open). Unchanged.
- A leaked reservation in a record an older binary wrote (call 6).
