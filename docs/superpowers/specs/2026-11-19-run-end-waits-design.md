# A run's end waits for the calls it sent (issues #123, #172, #173)

Design and record, 2026-11-19. Branch `end-waits-for-sent-calls`, off `main`
at `70d71b4`.

## 1. What was wrong

A run that winds down does not become terminal over work whose outcome it
does not know: the settle pass waits until nothing blocks settlement
(specification 8.7 and 11.5). A run whose end was decided for it skipped
that wait. Five arms of `settle_proposal` and both budget paths called
`terminate` directly:

- the task accepts the result, refuses the proposal, or cannot be decoded;
- the task rejects with no rejections left;
- the task rejects while the run is cancelling;
- a budget is spent that cannot be topped up, or a top-up grants nothing.

An ended run takes a result only for the four kinds the wind-down fence
exempts, and answers a reconciliation checkpoint only for a memory
promotion or a claim append (issue #120). So whatever else the run held at
that moment stayed on its record for good. The three issues are three
faces of it:

| Issue | What the run held | What was left |
|---|---|---|
| #172 | a goal evaluation, sent | `Ready` for good; the dispatcher's cancellation refused `run-terminal` |
| #173 | a goal evaluation, outcome unknown | its reconciliation checkpoint open, every decision refused |
| #123 | a compensation or a workflow cancel, in flight | its result taken, but a park opened no checkpoint |

#172 and #173 were reproduced when filed. #123 was filed from reading. It
is reproduced now for both kinds, by tests that fail on `main`: the run
ends `Completed` with the compensation `Ready`, and with the workflow
cancel `Ready`.

## 2. Approaches

Put to the owner on 2026-11-19 as one question.

- **The end waits.** Chosen by the owner. The run records the reason it was
  given and waits as a run that is winding down waits. It takes the calls'
  results and answers their parks as a live run, then ends under that
  reason. It reuses the wind-down; no decision gains a new meaning.
- **End at once, settle after.** The ended run would take the result of
  anything it sent and answer its parks, extending #120. It needs a ruling
  on what each reconciliation decision means on an ended run for three
  more kinds, widens what an ended run accepts, and has the host driving
  ended runs until they settle.

A spike of the first, since thrown away, passed 1,767 of 1,769 `rakka-agent`
tests before the question was asked. The two failures held a call that was
never sent, which the rule below leaves to the end's own fence.

## 3. The rule

A run does not end while it holds **a call an end waits for**: a call that
is not a memory promotion or a claim append, and that either was sent, has
an unknown outcome, or is of a kind the fence exempts and is still to be
sent (`AgentLoopState::holds_call_an_end_waits_for`).

`end_when_settled(state, reason, now)` is where a decided end arrives.

- The run holds no such call: `terminate`, in the same transition, exactly
  as before.
- It holds one: the reason is recorded, the proposal the task answered is
  dropped from the record, what was never sent is cancelled
  (`fence_unsent`), and `settle_run_disposition` puts the run on
  `WaitingForReconciliation` if one of those calls has an unknown outcome
  and on `Cancelling` otherwise.

`settle_run_disposition` asks first whether the run's recorded reason is a
decided end (`AgentRunTerminalReason::is_decided_end`). If so it ends the
run the moment it holds no call an end waits for, and otherwise sets the
status above. Every transition that settles a call or answers a park
already ends in that function, so the transition that settles the last
call ends the run.

## 4. The wind-down and an answered proposal

A wind-down waits for a proposal's answer. Three arms of `settle_proposal`
take an answer on a run that goes on winding down, and each left the
proposal on the record, or skipped the wind-down's wait:

- **Rejected while winding down.** It ended the run on the spot, whatever
  the wind-down still waited for. It now ends there only if the proposal
  was all it waited for. Otherwise the answered proposal is dropped and
  the wind-down goes on, for its calls and its children.
- **The same, on a parked run.** The arm asked whether the status was
  `Cancelling`. A run that is winding down and parked on an unknown
  outcome is `WaitingForReconciliation`, so the rejection began a new turn
  on a cancelled run, whose model call was then never sent. It asks
  `winding_down()` now. This was on `main`; the review found it.
- **Refused because the task is being cancelled.** The run takes the
  refusal as its own cancellation. The refused proposal stayed on the
  record, so the wind-down waited for it for good, and the task's
  cancellation with it. It is dropped now. This was on `main` too; the
  review found it.

## 5. My calls

1. **What holds a decided end is narrower than what holds a wind-down.** A
   wind-down waits for every outstanding effect, the proposal and the
   children. A decided end waits for none of the last two, and not for a
   memory promotion or a claim append. Each is what such an end did before:
   the proposal's answer is what decided the end; a promotion and a claim
   append are owed after the end by design; and an accepted run did not
   wait for a child to finish.
2. **The status while waiting is `Cancelling`,** and not a new one. The run
   is winding down: it dispatches nothing but what the fence exempts and
   takes no turn. `Cancelling` already ends `Completed` when an acceptance
   wins a race with a cancel. A new status would be a durable vocabulary
   change for a state that lasts one dispatch pass in the common case.
   **Its cost:** a reader that takes `Cancelling` for a cancelled scope is
   wrong about a waiting run. There was one in this crate, the chase of a
   run's children, and the review found it: a waiting run sent a cancel to
   every child it had. The chase now reads the reason too. An earlier
   build's chase does not (section 6).
3. **A waiting run does to its children what an end at once did:** nothing,
   until it is terminal. It neither waits for them nor cancels them.
4. **A decided end replaces a wind-down's reason,** as `terminate` did when
   it ended the run on the spot. A wind-down's reason does not replace a
   decided end's: `cancel`, a compensation and a failed effect each record
   theirs only when none is set. A handoff the task accepted is the one
   exception, as it was: it replaces any reason, and the run then winds
   down as a handed-off run does.
5. **The answered proposal is dropped from the record of a run that goes
   on,** and kept on the record of a run that ends in the same transition,
   which is the record such an end always left. So a waiting run does not
   report a pending proposal, and neither does the record of a run that
   waited.
6. **A cancelling run whose proposal is rejected waits for its children
   too.** It skipped that wait. Specification 8.7 does not let a run
   project `Cancelled` over a child that has not reported.
7. **One settle-pass repair,** `AgentRun::needs_decided_end`, for mixed
   builds (section 6). It is unreachable under this build alone.
8. **Records an earlier build ended are not repaired.** None is known.
   Each would need its own ruling: what an ended run does with an
   evaluation it cannot know was invoked, and with a park it never offered.

## 6. Mixed builds

An earlier build that loads a waiting run:

- dispatches for it as for any run that is winding down, and takes the
  call's result;
- **reads `Cancelling` as a cancelled scope, and sends a cancel to every
  child the run has**;
- ends the run under its recorded reason once nothing is out, but waits
  for the run's children first, where this build does not.

So a waiting run an earlier build activates can have its children
cancelled. A deployment that cannot accept that finishes the upgrade
before it relies on this, or drains runs with live children first.

Where an earlier build settled the last call and left the run waiting for
a child, this build's next settle pass ends it
(`AgentRun::needs_decided_end`). A test holds that; without the repair the
run stayed `Cancelling` under this build too, because a settle pass does
not otherwise re-run the disposition of a run with nothing out.

## 7. Tests

`crates/rakka-agent/tests/run_end_waits.rs`, new, fifteen tests; three in
`workflow_tool.rs`; two unit tests in `run.rs`.

- An accepted run waits for the evaluation it sent, takes the dispatcher's
  cancellation, and ends `Completed` (#172). It takes the evaluation's own
  result too, and the dispatcher alone settles it.
- An accepted run parked on an unknown outcome stays parked, answers the
  checkpoint, and ends (#173).
- A run winding down for a compensation is accepted: the reason is
  replaced, the run waits, and ends `Completed` (#123).
- An accepted run waits for a straggler's workflow cancel, and cancels a
  review it never sent at once. The cancel ends unknown, the park opens its
  checkpoint, a person answers, the run ends (#123).
- An accepted run that waits leaves its children as one that does not.
- A cancelling run whose proposal is rejected goes on winding down, for
  its call, and for its child. A parked one begins no turn.
- A proposal refused for the task's cancellation does not hold the
  wind-down.
- A run out of rejections, and a run out of budget, wait and then end
  `Failed` under their own reasons.
- A claim append does not hold the end. Parked, it neither holds the end
  nor decides the waiting run's status, and its checkpoint stays on the
  ended run.
- A run that holds nothing, and one that holds only a call it never sent,
  end at once, and their record is as it was.
- A waiting run an older build settled is ended by the next settle pass.
- Unit: which reasons are decided ends; and the three end sites no whole
  run reaches easily (a refusal, an undecodable decision, a top-up
  answered with nothing) each wait.

Of the eighteen whole-run tests, fifteen fail on `main`. The three that
pass there hold what must not change: a run that holds nothing, one that
holds only a call it never sent, and one that holds only a claim append
each end at once. The two unit tests name functions `main` does not have.

Nine breaks of the review's fixes and of the sites it found untested were
each run, and each is caught by one test.

## 8. The review

A fresh reviewer read the first commit, with probes and twenty-two breaks
of its own. It found:

- **A waiting run cancelled every child it had** (call 2). New with the
  first commit. Fixed, with a test.
- **Two defects already on `main`** in the function the commit rewrites
  (section 4, the last two arms). Fixed here, each with a test.
- **A handoff notice could replace a decided reason and leave the run
  waiting for an answered proposal.** By reading. The proposal is dropped
  now, so the run ends as handed off; call 4 says what replaces what.
- **The compensation test did not test what it said.** Its proposal had
  been answered before the compensation was scheduled. It is rewritten to
  the issue as filed, and now holds the reason's replacement.
- **Thirteen breaks survived.** One changes nothing observable but the
  dropped proposal, which a test now holds. The rest each has a test now.
- **A waiting run reported a pending proposal** beside its accepted result
  (call 5).

## 9. Not done

- Records already ended in one of these states (call 8).
- A run parked on an unknown outcome for a completed task waits for a
  person. That is the rule's cost, and what the owner chose.
- What a task or an agent that has decided does while its run is not yet
  terminal was not examined beyond the tests here: the wake controller's
  slot, the escrow's timing, the A2A projection. The reviewer listed it as
  not reached, and so do I.
- A `Cancel` on a waiting run is accepted and changes nothing; the
  operational snapshot's cancellation progress stays `NotRequested` for
  it.
