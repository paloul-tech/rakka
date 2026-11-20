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
   until it is terminal. It neither waits for them nor cancels them. A
   straggler of a resolved fan-in is the exception it always was for a run
   that has not ended: it is chased, and the run waits for that cancel.
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
7. **Three settle-pass repairs,** each unreachable under this build
   alone. `AgentRun::needs_decided_end` is for mixed builds (section 6).
   `AgentRunState::needs_answered_proposal_drop` ends a wind-down an
   earlier build left waiting for a refused proposal (section 7).
   `AgentRun::needs_wind_down_fence` ends one it left holding a call it
   never sent (section 8).
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
  first for the run's children, and for a memory promotion or a claim
  append still out, where this build waits for neither.

So a waiting run an earlier build activates can have its children
cancelled. A deployment that cannot accept that finishes the upgrade
before it relies on this, or drains runs with live children first.

Where an earlier build settled the last call and left the run waiting for
one of those, this build's next settle pass ends it
(`AgentRun::needs_decided_end`). A test holds that; without the repair the
run stayed `Cancelling` under this build too, because a settle pass does
not otherwise re-run the disposition of a run with nothing out.

## 7. The stuck wind-down, repaired

Added at the owner's word, after the pull request was open.

An earlier build took the task's refusal of a proposal, made because the
task was being cancelled, as the run's own cancellation, and left the
refused proposal on the record. Its wind-down waited for that proposal for
good, and the task's cancellation never completed. Section 4 stops new
ones. This repairs the ones that exist.

**The record alone does not say the proposal was answered.** Its status is
`Cancelling`, or `WaitingForReconciliation` where a call of the run was
parked, and its reason the one its wind-down recorded, most often a
cancellation. So are those of a cancelled
run whose proposal is still on its way, and that run must go on waiting:
an acceptance still wins the race with a cancel. The reason's text does
not settle it either. Where the task's own cancel reached the run before
the refusal did, the reason is the canceller's, not the refusal's code.

**The run's exchange journal does.** A proposal is recorded in the
transition that owes its exchange, and the exchange stays owed, and
re-driven, until its answer settles it. Nothing withdraws a run's
proposal. So a proposal whose exchange the journal no longer owes has been
answered. `AgentRunState::needs_answered_proposal_drop` asks that of a run
that is winding down, and the settle pass drops the proposal and re-runs
the disposition.

Two tests. One builds the earlier build's record and sees the run end
`Cancelled` and the task's cancellation complete; it fails without the
repair. The other holds the condition: a cancelled run whose proposal is
unanswered goes on waiting through settle passes that cannot reach the
task, and is then accepted. With the journal condition removed, that test
and five others across the crate fail.

## 8. The turn begun on a cancelled run, repaired

Added at the owner's word, after the first repair.

An earlier build read a task's rejection against the run's status alone
(section 4, the second arm). A cancelled run parked on an unknown outcome
is `WaitingForReconciliation`, not `Cancelling`, so the rejection began a
new turn on it, and the build's next step committed that turn's model
call. A run that is winding down hands the sink only the kinds the fence
exempts, so the call was never sent. No fence ran after it, so it was
never cancelled. The wind-down waited for it for good: `WaitingForEffect`
while the park was open, `Cancelling` once a person had answered it.
Section 4 stops new ones. This repairs the ones that exist.

**The repair is the fence the wind-down owed.**
`AgentRun::needs_wind_down_fence` asks whether a run that is winding down
and has not ended holds a call the fence would cancel: one never handed to
the sink, of a kind the fence does not exempt. The settle pass then runs
the fence and re-runs the disposition. The call is cancelled and gives
back the attempts it reserved, a gate it waited behind is closed, and the
run rests on its park, or ends as cancelled if the call was all it waited
for.

**Nothing is lost by it.** The call is one no flush would send: the flush
and the fence read the same exemption. The question and the fence read one
predicate too, so what is asked and what is done cannot drift apart.

My calls:

1. **It asks every run that is winding down, not only the record that arm
   left.** It reads what the fence acts on, and not the turn or the phase.
   So it also takes a record no build is known to write: a gated call
   still `Pending` on a run that is winding down. The sweep of forged
   records added for issue #133 asserted that record was left as it was,
   on the ground that no wind-down leaves a call `Pending`. An earlier
   build did leave one, so that assertion is changed: the call is
   cancelled and its gate closed. A grant could not have released such a
   call, because the flush would not send it.
2. **The crank is not stopped.** A record caught between the earlier
   build's two writes has the turn begun and no model call yet. This build
   commits the call, as that build's next step did, and fences it in the
   step after. Both tests of the repair go that way. One model call is
   counted against the run's budget that was never made, as it was under
   the earlier build.
3. **No door is added.** A decision can reach a gated one of these before
   the pass does, and is applied as it always was. Only a forged record
   has a gate there.
4. **The run ends as cancelled, on the turn that build began.** The turn
   number and the rejection's feedback stay on the record.

Two tests build the earlier build's record. The two writes of its arm are
made on a record this build wrote, and this build's next step commits the
model call by code the two share. Run under that build's sources, the same
world leaves the same record but for two clock values.

- With the park open: the call is cancelled, its attempts are given back,
  the run rests on its park, a further pass writes nothing, and the park's
  answer ends the run `Cancelled`. It fails without the repair.
- With the park answered while the run is stuck: the step that fences the
  call ends the run, and the run settles its budget with its task. The
  record is put back as the earlier build left it, by the three fields the
  repair wrote, because this build's settle pass will not leave it so.

The earlier build's own two records, dumped under its sources, were also
run through this build. Both end as these do. Those runs are not kept.

Two more tests hold what the repair must not do. A waiting run that holds
a claim append behind its gate, which the fence exempts, is at rest, and
the append outlives the run's end. And the step that fences a call
measures no turn.

Nine breaks of the repair were each run against the whole crate. Eight
are caught, each by a named test: the repair removed, either condition of
its question dropped, the fence's exemption ignored, the disposition not
re-run, the end's exchanges not owed, and the question left out of either
of the settle loop's two lists. The ninth cancels the call and leaves its
gate to the repair of issue #133, one step later in the same pass. No test
tells that from the fence closing it. Two of the eight survived the first
tests, and the two tests of the last paragraph were written for them.

## 9. Tests

`crates/rakka-agent/tests/run_end_waits.rs`, new, twenty tests; four in
`workflow_tool.rs`; two unit tests in `run.rs`; one more in
`wind_down_fence.rs`, where one expectation of the forged sweep changed
(section 8, call 1).

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
- An accepted run that waits leaves its children as one that does not:
  workflow children, and a child it delegated to (the second review's
  probe, kept).
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
- The two stuck records of sections 7 and 8 are repaired, and what each
  repair must leave alone is left alone.
- Unit: which reasons are decided ends; and the three end sites no whole
  run reaches easily (a refusal, an undecodable decision, a top-up
  answered with nothing) each wait.

Of the twenty-four whole-run tests, twenty fail on `main`. The three
stuck-record tests of sections 7 and 8 fail there at their first step,
which is the answer `main` mishandles; each failed on this branch, at its
repair's own assertion, until the repair was written. The four that pass
on `main` hold what must not change: a run that holds nothing, one that
holds only a call it never sent, and one that holds only a claim append
each end at once, and a cancelled run whose proposal is unanswered goes on
waiting for it. In `wind_down_fence.rs` the new test and the sweep's
changed expectation fail on `main` too. The two unit tests name functions
`main` does not have.

Nine breaks of the review's fixes and of the sites it found untested were
each run, and each is caught by one test.

## 10. The review

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

A second reviewer read the second commit. It found no defect in the four
code changes and confirmed the test counts on `main`. It corrected four
statements, and ran the record that section 7 now repairs.

A third reviewer read the repair. It found it sound: a run's pending
exchange leaves the journal only by being settled, in the transition that
applies its answer; the record the test rebuilds is byte for byte the one
`main` leaves; and the repair writes once. It corrected two statements:
how many tests the journal condition holds, and what the record that
section 8 now repairs reports.

## 11. Not done

- Records already ended in one of these states (call 8).
- A run parked on an unknown outcome for a completed task waits for a
  person. That is the rule's cost, and what the owner chose.
- What a task or an agent that has decided does while its run is not yet
  terminal was not examined beyond the tests here: the wake controller's
  slot, the escrow's timing, the A2A projection. The reviewer listed it as
  not reached, and so do I.
- A `Cancel` on a waiting run changes neither its reason nor its wait. It
  still does what a cancel does to gates: it closes a waiting approval or
  authorization, and cancels a memory promotion or claim append that
  waited behind one. Left alone, that append would have outlived the end
  and been answerable on the ended run. The operational snapshot's cancellation progress is read
  from the reason, so it stays `NotRequested` under an accepted result,
  and reads as a cancellation under a refusal whose task was cancelled.
- A straggler chased while the run waits is marked with the run's reason
  (`result-accepted`), not `fan-in-straggler`. Found by the second review;
  not changed.
- A run that waits for an end that will be `Cancelled` (its task was
  cancelled, and refused its proposal) sends its children their cancel
  when it ends, not while it waits. The wait can be for a person.
- Two leads from the second review, read and not run, both on `main`
  already: a top-up grant sets a cancelled run parked on a top-up back to
  `Running`; and a run out of budget with children out never chases them.
- What this build's step does to a record caught between the earlier
  build's two writes (section 8, call 2) when the turn it commits finds
  the run's budget spent was not run.
