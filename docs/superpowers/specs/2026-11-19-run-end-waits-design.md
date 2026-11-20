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
   `AgentRun::needs_wind_down_rest` ends one it left holding a call it
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
new turn on it and set it `Running`, and the build's next step committed
that turn's model call. A run that is winding down hands the sink only the
kinds the fence exempts, so the call was never sent. No fence ran after
it, so it was never cancelled. The wind-down waited for it for good:
`WaitingForEffect` while the park was open, `Cancelling` once a person had
answered it. Section 4 stops new ones. This repairs the ones that exist.

**A run that is winding down takes no step of the loop, and holds no call
it never sent.** `AgentRun::needs_wind_down_rest` asks whether a run that
is winding down and has not ended is off that rest: its status permits the
loop a step, or it holds a call the fence would cancel, one never handed
to the sink and of a kind the fence does not exempt. The settle pass then
runs the fence the wind-down owed and re-runs the disposition, before the
loop's own step. The call is cancelled and gives back the attempts it
reserved, a gate it waited behind is closed, and the run rests on its
park, or ends as cancelled if the call was all it waited for.

**Nothing is lost by it.** The call is one no flush would send: the flush
and the fence read the same exemption. The question and the fence read one
predicate too, so what is asked and what is done cannot drift apart.

**The review found `main` writing the same stuck run through commands
alone, and my first version writing the record and then fencing it.** A
run parked on a top-up request, cancelled while the request was on its
way, and still waiting for a call it sent, has not ended when the task's
grant arrives. The grant set the run `Running`, whatever it then waited
on. The loop took a step, and the next turn's model call was committed and
never sent. **A top-up's grant now moves no status** (`apply_top_up_grant`).
A run parks under `Running` and takes no step while parked, so the write
was either idle or wrong. The grant is credited and clears the park, and
the run stays where it waits.

My calls:

1. **The repair asks every run that is winding down, not only the record
   that arm left.** It reads the status and what the fence acts on, and
   not the turn or the phase. So it also takes a gated call still
   `Pending` on a run that is winding down. The sweep of forged records
   added for issue #133 asserted that record was left as it was, on the
   ground that no wind-down leaves a call `Pending`. An earlier build left
   one, so that assertion is changed: the call is cancelled and its gate
   closed. A grant of the gate could not have released such a call,
   because the flush would not send it.
2. **A record caught between the earlier build's two writes commits
   nothing.** It has the turn begun and `Running`, and no model call yet.
   The pass puts it on its rest before the loop can step. My first version
   let the loop step and fenced the call after. The review ran that record
   with its budget spent: the step asked the task for a top-up, and where
   the task had nothing to grant it ended the cancelled run as out of
   budget.
3. **The grant's change is here, though nobody asked for it.** It was the
   one status write on a run that is winding down that nothing guarded, it
   writes the record this repair is for, and the function's own comment
   said such a grant was ignored. I first guarded it for a run that is
   winding down. The next review ran the same write on a run that is not:
   one parked on an unknown outcome, which the grant set going past its
   open reconciliation, a model call sent. It also ran the whole crate
   with the write removed, and nothing failed. So the write is removed,
   and both runs stay where they wait. A grant of nothing is not changed
   in its reason: it still ends a run that was cancelled while it waited
   as out of budget, and not as cancelled.
4. **No door is added.** A decision can reach a gated one of these before
   the pass does, and is applied as it always was. The review ran a grant,
   a denial and an expiry on such a record. None is worse than before, and
   a granted call is now cancelled where it stayed `Pending` for good.
5. **The run ends as cancelled, on the turn that build began.** The turn
   number and the rejection's feedback stay on the record.

The earlier build's record is rebuilt in two steps. The two writes of its
arm are made on a record this build wrote. For the call, the run's reason
is set aside for the one step in which this build's loop commits it, by
code the two builds share, and the call is put back as never sent. Run
under that build's sources, the same world leaves the same record but for
two clock values.

- Caught between the two writes: the pass commits no call and charges
  nothing, and the park's answer ends the run `Cancelled`.
- With the call committed and the park open: the call is cancelled, its
  attempts are given back, the run rests on its park, a further pass
  writes nothing, and the park's answer ends the run `Cancelled`.
- With the park answered while the run is stuck: the step that fences the
  call ends the run, and the run settles its budget with its task.
- The grant, in three whole runs. A cancelled run whose top-up is granted
  is credited, in one write, takes no turn and spends nothing more. A run
  parked on an unknown outcome is credited and sends nothing until its
  reconciliation is answered, and then goes on. A cancelled run whose
  top-up is answered with nothing waits for the call it sent and ends as
  out of budget. And a unit test of the grant on each of those two runs,
  beside one nothing has moved, which is `Running` as it parked.
- A parked run that holds an unsent call is fenced too.

The earlier build's own two records, dumped under its sources, were also
run through this build. Both end as these do. Those runs are not kept.

Two more tests hold what the repair must not do. A waiting run that holds
a claim append behind its gate, which the fence exempts, is at rest, and
the append outlives the run's end. And the step that fences a call
measures no turn.

Fourteen breaks of the repair and of the grant's guard were each run
against the whole crate. Thirteen are caught, each by a named test: the
repair removed; each condition of its question dropped, the status and the
unsent call each on its own; a parked run not asked; the fence's exemption
ignored, in whole and for the two kinds a wind-down schedules; the
disposition not re-run; the end's exchanges not owed; the question left
out of either of the settle loop's two lists; and the grant setting a
cancelled run `Running`. The fourteenth cancels the call and leaves its
gate to the repair of issue #133, one step later in the same pass. No test
tells that from the fence closing it.

Four breaks of the grant as it now stands were run the same way, and each
is caught: the grant sets `Running`, as on `main`; it sets `Running`
unless the run is winding down, as in my first answer; a grant to a run
that is winding down is not credited; and a grant of nothing leaves such a
run on its wind-down.

## 9. Tests

`crates/rakka-agent/tests/run_end_waits.rs`, new, twenty-four tests; four
in `workflow_tool.rs`; four unit tests in `run.rs`; two more in
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
- A top-up's grant moves no status: not a cancelled run's, and not that of
  a run parked on an unknown outcome. Answered with nothing, it ends a
  cancelled run as out of budget, after the call it sent.
- Unit: which reasons are decided ends; and the three end sites no whole
  run reaches easily (a refusal, an undecodable decision, a top-up
  answered with nothing) each wait.

Of the twenty-eight whole-run tests, twenty-four fail on `main`. The
stuck-record tests of sections 7 and 8 fail there at their first step,
which is the answer `main` mishandles; each failed on this branch, at its
repair's own assertion, until the repair was written. The three tests of
the grant fail on `main` at their own assertions: `main` takes the turn on
the cancelled run, sends a call past the unknown outcome, and ends the
refused run at once over the call it sent. The four that pass on `main`
hold what must not change: a run that holds nothing, one that holds only a
call it never sent, and one that holds only a claim append each end at
once, and a cancelled run whose proposal is unanswered goes on waiting for
it. In `wind_down_fence.rs` the two new tests and the sweep's changed
expectation fail on `main` too. Of the four unit tests, three name
functions `main` does not have; the fourth, of the grant, fails with the
grant's status write put back.

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

A fourth reviewer read the second repair. It confirmed every count, and
that the rebuilt records are the earlier build's but for two clock values,
and found the repair right for the record it was written for. It found
three things. This build wrote the same stuck run itself, through a top-up
granted to a cancelled run, as `main` does: the grant's guard answers it
(section 8). A record caught between the earlier build's two writes was
stepped once before it was fenced, and that step could ask the task for a
top-up or end the run as out of budget: the repair now comes before the
step. And five statements were not true of the code, the largest that the
repaired shape was unreachable under this build. It ran eight more breaks;
the three that survived for want of a test have one each now.

A fifth reviewer read that answer. It found no defect in its two changes.
It put a panic on every write of a run that is winding down under a status
that permits progress, and ran the whole crate: none. So no wind-down
needs the loop, and the shape is unreachable under this build. It found
the grant's guard too narrow, as section 8 call 3 says, and showed the
fix. It found two of my statements held by no test, that the grant is
credited and that a grant of nothing ends the run as out of budget, and a
unit test whose last part did not test what it said; each has a test now.
And it corrected five sentences. The last commit, which answers it, was
read by no reviewer: its one code change is the break that review ran
against the whole crate.

## 11. Not done

- Records already ended in one of these states (call 8).
- A run parked on an unknown outcome for a completed task waits for a
  person. That is the rule's cost, and what the owner chose.
- What stands above a task while its run is not yet terminal was not
  examined when this was merged. It was examined afterwards: section 12.
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
- One lead from the second review, read and not run, on `main` already: a
  run out of budget with children out never chases them. Its other lead,
  a top-up grant that set a cancelled run `Running`, was run by the fourth
  review and is fixed (section 8).
- A top-up answered with nothing still ends a run that was cancelled while
  it waited as out of budget, and not as cancelled (section 8, call 3).
  The fifth review read two things downstream of that and did not run
  them, both on `main`: what such an answer does to the task's goal when
  the run that asked was already cancelled, and a cancelled run with a
  child out that is ended as out of budget by one.
- A run that is winding down still sends the top-up request it owed, which
  is why an answer reaches it at all. A grant then holds task headroom for
  a run that will not use it, until the run's return.
- An earlier build's grant could resume a cancelled run at its tool turn
  too, where the step commits the turn's tool calls and opens a gated
  one's gate. By reading, the repair cancels those calls and closes the
  gate, as for the forged sweep's record. Not run.

## 12. Above the task, examined

Done on 2026-11-20, after the merge, at the owner's word. It changes no
code. It adds seven tests and the paragraph "Above the task, while its run
waits" in `docs/rakka-compatibility.md`.

**What decides most of it.** What settles a task's budget goes upward on
its ledger, not on its status. `owed_epoch_result`,
`owed_delegation_result` and `finalize_task_cancellation` each return
nothing while a child of the task's ledger is out, and a run returns its
budget only when it ends. None of the three changed in this work. A
cancelled run parked on an unknown outcome already held its task's ledger
open; the new thing is a task that is `Completed` or `Failed` in that
state.

**What does not wait.** `owed_team_terminal_notice` and
`owed_dependent_outcomes` read the task's status alone. The team's board
and a task's dependents are told in the commit that decides the task. My
first pass missed both, and said the task told nobody; the review found
them.

**Measured, and held by a test each.**

| What | Result | Test |
|---|---|---|
| The wake of an accepted epoch whose run waits | Active until the run ends. A later occurrence is coalesced behind it and admitted when the epoch's result releases the wake | `an_accepted_epoch_holds_its_wake_until_its_run_ends` |
| The same, the run parked on an unknown outcome | Held until a person answers the park. Later occurrences share the one parking place. The result carries two effect attempts: the model call's and the evaluation's | `a_parked_accepted_epoch_holds_its_wake_until_the_park_is_answered` |
| An operator's `CompleteWakeOccurrence` on the held wake | Under a policy that bounds each epoch, the next epoch runs beside the parked one. The first epoch's late result returns its budget and releases nothing, with another occurrence active and one parked | `an_operator_can_release_the_wake_a_parked_epoch_holds` |
| A goal cancelled over a parked accepted epoch | `Propagating` until the park is answered, then `Cancelled`. A `Cancel` on the run changes nothing | `a_goal_cancelled_over_a_parked_accepted_epoch_ends_when_the_park_is_answered` |
| A delegated child whose run waits | `Completed`, and its `DelegationResult` is not sent until the run ends | `a_delegated_child_whose_run_waits_reports_when_the_run_ends` |
| A verdict that lands after the acceptance | Recorded on the run, reported, refused `task-goal-evaluation-forged` by the task's door. The goal stays undecided | `a_verdict_that_lands_after_acceptance_decides_nothing` |
| The team's board and a dependent task | The board entry closes and the dependent is unblocked while the run waits, and both stay so while it is parked | `a_decided_task_tells_its_board_and_its_dependents_at_once` |

The first four are in `crates/rakka-agent/tests/waiting_end_task_side.rs`,
the last three in `run_end_waits.rs`, which now holds twenty-seven. Each
passed when it was written, since it holds behaviour that already existed.
Seven breaks were then run against them, one at a time: each of the three
ledger gates switched off, the door's check of the sender switched off, a
ledger gate added to the team's notice and to a dependent's outcome, and a
release of a wake that is not active made to release the one that is. Each
is caught by the test that names it.

**Read, not run.**

- `rakka-a2a`, with its `agents` feature, projects the task's status
  (`crates/rakka-a2a/src/agents/projection.rs`) and reads a run only
  through the task's assignment (`agents/service.rs`). A decided task has
  none, so the public task is terminal when the task is decided, as before.
  My first pass looked only at the crate's top-level sources and said it
  read no agent task or run. That was false; the review found it.
- The agent entity keeps no list of its live runs.
- A parent's fan-in over a real child whose run waits. The child's side is
  held by the test above, and the fan-in's deadline by its own existing
  test; no one world joins them.
- The host, at its pin `2a43cbb`, which is before this work. I read its
  fleet-index derivation, its settle sweep and its task page, and changed
  nothing there. A task's row closes on the task's status and a run's row
  on the run's, each from its own record. A waiting run stays on the active
  roster, so its task is settled on every pass, and the result the task
  owes when the run ends is delivered by the same sweep that delivered it
  before. I did not read the whole host.

**Two things a caller may not expect.** Neither is new behaviour of the
code that decides it, and neither is changed.

- A verdict from a run whose task is decided is called `forged` by the
  door, though the run is the one the task assigned. The goal stayed
  undecided before this work too: the ended run refused the result.
- A run that waits after its last rejection still reports
  `RepeatedIterationFailure`, since struggle signals are derived for every
  run that is not terminal. One that waits after an acceptance does not.
  Run by me and again by the reviewer, with the threshold at one turn;
  held by no test.

**The review.** A fresh reviewer read the first commit, re-ran its four
breaks, and ran probes of its own. The answer is the third commit.

- Two statements of mine were false: that `rakka-a2a` reads no agent task
  or run, and that a task tells nobody above it that it is done until its
  ledger is closed. Both are corrected above, and the second has a test.
- "The next epoch runs beside the parked one" holds only under a policy
  that bounds each epoch. Under the fixture's own policy the next epoch is
  created with no budget (#179). The docs now say which.
- The test of the operator's release did not test "releases nothing a
  second time": nothing was active when the late result came. It now has
  an occurrence active and one parked, and the reviewer's break is caught.
- An assertion of two effect attempts, where the test reported the
  evaluation as never invoked, measured the attempt number the test's own
  helper reports. It is removed. The real dispatcher reports the same
  number for a call it never made, which is the second draft below.
- "A task that is `Completed` in that state" left out `Failed`. "The
  budget reported" is true of an epoch's result, not of a delegated
  child's. Three comments in the tests said more than the tests hold.

**Found beside it, and not part of this work.**

- A continuous goal that has spent a budget dimension still admits its
  next wake. The epoch is created with nothing in that dimension, can
  never be assigned, and holds the wake for good; the goal's exhaustion
  policy is never asked. Reproduced with no waiting run. Filed as #179.
- A person's result on a continuous goal's control task completes it over
  an epoch that is still out. The epoch's result is then refused
  `task-terminal`, and the goal's ledger keeps that child out for good.
  Found by the reviewer; I re-ran its probe. A draft is in `tmp/`, not
  filed.
- A call the dispatcher cancels before invoking it is charged one effect
  attempt, against the comment where the run settles attempts. Found by
  the reviewer with the real dispatcher; I re-ran its probe. A draft is in
  `tmp/`, not filed.
