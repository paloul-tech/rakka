# A reconciliation park holds a live run until it is answered (issue #186)

2026-11-20. Short design, with my calls listed. The owner reviews at the pull
request.

## 1. What was wrong

A live run that holds an effect whose outcome is unknown is parked:
`WaitingForReconciliation`, with a reconciliation checkpoint open. The loop
takes no step there until a person answers what the call did
(`settle_run_disposition`).

Two transitions wrote `Running` over that park and did not settle the run's
status afterwards:

- **The task rejects the run's proposal.** The rejected arm of
  `settle_proposal` begins the next turn and writes `Running`.
- **A fan-in wait's deadline fires.** `fire_fan_in_deadline` resolves the
  wait and writes `Running`.

In both, the run's next step prepared a turn and sent its model call, with
the outcome still unknown and the checkpoint still waiting. Measured on
`main` at `d8acd92`: after the rejection the run is `WaitingForEffect` on
turn 2 with one more call sent, and after the deadline it is `AwaitingModel`
on turn 2 with one more call sent.

The run was not lost. The next transition that settled its status parked it
again. It had acted past an outcome nobody had answered, which is what the
park is there to prevent.

## 2. Approaches

1. **Both transitions end in `settle_run_disposition`**, as the transitions
   beside them do. A child's result that resolves the same fan-in wait
   settles the status, and so does a rejection on a run that is winding
   down.
2. **Narrow the park** to a call of the turn, so that a memory promotion or
   a claim append whose outcome is unknown does not hold the loop at all.
3. **Guard the loop**: refuse the step in the settle pass on any run that
   holds an unknown outcome, whatever wrote its status.

I chose the first, and added the third behind it. The second changes what a
park means, for every deployment, and the code is unambiguous today: any
unknown outcome parks the run. That is a decision about the product and not
a fix for this issue.

## 3. The rule

A live run that holds an effect whose outcome is unknown takes no step of
the loop until the outcome is answered. A transition that would set such a
run going leaves it parked, and the answer sets it going from where that
transition left it.

## 4. The change

- **`settle_proposal`, the rejected arm.** The rejection is taken as before:
  the turn is counted and its feedback kept. The run's status is then
  settled, so a parked run stays parked. Nothing of the turn is prepared or
  sent until the answer.
- **`fire_fan_in_deadline`.** The wait is resolved as before, with the
  stragglers timed out. The status is then settled. A parked run stays
  parked on a resolved wait, and records the turn after the answer.
- **The settle pass** (`AgentRun::needs_park`, in `advance_once`). A live run
  that is not winding down, holds an unknown outcome and is not parked is
  parked before the loop's step.

When the park ends, the status is read from what the run rests on
(`live_status`), as it is for every park since issue #131. After a rejection
that is the turn it began; after a deadline it is the turn to record.

## 5. My calls

1. **The park holds for every kind of effect**, as it does today
   (section 2). Yours to reverse.
2. **The rejection's turn is counted while the run is parked.** The turn
   number moves and the feedback is recorded in the transition that takes
   the rejection. Holding the rejection back until the answer would need a
   place to keep it, and the task has already charged the iteration.
3. **The settle pass parks a run that is off its park.** Two reasons. A
   record a build with this defect left between its two writes, `Running`
   with a turn begun and nothing sent yet, would otherwise still take that
   step under this build. And it holds the rule for any transition I did not
   find: the issue says I checked two.
4. **The park holds the loop and recalls nothing.** A call an earlier build
   committed past the park and had not yet sent is sent by this build: the
   run is parked, and a committed call of a run that is not winding down is
   handed over whatever its status. A call it had sent stays sent. The result
   of either is taken as on any parked run. Withdrawing a committed call
   would be a new kind of step, for a record that needs an earlier build to
   have crashed between two writes of one pass.
5. **The park does not hold what is owed outside the loop.** A child that a
   fan-in deadline timed out is sent its cancellation while the run is
   parked, as on a run that is not. A child left running until a person
   answers an unrelated outcome would run for as long as that takes.
6. **A run that is winding down is not asked by the guard.** Where its
   status permits the loop a step, the wind-down's own repair asks it
   (issues #123, #172 and #173); where it does not, the loop takes none.
   Under which status it then waits is the disposition's to say. My first
   version asked a winding-down run whose end was not decided, and the
   review showed what that cost (section 9). The two repairs now ask
   disjoint sets of runs, so their order decides nothing.
7. **A fixture changed.** Two existing tests build an earlier build's record
   by setting a cancelled run's reason aside for one step of the loop. With
   the reason aside that run is live and holds an unknown outcome, so the
   guard now refuses the step. The fixture sets the unknown outcome aside
   for that step too, and puts it back. The record it builds is the same:
   compared field by field with the one the old recipe built with the guard
   switched off, there is no difference.

## 6. Mixed builds and rollback

Nothing durable is added. An earlier build still steps past a park at these
two transitions, and this build parks such a record at its next settle pass.

## 7. Tests

Nine whole-run tests, each beside the tests of its sibling transition, and
one unit test.

In `crates/rakka-agent/tests/run_end_waits.rs`, with a goal evaluation whose
outcome is unknown:

- **A rejection on a run parked on an unknown outcome does not resume it.**
  The run stays `WaitingForReconciliation`, the turn is counted, no model
  call of that turn exists, nothing more is sent, and the checkpoint still
  waits. The rejection is one write, which holds the transition itself: with
  only the guard, the run is parked by a second write. The answer then sends
  the turn's model call.
- **A run an earlier build left going past an unknown outcome is parked
  again.** This build's record after the rejection, with its status written
  `Running` as the earlier arm left it. The settle pass parks it and sends
  nothing, a further pass writes nothing, and the answer sets it going.
- **The same record one step further on is parked with its call out.** The
  earlier build's step has run: the turn's model call is out and the run
  reads `WaitingForEffect`. The first pass parks it, the call stays out, and
  the answer leaves the run waiting on that call. This record was compared
  field by field with the one the unfixed transitions write for the same
  world: it differs in three clock values.
- **A call an earlier build committed past an unknown outcome is sent from
  the park** (call 4). The same record with the model call committed and
  not sent. The first pass parks the run and sends the call.
- **A cancellation after a rejection on a parked run keeps it parked until
  answered.** The run is on a turn it has not prepared. The cancellation
  leaves it parked under its reason and sends nothing; the answer ends it
  cancelled, with the turn never prepared.

In `workflow_tool.rs`, with a claim append whose outcome is unknown:

- **A fan-in deadline does not resume a run parked on an unknown outcome.**
  The command's own reply says `WaitingForReconciliation` at `RecordingTurn`,
  which holds the transition itself. This world's child cannot be cancelled,
  so nothing is sent. The answer takes the run on to its next model call.
- **A fan-in deadline on a parked run still cancels the child it timed out**
  (call 5). The same world with a child that can be cancelled: one call is
  sent, the cancellation, and the run stays parked and at rest.
- **A fan-in deadline resumes nothing on a parked run that is winding
  down.** The reply's status and phase are as they were.
- **A run an earlier build left going past a parked append is parked
  again.** This build's record after the deadline, with its status written
  `Running`. The settle pass parks it before the loop records the turn. The
  pass that parks it measures no turn: the turn's duration is sampled once,
  when the turn is recorded after the answer.

One unit test in `run.rs` holds who the guard asks: a live run that is not
winding down, holds an unknown outcome and is off its park. Not a run
already parked, an ended run, or a run that is winding down on any status.

Two fail on `main` at `d8acd92`. The rejection's test fails with the values
in section 1. The deadline's fails at the command's reply, which says
`Running` at `RecordingTurn`. The four added after the review pin what the
first commit already did, and passed when written. Breaks hold three of
them. The cancellation's test describes a state this fix creates, and no
break of the fix fails it.

Thirteen breaks were run, one at a time. Twelve are each caught:

- the rejection not settling the status, and the deadline not settling it:
  each by its own test, through the single write and the command's reply;
- the settle pass with no guard step, and the guard not being what lets the
  pass act on a run that owes no step;
- the guard step measured as a turn;
- the guard asking a run that is already parked;
- the guard reading only a call of the run's own, and not a claim append;
- the guard asking an ended run, and asking a run that is winding down:
  each by the unit test alone;
- the wind-down repair asking only a run with an unsent call, and that
  repair's step removed (the review's two, section 9);
- the deadline resuming a run that is winding down, which is a condition
  from before this change: by the new test of that run.

One survives the whole crate: the deadline settling the status only where it
had set the run going. On every other path the settle at its end changes
nothing in any world the tests build. It is placed at the end, as it is in
the transitions beside it.

The guard's place among the repairs of the settle pass is not held either.
The review ran it first, after the gate-close repair and last, and the whole
crate passed each time. What is held is that it runs before the loop's step.

The whole crate passes: 1,855 tests.

## 8. Not done

- A call an earlier build committed or sent past its park is not withdrawn
  (call 4).
- Whether a park should hold the loop for a call outside the turn at all
  (call 1), and whether it should hold more than the loop: a parked run
  still sends an evaluation a command asks for, and a call whose approval is
  granted. The review ran both; neither is changed, and no test here holds
  either.
- The whole-run tests answer each park one way. The review answered both
  states every way, by checkpoint and by command, and found each at rest;
  those probes are not kept.

## 9. Review

A fresh reviewer read the first commit, with probes and breaks of its own.
It found no third transition that lifts a park: it read every write of the
run's status and every caller of the disposition. It confirmed both record
comparisons and the breaks listed in the first version of this record. What
it found is answered in the second commit:

- **The guard had disarmed a test of #178.** The wind-down repair asks a
  run on a status that permits progress, or one that holds an unsent call
  the fence cancels. One test held the first half. Its record is a cancelled
  run that holds an unknown outcome, and my guard asked that run too, so the
  test passed with the first half removed, and with the whole repair step
  removed. The guard no longer asks a run that is winding down (call 6),
  and both breaks are caught again.
- **"Spends nothing while it waits" was false.** A deadline on a parked run
  still sends the cancellation of the child it timed out. Corrected in the
  compatibility entry, decided as call 5, and held by a test.
- **The committed call** (call 4) was missing from the docs, which spoke
  only of a call already sent. Stated, and held by a test.
- **"The first and the fourth fail on main with the values in section 1"**
  was wrong about the fourth (section 7).
- The agents guide did not say its passage is about a run that is not
  winding down, and the doc comment of `fire_fan_in_deadline` said a
  terminal run records the resolution. Both corrected.
- No test held a cancellation in the state a rejection leaves, or a deadline
  on a run that is winding down. Both added.

Two things it found beside the fix are drafted as issues and not filed:

- A turn rested with a call of its own still unknown goes on to the next
  turn when that call is answered "never executed". The reviewer reproduced
  it; it is the same before this change.
- The `run-resume` telemetry segment is decided by the loop's phase, so a
  wait resolved on a parked run reports a resume that did not happen, and
  the park's answer reports none.
