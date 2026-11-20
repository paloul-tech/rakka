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
  that holds an unknown outcome and is not parked is parked before the
  loop's step.

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
   find: the issue says I checked two. What such a run has already sent
   stays sent, and its result is taken as on any parked run.
4. **A run whose end was decided is not asked.** It waits under a status of
   its own, and a memory promotion or a claim append whose outcome is
   unknown does not park it (issues #123, #172 and #173).
5. **The guard runs after the wind-down's repair.** A run that is winding
   down and off its rest is fenced first, and that repair parks it, in one
   write. The other order reaches the same record in two. No test tells them
   apart.
6. **A fixture changed.** Two existing tests build an earlier build's record
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

Five whole-run tests, each beside the tests of its sibling transition, and
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

In `workflow_tool.rs`, with a claim append whose outcome is unknown:

- **A fan-in deadline does not resume a run parked on an unknown outcome.**
  The command's own reply says `WaitingForReconciliation` at `RecordingTurn`,
  which holds the transition itself. Nothing more is sent, and the answer
  takes the run on to its next model call.
- **A run an earlier build left going past a parked append is parked
  again.** This build's record after the deadline, with its status written
  `Running`. The settle pass parks it before the loop records the turn. The
  pass that parks it measures no turn: the turn's duration is sampled once,
  when the turn is recorded after the answer.

One unit test in `run.rs` holds who the guard asks: a live run that holds an
unknown outcome and is off its park, and a run that is winding down with no
end decided. Not a run already parked, an ended run, or a run whose end was
decided.

The first and the fourth fail on `main` at `d8acd92`, with the values in
section 1.

Ten breaks of the fix were run, one at a time. Nine are each caught by a
test:

- the rejection not settling the status, and the deadline not settling it:
  each by its own test, through the single write and the command's reply;
- the settle pass with no guard step, and the guard not being what lets the
  pass act on a run that owes no step;
- the guard step measured as a turn;
- the guard asking a run that is already parked, an ended run, and a run
  whose end was decided;
- the guard reading only a call of the run's own, and not a claim append.

One survives the whole crate: the guard run before the wind-down's repair
instead of after it (call 5). Both orders end in the same record. The order
chosen fences and parks such a run in one write, where the other takes two,
and no test counts those writes.

The whole crate passes: 1,851 tests.

## 8. Not done

- A run that already sent a call past its park keeps it sent. The call's
  result is taken, and the run is parked before its next step.
- Whether a park should hold the loop for a call outside the turn at all
  (call 1).
