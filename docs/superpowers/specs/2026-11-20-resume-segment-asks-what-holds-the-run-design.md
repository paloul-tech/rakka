# The `run-resume` segment is decided on what holds the run (issue #191)

2026-11-20. Short design, with my calls listed. The owner reviews at the pull
request.

## 1. What was wrong

A command applied to a run closes a `run-resume` segment when it finds the
run on a wait and leaves it off one. Both halves read the loop's phase alone:
`AwaitingModel`, `AwaitingTools` and `AwaitingChildren` are waits, and
nothing else is. A run can be held where its phase is none of those, so a
command that moved the phase off a wait and left the run held reported a
resume that did not happen.

Measured on `main` at `49feccf`. Each row is one command.

| The command | The run after it | It closed |
|---|---|---|
| A fan-in wait's deadline fires on a run parked on an unknown outcome | `WaitingForReconciliation` at `RecordingTurn` | `run-resume` |
| A child's result resolves the fan-in wait of the same run | `WaitingForReconciliation` at `RecordingTurn` | `run-resume` |
| The model's result lands on a run parked at its model call | `WaitingForReconciliation` at `EvaluatingModelOutput` | `run-resume` |
| The model's result lands and the turns that follow end on a top-up request | `Running` at `PreparingContext`, top-up pending | `run-resume` |
| The model's result lands on a cancelled run that still holds an evaluation | `Cancelling` at `EvaluatingModelOutput` | `run-resume` |

The other side of the same reading: a command that found the run held at a
phase that is no wait started no segment, whatever it did. The answer that
takes a parked run from `EvaluatingModelOutput` on to its proposal closed
none, and neither did the outcome that ends the cancelled run of the last
row.

The draft this issue came from counted "the answer to a park closes none" as
wrong in general. It is not. A command that leaves the run on its next wait
closes no `run-resume` anywhere, by a rule two existing tests pin: an
ordinary run closes one, on the model result that leaves it on its proposal.
The difference shows only where the command leaves the run off every wait.

Runtime events are observability. No state and no decision reads this.

## 2. Approaches

1. **One answer to "is the run on a durable wait", asked before the command
   and after it.** The phase, the status, a top-up request, and a wind-down.
2. **Ask it only after the command.** That removes every false segment and
   adds none. A command that takes a run off a park it held at a phase that
   is no wait would still close nothing.
3. **Only the reconciliation park**, after the command: the two rows the
   draft named.

I chose the first. The second leaves the answer and the model's result
disagreeing about the same wait; the third leaves three measured rows.

## 3. The rule

A `run-resume` segment is closed by a command that finds the run on a durable
wait and leaves it on none.

A run is on a durable wait (`AgentRun::is_waiting`) when any of these holds,
and it has not ended:

- its phase is a wait on an effect or on its children;
- its status is a wait: a dispatch wait, an approval or an authorization, a
  reconciliation park;
- it is parked on a top-up request;
- it is winding down, and so waits for what it still holds.

A live run on none of the four either stands on its proposal or can still
take a step by itself. A proposal that awaits its task's decision is no
wait: that is the convention the segment already had.

**Where the two readings differ.** I logged every command of the crate's
tests on which this answer differs from the phase's, before the command or
after it. There are 191, and they are three kinds of run, each at a phase
that is no wait: one parked on an unknown outcome
(`WaitingForReconciliation`), one that is winding down (`Cancelling`, or
parked with its end decided), and one parked on a top-up request under
`Running`. No test world puts an approval, an authorization, a dispatch or a
timer wait at a phase that is no wait. The most common of them is a run
parked while it stands on its proposal.

## 4. The change

`AgentRun::is_waiting`, and its two uses in `AgentRunEntityStore::apply`:
where the resume timer starts, and where the segment is closed. Where the
settle pass lost a write and the record is gone, the segment still closes
only if the run was terminal at the commit, as before.

Nothing durable is added: no field, status, error code, decision kind or
segment class.

## 5. My calls

1. **The same question on both sides.** It adds `run-resume` segments where
   there were none: on the command that takes a run off a hold at a phase
   that is no wait and leaves it on its proposal or ended. Those are
   resumes by the rule. Yours to reverse to the second approach, which only
   removes segments.

   **What it costs.** A run parked while it stands on its proposal now
   closes two: one on the model's result that put it on its proposal, as
   before, and one on the answer that takes it off the park and back to
   that proposal. The second is a resume by the rule and still leaves the
   run where it does nothing until its task decides. That follows from
   call 3.
2. **A top-up request and a wind-down are holds too.** The issue named the
   park. Both are measured rows, and with either left out the rule would
   still call a held run resumed. A wind-down had to be in for call 1 to be
   safe: without it, the answer to a park on a cancelled run that still
   holds another call would have closed a new false segment.
3. **A proposal awaiting its task's decision is still no wait.** Counting it
   would move the one segment an ordinary run closes from the model's result
   to the task's decision, which arrives as an exchange and closes none.
4. **Only a command closes the segment, as before.** An exchange delivered to
   the run (a task's decision, a top-up's grant or refusal, a delegation's
   result) closes none. Read, not changed.

## 6. Mixed builds and rollback

Nothing is stored. An earlier build closes the segments it closed.

## 7. Tests

Six whole-run tests and one unit test. Each whole-run test reads the
segments the run closed from an in-memory sink.

In `crates/rakka-agent/tests/workflow_tool.rs`:

- **A wait resolved on a parked run closes no resume segment.** The run
  rests on its child, parked on a claim append whose outcome is unknown.
  The deadline, and in a second pass the child's report, resolves the wait
  and leaves the run parked at `RecordingTurn`: no `run-resume`. The answer
  then takes the run on to its next model call and closes none either.
- **A command that rests the run on its children closes no resume
  segment.** What the change leaves as it was: `AwaitingChildren` under
  `Running` is a wait by its phase.

In `run_end_waits.rs`:

- **A command that parks the run on a top-up closes no resume segment**, and
  neither does the grant, which takes the run to its next model call.
- **The resume of a run parked at its model call belongs to the answer.**
  The model's result on the parked run closes none. The answer takes the run
  on to its proposal and closes one, beside its own `checkpoint-resolve`.
  The task is out of reach in this test, so the run is left standing on its
  proposal.
- **The resume of a cancelled run belongs to the command that ends its
  wait.** The model's result on a cancelled run that still holds an
  evaluation closes none. The evaluation's outcome ends the run and closes
  one.
- **A park answered on a run that stands on its proposal closes a resume
  segment.** The cost of call 1, held: one resume on the model's result,
  none on the outcome that parks the run, one on the answer.

One unit test in `run.rs` holds `AgentRun::is_waiting` over every phase that
is a wait, every phase that is not under each status that is a wait, a
top-up request, a wind-down by status and by reason, and every ended status.

**On `main` at `49feccf`** five of the six whole-run tests fail, each at
the assertion it is named for, and the sixth (a run resting on its
children) holds what was already right. For the two tests of a resume that
moved, both commands were measured there: the model's result closes one
segment and the answer, or the outcome that ends the run, closes none. With
the change it is none and then one.

## 8. Breaks

Ten breaks were run one at a time, each against the unit tests and five
test files, and a survivor again against the whole crate. All ten are
caught.

- **What counts as a wait.** The status not asked, a top-up request not
  asked, a wind-down not asked: each by a whole-run test of its own and by
  the unit test. The phase not asked: by the unit test alone when the breaks
  were run; the test of a run resting on its children was added for it, and
  fails under that break. The status asked only for a reconciliation park:
  the unit test alone. An approval or an authorization wait at a phase that
  is no wait is not a state any whole-run test builds.
- **An ended run asked**: five tests, three of them existing tests of the
  segment.
- **A proposal with its task counted as a wait**: the answer's test.
- **The timer started on the phase alone**: the two tests of a resume that
  moved. **The segment closed on the phase alone**: every test of a run
  left held. **Both, which is the defect**: the same four.

The test of a park answered on a run that stands on its proposal was
written after the breaks were run. It fails on `main`, so it fails under the
last break, which is `main`'s code.

The whole crate passes: 1,884 tests.
