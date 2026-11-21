# The `run-resume` segment closes on what holds the run (issue #191)

2026-11-20. Short design, with my calls listed. The owner reviews at the pull
request. Section 9 says what the review of the first build changed: the
approach itself.

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

The crate's tests reach one more of the same kind: the last tool result of a
turn, on a run parked on an effect outside that turn, leaves it
`WaitingForReconciliation` at `RecordingTurn`.

Segments are observability. No state and no decision reads them.

## 2. Approaches

1. **Ask what holds the run after the command, and leave the timer where it
   is.** The segment's timer still starts when the command finds the run on
   a wait of its phase. Whether the segment closes is decided on the phase,
   the status, a top-up request and a wind-down. This can only take
   segments away.
2. **Ask the same question before the command and after it.** A command
   that finds the run held at a phase that is no wait, and leaves it on its
   proposal or ended, would close a segment too.
3. **Only the reconciliation park**, after the command.

I built the second first, and the review measured what it did (section 9).
This is the first. The third leaves two measured rows.

## 3. The rule

A command applied to a run closes `run-resume` when it finds the run on a
wait of its phase and its pass leaves the run resumed
(`AgentRun::has_resumed`).

A run that has ended has left its wait. A live run has resumed when nothing
holds it, and any of four things does:

- its phase is a wait on an effect or on its children;
- its status is a wait: a dispatch wait, an approval or an authorization, a
  reconciliation park;
- it is parked on a top-up request;
- the command found it winding down. A run that is winding down commits no
  new work, so nothing short of its end resumes it.

Two things are no hold. A proposal that awaits its task's decision never was
one. And a wind-down the command itself brought on is none: a run whose end
is decided within the command's pass, while it still holds a call it sent,
went on to that end and waits only to reach it.

"Winding down" is `AgentRun::winding_down`: the run is `Cancelling`, or its
end is decided and not yet reached.

**Measured over the crate's tests**, the tests of this change included. I
logged, for every command that finds the run on a wait of its phase, what
the phase alone answers and what this rule answers.

| | Commands |
|---|---|
| Find the run on a wait of its phase | 6,457 |
| Closed `run-resume` under the phase alone | 1,622 |
| Close it under this rule | 1,591 |
| Close it under this rule and not under the phase | 0 |

The 31 that no longer close one:

| The command left the run | Commands |
|---|---|
| Parked on a top-up request, `Running` at `PreparingContext` | 12 |
| Parked on an unknown outcome at `RecordingTurn`, from `AwaitingTools` | 9 |
| Parked on an unknown outcome at `RecordingTurn`, from `AwaitingChildren` | 5 |
| Winding down as it was found, `Cancelling` at `EvaluatingModelOutput` | 3 |
| Parked on an unknown outcome at `EvaluatingModelOutput` | 2 |

Of the 1,591 that still close one, 1,203 leave the run ended, 127 leave it
on its proposal, 241 leave it `Running` at a phase from which it can take a
step, 19 leave it at its decided end with a call still out, and one lost
its record after the run had ended. No command in the crate's tests leaves a
run on an approval, an authorization, a dispatch or a timer wait at a phase
that is no wait.

## 4. The change

`AgentRun::has_resumed`, and `AgentRunEntityStore::apply_unboxed`, which
reads whether the run is winding down where it starts the resume timer and
asks `has_resumed` where it closes the segment. Where the settle pass lost a
write and the record is gone, the segment still closes only if the run was
terminal at the commit, as before.

Nothing durable is added: no field, status, error code, decision kind or
segment class.

## 5. My calls

1. **Only the close changes; the timer starts on the phase, as it did.** So
   no command closes a segment it did not close before, and a replay closes
   what it closed.

   **What it costs.** A command that finds the run held at a phase that is
   no wait closes none, whatever it does, as before. The answer that takes
   a parked run from `EvaluatingModelOutput` on to its proposal is one. So
   the turn of a run parked at its model call closes no `run-resume`, where
   it closed one on the wrong command. Section 10 says what closing it on
   the right command would take.
2. **A wind-down the command found the run in is a hold too.** The issue
   named the park and the top-up request. The cancelled run is the same
   defect, measured.
3. **A wind-down counts only where the command found the run in it.** The
   model's result on an unheld run whose task accepts within the same pass,
   while the run holds an evaluation it sent, leaves the run winding down.
   That run went on: it evaluated, proposed and was accepted. It keeps its
   segment. Counting every wind-down would take the segment from those 19
   commands and close it nowhere.
4. **A proposal that awaits its task's decision is still no hold.** Where
   the task answers within the pass of the model's result, the run has
   ended by the time it is asked (885 of the crate's commands), and this
   call changes nothing. Where the task is not reached in that pass, the
   run is left on its proposal (127), and counting the proposal would take
   the segment from them: the task's decision is the reply to an exchange
   the run sent, taken in whichever later pass carries it, and that pass
   starts no timer.
5. **The status is asked by its own definition of a wait**
   (`AgentRunStatus::is_waiting`), and not only for the park. A run under
   an approval or an authorization wait cannot take a step either. Only the
   park reaches a whole-run test; the others are held by the unit test.

## 6. Mixed builds and rollback

Nothing is stored. An earlier build closes the segments it closed.

## 7. Tests

Nine whole-run tests and one unit test. Each whole-run test reads the
segments the run closed from an in-memory sink.

**The four that fail on `main` at `49feccf`**, each at the assertion it is
named for:

- `workflow_tool.rs`, **a wait resolved on a parked run closes no resume
  segment.** The run rests on its child, parked on a claim append whose
  outcome is unknown. The deadline, and in a second pass the child's
  report, resolves the wait and leaves the run parked at `RecordingTurn`.
  The answer then takes the run on to its next model call and closes none
  either.
- `run_end_waits.rs`, **a command that parks the run on a top-up closes no
  resume segment.**
- **A model result on a parked run closes no resume segment.** The answer
  then takes the run on to its proposal, with the task out of reach. It
  closes its `checkpoint-resolve` and no `run-resume`: the cost of call 1,
  held.
- **A model result on a cancelled run closes no resume segment.** The
  evaluation's outcome then ends the run and closes none, as before.

**The five that hold what the change leaves as it was**, and pass on
`main`:

- `workflow_tool.rs`, **a command that rests the run on its children closes
  no resume segment**: `AwaitingChildren` under `Running` is a wait by its
  phase.
- `run_end_waits.rs`, **a command that finds the run parked on a top-up
  closes no resume segment.** The evaluation's result lands, its pass
  carries the top-up request, the task refuses, and the run ends. Holds the
  timer to the phase against a top-up request.
- **A park answered on a run that stands on its proposal closes no resume
  segment.** Holds the timer to the phase against the status.
- **The command that ends a cancelled run closes its resume segment.** A
  run found winding down is held until it ends, and here it ends.
- **A run accepted while it holds a call closes its resume segment.** Call
  3.

One unit test in `run.rs` holds `AgentRun::has_resumed` over every phase
that is a wait, every phase that is not under each status that is a wait, a
top-up request, a wind-down found and one brought on, and every ended
status.

## 8. Breaks

Thirteen breaks were run one at a time, each against the unit tests and
five test files, with a survivor to be run again against the whole crate.
All thirteen are caught, and none needed the whole crate.

- **What holds a run.** The phase not asked after the command: three
  whole-run tests. The status not asked: the two tests of a parked run. A
  top-up request not asked: its test. A wind-down the command found not
  asked: the cancelled run's test. Each is caught by the unit test as well.
  The status asked only for a reconciliation park: the unit test alone
  (call 5).
- **What does not.** Every wind-down asked, found or brought on: the
  accepted run's test and the unit test. A proposal counted as a hold: the
  test of a park answered on a run that stands on its proposal, whose
  model's result is taken with the task out of reach.
- **An ended run not let through**: the test of the command that ends a
  cancelled run, and the unit test.
- **The timer.** Started on the status too: two tests. On a top-up request
  too: one. On a wind-down too: three, one of them an existing test. The
  wind-down never read before the command: the cancelled run's test.
- **The close asking the phase alone, which is the defect**: the four tests
  that fail on `main`.

The whole crate passes: 1,887 tests.

## 9. What the review found

A fresh reviewer read the first build, which asked the same question on
both sides (approach 2), with a log of every command in the crate's tests,
probes of its own and a mutation run.

- **A segment closed on a command that did nothing to the hold.** A top-up
  park, and a wind-down on a proposal, are ended by the reply to an exchange
  the run itself sent. Whichever pass carries that exchange takes the reply,
  and every committing command and every replay runs one. So a `Duplicate`
  replay closed a `run-resume`, and so did the grant of an unrelated gate.
  `main` cannot do this: no wait of the phase is ended by such a reply.
- **It changed 100 commands of the crate's tests**: 52 gained a segment and
  48 lost one. I had written "one case adds a span".
- **The largest change was in neither of my tables**: the run accepted
  while it holds a call (call 3) lost its segment on the model's result and
  gained it on the outcome that ends the run.
- **My reason for leaving the proposal out was false.** I wrote that
  counting it would move an ordinary run's segment. Where the task answers
  within the pass the run has ended, and nothing moves. Call 4 has the
  true trade.
- **A sentence was wrong about exchanges.** A task's decision and a
  top-up's answer are not delivered to the run. They are replies to
  exchanges the run sent.
- **Three parts of the predicate were held by the unit test alone**, and
  half of one test held nothing: a grant taken by a sweep, which closes no
  segment under any rule.
- **A product doc still stated the phase rule**, and two test comments.

This build is the answer to the first two: approach 1 adds no segment, so
it has no command to get wrong. The table of section 3 is my own log of this
build, taken the way the reviewer took theirs. The rest are corrected where
they stood.

## 10. Not done

- **The command that sets a held run going from a phase that is no wait
  closes no `run-resume`**, as before. Closing it there means starting the
  timer on more than the phase, and the review shows what that needs: a
  hold that only a command can end. A reconciliation park is close to one,
  but a settle pass also ends a stale park, and the answer to a park on a
  run that stands on its proposal would close a second segment for a run
  that still does nothing until its task decides.
- **A non-terminal run whose settle pass ends a wait and then loses a later
  write** closes no `run-resume` on either call, as the validation matrix
  already records under "Owed".
