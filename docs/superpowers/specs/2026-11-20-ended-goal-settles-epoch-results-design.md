# A goal's control task that has ended still settles its epochs' results (issue #181)

2026-11-20. Short design, with my calls listed. The owner reviews at the pull
request. Stacked on the fix for issue #179.

## 1. What was wrong

A continuous goal's control task can end while an epoch it admitted is still
out. The epoch goes on to its own end, returns its budget to the epoch task,
and the epoch task sends its `EpochResult` to the control task.

`apply_epoch_result` began with `continuous_task_mut`, which refuses a
terminal task. So the result was refused `task-terminal`. The refusal settles
the exchange, and nothing sends it again. The ended control task was left
with:

- one child of its ledger out for good;
- none of what the epoch spent counted;
- a wake controller that still showed the occurrence as active.

Measured with a person's result (`SubmitHumanResult`) on a human-owned control
task, both while an epoch was simply running and while an accepted epoch's run
was parked on an unknown outcome. The second is the window issue #178 opened,
and it can last until a person answers.

## 2. Approaches

1. **Refuse the person's result while the ledger has a child out.** The
   person waits for the epoch, or cancels it first.
2. **Let the ended control task still take an epoch's result,** as a
   settlement that returns the budget, releases the wake's place and moves no
   status.

I chose the second. The refusal is in the result's door, not in the door the
task ended through, so it bites however the task ended. A person's result is
one way. A policy that ends the goal while another epoch is out is a second,
and an agent-owned continuous goal whose own run is accepted while an epoch
is out is a third. I ran only the first; the other two are from reading. The
first approach closes one of those doors, and makes a person wait on a park
they may not be able to answer.

## 3. The rule

An epoch's result that reaches a control task that has ended is taken as a
settlement:

- the epoch's budget is settled and returned, as for any result;
- the wake's place is released, if it is still held;
- one `epoch-settled` history entry is recorded.

Nothing else happens. The task's status does not move. No occurrence is
promoted into the freed place, whatever the gate says. The outcome is not
counted toward a failure streak or a stagnation, since the goal can no longer
act on either.

A terminal task that is not a continuous goal's control task still refuses
the result `task-terminal`.

## 4. My calls

1. **The ended task takes the result** (section 2). Yours to reverse.
2. **Nothing is promoted.** The controller gains `release_ended`, a release
   that promotes nothing. The gate of a control task that a person completed
   is still open, so the ordinary release would have admitted the parked
   occurrence into a goal that is over.
3. **The outcome is not accounted.** `record_epoch_outcome` and
   `record_epoch_progress` can suspend or retire the gate and trip the
   stagnation policy. None of that means anything on an ended goal. The cost:
   the controller's failure streak and stagnation counters do not include
   that last epoch.
4. **The record's bound is checked with no reserve.** The settlement removes
   a ledger child and an active occurrence and adds one history entry. An
   ended record has no further growth to reserve for, and a refusal here
   would be the defect again.
5. **A person's result is still accepted over an epoch that is out.** The
   epoch runs to its own end and spends what it was given. I did not make
   the end of a goal cancel its epochs.
6. **Records already left this way are not repaired.** The epoch's result
   was refused and is not sent again.

## 5. Mixed builds and rollback

Nothing durable is added. An earlier build refuses the result as before, and
leaves the record as before.

## 6. Tests

Three whole-run tests in `crates/rakka-agent/tests/ended_goal_epoch_result.rs`
(new) and one in `waiting_end_task_side.rs`:

- An ended goal settles the result of an epoch that was running: the epoch's
  result is `accepted`, the control task stays `Completed` with its ledger
  closed, the epoch's iteration is counted, and the place is released.
- The same over an accepted epoch whose run was parked, answered after the
  person's result.
- An ended goal promotes nothing into the place its epoch frees: an
  occurrence parked behind the epoch stays parked, and no epoch is created
  for it.
- An ended task that is no continuous goal still refuses an epoch's result
  `task-terminal`.

One unit test in `wake.rs` holds `release_ended`. The first three whole-run
tests fail on the commit below this one, with the reply `task-terminal`.

Six breaks of the fix were run, one at a time, against the two test files and
the controller's unit tests: the ended task refusing as before, any ended
task taking the result, the place not released, the settlement skipped, the
ended goal's release promoting like any other, and `release_ended` not
counting the release. Each is caught by a test. The whole crate passes: 1,825
tests.

## 7. Not done

- Records already left with a refused result (call 6).
- The two other ways of ending a control task over an epoch (section 2) are
  not run.
- The end of a goal does not cancel the epochs it has out (call 5).
