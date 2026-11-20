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
was parked on an unknown outcome. The second is the window pull request #178
opened, and it can last until a person answers.

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
is out is a third. The first and the third have kept tests. The reviewer ran
the second, with a stagnation policy that ended the goal over two epochs, and
both results settled. The first approach closes one of those doors, and makes
a person wait on a park they may not be able to answer.

## 3. The rule

An epoch's result that reaches a control task that has ended is taken as a
settlement:

- the epoch's budget is settled and returned, as for any result;
- the wake's place is released, if it is still held;
- one `epoch-settled` history entry is recorded;
- the record's `updated_at` moves, and the `rakka.agent.epochs` metric counts
  the epoch's outcome, as for any result;
- the reports the ended task itself owes upward are owed, if this settlement
  closed its ledger (call 7).

The task's status does not move. No occurrence is promoted into the freed
place, whatever the gate says. The outcome is not counted toward a failure
streak or a stagnation, since the goal can no longer act on either.

A result for a wake the ended task never admitted is accepted too. It settles
and releases nothing, and one history entry records it. A control task that
has not ended answers the same way: the only fence in either is that the
result comes from the epoch task its wake derives.

A terminal task that is not a continuous goal's control task still refuses
the result `task-terminal`.

## 4. My calls

1. **The ended task takes the result** (section 2). Yours to reverse.
2. **Nothing is promoted.** The controller gains `release_ended`, a release
   that promotes nothing. The gate of a control task that a person completed
   is still open, so the ordinary release would have admitted the parked
   occurrence into a goal that is over.
3. **The outcome is not accounted.** On a live goal `record_epoch_outcome`
   can suspend the gate, `record_epoch_progress` can trip the stagnation
   policy, and the result's own transition can retire the gate. None of that
   means anything on an ended goal. The cost: the controller's failure streak
   and stagnation counters do not include that last epoch.
4. **The record's bound is checked with no reserve.** The settlement takes a
   ledger child and an active occurrence off the measured record and adds
   nothing to it; history is kept beside the record. An ended record has no
   further growth to reserve for, and a refusal here would be the defect
   again. No test holds the zero: it would need a record within the reserve
   of its bound.
5. **A person's result is still accepted over an epoch that is out.** The
   epoch runs to its own end and spends what it was given. I did not make
   the end of a goal cancel its epochs.
6. **Records already left this way are not repaired.** The epoch's result
   was refused and is not sent again.
7. **The settlement owes the ended task's own reports.** A control task can
   be a delegated child: creation refuses a delegated child a goal contract,
   not a continuous mode. Such a child owes its parent run a result once it
   has ended and its ledger is closed, and here the epoch's result is what
   closes it. The reviewer read that the settlement did not consult the owed
   reports; a test then showed the parent was never told. The settlement now
   consults them, as the ledger exchanges do.

## 5. Mixed builds and rollback

Nothing durable is added. An earlier build refuses the result as before, and
leaves the record as before.

## 6. Tests

`crates/rakka-agent/tests/ended_goal_epoch_result.rs`, new, eight tests. Three
run an epoch to its end, and five hand the control task a result as the
epoch's task would send it.

- An ended goal settles the result of an epoch that was running: the epoch's
  result is `accepted`, the control task stays `Completed` with its ledger
  closed, the epoch's iteration is counted, and the place is released.
- An ended goal promotes nothing into the place its epoch frees: an
  occurrence parked behind the epoch stays parked, and no epoch is created
  for it.
- A goal ended by its own run's result, on an agent-owned continuous goal,
  settles its epoch's result the same way.
- An ended goal records a failed epoch's settlement and counts no failure:
  the reply names the released wake and no successor, one `epoch-settled`
  entry is recorded, the metric counts the failure, the record's stamp moves,
  and the failure streak stays at zero. A second test shows a live goal's
  streak growing on the same result.
- An ended goal settles an epoch whose wake was released by hand: the budget
  returns, the reply names no release, and a second delivery of the same
  result is answered from the record.
- An ended control task that is a delegated child tells its parent when the
  epoch's result closes its ledger (call 7).
- An ended task that is no continuous goal still refuses an epoch's result
  `task-terminal`.

One more in `waiting_end_task_side.rs`: the same settlement over an accepted
epoch whose run was parked, answered after the person's result. One unit test
in `wake.rs` holds `release_ended`.

On the commit below the fix, the two tests of a person's result over a running
or parked epoch fail with the reply `task-terminal`, and the test that nothing
is promoted fails on the controller's counts.

Fourteen breaks of the fix as it now stands were run, one at a time, against
the two test files and the crate's unit tests. Thirteen are each caught by a
test:

- the ended task refusing as before, and any ended task taking the result;
- the settlement skipped, and the epoch's spend not counted;
- the place not released, `release_ended` not counting the release, and the
  ended goal's release promoting like any other;
- a wake no longer active refusing the result;
- no history entry, the record's stamp not moving, and the reply carrying no
  release;
- the outcome accounted on the ended goal;
- the ended task's own reports not owed.

One survives: the bound checked with the growth reserve in place of zero
(call 4). The whole crate passes: 1,837 tests.

## 7. The review

A fresh reviewer read the first two commits and ran probes and twelve breaks.
It found no defect in the ended branch. What changed in answer:

- **The delegated control task** (call 7): read by the reviewer, then
  reproduced by a test, then fixed.
- **Four things the docs stated and no test held:** the history entry, the
  outcome left unaccounted, the release in the reply, and the record's stamp.
  Each is now asserted. The zero reserve of call 4 is still unheld.
- **Two of the three ways a control task ends over an epoch were from
  reading.** One now has a kept test, and the reviewer ran the other.
- **Sentences corrected:** #178 is a pull request; "nothing else happens"
  left out the metric, the stamp and the owed reports; neither accounting
  function retires the gate; the bound does not measure history; the count
  and kind of the tests.

## 8. Not done


- Records already left with a refused result (call 6).
- A policy that ends the goal over an epoch (section 2) has no kept test.
- **A control task with no goal contract that is asked to cancel keeps
  admitting epochs.** Found by the reviewer and reproduced; it is not from
  this change. A cancelling task is not terminal, so its epochs' results take
  the live path, and each release promotes the next parked occurrence. Its
  controller is retired at the request only when the task has a goal record.
  Drafted as an issue, not filed.
- The end of a goal does not cancel the epochs it has out (call 5).
