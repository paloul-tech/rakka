# A cancel closes a continuous control task's wake gate, goal record or not (issue #198)

2026-11-21. Short design, with my calls listed. The owner reviews at the pull
request.

## 1. What was wrong

A continuous goal's control task that holds no goal record (a continuous
goal mode, no goal contract) was asked to cancel while one epoch was out and
one occurrence was parked behind it. The cancellation was recorded, and the
wake gate stayed `Active`. Each epoch result was then taken as if from a live
goal. It released its occurrence and admitted the next parked one, and that
occurrence's epoch was created, assigned and run. The cancellation finalizes
only when the ledger has nothing out, so the task went on starting epochs for
as long as occurrences kept coming. The issue measured two epochs admitted
and run after the cancel was accepted.

`request_task_cancellation` says admission closes at the request. Its only
closing act was `project_goal_decision`, which retires the controller only
in the arm for a goal record that is not yet terminal. With no goal record
it did nothing. Nothing on the admission path reads `task.cancellation`, so
the gate was the only fence, and it was still open.

Specification 8.7: "Acceptance of a cancellation request MUST immediately
fence new model, tool, workflow, and delegation dispatch for the affected
scope." A new epoch's run is new dispatch.

A control task with a goal record does not do this. Its cancel decides the
goal `Cancelled` and retires the gate in the same compare-and-set.

## 2. The rule

A cancellation request closes the task's wake gate. The gate is retired
whether or not the task holds a goal record, in the compare-and-set that
records the request.

Every way a task is asked to cancel takes `request_task_cancellation`, the
only place that sets the marker: the `Cancel` command, a parent run's
`DelegationCancel`, a failed dependency, the unclaimed-expiry sweep, and the
settle pass's cancellation of a goal that ended. So the rule holds for all
of them. A delegated control task is the case that matters
most here: creation refuses a delegated child a goal contract, not a
continuous mode, so every delegated control task has no goal record, and its
parent's cancel reaches it as a `DelegationCancel`.

Once the gate is retired, the controller's own fences apply, as they already
do for a task with a goal record:

- an epoch's result releases its occurrence and admits nothing in its place;
- an occurrence delivered meanwhile is `barred` (`retired`), and the scanner
  marks its timer entry terminal;
- an operator's `CompleteWakeOccurrence` releases and admits nothing;
- `ResumeContinuousGoal`, `SuspendContinuousGoal`, `RenewContinuousGoal` and
  `RetireContinuousGoal` are refused `wake-lifecycle-terminal`.

The epochs already out run to their own ends, and the cancellation finalizes
once the last of them has settled.

## 3. My calls

1. **The request retires the gate whenever the task has one** (the issue's
   first question). A task with a goal record is unchanged: its goal
   decision has already retired the gate, the close finds it retired, and
   the decision does not read the gate, so its reason is still
   `root-task-cancelled`.
2. **Every transition that can admit closes the gate first when the
   cancellation is recorded** (the issue's second question). These are the
   four that can admit on a cancelling task: an epoch's result, a delivery,
   `CompleteWakeOccurrence` and `ResumeContinuousGoal`. The gate stays the
   one fence, but it no longer rests on the request having closed it. This
   is what stops a record an earlier build left cancelling with its gate
   open: the first of these to touch it after the upgrade retires the gate
   before it asks it. `ActivateGoal` and `ResumeGoal` also promote, but both
   need a goal record, and a cancelling task's goal record was decided at the
   request, so both refuse before they could promote. They are left alone.
   Yours to reverse: the alternative is to fix only the request and leave
   such records to admit until their queue runs dry, or until an operator
   retires the gate.
3. **No history entry and no new vocabulary.** The `cancellation-requested`
   entry stands for the closing, as the `goal-decided` entry does for a task
   with a goal record. The close runs before an admitting transition reads
   its lifecycle, so it is never recorded as an observed lifecycle change.
   The `rakka.agent.goal.lifecycle` metric counts `retired` once, from the
   status difference across the transition that closed the gate, as it does
   today for a task with a goal record.
4. **Ended records are not repaired** (the issue's third question). A
   control task that an earlier build let end `Cancelled` still reads its
   gate `Active`. Nothing reads that gate: a terminal task refuses every wake
   command `task-terminal`, and an epoch's result reaching it is taken as a
   settlement that promotes nothing (issue #181).
5. **The cancel still does not cancel the epochs out.** That is the existing
   rule ("the end of a goal does not cancel its epochs"); this change does
   not touch it.

## 4. What a deployment sees

A control task with no goal record stops admitting when it is asked to
cancel. An occurrence delivered while its epochs finish is `barred` rather
than admitted or coalesced. Its gate reads `retired` from the request on,
with its lifecycle revision moved by one, and the lifecycle metric counts the
retirement. The operator's workaround the issue names (`RetireContinuousGoal`
on the cancelling task) is no longer needed, and is refused
`wake-lifecycle-terminal` because the gate is already retired. An occurrence
left parked behind the last epoch stays parked, as it does for a task with a
goal record.

## 5. Mixed builds and rollback

Nothing durable is added and no record changes shape. An earlier build that
loads a record this build closed reads a retired gate, and its own admission
paths fence on it, so it admits nothing either. A record an earlier build
left cancelling with the gate open is closed by this build (call 2).

## 6. Tests

`crates/rakka-agent/tests/cancel_closes_the_wake_gate.rs`, new, thirteen
tests. The issue's reproduction runs with real epochs: the scanner's
delivery, an epoch task and its run, the model call answered.

- The cancel retires the gate of a control task with no goal record, moves
  its lifecycle revision by one and nothing else on the controller, and adds
  no history entry beyond the request's.
- The issue's measurement, reversed: A's result releases A and admits
  nothing; B's epoch is never created; the task is `Cancelled` once A has
  settled, with B still parked; the history has no `epoch-admitted` after
  the request.
- An occurrence delivered while the cancelling task waits for its epoch is
  `barred` (`retired`), and its timer entry is marked terminal.
- `CompleteWakeOccurrence` on the cancelling task releases and admits
  nothing.
- A cancel that finds the gate suspended retires it, and the operator's
  `ResumeContinuousGoal` is refused `wake-lifecycle-terminal`.
- `SuspendContinuousGoal`, `RenewContinuousGoal` and `RetireContinuousGoal`
  are refused `wake-lifecycle-terminal` after the cancel.
- The lifecycle metric counts one `retired`, at the cancel, and no second
  one by the time the task is `Cancelled`.
- A delegated control task told to cancel by its parent run's
  `DelegationCancel` retires its gate, admits nothing on A's result, and
  owes its parent its `DelegationResult` once A has settled.
- A control task with a goal record, measured the same way: gate retired,
  goal `Cancelled` under `root-task-cancelled`, A's result admits nothing.
  It passed before the change too; it holds the contrast the issue draws.
- One test per admitting transition of call 2 (an epoch's result, a
  delivery, `CompleteWakeOccurrence`, `ResumeContinuousGoal`), over a
  record an earlier build left cancelling with the gate open (or suspended,
  for the resume). The record is made by this build and then given back the
  gate it had before the cancel, which is the only thing an earlier build's
  cancel did not change on the controller.

Under the fixture's bounded definition the goal's ledger cannot pay for B
while A holds its budget, so a release by hand or a resume promotes nothing
there anyway. The tests of those two doors use a control task with no
ceiling, on which, before this change, both admitted B.

On `main` before the change, twelve of the thirteen fail, each on the
defect; the goal-record control passes.
