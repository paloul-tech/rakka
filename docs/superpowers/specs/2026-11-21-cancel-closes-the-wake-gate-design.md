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

The epochs already admitted run to their own ends, including one whose
creation was still on its way to its task when the request came, and the
cancellation finalizes once the last of them has settled.

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
   open: each of these retires the gate before it asks it, so none admits.
   The first that commits saves the retirement. One that is refused saves
   nothing, the close included: a resume on such a record is always refused,
   and leaves the stored gate as it was. `ActivateGoal` and `ResumeGoal` also promote, but both
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
   today for a task with a goal record. The two requests the settle pass
   makes (unclaimed expiry, an ended goal's cancellation) count no lifecycle
   change; that is so already for a task with a goal record, and I left it.
4. **Ended records are not repaired** (the issue's third question). A
   control task that an earlier build let end `Cancelled` still has its
   gate `Active`, and its task snapshot shows it so. Nothing acts on that
   gate: a terminal task refuses every wake command `task-terminal`, and an epoch's result reaching it is taken as a
   settlement that promotes nothing (issue #181).
5. **The cancel still does not cancel the epochs out.** That is the existing
   rule ("the end of a goal does not cancel its epochs"); this change does
   not touch it.

## 4. What a deployment sees

A control task with no goal record stops admitting when it is asked to
cancel. An occurrence delivered while its epochs finish is `barred` rather
than admitted or coalesced. Its gate reads `retired` from the request on,
with its lifecycle revision moved by one (a gate that had already expired
stays `expired`), and the lifecycle metric counts the retirement when the
request comes by a command or an exchange. The operator's workaround the issue names (`RetireContinuousGoal`
on the cancelling task) is no longer needed, and is refused
`wake-lifecycle-terminal` because the gate is already retired. An occurrence
left parked behind the last epoch stays parked, as it does for a task with a
goal record.

## 5. Mixed builds and rollback

Nothing durable is added and no record changes shape. An earlier build that
loads a record this build closed reads a retired gate, and its own admission
paths fence on it, so it admits nothing either. A cancellation an earlier
build records leaves the gate open, and that build goes on admitting on it;
this build retires the gate when it next commits one of the four
transitions of call 2 on the record. A second cancellation request does not
retire it: a request that finds the cancellation recorded is answered
without a transition, and I left that contract alone.

## 6. Tests

`crates/rakka-agent/tests/cancel_closes_the_wake_gate.rs`, new, sixteen
tests (fifteen before the review, section 7). The issue's reproduction runs with real epochs: the scanner's
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
- The lifecycle metric counts one `retired`, at the cancel, and A's result
  handed to an entity that counts adds no second one. A record an earlier
  build left open is counted once, by the epoch result that retires its
  gate, and that result records no history entry for it.
- A delegated control task told to cancel by its parent run's
  `DelegationCancel` retires its gate, admits nothing on A's result, and
  owes its parent its `DelegationResult` once A has settled.
- A control task with a goal record, measured the same way: gate retired,
  goal `Cancelled` under `root-task-cancelled`, A's result admits nothing.
  It passed before the change too; it holds the contrast the issue draws.
- The two cases the issue did not run, both right before this change and
  held as they are: a goal record that is `Proposed` when the cancel
  arrives (nothing is out, so the task ends at once, its goal decided
  `root-task-cancelled` and its gate retired), and a goal record decided
  before the cancel (the decision retired the gate, and A's result admits
  nothing).
- One test per admitting transition of call 2 (an epoch's result, a
  delivery, `CompleteWakeOccurrence`, `ResumeContinuousGoal`), over a
  record an earlier build left cancelling with the gate open (or suspended,
  for the resume; the refused resume leaves the stored gate suspended). The record is made by this build and then given back the
  gate it had before the cancel, which is the only thing an earlier build's
  cancel did not change on the controller.

Under the fixture's bounded definition the goal's ledger cannot pay for B
while A holds its budget, so a release by hand or a resume promotes nothing
there anyway. The tests of those two doors use a control task with no
ceiling, on which, before this change, both admitted B.

On `main`'s `task.rs`, twelve of the fifteen fail, each on the defect; the
goal-record control and the two cases above pass.

Nine breaks of the fix were run one at a time against the whole crate
(1,929 tests at the time). Each is caught:

- the request's close removed: five tests fail. The tests that drive an
  admitting transition still pass, because that transition closes the
  gate itself;
- each of the four admitting transitions' close removed, one at a time:
  exactly one test fails each time, the matching earlier-build test;
- the close made unconditional (every gate retired, cancelled or not):
  82 tests fail;
- the close moved after the lifecycle read, in a delivery, an epoch's
  result and a release by hand: exactly one test fails each time, on the
  `goal-retired` history entry the move records.

## 7. The review

A fresh reviewer read the change at `403094d5` and ran probes. It found no
path that still admits or creates an epoch after the request. What changed
in answer:

- **A clippy failure** in the new test file (`filter_map_bool_then`), which
  `scripts/validate.sh` had also caught. Fixed.
- **"Closed by the first of the four transitions that touches it" was
  false.** A command or exchange runs on a copy of the record, so one that
  is refused saves nothing of the close. A resume on a cancelling task is
  always refused. The doc comment, the compatibility entry and call 2 now
  say that the first that commits saves it. The resume test is renamed
  (`an_earlier_builds_suspended_gate_refuses_the_resume`) and now asserts
  the stored gate is still suspended after the refusal.
- **The metric test's "no second retirement" could not fail.** The
  in-process courier builds its entities with no recorder, so A's result
  was never counted. The test now hands the result to an entity that
  counts, and a new test holds that the repair of an earlier build's
  record is counted once. Breaking the epoch result's close fails both
  that test and the earlier-build epoch-result test.
- **Sentences made true:** the metric counts the request only when it
  comes by a command or an exchange; a gate that had already expired stays
  expired; the task snapshot does show an ended record's gate `active`;
  an earlier build still admits on a cancellation it recorded; a second
  cancellation request closes nothing; and "the epochs already out" now
  reads "already admitted", since an epoch admitted before the request
  whose creation was still on its way is created and run after it. The
  overview sentence in `docs/rakka-agents.md` moved after the straggler
  sentence it had split.

## 8. Not done

- **An epoch admitted before the request is still created and run after
  it.** The reviewer showed it: A's result admits B and owes B's creation,
  the cancel then retires the gate, and the cancel's own settle pass
  delivers B's creation, whose run sends its model call. The same happens
  for a task with a goal record, so it is not from this change, but
  specification 8.7 reads as fencing that dispatch. Not filed.
- A second cancellation request on a record an earlier build left open does
  not retire its gate (section 5).
- The settle pass's two cancellation requests count no lifecycle change
  (call 3).
