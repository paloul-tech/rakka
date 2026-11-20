# A continuous goal admits an epoch only when its ledger can pay (issue #179)

2026-11-20. Short design, with my calls listed. The owner reviews at the pull
request.

## 1. What was wrong

An epoch's budget is debited from the goal's ledger when its wake is admitted.
`owe_epoch_creation` asks `escrow.open_child` for the wake policy's epoch
budget, and `open_child` grants that request narrowed to what the goal has
left: to zero where it has nothing, without refusing.

So a goal with nothing left in a dimension still admitted its next wake. The
epoch was created with zero in that dimension. Its task was refused its run on
every settle pass (`budget-unavailable`), stayed `Created`, and held the wake
that admitted it. The refusal landed on the epoch task, which carries no goal
record, so the goal's exhaustion policy was never asked: the goal stayed
`active` with its gate open.

The same epoch was created with budget left, where two epochs held budget at
once and the wake policy leaves a dimension open: the first epoch takes all
the goal has in it. An operator's `CompleteWakeOccurrence` on a running epoch
reaches that.

## 2. Approaches

1. **Decide at admission.** Before an occurrence is admitted, ask whether the
   goal's ledger can pay for its epoch. If it cannot, park the occurrence.
   Where nothing could refill the ledger, ask the goal's exhaustion policy in
   that transition.
2. **Let the epoch's refusal reach the goal.** Create the epoch as now. Its
   task reports a permanent refusal to the controller, which applies the
   policy and releases the wake.

I chose the first. The second leaves a `Created` task behind for every
refused occurrence, needs a new report from an epoch task to its controller,
and does nothing for the case with budget left: that epoch's budget is fixed
at zero when it is created, and nothing would carve it again when the first
epoch returns. The first creates nothing it cannot run, adds no exchange, and
handles both cases with the one question.

## 3. The rule

An occurrence is admitted only when the goal's ledger can pay for its epoch.

- **The question.** The ledger cannot pay where the goal itself has nothing
  left in a dimension the epoch's run works in. The epoch's grant would be
  empty there, and its task could never escrow a run.
- **An occurrence the goal cannot pay for is parked**, as one the goal window
  cannot pay for already is: disposition `Deferred`, naming the dimension. No
  epoch is created, no budget is debited, the goal window is not charged, and
  no place is held.
- **Where an epoch still out holds that dimension,** the occurrence waits for
  that epoch's result. The result returns budget and retries the admission.
- **Where no epoch still out holds it,** the goal's exhaustion policy
  decides: it parks the goal and closes the gate, or escalates, or ends the
  goal and its control task. A resume that tops the goal up admits the
  occurrence that waited.
- **A control task with no goal record, or whose goal is only proposed,** has
  no policy that can act. Its occurrence stays parked.

## 4. How

The wake controller holds no ledger. It is told, with each admission, the
dimension in which the goal's ledger cannot pay, if there is one:

- `admit_within`, `promote_admittable_within` and `release_within` take that
  answer. `admit`, `promote_admittable` and `release` keep their signatures
  and mean "the ledger can pay".
- `holds_admittable` says whether a parked occurrence could take a free place
  now, were it paid for.

The goal's task reads its ledger before each admission
(`epoch_ledger_shortfall`), in all six transitions that can admit: a delivery,
an epoch's result, an operator's release, and the three resumes. That is seven
readings: a delivery reads twice, and an epoch's result has two ways to
promote, the release and the late result's retry, on one reading. After a
delivery and after an epoch's result it runs `settle_epoch_ledger_gate`,
which asks the exhaustion policy when the shortfall is permanent and a parked
occurrence is what waits on it.

## 5. My calls

1. **Decided at admission** (section 2). Yours to reverse.
2. **`Deferred` is reused.** It already means "parked because its epoch
   cannot be paid for", for the goal window. No disposition, status, field or
   error code is added. The cost: the disposition does not say whether the
   window or the ledger refused. Where the policy was asked, the goal's wait
   reason says it was the ledger; with no contract nothing says.
3. **Only a dimension the run works in counts.** A grant with no descendants
   left only disables delegation, as for a task's own run.
4. **Permanent means no epoch still out holds any of that dimension.** The
   goal's own run, on an agent-owned goal, is not counted as relief. Its
   budget comes back only when the task ends or its next generation takes it
   again. The first version counted it, and the review showed the result: the
   policy was never asked, and the goal read `active` over wakes it could
   never admit. **This is a behaviour change for an agent-owned continuous
   goal whose own run takes all of a bounded dimension:** its policy is asked
   at its first wake, and under a terminate policy the goal ends there. Yours
   to reverse.
5. **The policy is asked at a delivery and at an epoch's result.** It is not
   asked in the three resumes or at an operator's release. A resume that
   leaves the ledger short admits nothing. The cost, which the review ran: a
   resume that tops up one dimension and leaves another empty leaves the goal
   `Active` over a parked occurrence until the next delivery or result. I did
   not want a resume to park the goal again inside its own reply. Yours to
   reverse.
6. **A late result retries the parked occurrence.** An epoch whose wake was
   released by hand still returns its budget with its result. That result
   used to promote nothing, since its wake was no longer active. An occurrence
   that waited for the budget would then wait for the next delivery. Now the
   late result retries it. This is new behaviour for a late result.
7. **Two admissions in one transition do not count on the same budget.** A
   delivery can promote a parked occurrence and admit the fresh one. The
   ledger is read again after the first epoch is debited.
8. **The policy is asked only of a goal that is authorized or parked.** A
   goal that is only proposed cannot be ended or parked for budget, and the
   terminate arm of the policy fails on one. The review reproduced that
   failure through the first version: inside an epoch's result it became a
   durable refusal that held the wake for good. A proposed goal's occurrence,
   and that of a task with no goal record, is parked and nothing else.
9. **A wake policy's own zero is not the goal's exhaustion.** The ledger is
   what refuses only where the goal itself has nothing left. Where the goal
   still has budget and the policy grants none of it, no top-up could help.
   The first version read the empty grant as the goal's exhaustion and, under
   a terminate policy, ended the goal at its first wake. Such a policy is now
   left as it was on `main`: the epoch is admitted and cannot run. Refusing
   the policy when it is set would be the fix, and is not done here.
10. **A stuck epoch that an earlier build created is not repaired.** It still
    holds its wake.

## 6. Mixed builds and rollback

Nothing durable is added. An earlier build reads a `Deferred` occurrence as
parked for the goal window, and retries it when its window can pay. It then
admits it into an epoch with no budget, which is the defect this fixes.

## 7. Tests

`crates/rakka-agent/tests/epoch_admission_ledger.rs`, new, fourteen tests:

- A goal out of budget parks at the occurrence it cannot pay for: the
  occurrence is `Deferred` on the dimension, no epoch task exists, nothing is
  held, the goal is `Waiting` on a budget exhaustion and its gate is closed.
  The goal is not parked before that occurrence comes. A resume with a
  top-up admits it, and its epoch runs.
- Under a terminate policy the goal and its control task end at that
  occurrence.
- A control task with no goal record parks the occurrence, and the next one
  takes the one parking place.
- An occurrence waits for the budget a running epoch holds, after an
  operator released that epoch's wake by hand. The goal is not parked. The
  occurrence is admitted when the epoch's result returns the budget.
- A late result that returns nothing promotes nothing, and the goal parks.
- An occurrence parked behind the last epoch the goal can pay for is not
  promoted when that epoch's result frees the place, and the goal parks.
- Two admissions in one transition do not count on the same budget (call 7).
- An operator's resume of the gate, a goal's activation, and a goal's resume
  that relieves one dimension of two each promote nothing the goal cannot
  pay for (call 5).
- An agent-owned goal whose own run holds the budget asks its policy at the
  first occurrence (call 4).
- A proposed goal whose gate an operator opened is not asked its policy, and
  the delivery is answered (call 8).
- A wake policy's own zero is not read as the goal's exhaustion (call 9).
- Under a policy that allows two epochs at once, the second occurrence waits
  for the first epoch's result, and the goal is not parked for it (call 4).

Three unit tests in `wake.rs` hold the controller's side: a ledger that
cannot pay defers the occurrence and charges no window; a release told so
promotes nothing; and an occurrence past its admission window, a missed one
and one replayed under bounded catch-up are each deferred.

The first eight of these fail on `main`'s `task.rs` and `wake.rs`. Twenty-four
breaks of the fix as it now stands were run, one at a time, against this file
and the controller's unit tests. Each is caught by a test:

- in the shortfall: the controller never told; every shortfall read as
  permanent; the goal's own run counted as relief; permanent read as "no
  child out at all"; an empty grant read as the goal's own zero;
- in the policy call: never asked; asked of a proposed goal; asked whether or
  not an occurrence waits;
- at the seven readings: the ledger not read again after a promotion; an
  epoch's result, an operator's release, a gate resume, an activation and a
  goal's resume each promoting without asking; the late result retrying
  nothing, and retrying without asking;
- in the controller: admitting and promoting whatever it is told; charging
  the window before the ledger is asked; a release telling its promotion the
  ledger can pay; a missed and a late occurrence admitted whatever the ledger
  says.

The whole crate passes: 1,827 tests.

**Existing tests whose worlds changed.** Thirteen tests in
`wake_coalescing.rs`, `wake_fencing.rs`, `wake_scanner.rs` and
`goal_window.rs` failed with the fix: an admission came back `Deferred`, the
first in the three `wake_*` files and the second in `goal_window.rs`. Each
admits occurrences and never runs their epochs. Their goal allowed three loop
iterations, and either the goal's own run held all three (the agent-owned
fixture) or the first epoch did. So every epoch those tests
admitted was one that could never run. Their subjects are coalescing,
fencing, the scanner and the goal window, not budgets. The agent-owned
fixture and the windowed test now give the goal a ledger with no ceiling
(`unbounded_task_definition`), and no expectation in them changed. **Yours to
reverse.**

## 8. The review

A fresh reviewer read the first two commits, ran probes of its own and
twenty breaks. The third commit answers it.

- **A new failure.** Under a terminate policy, a proposed goal whose gate an
  operator had opened made the policy call fail. At a delivery the wake was
  refused on every scan. Inside an epoch's result the failure was the
  exchange's durable refusal: the settlement was dropped and the wake held
  for good. On `main` the same result is accepted. Fixed by call 8, with a
  test.
- **A wrong diagnosis.** A wake policy's own zero was reported as the goal's
  exhaustion, and a terminate policy ended the goal for it. Fixed by call 9,
  with a test.
- **The policy never asked** on an agent-owned goal while its own run held
  the budget. Fixed by call 4, with a test.
- **Two breaks that put the defect back survived my tests:** the goal's
  resume and the late result's retry each passing "the ledger can pay". Each
  now has a test. So do the three admission paths for a late or missed
  occurrence, which also survived.
- **The policy call at an operator's release is removed.** It could fire only
  for an epoch an earlier build created with no budget, and no test held it.
- **Five sentences of the docs were false or unqualified,** and one comment
  in `wake.rs`. Corrected.
- The reviewer confirmed the count of thirteen and the diagnosis of the
  fixture changes, and that nothing those tests asserted was lost.

## 9. Not done

- Records an earlier build left with a stuck epoch (call 10).
- The policy is not asked inside a resume (call 5).
- A wake policy that grants nothing in a dimension its epoch's run asks for
  is still accepted (call 9).
- A ledger that already holds as many children as it can makes the epoch's
  creation fail, and inside an epoch's result that is a refusal. It was so
  before this change. Read by the reviewer, not run.
- `holds_admittable` does not look at the goal window, so the policy can be
  asked while the window could not pay either.
- No test runs an escalate policy, a backoff in force when a place comes
  free, or a delivery that promotes an occurrence parked for the ledger. The
  reviewer ran the backoff and parallel epochs with an open dimension, and
  found both sound.
- No test runs an agent-owned continuous goal on a bounded ledger beyond the
  one of call 4.
- **Budget that comes back from the goal's own run retries nothing.** An
  agent-owned continuous goal has a run of its own, whose budget is a child
  of the same ledger. When that run returns budget and the task goes on (a
  refused or failed generation), the transition is the ledger's, not one of
  the six above, and it promotes nothing. An occurrence parked for that
  budget waits for the next delivery, result, release or resume. I left it:
  the next generation of that run usually takes the budget straight back.
- `open_child` still narrows to zero without refusing. Admission no longer
  asks it for an epoch it cannot pay for, and the task's own run is refused
  before it is asked.
