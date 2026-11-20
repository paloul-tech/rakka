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

- **The question** is the one the epoch's own task would ask before its first
  run: the wake policy's epoch budget, narrowed to what the goal has left, is
  empty in a dimension the epoch's run asks for.
- **An occurrence the goal cannot pay for is parked**, as one the goal window
  cannot pay for already is: disposition `Deferred`, naming the dimension. No
  epoch is created, no budget is debited, the goal window is not charged, and
  no place is held.
- **Where an epoch still out holds that dimension,** the occurrence waits for
  that epoch's result. The result returns budget and retries the admission.
- **Where nothing still out holds it,** the goal's exhaustion policy decides,
  in the same transition: it parks the goal and closes the gate, or escalates,
  or ends the goal and its control task. A resume that tops the goal up admits
  the occurrence that waited.
- **A control task with no goal record** has no policy. Its occurrence stays
  parked.

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
an epoch's result, an operator's release, and the three resumes. After a
delivery, a result or a release it runs `settle_epoch_ledger_gate`, which
asks the exhaustion policy when the shortfall is permanent and a parked
occurrence is what waits on it.

## 5. My calls

1. **Decided at admission** (section 2). Yours to reverse.
2. **`Deferred` is reused.** It already means "parked because its epoch
   cannot be paid for", for the goal window. No disposition, status, field or
   error code is added. The cost: the disposition alone does not say whether
   the window or the ledger refused; the dimension and the goal's wait reason
   do.
3. **Only a dimension the run works in counts.** A grant with no descendants
   left only disables delegation, as for a task's own run.
4. **Permanent is read as it is for a task's own run:** no child still out
   holds any of that dimension.
5. **The policy is asked where an occurrence arrives or a place comes free:**
   a delivery, an epoch's result, an operator's release. It is not asked in
   the three resume transitions. A resume that leaves the ledger short admits
   nothing, and the next delivery or release asks the policy. I did not want a
   resume to park the goal again inside its own reply.
6. **A late result retries the parked occurrence.** An epoch whose wake was
   released by hand still returns its budget with its result. That result
   used to promote nothing, since its wake was no longer active. An occurrence
   that waited for the budget would then wait for the next delivery. Now the
   late result retries it. This is new behaviour for a late result.
7. **Two admissions in one transition do not count on the same budget.** A
   delivery can promote a parked occurrence and admit the fresh one. The
   ledger is read again after the first epoch is debited.
8. **A control task with no goal record parks the occurrence and does nothing
   else.** There is no policy to ask and no resume that tops it up.
9. **A stuck epoch that an earlier build created is not repaired.** It still
   holds its wake. Not run: what an operator's release and a cancel of that
   epoch task do to it.

## 6. Mixed builds and rollback

Nothing durable is added. An earlier build reads a `Deferred` occurrence as
parked for the goal window, and retries it when its window can pay. It then
admits it into an epoch with no budget, which is the defect this fixes.

## 7. Tests

`crates/rakka-agent/tests/epoch_admission_ledger.rs`, new, eight tests:

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
- An occurrence parked behind the last epoch the goal can pay for is not
  promoted when that epoch's result frees the place, and the goal parks.
- Two admissions in one transition do not count on the same budget (call 7).
- An operator's resume of the gate, and a goal's activation, each promote
  nothing the goal cannot pay for (call 5).

Two unit tests in `wake.rs` hold the controller's side: a ledger that cannot
pay defers the occurrence and charges no window, and a release told so
promotes nothing.

**Existing tests whose worlds changed.** Thirteen tests in
`wake_coalescing.rs`, `wake_fencing.rs`, `wake_scanner.rs` and
`goal_window.rs` failed with the fix: their first admission came back
`Deferred`. Each admits occurrences and never runs their epochs. Their goal
allowed three loop iterations, and either the goal's own run held all three
(the agent-owned fixture) or the first epoch did. So every epoch those tests
admitted was one that could never run. Their subjects are coalescing,
fencing, the scanner and the goal window, not budgets. The agent-owned
fixture and the windowed test now give the goal a ledger with no ceiling
(`unbounded_task_definition`), and no expectation in them changed. **Yours to
reverse.**

## 8. Not done

- Records an earlier build left with a stuck epoch (call 9).
- The policy is not asked inside a resume (call 5).
- `open_child` still narrows to zero without refusing. Admission no longer
  asks it for an epoch it cannot pay for, and the task's own run is refused
  before it is asked.
