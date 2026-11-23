# A claim checks its lease right after the reconciler answers (issue #212)

Status: design record for issue #212. It is stacked on #207's branch
(PR #213), whose check in `authorize_attempt` it mirrors.

## 1. What is wrong

The reconciler can answer after the claim's lease lapsed. By then another
worker may hold the ticket and have its retry out. In `reconcile_ambiguous`
(`crates/rakka-agent/src/dispatch.rs`) only a finding of "not executed" is
checked against the lease, and only where an attempt follows it (#171, #195).
Every other finding is acted on:

- **`Unknown` on a live run** burns an attempt: it records a failure on the
  row unless the row is finished, without asking which worker wrote it.
  Over another worker's `Dispatching` mark, that records a failure over the
  worker's call in flight. Where it spends the budget, the call is parked:
  `tell_indeterminate` tells the run, and only then does the fenced fleet
  write end the pass in `claim-fenced`. The review's probe A: a budget of 2,
  worker two's retry succeeds, and the run rests at
  `WaitingForReconciliation` with the call `Indeterminate` and the row
  `Exhausted, 2`. A person is asked to decide about a call known to have
  run, and its result is lost.
- **An `Executed` whose outcome does not belong to the call** is handled as
  `Unknown`, as above. **On a winding-down run** either one parks at once.
- **`Executed`** delivers the reconciled outcome before its fenced settle.
- **"Not executed" with no attempt after it** (the budget spent) burns and
  settles `Exhausted` under the lapsed lease. #171 kept this deliberately:
  no worker can make another attempt, so it is harmless.

The review's probe B reaches probe A's outcome another way. A late *error*
from the authority, to the question asked before the reconciler (#195),
keeps its handling (#207, call 3). So the claim asks the reconciler on a
lapsed lease, and the reconciler's prompt `Unknown` is acted on. Both probes
give the same result on `main`.

## 2. The owner's ruling (2026-11-23)

**Every answer.** The lease is checked right after the reconciler answers,
whatever the finding, as #207 checks it after the authority answers. That
includes #171's exception: a late "not executed" with no budget left now
ends out of lease too, and the claim that follows asks once more and
settles it. **Accepted cost:** a reconciler that always answers slower than
the lease never resolves the call. It is asked once per claim, without end,
where an `Unknown` used to spend the budget and park the call for a person.
"Not executed" has had that cost since #195.

## 3. The change

In `reconcile_ambiguous`, right after `reconciler.reconcile(...)` returns a
finding and before anything is written: if `is_current_claim_for(&claim, 0)`
is false, return `ClaimStep::Concluded(ClaimConclusion::OutOfLease)`. The
claim writes nothing, is counted in `AgentDispatchPass::lapsed`, and leaves
the ticket to the claim that follows.

The "not executed" arm keeps #171's check for another attempt under the
same claim: that the lease covers the attempt's bound
(`covered_ms = timeout_ms`). #195's check (that the lease is still current
before a claim's first attempt) is folded into the new one: it read the same
lease with nothing written in between. `asked_at` is still read before the
cover check, so the next attempt's bound counts from inside the lease the
fleet answered for.

Doc comments to change: the arm's comment block (the #171 exception and the
#195 paragraph); `reconcile_ambiguous`; `settle_unknown_finding` ("which is
what keeps the queries inside the budget" now holds for findings inside the
lease); `AgentDispatchPass::lapsed`; `ClaimConclusion::OutOfLease`.

## 4. Calls made in the design (each the owner's to reverse)

1. **"Still current", not "covers"**, for the reason the owner gave for #195:
   a cover check would never act on a finding whose question and next
   attempt together outlast the lease.
2. **One check before the `match`**, so no arm can be missed, and a new
   finding kind is checked when it is added.
3. **An error from the reconciler keeps its handling.** It ends the pass in
   an error and writes nothing.
4. **Probe B's route gets no check of its own.** After a late authority
   error, the reconciler is still asked once on a lapsed lease. Its answer
   is then discarded by the new check. That costs one question to the
   system of record, in a case that needs both a late authority and an
   authority error. Checking there too would narrow #207's call 3 for one
   caller.
5. **What stays open:** a worker that stalls past its lease between this
   check and its first write can still act. That first write is the burn,
   the delivery of an executed outcome, the park, or the wind-down's
   cancel. It is the same window #207 leaves before `Started`, and the docs
   say so.

## 5. What a deployment sees

- **A late `Unknown` is not acted on.** It records no failure over another
  worker's call and parks nothing, so a call another worker made
  successfully completes the run. With no other worker, the next claim asks
  again, and burns the attempt once.
- **A late `Executed` is not delivered.** The next claim asks again and
  delivers it.
- **A late "not executed" with no budget left** is settled by the next
  claim, after one more question. It used to be settled `Exhausted` at once,
  and the pass ended in `claim-fenced`.
- **A pass whose claim gets a late finding returns its report**, with one
  lapsed claim, where some passes used to end in `claim-fenced`.
- **The cost:** the lease has to cover the reconciler's answer for every
  finding. "A reconciler that keeps answering `Unknown` is asked at most
  once for each attempt in the budget" now holds for answers that come
  inside the lease.
- **No durable field, status, error code or decision kind is added.**

## 6. Mixed builds and rollback

Each claim is decided by the worker that holds it, and an earlier build's
worker acts on a late finding as before. Rolled back, nothing is left to
undo.

## 7. Tests

The new tests go in `crates/rakka-agent/tests/dispatch_refused_before_started.rs`,
beside #207's, whose helpers the review's probes already use
(`a_call_whose_first_attempt_failed`, `HeldTool`, `worker`). Its
`LateReconciler`, which moves the clock past the lease and hands off before
answering, joins them.

- **Probe A as a test.** A late `Unknown` beside worker two's call
  (reconcileable, budget 2): worker one ends out of lease and writes
  nothing, worker two's call succeeds, and the run completes.
- **Probe B as a test.** A late authority error, then a prompt `Unknown`:
  the same outcome. The reconciler is asked once and its answer discarded.
- **A late `Unknown` alone:** the row is unchanged. The next claim asks
  again and burns once, so the row counts each attempt once.
- **A late `Executed` alone:** nothing is delivered. The next claim asks
  again and delivers, so the reconciler is asked twice.
- **#171's exception, turned around:** a late "not executed" with no budget
  left ends out of lease. The row is unchanged, and the next claim settles
  it `Exhausted`. The build first measures, crate-wide, which tests pin the
  old behaviour, and turns those around as rulings.
- **A late `Unknown` on a winding-down run:** no park. The next claim parks.

**Guards, unchanged:** a finding inside a current lease is acted on (the
existing reconciler tests), and #171's cover check still ends a claim whose
lease does not cover its next attempt (`dispatch_attempt_lease.rs`).

**Breaks to run:**
- the check removed;
- the check made for `Unknown` only;
- the check moved after the `Executed` delivery;
- #171's exception restored (no check where no attempt follows a "not
  executed").

## 8. Documents

- `docs/rakka-agents.md`:
  - `:199-203`, the `Unknown` burn: qualify "asked at most once for each
    attempt in the budget";
  - `:391`, #171's exception: remove;
  - `:394`, #195's bullet: widen to every finding (#212).
- `docs/rakka-compatibility.md`: an entry after #207's.
- `CHANGELOG.md`: a first bullet under Fixed.
