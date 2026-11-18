# An approval wait whose gate closed without a grant

Issue #166. Found by the two reviews of the fix for #131 and #130
(`2026-11-17-run-status-after-reconciliation-design.md`, section 9).

## 1. What was wrong

A live run could end on `WaitingForApproval` with no gate open, nothing
outstanding and no wake. That status does not permit progress, so the run
took no loop transition again.

Two facts of the code as it stood make it:

1. **A turn is held for a goal evaluation and for a workflow cancel, and
   neither's outcome rests it.** Both are counted among the effects a turn
   awaits. When the turn's last tool result lands with one out, the tool's
   arm declines to rest the turn. The evaluation's or the cancel's own
   outcome then rests nothing. The settle pass rests such a turn
   (`AgentRun::needs_turn_rest`), but only on a status that permits
   progress.
2. **Nothing takes a run off an approval or authorization wait but a
   grant.** A grant recomputes the wait. A gate that closes any other way
   (denied, expired) fails its effect, and for a tool call that winds the
   run down. For an effect whose failure the run survives, the wait it was
   on stays.

The ways in, each reproduced on `main`:

| | The run is put on the wait by | The gate closes | Then |
|---|---|---|---|
| 1 | a turn that commits its tool call with a review of the goal open | the review is denied or expires | the evaluation fails and writes no status |
| 2 | a gated tool call's grant, with a review of the goal open | the same | the same |
| 3 | a gated workflow start (or A2A send) of a fan-out | it is denied: a fan-in disposition, which the run survives | the turn is not rested, because a goal evaluation is out; the evaluation then settles and writes nothing |

And one that is not stuck, but wrong: a review granted while the run rests
on its proposal wrote the dispatch wait, and the run reported
`WaitingForEffect` there until something else wrote its status. That is
#130's symptom through a door #130 did not name.

## 2. Approaches

1. **Take the goal evaluation and the workflow cancel out of the turn's
   count,** as gap slice 3 took the memory promotion and the claim append
   out. A turn is then never held for one, and a review's gate is never the
   turn's wait. It changes what a run does, not only where it can get
   stuck: today a turn waits for an evaluation that is out before it is
   recorded, so a coordinator's evaluation is decided before the run can
   propose its result. Without the hold a run can propose first, the task
   can accept, and the evaluation's exchange is then refused as coming
   from a run no longer assigned.
2. **Read the run's approval wait only from the gate of a call whose
   outcome moves the run,** as #164 does when a park ends. No gate the run
   rests on can then close without a status being written. But issue #126
   decided, and a test pins, that a review of the goal open when a turn
   commits *is* the turn's approval wait and the run reports it.
3. **Leave what a run waits for and reports as it is, and release a wait
   whose gate is gone.** The function every transition ends in already
   ends a reconciliation park that has nothing left to reconcile (#131). It
   does the same for an approval or authorization wait that no gate holds.

Chosen: 3.

## 3. The change

`crates/rakka-agent/src/run.rs`.

- `settle_run_disposition`, for a live run that is not parked and not
  winding down: a run on an approval or authorization wait that no waiting
  gate of the turn holds takes the status of what it rests on
  (`live_status`, the function a park's end already uses). Every
  transition that can close a gate ends in this function: a decision, the
  checkpoint timers, an effect's result.
- With the wait released, the settle pass that follows rests the turn, as
  it already does when an evaluation or a cancel ends on a status that
  permits progress.
- **A record already stuck** is repaired on the first settle pass that
  touches it: `AgentRun::needs_gate_wait_release`, beside `needs_unpark`.
  The repair is `settle_run_disposition` itself.
- A grant recomputes the dispatch wait only for a run that is on one. A
  review of the goal granted at any other rest leaves the status alone.
- Three doc comments said what is no longer so, or never was: that the
  shape `needs_turn_rest` repairs is unreachable under the fixed
  transitions (it is how a turn held for an evaluation or a cancel is
  rested), and two that gave "nothing takes the run off a review's wait"
  as the reason a park's end does not read it.

Nothing a run waits for changes: the turn's count, the hold, and the wait a
run reports while a gate is open are as they were. No durable field,
status, error code, event or metric is added.

## 4. My calls

1. **Release the wait; do not change what a turn waits for** (section 2).
   The hold stays, and so does #126's rule that a review open when a turn
   commits is the turn's approval wait.
2. **One place, not one per arm.** The release is in the function every
   transition ends in, so it also covers a gate that closes in a way not
   listed in section 1.
3. **The settle pass still rests a turn whose hold has ended,** not the
   evaluation's or the cancel's own arm. That is how the code already
   rested it on a status that permits progress. The doc comment on
   `needs_turn_rest` said the shape was unreachable under the fixed
   transitions; it is the ordinary path for these two kinds, and the
   comment now says so.
4. **Stuck records are repaired,** as #131's are: one write to release the
   wait and one to rest the turn, once.
5. **The released status is what the run rests on, not the next gate's
   wait.** They are the same unless the gate that still stands is a goal
   evaluation's: a fan-out member's approval is denied while an evaluation
   that needs a security authorization waits. The run then goes to
   `WaitingForEffect` or `Running`, not `WaitingForAuthorization`. A park
   that ends in that state lands on the same status.
6. **The grant's rule is stated on the run, not on the effect:** it
   recomputes the dispatch wait of a run that is on one. For a call that
   moves the run that is every case that could be observed: its gate is
   only open on a dispatch wait, or under a park or a wind-down, and the
   settle that follows the grant keeps both. A first version also kept
   "or the effect moves the run"; no test could tell it apart, so it is
   gone.

## 5. What a reader of the status sees

| State | Before | Now |
|---|---|---|
| A review of the goal held a turn, and is denied or expires after the turn's last result landed | `AwaitingTools`, `WaitingForApproval`, for good | the run goes on to its next turn |
| The same review, denied or expired while a tool call of the turn is out | `WaitingForApproval` until the result lands | `WaitingForEffect` (or the wait of a gate on that call) |
| A fan-out member's gated send or start is denied with a goal evaluation out | `AwaitingTools`, `WaitingForApproval`; for good once the evaluation settles | a status that permits progress, and the run goes on once the evaluation settles |
| A review granted while the run rests on its proposal or its children | `WaitingForEffect` until another transition wrote the status | the status it had |
| A review granted while the run is on a dispatch wait | the dispatch wait | unchanged |
| Any gate while it is open | its wait | unchanged |

## 6. Tests

`tests/goal_evaluation.rs`:

- a review that held a turn, denied and expired, after the tool's result
  landed: the run goes on to turn two;
- the same wait reached through a gated tool call's grant;
- a review denied with the tool call out: the run waits for the tool, and
  its result rests the turn;
- a review denied beside a tool call's security authorization: the run
  waits on the authorization;
- a review granted after the tool landed: the turn is held for the
  evaluation, and its result lets the run go on;
- a standing review keeps its wait, and a settle pass writes nothing;
- a record forged as an older binary left it heals on its next settle pass
  and goes on, and is not written again;
- a turn is held for an evaluation that is out, and goes on when it
  settles (what this change leaves as it was).

`tests/workflow_tool.rs`: a fan-out member's gated start denied with a goal
evaluation out. `tests/communal_claim_append.rs`: a review granted while
the run rests on its proposal.

Seven of the ten fail on `main`. The three that pass there hold what must
not change: the standing review, the granted review on a dispatch wait,
and the hold. The rest of the `rakka-agent` suite passes unchanged, the
test that pins #126's rule among it.

The change was then broken on purpose, eight ways:

| Break | Tests that failed |
|---|---|
| the settle does not release the wait | 6 |
| the settle pass does not repair a record on such a wait | 1 |
| the loop is not entered for a record on such a wait | 1 |
| a grant always recomputes the dispatch wait, as on `main` | 1 |
| a grant recomputes only for a call that moves the run | 1 |
| a wait counts as held while any gate stands, of either family | 1 |
| a grant recomputes only for a run on a dispatch wait | 0 |
| the repair step is measured as a turn | 0 |

The seventh is the simplification of call 6, and the code now is that
break. The eighth cannot be observed: a release is made at
`AwaitingTools`, and a turn is measured at `RecordingTurn`. The repair
step is kept out of the measurement all the same, so that the measured
steps stay exactly the steps that are not repairs.

## 7. Not done

- The three other defects listed in
  `2026-11-17-run-status-after-reconciliation-design.md`, section 9, are
  not touched: a re-sent tool call that leaves the run a turn ahead, a
  standing park lifted without an answer, and a failed workflow cancel
  that winds down a live run. None is filed.
- A review of the goal commanded after its turn committed is still not
  reported as the run's approval wait, and a park's end still does not put
  a run on a review's wait. So the same state can report
  `WaitingForApproval` or `WaitingForEffect` by the order things happened
  in. Both now end the same way.
- #128 (a gated workflow cancel never opens its gate) is as it was.
