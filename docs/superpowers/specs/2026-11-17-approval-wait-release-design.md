# An approval wait whose gate closed without a grant

Issue #166. Found by the two reviews of the fix for #131 and #130
(`2026-11-17-run-status-after-reconciliation-design.md`, section 9).

## 1. What was wrong

A live run could end on `WaitingForApproval` or `WaitingForAuthorization`
with no gate open, nothing outstanding and no wake. Neither status permits
progress, so the run took no loop transition again.

Two facts of the code as it stood make it:

1. **A turn that makes a call is held for a goal evaluation and for a
   workflow cancel, and neither's outcome rests it.** Both are counted
   among the effects a turn awaits. When the turn's last result lands with
   one out, the arm that lands it declines to rest the turn. The
   evaluation's or the cancel's own outcome then rests nothing. The settle
   pass rests such a turn (`AgentRun::needs_turn_rest`), but only on a
   status that permits progress.
2. **Nothing recomputed a run's approval or authorization wait but a
   grant.** A gate that closes any other way (denied, expired) fails its
   effect, and for a tool call that winds the run down. For an effect whose
   failure the run survives, the wait it was on stayed.

The ways in, each reproduced on `main`:

| | The run is put on the wait by | The gate closes | Then |
|---|---|---|---|
| 1 | a turn that commits its tool call with a goal evaluation's gate open: a review of the goal, or an evaluation the deployment's spec gates | it is denied or expires | the evaluation fails and writes no status |
| 2 | a gated tool call's grant, with a review of the goal open | the same | the same |
| 3 | the gated start or A2A send of a fan-out member | it is denied or expires: a fan-in disposition, which the run survives | the turn is not rested, because a goal evaluation is out; the evaluation then settles and writes nothing |

And one that is not stuck, but wrong: a review granted while the run rests
on its proposal or its children wrote the dispatch wait, and the run
reported `WaitingForEffect` there until something else wrote its status.
That is #130's symptom through a door #130 did not name.

## 2. Approaches

1. **Take the goal evaluation and the workflow cancel out of the turn's
   count,** as gap slice 3 took the memory promotion and the claim append
   out. A turn is then never held for one, and a goal evaluation's gate is
   never the turn's wait. It changes what a run does, not only where it
   can get stuck: a turn that makes a call would no longer wait for an
   evaluation that is out before it is recorded. (A turn that makes no
   call is not held today either.)
2. **Read the run's approval wait only from the gate of a call whose
   outcome moves the run,** as #164 does when a park ends. No gate the run
   rests on can then close without a status being written. But issue #126
   decided, and a test pins, that a review of the goal open when a turn
   commits *is* the turn's approval wait and the run reports it.
3. **Leave what a run waits for and reports as it is, and recompute a wait
   whose gate is gone.** A grant already recomputes it. The function that
   every gate-closing transition ends in does the same when the gate closed
   without one.

Chosen: 3.

## 3. The change

`crates/rakka-agent/src/run.rs`.

- `AgentRun::needs_gate_wait_release`: the run is on `WaitingForApproval`
  and no approval of the turn waits, or on `WaitingForAuthorization` and
  no security authorization of the turn waits.
- `settle_run_disposition`, for a live run that is not parked and not
  winding down: such a run has its dispatch wait recomputed, exactly as a
  grant recomputes it (`checkpoint_wait_status`). It takes the wait of a
  gate that still stands, or the effect wait. Every transition that can
  close a gate ends in this function: a decision, the checkpoint timers,
  an effect's result.
- From the effect wait, the settle pass that follows rests a turn that has
  nothing left out, as it already does when an evaluation or a cancel
  ends.
- **A record already stuck** is repaired on the first settle pass that
  touches it, by the same predicate, beside `needs_unpark`. The repair is
  `settle_run_disposition` itself.
- A grant recomputes the dispatch wait only for a run that is on one. A
  review of the goal granted at any other rest leaves the status alone.
- Doc comments that said what is not so are corrected: that the shape
  `needs_turn_rest` repairs is unreachable under the fixed transitions (it
  is how a turn held for an evaluation or a cancel is rested), and why a
  park's end does not read a review's gate.

Nothing a run waits for changes: the turn's count, the hold, and the wait a
run reports while a gate of that kind is open are as they were. No durable
field, status, error code, event or metric is added.

## 4. My calls

1. **Recompute the wait; do not change what a turn waits for** (section
   2). The hold stays, and so does #126's rule.
2. **One place, not one per arm.** The recompute is in the function every
   gate-closing transition ends in, so it also covers a gate that closes in
   a way not listed in section 1.
3. **The settle pass still rests a turn whose hold has ended,** not the
   evaluation's or the cancel's own arm. That is how the code already
   rested it on a status that permits progress.
4. **Stuck records are repaired,** as #131's are: one write to release the
   wait and one to rest the turn, once.
5. **The wait is judged against gates of its own kind,** and released to
   what a grant would have written. The first build did neither, and the
   review showed what each cost (section 7).
6. **The grant's rule is stated on the run, not on the effect:** it
   recomputes the dispatch wait of a run that is on one. For a call that
   moves the run that is every case that could be observed: its gate is
   only open on a dispatch wait, or under a park or a wind-down, and the
   settle that follows the grant keeps both.

## 5. What a reader of the status sees

| State | Before | Now |
|---|---|---|
| A goal evaluation's gate held a turn, and is denied or expires after the turn's last result landed | `AwaitingTools`, on that gate's wait, for good | the run goes on to its next turn |
| The same, reached through the grant of a gated tool call | the same | the same |
| The same gate, denied or expired while a call of the turn is out | that gate's wait until the call's result lands | `WaitingForEffect`, or the wait of a gate that still stands |
| A fan-out member's gated send or start is denied or expires with a goal evaluation out | `AwaitingTools`, `WaitingForApproval`; for good once the evaluation settles | `WaitingForEffect`, or the wait of the evaluation's own gate; the run goes on once the evaluation settles |
| A review granted while the run rests on its proposal or its children | `WaitingForEffect` until another transition wrote the status | the status it had |
| A review granted while the run is on a dispatch wait | the dispatch wait | unchanged |
| A gate while it is open, and no gate of its kind has closed | its wait | unchanged |

## 6. Tests

`tests/goal_evaluation.rs`:

- a review that held a turn, denied and expired, after the tool's result
  landed: the run goes on to turn two;
- the same for an evaluation the spec gates behind a security
  authorization, denied and expired;
- the same wait reached through a gated tool call's grant alone;
- a review denied with the tool call out: the run waits for the tool, and
  its result rests the turn;
- a review denied beside a tool call's security authorization: the run
  waits on the authorization;
- a review commanded beside a tool call's standing authorization: the run
  stays on it, and three settle passes write nothing;
- a review granted after the tool landed: the turn is held for the
  evaluation, and its result lets the run go on;
- a standing review keeps its wait, and a settle pass writes nothing;
- a record forged as an older binary left it, on the approval wait and on
  the authorization wait, heals on its next settle pass and goes on, and
  is not written again;
- a turn is held for an evaluation that is out, and goes on when it
  settles (what this change leaves as it was).

`tests/workflow_tool.rs`:

- a fan-out member's gate closes with a goal evaluation out, for a
  workflow start denied, a workflow start expired and an A2A send denied:
  the held turn is on the effect wait, and the run goes on once the
  evaluation settles;
- a start's approval denied while the evaluation waits on a security
  authorization of its own: the run reports that authorization, a settle
  pass writes nothing, and its grant puts the run on the effect wait;
- a review granted while the run rests on its children leaves its status.

`tests/communal_claim_append.rs`:

- a review granted while the run rests on its proposal leaves its status;
- a denied review releases the run with a gated claim append's approval
  still open beside it.

Twelve of the sixteen fail on `main`. The four that pass there hold what
must not change: the standing review, the review beside a standing
authorization, the granted review on a dispatch wait, and the hold. The
rest of the `rakka-agent` suite passes unchanged, the test that pins
#126's rule among it.

The change was then broken on purpose, nine ways, and each break failed
tests:

| Break | Tests that failed |
|---|---|
| the settle does not release the wait | 9 |
| only an approval wait is released | 2 |
| a wait counts as held while any gate stands, of either kind | 2 |
| the wait is compared with the wait the gates add up to (the first build) | 1 |
| a released run takes the status of what it rests on, not the dispatch wait (the first build) | 2 |
| the settle pass does not repair a record on such a wait | 2 |
| the loop is not entered for a record on such a wait | 2 |
| a grant recomputes the dispatch wait of any live run, as on `main` | 2 |
| a gate outside the turn holds the run's wait | 1 |

One line no test can reach: the repair step is kept out of the turn's
measurement, as the other repairs are. A release is made at
`AwaitingTools`, and a turn is measured at `RecordingTurn`.

## 7. The review

A fresh reviewer read the first build and probed it. It found the ways in
closed and no state where a run advances past a standing gate, and these
faults, all corrected:

1. **A regression: the first build rewrote a wait that stood, 32 times a
   settle pass.** It judged a wait stale by comparing the run's status
   with the wait the turn's gates add up to, in which an approval is
   reported ahead of an authorization. A run on a tool call's
   authorization, with a review of the goal commanded afterwards, has an
   approval open beside it: the wait looked unheld, was released to the
   same status, and stayed "releasable" at every step. The wait is judged
   against gates of its own kind now, and a test holds that three settle
   passes write nothing there.
2. **A released run could report `Running` with a gate of the turn still
   open.** The first build released a wait to the status of what the run
   rests on, which does not read a goal evaluation's gate. So a fan-out
   member's approval denied beside an evaluation's security authorization
   left the run `Running`, where an adapter reads "working" and not
   "authorization required". And a held turn was `Running` where every
   other held turn is `WaitingForEffect`. The release recomputes the
   dispatch wait now, exactly as a grant does.
3. **Nothing held the authorization half.** Narrowing the fix to
   `WaitingForApproval` left the suite green. An evaluation gated behind a
   security authorization is stuck on `main` the same way; two tests hold
   it.
4. **The test of the second way in did not isolate it.** It commanded the
   review before the turn committed, so the run was already on the
   review's wait by the first way. It commands it after, now.
5. **Three documented behaviours had no test:** the A2A send of a fan-out
   member, a gate that expires, and a review granted while the run rests
   on its children. Added.
6. **A doc link from a public item to a private one** gave the crate its
   only rustdoc warning. Removed.
7. **Statements that were not true:** "the function every transition ends
   in" (it is every transition that can close a gate); that a turn waits
   for an evaluation before it is recorded (only a turn that makes a
   call); that a review commanded after its turn committed is never
   reported (a grant of another gate of the turn reports it); and a list
   of the gates a run survives that read as complete and was not.

## 8. Not done

- The three other defects listed in
  `2026-11-17-run-status-after-reconciliation-design.md`, section 9, are
  not touched: a re-sent tool call that leaves the run a turn ahead, a
  standing park lifted without an answer, and a failed workflow cancel
  that winds down a live run. None is filed.
- A review of the goal commanded after its turn committed is not reported
  as the run's approval wait until something recomputes the dispatch wait
  with it open: a grant, or another gate of the turn closing. And a park's
  end still does not put a run on a review's wait. So the same state can
  report `WaitingForApproval` or `WaitingForEffect` by the order things
  happened in. Both now end the same way.
- #128 (a gated workflow cancel never opens its gate) is as it was.
