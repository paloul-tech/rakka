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

A turn is held for a workflow cancel as it is for a goal evaluation, so a
member's gate that closes with a cancel out reads as the third way in too.
I did not construct it.

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
   whose gate is gone.** A grant already recomputes it. The function a
   decision and the checkpoint timers end in does so when the gate closed
   without one.

Chosen: 3.

## 3. The change

`crates/rakka-agent/src/run.rs` and `loop_runtime.rs`.

- **The question.** `AgentRun::needs_gate_wait_release`: the run is on
  `WaitingForApproval` and no approval of the turn waits, or on
  `WaitingForAuthorization` and no security authorization of the turn
  waits. The gates are read by `AgentLoopState::turn_gate_waits`, over the
  one function (`waiting_gates_among`) the dispatch wait itself is read
  from, so the two cannot come to read different gates.
- **The release.** `settle_run_disposition`, for a live run that is not
  parked and not winding down. Where the run waits on dispatch
  (`AwaitingModel`, `AwaitingTools`) the dispatch wait is recomputed with
  the function a grant recomputes it with (`checkpoint_wait_status`): the
  wait of a gate that still stands, or the effect wait. At any other rest
  the run holds no dispatch wait, and takes the status of what it rests on
  (`live_status`, as when a park ends).
- **Where it runs.** A decision and the checkpoint timers both end in this
  function, and those are the transitions that close a gate without a
  grant on a run that goes on. (A cancel closes gates too and ends there,
  in the wind-down. `clear_turn` drops a recorded turn's gates without it,
  from a status that permits progress, and writes `Running`.)
- **The rest.** From the effect wait, the settle pass that follows rests a
  turn that has nothing left out, as it already does when an evaluation or
  a cancel ends.
- **A record already stuck** is repaired on the first settle pass that
  touches it, by the same predicate, beside `needs_unpark`. The repair is
  `settle_run_disposition` itself, and the step is kept out of the turn's
  measurement as the other repairs are.
- **The grant** recomputes the dispatch wait only where the run waits on
  dispatch. On `main` it did so at any rest, which is how a review granted
  on the proposal or the children left the run on `WaitingForEffect`.
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
2. **One place, not one per arm.** The recompute is in the function the
   gate-closing transitions end in, and it asks a question of the record,
   not of the event. So it covers a gate that closes in a way not listed
   in section 1, and it repairs a record an older binary left.
3. **The settle pass still rests a turn whose hold has ended,** not the
   evaluation's or the cancel's own arm. That is how the code already
   rested it on a status that permits progress.
4. **Stuck records are repaired,** as #131's are: one write to release the
   wait and, where the turn has nothing out, one to rest it, once.
5. **A wait is judged against gates of its own kind,** read by the function
   the wait is computed from. The first build compared the status with the
   wait the gates add up to, and the first review showed the cost (section
   7).
6. **Whether there is a dispatch wait is decided by where the run rests,
   not by the status it has,** for the grant and for the release alike.
   The second build decided the grant by status, and the second review
   found the state where that differs: a held turn a park's end left
   `Running`.
7. **A gate that closes without a grant recomputes only when it was the
   last of its kind.** A grant always recomputes. With two members behind
   authorizations and a review commanded after the turn committed,
   granting one member's gate reports the review, and denying it leaves
   the run on the other's authorization. Both are true waits, and the
   denial writes nothing. Making the denial recompute would have it report
   a review the run was not on.

## 5. What a reader of the status sees

| State | Before | Now |
|---|---|---|
| A goal evaluation's gate held a turn, and is denied or expires after the turn's last result landed | `AwaitingTools`, on that gate's wait, for good | the run goes on to its next turn |
| The same, reached through the grant of a gated tool call | the same | the same |
| The same gate, denied or expired while a call of the turn is out | that gate's wait until the call's result lands | `WaitingForEffect`, or the wait of a gate that still stands |
| A fan-out member's gated send or start is denied or expires with the turn held for a goal evaluation | `AwaitingTools`, on that gate's wait; for good once the evaluation settles | `WaitingForEffect`, or the wait of a gate that still stands; the run goes on once the evaluation settles |
| A review granted while the run rests on its proposal or its children | `WaitingForEffect` until another transition wrote the status | the status it had |
| A review granted where the run waits on dispatch | the dispatch wait | unchanged |
| A gate while it is open, and a gate of its kind stands | its wait | unchanged |
| A record an older binary left on an approval wait while the run rests on its proposal or its children | that wait, until the next status write | the status of what it rests on |

## 6. Tests

Twenty-two new tests.

`tests/goal_evaluation.rs`:

- a review that held a turn, denied and expired, after the tool's result
  landed: the run goes on to turn two;
- the same for an evaluation the spec gates behind a security
  authorization, denied and expired;
- the same wait reached through a grant alone: the tool call's
  authorization is granted, which puts the run on the wait of a review
  commanded after the turn committed;
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
- a checkpoint timer with nothing due repairs such a record, a fan-in
  deadline with nothing due does not, and neither writes a record that
  needs no repair;
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
- two members behind authorizations and a review commanded after the turn
  committed: a grant of one reports the review, and its denial keeps the
  other's authorization and writes nothing (call 7);
- a review granted while the run rests on its children leaves its status.

`tests/communal_claim_append.rs`:

- a review granted while the run rests on its proposal leaves its status;
- a denied review releases the run with a gated claim append's approval
  still open beside it;
- a review granted on a held turn that a park's end left `Running` puts it
  on the effect wait (call 6), and one denied there lets the run go on;
- **the forged matrix:** 296 records. Every combination of gates a run can
  hold at four rests (the tool call out, the turn held, the next model
  call, the proposal), each of the two waits forged onto the stored
  record, with and without an effect dropped from under its gate, and
  four settle passes with the stored revision read after each. A held
  wait is not written; an unheld one is released by the first pass; no
  later pass writes; none is left on a wait no gate of its kind holds; no
  turn is left unrested; and a record released at the proposal rest takes
  the status it had there.

`tests/private_memory_promotion.rs`:

- a record released at `RecordingTurn` measures its turn once, as an
  answered one does.

Fifteen of the twenty-two fail on `main`. The seven that pass there hold
what must not change: the standing review, the review beside a standing
authorization, the granted review on a dispatch wait, the hold, a grant of
one of two gates of a kind, and a review granted or denied on a held turn
that is `Running`. The rest of the `rakka-agent` suite passes unchanged,
the test that pins #126's rule among it.

The change was then broken on purpose, fourteen ways, against the four
test files above, and each break failed tests:

| Break | Tests that failed |
|---|---|
| the settle does not release the wait | 13 |
| only an approval wait is released | 3 |
| a wait counts as held while any gate of the turn stands, of either kind | 3 |
| the wait is compared with the wait the gates add up to (the first build) | 4 |
| a released run always takes the status of what it rests on (the first build) | 2 |
| a released run always takes the dispatch wait (the second build) | 1 |
| the settle pass does not repair a record on such a wait | 4 |
| the loop is not entered for a record on such a wait | 4 |
| the release step is measured as a turn | 1 |
| a grant recomputes the dispatch wait at any rest, as on `main` | 2 |
| a grant recomputes nothing | 2 |
| a grant recomputes by the status the run has, not by where it rests (the second build) | 1 |
| a gate outside the turn holds the run's wait | 2 |
| a gate whose effect is gone is not read | 1 |

## 7. The reviews

Two fresh reviewers, one of the first build and one of the rework that
answered it. Neither found a state where a run advances past a standing
gate, or a way back into the stuck state.

**The first review** found, all corrected:

1. **A regression: the first build rewrote a wait that stood, 32 times a
   settle pass.** It judged a wait stale by comparing the run's status
   with the wait the turn's gates add up to, in which an approval is
   reported ahead of an authorization. A run on a tool call's
   authorization, with a review of the goal commanded afterwards, has an
   approval open beside it: the wait looked unheld, was released to the
   same status, and stayed "releasable" at every step. The wait is judged
   against gates of its own kind now.
2. **A released run could report `Running` with a gate of the turn still
   open,** because the first build released a wait to the status of what
   the run rests on, which does not read a goal evaluation's gate. A
   fan-out member's approval denied beside an evaluation's security
   authorization left the run `Running`, where an adapter reads "working"
   and not "authorization required". Where the run waits on dispatch the
   release recomputes the dispatch wait now.
3. **Nothing held the authorization half.** Narrowing the fix to
   `WaitingForApproval` left the suite green.
4. **Three documented behaviours had no test:** the A2A send of a fan-out
   member, a gate that expires, and a review granted while the run rests
   on its children.
5. **A doc link from a public item to a private one** gave the crate its
   only rustdoc warning.
6. **Statements that were not true,** among them "the function every
   transition ends in" and that a turn waits for an evaluation before it
   is recorded (only a turn that makes a call does).

**The second review** forged 296 records and found the rework quiet: none
written twice, none left stale. It found, all corrected:

1. **Nothing held the grant's recompute.** With it removed the whole suite
   passed, because the release now does a grant's work in the same
   transition. The two states where they differ are now tests: one of two
   gates of a kind granted, and a grant on a held turn that is `Running`.
2. **The second build changed what a grant does on a held turn that a
   park's end left `Running`:** `main` put it on the effect wait, and the
   second build left it `Running`, having decided "is on a dispatch wait"
   from the status. It is decided from where the run rests now.
3. **At a rest other than a dispatch rest the release wrote #130's
   symptom:** a record an older binary left on an approval wait while the
   run rested on its proposal was released to `WaitingForEffect` with
   nothing out. It takes the status of what it rests on now.
4. **Nothing tied the predicate and the released status to the same
   gates.** A break that made the predicate stop counting a gate whose
   effect was gone left the suite green and rewrote 84 forged records 32
   times a pass. Both read one function now, and the forged matrix is in
   the suite.
5. **The test of the second way in still did not isolate it:** the turn
   committed on the tool's approval, so the grant changed no status. The
   tool is behind an authorization now, and the grant visibly moves the
   run onto the review's wait.
6. **"One line no test can reach" was reachable.** A record released at
   `RecordingTurn` is the case, and it is a test.
7. **Statements that were not true:** "exactly as a grant recomputes it"
   (call 7); "a timer with nothing due" does not repair a record (a
   checkpoint timer does, and the same was said of #131's park, where it
   is as untrue; both entries are corrected); "an effect's result" closes
   no gate; "every other held turn is `WaitingForEffect`" (section 8).

## 8. Not done

- **A held turn whose park is answered reports `Running` with a goal
  evaluation's gate open.** A park's end reads the status from the calls
  that move the run (#131), and so does not put the run back on a
  review's wait that the park overwrote. `main` does the same. It is not
  stuck: the grant puts it on the effect wait and a denial rests the
  turn. #131 gave as its reason that nothing could clear a review's wait;
  this change removes that reason, so a park's end could now read the
  dispatch wait where the run waits on dispatch. That changes a rule #164
  tested, and is the owner's to decide.
- A review of the goal commanded after its turn committed is not reported
  as the run's approval wait until something recomputes the dispatch wait
  with it open: a grant, or the last gate of a kind closing. So the same
  gates can report `WaitingForApproval`, `WaitingForAuthorization` or
  `WaitingForEffect` by the order things happened in. All now end the
  same way.
- The three other defects listed in
  `2026-11-17-run-status-after-reconciliation-design.md`, section 9, are
  not touched: a re-sent tool call that leaves the run a turn ahead, a
  standing park lifted without an answer, and a failed workflow cancel
  that winds down a live run. None is filed.
- #128 (a gated workflow cancel never opens its gate) is as it was.
