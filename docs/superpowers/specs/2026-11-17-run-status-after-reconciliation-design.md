# A live run's status after its reconciliation park is answered

Issues #131 and #130. Both come from one gap: parking a run in
reconciliation overwrites its status, and no answer puts back what the run
was resting on.

## 1. What was wrong

A live run's effect ends `Indeterminate`. `settle_run_disposition` moves
the run to `WaitingForReconciliation`, whatever the effect's kind. A person
then answers, by `ResolveIndeterminateEffect` or through the reconciliation
checkpoint.

| Answer | What the run's status became |
|---|---|
| `ConfirmedExecuted` | stayed `WaitingForReconciliation`, unless the outcome's own arm wrote one (#131) |
| `AbandonAndFail` | the same (#131) |
| `ConfirmedNotExecuted` | the dispatch wait, whatever the run rested on (#130) |

An outcome's arm writes a status only when it rests the turn. A memory
promotion's, a claim append's, a goal evaluation's and a workflow cancel's
never do. A tool call's does not while another effect of the turn is out.

`WaitingForReconciliation` does not permit progress. A run left there with
nothing to reconcile takes no loop transition until something else writes
its status. A test now shows the run for which nothing ever would: a tool
result lands during the park and rests the turn, the park is answered, and
the run sits in `RecordingTurn`, parked, with no effect in flight.

## 2. What a live run's status is read from

A live run that is neither parked nor winding down holds

- the dispatch wait (`WaitingForEffect`, or the approval or authorization
  wait of an open gate) while a call **whose outcome moves the run** is
  outstanding, and
- `Running` otherwise.

The calls whose outcome moves the run are the model call, a tool call, an
A2A send and a workflow start: each one's success writes the run's phase
and status. (A compensation resolves as a tool call does, but exists only
on a run that is winding down.) The other four kinds are committed without
a status, and the success of one writes none:

| Kind | Committed by | Its success writes | Its failure |
|---|---|---|---|
| memory promotion | a command | nothing of the run's | fails the generation only |
| claim append | a command | nothing of the run's | fails the generation only |
| goal evaluation | a command | nothing of the run's | fails the generation only |
| workflow cancel | a fan-in that resolved, chasing a straggler | nothing of the run's | winds the run down |

A status must not be read from one of those, or from the gate of one: a
run put on the dispatch wait for it has nothing to take it off again.
Section 8 has what happened when the first build did.

## 3. Approaches

1. **Record the status at the park and restore it.** A new durable field.
   And wrong when the run moves during the park: the tool result above
   rests the turn, and the status recorded at the park is then the wait
   the run has left.
2. **Recompute the status when the park ends.** No new field.
3. **Recompute it on every settle,** parked or not. It would also correct a
   record #130 left on the wrong wait. But it overwrites statuses on paths
   that are right today, and section 6 has a state where the recomputed
   status and the stored one differ by the rule of the code as it stands.

Chosen: 2, in the one function every answer already ends in.

## 4. The change

- `AgentRunEffectKind::moves_the_run` names the kinds of section 2, and
  `AgentLoopState` reads the two things the status is made from: whether
  such a call is outstanding, and the approval wait among such calls'
  gates. All three are crate-private.
- `live_status(&AgentLoopState)` answers section 2.
- `settle_run_disposition`: a live run that is not winding down, holds no
  indeterminate effect and is still `WaitingForReconciliation` takes
  `live_status`. Nothing else in that function changes.
- The re-send arm of `apply_indeterminate_resolution` writes `live_status`
  where it wrote the dispatch wait.
- **A record already stuck** is repaired on the first settle pass that
  touches it, as a wedged turn and an unresolvable park already are:
  `AgentRun::needs_unpark` beside `needs_turn_rest` and
  `needs_park_repair`. The repair is `settle_run_disposition` itself.
- A repair step is no longer measured as a turn. The loop measured a turn
  at every committed step in `RecordingTurn`, and a repair commits there
  without folding the turn, so a healed record counted its turn twice.
- Three doc comments said `AbandonAndFail` fails the run terminally. For a
  memory promotion, a claim append or a goal evaluation it fails the
  generation only. Corrected.

`turn_rest` is not changed. No durable field, status, error code, event or
metric is added.

## 5. My calls

1. **Recompute, not restore** (section 3). The owner leaned this way.
2. **Only at the un-park,** not on every settle.
3. **Stuck records are repaired,** in the settle pass. The cost is one
   write for each such record, once. A standing park is not written.
4. **A record #130 left on `WaitingForEffect` is not repaired.** That
   status permits progress, and the next status write corrects it.
5. **The re-send arm keeps a status write** and does not rely on the
   settle that follows it.
6. **The status is read from the calls whose outcome moves the run, and
   not from the turn's own count of effects.** The two differ for a goal
   evaluation and a workflow cancel (section 6). This is the call the
   review forced; the first build made the other one.

## 6. The writes, and where they agree

Every place `run.rs` writes a live status, other than the park and the
wind-down:

| Where | Writes | `live_status` there |
|---|---|---|
| creation | `Accepted` | not asked: no effect exists to park the run on |
| `prepare_context` | `WaitingForEffect`, with the model call committed | the same |
| the model result's arm; a turn with no calls; `record_turn`; `decide_continuation`; a fan-in, delegation or workflow result that completes the turn; the task's decision on a proposal | `Running` | the same: each is reached with no such call out |
| `park_for_top_up`, and its grant | `Running` | the same when no such call is out |
| `turn_rest`, at every turn-completing site | the dispatch wait while an effect *the turn counts* is out, else `Running` | **differs** when a goal evaluation or a workflow cancel is out |
| a granted gate on an effect the turn counts | the dispatch wait, counting every gate of the turn | **differs** when the granted effect is a goal evaluation, or a review of the goal is open |

**Where they differ.** `turn_rest` counts a goal evaluation and a workflow
cancel among the effects a turn awaits
(`AgentRunEffectKind::outside_the_turn` names only the promotion and the
claim append), and the gate of an evaluation among its approval waits. So
a turn that commits with a human review open is rested on
`WaitingForApproval`, where `live_status` answers `WaitingForEffect`. A
park that ends in that state leaves the run on the second, which is the
status it would hold had the review been commanded a moment after the turn
committed, and which permits progress. The grant of a gate uses the same
count. This change touches neither; section 9 says what they leave.

## 7. Tests

`tests/private_memory_promotion.rs`, on the suite's live worlds:

- every rest (model call out, tool call out, resting on the proposal),
  every answer and both doors: the status and phase after the answer are
  those before the park, and for the re-send also once the re-sent
  generation is out and once it has settled;
- a turn that rests during the park goes on to turn two once answered;
- one of two parked promotions answered leaves the run parked, by command
  and at the checkpoints;
- a tool call's park confirmed with the turn's other call out;
- a winding-down run's park never takes a live status;
- `Escalate` keeps the park; a gated promotion re-sent re-parks its gate;
  the model call's own park, each answer;
- a stuck record heals on its next settle pass at every rest, in one
  write, and is not written again; so do the record #131 left for good and
  one both parked and wedged, forged as an older binary wrote them;
- a healed record measures its turn once;
- a standing park is not written by the settle pass.

`tests/communal_claim_append.rs`: a parked claim append, three answers;
and, with a human review of the goal open, a run whose append parked and
was confirmed ends where a run whose append landed normally ends.
`tests/workflow_tool.rs`: a run resting on its children, and a run with a
straggler's workflow cancel out. `tests/goal_evaluation.rs`: a parked
evaluation found to have failed.

Not covered: a park that ends with an A2A send or a workflow start still
out, and a repair at `RecordingTurn` that comes through `needs_park_repair`
and not `needs_unpark`.

The whole `rakka-agent` suite passed unchanged with the fix, so no test
had pinned the old statuses. Each part of the change was then broken on
purpose, sixteen ways, and every break failed at least one of these tests.

## 8. The review

A fresh reviewer read the first build and probed it. Its probes are now
tests. What it found:

1. **Critical: the first build read the status from the turn's own count
   of effects.** With a human review of the goal open, the un-park put the
   run on `WaitingForApproval`, which does not permit progress. When the
   review was then denied nothing wrote a status again: no gate, nothing
   in flight, for good. The first build's own design had named a milder
   form of this as a wart and left it. Section 2 is the correction.
2. **A second kind had the same shape:** a workflow cancel chasing a
   fan-in straggler. Same correction.
3. **A healed record measured its turn twice.** Fixed (section 4).
4. **Doc statements** that an answer always returns the run to what it
   rests on. Not when the answer fails the model call or a tool call, and
   not for `Compensate` or `Escalate`. Corrected in the product docs.
5. The heal test took its "not written again" baseline one pass late.
   It now takes it after the first pass and asserts one write.

A second reviewer then read the rework alone. It found no defect in the
code: the kind set is right arm by arm, `approval_family_wait` kept its
behaviour, and the measurement condition is exactly the three repair
branches. It found claims that were too strong, all corrected here and in
the code comments:

- "settle without a status, whether they succeed or fail" is not true of a
  workflow cancel, whose failure winds the run down;
- "answering a goal evaluation's gate moves no status" is not true of a
  grant, which writes the dispatch wait;
- "any command or exchange runs a settle pass" is not true of a read, a
  refused command or a timer with nothing due;
- the difference on the re-send was stated for a tool call only.

It also had five tests tightened: two compared a parked world with a
control and did not check that the control had advanced, one did not
assert the state its name described, one asserted two measurements agreed
and not their value, and one forged a record no older binary wrote, which
is removed.

## 9. Found on main, and not fixed here

The two reviews found four defects that this change neither causes nor
cures. Each reproduces on `a802f52`. The first has one root and several
ways in.

- **A run can be left on a wait nothing clears, because the turn counts
  effects whose outcome rests nothing.** A goal evaluation and a workflow
  cancel are counted among the effects a turn awaits, so a turn-rest site
  declines to rest the turn while one is out, and the effect's own outcome
  then rests nothing. `needs_turn_rest` repairs that only for a status
  that permits progress. Three ways in were reproduced:
  - a human review of the goal is open when a tool turn commits, the tool
    result lands, the review is denied or expires: `AwaitingTools`,
    `WaitingForApproval`, nothing outstanding, for good;
  - a fan-out member's gated send or start is denied while an evaluation
    is out, and the evaluation then settles: the same;
  - a review granted while the run rests on its proposal leaves the run
    on `WaitingForEffect` after the evaluation settles, which is #130's
    symptom through a door #130 does not name.
- **A re-sent tool call can leave the run a turn ahead of it.** In a
  two-call turn, one call parks and the other lands. The tool arm rests
  the turn, because it counts only outstanding effects and the parked one
  is not. The parked call is then answered `ConfirmedNotExecuted`: the run
  records the turn and commits turn two's model call with turn one's tool
  call in flight.
- **A standing park is lifted without an answer** by three writers that
  set `Running` and do not settle the disposition: a fan-in deadline, a
  top-up grant, and a rejected proposal. The run goes on holding its
  indeterminate effect and its open checkpoint.
- **A failed workflow cancel winds down a live run.** A fan-in that
  resolved chases its straggler with a workflow cancel. If the cancel is
  refused, or no cancel executor is wired, the failure arm treats it as
  any other call's and the coordinating run ends `Failed`, though its
  fan-in had resolved.
