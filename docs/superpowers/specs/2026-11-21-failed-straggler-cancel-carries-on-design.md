# A failed straggler cancel leaves the run going (issue #197)

2026-11-21. Short design. The owner's ruling is in section 2; everything else
is my call and is listed in section 6. Line citations are at `616ee1a`.

## 1. What was wrong

A fan-in under `Any` (or a quorum, or a fired deadline) resolves and leaves a
started workflow child behind. The run sends that straggler a
`WorkflowCancelCall`. The run is live, and the fan-in has already decided what
it waited for.

If the cancel fails, the failure arm of `apply_effect_outcome`
(`crates/rakka-agent/src/run.rs:5520`) treats it as it treats a failed model or
tool call: `fence_unsent`, `terminal_reason = EffectFailed`, and the run winds
down. It does so for every failed kind except a memory promotion, a goal
evaluation, a claim append and a fan-in member's send or start (`:5782`).

The cancel fails when no cancel executor is wired
(`workflow-cancel-executor-missing`, `dispatch.rs:5692`), when the executor
answers `Refused` (`dispatch.rs:5710`), when its retries are spent
(`Exhausted`), and when a reconciliation answer confirms it executed and
failed (`ConfirmedExecuted`, which re-enters the same arm).

In two orders the run's result never reaches the task (measured at `616ee1a`,
`tmp/2026-11-21-draft-probes/straggler.md`):

- the cancel fails while the model call is out: the proposing turn is never
  evaluated, and the run ends `Failed` when the straggler reports;
- the proposal is out and the task rejects it: no new turn begins, and the run
  ends `Failed` when the straggler reports.

Either way the task stays `InProgress` with no result, and a straggler that
never reports holds the run `Cancelling`.

The code's own contracts already say this should not happen:

- `AgentWorkflowCancelFinding::Refused` (`dispatch.rs:1105`): "The parent then
  waits for the child's natural terminal result." The missing-executor branch
  (`dispatch.rs:5687`) says the same.
- The delegation half of the same chase (`owed_child_cancel_exchanges`,
  `run.rs:2819`): owed cancels "never touch the run's own terminal reason, so a
  chase can never wind the parent down".
- A descriptor without cancel support (`Unsupported`) already leaves a
  straggler running, and the run completes.

Only `moves_the_run`'s doc (`effect.rs:1186`) states the wind-down, and it was
written to describe the code.

## 2. The ruling

The owner, 2026-11-21, picked "Run carries on": a failed straggler cancel no
longer winds the run down. The failure stays on the effect record, the cell
keeps `cancel = Committed { effect }`, and records an earlier build already
wound down are not repaired.

**Second ruling, after the review (2026-11-21).** I had told the owner the
failed cancel shows in the run snapshot. The review showed it shows on no
query surface: the operational snapshot lists only effects that are out or
unknown, `AgentRunSnapshot` holds a count, and the goal view reads
`cancel = Committed`. Asked again, the owner picked "Add it to the goal
view": `AgentGoalWorkflowInvocationView` gains `cancel_status` and
`cancel_error_code`, read from the effect a `Committed` disposition names.
They are view fields, so no stored record changes.

## 3. Approaches

1. **Add `WorkflowCancelCall` to the failure arm's exceptions.** A failed or
   exhausted cancel then does what a successful one does: it settles its own
   effect and moves nothing else. Chosen.
2. **Exempt only a straggler's cancel on a live run**, with a membership test
   like the fan-in send arm's. Every cancel a live run holds is a straggler's
   (`chased_workflow_invocations`), and a winding-down run already has its
   reason, so the test would decide nothing.
3. **Settle the cell to a new `Failed` disposition** so the goal view shows the
   failure directly. Declined by the owner: a new variant in a stored enum is
   unreadable to an older binary during a rolling upgrade.

## 4. The change

**The arm.** In the failure arm's exception list, `WorkflowCancelCall` joins
the memory promotion, the goal evaluation and the claim append. The comment
above the list gains the reason: a cancel is a request with an observable
outcome, not the run's work (specification 8.7). The fan-in it serves has
decided, and a cancel that did not reach the child leaves that child to report
on its own, as `Unsupported` does.

**What a failed cancel leaves.** The effect reads `failed` or `exhausted`, with
`last_error_code` and its reason. Its attempts settle as any resolved call's do
(the code below the arm, unchanged). The cell still reads `Committed { effect }`.
No terminal reason, no fence, no status write: the same as the success arm
(`run.rs:5511`).

**The turn.** A cancel is counted among the effects a turn awaits, so a turn
whose last tool result came back while the cancel was out stays open. Neither
the success arm nor now the failure arm rests it. The settle pass rests it:
`AgentRun::needs_turn_rest` (`run.rs:871`) already names "a turn held for a goal
evaluation or a workflow cancel", and a `Failed` or `Exhausted` effect no longer
counts in `has_unsettled_turn_effect`.

**A run already winding down.** Nothing it ends as changes. Its reason is
already recorded, so the arm never wrote `EffectFailed` there. The arm's
`fence_unsent` found nothing new: every way into a wind-down fences when it
starts (`cancel`, the first failed call, `end_when_settled` at `run.rs:1925`),
and a winding-down run commits only fence-exempt kinds after that. A cancelled
run still waits for the straggler's own result, through `awaits_children`, as
the `Refused` contract says.

**Docs.**

- `effect.rs:1186`, `moves_the_run`: "one that fails winds the run down"
  becomes "one that fails writes nothing either".
- `AgentWorkflowCancelDisposition::Committed`: delivery is read from the
  effect the variant names, which a failed delivery leaves `failed` or
  `exhausted` with its code.
- `docs/rakka-agents.md:201`: a reconciliation answer that fails a workflow
  cancel no longer winds the run down.
- `docs/rakka-compatibility.md`: an entry in the shape of gap slice 2's claim
  append entry (`:119`). No record changes shape. A deployment that relied on a
  failed cancel to end a run must cancel it. In a mixed deployment the binary
  that applies the outcome decides: an older one still winds the run down.
- `CHANGELOG.md`: one entry.

## 5. Tests

A new file, `crates/rakka-agent/tests/failed_straggler_cancel.rs`, built from
the filing probe (`tmp/2026-11-21-draft-probes/zz_probe_straggler_cancel.rs`)
and the straggler world of `tests/workflow_tool.rs`:

- each door (no executor, `Refused`, retries spent) in each order (cancel fails
  while the model call is out; while the proposal is out and then accepted;
  while the proposal is out and then rejected). The run ends `Completed` with
  `ResultAccepted`, and the task `Completed` with the result. After a rejection
  the run begins its next turn. Before the proposal is decided the run has no
  terminal reason;
- the cancel effect reads `failed` (or `exhausted`) with its code, and the
  cell reads `Committed`;
- a straggler that never reports does not hold an accepted run's end;
- a turn held open by the cancel rests once the cancel fails;
- a cancelled run whose cancel fails still waits for the straggler's own
  result and ends `Cancelled` (unchanged; pinned here);
- a reconciliation answer `ConfirmedExecuted { Failed }` for a parked cancel
  leaves a live run going, if a parked cancel can be reached the way existing
  tests park one. If it cannot, the builder says so instead of forging it.

Falsification: with the exception removed, the new tests that assert the run
goes on fail, and the whole crate is run to count them.

## 6. My calls

- Approach 1 over 2 (section 3).
- `Exhausted` and the reconciliation answer are covered along with the two
  doors the issue names: they enter the same arm, and the ruling is about the
  cancel failing, not about how.
- No metric and no decision record for a failed cancel. The effect record is
  durable for the run's life (effect records are only ever appended).
  **Corrected:** I first wrote that it "is in the run snapshot". It was not,
  and the goal-view fields of section 2's second ruling are what make it
  visible.
- `AbandonAndFail` of a parked cancel and a refusal by the dispatch authority
  enter the same failure arm, and are covered with the rest (the review's
  probe; a test pins the first).

## 7. Not in scope

- A cancel whose outcome is unknown still parks the run in
  `WaitingForReconciliation`. That is the park rule for every kind inside the
  turn, and not what the issue asks.
- The delegation cancel. It already never winds the parent down.
- Records an earlier build already wound down (the ruling).
