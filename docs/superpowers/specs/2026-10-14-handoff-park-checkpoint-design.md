# Handoff send park: a checkpoint for the wait, and the task's answer as its release

Status: written 2026-10-14, awaiting the owner's review. Branch `handoff-park-checkpoint`, off `main` at `533fa20`.

Brief: issue #106 (the rakka.host handoff-park brief), drafted against `533fa20`. The host pins `2c9e919`, and the defect is the same at both.

Owner rulings (2026-10-14), on the assessment of the brief:
1. A `Refused` handoff-result notice settles a parked send as **executed**, not as not executed (the brief's Ask 2 said the opposite).
2. `AbandonAndFail` on a handoff send **fails the run terminally**, as the decision's contract says.
3. `Compensate` on a handoff send **settles the handoff cell failed** in the same compare-and-set.
4. Records already parked by the defect are **repaired** in this change.
5. The credential-resolution park (below) is **a follow-up issue**, not part of this change.

## The defect, verified at `533fa20`

A handoff send (`AgentRunEffectKind::A2aSendCall` held by the run's handoff cell) whose retry budget spends out arrives as `Exhausted`. The handoff arm of `apply_effect_outcome` turns that into an `Indeterminate` generation (`run.rs:4504-4516`), because the executor's probe could not establish whether the task recorded the transfer. That much is deliberate (slice 5.1). What follows is not:

1. **No checkpoint.** `record_effect_result` opens the reconciliation checkpoint only when the *outcome* is `Indeterminate` (`run.rs:4220`). This outcome is `Exhausted`, so the run goes `WaitingForReconciliation` through `settle_run_disposition` (`run.rs:4862`) with nothing to decide on. No SLA fires, and every checkpoint decision answers `run-unknown-checkpoint` (`run.rs:5266`).
2. **The task's answer doesn't release it.** If the transfer did land, the task still owes and delivers its `handoff-result` exchange. `accept_handoff_result` applies it (the cell settles, `HandedOff` is recorded at `run.rs:1941`, or the run resumes on a refusal) but leaves the effect `Indeterminate`, so `settle_run_disposition` keeps the run parked. The exchange answers `accepted`, so the task marks it settled and never re-drives it.

No test drives a handoff send to exhaustion at the run level today. The only related test (`rakka-a2a/tests/handoff_surface.rs:955`) pins the executor's `handoff-unrecorded` code.

## What the brief missed

These came out of the assessment and are part of this design:

- **A `Refused` notice proves the send executed.** The task owes a handoff result only from its recorded provenance (`owed_handoff_result`, `task.rs:6314`). Settling the generation "not executed" on a live run would start a new generation and send the handoff again. Ruling 1.
- **A second path re-parks with no checkpoint.** `AgentEffectResolution::validate` accepts `ConfirmedExecuted { Exhausted }`. On a handoff send that re-enters the same arm and parks again, after the resolution path has already dropped the checkpoint.
- **The notice can arrive first.** A handoff result can land while the send is still in flight. A later `Exhausted` result then parks a run whose record already holds the proof.
- **Opening the checkpoint makes all five decisions reachable,** and two of them do the wrong thing for a handoff today:
  - `AbandonAndFail` takes the handoff arm's *survivable* path. The run resumes, under the false tool result "no transfer was recorded", although the decision's contract (`checkpoints.rs:570`) says the run fails.
  - `Compensate` winds the run down but leaves the cell `Pending`. If the transfer never landed, no notice ever comes, and `awaits_children` holds the run `Cancelling` forever.
- **Records already parked won't heal.** Where the notice already arrived, the task considers it delivered. Where it hasn't, the record has no checkpoint. Neither changes without a repair (ruling 4).
- **The docs are wrong in two places, not one:** `docs/rakka-v1-reliability-boundaries.md:224` and `docs/rakka-agents.md:101`.
- **Not fixed here (ruling 5):** the handoff send shares the `a2a_send` effect spec (`effect.rs:945`), which can carry a credential binding. If credential resolution fails, the send never leaves, but the spent budget still arrives `Exhausted` and parks the run as if the transfer were ambiguous. That is the #103 pattern, and it gets its own issue.

## Design

Everything lives in `crates/rakka-agent/src/run.rs` (plus doc comments in `checkpoints.rs` and `dispatch.rs`). No durable field, schema version, public type, or stable code is added.

### D1. One question decides how a handoff send's outcome lands

Every site below asks the same thing: does the run's handoff cell hold this effect, and has the task answered? A private classifier answers it:

```rust
/// How the run's handoff cell stands toward one send effect.
enum HandoffSend {
    /// The effect is not the run's handoff send.
    NotHeld,
    /// The cell holds the effect and the transfer awaits the target
    /// (`Pending` or `Sent`).
    Unresolved,
    /// The task answered: the cell settled `Accepted` or `Refused` from a
    /// handoff-result notice. The send executed.
    ResolvedByTask,
    /// The cell settled `Failed` on the run's own decision.
    SettledLocally,
}

fn handoff_send(loop_state: &AgentLoopState, effect_id: &AgentEffectId) -> HandoffSend
```

The existing handoff branch of the `Failed | Exhausted` arm (`run.rs:4504`) narrows from "the cell holds this effect" to `HandoffSend::Unresolved`. A failure can reach a cell that is no longer unresolved in two ways. A task-resolved cell is D4's case, checked first. A locally settled cell is reached only by the new abandonment path (D5), which must take the shared wind-down.

### D2. The checkpoint opens wherever a result leaves an effect parked (Ask 1)

`record_effect_result` opens the `IndeterminateEffectReconciliation` checkpoint when the effect **ends the transition `Indeterminate`** on a live run, whatever the outcome was. The `Indeterminate` outcome always did this; the handoff send's `Exhausted` now does too. It uses the same `open_effect_checkpoint`, so the id (`AgentCheckpoint::id_for_effect`, `{effect}#ck-reconcile-g{n}`), the five default decisions, the summary ("Reconcile indeterminate effect on turn N"), the SLA deadlines (escalation, never expiry), the parked span identity and the `checkpoint-open` segment (diffed generically at `run.rs:6945`) are exactly the ordinary path's.

After D2 and D5, the invariant is: **every `Indeterminate` effect on a live run has its reconciliation checkpoint.** The arms that set the status are the `Indeterminate` outcome (`run.rs:4754`) and the handoff `Exhausted` arm (`run.rs:4515`). Both go through `record_effect_result`, and D5 closes the only reconciliation path that could reach the second one. D6 restores the invariant for records written before.

### D3. The task's answer settles a parked send as executed (Ask 2)

`accept_handoff_result`, before it applies the notice, settles a parked send: when the cell's effect is `Indeterminate` on a live run, the generation resolves as executed, exactly as `ConfirmedExecuted` with a receipt would, and its reconciliation checkpoint drops. This all happens in the same compare-and-set as the notice.

- **The receipt** is built from the notice: `handoff` is the notice's (already checked equal to the cell's), `target_generation` is the accepted generation (`None` on a refusal), and `peer_status` is `"accepted"` or `"refused"`. The receipt is never persisted; the A2aHandoff arm reads only its handoff id and generation.
- **The resolution** runs `apply_effect_outcome` with `AgentRunEffectOutcome::A2aHandoff { receipt }`. That marks the effect `Succeeded` and the cell `Sent`, and rests the turn. The generation's attempts were billed when it parked, so `first_resolution` is false and nothing is billed twice. The `last_error_code`, `last_error_reason` and `unknown_outcome` the park recorded stay, under the issue #100 ruling that a confirmation keeps the generation's history.
- **Then the notice applies as it always has.** `Accepted` corrects the cell, records the tool result and `HandedOff`, and `settle_run_disposition` terminalizes the run `HandedOff`. `Refused` settles the cell refused, records the failed tool result, rests the turn, and the run resumes (ruling 1).
- **A winding-down parked run** (cancellation requested while parked) ends the same way the unparked one does: `HandedOff` wins on acceptance, and the standing cancellation terminalizes on refusal.
- **A duplicate notice to a settled cell** keeps its early return. A record whose cell already settled while its send stayed parked is D6's shape, and the settle pass `accept` already triggers (`make_local_progress`) repairs it.

A shared helper does the settle-and-drop, for D3 and D6:

```rust
/// Resolves the run's parked handoff send as executed, from the task's
/// answer, and drops its reconciliation checkpoint. Answers whether there
/// was a parked send to settle.
fn settle_parked_handoff_send(
    state: &mut AgentRunState,
    receipt: AgentA2aHandoffReceipt,
    now: AgentTimestampMillis,
) -> AgentRunResult<bool>
```

### D4. A send result after the task already answered

In the `Failed | Exhausted` arm and the `Indeterminate` arm, a handoff send classified `ResolvedByTask` is **executed, whatever the dispatcher reports**: the task's durable record is the proof. The effect settles `Succeeded`, the turn rests on a live run that is not winding down and awaits no other turn effect (as the A2aHandoff arm does), and the attempt reservation settles once. The cell and the terminal reason are untouched, because the notice already set them. So:

- after an `Accepted` notice, the run (held `Cancelling` by the in-flight send) terminalizes `HandedOff`;
- after a `Refused` notice, the run resumes.

The A2aHandoff (success) arm already handles this order correctly and does not change.

### D5. The decisions on a handoff send's checkpoint

With D2, a handoff send's park offers the five ordinary decisions. Each now has a defined meaning for a handoff:

| Decision | Before | After |
|---|---|---|
| `ConfirmedCompleted` with an `A2aHandoff` receipt | The effect succeeds and the cell goes `Sent`; the run waits for the notice. | Unchanged. |
| `ConfirmedCompleted` with `Failed` | The cell settles failed, the call fails, and the run survives. | Unchanged. |
| `ConfirmedCompleted` with `Exhausted` | Re-parks `Indeterminate` with no checkpoint. | **Refused**, `effect-policy-invalid`; nothing changes. |
| `ConfirmedNotExecuted`, live run | A new generation re-sends; the task deduplicates on the handoff id (`coordination.rs:1421`), so a send that did land converges. | Unchanged. |
| `ConfirmedNotExecuted`, winding down | The effect is cancelled and the cell settles failed `run-winding-down`. | Unchanged. |
| `Compensate` | The effect is compensated and the run winds down, but the cell stays `Pending`, which can hold the run `Cancelling` forever. | The cell **settles failed `compensated`** in the same compare-and-set (ruling 3). |
| `Escalate` | Keeps the wait. | Unchanged. |
| `AbandonAndFail` | The cell settles failed with the false result "no transfer was recorded", and the run **survives**. | The cell settles failed **`reconciliation-abandoned`**, and the run **fails**, `EffectFailed` under `reconciliation-abandoned` (ruling 2). |

The same rules hold on the effect-layer path (`ResolveIndeterminateEffect`) for the two decisions it carries.

- **The refusal** sits in `apply_indeterminate_resolution`, after its status fence and before any change. A `ConfirmedExecuted` whose outcome is `Exhausted`, on the run's handoff send in any standing, returns `AgentEffectError::InvalidPolicy` (stable code `effect-policy-invalid`, the code `validate()` already gives an `Indeterminate` or `Cancelled` confirmation). The message says an exhausted outcome is what parked the send and resolves nothing. Through `ResolveCheckpoint`, the error rejects the whole transition, so the checkpoint stays open in the durable record.
- **Abandonment.** The `Abandoned` arm of `resolve_checkpoint` settles an `Unresolved` handoff cell `Failed { code: "reconciliation-abandoned" }` before it applies the abandonment failure. With D1's narrowing, the failure then takes the shared wind-down: unsent effects are fenced, `EffectFailed` is recorded unless a wind-down reason already stands (scenario 57), and `settle_run_disposition` terminalizes, because a `Failed` cell no longer `awaits_children`. No tool result is recorded, as for any abandoned effect.
- **Compensation.** `schedule_compensation`, after it marks the effect `Compensated`, settles an `Unresolved` handoff cell `Failed { code: "compensated" }` (the effect record's own code). That releases `awaits_children`, so the run terminalizes `EffectCompensated` once the compensation's outcome lands.
- **A notice after a local decision** follows the existing contradicted-settled rule (slice 5.1, `accept_handoff_result`): an `Accepted` notice on a live run (a compensating wind-down) corrects the cell and terminalizes `HandedOff`; on a terminal run (an abandonment) it corrects the cell only, and the terminal history stands. A `Refused` notice to a locally failed cell is absorbed as a duplicate.

### D6. Records parked before this change heal on their next settle pass (ruling 4)

Two shapes exist only in records the fixed code can't write:
- **(a)** a live run whose handoff cell the task already resolved (`Accepted` or `Refused`, from a notice applied before this change) while the cell's send is still `Indeterminate`;
- **(b)** a live run with an `Indeterminate` effect and no `IndeterminateEffectReconciliation` checkpoint bound to that effect generation.

They are repaired on the settle pass, following the gap slice 3 precedent (`needs_turn_rest` / `rest_unrested_turn`, `run.rs:728`, `2520`):

- `AgentRun::needs_park_repair()`, crate-private, answers whether the run is live and holds either shape.
- `advance_loop`'s gate becomes `can_advance() || needs_turn_rest() || needs_park_repair()` (`run.rs:7630`).
- `advance_once` checks `needs_park_repair()` first, runs `repair_park(state, policies, now)` as its one transition, and returns `owed_run_exchanges(state, now)`, because shape (a) after an acceptance terminalizes `HandedOff` and owes the ledger settlement.
- `repair_park` in one compare-and-set:
  1. settles shape (a) through `settle_parked_handoff_send`, with the receipt built from the cell's settled status (`Accepted { generation }` gives `Some(generation)` and `"accepted"`; `Refused` gives `None` and `"refused"`);
  2. opens the reconciliation checkpoint for every remaining shape-(b) effect, through `open_effect_checkpoint`;
  3. runs `settle_run_disposition`.

Shape (a) after an acceptance terminalizes `HandedOff`, and after a refusal resumes the run (its turn was already rested by the old accept). Shape (b) keeps the run parked, now with its checkpoint, so a later notice settles it through D3.

Shape (b) is deliberately not limited to handoff sends: under the D2 invariant no live run can hold one, so the predicate only ever matches a record from an older binary. Terminal runs are excluded. A terminal run's checkpoint-less `Indeterminate` promotion or claim append is the deliberate shape `record_effect_result`'s comment describes (`run.rs:4211-4218`).

A record heals on the first settle pass that touches it: any `settle_side_effects`, and any command or exchange the run accepts.

### D7. The docs agree with the code (Ask 3)

- `docs/rakka-v1-reliability-boundaries.md:224` and `docs/rakka-agents.md:101`: a delegation send's exhaustion is a fan-in disposition; a handoff send's definitive refusal fails the call and the run survives; and a handoff send that exhausts without an answer parks the run behind a reconciliation checkpoint, which the task's handoff result settles if it arrives.
- `dispatch.rs:841-844` (the handoff executor's doc): the same, naming the checkpoint and the notice.
- `run.rs` comments: the park condition above `record_effect_result`'s checkpoint open, and the handoff arm's comment.
- `checkpoints.rs`: the `Compensate` and `AbandonAndFail` variant docs say what each does to a handoff send's cell.
- `docs/rakka-compatibility.md`: an "Issue #106" entry above the "Issue #103" one.
- `CHANGELOG.md`: a `### Fixed` entry.
- `docs/plans/rakka-agent/spec.md` needs no change. Section 8.9 already names "explicit recovery/reconciliation" as the other way a handoff resolves.

## Durable and wire compatibility

- No durable field, serde attribute, schema version or wire shape changes. Every record that decodes today decodes the same.
- No public type, method or stable code is added. `needs_park_repair` is crate-private.
- What changes is behaviour, and the compatibility entry states each item:
  1. An exhausted handoff send's park now carries an open reconciliation checkpoint.
  2. A handoff-result notice settles a parked send as executed.
  3. A send result after the notice settles the send as executed.
  4. `ConfirmedCompleted`/`ConfirmedExecuted` with an `Exhausted` outcome is refused `effect-policy-invalid` on a handoff send.
  5. `AbandonAndFail` on a handoff send fails the run (it used to survive).
  6. `Compensate` on a handoff send settles the cell failed `compensated`.
  7. The settle pass repairs both pre-fix shapes.
- **A rolling downgrade** to a binary without this change reads the new checkpoint as an ordinary reconciliation checkpoint. Its decisions then follow that binary's rules, which include the old `AbandonAndFail` survival.

## What this does not change

- The executor's probe, and when a send counts as exhausted. The `Exhausted → Indeterminate` mapping for an unresolved handoff send stands, and a definitive refusal (`Failed`) is still survivable.
- The five default decisions on the checkpoint, and that a reconciliation checkpoint never hard-expires.
- **The credential-resolution park** (ruling 5): a handoff send whose credential can't be resolved still parks. The follow-up issue is filed before the PR.
- **Telemetry for the release:** a notice that settles a parked send drops the checkpoint inside an exchange acceptance, which closes no `checkpoint-resolve` segment. That segment belongs to the `ResolveCheckpoint` and `ResolveIndeterminateEffect` commands, and adding one for an exchange is out of scope. The `checkpoint-open` segment for the new park and for the repair needs no new code.
- **Late decisions:** an operator decision that arrives after the notice settled the send is refused as usual: `run-unknown-checkpoint` through the checkpoint, `run-stale-effect-result` through the effect-layer command.

## What rakka.host sees at the pin bump

- **No source change.** The snapshot's shape is the same.
- A parked handoff run now lists one open checkpoint:
  - kind `indeterminate-effect-reconciliation`;
  - `bound_effect` naming the send (`a2a-send-call`);
  - the five allowed decisions;
  - the summary "Reconcile indeterminate effect on turn N".
- A run whose transfer landed no longer waits: it ends `handed-off`, or resumes on a refusal, without any decision.
- **Changed decision semantics:**
  - `abandon-and-fail` on a handoff send now ends the run `failed`;
  - `confirmed-completed` with an exhausted outcome is refused `effect-policy-invalid`.
- Runs the host holds parked today heal on their next settle pass.

## Proof

**Unit tests** go in `run.rs`'s test module, over a shared fixture that builds a run whose handoff cell holds a dispatched send:
1. An exhausted send parks behind its checkpoint. Checks: the id is `id_for_effect(.., reconcile)`, the kind, the summary, the bound effect, an SLA due date and no expiry; the status is `WaitingForReconciliation`; and the cell is still `Pending`.
2. An `Accepted` notice on a parked send: the effect is `Succeeded`, no checkpoint is open, and the run is `HandedOff`.
3. A `Refused` notice on a parked send: the effect is `Succeeded`, no checkpoint is open, the cell is `Refused`, the run is live and not in reconciliation, and the fence is released.
4. A notice first, then `Exhausted` for the in-flight send: `Accepted` ends `HandedOff`, `Refused` resumes, the effect is `Succeeded`, and no checkpoint opened.
5. `ConfirmedExecuted { Exhausted }` on a parked send is refused `effect-policy-invalid`, through both `resolve_indeterminate_effect` and `resolve_checkpoint`.
6. `AbandonAndFail`: the run is `Failed`, with `EffectFailed` under `reconciliation-abandoned` and the cell `Failed` under the same code.
7. `Compensate`: the cell is `Failed { compensated }`, `awaits_children` is false, and the run terminalizes once the compensation's outcome is recorded.
8. Repair shape (b): the checkpoint opens, and the predicate then answers false.
9. Repair shape (a) with an accepted cell: the effect is `Succeeded`, and the run ends `HandedOff`.
10. Repair shape (a) with a refused cell: the run resumes.
11. No repair for a terminal run's checkpoint-less promotion.

**Integration tests** go in a new file, `crates/rakka-agent/tests/handoff_send_park.rs`, through the entity, the scripted dispatcher and a real task:
1. The transfer landed but the answer was lost. An executor applies `RecordHandoff`, then answers a retryable error, which the scripted dispatcher records as `Exhausted`. The run parks, emitting one `checkpoint-open` segment, and ends `HandedOff` once the task answers, with no open checkpoint.
2. The same, with the target never instantiated: the run parks, then resumes and completes.
3. The transfer never landed (the executor errors without applying). The run waits behind the checkpoint, and `AbandonAndFail` through `ResolveCheckpoint` fails the run.
4. A record parked before the fix. The test removes the checkpoint by editing the stored record (the gap slice 3 recipe), and the next settle pass reopens it. This is the proof that `advance_loop`'s gate is wired, which a unit test of `advance_once` can't give.

**Falsification:** each behaviour is broken once on purpose to confirm its test fails: the D2 condition, the D3 settle, the D4 branch, the D5 refusal, abandonment, compensation, the D6 gate and each repair arm.

**Validation:** `scripts/validate.sh` with its exit code captured to a file, plus the per-crate runs the plan lists.
