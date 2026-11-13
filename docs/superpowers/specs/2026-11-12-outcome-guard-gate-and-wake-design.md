# Outcome guard, command-opened gates, and live wake deadlines (issues #125, #126, #127)

Status: design, approved by the owner on 2026-11-12. Branch `outcome-guard-gate-and-wake`, off main `36d742b`.

## 1. Intent

Three defects were found while building issue #120 (PR #124) and filed as follow-ups. All three sit in `crates/rakka-agent`, in code PR #124 touched. This batch fixes them in one branch and one PR.

- **#125.** The run applies an effect outcome without checking that it belongs to the effect it settles. A model outcome confirmed onto a parked memory promotion installs a model turn.
- **#126.** A deployment can gate memory promotions and claim appends behind an approval, but a promotion or claim append made by command never opens that approval. The effect waits `Pending` forever.
- **#127.** `next_wake` on the run's operational snapshot reports a checkpoint deadline that no timer will act on.

Two more gaps of #126's family were found while designing, both in goal evaluation. The owner folded them into this batch on 2026-11-12 (section 5.2.7):

- A goal evaluation whose spec requires authorization alone never opens its gate.
- The command that opens a goal evaluation's gate exports no `checkpoint-open` segment for it.

The host (rakka.host, pinned at `36d742b`) reaches none of the three today. It never gates the two kinds, sets no checkpoint SLA, and builds every confirmation from the effect's own kind. So nothing here is urgent for the host. The reason to do it now is that each is a real hole for any other deployment, and the context is fresh.

## 2. The code at `36d742b`, verified

Paths are under `crates/rakka-agent/src/`. Each fact below was read at `36d742b` on 2026-11-12.

### 2.1 Outcomes (#125)

- `record_effect_result` (`run.rs:4458`) fences on terminal status, unknown effect, generation, and outstanding status, then calls `apply_effect_outcome` (`run.rs:4604`) with whatever outcome it carries.
- `apply_indeterminate_resolution` (`run.rs:5461`) does the same for a confirmation. It checks the outcome only on an ended run (`run.rs:5529`): the effect's own receipt, `Failed`, or `Exhausted`, else `effect-policy-invalid`.
- `apply_effect_outcome` matches on the outcome's variant, never on the effect's kind. The `Model` arm installs a pending turn and moves the phase. The `Tool` arm records a tool result under the outcome's `call_id`.
- The `Tool` arm never compares the outcome's `call_id` with the effect's call. `record_tool_result` (`loop_runtime.rs:1413`) records under whatever id it is given. So a result for call A can record under call B.
- The `A2aSend` arm looks the delegation cell up by `receipt.delegation` (`run.rs:4731`). A receipt naming another held delegation settles that other cell. A receipt naming none returns `UnknownDelegation`. The `A2aHandoff` and `WorkflowStart` arms have the same shape.
- `AgentRunEffectKind::A2aSendCall` covers two requests: `AgentRunEffectRequest::A2aSend` and `A2aHandoff`. So the check must read the request, not only the kind.
- A compensation settles through the `Tool` arm, under `compensation_call_id(effect)` (`effect.rs`, "The stable call id a compensation effect's result settles under").
- `AgentEffectResolution::validate` already refuses a confirmation that carries `Indeterminate` or `Cancelled`, with `effect-policy-invalid`. The handoff rule at `run.rs:5515` refuses a confirmed exhaustion of a handoff send, with the same code.
- **The dispatcher's own outcomes.** `invoke` (`dispatch.rs:4082`) builds the outcome's variant from the intent's request, and fills every receipt's identity and the evaluation record's `effect_id` and `generation` from the intent (`dispatch.rs:4224`, `4256`, `4293`, `4417`). So an executor cannot make the built-in dispatcher produce another kind's variant or another cell's receipt.
- **Two dispatcher paths can still carry a foreign outcome.**
  - `reconcile_ambiguous` delivers a reconciler's `AgentReconciliationFinding::Executed { outcome }` verbatim (`dispatch.rs:3572`). The outcome is application-supplied.
  - A tool outcome settles under the `call_id` of `granted.tool_call` when the authority transformed the call (`dispatch.rs:4131`, `4153`). The built-in authority keeps the id; `AgentGrantedDispatch.tool_call` is a public field, and a custom authority need not.
- `deliver_outcome` (`dispatch.rs:4534`) treats `StaleEffectResult`, `StaleEffectGeneration`, `UnknownEffect` and `Terminal` as convergence. Any other refusal propagates as an error, the claim is not settled, and the ticket is claimed again. This last step comes from reading and has no test.
- `reconcile_ambiguous`'s `Unknown` arm (`dispatch.rs:3603`) is the existing path for a reconciler that cannot establish the outcome. On a live run it burns one attempt and leaves the row retry-scheduled, so a later claim queries the protocol again, and a spent budget parks the generation. On a winding-down run it parks `Indeterminate` at once.
- `AgentEffectError` (`effect.rs:2843`) is `#[non_exhaustive]`.
- The host builds a confirmation's `call_id` from the effect itself: `call.call_id` for a tool, `compensation_call_id(effect)` for a compensation (rakka.host `run_port.rs:617-619`, at `f2a95b2`). An identity check does not break it.

### 2.2 Gates (#126)

- `promote_memory` (`run.rs:6400`) and `append_claim` (`run.rs:6491`) record the effect with the deployment's spec and open no checkpoint.
- `is_dispatchable` (`loop_runtime.rs:1211`) is false for a gated effect with no grant, so the settle pass never flushes it.
- `evaluate_goal` (`run.rs:6576`) opens its own `Approval` in the transition that records the effect (`run.rs:6655`), and moves no status.
  - It opens the gate only when the effect requires a checkpoint (`run.rs:6642`), and always as an `Approval`.
  - So an evaluation spec that requires authorization alone records a gated effect with no gate. `is_dispatchable` is false for it, and it waits `Pending`. This comes from reading.
  - A spec that requires both opens an `Approval`, where the tool-turn block opens a `SecurityAuthorization` (`run.rs:4154`).
  - A human-review evaluation forces `checkpoint_required` (`run.rs:6623`), and the dispatcher builds its verdict from the grant in `granted.checkpoint` (`dispatch.rs:4424`).
- The tool-calling turn's gate block (`run.rs:4140-4165`) opens a checkpoint for every effect of the current turn that is gated and holds no grant. It reads neither the kind nor the status.
- `open_effect_checkpoint` (`run.rs:5738`) derives the checkpoint id from the effect, generation and kind, and `record_checkpoint` (`loop_runtime.rs:1228`) ignores an id it already holds. Opening twice is a no-op while the first is held.
- The re-send path (`run.rs:5648-5657`) already re-opens the gate for a new generation of any gated kind.
- **Cancel.** `cancel` (`run.rs:5683`) calls `fence_unsent_effects`, which skips the four fence-exempt kinds (`loop_runtime.rs:1056`), then `cancel_open_checkpoints`, which closes every waiting approval and authorization (`loop_runtime.rs:1285`). A `Pending` gated promotion is left `Pending` with no gate and no grant. `awaits_settlement` (`loop_runtime.rs:947`) counts it, so `settle_run_disposition` keeps the run `Cancelling` and nothing can move it. This comes from reading; the accidental tool-turn path can reach it today.
- **Status on a grant.** `resolve_checkpoint`'s `Granted` arm (`run.rs:6110`) writes `checkpoint_wait_status` on any live run. With no approval left that is `WaitingForEffect` (`run.rs:4250`), whatever the run was waiting on.
- `approval_family_wait` (`loop_runtime.rs:1178`) counts every waiting approval and authorization. `turn_rest` reads it through `checkpoint_wait_status`.
- A failure wind-down (`run.rs:5151`) fences effects but does not close approvals. A gated promotion's approval stays open and answerable, and the run waits `Cancelling` on it.
- **Telemetry.** A gate's `checkpoint-open` segment is closed by `close_checkpoint_open_segments`, which only the loop advance and the `RecordEffectResult` arm call (`run.rs:8779`, `7996`). The `PromoteMemory`, `AppendClaim` and `EvaluateGoal` arms (`run.rs:8141-8174`) do not.
- PR #124 pinned today's behaviour in `crates/rakka-agent/tests/private_memory_promotion.rs`: `a_gated_promotion_on_an_ended_run_opens_no_approval` and `a_gated_promotion_in_a_tool_calling_turn_opens_its_approval`.

### 2.3 Wake deadlines (#127)

- `next_wake` (`query.rs:888`) is the earliest `due_at` or `expires_at` over the waiting checkpoints' views.
- `on_timer` (`checkpoints.rs:1033`) acts on `expires_at` only when the kind is not `IndeterminateEffectReconciliation`. It acts on `due_at` only when the status is `Open` and an escalation target is set.
- Escalation, by timer or by a person's `Escalate`, leaves `due_at` as it was.
- The run writes `due_at` whenever the SLA sets `due_after_ms` (`run.rs:5813-5820`), whether or not an escalation target exists.
- On an ended run, `fire_checkpoint_timers` acts only on a checkpoint bound to an effect outside the turn (`run.rs:6699-6706`). An approval a failed run left open on a tool effect stays waiting, and no timer acts on it.
- So `next_wake` names a deadline nothing acts on in four cases:
  - a past `due_at` after escalation;
  - a `due_at` with no escalation target, even before it passes;
  - an `expires_at` on a reconciliation checkpoint, which the run itself never writes;
  - any deadline on a checkpoint an ended run's sweep skips.

## 3. Goals, non-goals, anti-goals

**Goals**

- An outcome that does not belong to its effect is refused before anything changes, on every run and on both paths.
- A foreign outcome that reaches the dispatcher settles in bounded work, as an outcome that is still unknown.
- A gated promotion or claim append made by command opens its gate, on a live run and on an ended one.
- A gated promotion or claim append can never be `Pending` and ungranted with no waiting gate.
- A goal evaluation that requires authorization opens a `SecurityAuthorization`, and the command that opens an evaluation's gate exports its `checkpoint-open` segment.
- `next_wake` names only a deadline `on_timer` will act on.

**Non-goals**

- Issue #123 (a compensation or workflow cancel outstanding when a run ends). It needs its own ruling.
- A repair for records written before this fix that already hold a gated, ungranted promotion or claim append with no gate. No known deployment gates these kinds. A live run in that state can be cancelled cleanly after this fix (section 5.2.4).
- Changing what a failure wind-down does with an open approval.
- Changing `workflow-start-run-mismatch`. A start receipt naming the wrong child run still settles the invocation cell failed, as today.
- Any durable schema change, new decision kind, or new checkpoint kind.
- A gated compensation or workflow cancel, whose gate never opens. It is filed as #128, because the fix needs a ruling: what a denied gate means for a compensation the run has already recorded as done.

**Anti-goals**

- Refusing an outcome the built-in dispatcher legitimately produces today. The whole existing suite must pass unchanged, apart from the two tests section 5.2 names and any test that pins an `Approval` on an evaluation requiring both gates (section 5.2.7).
- Changing what a stale or duplicate delivery answers. The dispatcher depends on those answers for convergence.
- Moving a live run's status or phase because of a gate on an effect outside the turn.

## 4. Constraints

- Error codes are part of the compatibility surface. This batch adds one and changes one shipped a week ago (section 5.1.2).
- Runtime events are observability only. Nothing here may make a decision event the source of correctness.
- Workspace lints: every public item needs a doc comment; clippy runs with `-D warnings`.
- A refused command changes nothing: the checks run before any mutation in the transition.

## 5. Design

### 5.1 The outcome guard (#125)

#### 5.1.1 One rule, read from the effect

A single function on the effect, in `effect.rs`, answers whether an outcome settles this effect. The builder names it; this document calls it the guard. It reads the effect's request, so it distinguishes a delegation send from a handoff send.

An effect accepts exactly one success variant:

| Request | Success variant | Identity that must match |
|---|---|---|
| `Model` | `Model` | none |
| `Tool` | `Tool` | `call_id` equals the request's `call.call_id` |
| `Compensation` | `Tool` | `call_id` equals `compensation_call_id(effect)` |
| `MemoryPromotion` | `MemoryPromotion` | none |
| `Evaluation` | `Evaluation` | the record's `effect_id` and `generation` equal the effect's |
| `A2aSend` | `A2aSend` | `receipt.delegation` equals the request's delegation id |
| `A2aHandoff` | `A2aHandoff` | `receipt.handoff` equals the request's handoff id |
| `WorkflowStart` | `WorkflowStart` | `receipt.invocation` equals the request's invocation id |
| `WorkflowCancel` | `WorkflowCancel` | none |
| `ClaimAppend` | `ClaimAppend` | none |

The guard takes the role the outcome arrives in:

- **A dispatcher result** may also be `Failed`, `Exhausted`, `Indeterminate` or `Cancelled`, for any effect.
- **A person's confirmation** may also be `Failed` or `Exhausted`. `Indeterminate` and `Cancelled` stay refused by `AgentEffectResolution::validate`, and a confirmed exhaustion of a handoff send stays refused by its own rule. Both keep `effect-policy-invalid`.

The identity column is my call, not the owner's ruling: it is the same defect inside a kind. The `Evaluation` row holds only if the built-in dispatcher stamps both fields on every record it delivers; the builder verifies that, and drops the row's identity check if it does not hold.

#### 5.1.2 The error

A new variant, `AgentEffectError::OutcomeMismatch`, with the stable code `effect-outcome-mismatch`. It carries the effect's kind label, the outcome's variant label, and a bounded message. It reaches callers through `AgentRunError` as every effect error does.

Owner's ruling: the new code applies everywhere. PR #124's ended-run refusal of a foreign confirmation changes from `effect-policy-invalid` to `effect-outcome-mismatch`. The host cannot trigger it and does not match on it.

#### 5.1.3 Where it runs

- **`record_effect_result`**: after the stale-generation and stale-result fences, before `record_attempt`. A stale or duplicate delivery answers exactly as today, mismatched or not.
- **`apply_indeterminate_resolution`**: after its stale fences and the handoff-exhaustion rule, for a `ConfirmedExecuted`. It replaces the ended-run block at `run.rs:5529`. On an ended run the guard admits the same three outcomes that block did.

The `UnknownDelegation`, `UnknownHandoff` and `UnknownWorkflowInvocation` returns inside `apply_effect_outcome` stay as they are.

#### 5.1.4 The dispatcher

Owner's ruling: a foreign outcome the dispatcher receives is an outcome that is still unknown.

The question that ruling answered assumed an executor could hand the dispatcher another kind's outcome. Section 2.1 shows it cannot. The ruling is applied at the two places a foreign outcome can actually arrive. Both placements are my call.

- **The reconciler's finding.** In `reconcile_ambiguous`, an `Executed { outcome }` that fails the guard for the intent is handled as the reconciler's `Unknown` finding is today: on a live run it burns one attempt and is asked again later, and a spent budget parks the generation; on a winding-down run it parks at once. The failed-attempt line and the park carry the code `effect-outcome-mismatch`, so an operator can see why.
- **The tool call id.** `invoke` settles a tool outcome under the intent's own `call.call_id`, whatever id a transformed call carries. The tool ran and its result is good; only the label could be wrong, so the fix is at the source and nothing parks.

`attempt_invocation` gets no check of its own: with the two changes above no outcome it builds can fail the guard, so a check there could never be exercised. `deliver_outcome` is unchanged.

### 5.2 Command-opened gates (#126)

#### 5.2.1 The commands open their gate

Owner's ruling: open the gate in the command's own transition.

`promote_memory` and `append_claim`, after recording the effect, open a `SecurityAuthorization` checkpoint when the effect requires authorization, else an `Approval` when it requires a checkpoint. That is the precedence the tool-turn block and the re-send path already use. One helper answers the gate kind for an effect, and all three sites use it.

On an ended run the same code runs. `open_effect_checkpoint` already drops the decisions an ended run refuses, and PR #124's paths answer the gate: a grant dispatches without moving the status, and a denial or expiry fails only that generation.

A run that already holds the maximum number of open checkpoints refuses the command with the existing overflow error, and records nothing.

#### 5.2.2 The gate is outside the turn

My call: a gate on an effect outside the turn moves no status and no phase on a live run, as on an ended one. The open checkpoint is visible in the snapshot's `open_checkpoints`.

- The commands write no status, as `evaluate_goal` does not.
- `approval_family_wait` ignores a checkpoint bound to an effect outside the turn. A reconciliation checkpoint is already excluded there for the same reason: it is not the turn's wait.
- `resolve_checkpoint`'s `Granted` arm skips its status write when the granted effect is outside the turn. Today it skips only on an ended run.
- A denial or expiry applies `Failed` through the existing arm, which already exempts both kinds from the wind-down.

#### 5.2.3 The tool-turn block skips the two kinds

The gate block at `run.rs:4140` skips effects outside the turn. They own their gate. Without the skip, a promotion denied earlier in the turn would get a fresh approval on a failed effect.

#### 5.2.4 Cancel settles the effect with its gate

Owner's ruling: a cancel cancels the effect too.

In `cancel`, in the same transition: every effect outside the turn that is `Pending`, gated, and holds no grant for its generation settles `Cancelled`. Its attempt reservation is released the way a fenced `Pending` effect's is; the builder verifies where that happens. The rule reads the effect, not the checkpoint, so a record written before this fix is covered.

A granted effect is untouched: it is dispatchable and rides the outbox as today.

The invariant this gives, which section 7 proves: **a gated effect outside the turn that is `Pending` and ungranted always has a waiting gate.** Whatever closes the gate without a grant settles the effect: a denial or expiry fails it, a cancel cancels it.

One consequence to know: a gated promotion issued while a run is already winding down opens its gate and holds the run `Cancelling` until a person answers, it expires, or a second cancel arrives. An ungated one holds the run until it dispatches, so this is the same shape.

#### 5.2.5 Decision events

Opening the gate queues `request-approval` or `request-authorization` through `open_effect_checkpoint`, as every gate does. The cancel records the closed checkpoint as it does today. No new kind, field or slot.

#### 5.2.6 Telemetry

The `PromoteMemory`, `AppendClaim` and `EvaluateGoal` arms close the `checkpoint-open` segment for a gate they open, as the loop advance and `RecordEffectResult` arms do.

- For the first two, without it the gate's later resolution would link a parked span that was never exported (specification 17.11), because the tool turn no longer opens this gate.
- For `EvaluateGoal` the segment is missing today. The builder shows that with a failing test first.

#### 5.2.7 Goal evaluation

Owner's decision (2026-11-12): fold the two goal-evaluation gaps into this batch.

`evaluate_goal` picks its gate through the same helper as section 5.2.1:

- authorization required, with or without a checkpoint: a `SecurityAuthorization`;
- a checkpoint alone: an `Approval`, as today;
- neither: no gate, as today.

One behaviour changes beyond the gap, and it is my call: an evaluation that requires both gates opens a `SecurityAuthorization` where it opens an `Approval` today. That is the precedence every other site uses, and it stops a plain approval from satisfying an authorization requirement. A human-review evaluation under such a spec takes its verdict from the authorization's grant; the builder verifies the grant reaches `granted.checkpoint` for that kind, with a test.

Nothing else about an evaluation's gate changes. It still moves no status, it is still counted as the turn's approval wait, and a cancel still closes it and fences the effect, because an evaluation is not exempt from the wind-down fence.

### 5.3 Live wake deadlines (#127)

A new public method on `AgentCheckpoint`, in `checkpoints.rs` beside `on_timer`, answers the earliest deadline `on_timer` can still act on:

- nothing, when the checkpoint is not waiting;
- `expires_at`, unless the kind is `IndeterminateEffectReconciliation`;
- `due_at`, only while the status is `Open` and an escalation target is set;
- the earlier of the two when both apply.

A deadline that has passed but that no timer has fired yet is still reported: the timer will act on it.

`AgentOperationalSnapshot` computes `next_wake` from the checkpoints through that method, not from the views. The views keep `due_at` and `expires_at` exactly as the record holds them. Nothing durable changes.

On an ended run the snapshot also leaves out every checkpoint the run's timer sweep skips. One predicate on the run answers "does the sweep act on this checkpoint", and both `fire_checkpoint_timers` and the snapshot read it, so the two cannot drift.

The second and fourth cases in section 2.3 are beyond the issue. Both are my call; they follow from the same goal.

## 6. Rollout and compatibility

- No durable schema change. No new decision vocabulary.
- New public surface: `AgentEffectError::OutcomeMismatch`, the code `effect-outcome-mismatch`, the checkpoint's deadline method. All additive.
- **Changed code.** A foreign confirmation on an ended run answers `effect-outcome-mismatch` where `36d742b` answers `effect-policy-invalid`.
- **Newly refused.** On a live run, a confirmation or dispatcher result that fails the guard. A deployment whose facade builds a tool confirmation under a `call_id` other than the effect's own is refused where it was applied.
- **Changed behaviour for a deployment that gates goal evaluations behind authorization.** The gate now opens when authorization alone is required. When both gates are required it is a `SecurityAuthorization`, with a `request-authorization` decision event, where `36d742b` opens an `Approval`.
- **New telemetry.** A goal evaluation's gate exports a `checkpoint-open` segment where `36d742b` exports none.
- **Changed behaviour for a deployment that gates the two kinds.** The gate opens at the command. A cancel settles the ungranted effect. A tool-calling turn no longer opens that gate, and the run's status no longer shows an approval wait for it.
- **Mixed binaries.** A `36d742b` binary answering a gate that a new binary opened on a live run writes `WaitingForEffect` on the grant, and counts the gate as an approval wait. Only a deployment that gates the two kinds can see this, and only during the roll.
- **`next_wake`** can now be `None`, or later, where it was a stale time.

## 7. Proofs

Each guard gets a falsification: remove it, and a named test fails. PR #124's reviews found gates whose action was unscoped, so each test asserts the state after the refusal as well as the refusal.

**#125**

- For each request in the table, the wrong variant is refused `effect-outcome-mismatch` on a live run, on both paths, and the run's state is byte-identical before and after.
- A `Tool` outcome under another call's id, and a send, handoff and start receipt naming another cell, are each refused the same way, and the other cell does not move.
- A compensation accepts a `Tool` outcome under its derived call id.
- A stale or duplicate mismatched delivery still answers the stale error.
- The ended-run tests PR #124 added pass with the new code.
- Dispatcher, reconciler: a reconciler that answers `Executed` with another kind's outcome burns one attempt per query and parks the generation at its attempt bound under `effect-outcome-mismatch`; on a winding-down run it parks at once. A failing test first shows today's behaviour, which is expected to be an unbounded re-query.
- Dispatcher, tool call id: an authority that transforms a call under another `call_id` still settles the result under the intent's own id, and the run accepts it.
- The whole existing suite passes, which is the proof that no legitimate outcome is refused.

**#126**

- A gated promotion and a gated claim append, by command, each open their gate on a live run and on an ended run; with authorization required, the gate is a `SecurityAuthorization`.
- On a live run in each rest state (awaiting the model, awaiting tools, resting on a proposal), opening, granting, denying and expiring the gate leave status and phase unmoved.
- On a run winding down after a failure, opening and granting leave status and phase unmoved. A denial or expiry settles the last blocking effect, so the wind-down completes under its original reason; nothing else moves.
- The command that opens a gate exports one `checkpoint-open` segment under the parked identity the checkpoint stores.
- A grant dispatches the effect and the receipt lands.
- A cancel with the gate open settles the effect `Cancelled` and the run reaches `Cancelled`. Without the rule the run stays `Cancelling`: that is the failing test first.
- A cancel leaves a granted, still-`Pending` effect alone, and it dispatches.
- A tool-calling turn opens no second gate, and none on a promotion denied earlier in the turn.
- The two tests PR #124 added to pin the old behaviour are rewritten to the new behaviour, not deleted.
- The invariant in 5.2.4 is asserted after every step of these tests.

**Goal evaluation (5.2.7)**

- An evaluation requiring authorization alone opens a `SecurityAuthorization`; a grant dispatches it and the record lands. Without the fix it opens nothing and waits `Pending`: that is the failing test first.
- An evaluation requiring both gates opens one `SecurityAuthorization` and no `Approval`.
- A human-review evaluation under an authorization-required spec settles `Satisfied` with the authorizer as `evaluated_by`.
- An evaluation requiring a checkpoint alone still opens an `Approval`.
- The `EvaluateGoal` command exports one `checkpoint-open` segment under the parked identity the checkpoint stores. A failing test first shows none today.

**#127**

- After a timer escalation, and after a person's `Escalate`, `next_wake` no longer reports the past `due_at`.
- A `due_at` with no escalation target is never reported.
- A reconciliation checkpoint given an `expires_at` through `with_deadlines` is not reported.
- The tie to `on_timer`: for a checkpoint in each status and kind, `on_timer` at the method's answer acts, and where the method answers nothing, `on_timer` at any time does nothing.
- On an ended run, an approval left open on a tool effect contributes nothing to `next_wake`, and a gate on a promotion does. The run-level tie: the timer command at the reported time changes the record, and with nothing reported it changes nothing.

**Whole batch:** `scripts/validate.sh` exits 0, with its exit code captured from a file redirect, not a pipe.

## 8. Docs and changelog

- `docs/rakka-agents.md`: the reconciliation section that names `effect-policy-invalid` for an ended run (line 298 at `36d742b`); wherever gating of effects and `next_wake` are described.
- `docs/rakka-compatibility.md`: one entry covering section 6.
- `CHANGELOG.md`: one entry under Unreleased.
- `docs/rakka-agent-telemetry-validation-matrix.md`: check the `next_wake` sentence still holds.
- Doc claims in this repo are held by parsing tests; the builder runs them.

## 9. Left to the builder

- The guard's name and exact signature, and the labels the error carries.
- Whether the gate-kind helper lives on the effect or the loop state.
- The `Evaluation` identity check (5.1.1).
- How a cancelled `Pending` effect's reservation is released (5.2.4).
- **To check and report, not to fix.** The tool-turn gate block reads no status. Check with a test whether a goal evaluation denied earlier in a turn gets a fresh gate when that turn later calls tools. If it does, say so before changing the block.

## 10. After the merge

A short report on each of #125, #126 and #127 saying what landed and what changed for a deployment, then the three issues close. The report on #126 also names the two goal-evaluation fixes. #123 stays open, and so does the new issue for a gated compensation or workflow cancel (#128).

## 11. As built

What the build found that differs from sections 1-10, or that they left to the builder. Commits are on `outcome-guard-gate-and-wake`.

### 11.1 Names

- The guard is `AgentRunEffect::check_outcome(outcome, source)`, `pub(crate)` in `effect.rs` (`3e8d928`).
- The role is `AgentEffectOutcomeSource`, `pub(crate)`, with `Dispatcher` and `Confirmation` (`3e8d928`).
- `AgentEffectError::OutcomeMismatch` carries `effect` (the effect's `kind().as_label()`), `outcome` (the new `AgentRunEffectOutcome::variant_label`) and a fixed `detail`. A delegation send and a handoff send share the kind label `a2a-send-call` (`3e8d928`).
- The gate-kind helper is `AgentRunEffect::gate_kind`, `pub(crate)`, on the effect (`ae6d108`). The tool-turn block, the re-send path, `promote_memory`, `append_claim` (through `open_commanded_gate`) and `evaluate_goal` all read it (`ae6d108`, `35fcb3d`).
- The deadline method is `AgentCheckpoint::next_timer_deadline`, public, beside `on_timer` (`bfac254`).
- The shared sweep predicate is `AgentRun::timer_sweep_acts_on`, `pub(crate)`. `fire_checkpoint_timers` and `AgentOperationalSnapshot`'s `next_wake` both read it (`bfac254`).

### 11.2 The #125 proofs as built

- All ten requests are proven in the guard's unit tests in `effect.rs`: each request's own variant admitted, every other success variant refused, the identity checks, and the failure outcomes by source (`3e8d928`).
- On the run, the result path is proven on three effect kinds: a model call, a tool call and a memory promotion, each offered every other success variant (`tests/effect_outcome_guard.rs`, `f1a5f5a`).
- The confirmation path is proven on a parked promotion offered three foreign outcomes (model, evaluation, claim append), through both resolution commands (same file).
- Each other kind is proven once through the result path, in its own test file: evaluation, delegation send, handoff send, workflow start, claim append and compensation. A workflow cancel is proven by the unit tests only.
- The `Evaluation` identity check was kept. `invoke_goal_evaluation` stamps the record with the intent's `effect_id` and `generation`, and no existing test failed the check. The crate suite passed with one existing test edited, `foreign_confirmations_are_refused`, whose expected code is the one section 5.1.2 changes.

### 11.3 The dispatcher (Task 3, `dc1a547`)

- Before the fix, the first reconciler test showed the expected unbounded re-query. Over eight passes the reconciler was queried eight times against a three-attempt budget. Each pass returned `effect-outcome-mismatch`, burned no attempt, and left the effect `Ready` and the run `WaitingForEffect` with no checkpoint. The winding-down case re-queried on every pass too.
- The build found a defect in the existing `Unknown` arm, which section 2.1 assumed parks cleanly. At a spent budget it called `fleet.record_claim_failure` and then `park_indeterminate`, whose `settle_ticket_cancelled` recorded the same claim again. The second record answered `claim-fenced`. The park was durable, but the pass returned the error and the fleet index entry ended `Exhausted`. No existing test reached that branch.
- Ruling: drop the first `record_claim_failure`. The park now settles the ticket once, the pass returns normally, and the fleet entry ends `Cancelled` with the code `indeterminate`, as every other park leaves it. This changes the existing `Unknown` arm too. `an_unknown_finding_at_a_spent_budget_parks_without_error` pins it.
- Both findings share one helper, `settle_unknown_finding`, which takes the failed-attempt line and the two park codes.
- Ruling on the line: section 5.1.4 says the failed-attempt line carries the code, and the plan's line (`reconciliation outcome mismatched`) had dropped it. The line is `effect-outcome-mismatch: the reconciled outcome does not fit the effect`. The `Unknown` arm keeps `reconciliation outcome unknown`.
- The dispatcher also logs a `tracing::warn!` with the effect id, generation and the error's static labels when it handles a mismatched finding.

### 11.4 Command-opened gates (Task 4, `ae6d108`)

- `assert_gate_invariant` lives in `tests/common/mod.rs`, so `private_memory_promotion.rs` and `communal_claim_append.rs` share one definition.
- The checkpoint-bound test reaches the bound on checkpoints alone. Eight ungated promotions are dispatched and come back `Indeterminate` on the live run, so the run holds eight reconciliation checkpoints and one outstanding effect, the model call. `record_effect` cannot refuse, so the `effect-pending-overflow` comes from `record_checkpoint`.
- Status precedence changes with two gates open. A gate on a promotion or claim append is no longer the turn's approval wait. So a run that holds a tool's `SecurityAuthorization` and a promotion's `Approval` reports `WaitingForAuthorization` where `36d742b` reported `WaitingForApproval`. No test holds both gates at once; this follows from `approval_family_wait`.

### 11.5 Cancel (Task 5, `d1c1fa6`)

- Section 5.2.4's premise is false. `fence_unsent_effects` sets a fenced `Pending` effect `Cancelled` without `settle_effect`, so it releases nothing.
- The rule therefore settles through `apply_effect_outcome` with `Cancelled { reason: "run-cancelled" }`. Its shared tail releases the reservation and consumes no attempt.
- A record an older binary already left `Cancelling`, with its gate cancelled and its promotion or claim append still `Pending`, is not repaired by a settle pass. A further `Cancel` under a fresh operation id re-enters `cancel` and settles it.
- No existing test failed when the rule's `outside_the_turn` term was removed. `a_cancel_leaves_a_gated_compensation_as_it_was` was added to hold it. It pins today's behaviour for a gated compensation, which issue #128 may change.

### 11.6 Goal evaluation (Task 6, `35fcb3d`; Task 10)

- No existing test pinned an `Approval` on an evaluation requiring both gates, so the anti-goal's exception in section 3 was not used.
- The `SecurityAuthorization` grant reaches `granted.checkpoint` through the real dispatch authority. The dispatcher did not change.
- The section 9 probe failed. A human-review evaluation denied while the run awaited its model call got a fresh `Approval` from that turn's tool-call gate block when the turn called a tool. The run then reported `WaitingForApproval` on a `Failed` effect. It reproduced with default policies.
- Cause: the block selected effects of the current turn, not outside the turn, with no grant, and read no status. The denial dropped the checkpoint, so `record_checkpoint` no longer held its id and the reopen was new.
- Owner's decision (2026-11-12): fold the fix into this batch. It was built as Task 10, in the commit "Open a tool-calling turn's gate only for an effect that still waits for one (issue #126)". The gate block's filter now also requires the effect to be `Pending`. A gated effect with no grant is never handed to the dispatcher, so an effect that still waits for its gate is always `Pending`, and one that has settled waits for none. Nothing else in the block changed.
- The probe is committed as `a_denied_evaluation_gets_no_fresh_gate_from_a_later_tool_turn` in `tests/goal_evaluation.rs`. Beside it: `an_expired_evaluation_gate_is_not_reopened_by_a_later_tool_turn`, the same with the gate expired by the run's timer; `an_open_evaluation_gate_survives_a_later_tool_turn_unchanged`; and `a_gated_tool_call_still_opens_its_gate`. Removing the `Pending` term fails the first two.

### 11.7 Telemetry (Task 7, `568785c`; Task 7b, `e5a2a40`)

- The first build wrapped `transition` in an async helper. It grew `AgentRunEntityStore::apply`'s future from 11,840 B to 12,416 B in a debug build. `a_gated_promotion_commanded_while_winding_down_holds_the_run_until_answered` then overflowed the 2 MiB test-thread stack.
- The built shape keeps each arm's own `transition` call and puts the shared code in two synchronous helpers around it: `current_segment_marks` and `close_checkpoint_open_segments_since`. The `apply` future is 11,816 B.
- That test ran within one 16 KiB page of the default stack before this batch too. An added task, 7b, made the promotion tests' margin robust, in test code only. It boxed the bottom helper awaits and the world builders' awaits, and flattened one arm of `winding_world`.
- Before 7b the named test needed 2048 KiB of stack, and 32 of the file's 59 tests overflowed at 1.5 MiB. After it the named test needs 1456 KiB, and none overflows at 1.5 MiB.

### 11.8 Wake deadlines (Task 8, `bfac254`)

- Departure: after a person's `Escalate`, `next_wake` is `None`, not the `expires_at` the plan expected. A person can escalate only a reconciliation checkpoint (`check_decision_kind`), and the run writes no `expires_at` on one. The test escalates a reconciliation checkpoint for that half; the timer half asserts `expires_at`.
- Departure: the ended-run state with an approval left open on a tool effect was built by commands, not by editing the stored record as the plan allowed. A gated tool call beside a sibling that fails definitively ends the run `Failed`, and the gated call's approval stays open.

### 11.9 Found and left alone

These are pre-existing and outside this design. Each is a follow-up candidate for the owner.

- The re-send path in `apply_indeterminate_resolution` writes `WaitingForEffect` on a live run whatever the run rested on. A promotion re-sent while the run rests on its proposal or its children comes back `WaitingForEffect`, not `Running`. An ungated promotion behaves the same.
- `fence_unsent_effects` does not release a fenced effect's reserved attempts. It matters only under an `EffectAttempts` limit.
- A run can end `Failed` with a tool call's approval still open, and nothing can answer it.
