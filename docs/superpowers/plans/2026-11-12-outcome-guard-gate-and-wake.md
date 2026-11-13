# Outcome Guard, Command-Opened Gates and Live Wake Deadlines Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix issues #125, #126 and #127 in `crates/rakka-agent`, plus the two goal-evaluation gate gaps, in one branch and one PR.

**Architecture:** One guard on the effect decides whether an outcome settles it, and both run paths and the dispatcher's reconciliation path consult it. One helper on the effect picks its gate kind, and every site that opens a gate uses it. One method on the checkpoint names the deadline its timer can still act on, and the snapshot's `next_wake` reads it.

**Tech Stack:** Rust 1.88, the `rakka-agent` crate, `tokio` tests, the in-crate `testkit` and `tests/common` fixtures.

**Spec:** `docs/superpowers/specs/2026-11-12-outcome-guard-gate-and-wake-design.md`. Read it before any task. Section numbers below refer to it.

## Global Constraints

- Branch `outcome-guard-gate-and-wake`, off main `36d742b`. Line numbers in this plan are at `36d742b`; earlier tasks shift them, so find each site by the function name given.
- Every public item needs a doc comment. `cargo clippy -p rakka-agent --all-targets --all-features -- -D warnings` must pass before each commit, and `cargo fmt --all -- --check`.
- A refused command changes nothing: every new check runs before any mutation in its transition.
- No durable schema change, no new decision kind, no new checkpoint kind, no new status.
- Stable codes: add `effect-outcome-mismatch`; change nothing else. `effect-policy-invalid` keeps every use except the ended-run block Task 2 replaces. `workflow-start-run-mismatch` is untouched.
- Each guard gets a falsification: before committing, remove the guard, see the named test fail, restore it. Say which test failed in the task report.
- Each refusal test asserts the state after the refusal, not only the error.
- Runtime events are observability only; no test or code may read a decision event to decide behaviour.
- Commit messages follow the repository's style (a sentence, then `(issue #NNN)`), and end with `Co-Authored-By: Claude Fable 5.2 <noreply@anthropic.com>`.
- The controller runs `cargo clean` between tasks. An implementer never runs it.
- Run tests per file: `cargo test -p rakka-agent --test <file> -- <name>`. Do not run the whole workspace inside a task.
- If an existing test fails for a reason this plan does not name, stop and report it. Do not edit the test.

## Review Focus

- **A replayed gated command.** `PromoteMemory` replayed under the same operation id, after its gate was opened or denied, answers `Duplicate` and opens no second gate. Test in Task 4.
- **A reconciler finding of the right kind and the wrong identity.** A reconciler that answers `Executed` with a `Tool` outcome under another call's id is handled as an unknown finding, like another kind's outcome. Test in Task 3.
- **A re-sent gated promotion on a live run.** `ConfirmedNotExecuted` on a live run's parked gated promotion opens the new generation's gate, and the run's status is not an approval wait. Test in Task 4.
- **A run at the open-checkpoint bound.** A gated `PromoteMemory` on a run already holding 8 open checkpoints is refused `effect-pending-overflow`, and the run records no effect and reserves no attempts. Test in Task 4.
- **An ended run holding both kinds of checkpoint.** With a sweep-skipped checkpoint whose deadline is earlier and a promotion gate whose deadline is later, `next_wake` is the promotion gate's deadline. Test in Task 8.

---

### Task 1: The outcome guard and its error

**Files:**
- Modify: `crates/rakka-agent/src/effect.rs` (`AgentEffectError` near `:2843`, `AgentRunEffectOutcome` impl near `:2445`, `impl AgentRunEffect`)
- Test: inline `#[cfg(test)]` module of `crates/rakka-agent/src/effect.rs`

**Interfaces:**
- Produces:
  - `AgentEffectError::OutcomeMismatch { effect: &'static str, outcome: &'static str, detail: &'static str }`, `code()` = `"effect-outcome-mismatch"`. Public, documented.
  - `pub(crate) enum AgentEffectOutcomeSource { Dispatcher, Confirmation }`
  - `impl AgentRunEffectOutcome { pub(crate) const fn variant_label(&self) -> &'static str }`, returning `model`, `tool`, `memory-promotion`, `evaluation`, `a2a-send`, `a2a-handoff`, `workflow-start`, `workflow-cancel`, `claim-append`, `failed`, `exhausted`, `indeterminate`, `cancelled`.
  - `impl AgentRunEffect { pub(crate) fn check_outcome(&self, outcome: &AgentRunEffectOutcome, source: AgentEffectOutcomeSource) -> AgentEffectResult<()> }`

- [ ] **Step 1: Write the failing unit tests**

In the test module of `effect.rs`. All ten requests of the table in Step 3 must be covered. If a request's record has no builder reachable from this module, cover that row in the integration file Task 2 names for it, and say so in the task report.

- `the_guard_admits_each_requests_own_success_variant`: for every request, its own variant with the matching identity answers `Ok(())` for both sources.
- `the_guard_refuses_another_requests_success_variant`: for a `Tool` effect, each of the other eight success variants answers `Err` with `code() == "effect-outcome-mismatch"`, `effect` equal to the effect's `kind().as_label()`, and `outcome` equal to that variant's label. The same for a `MemoryPromotion` effect.
- `the_guard_checks_identity_inside_a_kind`:
  - a `Tool` effect refuses a `Tool` outcome whose `call_id` is not `call.call_id`;
  - a `Compensation` effect admits a `Tool` outcome under `compensation_call_id(effect)` and refuses one under any other id.
- `the_guard_admits_failure_outcomes_by_source`:
  - `Dispatcher` admits `Failed`, `Exhausted`, `Indeterminate`, `Cancelled`, for any effect;
  - `Confirmation` admits `Failed` and `Exhausted`, and refuses `Indeterminate` and `Cancelled` with `effect-outcome-mismatch`.
- `the_outcome_mismatch_code_is_stable`: `AgentEffectError::OutcomeMismatch { .. }.code() == "effect-outcome-mismatch"`.

- [ ] **Step 2: Run them and see them fail to compile**

Run: `cargo test -p rakka-agent --lib effect::tests::the_guard`
Expected: compile error, `check_outcome` not found.

- [ ] **Step 3: Implement**

`check_outcome` matches on `(&self.request, outcome)` by the table in spec section 5.1.1:

| Request | Admitted success variant | Identity |
|---|---|---|
| `Model` | `Model` | none |
| `Tool { call }` | `Tool` | `call_id == call.call_id` |
| `Compensation` | `Tool` | `call_id == compensation_call_id(self)` |
| `MemoryPromotion` | `MemoryPromotion` | none |
| `Evaluation` | `Evaluation` | `record.effect_id == self.effect_id && record.generation == self.generation` |
| `A2aSend { delegation }` | `A2aSend` | `receipt.delegation == delegation.delegation` |
| `A2aHandoff { handoff }` | `A2aHandoff` | `receipt.handoff == handoff.handoff` |
| `WorkflowStart { invocation }` | `WorkflowStart` | `receipt.invocation == invocation.invocation` |
| `WorkflowCancel` | `WorkflowCancel` | none |
| `ClaimAppend` | `ClaimAppend` | none |

`detail` is one of three fixed strings: `"the outcome belongs to another kind of effect"`, `"the outcome names another call, cell or generation"`, `"a confirmation cannot carry this outcome"`. `Display` writes: `the {outcome} outcome does not settle this {effect} effect: {detail}`.

The match on the request must be exhaustive with no wildcard arm, so a new request kind fails to compile here.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p rakka-agent --lib effect::tests`
Expected: PASS.

- [ ] **Step 5: Lint and commit**

```bash
cargo clippy -p rakka-agent --all-targets --all-features -- -D warnings
git add crates/rakka-agent/src/effect.rs
git commit -m "Add the outcome guard and its mismatch error (issue #125)"
```

---

### Task 2: The run refuses a foreign outcome on both paths

**Files:**
- Modify: `crates/rakka-agent/src/run.rs` (`record_effect_result` near `:4458`, `apply_indeterminate_resolution` near `:5461`)
- Modify: `crates/rakka-agent/tests/private_memory_promotion.rs` (`foreign_confirmations_are_refused` near `:2687`)
- Create: `crates/rakka-agent/tests/effect_outcome_guard.rs`
- Modify (one test each): `tests/goal_evaluation.rs`, `tests/delegation_dispatch.rs`, `tests/handoff_send_park.rs`, `tests/workflow_tool.rs`, `tests/communal_claim_append.rs`, `tests/tool_slot_compensation.rs`

**Interfaces:**
- Consumes: `AgentRunEffect::check_outcome`, `AgentEffectOutcomeSource` (Task 1).
- Produces: nothing later tasks call. Task 3 relies on the run refusing a foreign `RecordEffectResult` with `effect-outcome-mismatch`.

- [ ] **Step 1: Write the failing tests**

In the new `tests/effect_outcome_guard.rs`, with a live run built from the fixtures `private_memory_promotion.rs` uses (`tool_promoting_world`, `answer_effect`, `park_promotion`, `resolve_indeterminate`, `resolve_checkpoint_command`; copy what is needed, the file-local helpers are not shared). A helper `run_state_json(fx)` recovers the run and returns `serde_json::to_value` of its state.

- `a_live_run_refuses_a_foreign_result`: for a live run's outstanding model effect, tool effect and promotion effect in turn, a `RecordEffectResult` carrying each other success variant is refused with `code() == "effect-outcome-mismatch"`, and `run_state_json` is equal before and after.
- `a_live_run_refuses_a_tool_result_under_another_calls_id`: a turn with two tool calls; the result for call A delivered to effect B's id and generation is refused, and neither tool result is recorded.
- `a_live_run_refuses_a_foreign_confirmation`: a live run's parked (`Indeterminate`) promotion; `ResolveIndeterminateEffect` and `ResolveCheckpoint` with `ConfirmedExecuted` carrying a `Model`, an `Evaluation` and a `ClaimAppend` outcome are each refused `effect-outcome-mismatch`; state equal before and after; the reconciliation checkpoint is still open.
- `a_stale_mismatched_result_still_answers_stale`: a foreign outcome delivered for a superseded generation answers `StaleEffectGeneration`, and for a settled effect answers `StaleEffectResult`.
- `a_confirmation_of_an_indeterminate_outcome_keeps_its_code`: `ConfirmedExecuted` carrying `Indeterminate` is still refused `effect-policy-invalid`.

In each owning file, one test named `a_foreign_outcome_does_not_settle_<kind>` that delivers, through `RecordEffectResult`:
- `goal_evaluation.rs`: an `Evaluation` record stamped with another `generation`; refused, the evaluation cell unmoved.
- `delegation_dispatch.rs`: an `A2aSend` receipt naming the run's other held delegation; refused, and the other cell does not move.
- `handoff_send_park.rs`: an `A2aSend` receipt delivered to the handoff send; refused, the handoff cell unmoved. And a confirmed exhaustion of the handoff send is still refused `effect-policy-invalid`.
- `workflow_tool.rs`: a `WorkflowStart` receipt naming another held invocation; refused, the other cell unmoved. A receipt naming the right invocation and the wrong child run still settles the cell failed under `workflow-start-run-mismatch`.
- `communal_claim_append.rs`: a `MemoryPromotion` outcome delivered to a claim append; refused.
- `tool_slot_compensation.rs`: a compensation accepts a `Tool` outcome under `compensation_call_id(effect)` and refuses one under another id.

In `private_memory_promotion.rs`, change the expected code in `foreign_confirmations_are_refused` to `"effect-outcome-mismatch"`, and its doc comment with it.

- [ ] **Step 2: Run them and see them fail**

Run: `cargo test -p rakka-agent --test effect_outcome_guard`
Expected: FAIL. The foreign results are applied (the model outcome installs a pending turn), so the refusal assertions fail.

- [ ] **Step 3: Implement**

- `record_effect_result`: after the `!effect.is_outstanding()` fence and before `effect.record_attempt(attempt, fence)`, call `effect.check_outcome(&outcome, AgentEffectOutcomeSource::Dispatcher)?`.
- `apply_indeterminate_resolution`: delete the `if ended { ... }` block that answers `InvalidPolicy` (near `:5529`). In its place, for a `ConfirmedExecuted { outcome }`, call `effect.check_outcome(outcome, AgentEffectOutcomeSource::Confirmation)?`. It stays after the handoff-exhaustion rule. Keep the comment's reasoning, reworded for every run.

- [ ] **Step 4: Run the new tests, then the files that deliver hand-built outcomes**

Run: `cargo test -p rakka-agent --test effect_outcome_guard --test private_memory_promotion --test goal_evaluation --test delegation_dispatch --test handoff_send_park --test workflow_tool --test communal_claim_append --test tool_slot_compensation`
Expected: PASS.

- [ ] **Step 5: Run the whole crate suite**

Run: `cargo test -p rakka-agent --all-features > <scratchpad>/task2.log 2>&1; echo "exit=$?"`
Expected: exit 0. This is the proof that no legitimate outcome is refused. If a test that hand-builds an outcome fails the guard, stop and report the test and the mismatch. Do not edit it.

- [ ] **Step 6: Falsify, lint and commit**

Remove each `check_outcome` call in turn and confirm a named test fails; restore.

```bash
cargo clippy -p rakka-agent --all-targets --all-features -- -D warnings
git add crates/rakka-agent/src/run.rs crates/rakka-agent/tests
git commit -m "Refuse an outcome that does not belong to its effect, on every run (issue #125)"
```

---

### Task 3: The dispatcher treats a foreign finding as unknown, and keeps the intent's call id

**Files:**
- Modify: `crates/rakka-agent/src/dispatch.rs` (`reconcile_ambiguous` near `:3530`, `invoke`'s `Tool` arm near `:4131`)
- Modify: `crates/rakka-agent/tests/effect_dispatch.rs`
- Modify: `crates/rakka-agent/tests/common/mod.rs` (a decorator beside `ExpiredGrantAuthority`, near `:2569`)

**Interfaces:**
- Consumes: `AgentRunEffect::check_outcome` with `AgentEffectOutcomeSource::Dispatcher`.
- Produces: in `tests/common/mod.rs`, `pub struct RelabelingAuthority<Inner>(pub Inner)`: an `AgentDispatchAuthority` decorator that, on a `Granted` decision for a tool intent, sets `granted.tool_call` to a copy of the intent's call with `call_id` `"relabelled-call"`. It forwards every other method, as `ExpiredGrantAuthority` does.

- [ ] **Step 1: Write the failing tests** in `tests/effect_dispatch.rs`, modelled on `an_unknown_reconciliation_finding_is_requeried_before_any_retry` (near `:685`):

- `a_reconciler_finding_of_another_kind_is_handled_as_unknown`: a reconcileable tool spec with 3 attempts; the reconciler answers `Executed` with a `Model` outcome on every query (script one finding; `ScriptedReconciler` repeats its last). After the ambiguous attempt and repeated lease expiry and pumping:
  - `fx.reconciler.queries()` is bounded by the spec's attempts, not by the number of pumps;
  - `fx.tools.invocation_count(TOOL) == 1`;
  - the run's tool effect is `Indeterminate` with `last_error_code == Some("effect-outcome-mismatch")`;
  - the run is `WaitingForReconciliation` with one open reconciliation checkpoint;
  - no pump returned an error.
- `a_reconciler_finding_under_another_calls_id_is_handled_as_unknown`: the same, with a `Tool` outcome whose `call_id` is `"call-elsewhere"`.
- `a_mismatched_finding_on_a_winding_down_run_parks_at_once`: cancel the run before the reconciliation pass; one query, and the effect parks `Indeterminate` under `effect-outcome-mismatch`. Model it on the existing test that expects `reconciliation-unknown`.
- `a_transformed_call_settles_under_the_intents_own_call_id`: the pipeline's authority wrapped in `RelabelingAuthority`; the tool runs once, the run records the tool result under the model's own call id, and the run completes.

- [ ] **Step 2: Run them and see them fail**

Run: `cargo test -p rakka-agent --test effect_dispatch -- a_reconciler_finding a_mismatched_finding a_transformed_call`
Expected: FAIL. The reconciler tests fail because the run now refuses the delivery and the pass returns the error with no attempt burned. The relabelling test fails with `effect-outcome-mismatch` from the run. Record the observed failure of the first test in the task report: the spec expects an unbounded re-query here.

- [ ] **Step 3: Implement**

- Extract the body of `reconcile_ambiguous`'s `Unknown` arm into a private method, called by that arm with today's values unchanged:

```rust
async fn settle_unknown_finding(
    &mut self,
    scope: &AgentRunScope,
    claim: AgentDispatchClaim,
    intent: &AgentRunEffect,
    attempt: u32,
    winding_down: bool,
    line: &str,                 // "reconciliation outcome unknown"
    winding_down_code: &str,    // "reconciliation-unknown"
    exhausted_code: &str,       // "reconciliation-exhausted"
    pass: &mut AgentDispatchPass,
) -> AgentDispatchResult<ClaimConclusion>
```

- In the `Executed { outcome }` arm, before `deliver_outcome`: if `intent.check_outcome(&outcome, AgentEffectOutcomeSource::Dispatcher)` is an error, return `settle_unknown_finding` with `line = "reconciliation outcome mismatched"` and both codes `"effect-outcome-mismatch"`.
- In `invoke`'s `Tool` arm, bind the intent's own `call.call_id` before the name `call` is shadowed by the granted call, and pass that id to `reviewed_tool_outcome`. The granted call still executes.
- `attempt_invocation` and `deliver_outcome` are not changed.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p rakka-agent --test effect_dispatch --test dispatch_failure_outcomes`
Expected: PASS, including every existing test.

- [ ] **Step 5: Falsify, lint and commit**

Remove the `check_outcome` branch, then the call-id binding; each must fail a named test. Restore.

```bash
cargo clippy -p rakka-agent --all-targets --all-features -- -D warnings
git add crates/rakka-agent/src/dispatch.rs crates/rakka-agent/tests/effect_dispatch.rs crates/rakka-agent/tests/common/mod.rs
git commit -m "Handle a reconciler's foreign finding as unknown and settle a tool under its own call id (issue #125)"
```

---

### Task 4: A promotion or claim append made by command opens its gate, outside the turn

**Files:**
- Modify: `crates/rakka-agent/src/effect.rs` (`impl AgentRunEffect`)
- Modify: `crates/rakka-agent/src/run.rs` (the tool-turn gate block near `:4140`, the re-send gate near `:5648`, `resolve_checkpoint`'s `Granted` arm near `:6110`, `promote_memory` near `:6400`, `append_claim` near `:6491`)
- Modify: `crates/rakka-agent/src/loop_runtime.rs` (`approval_family_wait` near `:1178`)
- Modify: `crates/rakka-agent/tests/private_memory_promotion.rs`, `crates/rakka-agent/tests/communal_claim_append.rs`

**Interfaces:**
- Produces:
  - `impl AgentRunEffect { pub(crate) fn gate_kind(&self) -> Option<AgentCheckpointKind> }`: `Some(SecurityAuthorization)` when `authorization_required`, else `Some(Approval)` when `checkpoint_required`, else `None`.
  - In `private_memory_promotion.rs`, a helper `assert_gate_invariant(fx: &Fixture)`: every held effect with `kind().outside_the_turn()`, status `Pending`, `gate_kind` not `None`, and no grant for its generation has a waiting checkpoint bound to its `effect_id` and `generation`. Read `granted` from the operational snapshot's `pending_effects`, or from the loop state's `grant_for`.

- [ ] **Step 1: Rewrite the two pinning tests and write the new failing tests** in `private_memory_promotion.rs`

Rewrites (keep both, change name, doc comment and assertions):
- `a_gated_promotion_on_an_ended_run_opens_no_approval` becomes `a_gated_promotion_on_an_ended_run_opens_its_approval`: one `Approval` bound to the promotion opens with the command; `allowed_decisions` on the snapshot view holds nothing an ended run refuses; a grant dispatches it through `pump_post_terminal`, the private store holds the memory, and `assert_terminal_unmoved` holds throughout.
- `a_gated_promotion_in_a_tool_calling_turn_opens_its_approval` becomes `a_gated_promotion_opens_one_approval_at_the_command`: the approval exists straight after `promote_command`, before `drive_turn`; after `drive_turn` there is still exactly one; the run's status and phase equal those of the same world with an ungated promotion.

Update the doc comment of the fixture `gated_promotion_in_a_tool_turn`: the gate now opens at the command. The tests that use the fixture must still pass unchanged.

New tests:
Every new test in this task calls `assert_gate_invariant` after each command it applies.

- `a_gated_promotion_by_command_opens_its_gate_on_a_live_run`: `Approval` for `with_checkpoint_required()`; `SecurityAuthorization` for a spec that requires authorization (alone, and with a checkpoint).
- `a_promotion_gate_moves_no_status_or_phase`: in each of three rests (awaiting the model, awaiting tools, resting on a proposal), record `(status, phase)`, then open, grant; open, deny; open, expire by timer. After every step `(status, phase)` is unchanged and `assert_gate_invariant` holds. After the grant the promotion dispatches and its receipt lands. After the denial and the expiry the promotion is `Failed` and the run is not winding down.
- `a_promotion_gate_on_a_failing_run_stays_answerable`: the gate is open when a tool fails; the run is `Cancelling`, the gate is still open, and `(status, phase)` do not move when it is granted. Once the promotion's receipt lands the run ends under its original terminal reason. In a second world a denial, and in a third an expiry, ends the run under that same reason.
- `a_commanded_gate_records_its_request_event`: with a decision sink under `Version2`, the gated command records exactly one `request-approval` (or `request-authorization`) event naming the checkpoint, and a cancel records the checkpoint's close as it does for any gate.
- `a_tool_calling_turn_opens_no_gate_for_a_promotion`: deny the promotion's approval while the run awaits its model call, then `drive_turn`; no checkpoint is open and the promotion is `Failed`.
- `a_replayed_gated_promotion_opens_one_gate`: the same `promote_command` twice answers `Applied` then `Duplicate`, with one checkpoint; and again after a denial, still `Duplicate`, with none.
- `a_re_sent_gated_promotion_on_a_live_run_opens_the_new_generations_gate`: park the gated, granted promotion `Indeterminate` on a live run, confirm `ConfirmedNotExecuted`; a gate is open for generation 2, and the run's status is neither `WaitingForApproval` nor `WaitingForAuthorization`.
- `a_gated_promotion_at_the_checkpoint_bound_is_refused_whole`: with 8 open checkpoints, the command is refused `effect-pending-overflow`; the effect count and the budget's `reserved_attempts` are unchanged.

In `communal_claim_append.rs`: `a_gated_claim_append_by_command_opens_its_gate`, on a live run and on an ended one, and a grant lands the claim.

- [ ] **Step 2: Run them and see them fail**

Run: `cargo test -p rakka-agent --test private_memory_promotion -- gated a_promotion_gate a_tool_calling_turn a_replayed a_re_sent`
Expected: FAIL: no checkpoint opens at the command.

- [ ] **Step 3: Implement**

- `gate_kind` on the effect. Use it at the tool-turn gate block and at the re-send gate in `apply_indeterminate_resolution`, replacing their inline `if authorization_required` chains.
- Tool-turn gate block: add `!effect.kind().outside_the_turn()` to its filter.
- `promote_memory` and `append_claim`: after `record_effect`, read the recorded effect's id and `gate_kind()`, and when it is `Some(kind)` call `open_effect_checkpoint(state, policies, &effect_id, kind, now)?`. Write no status.
- `approval_family_wait`: skip a checkpoint whose bound effect is held and has `kind().outside_the_turn()`. Extend its doc comment: such a gate is not the turn's wait.
- `resolve_checkpoint`'s `Granted` arm: write the status only when the run is not terminal and `!run.holds_outside_the_turn_effect(&bound_effect_id)`.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p rakka-agent --test private_memory_promotion --test communal_claim_append --test checkpoint_run --test checkpoints --test decision_events_checkpoints`
Expected: PASS.

- [ ] **Step 5: Falsify, lint and commit**

Falsify each of: the command's `open_effect_checkpoint` call, the tool-turn filter term, the `approval_family_wait` skip, the `Granted` arm's term. Each must fail a named test.

```bash
cargo clippy -p rakka-agent --all-targets --all-features -- -D warnings
git add crates/rakka-agent/src crates/rakka-agent/tests
git commit -m "Open a commanded promotion's or claim append's gate, outside the turn (issue #126)"
```

---

### Task 5: A cancel settles the ungranted gated effect

**Files:**
- Modify: `crates/rakka-agent/src/run.rs` (`cancel` near `:5683`)
- Modify: `crates/rakka-agent/tests/private_memory_promotion.rs`

**Interfaces:**
- Consumes: `gate_kind`, `assert_gate_invariant` (Task 4).

- [ ] **Step 1: Write the failing tests**

- `a_cancel_settles_a_gated_promotion_with_its_gate`: a live run with a gated promotion's approval open; `Cancel`. The checkpoint is cancelled, the promotion is `Cancelled` with `attempts == 0`, the budget's `reserved_attempts` equals its value before the promotion was commanded, and after the run's other work settles its status is `Cancelled`. `assert_gate_invariant` holds.
- `a_cancel_leaves_a_granted_promotion_to_dispatch`: grant first, then `Cancel` before any dispatch; the promotion stays `Pending`, dispatches, and its receipt lands; the run then reaches `Cancelled`.
- `a_cancel_settles_a_gated_promotion_that_has_no_gate`: a record as a binary before this fix left it. Build it with the file's `unbind_checkpoints` helper (near `:3532`) so the promotion is `Pending`, gated, ungranted, with no checkpoint. `Cancel` settles it `Cancelled` and the run reaches `Cancelled`.
- `a_gated_promotion_commanded_while_winding_down_holds_the_run_until_answered`: on a `Cancelling` run, a gated promotion opens its gate and the run stays `Cancelling`; a second `Cancel` settles it.

- [ ] **Step 2: Run them and see them fail**

Run: `cargo test -p rakka-agent --test private_memory_promotion -- a_cancel`
Expected: FAIL: the promotion stays `Pending` and the run stays `Cancelling`.

- [ ] **Step 3: Implement**

In `cancel`, after the loop that records each cancelled checkpoint and before the terminal reason is set: collect the ids of every effect with `kind().outside_the_turn()`, `is_pending()`, `gate_kind().is_some()`, and no `grant_for`. For each, call `apply_effect_outcome(state, &effect_id, &AgentRunEffectOutcome::Cancelled { reason: "run-cancelled".to_string() }, now)?`. That arm sets `Cancelled` and settles the attempt reservation. Extend `cancel`'s doc comment with the rule and its invariant (spec 5.2.4).

- [ ] **Step 4: Run the tests**

Run: `cargo test -p rakka-agent --test private_memory_promotion --test cancellation_propagation --test cancellation_recovery`
Expected: PASS.

- [ ] **Step 5: Falsify, lint and commit**

Falsify the rule as a whole, then its `grant_for` term (the granted test must fail), then its `outside_the_turn` term.

```bash
cargo clippy -p rakka-agent --all-targets --all-features -- -D warnings
git add crates/rakka-agent/src/run.rs crates/rakka-agent/tests/private_memory_promotion.rs
git commit -m "Settle an ungranted gated promotion or claim append when its run is cancelled (issue #126)"
```

---

### Task 6: A goal evaluation opens the gate its spec requires

**Files:**
- Modify: `crates/rakka-agent/src/run.rs` (`evaluate_goal` near `:6576`)
- Modify: `crates/rakka-agent/tests/goal_evaluation.rs`

**Interfaces:**
- Consumes: `gate_kind` (Task 4).

- [ ] **Step 1: Write the failing tests** in `goal_evaluation.rs`, modelled on `a_human_review_approval_satisfies_with_the_resolver_on_record` (near `:776`) and `approve_open_review` (near `:226`):

- `an_evaluation_requiring_authorization_alone_opens_a_security_authorization`: policies with a goal-evaluation spec that requires authorization; `EvaluateGoal` opens one `SecurityAuthorization` bound to the evaluation; a grant dispatches it and the evaluation record lands.
- `an_evaluation_requiring_both_gates_opens_one_security_authorization`: exactly one checkpoint, of kind `SecurityAuthorization`, and no `Approval`.
- `a_human_review_under_an_authorization_spec_takes_its_verdict_from_the_authorizer`: the record's outcome is `Satisfied`, `reason_code == "human-approved"`, and `evaluated_by` is the resolver of the authorization.
- `an_evaluation_requiring_a_checkpoint_alone_still_opens_an_approval`.

- [ ] **Step 2: Run them and see them fail**

Run: `cargo test -p rakka-agent --test goal_evaluation -- an_evaluation_requiring a_human_review_under`
Expected: the first fails with no checkpoint open and the effect `Pending`; the second and third fail on the kind.

- [ ] **Step 3: Implement**

In `evaluate_goal`, replace `checkpoint_owed = effect.checkpoint_required` with the effect's `gate_kind()`, and open that kind. Nothing else changes.

If the third test shows the grant does not reach `granted.checkpoint` for a `SecurityAuthorization`, stop and report. Do not change the dispatcher.

- [ ] **Step 4: Probe the tool-turn block, and report**

Write `a_denied_evaluation_gets_no_fresh_gate_from_a_later_tool_turn`: deny an evaluation's gate while the run awaits its model call, then let that turn call a tool; assert no checkpoint is bound to the evaluation.
- If it passes, keep it.
- If it fails, do not change the gate block and do not commit the test. Put the test body and its failure in the task report.

- [ ] **Step 5: Run the tests**

Run: `cargo test -p rakka-agent --test goal_evaluation --test goal_lifecycle --test goal_stagnation`
Expected: PASS.

- [ ] **Step 6: Falsify, lint and commit**

```bash
cargo clippy -p rakka-agent --all-targets --all-features -- -D warnings
git add crates/rakka-agent/src/run.rs crates/rakka-agent/tests/goal_evaluation.rs
git commit -m "Open the gate a goal evaluation's spec requires (issue #126)"
```

---

### Task 7: The commands export their gate's checkpoint-open segment

**Files:**
- Modify: `crates/rakka-agent/src/run.rs` (the `PromoteMemory`, `AppendClaim` and `EvaluateGoal` arms near `:8141-8174`)
- Modify: `crates/rakka-agent/tests/telemetry_segments.rs`

**Interfaces:**
- Consumes: the existing `segment_marks`, `segment_additions` and `close_checkpoint_open_segments`, used as the `RecordEffectResult` arm uses them (near `:7972-7996`).

- [ ] **Step 1: Write the failing tests** in `telemetry_segments.rs`, beside the test that filters `AgentSegmentOperation::CheckpointOpen` (near `:390`):

- `a_commanded_gate_exports_its_checkpoint_open_segment`: for a gated `PromoteMemory`, a gated `AppendClaim` and a gated `EvaluateGoal` in turn, the command exports exactly one `CheckpointOpen` segment whose checkpoint-kind attribute is the gate's label and whose span id is `AgentCheckpoint::parked_span_identity(&effect.telemetry, &checkpoint_id)`.
- `an_ungated_command_exports_no_checkpoint_open_segment`: the same three commands with ungated specs export none.
- `a_replayed_gated_command_exports_no_second_segment`.

- [ ] **Step 2: Run them and see them fail**

Run: `cargo test -p rakka-agent --test telemetry_segments -- a_commanded_gate`
Expected: FAIL with zero segments for all three commands.

- [ ] **Step 3: Implement**

In each of the three arms: read `segment_marks` from the state before the transition, and after it commits pass `segment_additions(loop_state, &marks).1` to `close_checkpoint_open_segments`. Share the code between the three arms.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p rakka-agent --test telemetry_segments --test checkpoint_resolve_segment --test otel_span_mapping --test trace_scenarios`
Expected: PASS.

- [ ] **Step 5: Falsify, lint and commit**

```bash
cargo clippy -p rakka-agent --all-targets --all-features -- -D warnings
git add crates/rakka-agent/src/run.rs crates/rakka-agent/tests/telemetry_segments.rs
git commit -m "Export the checkpoint-open segment of a gate a command opens (issue #126)"
```

---

### Task 8: `next_wake` names only a deadline a timer will act on

**Files:**
- Modify: `crates/rakka-agent/src/checkpoints.rs` (beside `on_timer`, near `:1033`)
- Modify: `crates/rakka-agent/src/run.rs` (the run record's impl that holds `holds_outside_the_turn_effect`, near `:790`; `fire_checkpoint_timers` near `:6688`)
- Modify: `crates/rakka-agent/src/query.rs` (`AgentOperationalSnapshot::derive`, `next_wake` near `:888`)
- Test: inline tests in `checkpoints.rs`; `crates/rakka-agent/tests/operational_query.rs`; `crates/rakka-agent/tests/private_memory_promotion.rs`

**Interfaces:**
- Produces:
  - `impl AgentCheckpoint { pub fn next_timer_deadline(&self) -> Option<AgentTimestampMillis> }`, documented, public.
  - On the run record: `pub(crate) fn timer_sweep_acts_on(&self, checkpoint: &AgentCheckpoint) -> bool`: `checkpoint.status.is_waiting()`, and on a terminal run also `holds_outside_the_turn_effect(&checkpoint.bound_effect.effect_id)`.

- [ ] **Step 1: Write the failing tests**

In `checkpoints.rs`:
- `the_next_timer_deadline_is_what_on_timer_acts_on`: for every kind (3) and every status, with `due_at = 100`, `expires_at = 200`, with and without an escalation target:
  - when `next_timer_deadline()` is `Some(t)`, a clone's `on_timer(t)` is not `Pending`, and `on_timer(t - 1)` is `Pending`;
  - when it is `None`, `on_timer(u64::MAX)` is `Pending`.
- `the_next_timer_deadline_by_case`: `Open` approval with a target answers 100; without a target 200; `Escalated` answers 200; an `Open` reconciliation checkpoint with a target answers 100, `Escalated` answers `None`; a reconciliation checkpoint given `expires_at` through `with_deadlines` and no target answers `None`; any terminal status answers `None`.

In `operational_query.rs`, using `checkpointed_fixture` with an SLA that sets both deadlines:
- `next_wake_drops_a_due_time_after_escalation`: after the timer escalates, and separately after a person's `Escalate`, `next_wake` is the `expires_at`, not the past `due_at`.
- `next_wake_ignores_a_due_time_with_no_escalation_target`.
- `next_wake_reports_a_deadline_that_has_passed_but_not_fired`.
- `the_timer_at_next_wake_changes_the_record`: applying the run's timer command at `next_wake` changes the stored record; when `next_wake` is `None`, the timer command at a far-future time changes nothing in the checkpoints.
- The checkpoint views still carry the record's `due_at` and `expires_at` unchanged.

In `private_memory_promotion.rs`:
- `an_ended_runs_next_wake_reads_only_what_its_sweep_acts_on`: an ended run with a promotion gate under an SLA reports that gate's deadline. Then add to the stored record a waiting approval bound to an effect that is not outside the turn, with an earlier deadline (edit the stored state as `unbind_checkpoints` does; say in the test's doc comment if no command path reaches this state). `next_wake` is still the promotion gate's deadline, and the timer command at the earlier deadline changes nothing.

- [ ] **Step 2: Run them and see them fail**

Run: `cargo test -p rakka-agent --lib checkpoints::tests::the_next_timer_deadline`
Expected: compile error, `next_timer_deadline` not found.

- [ ] **Step 3: Implement**

- `next_timer_deadline`: `None` unless `status.is_waiting()`; the expiry candidate is `expires_at` unless the kind is `IndeterminateEffectReconciliation`; the due candidate is `due_at` only while `status == Open` and `escalation_target.is_some()`; answer the earlier candidate.
- `timer_sweep_acts_on`, used by `fire_checkpoint_timers` in place of its two inline filters.
- `AgentOperationalSnapshot::derive`: compute `next_wake` as the minimum of `checkpoint.next_timer_deadline()` over the run's checkpoints for which `run.timer_sweep_acts_on(checkpoint)` holds. The `open_checkpoints` views are built as today.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p rakka-agent --lib checkpoints && cargo test -p rakka-agent --test operational_query --test private_memory_promotion --test checkpoint_run --test run_timer_unchanged`
Expected: PASS.

- [ ] **Step 5: Falsify, lint and commit**

Falsify each of: the reconciliation exclusion, the `Open` term, the escalation-target term, the snapshot's use of `timer_sweep_acts_on`.

```bash
cargo clippy -p rakka-agent --all-targets --all-features -- -D warnings
git add crates/rakka-agent/src crates/rakka-agent/tests
git commit -m "Report in next_wake only a deadline a timer will act on (issue #127)"
```

---

### Task 9: Docs, changelog and whole-batch validation

**Files:**
- Modify: `docs/rakka-agents.md` (the reconciliation passage naming `effect-policy-invalid` at `:298`; the passages on gated effects near `:274` and `:394-401`; wherever `next_wake` is described)
- Modify: `docs/rakka-compatibility.md` (one entry, beside the issue #120 entry near `:214`)
- Modify: `CHANGELOG.md` (one entry under Unreleased, beside the issue #120 entry)
- Check: `docs/rakka-agent-telemetry-validation-matrix.md` (`next_wake` near `:281`, and the `checkpoint-open` rows)

- [ ] **Step 1: Write the docs**

Cover exactly what spec section 6 lists, in the documents' existing voice:
- the new code `effect-outcome-mismatch`, where it is answered, and that an ended run's foreign confirmation now answers it where it answered `effect-policy-invalid`;
- that a tool confirmation must carry the effect's own call id, and a compensation's the derived id;
- that a reconciler's `Executed` finding that does not fit the effect is handled as `Unknown`, under `effect-outcome-mismatch`;
- that a commanded promotion or claim append opens its gate at the command, moves no status, and is cancelled with its run when ungranted;
- that a goal evaluation requiring authorization opens a `SecurityAuthorization`, including when it also requires a checkpoint;
- the new `checkpoint-open` segments;
- `AgentCheckpoint::next_timer_deadline` and what `next_wake` now reports;
- the mixed-binary note from spec section 6;
- that issue #128 (a gated compensation or workflow cancel) is open.

- [ ] **Step 2: Run the doc-holding tests**

Run: `cargo test -p rakka-agent --test product_doc_currency --test compatibility_currency --test decision_event_vocabulary --test metric_catalogue`
Expected: PASS. A failure names the claim to fix.

- [ ] **Step 3: Commit**

```bash
git add docs/rakka-agents.md docs/rakka-compatibility.md docs/rakka-agent-telemetry-validation-matrix.md CHANGELOG.md
git commit -m "Document the outcome guard, command-opened gates and live wake deadlines (issues #125, #126, #127)"
```

- [ ] **Step 4: Validate the whole batch**

Run from the repository root, redirecting to a file and reading the exit code directly, never through a pipe:

```bash
scripts/validate.sh > <scratchpad>/validate.log 2>&1; echo "exit=$?"
```

Expected: `exit=0`. If the workspace test step is killed under load, run the workspace tests crate by crate and report each result.

- [ ] **Step 5: Add an "As built" section to the spec**

Append to the spec whatever the build found that differs from it: the Task 6 probe's result, any stopped-and-reported item and how it was resolved, the observed pre-fix behaviour of the reconciler test in Task 3. Commit it.
