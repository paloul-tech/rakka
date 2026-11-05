# Ended-run reconciliation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** An ended run's ambiguous memory promotion or claim append parks behind a reconciliation checkpoint, and either resolution path answers it, without moving the run's terminal record (issue #120).

**Architecture:**
- One predicate scopes every change: on an ended run, an effect is answerable only if the run holds it and its kind is `outside_the_turn()`.
- Each change lives at an existing site, all in `crates/rakka-agent/src/run.rs`:
  - the park's checkpoint open (`record_effect_result`);
  - the two resolution guards (`apply_indeterminate_resolution`, `resolve_checkpoint`);
  - the park repair (`needs_park_repair`);
  - the checkpoint timers (`fire_checkpoint_timers`).
- Two status writes are skipped on an ended run.
- No new type, field, variant or error code.

**Tech Stack:** Rust 1.88 (MSRV), the `rakka-agent` crate, tokio tests.

**Spec:** `docs/superpowers/specs/2026-11-04-ended-run-reconciliation-design.md` (commit `fd42fa2`). Section numbers below are the spec's.

## Global Constraints

- **The terminal record:** on an ended run, `status`, `terminal_reason` and `terminal_at` never move. Every new test asserts all three after each step.
- **Scope:** exactly `AgentRunEffectKind::outside_the_turn()` (`MemoryPromotionCall`, `ClaimAppendCall`). Compensation and workflow cancel stay as today (#123).
- **Codes:** every other refusal on an ended run stays `run-terminal`, byte for byte. No new error code, field, variant or type.
- **No window:** neither resolution reads `check_post_terminal_memory_window`.
- **Re-send:** `ConfirmedNotExecuted` on an ended run re-sends under a new generation (the live arm, never the winding-down arm). It is a person's assertion that the attempt never ran, not a probe.
- **Ended-run checkpoint decisions:** a reconciliation checkpoint opened on an ended run lists exactly `confirmed-completed`, `confirmed-not-executed`, `escalate`, `abandon-and-fail`. `Compensate` on an ended run is refused `run-terminal`.
- **Decision events:**
  - every recording site queues through `queue_decision`, never commits directly;
  - each new scenario proves no hole with `CountingDecisionSink` plus `assert_decision_stream_whole`, and also asserts the events are present, because the helper passes on an empty stream.
- **Lints:** `missing_docs` and `clippy -D warnings`. Every changed doc comment says what the code now does on an ended run.
- **Validation:** redirect `scripts/validate.sh` to a file and capture `$?`; never pipe it into `tail`. Run `cargo clean` at each task boundary.
- **Commits** end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. **A replayed decision on an ended run** (the same operation id) answers `Duplicate` and mints no second generation. Task 2 pins it.
2. **Two decisions for one park through different paths.** A `ResolveIndeterminateEffect`, then a `ResolveCheckpoint` naming the checkpoint it dropped: the second answers `run-terminal`, and nothing is re-sent twice. Task 3 pins it.
3. **A re-sent generation that parks again** opens its own checkpoint (`-g2`) and records its own `request-reconciliation`, with no hole. Task 2 pins it.
4. **A host at the default `Version1` vocabulary** records the ended run's `reconcile`, and `assert_bc0c224_round_trips` holds. Task 2 pins it.
5. **An approval checkpoint a failed run left open on a tool effect** is never expired by a timer on the ended run. Task 4 pins it.

---

### Task 1: The park opens its checkpoint on an ended run (spec 5.1, 5.5)

**Files:**
- Modify: `crates/rakka-agent/src/run.rs`:
  - `record_effect_result` (`:4416`): the condition at `:4498` and the comment at `:4483-4497`;
  - `open_effect_checkpoint` (`:5637`).
- Test: `crates/rakka-agent/tests/private_memory_promotion.rs`, replacing `an_indeterminate_post_terminal_outcome_opens_no_checkpoint` (`:1583`) and `an_indeterminate_post_terminal_outcome_records_no_decision_event` (`:1623`).
- Test: `crates/rakka-agent/tests/communal_claim_append.rs`.

**Interfaces:**
- Produces: `impl AgentRun { pub(crate) fn holds_outside_the_turn_effect(&self, effect_id: &AgentEffectId) -> bool }`. It is true iff the loop state holds `effect_id` and its `kind().outside_the_turn()`. It does not read the status. Tasks 2-4 call it.

- [ ] **Step 1: Write the failing tests.** In `private_memory_promotion.rs`, using `ended_world`, `promote_command`, `resolve_promotion` and an indeterminate outcome:

```rust
#[tokio::test]
async fn an_ended_runs_ambiguous_promotion_opens_its_reconciliation_checkpoint() {
    for ending in Ending::ALL {
        // ended_world(ending); apply promote_command(promotion(1, 1), "ambiguous");
        // resolve_promotion(.., AgentRunEffectOutcome::indeterminate(
        //     "dispatcher-lost-after-started", "the recovery retry was refused"));
        // assert_eq!(parked.status, AgentRunEffectStatus::Indeterminate);
        // let open = <recovered loop state>.open_checkpoints();
        // assert_eq!(open.len(), 1);
        // assert_eq!(open[0].kind, AgentCheckpointKind::IndeterminateEffectReconciliation);
        // assert_eq!(open[0].checkpoint_id, AgentCheckpoint::id_for_effect(
        //     &parked.effect_id, parked.generation,
        //     AgentCheckpointKind::IndeterminateEffectReconciliation));
        // let decisions: Vec<&str> = open[0].allowed_decisions.iter().map(|d| d.value.as_str()).collect();
        // assert_eq!(decisions, ["confirmed-completed", "confirmed-not-executed", "escalate", "abandon-and-fail"]);
        // the snapshot's open_checkpoints has len 1;
        // status, terminal_reason, terminal_at equal the snapshot taken before the promotion.
    }
}

#[tokio::test]
async fn an_ended_runs_ambiguous_promotion_records_one_reconciliation_open() {
    // ended_world_recording(Ending::Cancelled, Some(sink)) (Version2); park as above;
    // the sink gains exactly one event, of kind RequestReconciliation, with
    // reason_code Some("dispatcher-lost-after-started") and an operation id whose
    // slot is ck-open-reconcile-t{effect.turn}-s{effect.slot}-g1;
    // re-run a settle pass (fx.run().recover + settle_side_effects) and re-apply the
    // same RecordEffectResult command: the count does not change;
    // assert_decision_stream_whole(&sink, &run_scope()).
}
```

In `communal_claim_append.rs`, add `an_ended_runs_ambiguous_claim_append_opens_its_reconciliation_checkpoint`. It makes the same checkpoint assertions as the first test, over `ended_goal_world`, for `Ending::Completed`. Deliver the indeterminate outcome with a `RecordEffectResult` built as `resolve_claim_append` (`:368`) builds it.

- [ ] **Step 2: Run them and see them fail.** Run: `cargo test -p rakka-agent --test private_memory_promotion --test communal_claim_append ended_runs_ambiguous`. Expected: FAIL, because no checkpoint opens.

- [ ] **Step 3: Implement.**
- Add `holds_outside_the_turn_effect`.
- In `record_effect_result`, open the checkpoint when `parked && (live || held outside the turn)`.
- In `open_effect_checkpoint`, when the run is terminal and the kind is `IndeterminateEffectReconciliation`, drop the `compensate` option from the new checkpoint's `allowed_decisions`. A filter after construction is fine.
- Rewrite the `record_effect_result` comment to say:
  - an ended run's park of a promotion or claim append opens its checkpoint and is answerable by either path;
  - a compensation or workflow cancel parked on an ended run opens none (#123);
  - the terminal status doesn't move.

- [ ] **Step 4: Run them and see them pass.** Use the Step 2 command. Expected: PASS. Then run `cargo test -p rakka-agent --test private_memory_promotion --test communal_claim_append --test decision_events_checkpoints`. Expected: PASS.

- [ ] **Step 5: Commit.** `git commit -m "Open the reconciliation checkpoint on an ended run's ambiguous promotion or claim append (issue #120)"`

### Task 2: `ResolveIndeterminateEffect` answers an ended run (spec 5.2)

**Files:**
- Modify: `crates/rakka-agent/src/run.rs` `apply_indeterminate_resolution` (`:5399`), at:
  - the guard `:5411-5413`;
  - `winding_down` at `:5414`;
  - the `checkpoint_wait_status` write in the live `ConfirmedNotExecuted` arm.
- Test: `crates/rakka-agent/tests/private_memory_promotion.rs`, `crates/rakka-agent/tests/communal_claim_append.rs`.

**Interfaces:**
- Consumes: `holds_outside_the_turn_effect` (Task 1).
- Produces: `apply_indeterminate_resolution` admits an ended run's outside-the-turn generation. Task 3's `EffectResolution` arm reaches it unchanged.

- [ ] **Step 1: Write the failing tests** in `private_memory_promotion.rs`. Each parks as in Task 1, then applies `AgentRunEntityCommand::ResolveIndeterminateEffect { operation_id, effect_id, generation, resolution, telemetry: Default }`. Use an `AgentOperationKind::CheckpointResolution` key, as `decision_events_checkpoints.rs:320-388` builds them. Each asserts the terminal record unchanged.

  - **`confirmed_not_executed_re_sends_an_ended_runs_promotion`**
    - First, `fx.clock.store(terminal_at + AGENT_POST_TERMINAL_MEMORY_WINDOW_DEFAULT_MS + 1, ..)`, so the window has closed.
    - Apply `ConfirmedNotExecuted` and expect `Applied`. The effect's generation is 2, its status `Pending` or `Ready`, and `open_checkpoints()` is empty.
    - `fx.pump_post_terminal(AgentRunEffectKind::MemoryPromotionCall)`. The effect is `Succeeded`, and `stores.private.len(&agent_scope()) == 1`.
    - Re-apply the same command. Expect `AgentRunEntityReply::Duplicate { .. }`, with the generation still 2.
  - **`a_re_sent_promotion_after_retention_fails_definitively`**
    - Purge the run's session rows: `stores.session.purge_run(&run_scope(), &policy, terminal_at, now)`, with a `SessionRetentionPolicy` that purges everything.
    - Then `ConfirmedNotExecuted` and `pump_post_terminal`. The effect is `Failed`, with `last_error_code == Some("memory-promotion-selection-empty")`.
  - **`confirmed_completed_records_an_ended_runs_promotion_outcome`**
    - `ConfirmedExecuted` with an `AgentRunEffectOutcome::MemoryPromotion { .. }` outcome. The effect is `Succeeded`, with generation 1.
  - **`an_unaffordable_re_send_on_an_ended_run_keeps_its_checkpoint`**
    - The world's budget can't reserve the promotion's attempts again. Set the ceilings in the fixture so the first promotion's reservation exhausts them.
    - `ConfirmedNotExecuted` is refused `run-redispatch-unaffordable`. The checkpoint is still open, and the effect still `Indeterminate` at generation 1.
  - **`an_ended_run_refuses_a_resolution_it_cannot_answer`**
    - An unknown effect id answers `run-terminal`.
    - In `ended_world(Ending::Failed)`, the failed tool effect (not outside the turn) answers `run-terminal`.
    - The promotion at `generation.next()` answers `run-stale-effect-generation`.
    - After `ConfirmedCompleted` settles it, a second `ResolveIndeterminateEffect` under a new key answers `run-stale-effect-result`.
  - **`a_re_sent_generation_that_parks_again_opens_its_own_checkpoint`**
    - Version2 sink, `ConfirmedNotExecuted`. `resolve_promotion` with an indeterminate outcome against generation 2.
    - One checkpoint is open, with id `id_for_effect(.., generation 2, ..)`. The sink holds two `RequestReconciliation` events, slots ending `-g1` and `-g2`, and one `Reconcile`.
    - `assert_decision_stream_whole` holds.
  - **`a_version_one_host_records_the_ended_runs_reconcile`**
    - Use `ended_world_recording` with a `Version1` sink. Add a vocabulary parameter to the helper, or a sibling helper (builder's call).
    - After the park and `ConfirmedCompleted`, the events after the run's end are exactly `[Reconcile]`: `RequestReconciliation` isn't written under Version1.
    - `assert_bc0c224_round_trips(&events)` and `assert_decision_stream_whole` hold.

  In `communal_claim_append.rs`, add **`confirmed_not_executed_re_sends_an_ended_runs_claim_append_under_a_new_key`**. After `ConfirmedNotExecuted`, the effect's `safety.external_key()` ends with `#g2`, and the pumped append lands as `Succeeded`.

- [ ] **Step 2: Run them and see them fail.** Run: `cargo test -p rakka-agent --test private_memory_promotion --test communal_claim_append ended_run`. Expected: FAIL with `run-terminal`.

- [ ] **Step 3: Implement.** In `apply_indeterminate_resolution`:
  - the guard refuses `Terminal` iff the run is terminal and `!run.holds_outside_the_turn_effect(effect_id)`;
  - `winding_down` becomes `run.terminal_reason.is_some() && !run.status.is_terminal()`, so an ended run takes the live arm;
  - the live arm's `run.status = checkpoint_wait_status(..)` runs only when the run is not terminal.

  Update the function's doc comment for the ended-run case.

- [ ] **Step 4: Run them and see them pass.** Use the Step 2 command, then `cargo test -p rakka-agent --test effect_dispatch --test decision_events_checkpoints --test private_memory_promotion --test communal_claim_append`. Expected: PASS.

- [ ] **Step 5: Commit.** `git commit -m "Answer an ended run's ambiguous promotion or claim append through ResolveIndeterminateEffect (issue #120)"`

### Task 3: `ResolveCheckpoint` answers an ended run (spec 5.2)

**Files:**
- Modify: `crates/rakka-agent/src/run.rs` `resolve_checkpoint` (`:5921`), at:
  - the guard `:5931-5936`;
  - the `Granted` arm's `checkpoint_wait_status` write.
- Test: `crates/rakka-agent/tests/private_memory_promotion.rs`, and the inline `mod tests` in `run.rs`.

**Interfaces:**
- Consumes: `holds_outside_the_turn_effect` (Task 1), Task 2's core.

- [ ] **Step 1: Write the failing tests.** Each parks as in Task 1, then applies `AgentRunEntityCommand::ResolveCheckpoint { operation_id, checkpoint_id, resolver, decision, telemetry: Default }`. Build `decision` as `AgentCheckpointDecision::Reconciliation(..)`. Each asserts the terminal record unchanged.

  - **`a_checkpoint_decision_answers_an_ended_runs_promotion`**, over four fresh worlds:
    - **`ConfirmedNotExecuted`:** generation 2, and `pump_post_terminal` lands it `Succeeded`.
    - **`ConfirmedCompleted`** (with the promotion outcome): `Succeeded`.
    - **`AbandonAndFail`:** the effect is `Failed` with `last_error_code == Some("reconciliation-abandoned")`, and `terminal_reason` equals the one before.
    - **`Escalate`:** the checkpoint stays open with status `Escalated`, and the effect is still `Indeterminate`.
  - **`compensate_is_refused_on_an_ended_run`**
    - A `Compensate { compensation }` decision answers `run-terminal`.
    - The checkpoint is still open and waiting, and the effect still `Indeterminate`.
  - **`a_dropped_checkpoint_on_an_ended_run_answers_run_terminal`**
    - After `ResolveIndeterminateEffect` with `ConfirmedNotExecuted`, a `ResolveCheckpoint` naming the dropped checkpoint answers `run-terminal`.
    - The effect is still at generation 2.
  - **`resolve_checkpoint_refuses_an_ended_runs_checkpoint_on_another_kind`** (inline unit test in `run.rs`)
    - A terminal (`Failed`) state holding an approval checkpoint bound to a `ToolCall` effect.
    - `resolve_checkpoint` with an approve decision returns `AgentRunError::Terminal { status: Failed }`, and the state is unchanged.
  - **`an_ended_runs_checkpoint_decisions_record_their_events`**
    - Version2 sink. `Escalate` records one `EscalateCheckpoint` with reason `escalated`. Then `ConfirmedCompleted` records one `Reconcile`, with outcome `ConfirmedExecuted`, and its slot ends `-g1`.
    - Replay both commands: `Duplicate`, and no new events.
    - `assert_decision_stream_whole` holds.
  - **The gated case (spec section 9).** Check whether `promote_memory` on an ended run, with a spec carrying `with_checkpoint_required()`, opens an approval checkpoint.
    - If it does, add **`a_granted_gated_promotion_on_an_ended_run_dispatches_without_moving_the_status`**: `Approve` on that checkpoint, then `pump_post_terminal`. The effect is `Succeeded`, with the terminal record unchanged.
    - If it doesn't, add **`a_gated_promotion_on_an_ended_run_opens_no_approval`**, pinning what it does instead.

- [ ] **Step 2: Run them and see them fail.** Run: `cargo test -p rakka-agent --test private_memory_promotion checkpoint`. Expected: FAIL with `run-terminal`.

- [ ] **Step 3: Implement.** In `resolve_checkpoint`, on a terminal run, before `checkpoint.resolve(..)`:
  - look up the open checkpoint by id. If it's missing, or `!run.holds_outside_the_turn_effect(&checkpoint.bound_effect.effect_id)`, refuse `Terminal`;
  - refuse `Terminal` for a `Reconciliation(Compensate { .. })` decision.

  The `Granted` arm's status write runs only when the run is not terminal. Update the doc comment.

- [ ] **Step 4: Run them and see them pass.** Use the Step 2 command, then `cargo test -p rakka-agent --test checkpoints --test decision_events_checkpoints --test private_memory_promotion`. Expected: PASS.

- [ ] **Step 5: Commit.** `git commit -m "Answer an ended run's reconciliation checkpoint through ResolveCheckpoint (issue #120)"`

### Task 4: The park repair and the timers cover ended runs (spec 5.3, 5.4)

**Files:**
- Modify: `crates/rakka-agent/src/run.rs`:
  - `needs_park_repair` (`:800`) and its doc comment (`:795-799`);
  - `fire_checkpoint_timers` (`:6535`), at its guard `:6539-6544` and its checkpoint selection.
- Test: `crates/rakka-agent/tests/private_memory_promotion.rs`, and the inline `mod tests` in `run.rs` (the fixture near `:12750`, which builds states directly).

**Interfaces:**
- Consumes: `holds_outside_the_turn_effect` (Task 1).

- [ ] **Step 1: Write the failing tests.**
  - **`a_checkpoint_less_park_on_an_ended_run_heals_on_the_next_settle_pass`** (integration)
    - Park as in Task 1, with a Version2 sink.
    - Rewrite the record checkpoint-less, the same JSON edit as `unbind_checkpoints` in `tests/handoff_send_park.rs:372`: `open_checkpoints = []`.
    - Assert none are open. One settle pass (`recover` + `settle_side_effects`) opens exactly the checkpoint `id_for_effect(.., generation 1, IndeterminateEffectReconciliation)`, without `compensate`.
    - A second settle pass opens nothing more. The sink holds one `RequestReconciliation` for it.
    - `assert_decision_stream_whole` holds, and the terminal record is unchanged.
    - Then `ConfirmedCompleted` through `ResolveCheckpoint` settles it `Succeeded`.
  - **`a_timer_escalates_an_ended_runs_reconciliation_checkpoint`** (integration)
    - Policies with `with_checkpoint_sla(AgentCheckpointSla { due_after_ms: Some(1_000), expire_after_ms: None, escalation_target: Some("ops-oncall".to_string()) })`.
    - Park, then `fx.clock.store(<open time> + 1_001, ..)`, then apply `FireCheckpointTimers`.
    - The checkpoint's status is `Escalated`. Version2 records one `EscalateCheckpoint` with reason `sla-due`. The terminal record is unchanged.
  - **`timers_leave_an_ended_runs_other_checkpoints_alone`** (inline unit test in `run.rs`)
    - A state holding an approval checkpoint, with `expires_at`, bound to a `ToolCall` effect, with the run terminal (`Failed`).
    - `fire_checkpoint_timers(&mut state, <past expires_at>)` leaves the checkpoint waiting and the effect's status unchanged.
  - **`needs_park_repair_reads_only_outside_the_turn_effects_on_an_ended_run`** (inline unit test)
    - An ended run with a checkpoint-less `Indeterminate` promotion: `true`.
    - The same with a checkpoint-less `Indeterminate` `WorkflowCancelCall` instead: `false`.

- [ ] **Step 2: Run them and see them fail.** Run: `cargo test -p rakka-agent --test private_memory_promotion ended_run` and `cargo test -p rakka-agent --lib ended_run`. Expected: FAIL.

- [ ] **Step 3: Implement.**
  - **`needs_park_repair`:** for a terminal run, true iff an `Indeterminate` effect held outside the turn has no reconciliation checkpoint. A live run reads as today.
  - **`fire_checkpoint_timers`:** for a terminal run, act only on checkpoints whose bound effect is held outside the turn.
  - Update both doc comments.
  - `advance_loop`'s step gate and `advance_once` already route the repair. Change neither.

- [ ] **Step 4: Run them and see them pass.** Use the Step 2 commands, then `cargo test -p rakka-agent --test handoff_send_park --test private_memory_promotion`. Expected: PASS.

- [ ] **Step 5: Commit.** `git commit -m "Repair and time an ended run's reconciliation checkpoint (issue #120)"`

### Task 5: Docs and changelog (spec 8)

**Files:**
- Modify: `docs/rakka-agents.md`, `docs/rakka-compatibility.md`, `CHANGELOG.md`, and the spec's closing "As built" section (added only if the build differs).

- [ ] **Step 1: `docs/rakka-agents.md`.**
  - Where it describes the post-terminal window and reconciliation, say:
    - an ended run's ambiguous promotion or claim append parks behind a reconciliation checkpoint, without `compensate`;
    - either path answers it at any time after the run ended, and the terminal record doesn't move;
    - `ConfirmedNotExecuted` asserts the attempt never ran and re-sends under a new idempotency key, so a person who doesn't know chooses `ConfirmedCompleted` or `AbandonAndFail`.
  - Remove "An ended run's ambiguous memory promotion or claim append records no event (issue #120)."

- [ ] **Step 2: `docs/rakka-compatibility.md`.** Add a bullet after the decision-event bullet (issue #119) covering:
  - the behaviour change;
  - the four rows of spec section 6's table;
  - that a rollback to `bc0c224` is inside the window.

- [ ] **Step 3: `CHANGELOG.md`.**
  - Under Unreleased, add a Fixed entry for #120. It covers the checkpoint, both paths, the re-send contract, the refused `Compensate`, the repair, the timers, the events, and #123.
  - In the #119 entry, replace "An ended run's ambiguous memory promotion or claim append records no event until issue #120." with a pointer to that entry.

- [ ] **Step 4: Check the serialized shapes** (spec proof 10). Run: `git diff main -- crates/rakka-agent/src | grep -E '^\+\s*(pub )?([a-z_]+: |struct |enum )'`. Expected: no line that adds a field to, or declares, a serialized type. A hit in a function body or a test is fine; list any other hit in the commit message, with why it isn't serialized.

- [ ] **Step 5: Validate.** Run: `scripts/validate.sh > "$SCRATCH/validate-120.log" 2>&1; echo "rc=$?"`. Expected: `rc=0`, and `grep -E "^test result:" "$SCRATCH/validate-120.log"` shows 0 failed. `$SCRATCH` is the session scratchpad.

- [ ] **Step 6: Commit.** `git commit -m "Document the ended-run reconciliation (issue #120)"`
