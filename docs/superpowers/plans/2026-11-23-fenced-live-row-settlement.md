# Fenced live rows: witness and agent-tier settlement — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A fenced, unfinished, unheld fleet entry with a live outbox row is settled by the agent tier as well as the workflow tier (#211). A `Dispatching` row cancelled there keeps a durable witness that an attempt started, which the agent tier parks (#209).

**Architecture:**
- **The witness:** a new `rakka-workflow` inbox write counts the in-flight attempt with a fixed line and cancels the row, in one write. The workflow tier's `settle_cancelled_effects` uses it for `Dispatching` rows.
- **The reader:** in `rakka-agent`, the wind-down reader (`owed_by_finished_row` through `the_wind_down_parks`) parks a witnessed row, and a reconcileable row that counts attempts.
- **The settlement:** the agent's per-run sweep (`settle_waited_effects`) settles a fenced live row as `refresh_run` would, then tells the run.

**Tech Stack:** Rust 1.88 (MSRV), tokio tests, the `rakka-workflow`, `rakka-agent-workflow` and `rakka-agent` test fixtures.

**Spec:** `docs/superpowers/specs/2026-11-23-fenced-live-row-settlement-design.md`

## Global Constraints

- **No field, status or schema version is added.** One stable line string is added: `"cancelled while an attempt was in flight"`.
- **No error code is added.** Parks use `run-cancelled-after-attempt` (under the wind-down fence), `ticket-row-missing` (a row the run put back) and `ticket-row-settled` (not under the fence). The agent sweep cancels a row with the reason `"dispatch-cancelled"`, as `refresh_run` does.
- **The witness applies to `Dispatching` rows only.** `Pending` and `Failed` rows are cancelled as before.
- **The sweep leaves an entry under an active lease alone.**
- **Workspace lints:** every public item has a doc comment, and `cargo clippy ... -D warnings` is clean. Never add `-D warnings` to the doc build.
- **Debug-build stack depth:** the sweep is awaited inside every pass. Box any large new future, following the rule from issue #162. `examples/coordination-capability-acceptance/tests/stack_margin.rs`, which `validate.sh` runs, is the check.
- **Test runs:**
  - The target directory is cold, so the first build takes 30–50 minutes.
  - Run crate-wide with `--no-fail-fast`, using the `--all-features` feature set throughout.
  - Never pipe `scripts/validate.sh` into `tail`; redirect it to a file and capture `$?`.

## Review Focus

1. **A worker whose call outlived its lease finishes after the sweep settled its row.** Its own writes must find the row finished and conclude on it, and must not make a second outcome. Test: Task 3, `a_worker_that_finishes_after_the_sweep_settled_its_row_changes_nothing`.
2. **A fleet write lost between the sweep's cancel and its finish.** A crash, or a lost compare-and-set on `finalize_run_cancellations`, leaves a cancelled row under a fenced, unfinished entry. The next pass must settle it from the cancelled row, with the witness intact, and not write the row twice. Test: Task 3, `a_cancel_whose_finish_was_lost_is_finished_by_the_next_pass`.

I checked and found no other likely input. A late worker that turns `Dispatching` into `Failed` before the witnessed write is answered by the write itself, which touches only a `Dispatching` row. A plain cancel then follows, and the reader's reconcileable and idempotent rules cover the count.

---

### Task 1: The witness (#209, workflow side)

**Files:**
- Modify `crates/rakka-workflow/src/model.rs`: `OutboxEntry` gains `pub(crate) fn mark_cancelled_in_flight(&mut self, now: WorkflowTimestamp, line: impl Into<String>)`.
- Modify `crates/rakka-workflow/src/inbox.rs`: `DurableInbox` gains `record_outbox_cancelled_in_flight`, beside `record_outbox_cancelled` (around `:576`).
- Modify `crates/rakka-agent-workflow/src/dispatcher.rs`:
  - the constant;
  - `settle_cancelled_effects` (around `:3249`);
  - the rustdoc of `cancel_run_dispatches`, `refresh_run`, `request_run_cancellation` and `finalize_run_cancellations`.
- Modify `crates/rakka-agent-workflow/src/lib.rs`: export the constant.
- Test: `crates/rakka-workflow/tests/durable_inbox.rs` and `crates/rakka-agent-workflow/tests/dispatcher_fleet_cancellation.rs`.

**Interfaces:**
- Produces:
  - `rakka_workflow::DurableInbox::record_outbox_cancelled_in_flight(&mut self, message_id: &OutboxMessageId, line: impl Into<String>) -> WorkflowResult<Option<WorkflowTelemetryEvent>>`.
    - For a `Dispatching` row it does all of this in one persist: counts one attempt with no retry scheduled (as the attempts record's `record_exhaustion(line)` does: attempts + 1, the line as `last_error`), marks the row `Cancelled`, and answers `Some(OutboxDispatchCancelled { .. })` with the line as its message.
    - For any other row it writes nothing and answers `None`.
    - For an unknown row it answers `OutboxEntryNotFound`, as `record_outbox_cancelled` does.
  - `pub const DISPATCH_CANCELLED_IN_FLIGHT_LINE: &str = "cancelled while an attempt was in flight";`, in `rakka-agent-workflow`'s `dispatcher` module and exported from its crate root. Its doc says it is stored on rows and must not change.

- [ ] **Step 1: Write the failing tests.**
  - `durable_inbox.rs`:
    - `a_dispatching_entry_cancelled_in_flight_counts_the_attempt_with_its_line`. Schedule, `mark_outbox_dispatching`, then the new write with `"line"`. It answers `Some(..)`. The entry is `Cancelled`, `attempts() == 1` and `last_error() == Some("line")`. After `recover()`, the same holds.
    - `cancelling_in_flight_leaves_any_other_entry_alone`. For a `Pending` entry, a `Failed` one and a `Dispatched` one, the new write answers `None`, and status, attempts and line are unchanged.
    - `an_entry_cancelled_in_flight_at_its_budget_ends_cancelled`. With a retry policy of 1 attempt, a `Dispatching` entry ends `Cancelled` with `attempts() == 1`, not `Exhausted`.
  - `dispatcher_fleet_cancellation.rs`:
    - `a_dispatching_row_is_cancelled_with_the_witness`. Use `World`. Claim `effect-flight`, mark its row `Dispatching` through the run's inbox (`mark_outbox_dispatching`), and let the lease lapse. Register `effect-waiting`. Then `worker.cancel_run_dispatches(&run())`:
      - `effect-flight`'s row is `Cancelled`, `attempts == 1`, with last error `DISPATCH_CANCELLED_IN_FLIGHT_LINE`;
      - `effect-waiting`'s row is `Cancelled` with 0 attempts and no line;
      - both entries are `Cancelled`.
    - `a_refresh_settles_a_fenced_dispatching_row_with_the_witness`. The request lands while `effect-flight` is in flight (`request_run_cancellation` on the fleet). The row is marked `Dispatching`, and the lease lapses. Then `refresh_run(run(), None)`: the row carries the witness and the entry is `Cancelled`.

- [ ] **Step 2: Run them and check that they fail.** Run `cargo test -p rakka-workflow --test durable_inbox --no-fail-fast` and `cargo test -p rakka-agent-workflow --all-features --test dispatcher_fleet_cancellation --no-fail-fast`.
  - Expected: a compile error, since neither the new write nor the constant exists.
  - Record that in the ledger.

- [ ] **Step 3: Implement.**
  - The model method and the inbox write.
  - The constant and its export.
  - `settle_cancelled_effects`: a row with `status() == OutboxStatus::Dispatching` goes through `record_outbox_cancelled_in_flight(&message_id, DISPATCH_CANCELLED_IN_FLIGHT_LINE)`. Every other cancellable row keeps `record_outbox_cancelled(&message_id, reason)`.
  - The four rustdocs say that a `Dispatching` row is cancelled with the witness, and why: a lost attempt's outcome is unknown, and a reader must be able to tell it from a ticket that never started.

- [ ] **Step 4: Run the two test files, then both crates.**
  - Expected: the 5 new tests pass.
  - Then run `cargo test -p rakka-workflow --no-fail-fast` and `cargo test -p rakka-agent-workflow --all-features --no-fail-fast`. Expected: no failures. An older test that pinned `Cancelled, 0` for a `Dispatching` row is turned around, as a ledgered `Ruling:`.

- [ ] **Step 5: Run clippy on the two crates and commit.** "Cancel a Dispatching row with a witness that an attempt started (issue #209)", ending with the `Co-Authored-By` line.

---

### Task 2: The reader (#209, agent side, and the reconcileable ruling)

**Files:**
- Modify `crates/rakka-agent/src/dispatch.rs`:
  - `the_wind_down_parks` (around `:220`) and its doc;
  - its caller in the claim's fence (around `:4075`);
  - `owed_by_finished_row` (around `:566`) and its doc table.
- Create `crates/rakka-agent/tests/dispatch_fenced_live_row.rs`, with a module doc naming #211 and #209.

**Interfaces:**
- Consumes `rakka_agent_workflow::DISPATCH_CANCELLED_IN_FLIGHT_LINE` (Task 1).
- Produces `fn the_wind_down_parks(intent: &AgentRunEffect, started: u32, witnessed: bool) -> bool`. It is true when the call does not produce only its result, and either of these holds:
  - `witnessed`, and the class is not read-only;
  - `started > 0`, and the class is idempotent or reconcileable.

  The claim's fence passes `witnessed = false`. A reconcileable row with counted attempts never reaches the fence: it takes the ambiguous branch first. `owed_by_finished_row` passes `row.attempts().last_error() == Some(DISPATCH_CANCELLED_IN_FLIGHT_LINE)` for a `Cancelled` row.

- [ ] **Step 1: Write the test fixtures.**
  - `fleet_handle(fx)`: as `dispatch_missing_row.rs`'s `another_fleet_handle`.
  - `workflow_worker(fx)`: `AgentDispatcherWorker::with_clock_and_metrics` over the fixture's stores and clock.
  - `cancel_the_run(fx)`: as `dispatch_failed_attempt_recovery.rs`'s.
  - `a_lost_attempt(spec)`: start, pump to the tool ticket, `probe.arm(AgentDispatchWindow::AfterStarted)`, then one pass that dies. The row is `Dispatching, 0`.
  - The review's probe `tmp/2026-11-22-issue-206-review/zz_probe_fenced_live_row.rs` and `zz_review_206.rs` (`fleet_cancel_of_a_lost_first_attempt`) show both shapes.

- [ ] **Step 2: Write the failing tests.** Each test cancels the run, expires the lease, calls `workflow_worker(&fx).cancel_run_dispatches(&workflow_run_id(&run_scope()))`, then runs `fx.one_pass()`. Read the tool effect's `(status, attempts, last_error_code)` and the run's status.
  - `a_lost_attempt_the_workflow_tier_cancels_parks`: for `non_idempotent()` and `idempotent(3)`, `(Indeterminate, 1, Some("run-cancelled-after-attempt"))`, and the run is `WaitingForReconciliation`.
  - `a_lost_read_only_attempt_the_workflow_tier_cancels_is_cancelled_charged_one`: `read_only().with_max_attempts(3)` gives `(Cancelled, 1, _)`, and the run is `Cancelled`.
  - `a_failed_reconcileable_call_the_workflow_tier_cancels_parks`:
    - Setup: `reconcileable(3)`, with a tool whose every call fails retryably. One pass leaves the row `Failed, 1`. Then cancel the run and `cancel_run_dispatches`.
    - Expected: `(Indeterminate, 1, Some("run-cancelled-after-attempt"))`.
  - `a_lost_model_call_the_workflow_tier_cancels_is_cancelled`:
    - Setup: the run's first effect is a model call. Kill its worker after `Started`, then cancel the run and `cancel_run_dispatches`.
    - Expected: the model call is `Cancelled` with 1 attempt.
  - `a_reconcileable_row_cancelled_before_its_run_was_told_parks` (design §4, call 8):
    - Setup: `reconcileable(3)`, the first attempt failed, the run cancelled. Write the row `Cancelled` directly through the run's inbox (`record_outbox_cancelled`), without telling the run, as a worker lost between the fence's cancel and its tell leaves it.
    - Expected: a pass parks `run-cancelled-after-attempt` with 1 attempt.

- [ ] **Step 3: Run the file and check that it fails.**
  - Run `cargo test -p rakka-agent --all-features --test dispatch_fenced_live_row --no-fail-fast`.
  - Expected: the lost-attempt, reconcileable and call-8 tests fail, read as `Cancelled`, and the lost one charged 0. The model-call and read-only tests may already pass, because the witness from Task 1 counts the attempt. Record each.

- [ ] **Step 4: Implement** the widened predicate, its two callers, and the doc table rows.

- [ ] **Step 5: Run the file, then the crate.**
  - Expected: the file passes.
  - Then run `cargo test -p rakka-agent --all-features --no-fail-fast > <ledger dir>/crate-t2.log 2>&1`. Expected: no failures. A test that pinned `Cancelled` for a reconcileable or witnessed row is turned around, as a ledgered `Ruling:`.

- [ ] **Step 6: Run clippy and commit.** "Park a call whose cancelled row says an attempt may have run (issue #209)", ending with the `Co-Authored-By` line.

---

### Task 3: The agent sweep settles a fenced live row (#211)

**Files:**
- Modify `crates/rakka-agent/src/dispatch.rs`:
  - `settle_waited_effects` (around `:6560`): its cheap look and a new arm;
  - `WaitedTicket` (around `:6755`);
  - `InboxWrite` (around `:640`): a new variant, `CancelledInFlight { line: &'a str }`, applied through `record_outbox_cancelled_in_flight`, with the operation label `"cancelled-in-flight"`.
- Modify `crates/rakka-agent/tests/dispatch_fenced_live_row.rs`.

**Interfaces:**
- Consumes Task 1's write and line, and Task 2's reader.
- `WaitedTicket::of(entry: &AgentDispatchEntry, row: Option<&OutboxEntry>, now: AgentTimestampMillis) -> Self` gains two fields:
  - `awaits_settlement: bool`: `entry.awaits_cancellation_settlement(now)`;
  - `indexes_row: bool`: `row.is_none_or(|row| entry.indexes_row(row))`.

- [ ] **Step 1: Write the failing tests.**
  - **Shape A (a failed attempt):**
    - Setup: a tool executor that calls `fleet_handle(fx).request_run_cancellation(..)` and then fails retryably. This is the probe's `FencedWhileInFlight`. One pass leaves the row `Failed, 1` and the entry fenced. Then cancel the run, expire the lease, and run one pass on the agent tier only.
    - `a_failed_idempotent_call_fenced_at_the_fleet_parks_on_the_agent_tier_alone`: `(Indeterminate, 1, Some("run-cancelled-after-attempt"))`, and the entry is `Cancelled`.
    - `a_failed_read_only_call_fenced_at_the_fleet_is_cancelled_on_the_agent_tier_alone`: `(Cancelled, 1, _)`, and the run is `Cancelled`.
    - `a_failed_reconcileable_call_fenced_at_the_fleet_parks_on_the_agent_tier_alone`: as for idempotent.
  - **Shape B (a lost attempt):**
    - Setup: `a_lost_attempt(spec)`, then `request_run_cancellation` while the lease is active (`in_flight_entries == 1`). Then cancel the run, expire the lease, and run one pass.
    - `a_lost_attempt_fenced_at_the_fleet_parks_on_the_agent_tier_alone`: for `non_idempotent()` and `idempotent(3)`, `(Indeterminate, 1, Some("run-cancelled-after-attempt"))`. The row carries `DISPATCH_CANCELLED_IN_FLIGHT_LINE`.
    - `a_lost_read_only_attempt_fenced_at_the_fleet_is_cancelled_on_the_agent_tier_alone`: `(Cancelled, 1, _)`.
  - `an_entry_under_an_active_lease_is_left_to_its_worker`: shape B with the lease not expired. The pass leaves the row `Dispatching, 0`, the entry unfinished and the run `Cancelling`.
  - `a_row_the_run_put_back_under_a_fenced_entry_parks_ticket_row_missing`:
    - Setup: from `dispatch_missing_row.rs`'s #153 sibling fixtures. A ticket's row is lost, the run flushes a new `Pending` row with the scheduling stamp the entry does not index, and the entry is fenced and unheld.
    - Expected: one pass parks `(Indeterminate, _, Some("ticket-row-missing"))`, and the row is `Cancelled` with no witness.
  - `a_live_run_fenced_at_the_fleet_parks_after_a_lease`:
    - Setup: shape A, but without cancelling the run. The first pass settles and finishes the entry, and the call is not yet told.
    - Expected: after `fx.wf_clock.advance(LEASE_MS)`, a pass parks `(Indeterminate, _, Some("ticket-row-settled"))`.
  - `a_worker_that_finishes_after_the_sweep_settled_its_row_changes_nothing` (Review Focus 1):
    - Setup: a worker holds a call past its lease, and the entry is fenced. The sweep settles the row with the witness, then the worker's call succeeds and it goes on.
    - Expected: the worker's pass delivers nothing, the call stays parked `run-cancelled-after-attempt`, and the row is still `Cancelled`.
  - `a_cancel_whose_finish_was_lost_is_finished_by_the_next_pass` (Review Focus 2):
    - Setup: shape B, with the fleet's `finalize_run_cancellations` write lost once, using `AgentFleetWritePause` or a store-level conflict, as the #159/#160 tests do. The first pass ends with the row cancelled and witnessed and the entry unfinished.
    - Expected: the next pass finishes the entry and parks, and the row is written once.

- [ ] **Step 2: Run the file and check that the Task 3 tests fail.**
  - Expected: the shape A and shape B tests leave the run `Cancelling` and the call `Ready`. The put-back and live-run tests stall the same way. The active-lease test passes already, as a guard.
  - Record each.

- [ ] **Step 3: Implement.**
  - **The cheap look:** a ticket whose row is live counts as owed when its entry `awaits_cancellation_settlement(now)`. Read `now` before the look.
  - **The new arm,** placed before the existing `(Some(row), entry) if !row_is_finished(row.status())` arm. It takes `(Some(row), Some(entry))` where the row is live, `entry.awaits_settlement` holds, and `!entry.finished`:
    1. If `!entry.indexes_row`, cancel the row (`InboxWrite::Cancelled { reason: "dispatch-cancelled" }`), finish the entry, and tell the run `Indeterminate` under `TICKET_ROW_MISSING`, as the row-less fenced arm does.
    2. Otherwise, cancel with `InboxWrite::CancelledInFlight { line: DISPATCH_CANCELLED_IN_FLIGHT_LINE }` when the row is `Dispatching`, and with `InboxWrite::Cancelled { reason: "dispatch-cancelled" }` otherwise. Then finish the entry: `self.fleet.finalize_run_cancellations(&run_id, std::slice::from_ref(&effect_id))`, where `effect_id` is the entry's effect id.
    3. Then read the row again. On a winding-down run (`fenced`), tell the run as the fenced-`Cancelled` arm does (`owed_by_finished_row(&row, &intent, true)` and `tell_owed`). On a live run, leave it to the general arm on a later pass.
  - **A row already cancelled under an entry that still `awaits_settlement`** (its finish was lost) reaches the existing finished-row arms, which tell the run from it. That would leave a fenced, unfinished entry that no prune removes. So where those arms meet an entry that `awaits_settlement`, they first finish it, with the same `finalize_run_cancellations` call, and never write the row again. Review Focus 2's test holds this.

- [ ] **Step 4: Run the file, then the crate.**
  - Expected: the file passes.
  - Then run `cargo test -p rakka-agent --all-features --no-fail-fast > <ledger dir>/crate-t3.log 2>&1`. Expected: no failures. Any older sweep test that pinned "a fenced, unfinished entry is skipped" is turned around, as a ledgered `Ruling:`.

- [ ] **Step 5: Run clippy and commit.** "The agent sweep settles a fenced live row as refresh_run would (issue #211)", ending with the `Co-Authored-By` line.

---

### Task 4: Documents, breaks, validation

**Files:**
- Modify `docs/rakka-agents.md`:
  - the fleet cancellation passage (around `:337`): an agent-only deployment needs no `refresh_run` for a fenced run;
  - the wind-down fence bullets: a lost attempt under a fleet cancellation parks; a reconcileable call's failed attempt parks.
- Modify `docs/rakka-compatibility.md`: one entry for #211 and #209, after #212's. It covers what was wrong, the rule, the new line and counted attempt (durable), what a deployment sees, the crash window of call 8, and mixed builds and rollback (design §6).
- Modify `CHANGELOG.md`: a first bullet under `### Fixed`.

**Interfaces:**
- Consumes the test names of Tasks 1–3, as the assertions that hold each sentence of "what a deployment sees".

- [ ] **Step 1: Write the documents.** List, in the commit message, the test that holds each sentence of "what a deployment sees".

- [ ] **Step 2: Run the breaks one at a time.** Stage the changed source files first, and restore them from the index after each break. Run each against `--test dispatch_fenced_live_row`, plus the two Task 1 test files where the break is there. Expected: each break is caught.
  1. `settle_cancelled_effects` never uses the witnessed write.
  2. The reader passes `witnessed = false`.
  3. The reader drops the reconcileable rule.
  4. The sweep's new arm is never taken.
  5. The sweep's arm ignores `awaits_settlement` (it settles under an active lease).
  6. The sweep's arm ignores `indexes_row`.
  7. The cheap look skips the ticket.

- [ ] **Step 3: Validate.**
  - Run `scripts/validate.sh > <ledger dir>/validate.log 2>&1; echo $?`. Expected: `0`.
  - Run `scripts/package-check.sh > <ledger dir>/package.log 2>&1; echo $?`. Expected: `0`.

- [ ] **Step 4: Commit.** "Document the settlement of fenced live rows (issues #211 and #209)", listing the breaks and the validation, and ending with the `Co-Authored-By` line.
