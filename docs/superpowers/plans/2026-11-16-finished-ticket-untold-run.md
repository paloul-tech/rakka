# A Finished Ticket Whose Run Was Never Told (issue #134) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A dispatch ticket that is finished while its run still waits ends with the run told what the durable records say, and never with another invocation.

**Architecture:** A pure reader turns an `Exhausted` row's last failure line into the outcome its lost worker would have delivered. The claim path stops starting attempts on a finished row: it settles the fleet entry to follow the row, then tells the run. The per-run half of a pass sweeps the effects a run waits on and tells the run where no claim will come, which absorbs the lost-row part of the wind-down fence.

**Tech Stack:** Rust 1.88 MSRV, `rakka-agent` (the dispatcher), `rakka-agent-workflow` (fleet entry, read only), `rakka-workflow` (outbox row, read only). Tests over `AuthorityFixture` in `crates/rakka-agent/tests/common`.

**Spec:** `docs/superpowers/specs/2026-11-16-finished-ticket-untold-run-design.md`

## Global Constraints

- No change to `rakka-workflow` or `rakka-agent-workflow` sources. The guard on the `Dispatching` mark is the dispatcher's.
- The write order of the exhausting paths (`record_attempt_failure`, `retry_ambiguous`, `conclude_on_finished_row`, `settle_unknown_finding`) does not change.
- Fleet first, run second on the claim path. A settle answered `claim-fenced` tells the run nothing and does not fail the pass.
- Every `Indeterminate` goes through `tell_indeterminate`.
- An ordinary pass reads neither the fleet record nor the run's workflow record more often than it does today.
- Codes used: `ticket-row-missing`, `ticket-row-settled`, `reconciliation-exhausted`, `effect-outcome-mismatch`, `dispatcher-lost-after-started`. No new string.
- Every public item documented; `cargo clippy --workspace --all-targets --all-features -- -D warnings` clean; no doc link from a public item to a private one.
- Build runs are per crate while iterating (`cargo test -p rakka-agent --test <file>`); `cargo clean` only after the last task.
- Commit messages end with `Co-Authored-By: Claude Fable 5.2 <noreply@anthropic.com>`.

## Review Focus

1. A healthy worker between its fleet write and its delivery, while another node pumps the same run: the run must hear the worker's own outcome, reason included. Held by the one-lease wait (Task 4, `a_finished_entry_is_not_swept_before_a_lease_has_passed`).
2. A worker whose lease lapsed mid-batch reaches a finished row: it tells nothing and its pass answers `Ok` (Task 2, `a_worker_whose_claim_lapsed_tells_the_run_nothing`).
3. A row line of no known shape, an empty line, or no line (a row another build or another writer exhausted): a reconcileable effect parks, any other is told `Exhausted` under `ticket-row-settled` (Task 1 unit cases, Task 2 `a_row_whose_line_has_no_known_shape`).
4. A winding-down run with an `Exhausted` row: it is told the row's outcome, where today the wind-down branch cancels the ticket as never started (Task 2, `a_cancelling_run_is_told_what_its_exhausted_row_says`).
5. Larger futures on `pump_run`'s stack. This crate has overflowed test stacks before when an async path grew. If any `rakka-agent` test aborts with a stack overflow, `Box::pin` the sweep's call in `register_run_into` and say so in the as-built note (Task 4, step 6).

---

### Task 1: The row reader

**Files:**
- Create: `crates/rakka-agent/src/owed_outcome.rs`
- Modify: `crates/rakka-agent/src/lib.rs` (add `mod owed_outcome;`, private)
- Modify: `crates/rakka-agent/src/dispatch.rs` (use the shared constants at the three write sites)

**Interfaces:**
- Produces, all `pub(crate)` in `owed_outcome`:
  - `const WORKER_LOST_CODE: &str = "dispatcher-lost-after-started"`, `const WORKER_LOST_LINE: &str = "dispatcher lost after started"`
  - `const AMBIGUOUS_BUDGET_SPENT_MESSAGE: &str = "the retry budget was spent recovering ambiguous attempts"`
  - `const INDETERMINATE_OUTCOME_MESSAGE: &str` (moved from `dispatch.rs`, text unchanged)
  - `const TICKET_ROW_SETTLED: &str = "ticket-row-settled"`
  - `struct UnknownFinding { line: &'static str, winding_down_code: &'static str, exhausted_code: &'static str }`
  - `const RECONCILIATION_UNKNOWN: UnknownFinding` = (`"reconciliation outcome unknown"`, `"reconciliation-unknown"`, `"reconciliation-exhausted"`)
  - `const OUTCOME_MISMATCH: UnknownFinding` = (`"effect-outcome-mismatch: the reconciled outcome does not fit the effect"`, `"effect-outcome-mismatch"`, `"effect-outcome-mismatch"`)
  - `enum OwedByExhaustedRow { Exhausted { code: String, detail: String }, Indeterminate { code: &'static str } }` (`Debug, PartialEq, Eq`)
  - `fn owed_by_exhausted_row(line: Option<&str>, reconcileable: bool) -> OwedByExhaustedRow`

- [ ] **Step 1: Write the failing unit tests** in `owed_outcome.rs` under `#[cfg(test)]`, one test per row of this table (`reconcileable` both ways unless stated):

| `line` | `reconcileable` | Expected |
|---|---|---|
| `Some("tool-refused: the tool refused the call")` | either | `Exhausted { code: "tool-refused", detail: "the tool refused the call" }` |
| `Some("a: b: c")` | either | `Exhausted { code: "a", detail: "b: c" }` (split at the first `": "`) |
| `Some(WORKER_LOST_LINE)` | either | `Exhausted { code: WORKER_LOST_CODE, detail: AMBIGUOUS_BUDGET_SPENT_MESSAGE }` |
| `Some(RECONCILIATION_UNKNOWN.line)` | either | `Indeterminate { code: "reconciliation-exhausted" }` |
| `Some(OUTCOME_MISMATCH.line)` | either | `Indeterminate { code: "effect-outcome-mismatch" }` (matched before the split) |
| `Some("no colon here")`, `Some("two words: detail")`, `Some(": detail")`, `Some("")`, `None` | `true` | `Indeterminate { code: "reconciliation-exhausted" }` |
| the same five | `false` | `Exhausted { code: "ticket-row-settled", detail: <the line, or "" for None> }` |

- [ ] **Step 2: Run** `cargo test -p rakka-agent --lib owed_outcome`. Expected: does not compile (`owed_by_exhausted_row` not found).
- [ ] **Step 3: Implement** the constants and `owed_by_exhausted_row`. Order of the checks: the two finding lines, the worker-lost line, the split (prefix non-empty and without whitespace), the fallback.
- [ ] **Step 4: Use the constants where the lines are written.** In `dispatch.rs`: `WORKER_LOST` is built from `WORKER_LOST_CODE`/`WORKER_LOST_LINE`; `retry_ambiguous`'s two message literals become `AMBIGUOUS_BUDGET_SPENT_MESSAGE`; `settle_unknown_finding` takes `finding: &UnknownFinding` in place of its three `&str` parameters and its two callers pass `&RECONCILIATION_UNKNOWN` and `&OUTCOME_MISMATCH`. No behaviour changes.
- [ ] **Step 5: Run** `cargo test -p rakka-agent --lib owed_outcome` (PASS) and `cargo test -p rakka-agent --test dispatch_failure_outcomes --test dispatch_finished_row` (PASS, unchanged).
- [ ] **Step 6: Commit** `Read what an exhausted outbox row owes its run (issue #134)`.

### Task 2: The claim path never starts a finished row

**Files:**
- Modify: `crates/rakka-agent/src/dispatch.rs` (`execute_claim`, new `conclude_on_untold_row`, new `owed_by_finished_row`)
- Test: `crates/rakka-agent/tests/dispatch_owed_outcome.rs`, `crates/rakka-agent/tests/dispatch_finished_row.rs`

**Interfaces:**
- Consumes: Task 1's `owed_by_exhausted_row`, `OwedByExhaustedRow`, `TICKET_ROW_SETTLED`, `INDETERMINATE_OUTCOME_MESSAGE`.
- Produces, private to the dispatcher:
  - `fn owed_by_finished_row(row: &OutboxEntry, intent: &AgentRunEffect, fence_applies: bool) -> (AgentRunEffectOutcome, u32)`: the outcome and the attempt number. `Exhausted` row: Task 1's reader over `row.attempts().last_error()`, attempt = `row.attempts().attempts()`. `Cancelled` with `fence_applies`: `Cancelled { reason: "run-cancelled" }`. `Cancelled` otherwise, and `Dispatched`: `Indeterminate` under `ticket-row-settled` with `INDETERMINATE_OUTCOME_MESSAGE`, attempt = the larger of the run's count and the row's. Codes and details pass through `bounded_failure_code` / `bounded_failure_detail`.
  - `async fn tell_owed(&mut self, scope: &AgentRunScope, intent: &AgentRunEffect, attempt: u32, fence: u64, outcome: AgentRunEffectOutcome, pass: &mut AgentDispatchPass) -> AgentDispatchResult<()>`: an `Indeterminate` goes through `tell_indeterminate`, anything else through `deliver_outcome`.
  - `async fn conclude_on_untold_row(&mut self, scope: &AgentRunScope, claim: &AgentDispatchClaim, row: &OutboxEntry, intent: &AgentRunEffect, fence_applies: bool, pass: &mut AgentDispatchPass) -> AgentDispatchResult<ClaimConclusion>`: for `Cancelled` with `fence_applies`, today's two calls (`settle_ticket_cancelled(.., "run-cancelled", ..)` then the `Cancelled` delivery). Otherwise `self.fleet.settle_claim_from_row(claim, Some(row))`: on `Ok` count `pass.cancelled` for a `Cancelled` row and call `tell_owed` with the claim's fencing token; on `ClaimFenced` do nothing; any other error is returned. Always `Ok(ClaimConclusion::Settled)` after that.
- `execute_claim`: compute `winding_down` before the ambiguity test; after the replaced-row check, a row whose status is `Dispatched`, `Exhausted` or `Cancelled` goes to `conclude_on_untold_row` with `fence_applies = winding_down && !intent.kind().exempt_from_wind_down_fence()`.

**Test support** (in `dispatch_owed_outcome.rs`): `async fn finish_row(fx, effect, status: OutboxStatus, line: &str)` writes the row through the substrate (`AgentRunInbox::with_clock(workflow_run_id(&run_scope()), fx.workflow_store.clone(), fx.wf_clock.clone())`, then `inner_mut().record_outbox_exhausted` / `record_outbox_success` / `record_outbox_cancelled`); `fn resolved(fx) -> (AgentRunEffectStatus, Option<String>, u32, Option<AgentFailureReason>)` reads the effect at slot 1 as (status, `last_error_code`, `attempts`, `last_error_reason`). A reconcileable fixture is the one in `dispatch_failure_outcomes.rs` (`AgentEffectSpec::reconcileable(protocol, n)`, `ScriptedReconciler::new().with_finding(..)`, `fx.with_reconciler(..)`), copied.

- [ ] **Step 1: Write the failing tests.** Each crashed case is compared with an uncrashed twin built from the same spec and tool in a second fixture: `(status, code, attempts)` equal, and the crashed one's reason `None`.

| Test | Setup | Asserts |
|---|---|---|
| `dispatch_finished_row.rs`: rename the pinned test to `a_claim_on_an_exhausted_row_with_an_unresolved_intent_tells_the_run_and_invokes_nothing` | as today | `tool.calls() == 1`; effect `Exhausted`; entry `Exhausted` with `ticket-row-settled`; pass `delivered == 1`, `invoked == 0` |
| `a_failed_attempts_exhaustion_lost_before_the_fleet_write_is_told` | idempotent(3), `CountingRefusal`; fleet write 2 refused; expire lease; `claim_and_execute` | equals the uncrashed twin; one call |
| `the_same_for_a_reconcileable_effect_is_told_exhausted` | reconcileable(…, 3), a tool that refuses definitively on a first attempt, same crash | status `Exhausted`, equals the twin (decision 1b) |
| `an_ambiguous_budget_lost_before_the_fleet_write_is_told` | idempotent(1); probe kills `AfterStarted`; expire; fleet write refused during the recovery pass; expire; pass | `Exhausted` under `dispatcher-lost-after-started`, equals the twin |
| `a_reconciliation_that_established_nothing_lost_before_the_park_is_parked` | reconcileable(…, 1), reconciler `Unknown`; probe kills `AfterStarted`; expire; run store write 1 refused during the recovery pass; expire; pass | effect `Indeterminate` under `reconciliation-exhausted`; `parked_indeterminate == 1`; reconciler not queried again; tool calls 1 |
| `the_same_through_the_mismatch_arm_parks_under_its_code` | reconciler answers `Executed` with an outcome of another kind | `Indeterminate` under `effect-outcome-mismatch` |
| `a_row_whose_line_has_no_known_shape` | registered ticket; `finish_row(Exhausted, "something else")`; claim; once idempotent, once reconcileable | `Exhausted` under `ticket-row-settled`; `Indeterminate` under `reconciliation-exhausted` |
| `a_dispatched_row_nobody_delivered_is_parked` | `finish_row(Dispatched, "")` | `Indeterminate` under `ticket-row-settled`; entry `Completed`; no call |
| `a_cancelled_row_on_a_live_run_is_parked` | `finish_row(Cancelled, "x")` | `Indeterminate` under `ticket-row-settled`; entry `Cancelled` |
| `a_cancelling_run_is_told_what_its_exhausted_row_says` | `finish_row(Exhausted, "tool-refused: no")`, then cancel the run | effect `Exhausted`, not `Cancelled` |
| `a_worker_whose_claim_lapsed_tells_the_run_nothing` | two tickets of one run (the pattern of `two_registered_tool_tickets` in `dispatch_missing_row.rs`); the second in claim order has `finish_row(Exhausted, …)`; the first's tool advances the clock by `LEASE_MS + 1` and answers | pass `Ok`; the second effect still `Ready` after it; the next pass tells it |

- [ ] **Step 2: Run** `cargo test -p rakka-agent --test dispatch_owed_outcome --test dispatch_finished_row`. Expected: the new tests FAIL (a second invocation, or an effect still `Ready`); the two `pinned_` reproductions still pass.
- [ ] **Step 3: Implement** `owed_by_finished_row`, `tell_owed`, `conclude_on_untold_row` and the `execute_claim` intercept, with doc comments that say why fleet first (the spec's paragraph).
- [ ] **Step 4: Run** the same command (PASS), then `cargo test -p rakka-agent --test dispatch_missing_row --test dispatch_failure_outcomes --test effect_dispatch` (PASS).
- [ ] **Step 5: Commit** `Never start an attempt on a finished row; tell its run (issue #134)`.

### Task 3: The `Dispatching` mark does not rewrite a finished row

**Files:**
- Modify: `crates/rakka-agent/src/dispatch.rs` (`attempt_invocation`, new `mark_dispatching`, `settle_outbox`'s debug assertion)
- Test: `crates/rakka-agent/tests/dispatch_owed_outcome.rs`

**Interfaces:**
- Consumes: Task 2's `conclude_on_untold_row`.
- Produces: `async fn mark_dispatching(&self, scope: &AgentRunScope, message_id: &OutboxMessageId) -> AgentDispatchResult<Result<(), Box<OutboxEntry>>>`, `apply_outbox(.., InboxWrite::Dispatching, true)`. `attempt_invocation` calls it in place of `write_outbox(.., Dispatching)`; on `Err(row)` it returns `conclude_on_untold_row` with `fence_applies` computed from the `state` it was given.

- [ ] **Step 1: Write the failing test** `a_row_finished_between_the_claims_read_and_the_mark_is_not_invoked`: a registered idempotent ticket; `fx.workflow_store.before_next_load` with a hook that arms a second `before_next_load`, whose hook exhausts the row (`finish_row(Exhausted, "tool-refused: late")`); `claim_and_execute`. Asserts: no tool call; `pass.invoked == 0`; effect `Exhausted` under `tool-refused`; row still `Exhausted`.
- [ ] **Step 2: Run** it. Expected: FAIL, one tool call and a row left `Dispatching` or later.
- [ ] **Step 3: Implement** `mark_dispatching` and the branch.
- [ ] **Step 4: Run** `cargo test -p rakka-agent --test dispatch_owed_outcome --test dispatch_finished_row --test effect_dispatch` (PASS).
- [ ] **Step 5: Commit** `Guard the Dispatching mark against a finished row (issue #134)`.

### Task 4: The per-run sweep

**Files:**
- Modify: `crates/rakka-agent/src/dispatch.rs` (`register_run_into`; `fence_run` becomes `settle_waited_effects`)
- Test: `crates/rakka-agent/tests/dispatch_owed_outcome.rs`, `crates/rakka-agent/tests/dispatch_missing_row.rs`

**Interfaces:**
- Consumes: Task 2's `owed_by_finished_row`, `tell_owed`; `tell_indeterminate`; `attempts_counted`.
- Produces: `async fn settle_waited_effects(&mut self, scope: &AgentRunScope, state: &AgentRunState, winding_down: bool, fleet_current: bool, registered_from: &WorkflowState, pass: &mut AgentDispatchPass) -> AgentDispatchResult<()>`, called by `register_run_into` for every run that has a loop state, with the outbox state the registration last read.

Behaviour, per effect in `loop_state.ready_effects()`:

1. **The cheap look.** Find the effect's row in `registered_from`. A live row (`Pending`, `Dispatching`, `Failed`) needs nothing more unless `fleet_current` and the ticket's fleet entry is past claiming (`status.is_terminal() || cancellation_requested`); a row that is finished or absent there goes on to step 2. If no effect goes on and none has a live row under an entry past claiming, the sweep returns having read nothing.
2. **The fresh look.** Recover the run's inbox once for the sweep, and read the fleet record once unless `fleet_current`. Decide from those:

| Row | Fleet entry | Action |
|---|---|---|
| live | past claiming | `tell_indeterminate` under `ticket-row-missing` (attempt from `attempts_counted`, fence = the entry's `last_fencing_token`), then `write_outbox(.., Cancelled { reason: "ticket-row-missing" })` |
| live | otherwise | nothing |
| `Cancelled`, `winding_down`, kind not exempt | any | deliver `Cancelled { reason: "run-cancelled" }` (the fence's existing arm, unchanged) |
| finished | present, not past claiming | nothing |
| finished | past claiming, and `entry.updated_at + lease <= now` | `tell_owed(owed_by_finished_row(..))`, fence = the entry's `last_fencing_token` |
| finished | absent, and the row's `updated_at + lease <= now` | the same, fence 0 |
| absent | past claiming | `tell_indeterminate` under `ticket-row-missing` (PR #155's fence arm, now for a live run too) |
| absent | present, not past claiming | nothing |
| absent | none, `winding_down`, kind not exempt | the fence's tombstone and `Cancelled` (unchanged) |
| absent | none, otherwise | nothing |

`lease` is `self.fleet.settings().lease_duration_ms()`; `now` is `self.now()`.

- [ ] **Step 1: Write the failing tests.**

| Test | Setup | Asserts |
|---|---|---|
| rename `pinned_a_worker_lost_after_the_fleet_write_leaves_the_run_waiting` to `a_worker_lost_after_the_fleet_write_is_repaired_after_a_lease` | as the reproduction | a pump before the lease has passed delivers nothing; after `expire_lease`, a pump delivers 1 and the effect equals the uncrashed twin, reason `None`; a third pump delivers 0 |
| `a_finished_entry_is_not_swept_before_a_lease_has_passed` | the same state, clock advanced by `LEASE_MS - 1` | `register_run` delivers 0, effect `Ready` |
| `an_ambiguous_budget_lost_after_the_fleet_write_is_repaired` | idempotent(1), killed `AfterStarted`, recovery pass with run write 1 refused | after a lease: `Exhausted` under `dispatcher-lost-after-started` |
| `a_finished_row_whose_entry_was_pruned_is_repaired_from_the_row` | the first state, then `prune_fleet(AgentTimestampMillis::new(u64::MAX))` | told after a lease measured from the row |
| rename `pinned_a_live_run_whose_finished_ticket_lost_its_row_waits_for_good` to `a_live_run_whose_finished_ticket_lost_its_row_is_parked` | as the reproduction | one settle, one pump: `parked_indeterminate == 1`; effect `Indeterminate` under `ticket-row-missing`; the row the run put back is `Cancelled`; no call; a second pump delivers 0 |
| `the_same_for_an_entry_with_no_stamp` | strip `row_scheduled_at` from the fleet record first (`strip_the_row_stamps` in `dispatch_missing_row.rs`, copied) | parked |
| `a_live_run_whose_finished_ticket_lost_its_row_is_parked_before_it_settles` | the reproduction without the settle: the row is absent | parked under `ticket-row-missing` |
| `an_in_flight_ticket_is_swept_without_reading_the_fleet_record` | a ticket killed `AfterStarted` (row `Dispatching`, nothing due); `fx.fleet_store.before_next_load` sets a flag; `register_run` | the flag is unset; nothing delivered |
| `a_healthy_due_ticket_is_swept_without_another_outbox_read` | a registered, due ticket; `fx.workflow_store.before_next_load` with a hook that counts and arms itself again, so every load of the pass is counted; `register_run` | the count equals what the same test measures before step 3, written in as a literal |

- [ ] **Step 2: Run** `cargo test -p rakka-agent --test dispatch_owed_outcome`. Expected: the new and renamed tests FAIL (nothing delivered); Task 2's and 3's pass.
- [ ] **Step 3: Implement** `settle_waited_effects`, replacing `fence_run`. `register_run_into` keeps its reads in their present order, hands the sweep the inbox state it read last, and calls the sweep for a live run too.
- [ ] **Step 4: Run** `cargo test -p rakka-agent --test dispatch_owed_outcome --test dispatch_missing_row --test dispatch_finished_row --test effect_dispatch --test dispatch_failure_outcomes`. Expected: PASS. `dispatch_missing_row.rs`'s fence tests (`a_cancelling_run_that_is_pumped_is_parked_too`, `the_fence_alone_leaves_a_lost_row_to_the_claim`, the never-registered, finished-entry and fleet-fenced cases) pass unchanged.
- [ ] **Step 5: Run the crate** `cargo test -p rakka-agent --all-features` in the background, output to the scratchpad, exit code captured in a file.
- [ ] **Step 6: If a test aborts with a stack overflow**, wrap the sweep's call in `register_run_into` in `Box::pin(..)`, rerun, and record it for the as-built note.
- [ ] **Step 7: Commit** `Tell a run whose finished ticket no claim will come for (issue #134)`.

### Task 5: Falsify, document, validate

**Files:**
- Modify: `docs/rakka-compatibility.md`, `docs/rakka-agents.md`, `CHANGELOG.md`, the spec (an "As built" section 9)

- [ ] **Step 1: Falsify each guard once**, one mutation at a time against `dispatch_owed_outcome`, `dispatch_finished_row` and `dispatch_missing_row`, restoring from a copy: the claim-path intercept removed; run told before the fleet settle; `claim-fenced` returned as an error; the mark unguarded; the reader's finding-line match removed; the reader's worker-lost match removed; the split's whitespace test removed; the sweep's one-lease wait removed (entry, then row); the sweep's live-row-under-finished-entry arm removed; the sweep reading the fleet record always; `Dispatched` told `Exhausted`. Every mutation must fail a named test; a survivor gets a test.
- [ ] **Step 2: `docs/rakka-compatibility.md`**, a dated entry after the #153/#154 entry: what changed on the claim path and the per-run half; the table of what a finished row tells; `ticket-row-settled` new as an effect's indeterminate code and as an `Exhausted` code; behaviour changes a deployment can see (a winding-down run with an `Exhausted` row is told `Exhausted`; a run stuck in this state before the upgrade is told on its first pass after a lease; a rebuilt outcome carries no failure reason); mixed builds (an earlier build still re-invokes on its own claims); rollback (clean, no record changes).
- [ ] **Step 3: `docs/rakka-agents.md`** dispatcher section: one paragraph. **`CHANGELOG.md`** under `### Fixed`: one entry.
- [ ] **Step 4: The spec's section 9, "As built"**: names, anything that moved, the falsification table.
- [ ] **Step 5: Validate** under the CI toolchain, in the background, exit code to a file: `T=$(rustup run 1.100.0 rustc --print sysroot); env PATH="$T/bin:$PATH" scripts/validate.sh`, then `scripts/package-check.sh`. Expected: both exit 0. Read the log for warnings that are not the known manifest ones.
- [ ] **Step 6: Commit** `Document what a finished ticket tells its run (issue #134)`, then `cargo clean`.
