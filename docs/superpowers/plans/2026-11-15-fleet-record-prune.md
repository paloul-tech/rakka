# Fleet Record Prune and Fleet Metrics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let an application remove finished entries from the dispatcher fleet record while workers run, make that safe at any horizon in both dispatcher tiers, and hand the dispatcher's metrics recorder to its fleet (issue #145).

**Architecture:** The fleet gains an explicit `prune_terminal(before)`. Three rules make it safe: a fleet entry finishes only after its outbox row, a registration reads the fleet before the outbox, and a finished outbox row stays finished. A settle that loses its compare-and-set re-reads and re-applies under the claim's own fence.

**Tech Stack:** Rust 1.88, `tokio`, the workspace crates `rakka-agent-workflow` (fleet, workflow-tier worker) and `rakka-agent` (`AgentRunEffectDispatcher`). No new dependency.

**Spec:** `docs/superpowers/specs/2026-11-15-fleet-record-prune-design.md`. Section numbers below are the spec's. Read sections 2 and 6 before Task 1.

## Global Constraints

- **No durable field, no schema version, no new error code.** `claim-unavailable` exists and is first used here.
- **`rakka-workflow` is not changed.** The guards live in the two dispatchers.
- **Retry bound:** a lost compare-and-set is re-read and re-applied **up to 3 more times**, recognized by the stable code `revision-conflict`.
- **Stable strings, exact:** `rakka.agent_workflow.dispatcher.entries`, label `status`; counter `rakka.agent_workflow.dispatcher.fleet` with `operation=prune` / `outcome=pruned` / `detail=none`, and `operation=dispatch` / `outcome=skipped` / `detail=ticket-row-settled` or `ticket-row-missing`; entry codes `ticket-row-settled`, `ticket-row-missing`, `cancellation-requested`; settle reasons `run-cancelled`, `dispatch-cancelled`.
- **No metric label carries a run, effect, tenant or worker id.**
- **Issue #134 stays as it is:** the `Dispatching` write in `AgentRunEffectDispatcher` is not guarded.
- Every public item has a doc comment. `cargo clippy --workspace --all-targets --all-features -- -D warnings` is clean.
- Before any cargo command: `export PATH="$HOME/.rustup/toolchains/stable-aarch64-apple-darwin/bin:$PATH"`. Run `cargo clean` at every task boundary.
- `rakka-agent` test targets run with `--all-features`.
- Commit messages name `(issue #145)` and end with `Co-Authored-By: Claude Fable 5.2 <noreply@anthropic.com>`.
- Where a test names a **guard**, remove the guard after the test passes, confirm the test fails, and restore it. Before falsifying an uncommitted file, copy it to the scratchpad and restore from the copy.

## Review Focus

Inputs the spec implies and its proofs do not name. Each has a test in the task that owns the code.

1. **A prune on a store that holds no fleet record yet.** Expected: `pruned: 0`, `remaining: 0`, and no row is created. (Task 2)
2. **Two prunes racing from two handles.** Expected: both return `Ok`, and together they remove exactly the eligible entries. (Task 2)
3. **A record with 10,000 finished entries.** Expected: one call removes them in one write. (Task 2)
4. **A cancellation request for a run with no entries.** Expected: an empty result and no write. (Task 6)
5. **A row-matching settle on a claim whose lease has lapsed.** Expected: `claim-fenced`, nothing written. (Task 4)

## File Structure

| File | Responsibility |
|---|---|
| `crates/rakka-agent-workflow/src/dispatcher.rs` | The fleet's settle re-read, prune, gauge, `settle_claim_from_row`, cancellation methods; the worker's `dispatch_claim`, `run_once`, `cancel_run_dispatches`, `refresh_run` |
| `crates/rakka-agent-workflow/src/metrics.rs`, `src/lib.rs` | The metric catalogue entry and the exports |
| `crates/rakka-agent-workflow/tests/fleet_support/mod.rs` (new) | Test stores: one that injects a foreign write before chosen compare-and-sets, one that runs a one-shot hook before a load |
| `crates/rakka-agent-workflow/tests/dispatcher_fleet_settle.rs` (new) | Proofs 6 to 10 for the fleet |
| `crates/rakka-agent-workflow/tests/dispatcher_fleet_prune.rs` (new) | Proofs 1 to 5, 9, 18 and Review Focus 1 to 3 |
| `crates/rakka-agent-workflow/tests/dispatcher_fleet_rows.rs` (new) | Proofs 21 to 24 and Review Focus 5 |
| `crates/rakka-agent-workflow/tests/dispatcher_fleet_cancellation.rs` (new) | Proofs 12 to 15, 17 and Review Focus 4 |
| `crates/rakka-agent/src/dispatch.rs` | Registration read order, the finished-row guard in `write_outbox`, `prune_fleet`, `with_metrics` |
| `crates/rakka-agent/src/testkit.rs` | A one-shot load hook on `CrashingStateStore` |
| `crates/rakka-agent/tests/dispatch_fleet_prune.rs` (new) | Proofs 6, 11, 16, 18, 26 for `AgentRunEffectDispatcher` |
| `crates/rakka-agent/tests/dispatch_finished_row.rs` (new) | Proofs 19, 20, 25 |
| `examples/multi-agent-goal-acceptance/tests/fleet_prune.rs` (new) | Proof 30 |

---

### Task 1: The settle re-reads when it loses its write

**Files:**
- Modify: `crates/rakka-agent-workflow/src/dispatcher.rs` (`update_current_claim` at `:1696`, `is_current_claim` at `:1686`)
- Create: `crates/rakka-agent-workflow/tests/fleet_support/mod.rs`
- Test: `crates/rakka-agent-workflow/tests/dispatcher_fleet_settle.rs`

**Interfaces:**
- Produces (test support): `fleet_support::ContendedStore<S>`, a `DurableStateStore<S>` over `InMemoryDurableStateStore<S>` with `fn contend(&self, times: usize, write: impl Fn(&mut S) + Send + Sync + 'static)`: before each of the next `times` compare-and-sets it applies `write` to the stored state as a foreign write, so that compare-and-set loses.
- Produces: `const FLEET_WRITE_REAPPLICATIONS: u32 = 3;` (private), used by Tasks 1 and 2.
- Produces: `update_current_claim` takes `update: impl Fn(AgentDispatchEntry, AgentTimestampMillis) -> AgentDispatchEntry`.

- [ ] **Step 1: Write `fleet_support::ContendedStore` and the failing tests**

```rust
// dispatcher_fleet_settle.rs
#[tokio::test] async fn a_settle_survives_another_workers_claim()
// two entries E1, E2; handle A claims E1; handle B claims E2; A.complete_claim(E1)
// assert: Ok, E1 is Completed, E2 is still Claimed by B

#[tokio::test] async fn a_settle_whose_claim_was_taken_over_is_fenced_after_the_re_read()
// A claims E1; clock passes the lease; ContendedStore applies, as the foreign
// write, B's re-claim of E1 (token 2); A.complete_claim(E1)
// assert: Err code "claim-fenced"; E1 is Claimed with fencing token 2

#[tokio::test] async fn a_settle_that_loses_more_than_the_bound_returns_the_conflict()
// store.contend(4, bump updated_at); A.complete_claim(E1)
// assert: Err code "revision-conflict"; E1 still Claimed

#[tokio::test] async fn a_settle_that_loses_three_times_succeeds_on_the_fourth_write()
// store.contend(3, ..); assert Ok and Completed

#[tokio::test] async fn a_failure_other_than_a_lost_write_is_not_retried()
// a store whose compare_and_set fails with a store error once; assert that error, one attempt
```

- [ ] **Step 2: Run them**

Run: `cargo test -p rakka-agent-workflow --test dispatcher_fleet_settle`
Expected: `a_settle_survives_another_workers_claim` FAILS with `revision-conflict`; the fenced-after-re-read and three-losses tests fail the same way.

- [ ] **Step 3: Implement the re-read in `update_current_claim` (spec 6.5)**

Loop: take the handle's record (read if none) → find the entry (absent: `ClaimFenced`) → `is_current_claim` (not current: `ClaimFenced`) → apply and `persist` → on an error whose `code()` is `revision-conflict`, `recover()` and repeat, at most `FLEET_WRITE_REAPPLICATIONS` more times. `is_current_claim` on the fleet answers `Ok(false)` for an absent entry (spec 6.8).

- [ ] **Step 4: Run the file, then the crate**

Run: `cargo test -p rakka-agent-workflow --test dispatcher_fleet_settle && cargo test -p rakka-agent-workflow`
Expected: PASS. **Guard:** make the loop run once; `a_settle_survives_another_workers_claim` must fail.

- [ ] **Step 5: Commit**

```bash
git add crates/rakka-agent-workflow && git commit -m "Re-read the fleet record when a settle loses its write (issue #145)"
```

---

### Task 2: The prune

**Files:**
- Modify: `crates/rakka-agent-workflow/src/dispatcher.rs`, `crates/rakka-agent-workflow/src/lib.rs`
- Test: `crates/rakka-agent-workflow/tests/dispatcher_fleet_prune.rs`

**Interfaces:**
- Consumes: `FLEET_WRITE_REAPPLICATIONS`, `fleet_support::ContendedStore` (Task 1).
- Produces:

```rust
pub struct AgentDispatcherPrune { pub pruned: usize, pub remaining: usize }
impl AgentDispatcherFleet<Store, Clock> {
    pub async fn prune_terminal(&mut self, before: AgentTimestampMillis)
        -> AgentDispatcherResult<AgentDispatcherPrune>;
}
```

- [ ] **Step 1: Pin the record's bytes, on the code as it stands**

```rust
#[test] fn a_one_entry_record_keeps_the_bytes_it_had()
// build AgentDispatcherFleetState with one Completed entry through the public
// API (register, claim, complete under a ManualWorkflowClock), serialize with
// serde_json, and assert it equals a string literal captured from this run
```

Run it once to capture the literal, paste it, run again. Expected: PASS before any other change in this task. It must still pass at the end of Task 9.

- [ ] **Step 2: Write the failing tests**

```rust
#[tokio::test] async fn a_prune_removes_finished_entries_older_than_the_horizon()
// Completed at t=100, Exhausted at t=100, Cancelled at t=100, Completed at t=200
// prune_terminal(200): pruned == 3, remaining == 1; the t=200 entry is kept

#[tokio::test] async fn a_prune_keeps_every_live_entry_at_a_future_horizon()
// Pending, RetryScheduled, Claimed (live lease), Claimed (lapsed lease),
// cancellation_requested and unfinished; prune_terminal(u64::MAX): pruned == 0

#[tokio::test] async fn a_prune_with_nothing_eligible_writes_nothing()
// revision before == revision after

#[tokio::test] async fn a_prune_on_a_store_with_no_record_creates_none()      // Review Focus 1
// Ok(pruned 0, remaining 0); store.load(..) is None

#[tokio::test] async fn a_prune_retries_a_lost_write_and_succeeds()
// store.contend(3, ..): Ok, entries removed
#[tokio::test] async fn a_prune_that_loses_more_than_the_bound_returns_the_conflict()
// store.contend(4, ..): Err code "revision-conflict"

#[tokio::test] async fn two_prunes_from_two_handles_remove_the_eligible_entries_once()  // Review Focus 2
// both Ok; pruned sums to the eligible count; remaining agrees on a fresh read

#[tokio::test] async fn a_prune_removes_ten_thousand_entries_in_one_write()   // Review Focus 3
// revision moves by exactly 1

#[tokio::test] async fn a_lapsed_claim_on_a_pruned_entry_is_fenced()          // proof 9
// A claims E; lease lapses; B re-claims, completes; prune removes E;
// A.complete_claim(E): Err code "claim-fenced"; A.is_current_claim(E) == Ok(false)

#[tokio::test] async fn a_prune_from_another_handle_does_not_cost_a_claim_its_settle()  // proof 6, fleet level
// A claims E1; handle B prunes one older finished entry; A.complete_claim(E1)
// assert: Ok; E1 is Completed. Guard: Task 1's re-read

#[tokio::test] async fn a_prune_counts_what_it_removed()
// counter rakka.agent_workflow.dispatcher.fleet {operation=prune,outcome=pruned,detail=none} == 3

#[tokio::test] async fn registering_a_run_again_after_a_prune_inserts_nothing()  // proof 18, workflow tier
// dispatch to completion with AgentDispatcherWorker; prune; refresh_run:
// registered_effects == 0 and the entry is absent
```

- [ ] **Step 3: Run them**

Run: `cargo test -p rakka-agent-workflow --test dispatcher_fleet_prune`
Expected: FAIL to compile, `prune_terminal` not found.

- [ ] **Step 4: Implement `prune_terminal` (spec 6.2) and export `AgentDispatcherPrune` from `lib.rs`, in both export lists**

Each attempt calls `recover()`, never the cached record. Eligible is `status.is_terminal() && updated_at < before`. No eligible entry returns without a write. The state's `updated_at` comes from the fleet's clock. After a write: the counter, then `record_gauges`.

- [ ] **Step 5: Run the file, then the crate**

Run: `cargo test -p rakka-agent-workflow --test dispatcher_fleet_prune && cargo test -p rakka-agent-workflow`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/rakka-agent-workflow && git commit -m "Prune finished entries from the dispatcher fleet record (issue #145)"
```

---

### Task 3: The entries gauge

**Files:**
- Modify: `crates/rakka-agent-workflow/src/dispatcher.rs` (`record_gauges` at `:1774`), `src/metrics.rs`, `src/lib.rs`
- Test: `crates/rakka-agent-workflow/tests/workflow_metrics.rs`, `tests/load_backpressure_cardinality.rs`

**Interfaces:**
- Produces: `pub const METRIC_AGENT_DISPATCHER_ENTRIES: &str = "rakka.agent_workflow.dispatcher.entries";`

- [ ] **Step 1: Write the failing tests**

```rust
// workflow_metrics.rs
#[tokio::test] async fn the_entries_gauge_reports_every_status_zeros_included()
// one Pending, one Claimed, two Completed: gauge samples for all six
// AgentDispatchStatus labels; completed == 2.0, exhausted == 0.0
#[tokio::test] async fn the_entries_gauge_falls_after_a_prune()
// completed == 2.0, prune_terminal(future), completed == 0.0
#[test] fn the_entries_gauge_is_in_the_metric_catalogue()
// the catalogue in metrics.rs lists METRIC_AGENT_DISPATCHER_ENTRIES with the one label "status"

// load_backpressure_cardinality.rs: extend the existing fleet cardinality
// assertion to the new series: label keys == ["status"], at most 6 label sets
```

- [ ] **Step 2: Run them**

Run: `cargo test -p rakka-agent-workflow --test workflow_metrics --test load_backpressure_cardinality`
Expected: FAIL to compile, constant not found.

- [ ] **Step 3: Implement**

`record_gauges` records one sample per `AgentDispatchStatus` with the label `status` from `as_label`, zero for an absent status. Add the catalogue entry beside the in-flight and backlog gauges in `metrics.rs`, and export the constant.

- [ ] **Step 4: Run** the same command. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/rakka-agent-workflow && git commit -m "Report the fleet record's entries by status (issue #145)"
```

---

### Task 4: `settle_claim_from_row`

**Files:**
- Modify: `crates/rakka-agent-workflow/src/dispatcher.rs`
- Test: `crates/rakka-agent-workflow/tests/dispatcher_fleet_rows.rs`

**Interfaces:**
- Consumes: the re-reading `update_current_claim` (Task 1).
- Produces:

```rust
impl AgentDispatcherFleet<Store, Clock> {
    pub async fn settle_claim_from_row(
        &mut self,
        claim: &AgentDispatchClaim,
        row: Option<&OutboxEntry>,
    ) -> AgentDispatcherResult<AgentDispatchEntry>;
}
```

- [ ] **Step 1: Write the failing tests**

```rust
#[tokio::test] async fn a_dispatched_row_completes_the_entry()
// status Completed; last_error_code unchanged
#[tokio::test] async fn an_exhausted_row_exhausts_the_entry()
// status Exhausted; attempts == the row's attempts; last_error_code == "ticket-row-settled"
#[tokio::test] async fn a_cancelled_row_cancels_the_entry()
// status Cancelled; last_error_code == "ticket-row-settled"
#[tokio::test] async fn an_absent_row_cancels_the_entry()
// status Cancelled; last_error_code == "ticket-row-missing"
#[tokio::test] async fn an_unfinished_row_is_refused()
// for Pending, Dispatching, Failed: Err code "invalid-dispatch-entry"; entry unchanged
#[tokio::test] async fn a_lapsed_claim_is_fenced_and_writes_nothing()      // Review Focus 5
// Err code "claim-fenced"; revision unchanged
```

- [ ] **Step 2: Run.** `cargo test -p rakka-agent-workflow --test dispatcher_fleet_rows`. Expected: FAIL to compile.

- [ ] **Step 3: Implement (spec 6.6 table).** It goes through `update_current_claim`. The unfinished-row refusal is `InvalidEntry { field: "row", reason: "not-finished" }`.

- [ ] **Step 4: Run.** Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/rakka-agent-workflow && git commit -m "Settle a fleet claim from a finished outbox row (issue #145)"
```

---

### Task 5: The workflow tier never dispatches a finished row

**Files:**
- Modify: `crates/rakka-agent-workflow/src/dispatcher.rs` (`dispatch_claim` at `:2268`, `run_once` at `:2355`)
- Test: `crates/rakka-agent-workflow/tests/dispatcher_fleet_rows.rs`

**Interfaces:**
- Consumes: `settle_claim_from_row` (Task 4).
- Produces: `dispatch_claim` answers `AgentDispatcherError::ClaimUnavailable` for a finished or absent row; `run_once` skips such a claim.

- [ ] **Step 1: Write the failing tests**

```rust
#[tokio::test] async fn a_claim_on_a_row_already_dispatched_dispatches_nothing()   // proof 22
// worker dispatches E; the fleet write after the outbox success is made to
// fail (ContendedStore, 4 losses); lease lapses; a second worker claims E and
// calls dispatch_claim
// assert: Err code "claim-unavailable"; RecordingDispatcher saw E once;
// the entry is Completed; the row is still Dispatched

#[tokio::test] async fn a_claim_on_an_absent_row_settles_the_entry()               // proof 23
// Err code "claim-unavailable"; entry Cancelled, last_error_code "ticket-row-missing"

#[tokio::test] async fn a_cycle_goes_on_past_a_claim_that_needed_no_dispatch()     // proof 24
// two due claims, the first on a finished row: run_once is Ok, completions.len() == 1

#[tokio::test] async fn a_skipped_dispatch_is_counted()                            // proof 28
// counter {operation=dispatch,outcome=skipped,detail=ticket-row-settled} == 1
```

- [ ] **Step 2: Run.** `cargo test -p rakka-agent-workflow --test dispatcher_fleet_rows`. Expected: the first test FAILS with the dispatcher having seen E twice.

- [ ] **Step 3: Implement (spec 6.7).** Read the row after `inbox.recover()` and before `mark_outbox_dispatching`. The counter uses the fleet's three-label form (`operation`, `outcome`, `detail`).

- [ ] **Step 4: Run the file, then the crate.** Expected: PASS. **Guard:** remove the row check; the first test must fail.

- [ ] **Step 5: Commit**

```bash
git add crates/rakka-agent-workflow && git commit -m "Never dispatch a finished outbox row from the workflow-tier worker (issue #145)"
```

---

### Task 6: Workflow-tier cancellation is fence, settle, finish

**Files:**
- Modify: `crates/rakka-agent-workflow/src/dispatcher.rs` (`:1527-1625`, `:2170-2260`), `src/lib.rs`
- Modify: `crates/rakka-agent-workflow/tests/dispatcher_fleet.rs` (callers of the removed fleet method)
- Test: `crates/rakka-agent-workflow/tests/dispatcher_fleet_cancellation.rs`

**Interfaces:**
- Consumes: `fleet_support` (Task 1), `prune_terminal` (Task 2).
- Produces (test support): `fleet_support::HookedStore<S>` with `fn before_next_load(&self, hook)`: a one-shot async hook that runs before the next `load` returns.
- Produces:

```rust
pub struct AgentDispatcherCancellationRequest {
    pub run_id: AgentRunId,
    pub requested_at: AgentTimestampMillis,
    pub awaiting_settlement: Vec<AgentEffectId>,
    pub in_flight_entries: usize,
    pub already_terminal_entries: usize,
}
impl AgentDispatcherFleet<Store, Clock> {
    pub async fn request_run_cancellation(&mut self, run_id: &AgentRunId)
        -> AgentDispatcherResult<AgentDispatcherCancellationRequest>;
    pub async fn finalize_run_cancellations(&mut self, run_id: &AgentRunId, settled: &[AgentEffectId])
        -> AgentDispatcherResult<Vec<AgentEffectId>>;
}
impl AgentDispatcherFleetState {
    pub fn cancellations_awaiting_settlement(&self, run_id: &AgentRunId, now: AgentTimestampMillis)
        -> Vec<AgentEffectId>;
}
```

- Removed: `AgentDispatcherFleet::cancel_run_dispatches`. `AgentDispatcherWorker::cancel_run_dispatches` keeps its signature and `AgentDispatcherCancellation`.

- [ ] **Step 1: Write the failing tests**

```rust
#[tokio::test] async fn a_request_fences_and_finishes_nothing()
// one unclaimed, one in flight, one Completed
// awaiting_settlement == [unclaimed]; in_flight_entries == 1; already_terminal_entries == 1
// both unfinished entries have cancellation_requested; neither is terminal;
// the in-flight one has last_error_code "cancellation-requested", the other keeps its code

#[tokio::test] async fn a_request_for_a_run_with_no_entries_writes_nothing()      // Review Focus 4
// empty lists, zero counts, revision unchanged

#[tokio::test] async fn a_crash_after_the_fence_leaves_nothing_a_prune_can_remove()  // proof 12
// request_run_cancellation, then "crash" (no settle); prune_terminal(future): pruned == 0
// claim_due claims nothing; refresh_run: row Cancelled, entry Cancelled;
// RecordingDispatcher saw nothing

#[tokio::test] async fn a_crash_after_the_settle_is_finished_by_the_next_refresh()   // proof 13
// fence, settle the row by hand, no finalize; refresh_run finishes the entry

#[tokio::test] async fn a_lease_that_lapses_between_the_two_writes_is_not_finalized() // proof 14
// E in flight at the request; lease lapses; finalize_run_cancellations(run, &[]) and
// finalize with the unclaimed ids: E stays unfinished

#[tokio::test] async fn an_entry_an_earlier_binary_cancelled_first_is_still_settled()  // proof 15
// build the state by hand: entry Cancelled, row Pending; refresh_run: row Cancelled

#[tokio::test] async fn a_fresh_handles_refresh_reads_the_fleet_before_the_inbox()     // proof 17
// HookedStore on the fleet store: before the fresh handle's first fleet load, another
// worker claims, dispatches and completes E, then prunes it
// assert: registered_effects == 0, the entry is absent, the dispatcher saw E once

#[tokio::test] async fn the_workers_cancellation_reports_what_it_finished()
// cancelled_entries == 1, cancelled_effect_ids == [unclaimed], in_flight_entries == 1,
// row status Cancelled, entry Cancelled
```

- [ ] **Step 2: Run.** `cargo test -p rakka-agent-workflow --test dispatcher_fleet_cancellation`. Expected: FAIL to compile.

- [ ] **Step 3: Implement the fleet's methods (spec 6.3).** `finalize_run_cancellations` finishes an entry only when it is in `settled`, belongs to the run, is fenced, is unfinished and has no active lease.

- [ ] **Step 4: Rewrite `AgentDispatcherWorker::cancel_run_dispatches` and `refresh_run` in the spec's step order**, including step 1 of `refresh_run` (read the fleet when the handle holds no record) and step 6 (the repair for records an earlier binary wrote).

- [ ] **Step 5: Update `dispatcher_fleet.rs`** where a test called the removed fleet method or read an unclaimed entry as `Cancelled` straight after a fleet-level call.

- [ ] **Step 6: Run the crate.** `cargo test -p rakka-agent-workflow`. Expected: PASS. **Guards:** restore the fleet-first order and proof 12's test must fail; move the fleet read after the inbox read and proof 17's test must fail.

- [ ] **Step 7: Commit**

```bash
git add crates/rakka-agent-workflow && git commit -m "Cancel dispatches in the order fence, settle, finish (issue #145)"
```

---

### Task 7: The agent tier registers from a due list newer than its fleet read

**Files:**
- Modify: `crates/rakka-agent/src/dispatch.rs` (`register_run_into` at `:2666`), `crates/rakka-agent/src/testkit.rs` (`CrashingStateStore`)
- Test: `crates/rakka-agent/tests/dispatch_fleet_prune.rs`

**Interfaces:**
- Consumes: `prune_terminal` (Task 2).
- Produces (testkit, public): `CrashingStateStore::before_next_load(&self, hook: Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>)`: one-shot, taken before it runs, so loads made inside the hook do not fire it again.

- [ ] **Step 1: Write the failing tests**

```rust
#[tokio::test] async fn a_registration_never_inserts_an_effect_finished_before_its_fleet_read()  // proof 16
// AuthorityFixture with a tool ticket ready; worker A = fx.worker("worker-a", ..);
// fx.fleet_store.before_next_load(hook) where the hook runs worker B's pump_run to
// completion and then prunes at a future horizon; A.register_run(&run_scope())
// assert: pass.registered == 0; the fleet entry is absent; fx.tools invoked the tool once

#[tokio::test] async fn pumping_a_finished_run_after_a_prune_registers_nothing()               // proof 18
// run to completion; prune; pump_run: registered == 0, claimed == 0
```

- [ ] **Step 2: Run.** `cargo test -p rakka-agent --all-features --test dispatch_fleet_prune`. Expected: FAIL to compile (`before_next_load`), then proof 16's test FAILS with `registered == 1`.

- [ ] **Step 3: Add `before_next_load` to `CrashingStateStore`**, documented.

- [ ] **Step 4: Implement the second outbox read in `register_run_into` (spec 6.4).** Keep the first read as the gate for reading the fleet. Register what the second read finds due; register nothing when it finds none.

- [ ] **Step 5: Run the file, then `cargo test -p rakka-agent --all-features --test dispatch_pass_split --test effect_dispatch`.** Expected: PASS. **Guard:** register the first read's list; proof 16's test must fail with an inserted entry.

- [ ] **Step 6: Commit**

```bash
git add crates/rakka-agent && git commit -m "Register a run from a due list read after the fleet record (issue #145)"
```

---

### Task 8: A settle never rewrites a finished row

**Files:**
- Modify: `crates/rakka-agent/src/dispatch.rs` (`write_outbox` at `:2521`; the settle sites at `:3277`, `:3482`, `:3609`, `:3696`, `:3972`)
- Test: `crates/rakka-agent/tests/dispatch_finished_row.rs`

**Interfaces:**
- Consumes: `settle_claim_from_row` (Task 4), `before_next_load` (Task 7).
- Produces: nothing public. `write_outbox` reports "row already finished" with the row's status for `Succeeded`, `Failed` and `Exhausted`; one private step handles it at every site (spec 6.6).

- [ ] **Step 1: Write the failing tests**

```rust
#[tokio::test] async fn a_late_failure_does_not_reopen_a_row_another_worker_finished()   // proof 19
// a tool executor (fx.with_tool_executor) whose first call, made by worker A, advances
// the clock past LEASE_MS, drives worker B's claim_and_execute to completion, and then
// returns a retryable error to A
// assert: the row is Dispatched; A's pass is Ok; A's pass.failed_attempts == 0;
// the run's effect holds B's outcome; after a prune at a future horizon,
// pump_run registers nothing

#[tokio::test] async fn a_late_definitive_failure_does_not_reopen_it_either()             // proof 20
// the same, A's call returning a definitive error: the row is Dispatched, no
// Exhausted outcome is delivered

#[tokio::test] async fn a_late_success_leaves_the_row_and_the_fleet_as_they_are()          // proof 20
// the same, A's call succeeding late: the row is Dispatched, the entry is Completed,
// the tool was invoked twice (the lease's own ambiguity), the run holds one outcome

#[tokio::test] async fn every_finishing_site_writes_the_outbox_before_the_fleet()          // proof 11
// for each of: success, definitive failure, a reconciled outcome, a spent retry budget,
// a stale ticket: arm fx.fleet_store.crash_at(next write, BeforeWrite) and assert the
// outbox row is already finished or absent when the fleet write is refused

#[tokio::test] async fn a_claim_on_an_exhausted_row_with_an_unresolved_intent_still_starts_an_attempt()  // proof 25
// issue #134's state, built by a crash after the exhausting outbox write:
// the next claim invokes the tool. Pins the non-goal; the doc comment names #134.
```

- [ ] **Step 2: Run.** `cargo test -p rakka-agent --all-features --test dispatch_finished_row`. Expected: proof 19's test FAILS with the row `Failed`; proof 11 and proof 25 PASS already (they pin what holds today; proof 11 must fail if a site is reordered).

- [ ] **Step 3: Implement the row check in `write_outbox`,** in the same inbox read as the write, for `Succeeded`, `Failed` and `Exhausted` only.

- [ ] **Step 4: Implement the shared step at each settle site:** `settle_claim_from_row` with the row, a `claim-fenced` answer swallowed, no further delivery, `ClaimConclusion::Settled`.

- [ ] **Step 5: Run the file, then `cargo test -p rakka-agent --all-features --test dispatch_inbox_write_retry --test dispatch_failure_outcomes --test effect_dispatch`.** Expected: PASS. **Guard:** remove the row check; proof 19's test must fail.

- [ ] **Step 6: Commit**

```bash
git add crates/rakka-agent && git commit -m "Never rewrite a finished outbox row from a settle (issue #145)"
```

---

### Task 9: `prune_fleet`, and the dispatcher's recorder reaches its fleet

**Files:**
- Modify: `crates/rakka-agent/src/dispatch.rs` (`new` at `:2186`, `rebuild_fleet` at `:2289`, `with_metrics` at `:2461`), `crates/rakka-agent/src/lib.rs`
- Test: `crates/rakka-agent/tests/dispatch_fleet_prune.rs`, `crates/rakka-agent/tests/agent_metrics.rs`

**Interfaces:**
- Consumes: `prune_terminal`, `AgentDispatcherPrune` (Task 2), `METRIC_AGENT_DISPATCHER_ENTRIES` (Task 3).
- Produces: `AgentRunEffectDispatcher::prune_fleet(&mut self, before: AgentTimestampMillis) -> AgentDispatchResult<AgentDispatcherPrune>`.

- [ ] **Step 1: Write the failing tests**

```rust
// dispatch_fleet_prune.rs
#[tokio::test] async fn a_prune_from_another_dispatcher_does_not_cost_a_claim_its_settle()   // proof 6
// worker A's tool executor, mid-call, has worker B call prune_fleet(future) with one
// older finished entry present
// assert: A's pass is Ok; A's entry is Completed (not Cancelled); the tool ran once
// Guard: Task 1's re-read (set FLEET_WRITE_REAPPLICATIONS to 0): the entry ends Cancelled

#[tokio::test] async fn prune_fleet_reports_what_it_removed()
// two finished runs' entries: pruned == 2, remaining == 0

// agent_metrics.rs
#[tokio::test] async fn a_dispatcher_with_metrics_records_its_fleets_series()                  // proof 26
// pipeline.with_metrics(recorder): after one pump the recorder holds
// rakka.agent_workflow.dispatcher.fleet, .in_flight, .backlog and .entries
#[tokio::test] async fn the_fleets_recorder_survives_every_builder_order()
// with_metrics then with_fleet_settings then with_execution_classes, and the reverse:
// both record .entries
#[tokio::test] async fn a_dispatcher_without_metrics_records_no_fleet_series()
```

- [ ] **Step 2: Run.** `cargo test -p rakka-agent --all-features --test dispatch_fleet_prune --test agent_metrics`. Expected: FAIL to compile (`prune_fleet`).

- [ ] **Step 3: Implement.** `rebuild_fleet` uses the dispatcher's stored recorder; `with_metrics` stores it and calls `rebuild_fleet`. Update `with_metrics`'s doc comment: it now also records the fleet's series. Re-export `AgentDispatcherPrune` where `rakka-agent` re-exports fleet types, if it does.

- [ ] **Step 4: Run the same command, then `cargo test -p rakka-agent --all-features --test collector_allowlist --test metric_catalogue`.** Expected: PASS; if the allowlist test holds the fleet's series as data, add the new gauge there.

- [ ] **Step 5: Commit**

```bash
git add crates/rakka-agent && git commit -m "Prune the fleet from the dispatcher and wire its metrics (issue #145)"
```

---

### Task 10: End to end, two dispatchers over one fleet record

**Files:**
- Create: `examples/multi-agent-goal-acceptance/tests/fleet_prune.rs`
- Modify: `examples/multi-agent-goal-acceptance/src/wiring.rs` only if a second dispatcher over the world's stores needs a constructor; `examples/multi-agent-goal-acceptance/README.md`

**Interfaces:**
- Consumes: `prune_fleet` (Task 9).

- [ ] **Step 1: Write the test**

```rust
#[tokio::test] async fn a_prune_while_a_claim_is_in_flight_costs_nothing()      // proof 30
// run two tasks to completion through the world's dispatcher; start a third whose tool
// call is held open; a second dispatcher over the same stores calls
// prune_fleet(a future horizon); release the tool call
// assert: prune.pruned >= 2; the third run ends terminal; its fleet entry is Completed;
// the tool executor saw each call id exactly once
```

- [ ] **Step 2: Run.** `cargo test -p rakka-example-multi-agent-goal-acceptance --test fleet_prune`. Expected: PASS (the behaviour exists after Task 9). **Guard:** set `FLEET_WRITE_REAPPLICATIONS` to 0; the entry must end `Cancelled` and the test fail.

- [ ] **Step 3: Confirm the example's stdout.** `cargo run -p rakka-example-multi-agent-goal-acceptance` prints what its README documents. Add a README section naming the new test.

- [ ] **Step 4: Commit**

```bash
git add examples/multi-agent-goal-acceptance && git commit -m "Prove a prune under a live claim end to end (issue #145)"
```

---

### Task 11: Documents, changelog, validation

**Files:**
- Modify: `docs/rakka-agents.md`, `docs/rakka-compatibility.md`, `docs/rakka-agent-observability-catalogue.md`, `docs/rakka-v1-known-limitations-roadmap.md`, `docs/rakka-api-boundary-inventory.md`, `CHANGELOG.md`

- [ ] **Step 1: Write the documents from spec sections 7, 9 and 10.**
  - `rakka-agents.md`: the prune and its two entry points, the three rules, what a prune removes (the cancel reason), the three steps before the first prune.
  - `rakka-compatibility.md`: one entry for #145 with every line of spec section 7, including the source changes, the behaviour changes and the new stable strings.
  - The observability catalogue: the gauge, the prune counter, the skipped-dispatch outcome.
  - Known limitations: one record per database, bounded by a prune; a stale worker can still burn an attempt on an unfinished row; #134 is open.
  - The API inventory: the new public items.
  - `CHANGELOG.md`: Added (prune, gauge, `settle_claim_from_row`, `before_next_load`), Changed (settle re-read, cancellation order, `with_metrics`, `claim-fenced` for an absent entry), Fixed (the workflow tier's second dispatch, the reopened finished row), Removed (`AgentDispatcherFleet::cancel_run_dispatches`).

- [ ] **Step 2: Run the tests that hold documents as data.**

Run: `cargo test -p rakka-agent --all-features --test metric_catalogue --test collector_allowlist && cargo test -p rakka-agent-workflow --test operational_runbooks --test api_compatibility`
Expected: PASS.

- [ ] **Step 3: Run the whole validation, reading the exit code from a file.**

```bash
scripts/validate.sh > "$SCRATCH/validate.log" 2>&1; echo $? > "$SCRATCH/validate.code"
scripts/package-check.sh > "$SCRATCH/package.log" 2>&1; echo $? > "$SCRATCH/package.code"
```

Expected: both codes `0`. If the one-shot workspace test is killed under load, run the crates one at a time.

- [ ] **Step 4: Confirm Task 2's byte pin still passes.** `cargo test -p rakka-agent-workflow --test dispatcher_fleet_prune a_one_entry_record_keeps_the_bytes_it_had`. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add docs CHANGELOG.md && git commit -m "Document the fleet record prune (issue #145)"
```
