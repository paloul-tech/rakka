# Dispatch Missing-Row Park Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A claim on a fleet entry whose outbox row is gone ends the claim, and a run still waiting on that effect is parked on it as indeterminate (issue #150).

**Architecture:** The absent-row arm of `AgentRunEffectDispatcher::execute_claim` stops writing to the missing row. It finds the run's effect by the claim's ticket id; an outstanding effect is parked through the existing `park_indeterminate_with`, which gains a fleet-only way to end the ticket; anything else is settled at the fleet alone. Nothing changes on the run side.

**Tech Stack:** Rust 1.88, `rakka-agent` over `rakka-agent-workflow`'s `AgentDispatcherFleet`, tokio tests over the crate's `AuthorityFixture`.

**Spec:** `docs/superpowers/specs/2026-11-16-dispatch-missing-row-park-design.md`

## Global Constraints

- Nothing is invoked and nothing is written to the outbox on this path.
- The code on the fleet entry and on the run's effect is exactly `ticket-row-missing`.
- The run is told first, the fleet entry is settled second.
- `claim-fenced` from the fleet settle is "the claim is over", never a failed pass.
- The existing callers of `park_indeterminate_with` and `settle_ticket_cancelled` behave exactly as they do now.
- Every public item needs a doc comment; `clippy -D warnings` must pass. Error codes are a compatibility surface: add none.
- `cargo clean` at each task boundary (disk pressure on this machine). Run tests per crate, not the one-shot workspace run. Redirect `scripts/validate.sh` to a file and read `$?`, never pipe it into `tail`.
- No commit is pushed and no PR is opened without the owner asking.

## Review Focus

Inputs the spec implies and a person will meet, each pinned by a test in Task 1:

1. **A later claim in a batch whose lease lapsed while an earlier one ran**, and another worker already settled the orphan: the fleet answers `claim-fenced`; the late worker's pass answers `Ok` and delivers nothing.
2. **The run cannot be written when the park is delivered** (its store refuses): the pass fails, the entry stays `Claimed`, and the next lease parks the effect. Nothing is lost by the order.
3. **A run that has already ended** with the orphan still registered: the pass answers `Ok`, the entry is settled, and the tool is never called.
4. **A healthy ticket of the same run behind the orphan in one batch**: it is executed in that pass.
5. **Both records gone** (run and workflow): `Ok`, entry settled, nothing delivered.

---

### Task 1: Settle the orphan and park the run

**Files:**
- Modify: `crates/rakka-agent/src/dispatch.rs` (the absent-row arm of `execute_claim`, ~line 2955; `park_indeterminate_with`, ~line 4014; two new private helpers beside `conclude_on_finished_row`, ~line 2751)
- Create: `crates/rakka-agent/tests/dispatch_missing_row.rs`

**Interfaces:**
- Consumes: `AgentDispatcherFleet::settle_claim_from_row(&claim, None)`, `AgentDispatcherFleet::state()` + `AgentDispatcherFleetState::entry(&dispatch_id)` (its `attempts: u32` is public), `AgentRunEffect::dispatch_ticket_id()`, `AgentRunEffect::is_outstanding()`, `Self::run_state(scope)`, `Self::deliver_outcome`.
- Produces (all private to `dispatch.rs`):

```rust
/// The code a ticket whose outbox row is gone settles under, at the fleet
/// and on the run's effect.
const TICKET_ROW_MISSING: &str = "ticket-row-missing";
const TICKET_ROW_MISSING_MESSAGE: &str =
    "the dispatch ticket's outbox row is gone; whether an attempt invoked the target cannot be established";

/// How a park ends the claim's ticket.
enum ParkedTicket {
    /// The outbox row and the fleet entry, as every park but one does.
    BothLayers,
    /// The fleet entry alone: the row is gone.
    FleetOnly,
}

async fn conclude_on_missing_row(
    &mut self,
    scope: &AgentRunScope,
    claim: AgentDispatchClaim,
    pass: &mut AgentDispatchPass,
) -> AgentDispatchResult<ClaimConclusion>;

async fn settle_orphan_claim(
    &mut self,
    claim: &AgentDispatchClaim,
    pass: &mut AgentDispatchPass,
) -> AgentDispatchResult<()>;
```

`park_indeterminate_with` takes one more argument, `ticket: ParkedTicket`, placed before `pass`. `park_indeterminate` and every existing caller pass `ParkedTicket::BothLayers`.

- [ ] **Step 1: Write the test file's helpers and the first failing test**

`crates/rakka-agent/tests/dispatch_missing_row.rs`, module doc naming issue #150, `mod common; use common::*;`. Copy `tool_then_proposal`, `fleet_entry` and `ready_tool_ticket` from `dispatch_finished_row.rs` (tests do not share helpers across files here). Add:

```rust
/// Deletes the run's whole workflow record: its inbox and every outbox row.
async fn delete_workflow_record(fx: &AuthorityFixture);   // load, then DurableStateStore::delete at the loaded revision

/// Removes one effect's outbox row and leaves the record's other rows.
async fn drop_outbox_row(fx: &AuthorityFixture, effect: &AgentRunEffect);
```

`drop_outbox_row` round-trips the stored `WorkflowState` through `serde_json::Value`, removes the entry keyed by `effect.dispatch_ticket_id()`, decodes it back and writes it at the loaded revision. Assert inside the helper that `fx.outbox_row(effect)` is then `None`.

```rust
#[tokio::test]
async fn a_live_run_whose_ticket_row_is_gone_is_parked_and_the_entry_settled() {
    let spec = AgentEffectSpec::idempotent(3).expect("the spec is valid");
    let (fx, effect) = ready_tool_ticket(&spec, None).await;
    let mut pipeline = fx.pipeline();
    pipeline.register_run(&run_scope()).await.expect("the registration runs");
    delete_workflow_record(&fx).await;

    let pass = pipeline.claim_and_execute().await.expect("an orphan does not fail the pass");

    assert_eq!((pass.claimed, pass.cancelled, pass.parked_indeterminate, pass.delivered), (1, 1, 1, 1));
    assert_eq!(pass.invoked, 0);
    let entry = fleet_entry(&fx, &effect).await.expect("the entry is kept");
    assert_eq!(entry.status, AgentDispatchStatus::Cancelled);
    assert_eq!(entry.last_error_code.as_deref(), Some("ticket-row-missing"));
    let parked = fx.effect_at(1).await.expect("the effect is held");
    assert_eq!(parked.status, AgentRunEffectStatus::Indeterminate);
    assert_eq!(parked.last_error_code.as_deref(), Some("ticket-row-missing"));
    // one open checkpoint of kind IndeterminateEffectReconciliation, read the
    // way tool_authority.rs:337 reads `open_checkpoints()`
    assert!(fx.outbox_row(&effect).await.is_none(), "nothing was written to the missing row");

    fx.expire_lease();
    let again = fx.pipeline().claim_and_execute().await.expect("the pass runs");
    assert_eq!(again.claimed, 0, "a settled orphan is never claimed again");
}
```

Also assert the recording tool executor saw no call (`fx.tools`, by whatever count accessor it exposes).

- [ ] **Step 2: Run it and see the issue's own failure**

Run: `cargo test -p rakka-agent --test dispatch_missing_row -- --nocapture`
Expected: FAIL at `an orphan does not fail the pass` with code `outbox-entry-not-found`.

- [ ] **Step 3: Implement**

In `dispatch.rs`:

1. `settle_orphan_claim`: `self.fleet.settle_claim_from_row(claim, None)`; `Ok(_)` → `pass.cancelled += 1`; `Err(ClaimFenced { .. })` → `Ok(())` with no count; any other error is returned.
2. `park_indeterminate_with(.., ticket: ParkedTicket, pass)`: its last step becomes `match ticket { BothLayers => settle_ticket_cancelled(scope, &claim, "indeterminate", pass), FleetOnly => settle_orphan_claim(&claim, pass) }`. Nothing else in it moves.
3. `conclude_on_missing_row`: read `self.run_state(scope)`; find the effect in `loop_state().effects()` whose `dispatch_ticket_id().as_str() == claim.effect_id.as_str()` and which `is_outstanding()`. None (no run, no loop state, no match, not outstanding) → `settle_orphan_claim`, `Ok(Settled)`. Some → the attempt is `max(intent.attempts, the fleet entry's attempts)`, the entry read from `self.fleet.state()` by `claim.dispatch_id` and taken as 0 when unreadable; then `park_indeterminate_with(scope, claim, &intent, attempt, TICKET_ROW_MISSING, TICKET_ROW_MISSING_MESSAGE, None, None, ParkedTicket::FleetOnly, pass)`.
4. The absent-row arm of `execute_claim` becomes `return self.conclude_on_missing_row(scope, claim, pass).await;` with the comment rewritten: the row is gone, nothing is written to it, the entry is settled and a run still waiting is told.

Doc comment on `conclude_on_missing_row`: why indeterminate and not cancelled (the row was the only record of whether an attempt started), why the run is told before the entry is settled (a crash between the two is repaired by the next lease; the other order strands the run), and the accepted cost (a worker whose lease lapsed can park an effect another worker is running, if the run recreated the row in between).

If a test overflows its stack, box the new call in the arm with `Box::pin(..)`: `execute_claim`'s future is already large and this repo has met that before.

- [ ] **Step 4: Run the test and see it pass**

Run: `cargo test -p rakka-agent --test dispatch_missing_row -- --nocapture`
Expected: PASS, 1 test.

- [ ] **Step 5: Add the remaining tests, each run red-or-green as noted**

All in the same file. Names and assertions:

- `with_the_run_gone_too_the_entry_is_settled_and_nobody_is_told` — after `register_run`, delete the workflow record and the run's own record (`fx.fx.runs`, the run scope's persistence id). Pass `Ok`; `(claimed, cancelled, parked_indeterminate, delivered) == (1, 1, 0, 0)`; entry `Cancelled` / `ticket-row-missing`.
- `a_run_that_already_ended_is_told_nothing` — cancel the run to its terminal status through the fixture's run entity, without a `register_run` after it (that would fence the ticket first); delete the workflow record; `claim_and_execute`. Pass `Ok`; `invoked == 0`; entry `Cancelled` / `ticket-row-missing`; the effect's status is not `Indeterminate`.
- `a_healthy_ticket_behind_the_orphan_runs_in_the_same_pass` — a model turn with two tool calls (two tickets in one run); `register_run`; `drop_outbox_row` on the ticket `claim_due` hands out first (read its sort at `dispatcher.rs:1530` to pick it). Pass `Ok`; `claimed == 2`; `invoked == 1`; the orphan's entry is `Cancelled` / `ticket-row-missing`; the healthy ticket's entry is `Completed`.
- `a_refused_fleet_write_after_the_park_is_repaired_on_the_next_lease` — as `row_when_the_fleet_write_is_refused` in `dispatch_finished_row.rs`: `register_run`, delete the workflow record, `fx.fleet_store.crash_at(2, CrashPoint::BeforeWrite)`, `claim_and_execute` fails with `store-error`, `assert_crash_fired`, `survive`. Then: the effect is `Indeterminate`, the entry is `Claimed`. `expire_lease`; the next pass is `Ok` with `(claimed, cancelled, parked_indeterminate, delivered) == (1, 1, 0, 0)`; entry `Cancelled` / `ticket-row-missing`.
- `a_run_that_cannot_be_written_keeps_the_claim_for_the_next_lease` — arm `fx.fx.runs` to refuse the delivery's write; the pass fails; the entry is `Claimed` and the effect still `Ready`. `survive`, `expire_lease`; the next pass parks as in the first test.
- `a_late_worker_finds_the_orphan_already_settled_and_goes_on` — the two-tool-call run with the row of the ticket claimed *second* dropped. The first ticket's tool is an executor in the shape of `OverrunningTool`: its first call advances the clock past `LEASE_MS` and runs another worker's `claim_and_execute`, which takes both tickets over and parks the orphan. The first worker then reaches the orphan with a lapsed claim. Its pass answers `Ok`; the run's effect is `Indeterminate` once (the other worker's pass counted `parked_indeterminate == 1`, this one's `0`); entry `Cancelled` / `ticket-row-missing`.

Run: `cargo test -p rakka-agent --test dispatch_missing_row -- --nocapture`
Expected: PASS, 7 tests.

- [ ] **Step 6: Falsify each guard, then restore**

One at a time, with the suite run after each, and the change reverted before the next:

| Mutation | The test that must fail |
|---|---|
| the arm calls `settle_ticket_cancelled` again | the first test, with `outbox-entry-not-found` |
| `ClaimFenced` is returned instead of swallowed in `settle_orphan_claim` | `a_late_worker_…` |
| `FleetOnly` settles before `deliver_outcome` (swap the order for this variant) | `a_run_that_cannot_be_written_…` or `a_refused_fleet_write_…` (record which) |
| the `is_outstanding()` filter removed | `a_refused_fleet_write_…` (second pass counts a park) |
| the park skipped (always `settle_orphan_claim`) | the first test, at the effect's status |

A mutation no test catches is a missing test: add it before going on. In zsh, run each cargo command inline; a command held in an unquoted variable does not word-split and silently runs nothing.

- [ ] **Step 7: The neighbours still pass**

Run: `cargo test -p rakka-agent --test dispatch_finished_row --test dispatch_fleet_prune --test dispatch_pass_split --test dispatch_inbox_write_retry --test effect_dispatch --test tool_authority`
Expected: PASS, no test count lower than before.
Run: `cargo clippy -p rakka-agent --all-targets --all-features -- -D warnings` and `cargo fmt --all -- --check`
Expected: clean.

- [ ] **Step 8: Commit**

```bash
git add crates/rakka-agent/src/dispatch.rs crates/rakka-agent/tests/dispatch_missing_row.rs
git commit -m "Settle a dispatch ticket whose outbox row is gone and park its run (issue #150)"
```

Then `cargo clean`.

---

### Task 2: Documents, the spec's as-built note, and full validation

**Files:**
- Modify: `docs/rakka-compatibility.md` (a new entry beside the issue #145 one, ~line 356, and the issue #81 sentence at line 141 only if it is now inexact)
- Modify: `docs/rakka-agent-observability-catalogue.md` (only where the effect's indeterminate codes or a fleet entry's `last_error_code` values are listed)
- Modify: `CHANGELOG.md` (Unreleased, beside the issue #145 entry)
- Modify: `docs/superpowers/specs/2026-11-16-dispatch-missing-row-park-design.md` (status line; an "As built" section)

**Interfaces:**
- Consumes: Task 1 as merged into the branch: the code `ticket-row-missing`, the pass counters, the order.
- Produces: nothing code reads.

- [ ] **Step 1: Find what holds these documents to the code**

Run: `grep -rn "rakka-compatibility.md\|observability-catalogue.md\|CHANGELOG.md" crates/*/tests/*.rs | head -20`
Read each test that parses a document you are about to edit, so the entry is written in the form it checks.

- [ ] **Step 2: Write the entries**

Each says, in the document's own voice:

- A claim on a ticket whose outbox row is gone no longer fails the pass with `outbox-entry-not-found`; the fleet entry is finished as `Cancelled` with `last_error_code` `ticket-row-missing`, and the batch goes on.
- A run that still holds the effect unresolved is parked on it: the effect becomes `Indeterminate` with the code `ticket-row-missing`, behind the reconciliation checkpoint every indeterminate effect opens. `ticket-row-missing` is new as an effect's indeterminate code; it was already documented as a fleet entry's `last_error_code`.
- What does not move: no schema version, no new refusal code, no change to a ticket whose row exists. A row the run recreates before the claim arrives is run as a first attempt, as before.

- [ ] **Step 3: The spec's as-built note**

Status becomes "built". The "As built" section lists only where the build moved from the design: the names chosen (`ParkedTicket`, `conclude_on_missing_row`, `settle_orphan_claim`), where the attempt count came from, how the two-claim batch was built, which test the swapped-order mutation failed, and anything Step 6 of Task 1 turned up.

- [ ] **Step 4: Document tests**

Run the tests Step 1 found, plus `cargo test -p rakka --all-features` if the facade holds any of them.
Expected: PASS.

- [ ] **Step 5: Full validation**

With the rustup toolchain's `bin` first on `PATH` (CI floats on `stable`; the local default cargo is Homebrew's):

```bash
scripts/validate.sh > "$SCRATCH/validate.log" 2>&1; echo "exit $?"
scripts/package-check.sh > "$SCRATCH/package-check.log" 2>&1; echo "exit $?"
```

Expected: `exit 0` twice. If the one-shot workspace test is killed under load, run `cargo test -p <crate> --all-features` per crate and say so.

- [ ] **Step 6: Commit**

```bash
git add docs/rakka-compatibility.md docs/rakka-agent-observability-catalogue.md CHANGELOG.md docs/superpowers/specs/2026-11-16-dispatch-missing-row-park-design.md
git commit -m "Document the settled missing-row ticket and its park (issue #150)"
```

Then `cargo clean`.
