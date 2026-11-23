# The wind-down fence parks a call that may have executed — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** On a winding-down run, an idempotent call with at least one started attempt is parked `Indeterminate` under `run-cancelled-after-attempt` instead of being cancelled as never started (issue #206).

**Architecture:** One predicate in `dispatch.rs` ("an earlier attempt may have executed") is shared by the refusal path that already parks and by the three places that tell a winding-down run its call was cancelled: the claim's fence in `execute_claim`, the finished-row reader `owed_by_finished_row`, and the per-run sweep's fenced-`Cancelled` arm in `settle_waited_effects`. The run side is unchanged: it already parks on `Indeterminate`.

**Tech Stack:** Rust 1.88 (MSRV), tokio tests, `rakka-agent` test fixtures (`tests/common/mod.rs`).

**Spec:** `docs/superpowers/specs/2026-11-22-wind-down-fence-parks-possibly-executed-design.md`

## Global Constraints

- New stable code, exactly: `run-cancelled-after-attempt`. Park message: the existing `INDETERMINATE_OUTCOME_MESSAGE`.
- The charge is the attempts that started: the row's count (`attempt - 1` at the claim's fence, `row.attempts().attempts()` in the readers).
- Unchanged: read-only, reconcileable and non-idempotent classes; kinds with `exempt_from_wind_down_fence()`; the `Dispatching` idempotent retry in `recover_ambiguous`; no lease check before the fence's park; no repair of runs already told.
- No new durable field, schema version or dependency. No change to `run.rs`.
- Workspace lints: every public item documented; `cargo clippy ... -D warnings` clean.
- Focused test runs use one feature set throughout, so nothing rebuilds between them: `cargo test -p rakka-agent --all-features --test <file>`. The target directory is cold: the first build takes 30–50 minutes. Never pipe `scripts/validate.sh` into `tail`; redirect it to a file and capture `$?`.

## Review Focus

1. **An operator answers the park "not executed".** The cancel then finishes: the run ends `Cancelled`, and the call is never dispatched again. Test: Task 1, `the_park_answered_not_executed_lets_the_cancel_finish`.
2. **The run is pumped again after the park.** Nothing is dispatched or parked a second time. Test: Task 1, the second-pump assertions in `a_counted_lost_attempt_whose_worker_died_before_the_retry_is_parked_when_the_run_is_cancelled`.
3. **A row that counts two failed attempts is charged two.** This tells the row's count apart from a constant 1. Test: Task 1, `a_call_parked_after_two_failed_attempts_is_charged_both`.

I checked for others and found none a person is likely to meet that the tasks' tests leave open. Exempt kinds never reach the changed branches: the fence branch and `fence_applies` both exclude them.

---

### Task 1: The claim's fence parks a call that may have executed

**Files:**
- Modify: `crates/rakka-agent/src/dispatch.rs`. Add the predicate and code. Change the fence branch in `execute_claim` (the comment that reads "The fence: a ticket that provably never started…", around `:3943`) and `possibly_executed` in `attempt_invocation` (around `:4092`). Correct the module doc line "tickets that provably never started are cancelled" (around `:83`).
- Create: `crates/rakka-agent/tests/wind_down_possibly_executed.rs`
- Modify: `crates/rakka-agent/tests/effect_dispatch.rs`. Turn around `a_call_the_fence_cancels_before_its_retry_is_charged_the_attempt_it_made` (around `:3061`) and add two tests beside it.

**Interfaces:**
- Produces, both private to `dispatch.rs`:
  - `const RUN_CANCELLED_AFTER_ATTEMPT: &str = "run-cancelled-after-attempt";`, next to `TICKET_ROW_MISSING`, with a doc comment.
  - `fn earlier_attempt_may_have_executed(intent: &AgentRunEffect, started: u32) -> bool`. It is true iff `intent.safety.class() == AgentEffectSafetyClass::Idempotent && started > 0`. The doc comment carries the reason that sits today on `possibly_executed`.
  - Task 2 uses both.

- [ ] **Step 1: Write the failing tests in `effect_dispatch.rs`.** The helpers `a_tool_ticket_out`, `tool_row`, `tool_call_accounts` and `cancel_the_run` are already there. Rename the #182 test and give it these assertions:

```rust
// a_call_the_fence_parks_after_a_failed_attempt_is_charged_that_attempt
// (spec idempotent(3), executor .with_failure(TOOL, "upstream-unavailable", ..);
//  one pump_run, row (Failed, 1), cancel, expire_lease, pump)
assert_eq!(fx.tools.invocation_count(TOOL), 1, "the retry never started");
assert_eq!(tool_call_accounts(&fx).await, (AgentRunEffectStatus::Indeterminate, 1, spent + 1, 0));
assert_eq!(fx.effect_at(1).await.and_then(|e| e.last_error_code).as_deref(), Some("run-cancelled-after-attempt"));
assert_eq!(fx.fx.run_snapshot().await.expect("the run").status, AgentRunStatus::WaitingForReconciliation);
```

If the run drops the parked effect, `effect_at(1)` returns `None`. In that case read the code from the run state the way `tool_call_accounts` does.

  Add these two tests:
  - `a_call_parked_after_two_failed_attempts_is_charged_both`. Same setup, with a second `expire_lease` and `pump_run` before the cancel, so the row reads `(Failed, 2)`. Expect `(Indeterminate, 2, spent + 2, 0)` and two invocations.
  - `a_read_only_call_the_fence_cancels_before_its_retry_is_charged_the_attempt_it_made`. Spec `AgentEffectSpec::read_only().with_max_attempts(3)`, otherwise the old #182 setup. Expect `(AgentRunEffectStatus::Cancelled, 1, spent + 1, 0)` and one invocation. This is the guard: it passes on `main`.

- [ ] **Step 2: Write the failing tests in the new file.** Start the file with `mod common; use common::*;` and a module doc that names issue #206, following the existing test files. Use `AuthorityFixture::over(tool_then_proposal(), tool_registry_for_spec(TOOL, &AgentEffectSpec::idempotent(3)?), None)`. Port the setup from `tmp/2026-11-22-issue-196-review-probes/zz_review2_196.rs`: `p7_run` (around `:773`), `LateAuthority`, `worker()` and `FixedRefusal`. Put the cancel in a local `cancel_the_run(&fx)`. Each test asserts:

```rust
// a_counted_lost_attempt_whose_refusal_came_after_the_lease_is_parked_when_the_run_is_cancelled
// p7_run(lapse = true) up to the cancel; then fx.expire_lease(); let pass = fx.one_pass().await;
assert_eq!((pass.parked_indeterminate, pass.cancelled), (1, 1));
assert_eq!(fx.effect_status(1).await, Some(AgentRunEffectStatus::Indeterminate));
assert_eq!(status, Some(AgentRunStatus::WaitingForReconciliation));
// and the effect's last_error_code is "run-cancelled-after-attempt", attempts 1

// a_counted_lost_attempt_whose_worker_died_before_the_retry_is_parked_when_the_run_is_cancelled
// arm AfterStarted, expire, one_pass (died); arm BeforeStarted, expire, one_pass (died;
// assert the row is (Failed, 1)); cancel; expire; one_pass
// -> same four assertions as above; then the Review Focus 2 check:
fx.expire_lease();
let again = fx.one_pass().await;
assert_eq!((again.parked_indeterminate, again.cancelled, again.claimed), (0, 0, 0));
assert_eq!(fx.tools.invocation_count(TOOL), 1, "only the lost attempt ever invoked");

// the_park_answered_not_executed_lets_the_cancel_finish
// the route above to the park, then apply ResolveIndeterminateEffect { resolution:
// ConfirmedNotExecuted, .. } (shape: workflow_tool.rs `answer_the_append`, ~:1927)
assert_eq!(fx.fx.run_snapshot().await.expect("the run").status, AgentRunStatus::Cancelled);
// and one more expire + one_pass invokes nothing
```

`a_delegation_send_with_a_failed_attempt_waits_for_its_decision_and_then_its_child`:
  - Build the world as `delegation_dispatch.rs` does around `:266`: `AuthorityFixture` with `.with_delegation(delegation_config())` and `.with_a2a_send_executor(..)`, `instantiate_agent_with_envelope`, then the goal task creation command.
  - The executor's first call returns `Err(AgentDispatchError::collaborator("peer-unavailable", ..))`, which is retryable.
  - Pump until the send's row is `(Failed, 1)`, cancel the run, expire the lease, pump.
  - Assert three things after the park:
    - The send effect (`AgentRunEffectKind::A2aSendCall`) is `Indeterminate` with code `run-cancelled-after-attempt`.
    - The delegation cell's status is `AgentDelegationStatus::Pending`. On `main` it is `Failed { code: "run-winding-down", .. }`.
    - `AgentCancellationProgress::derive(run) == WaitingForReconciliation` (helper shape: `cancellation_propagation.rs` `run_view`, around `:100`).
  - Then apply `ResolveIndeterminateEffect` with `ConfirmedExecuted { outcome: AgentRunEffectOutcome::A2aSend { receipt } }`. Build the receipt naming a child task, the way the executor's `Sent` finding would.
  - Assert the cell is `ChildCreated { .. }` and the progress is `Propagating`: the run now waits on the child its cancel chases.
  - If the cell does not become `ChildCreated`, stop and report. That is the design's §5 claim failing, and `run.rs` is out of scope.

- [ ] **Step 3: Run the new tests and check that they fail.**

Run: `cargo test -p rakka-agent --all-features --test wind_down_possibly_executed --test effect_dispatch -- --nocapture`
Expected: the four new-file tests fail, and so do the turned-around #182 test and the two-attempts test: each reads `Cancelled` where `Indeterminate` is expected. The read-only guard passes.

- [ ] **Step 4: Implement it in `dispatch.rs`.**
  - Add the constant and `earlier_attempt_may_have_executed`.
  - In `attempt_invocation`, write `let possibly_executed = earlier_attempt_may_have_executed(intent, attempt.saturating_sub(1));` and move the reason into the predicate's doc.
  - In `execute_claim`'s fence branch, first compute `let started = attempt.saturating_sub(1);`. Where the predicate holds, `return self.park_indeterminate(scope, claim, &intent, started, RUN_CANCELLED_AFTER_ATTEMPT, INDETERMINATE_OUTCOME_MESSAGE, pass).await;`. Otherwise keep today's cancel.
  - Rewrite the branch's comment and the module doc line, so that "provably never started" is claimed only of a ticket the fence still cancels.
  - If the debug stack guard fails later (`examples/coordination-capability-acceptance/tests/stack_margin.rs`, run by validate), box this park with `Box::pin(..)` and say so in the commit.

- [ ] **Step 5: Run the tests and the whole crate.**

Run: `cargo test -p rakka-agent --all-features --test wind_down_possibly_executed --test effect_dispatch`
Expected: all pass.
Run: `cargo test -p rakka-agent --all-features > "$SCRATCH/t1-crate.log" 2>&1; echo $?` (use the session scratchpad).
Expected: `0`. Any other failing test should be one whose call is idempotent, whose row counts an attempt, and whose run is winding down. Turn each such test around and name it in the commit. Any other failure is a defect in this change.

- [ ] **Step 6: Clippy and commit.**

Run: `cargo clippy -p rakka-agent --all-targets --all-features -- -D warnings`
Expected: no warnings.

```bash
git add crates/rakka-agent/src/dispatch.rs crates/rakka-agent/tests/wind_down_possibly_executed.rs crates/rakka-agent/tests/effect_dispatch.rs
git commit -m "The wind-down fence parks an idempotent call that may have executed (issue #206)"
```

---

### Task 2: The finished-row readers park the same row

**Files:**
- Modify: `crates/rakka-agent/src/dispatch.rs`:
  - `owed_by_finished_row` (around `:498`): the `OutboxStatus::Cancelled if fence_applies` arm, plus its doc table row (around `:492`) and `RowVerdict::NeverStarted`'s doc.
  - The `settle_waited_effects` arm for `fenced && row.status() == OutboxStatus::Cancelled` (around `:6468`), its table row (around `:6298`) and its bullet (around `:6341`).
  - The `conclude_on_untold_row` doc line "keeps the two steps it always had".
- Modify: `crates/rakka-agent/tests/wind_down_possibly_executed.rs`

**Interfaces:**
- Consumes: `RUN_CANCELLED_AFTER_ATTEMPT` and `earlier_attempt_may_have_executed` from Task 1. Also the existing `RowVerdict::Unknown { code }` and `tell_owed`.

- [ ] **Step 1: Write the failing tests.** All three start with the tool's first attempt failing (`fx.tools` built `.with_failure(TOOL, "upstream-unavailable", ..)`, or `with_tool_executor`), one pass, the row at `(Failed, 1)`, then the run cancelled:

```rust
// the_fleets_own_cancellation_of_a_failed_call_parks_it
// AgentDispatcherWorker::with_clock_and_metrics(id, fx.fleet_store.clone(), fx.workflow_store.clone(),
//   AgentDispatcherFleetSettings::new(16, LEASE_MS), fx.wf_clock.clone(), Arc::new(NoopMetricsRecorder))
//   .cancel_run_dispatches(&rakka_agent::workflow_run_id(&run_scope()))   -> row (Cancelled, 1), entry finished
// then pump_run:
assert_eq!((pass.parked_indeterminate, pass.cancelled), (1, 0));
// effect Indeterminate, code "run-cancelled-after-attempt", attempts 1; run WaitingForReconciliation

// an_earlier_builds_cancelled_row_is_parked_by_the_claim
// cancel the row alone, as an earlier build's fence did before it died (dispatch_owed_outcome.rs
// `finish_row_in` with OutboxStatus::Cancelled, line "run-cancelled"); the fleet entry stays live.
// Advance the clock past the retry backoff, then pipeline.claim_and_execute() (no register_run,
// so the sweep does not answer first):
assert_eq!((pass.parked_indeterminate, pass.cancelled), (1, 1));
// effect Indeterminate under the code, attempts 1

// an_earlier_builds_cancelled_row_is_parked_by_the_sweep
// the same row, then pump_run (register_run's sweep reaches it first):
assert_eq!(pass.parked_indeterminate, 1);
// effect Indeterminate under the code, attempts 1
```

- [ ] **Step 2: Run them and check that they fail.**

Run: `cargo test -p rakka-agent --all-features --test wind_down_possibly_executed`
Expected: the three new tests fail, each reading `Cancelled`. Task 1's tests still pass.

- [ ] **Step 3: Implement it.**
  - In `owed_by_finished_row`, a `Cancelled` row with `fence_applies` returns `(RowVerdict::Unknown { code: RUN_CANCELLED_AFTER_ATTEMPT }, counted)` where `earlier_attempt_may_have_executed(intent, counted)` holds. Otherwise it returns `NeverStarted` as before.
  - In the sweep's fenced-`Cancelled` arm, take the verdict as well as the count and deliver it with `self.tell_owed(scope, &intent, attempt, 0, verdict, pass)`. Do not deliver `Cancelled` by hand.
  - `conclude_on_untold_row` needs no code change: an `Unknown` verdict already goes through `settle_claim_from_row` and `tell_owed`. Correct its doc sentence.
  - Update the three doc tables and bullets, so that each says a fenced `Cancelled` row of an idempotent call with a counted attempt parks under `run-cancelled-after-attempt`.

- [ ] **Step 4: Run the tests and the whole crate.**

Run: `cargo test -p rakka-agent --all-features --test wind_down_possibly_executed`
Expected: all pass.
Run: `cargo test -p rakka-agent --all-features > "$SCRATCH/t2-crate.log" 2>&1; echo $?`
Expected: `0`, under the same rule for flipped tests as in Task 1 step 5.

- [ ] **Step 5: Clippy and commit.**

Run: `cargo clippy -p rakka-agent --all-targets --all-features -- -D warnings`
Expected: no warnings.

```bash
git add crates/rakka-agent/src/dispatch.rs crates/rakka-agent/tests/wind_down_possibly_executed.rs
git commit -m "A cancelled row of a call that may have executed is parked, not told Cancelled (issue #206)"
```

---

### Task 3: Product documents, breaks, validation

**Files:**
- Modify: `docs/rakka-agents.md`. Add one passage in point 5, "Waiting" (around `:213`, after "never an automatic re-invocation."), covering:
  - the rule;
  - the code;
  - the charge;
  - the decision it waits for;
  - that a `Dispatching` idempotent row still retries.
- Modify: `docs/rakka-compatibility.md`. Add a new entry after issue #196's (around `:1050`), before "Run the default compatibility matrix". Use the #196 entry's sections:
  - What was wrong: all three routes, and the orphaned child.
  - The rule.
  - What a deployment sees, from design §5 as amended. Name `run-cancelled-after-attempt` as a new stable string.
  - Mixed builds and rollback, from §6.
  - Not repaired.
  - Not changed: the `Dispatching` retry; the other classes and the exempt kinds.
- Modify: `CHANGELOG.md`. Add a new first bullet under `### Fixed` (around `:589`), in the form of the #196 bullet.

- [ ] **Step 1: Write the three documents.** For every sentence in "What a deployment sees", name the test assertion that holds it, as a list in the commit message. A sentence with no assertion behind it is removed or gets a test.

- [ ] **Step 2: Run the breaks, one at a time**, each against `wind_down_possibly_executed`, `effect_dispatch`, `dispatch_refused_before_started` and `dispatch_owed_outcome`. Restore the file after each break.

| Break | Expected to fail |
|---|---|
| the claim's fence always cancels (Task 1 branch removed) | the four Task 1 tests in the new file; the turned-around #182 test; the two-attempts test |
| `owed_by_finished_row` always answers `NeverStarted` for a fenced `Cancelled` row | `an_earlier_builds_cancelled_row_is_parked_by_the_claim` |
| the sweep's arm delivers `Cancelled` by hand again | `the_fleets_own_cancellation_…`, `…_parked_by_the_sweep` |
| the predicate's `started > 0` becomes `started > 1` | every Task 1 and Task 2 test whose row counts one |
| `attempt_invocation` passes `attempt` to the predicate | an idempotent refusal test in `dispatch_refused_before_started.rs` |

  A break that compiles and fails nothing is a gap. Add the test that catches it, or declare why it can't be caught. A break that doesn't compile is not a catch; redo it by hand.

- [ ] **Step 3: Validate.**

Run: `scripts/validate.sh > "$SCRATCH/validate.log" 2>&1; echo $?`, then `scripts/package-check.sh > "$SCRATCH/package.log" 2>&1; echo $?`
Expected: `0` and `0`. If the workspace test step is killed under load, run the crates one at a time instead.

- [ ] **Step 4: Commit.**

```bash
git add docs/rakka-agents.md docs/rakka-compatibility.md CHANGELOG.md
git commit -m "Document the wind-down fence's park of a call that may have executed (issue #206)"
```
