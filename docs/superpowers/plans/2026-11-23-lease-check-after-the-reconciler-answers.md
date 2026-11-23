# A claim checks its lease right after the reconciler answers — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A reconciler finding that comes after the claim's lease lapsed, whatever the finding, ends the claim out of lease before anything is written (issue #212).

**Architecture:** `reconcile_ambiguous` in `crates/rakka-agent/src/dispatch.rs` checks `is_current_claim_for(&claim, 0)` right after the reconciler answers, before the `match` on the finding. On a lapsed lease it returns `ClaimStep::Concluded(ClaimConclusion::OutOfLease)`. The "not executed" arm keeps #171's cover check for another attempt under the same claim. #195's "still current" check is folded into the new one.

**Tech Stack:** Rust 1.88 (MSRV), tokio tests, `rakka-agent` test fixtures.

**Spec:** `docs/superpowers/specs/2026-11-23-lease-check-after-the-reconciler-answers-design.md`

## Global Constraints

- **The branch is stacked on #207's** (`lease-check-after-the-authority-answers`, PR #213). Base everything on it. Do not merge or rebase onto `main` until #213 has merged.
- **The new check asks only that the lease is still current:** `is_current_claim_for(&claim, 0)`. It applies to every finding, a winding-down run's included (the owner's ruling).
- **A reconciler error keeps its handling:** the `?` on `reconcile(...)` stays ahead of the check.
- **#171's cover check stays,** for another attempt under the same claim (`covered_ms = intent.timeout_ms.unwrap_or(0)`). `asked_at` is still read before it.
- **No new durable field, status, error code or decision kind.**
- **Workspace lints:** every public item has a doc comment, and `cargo clippy ... -D warnings` is clean. Never add `-D warnings` to the doc build.
- **Test runs:** use one feature set throughout, `cargo test -p rakka-agent --all-features ...`. The target directory was cleaned after #207, so the first build takes 30–50 minutes. Run crate-wide with `--no-fail-fast`. Never pipe `scripts/validate.sh` into `tail`; redirect it to a file and capture `$?`.

## Review Focus

1. **A finding inside a renewed lease is acted on.** The check must read the fleet's lease, which a batch's settles renew, not the expiry the claim was handed. #171's renewed-lease test does not tell the two apart at this check, so it gets a guard of its own. Test: Task 1, `a_finding_inside_a_renewed_lease_is_acted_on`.
2. **A claim's own attempt that ends unknown and is answered "unknown" after its lease.** The claim ends out of lease, and the claim that follows counts the attempt once. Test: Task 1, `a_claims_own_unknown_attempt_answered_after_the_lease_is_counted_once`.

I checked and found no other likely input. An `Executed` whose outcome does not belong to the call reaches the same check before the `match`, as every other finding does.

---

### Task 1: The check and its tests

**Files:**
- Modify `crates/rakka-agent/src/dispatch.rs`:
  - `reconcile_ambiguous` (around `:5065`): the check after `reconciler.reconcile(...)` (around `:5107`). In the "not executed" arm (around `:5187-5233`), drop `first_follows` and rewrite the comment block.
  - Docs: `settle_unknown_finding` (around `:5275`); `AgentDispatchPass::lapsed` (around `:2342`); `ClaimConclusion::OutOfLease` (around `:2385`).
- Modify `crates/rakka-agent/tests/dispatch_refused_before_started.rs`: a new section at the end, with two helpers and three tests.
- Modify `crates/rakka-agent/tests/dispatch_failed_attempt_recovery.rs`: a new section at the end, with a helper and four tests.
- Modify `crates/rakka-agent/tests/dispatch_attempt_lease.rs`: one guard test.

**Interfaces:**
- Consumes (`dispatch_refused_before_started.rs`):
  - `worker(fx, id, authority, tools) -> Pipeline`, `Pipeline::with_reconciler(Arc<dyn AgentEffectReconciler>)`;
  - `the_fixtures_authority(fx)`, `a_call_whose_first_attempt_failed(spec) -> (AuthorityFixture, u64)`, `reconcileable(n)`;
  - `HeldTool { calls, gate }`, `UnknownTool`.
- Consumes (`dispatch_failed_attempt_recovery.rs`):
  - `failing_reconcileable_call(n)`, `HookedReconciler::new(&fx, &answers)` and `arm(BeforeTheAnswer::OutlastsTheLease)`;
  - `claim_round`, `the_late_claim`, `tool_row`, `cancel_the_run`, `not_executed()`, `executed()`.
- Consumes (`dispatch_attempt_lease.rs`): `world_of`, `ready`, `pass_of`, `row_of`, `Calls::answers`.
- Produces: nothing a later task consumes.

- [ ] **Step 1: Write the failing tests in `dispatch_refused_before_started.rs`.** Section comment: "Every late finding (issue #212)". Two helpers, adapted from the review's probes in `tmp/2026-11-23-issue-207-review/zz_review_207.rs`:
  - `LateReconciler { answers: ScriptedReconciler, clock: SharedAtomicWorkflowClock, late: AtomicBool, hand_off: Mutex<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>> }`. Its first answer moves the clock `LEASE_MS + 1` and, if a hand-off is armed, signals and waits. Every answer is `answers`'. Add a constructor `fn late_reconciler(fx: &AuthorityFixture, answers: &ScriptedReconciler) -> LateReconciler`, with no hand-off armed.
  - `LateErrorAuthority { inner, clock, hand_off }`: the probe's, unchanged. Its first answer is an `authority-unavailable` collaborator error after the lease, and it hands off. Every later answer is `inner`'s.

  The three tests:
  - `a_late_unknown_finding_is_not_acted_on_beside_another_workers_call` (probe A):
    - Setup: `reconcileable(2)`. Worker one has a `LateReconciler` answering `Unknown`, with the hand-off armed. Worker two has a prompt "not executed" reconciler and a `HeldTool`. The three-future join is the probe's.
    - Worker one's pass is `Ok`, with `(lapsed, failed_attempts, parked_indeterminate, delivered) == (1, 0, 0, 0)`.
    - Worker two's pass has `(invoked, delivered) == (1, 1)`, and `tool.calls == 1`.
    - The row's status is `Dispatched`. Read it through `outbox_row` on the effect captured before the passes.
    - After `fx.pump()`, the run is `Completed`.
  - `an_unknown_finding_after_a_late_authority_error_is_not_acted_on_beside_another_workers_call` (probe B):
    - Setup: as above, but worker one has a `LateErrorAuthority` and a prompt `Unknown` reconciler, an `Arc<ScriptedReconciler>` kept for its count.
    - The same assertions, and worker one's `reconciler.queries() == 1` (design call 4: asked once, and the answer discarded).
  - `a_claims_own_unknown_attempt_answered_after_the_lease_is_counted_once` (Review Focus 2):
    - Setup: `AuthorityFixture::over(tool_then_proposal(), tool_registry_for_spec(TOOL, &reconcileable(3)), None)`, started and pumped to the tool ticket. A shared `UnknownTool`. Worker one has `late_reconciler(&fx, &unknown)`, with no hand-off.
    - Worker one's pass is `Ok`, with `(invoked, lapsed, failed_attempts) == (1, 1, 0)`. The row is `(Dispatching, 0)`.
    - Then `fx.expire_lease()`, and worker two runs with a prompt `Unknown` reconciler: `failed_attempts == 1`, `invoked == 0`. The row is `(Failed, 1)`, and `tool.calls == 1`.

- [ ] **Step 2: Write the failing tests in `dispatch_failed_attempt_recovery.rs`.** Section comment: "A finding after the lease (issue #212)". Add a helper, `fn unknown() -> ScriptedReconciler`. Each test builds its fixture as `a_claim_whose_lease_lapsed_while_the_reconciler_answered_makes_no_attempt` does: `HookedReconciler::new(&fx, &answers)`, `arm(BeforeTheAnswer::OutlastsTheLease)` before `fx.start()`, then a first `claim_round` with the row at `(Failed, 1)`. The late claim is `the_late_claim(&fx)`.
  - `a_late_unknown_finding_leaves_the_burn_to_the_claim_that_follows`:
    - Setup: `failing_reconcileable_call(3)`, with `answers = unknown()`.
    - The late claim: `(lapsed, failed_attempts, parked_indeterminate) == (1, 0, 0)`, `answers.queries() == 1`, and the row is still `(Failed, 1)`.
    - The next `claim_round`: `failed_attempts == 1`, `answers.queries() == 2`, the row is `(Failed, 2)`, and `fx.tools.invocation_count(TOOL) == 1`.
  - `a_late_executed_finding_is_left_to_the_claim_that_follows`:
    - Setup: `failing_reconcileable_call(3)`, with `answers = ScriptedReconciler::new().with_finding(executed())`.
    - The late claim: `(lapsed, delivered) == (1, 0)`, and the row is still `(Failed, 1)`.
    - The next `claim_round`: `delivered == 1` and `answers.queries() == 2`.
    - After `fx.pump()`, the run is `Completed`, and `fx.tools.invocation_count(TOOL) == 1`.
    - A resolved effect leaves `effects()` once its turn records, so don't read `tool_row` after the delivery.
  - `a_late_not_executed_with_no_budget_left_is_settled_by_the_claim_that_follows` (#171's exception, turned around):
    - Setup: `failing_reconcileable_call(1)`, with `answers = not_executed()`. Instead of the first `claim_round`, use `fx.probe.arm(AgentDispatchWindow::AfterStarted)` and `assert!(fx.one_pass().await.died)`. The row is `(Dispatching, 0)`.
    - The late claim: `(invoked, lapsed) == (0, 1)`, `answers.queries() == 1`, and the row is still `(Dispatching, 0)`.
    - The next `claim_round`: the row is `(Exhausted, 1)`, `answers.queries() == 2`, and `fx.tools.invocation_count(TOOL) == 0`. The effect's status is `Exhausted`.
  - `a_late_unknown_finding_on_a_winding_down_run_parks_nothing`:
    - Setup: `failing_reconcileable_call(3)`, with `answers = unknown()`. After the first `claim_round`, `cancel_the_run(&fx)`.
    - The late claim: `(lapsed, parked_indeterminate) == (1, 0)`, the row is still `(Failed, 1)`, and the effect's status is not `Indeterminate`.
    - The next `claim_round`: `parked_indeterminate == 1`, and the effect's status is `Indeterminate`.

- [ ] **Step 3: Write the guard in `dispatch_attempt_lease.rs`:** `a_finding_inside_a_renewed_lease_is_acted_on` (Review Focus 1).
  - Setup: `world_of(&[TOOL, OTHER_TOOL], 1, Some(40_000), 40_000)`, `ready`, and `calls.answers` set to `"call-1"`, as `a_renewed_claim_makes_the_attempts_its_renewed_lease_covers` does. Call one answers at 40 s, and its settle renews call two's lease to 100 s. Call two's only attempt ends unknown at 80 s, after the 60 s the batch was leased to, and the reconciler answers "not executed" then.
  - Assert the pass has `(invoked, lapsed) == (2, 0)`, and call two's row is `(Exhausted, 1)`. Call two's effect is `fx.effect_at(2)`; confirm the slot against `ready`.
  - This passes before the fix and must stay green after it. Under a check that read the claim's handed expiry, it would end with `lapsed == 1` and the row `(Dispatching, 0)`.

- [ ] **Step 4: Run the three files and check the failures.** Run `cargo test -p rakka-agent --all-features --test dispatch_refused_before_started --test dispatch_failed_attempt_recovery --test dispatch_attempt_lease --no-fail-fast`.
  - Expected: the 7 new tests from Steps 1–2 fail. Each fails on a pass that ended in `claim-fenced`, which panics at the pass's `expect`. Every other test passes, the Step 3 guard included.
  - Record how each failed in the ledger.

- [ ] **Step 5: Implement the change.**
  - In `reconcile_ambiguous`, between `let finding = reconciler.reconcile(&protocol, scope, intent).await?;` and `match finding`, add `if !self.fleet.is_current_claim_for(&claim, 0)? { return Ok(ClaimStep::Concluded(ClaimConclusion::OutOfLease)); }`.
  - Put a comment above it. It says that the reconciler can answer after the lease lapsed, when another worker may hold the ticket and have the attempt out, so the claim acts on no finding then (issue #212). It also says why the check asks "current" and not "covers".
  - In the "not executed" arm:
    - Remove `first_follows`.
    - The check becomes `if another_follows && !self.fleet.is_current_claim_for(&claim, intent.timeout_ms.unwrap_or(0))?`, with `asked_at` still read first.
    - Rewrite the comment block. Drop "An attempt that spends the budget … settled here as before" (#171's exception, now gone). Move #195's paragraph to the new check's comment.
  - Update the docs of `reconcile_ambiguous`, `settle_unknown_finding`, `AgentDispatchPass::lapsed` and `ClaimConclusion::OutOfLease` to say that every late finding is counted and left to the claim that follows. In `settle_unknown_finding`, "which is what keeps the queries inside the budget" now holds for findings that come inside the lease.

- [ ] **Step 6: Run the three files, then the whole crate.**
  - Run the Step 4 command. Expected: every test passes.
  - Then run `cargo test -p rakka-agent --all-features --no-fail-fast > <ledger dir>/crate.log 2>&1`. Expected: no failures.
  - An older test may fail because it pinned a late finding being acted on, #171's exception among them. Turn it around, or re-home it if its setup relied on that behaviour. Read its doc first, and ledger each as a `Ruling:`.

- [ ] **Step 7: Run clippy and commit.**
  - Run `cargo clippy -p rakka-agent --all-targets --all-features -- -D warnings`. Expected: clean.
  - Commit `dispatch.rs` and the three test files: "A claim checks its lease right after the reconciler answers (issue #212)". The body names the defect, the change, the folded #195 check, #171's exception turned around, and the tests. End with the `Co-Authored-By` line.

---

### Task 2: Documents, breaks, validation

**Files:**
- Modify `docs/rakka-agents.md`:
  - around `:199-203`: the `Unknown` burn bounds the questions for answers that come inside the lease;
  - around `:391`: remove #171's exception;
  - around `:394`: widen #195's bullet to every finding (#212), with the cost and the window that stays open (design §4 call 5).
- Modify `docs/rakka-compatibility.md`: a #212 entry after #207's, before "Run the default compatibility matrix". Use #207's shape: what was wrong, the rule, what a deployment sees, the cost, not changed, mixed builds.
- Modify `CHANGELOG.md`: a first bullet under `### Fixed`.

**Interfaces:**
- Consumes: Task 1's tests, as the assertions that hold each sentence of "what a deployment sees".
- Produces: nothing.

- [ ] **Step 1: Write the documents.** In the commit message, list the assertion that holds each sentence of "what a deployment sees".

- [ ] **Step 2: Run the breaks one at a time, restoring the file from the index after each.** Stage `dispatch.rs` first. Run each against `--test dispatch_refused_before_started --test dispatch_failed_attempt_recovery --test dispatch_attempt_lease`. Expected: each break is caught by at least the tests named.
  1. **The check removed:** all 7 new tests.
  2. **The check made for `Unknown` only:** `a_late_executed_finding_is_left_to_the_claim_that_follows` and `a_late_not_executed_with_no_budget_left_is_settled_by_the_claim_that_follows`.
  3. **The check skips `Executed`:** `a_late_executed_finding_is_left_to_the_claim_that_follows`. This is the design's "moved after the delivery", since the `Executed` arm delivers before anything else.
  4. **#171's exception restored:** the check is skipped for a "not executed" finding where no attempt follows, that is, neither `ambiguity.under_this_claim && attempt < intent.max_attempts` nor `!ambiguity.under_this_claim && (started < attempt || attempt < intent.max_attempts)`. Caught by `a_late_not_executed_with_no_budget_left_is_settled_by_the_claim_that_follows`.

- [ ] **Step 3: Validate.**
  - Run `scripts/validate.sh > <ledger dir>/validate.log 2>&1; echo $?`. Expected: `0`.
  - Run `scripts/package-check.sh > <ledger dir>/package.log 2>&1; echo $?`. Expected: `0`.

- [ ] **Step 4: Commit.** "Document the lease check after the reconciler answers (issue #212)". The body maps each "what a deployment sees" sentence to its test, and lists the breaks and the validation results. End with the `Co-Authored-By` line.
