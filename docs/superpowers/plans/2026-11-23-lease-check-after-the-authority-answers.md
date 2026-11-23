# A claim checks its lease right after the authority answers — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every dispatch-authority answer that comes after the claim's lease lapsed (grants included) ends the claim out of lease before anything is written (issue #207).

**Architecture:** `authorize_attempt` in `crates/rakka-agent/src/dispatch.rs` checks `is_current_claim_for(claim, 0)` after it closes the `tool-authorize` segment, and answers "out of lease" instead of the decision. Its two callers, `attempt_invocation` and `defer_before_reconciling`, end the claim as `ClaimConclusion::OutOfLease`.

**Tech Stack:** Rust 1.88 (MSRV), tokio tests, `rakka-agent` test fixtures.

**Spec:** `docs/superpowers/specs/2026-11-23-lease-check-after-the-authority-answers-design.md`

## Global Constraints

- **The check asks only that the lease is still current:** `is_current_claim_for(claim, 0)`. Never `timeout_ms`.
- **An authority error keeps its handling.** It propagates from `attempt_invocation`, and it answers `None` from `defer_before_reconciling`.
- **`settle_refused_before_started` keeps its own check.**
- **No new durable field, status, error code or decision kind.**
- **Workspace lints:** every public item has a doc comment, and `cargo clippy ... -D warnings` is clean.
- **Test runs:** use one feature set throughout, `cargo test -p rakka-agent --all-features ...`. The target directory was cleaned on 2026-11-23, so the first build takes 30–50 minutes. Run crate-wide with `--no-fail-fast`. Never pipe `scripts/validate.sh` into `tail`; redirect it to a file and capture `$?`.

## Review Focus

1. **A late grant on the retry of a counted lost attempt** (idempotent). The claim that counted the loss ends out of lease, and the next claim makes the retry exactly once. Test: Task 1, `a_late_grant_on_the_retry_of_a_counted_lost_attempt_leaves_it_to_the_next_claim`.
2. **An answer inside a renewed lease is still acted on.** The check reads the fleet's lease, which a batch's settles renew, not the expiry the claim was handed. Test: the existing guard `a_refusal_inside_a_renewed_lease_is_delivered` now passes through the new check first. Task 1 keeps it green.

I checked and found no other likely input: a late transient refusal on #195's path reaches the same check as a late grant there.

---

### Task 1: The check and its tests

**Files:**
- Modify `crates/rakka-agent/src/dispatch.rs`:
  - `authorize_attempt` (around `:4111`), and its two callers: `defer_before_reconciling` (around `:4158`) and `attempt_invocation`, where it asks the authority (around `:4251`).
  - The doc of `settle_refused_before_started` (around `:5515`): the authority's refusals are now checked first, in `authorize_attempt`.
- Modify `crates/rakka-agent/tests/dispatch_refused_before_started.rs`:
  - generalize `OutlastingAuthority` (around `:918`);
  - turn around `a_missing_resolver_refusal_on_a_lapsed_lease_is_told_and_parks_nothing_beside_it` (around `:1497`);
  - add five tests after `a_refusal_inside_a_renewed_lease_is_delivered`.

**Interfaces:**
- Produces, private to `dispatch.rs`:

```rust
/// What a claim learned from the dispatch authority (issue #207).
enum Authorized {
    /// The authority answered while the claim's lease was still current.
    Answered { decision: AgentDispatchDecision, asked_at: AgentTimestampMillis },
    /// The lease lapsed while the authority answered.
    OutOfLease,
}
async fn authorize_attempt(&self, scope: &AgentRunScope, claim: &AgentDispatchClaim,
    state: &AgentRunState, intent: &AgentRunEffect, attempt: u32)
    -> AgentDispatchResult<Authorized>
```

- [ ] **Step 1: Generalize `OutlastingAuthority`.** Add a field `refusal: AgentAuthorityRefusal`. `new` sets it to `tool-revoked`, so existing tests are unchanged. Add `fn refusing(self, refusal: AgentAuthorityRefusal) -> Self`. `authorize` answers `Refused(self.refusal.clone())`.

- [ ] **Step 2: Write the failing tests.** Section comment: "Every late answer (issue #207)". Use the file's helpers: `worker`, `the_fixtures_authority`, `LateAnswer`, `HeldTool`, `OutlastingAuthority`, `a_call_whose_first_attempt_failed`, `reconcileable`. Each test asserts:

```rust
// a_late_grant_makes_no_attempt_beside_another_workers_call
// non_idempotent tool; shape of a_refusal_whose_lease_lapsed_while_another_worker_called_the_tool_is_not_delivered,
// worker one asking LateAnswer { hold_on: 1, hand_off: Some((held, release)) } (a late GRANT)
assert_eq!((one_pass.lapsed, one_pass.invoked, one_pass.delivered, one_pass.cancelled), (1, 0, 0, 0)); // one_pass.expect(..)
assert_eq!((two_pass.invoked, two_pass.delivered), (1, 1));
assert_eq!(tool.calls.load(Ordering::SeqCst), 1, "the tool is called once");
// fx.pump(); run status Completed

// a_late_grant_alone_leaves_the_attempt_to_the_claim_that_follows
// non_idempotent tool, fresh ticket, worker one asking LateAnswer { hold_on: 1, no hand_off }
assert_eq!((first.lapsed, first.invoked), (1, 0));        // first = one.pump_run(..).expect(..)
assert_eq!(fx.tools.invocation_count(TOOL), 0);
// fx.expire_lease(); let second = fx.one_pass().await;
assert_eq!(second.invoked, 1);

// a_late_transient_refusal_ends_the_claim_out_of_lease_not_in_an_error
// non_idempotent tool, fresh ticket, OutlastingAuthority::new(&fx).refusing(AgentAuthorityRefusal::transient("agent-suspended", "the agent is suspended"))
let pass = one.pump_run(&run_scope()).await.expect("the pass runs");   // main: Err(claim-fenced)
assert_eq!((pass.lapsed, pass.deferred, pass.delivered), (1, 0, 0));

// a_late_answer_before_the_reconciler_ends_the_claim_unasked
// a_call_whose_first_attempt_failed(reconcileable(3)); reconciler = Arc::new(ScriptedReconciler::new().with_finding(NotExecuted));
// worker one asking LateAnswer { hold_on: 1 } (the question asked before the reconciler, issue #195), .with_reconciler(reconciler.clone())
assert_eq!(pass.lapsed, 1);
assert_eq!(reconciler.queries(), 0, "the reconciler is not asked");      // main: 1
assert_eq!(fx.tools.invocation_count(TOOL), 1, "only the first attempt ran");

// a_late_grant_on_the_retry_of_a_counted_lost_attempt_leaves_it_to_the_next_claim  (Review Focus 1)
// idempotent(3) tool; fx.probe.arm(AfterStarted); fx.expire_lease(); assert!(fx.one_pass().await.died);
// worker one asking LateAnswer { hold_on: 1 } (its only question: the retry's)
assert_eq!((pass.lapsed, pass.failed_attempts, pass.invoked), (1, 1, 0), "the loss is counted, and no retry is made");
// row (Failed, 1); fx.expire_lease(); let next = fx.one_pass().await;
assert_eq!(next.invoked, 1);
assert_eq!(fx.tools.invocation_count(TOOL), 1);
```

  Turn around the resolver race. Rename it `a_late_grant_ends_the_claim_before_a_missing_resolver_is_met`. Remove "This is the author's call, not the owner's ruling" from its doc, and say instead that the late grant now ends worker one out of lease before `Started`, so worker two makes the retry. Keep its setup and hand-offs, and replace the assertions with:

```rust
let one_pass = one_pass.expect("worker one's pass runs");
assert_eq!((one_pass.lapsed, one_pass.delivered, one_pass.cancelled), (1, 0, 0));
let two_pass = two_pass.expect("worker two's pass runs");
assert_eq!((two_pass.invoked, two_pass.failed_attempts, two_pass.parked_indeterminate), (1, 1, 0));
assert_eq!(fx.tools.invocation_count(TOOL), 2);
// row (Failed, 2); run terminal_reason None
```

- [ ] **Step 3: Run the tests and check that they fail.**

Run: `cargo test -p rakka-agent --all-features --test dispatch_refused_before_started > <log> 2>&1`
Expected:
- the five new tests and the turned-around one fail. Record how each fails in the ledger: a test that fails at setup is not a red;
- every other test in the file, the renewed-lease guard included, passes.

- [ ] **Step 4: Implement the change.**
  - Add `Authorized`, and change `authorize_attempt` to take the claim. Close the segment exactly as now, then `let decision = authorized?;`. If `!self.fleet.is_current_claim_for(claim, 0)?`, answer `Authorized::OutOfLease`; otherwise answer `Answered`.
  - In `attempt_invocation`, `OutOfLease` returns `Ok(ClaimStep::Concluded(ClaimConclusion::OutOfLease))`.
  - In `defer_before_reconciling`, `Ok(OutOfLease)` returns `Ok(Some(ClaimConclusion::OutOfLease))`. Its other arms are unchanged: a transient refusal defers, and anything else, an error included, answers `None`.
  - Doc comments:
    - On `authorize_attempt`, say why it checks: a late answer, grants included, would act beside another worker's claim. Name the accepted cost: the lease has to cover the authority's answer.
    - On `defer_before_reconciling`: a late answer ends the claim unasked.
    - On `settle_refused_before_started`: the authority's refusals reach it already checked, and its own check is for the policy-conflict refusal, which asks no authority.
  - Fix the stale comment at the `credential-resolver-missing` site, if one says a late grant can reach it.

- [ ] **Step 5: Run the file's tests, then the whole crate.**

Run: `cargo test -p rakka-agent --all-features --test dispatch_refused_before_started`
Expected: all pass.

Run: `cargo test -p rakka-agent --all-features --no-fail-fast > <log> 2>&1; echo $?`
Expected: `0`. If another test fails, it is one that pinned a late answer acting on a lapsed lease. Read it before changing it, list it in the commit, and treat any other failure as a defect in this change.

- [ ] **Step 6: Run clippy and commit.**

Run: `cargo clippy -p rakka-agent --all-targets --all-features -- -D warnings`
Expected: clean.

```bash
git add crates/rakka-agent/src/dispatch.rs crates/rakka-agent/tests/dispatch_refused_before_started.rs
git commit -m "A claim checks its lease right after the dispatch authority answers (issue #207)"
```

---

### Task 2: Documents, breaks, validation

**Files:**
- Modify `docs/rakka-agents.md`:
  - The bullet "**A refusal before `Started` is told to the run only under a current lease** (issue #196)" (around `:412`). Widen it to every answer, grants included, and to #195's question. Keep "the lease has to cover the authority's answer", now for every dispatch. Replace "A transient refusal is not asked … `credential-resolver-missing`" with what now happens: a late transient refusal ends out of lease too; a late grant never reaches `credential-resolver-missing`; a worker that stalls between the check and its write can still act.
  - The limitation "A worker that stalls past its lease between that check and its durable `Started`…" (around `:582`). Narrow it to the stall between the check after the authority's answer and the `Started` write.
- Modify `docs/rakka-compatibility.md`: a new entry after issue #206's, before "Run the default compatibility matrix". Use #196's sections: what was wrong, the rule, what a deployment sees (from design §5), mixed builds, not changed.
- Modify `CHANGELOG.md`: a new first bullet under `### Fixed`.

- [ ] **Step 1: Write the documents.** List, in the commit message, the assertion that holds each sentence of "What a deployment sees".

- [ ] **Step 2: Run the breaks one at a time, restoring the file from the index after each.** Stage `dispatch.rs` first. Run each against `--test dispatch_refused_before_started --test dispatch_failed_attempt_recovery --test effect_dispatch`.

| Break | Expected to fail |
|---|---|
| `authorize_attempt` never answers `OutOfLease` | the five new tests and the turned-around one |
| `defer_before_reconciling` maps `OutOfLease` to `None` | `a_late_answer_before_the_reconciler_ends_the_claim_unasked` |
| the check runs only for a grant | `a_late_transient_refusal_ends_the_claim_out_of_lease_not_in_an_error` |

A break that compiles and fails nothing is a gap: add a test for it, or say why it can't be caught. A break that doesn't compile is not a catch.

- [ ] **Step 3: Validate.**

Run: `scripts/validate.sh > <log> 2>&1; echo $?`, then `scripts/package-check.sh > <log> 2>&1; echo $?`
Expected: `0` and `0`.

- [ ] **Step 4: Commit.**

```bash
git add docs/rakka-agents.md docs/rakka-compatibility.md CHANGELOG.md
git commit -m "Document the lease check after the authority answers (issue #207)"
```
