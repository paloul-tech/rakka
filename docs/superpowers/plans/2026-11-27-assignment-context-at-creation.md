# Issue #224: A Run Records Its Trace Context Only at Creation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A run's trace context is the context of the assignment that created it, and a refused or repeated assignment leaves it unchanged.

**Architecture:** `accept_assignment`'s creating branch (`crates/rakka-agent/src/run.rs:1981`) records the envelope's context into the new run's loop state, after the run passes its size check. The write at the end of the run participant's `apply` (`run.rs:8100`) is removed, and its comment moves to the new site.

**Tech Stack:** Rust 1.88 workspace; crate `rakka-agent` only.

**Spec:** `docs/superpowers/specs/2026-11-27-assignment-context-at-creation-design.md`

## Global Constraints

- No durable shape change: no field, schema version or error code.
- Admission is unchanged: the context is recorded after `run.check_bounds(AGENT_RUN_STATE_GROWTH_RESERVE_BYTES)` passes, never before.
- Only the `Assignment` exchange records a context, and only when it creates the run.
- `missing_docs` is warned, and validation runs clippy with `-D warnings`. Never add `-D warnings` to the doc build.
- Run `scripts/validate.sh > <file> 2>&1; echo "exit=$?"`. Never pipe it.
- `cargo clean` between tasks.
- Commits end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Nothing is published or pushed without the owner's word.

## Review Focus

1. **A run created without a context.** A later assignment carrying one, whether a replay or a refusal, gives the run none: an entity never invents or adopts a context (specification 17.5). Owned by Task 1, Step 1.
2. **A replay past the journal window actually re-runs the transition.** The test must show the reply was applied, not answered from the journal, or it proves nothing. Owned by Task 1, Step 1.
3. **The first acceptance still records the context.** Trace scenario 23 (`tests/trace_scenarios.rs:90`) must pass unchanged. Owned by Task 1, Step 4.
4. **A refusal delivered to a parked run.** The settle pass it triggers derives the park identity from the unchanged context, so the later resolution links the exported `wait` span. Owned by Task 1, Step 1.
5. **Admission.** A run that fits today still fits: the record's size check sees exactly what it saw before. Owned by Task 1, Step 3.

---

### Task 1: Record the context at creation

**Files:**
- Modify: `crates/rakka-agent/src/run.rs:1981-2074` (`accept_assignment`) and `:8092-8107` (the tail write and its comment)
- Modify: `crates/rakka-agent/tests/common/mod.rs` (two helpers; `forget_exchanges` moves here from `child_wait_segments.rs`)
- Create: `crates/rakka-agent/tests/assignment_context.rs`
- Modify: `crates/rakka-agent/tests/child_wait_segments.rs` (one test; uses the moved `forget_exchanges`)

**Interfaces:**
- Produces, in `tests/common/mod.rs`:
  - `pub fn assignment_envelope(fixture: &Fixture, generation: AgentAssignmentGeneration) -> AgentExchangeEnvelope`, modelled on `child_result_envelope`. The payload is an `AgentRunAssignment` with:
    - `task: task_scope()`, `run: run_scope()` (the generation-1 run, whatever `generation` is), `generation`;
    - `definition: task_definition()`, `input: AgentTaskContent::inline(json!({ "ticket": 1 }))`, `goal: None`;
    - `budget: AgentBudgetGrant::unbounded()`, both revisions `AgentRevisionNumber::new(1)`, `delegation: None`, `assigned_at: fixture.now()`.

    The envelope's operation id is `assignment_operation_id(&task_scope(), generation)`, from `AgentEntityAddress::Task(task_scope())` to `AgentEntityAddress::Run(run_scope())`. It carries no context; a test adds one with `.with_telemetry(..)`.
  - `pub async fn forget_exchanges(fixture: &Fixture)`, moved unchanged from `child_wait_segments.rs:342`.
  - `pub async fn run_context(fixture: &Fixture) -> AgentTelemetryContext`: the run's `loop_state().telemetry()`, read from a freshly recovered facade.

- [ ] **Step 1: Write the failing tests.** In `assignment_context.rs` (`mod common;`), using `Fixture::new(ScriptedDispatcher::new())`, `instantiate_agent`, and `create_task_traced(context(INGRESS_PARENT))` or `create_task`. Deliver through a facade of its own (`fixture.run()`, `recover`, `accept(&envelope, &fixture.router, fixture.now())`).
  - `a_refused_generation_leaves_the_runs_trace_context`:
    - the run's context after creation is `INGRESS_PARENT`'s;
    - `assignment_envelope(&fx, AgentAssignmentGeneration::new(2)).with_telemetry(context(OTHER_PARENT))` is answered `Rejected { code: "run-generation-conflict", .. }`;
    - `run_context(&fx)` equals the context before.
  - `the_same_generation_sent_again_leaves_the_runs_trace_context`:
    - `forget_exchanges(&fx)` first;
    - `assignment_envelope(&fx, AgentAssignmentGeneration::new(1)).with_telemetry(context(OTHER_PARENT))` is answered `Accepted`, and `!reply.is_replayed()` (Review Focus 2);
    - the context is unchanged.
  - `a_run_created_without_a_context_takes_none_from_a_later_assignment` (Review Focus 1):
    - the task is created with `create_task()`, so the run's context is empty;
    - after `forget_exchanges`, a generation-1 re-send under `OTHER_PARENT`, then a generation-2 one under `OTHER_PARENT`;
    - the run's context is still `AgentTelemetryContext::default()` after each.
  - In `child_wait_segments.rs`, `a_refused_assignment_between_the_park_and_the_resolution_keeps_the_park_link` (Review Focus 4):
    - `parked_fan_out`, then a generation-2 assignment under `HOST_PARENT`, refused;
    - then both children's results, as `the_last_childs_result_resolves_the_wait_and_links_the_park_and_the_child` delivers them;
    - the one `wait-resolve` segment's `parked-wait` link names the `wait` segment's `span_id`.

  Constants: `INGRESS_PARENT` as in `child_wait_segments.rs`, and `OTHER_PARENT = "00-4ef7651916cd43dd8448eb211c8031a0-f7ad6b7169203335-01"`.

- [ ] **Step 2: Run them to verify they fail.**
  Run: `cargo test -p rakka-agent --all-features --no-fail-fast --test assignment_context --test child_wait_segments`
  Expected: the four new tests FAIL, each on its context assertion (the run took `OTHER_PARENT` or `HOST_PARENT`) or, for the child wait, on the link naming a different span. Every existing `child_wait_segments` test passes. If any new test passes, it is not reaching `apply`; find why before going on.

- [ ] **Step 3: Implement.**
  - In `accept_assignment`'s creating branch, after `run.check_bounds(..)` succeeds and before `state.run = Some(run)`, record `envelope.telemetry().clone()` into `run.loop_state` through `record_telemetry` when `envelope.has_telemetry()`. Bind `run` mutably for it.
  - Delete the tail write at `run.rs:8100-8105`. Move its comment to the new site, reworded: the context is recorded when the assignment creates the run. A refusal or a repeat of the assignment leaves it as it was, and a context-less exchange leaves it in place (specification 17.5). A child's delegation result carries the child's context, which links into the parent's spans rather than replacing them (specification 17.4).
  - Leave `AgentExchangeTransition::new(result)` as the arm's return.

- [ ] **Step 4: Run them to verify they pass**, plus the suites that read the run's context.
  Run: `cargo test -p rakka-agent --all-features --no-fail-fast --test assignment_context --test child_wait_segments --test trace_scenarios --test telemetry_context --test goal_view`
  Expected: PASS, including `ingress_context_flows_to_every_effect_and_survives_owner_loss` (Review Focus 3).

- [ ] **Step 5: Falsify.** Restore the tail write alone (keep the new site) and run Step 4's command. The four new tests must fail, and `trace_scenarios` must pass. Then remove the new site alone (no write anywhere): `ingress_context_flows_to_every_effect_and_survives_owner_loss` must fail. Restore.

- [ ] **Step 6: Lint and commit.**

```bash
cargo fmt --all && cargo clippy -p rakka-agent --all-targets --all-features -- -D warnings
git add crates/rakka-agent
git commit -m "Record a run's trace context only when its assignment creates it (issue #224)"
```

  Then `cargo clean`.

### Task 2: Docs and validation

**Files:**
- Modify: `CHANGELOG.md` (`## Unreleased`: a new `### Fixed` between `### Changed` and `### Validation`, and a line under `### Validation`)
- Modify: `docs/rakka-compatibility.md` (a new entry after issue #71 item 4's; narrow the #70 entry's cause at `:1200`)
- Modify: `docs/rakka-agent-observability-catalogue.md:348-354`
- Modify: `docs/superpowers/specs/2026-11-27-assignment-context-at-creation-design.md` (an "As built" section)

- [ ] **Step 1: CHANGELOG.** Under `### Fixed`, one `rakka-agent` entry (issue #224):
  - a refused or repeated assignment no longer replaces a run's trace context;
  - the context is recorded only when the assignment creates the run;
  - neither path was reachable through upstream's own task entity (a run's id names its generation, and a re-driven assignment is the stored envelope), so this is hardening;
  - records written before keep the context they hold;
  - no durable field, schema version or error code is added.

  Under `### Validation`, the `assignment_context.rs` suite.

- [ ] **Step 2: Compatibility.**
  - **The new entry:** what changes, why it was unreachable, what a deployment sees (nothing, through upstream's own composition), and mixed builds. Nothing durable changes shape; an older build still records a later assignment's context, and only a sender that builds its own assignment exchanges can reach that.
  - **The #70 entry:** "or under a context a later `Assignment` rewrote" becomes "or under a context a later `Assignment` rewrote, in a record written before issue #224's fix".

- [ ] **Step 3: The catalogue.** The same narrowing in the "A link can point at a span nobody exported" paragraph. Keep the paragraph's last sentence true: storing the identity would still remove only the first cause.

- [ ] **Step 4: The design's "As built".** Every ruling from the ledger, the tests that hold each proof row, and the falsification results.

- [ ] **Step 5: Validate.**
  Run: `scripts/validate.sh > <scratchpad>/validate.txt 2>&1; echo "exit=$?"`. If a workspace test is killed under load, re-run that crate alone.
  Expected: `exit=0`, and no warning lines in the output.

- [ ] **Step 6: Commit.**

```bash
git add CHANGELOG.md docs
git commit -m "Document that a run's trace context is recorded only at creation (issue #224)"
```

  Then `cargo clean`.
