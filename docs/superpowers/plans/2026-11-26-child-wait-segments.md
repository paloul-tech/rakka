# Child-wait segments (issue #70) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** a run parked on its children closes a `wait` segment, and the transition that resolves the group closes a `wait-resolve` segment. The resolution links the park and the trigger. `run-resume` keeps its rule and carries the same links when it closes.

**Architecture:**

- **Segment classes.** Two new `AgentSegmentOperation` variants, mapped to `rakka.agent.run.wait` and `rakka.agent.run.wait.resolve`.
- **Identity.** The park's identity is derived from the run's trace context and the fan-in group's `(opened_turn, opened_at)` by one function. It is stored nowhere.
- **Detection.** One before/after comparison of "parked on G" runs at each of the three points where a run transition commits: the command arm in `apply_command`, the exchange acceptance in `accept`, and each crank step. It closes `wait` on entry and `wait-resolve` on exit.
- **Commands.** `RecordWorkflowResult` and `FireFanInDeadline` gain an optional `telemetry`, so a host's span can be the resolution's `resume-request`.

**Tech Stack:** Rust 1.88, `rakka-agent` (`run.rs`, `observability.rs`, `otel.rs`, `fan_in.rs`), tokio tests.

**Spec:** `docs/superpowers/specs/2026-11-26-child-wait-segments-design.md`

## Global Constraints

- **No durable field.** Nothing is added to the run, the loop state or the fan-in group; spec section 3. Segments are never read by any state, decision or status.
- **The `run-resume` tally must not move.** Every `run-resume` the crate's tests close, counted per test, is the same before and after; spec section 3, last anti-goal. Task 1 takes the baseline before any source change.
- **Wire compatibility.** The new command fields are `#[serde(default)]`. Do not add `deny_unknown_fields` to any command.
- **Workspace lints.** `missing_docs` applies, so every public item gets a doc comment. `cargo clippy -p rakka-agent --all-targets --all-features -- -D warnings` stays clean.
- **Future size (issue #162).** `apply_command`, `accept` and the crank are measured futures. Hold no new large value across an `.await`: read the marks into a small `Copy` struct, and drop references before awaiting.
- **Docs.** Never add `-D warnings` to the doc build. Check new doc comments with `cargo doc -p rakka-agent --no-deps` locally.
- **Disk.** `cargo clean` at every task boundary, after the commit. Run tests per crate, never the one-shot workspace run.
- **`scripts/validate.sh`.** Redirect it to a file and read `$?` directly, never through a pipe.
- **Commits** end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. **A command that resolves a checkpoint and the group in one transition.** An approval on a member's send is denied under an `Any` policy, the send fails, and the group resolves. Expected: `checkpoint-resolve` is as today; `wait-resolve` links `parked-wait` and the command's `telemetry` as `resume-request`; a `run-resume`, if it closes, carries `parked-checkpoint`, `parked-wait` and one `resume-request`. Owned by Task 4, Step 6.
2. **The stale-refusal retry in `apply_unboxed`.** A refusal is re-applied after `rematerialize`. A park or resolution must close once, not twice. The marks are read inside `apply_command`, which the retry calls again. Owned by Task 3, Step 4.
3. **A settle pass that fails after the resolution committed.** The reply is `Err`. `wait-resolve` still closes, because the comparison runs before the settle pass. Owned by Task 4, Step 5.
4. **A re-applied `DelegationResult` past the journal window.** It re-runs the transition with first-writer-wins and must not close a second `wait-resolve`: the run is no longer parked on G. Owned by Task 4, Step 4.
5. **A run parked by an earlier build.** It resumes with a `parked-wait` link to a span nobody exported. This is accepted and documented; spec 5.8. Owned by Task 5's docs.

---

### Task 1: Baseline tally, the two classes, and the park's identity

**Files:**
- Create (not committed): `.superpowers/sdd/2026-11-26-child-wait-segments/tally.patch`
- Modify: `crates/rakka-agent/src/observability.rs` (`AgentSegmentOperation` at about `:1844`, `as_label` at about `:1970`)
- Modify: `crates/rakka-agent/src/otel.rs` (`AgentGenAiOperation` at about `:470`, `span_name`, `span_kind`, the `AgentSegmentOperation` → `AgentGenAiOperation` map at about `:659`)
- Modify: `crates/rakka-agent/src/fan_in.rs` (`impl AgentFanInCell`)
- Test: `crates/rakka-agent/tests/otel_span_mapping.rs`, and a unit test in `fan_in.rs`

**Interfaces:**
- Produces:
  - `AgentSegmentOperation::Wait { kind: &'static str }`, `as_label()` = `"wait"`
  - `AgentSegmentOperation::WaitResolve { kind: &'static str }`, `as_label()` = `"wait-resolve"`
  - `pub const AGENT_WAIT_KIND_CHILDREN: &str = "children";` in `observability.rs`, exported from `lib.rs`
  - `AgentGenAiOperation::RunWait` and `::RunWaitResolve`: span names `rakka.agent.run.wait` and `rakka.agent.run.wait.resolve`, both `AgentOtelSpanKind::Internal`
  - `impl AgentFanInCell { pub fn parked_span_identity(&self, run_context: &AgentTelemetryContext) -> Option<AgentTraceContext> }` = `agent_durable_span_identity(run_context, &["wait", "children", &self.opened_turn.to_string(), &self.opened_at.as_millis().to_string()])`

- [ ] **Step 1: Write the tally patch and take the baseline.**
  - **The patch.** Make `InMemoryAgentSegmentSink::record` (`observability.rs`, about `:2447`) do one extra thing when the env var `RAKKA_SEGMENT_TALLY` is set: append one line `"{thread name}\t{operation label}\n"` to that file, as one `write_all` on a file opened with `OpenOptions::new().create(true).append(true)`. The thread name is `std::thread::current().name()`, falling back to `"?"`.
  - Save it with `git diff > <workspace>/tally.patch`, then `git checkout -- crates/rakka-agent/src/observability.rs`.
  - **The baseline.** Apply the patch and run, in the background: `RAKKA_SEGMENT_TALLY=<workspace>/tally-before.txt cargo test -p rakka-agent --all-features > <workspace>/tally-before-run.txt 2>&1; echo "exit=$?"`. Then revert the patch.
  - Expected: `exit=0`, and a tally file with `run-resume` lines in it. Do not edit `src/` while it runs.

- [ ] **Step 2: Write the failing tests.**
  - In `otel_span_mapping.rs`: the two classes map as follows.

    ```rust
    assert_eq!(map(AgentSegmentOperation::Wait { kind: AGENT_WAIT_KIND_CHILDREN }).span_name(), "rakka.agent.run.wait");
    assert_eq!(map(AgentSegmentOperation::WaitResolve { kind: AGENT_WAIT_KIND_CHILDREN }).span_name(), "rakka.agent.run.wait.resolve");
    // both span_kind() == AgentOtelSpanKind::Internal; as_label() "wait" / "wait-resolve"
    ```

    Use whatever mapping call the file already uses for `CheckpointOpen`. If it holds an exhaustive list of operations (about `:1371`), add both there.
  - In `fan_in.rs`'s tests: `parked_span_identity` is `Some` and equal across two calls for a traced context. It changes when `opened_turn` or `opened_at` changes. It is `None` for `AgentTelemetryContext::default()`.

- [ ] **Step 3: Run the tests and watch them fail.**
  Run: `cargo test -p rakka-agent --all-features --test otel_span_mapping` and `cargo test -p rakka-agent --all-features --lib fan_in`
  Expected: compile errors naming the missing variants and method.

- [ ] **Step 4: Implement the Interfaces block.** Follow the compiler through every exhaustive match on `AgentSegmentOperation` and `AgentGenAiOperation`. Doc comments say:
  - `Wait` ends at the durable park;
  - `WaitResolve` is the resolving transition, the child wait's counterpart to `CheckpointResolve`.

- [ ] **Step 5: Run the tests and watch them pass.**
  Run: the Step 3 commands.
  Expected: PASS.

- [ ] **Step 6: Lint, and commit.**
  Run: `cargo fmt --all && cargo clippy -p rakka-agent --all-targets --all-features -- -D warnings`
  Expected: clean.

  ```bash
  git add crates/rakka-agent/src crates/rakka-agent/tests/otel_span_mapping.rs
  git commit -m "Add the wait and wait-resolve segment classes and the park's derived identity (issue #70)"
  ```

  Then `cargo clean`.

---

### Task 2: A trace context on `RecordWorkflowResult` and `FireFanInDeadline`

**Files:**
- Modify: `crates/rakka-agent/src/run.rs` (the two variants at about `:10754` and `:10796`, `record_workflow_result` at about `:10912`, and the two arms in `apply_command`)
- Modify: every test or example that builds either variant as a literal. Today `FireFanInDeadline {` appears in `cancellation_propagation.rs`, `workflow_tool.rs`, `run_decision_ledger.rs`, `goal_evaluation.rs`, `run_timer_unchanged.rs` and `fan_out_fan_in.rs`. `RecordWorkflowResult {` has 4 sites; find them with `grep -rn`.
- Test: `crates/rakka-agent/tests/child_wait_segments.rs` (new)

**Interfaces:**
- Produces:
  - `RecordWorkflowResult { …, #[serde(default)] telemetry: AgentTelemetryContext }`
  - `FireFanInDeadline { operation_id, #[serde(default)] telemetry: AgentTelemetryContext }`
  - `impl AgentRunEntityCommand { pub fn with_telemetry(self, telemetry: AgentTelemetryContext) -> Self }`, which sets the field on those two variants (sanitized through `sanitize_agent_telemetry_context`) and returns any other variant unchanged
  - `pub(crate) fn trigger_telemetry(&self) -> Option<&AgentTelemetryContext>` on the command. It returns the field for `RecordWorkflowResult`, `FireFanInDeadline`, `ResolveCheckpoint` and `ResolveIndeterminateEffect`, and `None` for every other variant. Task 4 consumes it.
- `record_workflow_result(...)` keeps its signature and sets an empty context.

- [ ] **Step 1: Write the failing test** `commands_without_a_trace_context_decode_to_an_empty_one` in `child_wait_segments.rs`:

  ```rust
  // JSON literals, written by hand, with no "telemetry" key:
  // {"fire-fan-in-deadline":{"operation_id":<a valid id>}} and a full record-workflow-result
  // each decodes; the field reads AgentTelemetryContext::default().
  // A command built with .with_telemetry(context(PARENT)) encodes a "telemetry" key and
  // round-trips to the same command.
  ```

  Get each variant's exact JSON by encoding one built with the constructor at `main`, then deleting the key. Write the literal into the test, so the test does not depend on today's encoder.

- [ ] **Step 2: Run it and watch it fail.**
  Run: `cargo test -p rakka-agent --all-features --test child_wait_segments`
  Expected: compile error (no `with_telemetry`), or a field-count mismatch.

- [ ] **Step 3: Implement the Interfaces block.** Update every literal site; `cargo test -p rakka-agent --all-features --no-run` finds them. The two `apply_command` arms bind `telemetry: _` until Task 4 reads it.

- [ ] **Step 4: Run it and watch it pass.**
  Run: `cargo test -p rakka-agent --all-features --test child_wait_segments --test fan_out_fan_in --test workflow_tool`
  Expected: PASS.

- [ ] **Step 5: Lint, and commit.** Use Task 1 Step 6's commands.

  ```bash
  git commit -am "Give RecordWorkflowResult and FireFanInDeadline an optional trace context (issue #70)"
  ```

  Then `cargo clean`.

---

### Task 3: The park closes `wait` on every committed transition

**Files:**
- Modify: `crates/rakka-agent/src/run.rs`:
  - `AgentSegmentMarks` and `segment_marks` (about `:11660`);
  - `apply_command` (about `:8913`);
  - `accept` (about `:9222`);
  - the crank's per-transition close (about `:9905`).
- Modify: `crates/rakka-agent/src/observability.rs` and `otel.rs`, for the attribute keys.
- Modify: `crates/rakka-agent/tests/common/mod.rs`, for a traced fan-out creation helper.
- Test: `crates/rakka-agent/tests/child_wait_segments.rs`

**Interfaces:**
- Consumes from Task 1: `AgentSegmentOperation::Wait`, `AGENT_WAIT_KIND_CHILDREN`, `AgentFanInCell::parked_span_identity`.
- Produces:
  - `fn parked_group(loop_state: &AgentLoopState) -> Option<(u64, AgentTimestampMillis)>`. It is `Some((opened_turn, opened_at))` exactly when `phase() == AwaitingChildren && awaits_fan_in()`.
  - `AgentSegmentMarks` gains `parked: Option<(u64, AgentTimestampMillis)>`, filled by `segment_marks`.
  - `fn wait_change(loop_state, marks) -> AgentWaitChange { parked: Option<(u64, AgentTimestampMillis)>, resolved: Option<(u64, AgentTimestampMillis)> }`:
    - `parked` = the group after, when after is `Some(g)` and `marks.parked != Some(g)`;
    - `resolved` = `marks.parked`, when it is `Some(g)` and after `!= Some(g)`.
  - `fn close_wait_changes(&self, change: AgentWaitChange, trigger: Option<&AgentTelemetryContext>, timer: AgentSegmentTimer, now)`. Task 3 implements the `parked` half; Task 4 adds the `resolved` half.
  - Attribute constants in `observability.rs`, aliased in `otel.rs` and added to `AGENT_SPAN_ATTRIBUTE_KEYS`:
    - `SEGMENT_ATTR_WAIT_KIND` = `"rakka.agent.wait.kind"`
    - `SEGMENT_ATTR_FAN_IN_POLICY` = `"rakka.agent.fan_in.policy"`
    - `SEGMENT_ATTR_FAN_IN_RESOLUTION` = `"rakka.agent.fan_in.resolution"`

    Reuse an existing key instead, if `grep` finds one that already carries the value.
  - In `tests/common/mod.rs`: `pub async fn create_fan_out_task_traced(fixture: &Fixture, policy: Option<AgentFanInPolicy>, telemetry: AgentTelemetryContext)`, which is `create_fan_out_task` with `creation.telemetry` set.

- [ ] **Step 1: Write the failing tests** in `child_wait_segments.rs`. Use `fan_out_fixture(...)` with `.with_segments(sink)` over an `InMemoryAgentSegmentSink`, and the run traced with `create_fan_out_task_traced(…, context(INGRESS_PARENT))`.
  - **`a_turn_awaiting_two_children_parks_one_wait_segment`.** After `fixture.pump()`, `sink.operations()` holds exactly one `"wait"`. That segment has:
    - `rakka.agent.wait.kind` = `children`;
    - `rakka.agent.fan_in.policy` = `all`;
    - a `span_id` equal to `cell.parked_span_identity(&loop_state.telemetry()).span_id`, read from the durable record.
  - **`a_result_that_leaves_the_group_open_parks_nothing_more`.** Deliver the first child's result (`child_result_envelope` + `accept`). Still exactly one `"wait"`, and no `"wait-resolve"`.
  - **`a_run_with_no_trace_context_still_parks`.** Use the untraced `create_fan_out_task`. One `"wait"` with `span_id == None`.
  - **`a_handoff_wait_parks_no_wait_segment`.** Reuse the fixture in `tests/handoff_send_park.rs` (copy the minimum setup, or add a helper to `common`). The run rests `AwaitingChildren` on a handoff with no `"wait"`.
  - **`an_expired_approval_on_a_members_send_parks_through_the_timer`.** The delegation's send is gated by an approval that expires: configure the spec through `AgentEffectPolicies` for the delegation tool. Find how an a2a-send effect takes its spec, and how a checkpoint gets an expiry deadline, in the checkpoint-timer tests (`grep -rln FireCheckpointTimers crates/rakka-agent/tests`). The other member is completed. Send `FireCheckpointTimers` past the deadline. The run rests `AwaitingChildren`, and exactly one `"wait"` closes on that command. If the turn cannot reach that rest through this path, the test records why in its doc comment, and the ledger gets a ruling.

- [ ] **Step 2: Run them and watch them fail.**
  Run: `cargo test -p rakka-agent --all-features --test child_wait_segments`
  Expected: every park test fails, with no `"wait"` closed.

- [ ] **Step 3: Implement.**
  - **Command arms.** In `apply_command`, read `segment_marks` into a local after the replay early-return and before the `match`. Start an `AgentSegmentTimer` there. After the `match` produces `reply` and before `note_committed_transition`, call `close_wait_changes(wait_change(…), command_trigger, timer, now)`. The command was moved into the match, so clone its trigger (`trigger_telemetry().cloned()`) before.
  - **Exchange.** In `accept`, take the marks before `self.host.accept` and compare after it, before `make_local_progress`. The trigger is `envelope.telemetry()`.
  - **Crank.** In the crank step, use the marks it already reads and call it beside `close_checkpoint_open_segments`, with no trigger.
  - **The `wait` segment.** `AgentSegmentTimer::start(now).close(Wait { kind: AGENT_WAIT_KIND_CHILDREN })`, with:
    - the two attributes;
    - `.span_id(identity.span_id)` when the identity is `Some`;
    - `.ok()`;
    - closed with `close_segment` (the run's context, no links).
  - **Every other path.** List every `self.transition(`, `self.host.initiate(` and `self.host.accept(` call in `run.rs`. Each must be inside one of the three points above, or get its own comparison. Write the list into the commit message.

- [ ] **Step 4: Run them and watch them pass, then cover Review Focus item 2.**
  Run: the Step 2 command, then `cargo test -p rakka-agent --all-features --test fan_out_fan_in --test workflow_tool --test handoff_send_park --test telemetry_segments`
  Expected: PASS. Read the stale-refusal retry in `apply_unboxed` and confirm that a retried command reads fresh marks, since they are taken inside `apply_command`. Say so in the commit message.

- [ ] **Step 5: Falsify.** Break each guard in turn, and revert each break:
  - skip the comparison in `apply_command`: the timer test must fail;
  - skip it in the crank: the two-children test must fail, if that park commits in a crank step (otherwise the `apply_command` break fails it; note which in the commit message);
  - drop the `marks.parked != Some(g)` check: the first-result test must fail with a second `"wait"`.

- [ ] **Step 6: Lint, and commit.** Use Task 1 Step 6's commands.

  ```bash
  git commit -am "Close a wait segment on the commit that parks a run on its children (issue #70)"
  ```

  Then `cargo clean`.

---

### Task 4: The resolution closes `wait-resolve`, with its links

**Files:**
- Modify: `crates/rakka-agent/src/run.rs` (`close_wait_changes`, the `run-resume` close in `apply_unboxed` at about `:8890`)
- Modify: `crates/rakka-agent/src/observability.rs` (`LINK_KIND_PARKED_WAIT`, `AGENT_TELEMETRY_LINK_KINDS`, and the `LINK_KIND_RESUME_REQUEST` doc)
- Test: `crates/rakka-agent/tests/child_wait_segments.rs`, `crates/rakka-agent/tests/workflow_tool.rs`, `crates/rakka-agent/tests/telemetry_context.rs`

**Interfaces:**
- Consumes from Task 1: `WaitResolve`, `parked_span_identity`. From Task 2: `trigger_telemetry`, `with_telemetry`. From Task 3: `AgentWaitChange`, `close_wait_changes`, `parked_group`.
- Produces:
  - `pub const LINK_KIND_PARKED_WAIT: &str = "parked-wait";`, in `AGENT_TELEMETRY_LINK_KINDS` and exported from `lib.rs`;
  - `fn parked_wait_links(loop_state, group, trigger) -> Vec<AgentSpanLink>`:
    - a `parked-wait` link to the group's `parked_span_identity(loop_state.telemetry())`, when `Some`;
    - a `resume-request` link to `trigger`, when the trigger holds a valid trace context (`require_agent_trace_context`).

- [ ] **Step 1: Write the failing tests.**

  In `child_wait_segments.rs`, traced as in Task 3:
  - **`the_last_childs_result_resolves_the_wait_and_links_the_park_and_the_child`.** Deliver both results, the second with `envelope.with_telemetry(context(CHILD_PARENT))`. Then:
    - exactly one `"wait-resolve"`;
    - its `span_links` hold `parked-wait`, whose `span_id` equals the `"wait"` segment's `span_id`, and `resume-request` to `CHILD_PARENT`'s span;
    - `rakka.agent.fan_in.resolution` = the group's resolution code;
    - no `"run-resume"` was added by either delivery, as today.

    Each delivery builds its own `fixture.run()`, so the park and the resolution are computed by different facades over the store. Say so in the doc comment: that is the passivation case.
  - **`a_duplicate_result_resolves_nothing`.** Deliver the second result again. No second `"wait-resolve"`.
  - **`the_deadline_resolves_the_wait_and_links_its_timer_when_given_one`.** Use `fan_out_turn_with_deadline()` (from `fan_out_fan_in.rs`; move it to `common` if needed). Send `FireFanInDeadline` past the deadline. One `"wait-resolve"`, with `resume-request` exactly when it was sent `.with_telemetry(...)`. Run it as two runs, one traced and one not.
  - **`a_run_with_no_trace_context_resolves_without_a_park_link`.** No `parked-wait`, and the segment closes.

  In `workflow_tool.rs`, next to the existing resume-segment tests (about `:4375`):
  - **`a_workflow_childs_result_resolves_the_wait_and_links_its_relay`.** Send `RecordWorkflowResult` `.with_telemetry(context(RELAY_PARENT))`. One `"wait-resolve"` with `parked-wait` and `resume-request`.
  - **`a_group_resolved_under_a_reconciliation_park_closes_wait_resolve`.** Reuse the setup at about `:4320-4373`. The group's resolution closes `"wait-resolve"` with `parked-wait`, even though the run stays parked.
  - **`a_cancelled_parents_last_child_closes_both_with_the_same_links`.** A cancelled parent's last workflow child reports and the run ends. One `"wait-resolve"`, and one `"run-resume"` whose links include `parked-wait` and `resume-request`. Find the setup in `cancellation_propagation.rs` or `workflow_tool.rs`.

  In `telemetry_context.rs`: nothing new. Its writer-bijection test must keep passing once `parked-wait` is catalogued and written.

- [ ] **Step 2: Run them and watch them fail.**
  Run: `cargo test -p rakka-agent --all-features --test child_wait_segments --test workflow_tool --test telemetry_context`
  Expected: the new tests fail with no `"wait-resolve"`. `telemetry_context` passes.

- [ ] **Step 3: Implement.**
  - **`close_wait_changes`'s resolved half.** It closes `timer.close(WaitResolve { kind: AGENT_WAIT_KIND_CHILDREN })` with:
    - `rakka.agent.wait.kind`;
    - `rakka.agent.fan_in.resolution` from `loop_state.fan_in().and_then(|g| g.resolution.as_ref())`;
    - `.ok()`;
    - closed by `close_segment_under(segment, loop_telemetry, parked_wait_links(…))`.
  - **The `run-resume` close.** In `apply_unboxed`, read `parked_group` from the cached state before the command, next to `resolving_checkpoint`, and the command's `trigger_telemetry().cloned()`. When the existing `run-resume` close fires and the run was parked on a group, append `parked_wait_links(…)` to `links`. Do not add a second `resume-request` when the checkpoint resolution already added one: compare by `span_id`.
  - **The vocabulary.** Add `LINK_KIND_PARKED_WAIT` to the catalogue. Widen `LINK_KIND_RESUME_REQUEST`'s doc to "the incoming request, exchange or timer that caused a resume".

- [ ] **Step 4: Run them and watch them pass, then cover Review Focus item 4.**
  Run: the Step 2 command, plus `--test fan_out_fan_in --test checkpoint_resolve_segment --test telemetry_segments --test trace_scenarios`
  Expected: PASS. In `a_duplicate_result_resolves_nothing`, also re-apply the second result past the journal window, if `fan_out_fan_in.rs`'s `duplicate_results_accept_idempotently` shows how. No second `"wait-resolve"`.

- [ ] **Step 5: Cover Review Focus item 3.** Write `a_resolution_whose_settle_pass_fails_still_closes_wait_resolve`. Use the testkit's failing effect sink or a `CrashPoint`, as `checkpoint_resolve_segment.rs` does for `checkpoint-resolve`. Make the settle pass after the last child's result fail. The reply is `Err` and `"wait-resolve"` closed once. Run it, watch it pass, and say in the commit message what it covers.

- [ ] **Step 6: Cover Review Focus item 1, and the unreachable row.**
  - **Item 1.** If a denied approval on a member's send can resolve an `Any` group in the `ResolveCheckpoint` arm's own transition, write `a_denied_members_approval_that_resolves_the_group_links_once`. It asserts the links listed in Review Focus item 1. If that path cannot occur, write why in the ledger as a ruling.
  - **The spec's row "a member's send fails definitively and resolves the group in a crank step".** While a run is parked on G, every send of the turn has settled. Confirm by reading `turn_rest` and `awaits_turn_effect` whether a parked group can resolve with no outside trigger. If it can, test it, expecting `parked-wait` only. If it cannot, ledger a ruling and drop the row.

- [ ] **Step 7: Falsify.** Break each guard in turn, and revert each break:
  - derive the resolution's identity from `opened_turn + 1`: the last-child test must fail on the `parked-wait` id;
  - close `wait-resolve` whenever the run is not parked after the transition, without checking `marks.parked`: the duplicate test, or every other suite, must fail;
  - close it on the first result: the first-result test from Task 3 must fail.

- [ ] **Step 8: Lint, and commit.** Use Task 1 Step 6's commands.

  ```bash
  git commit -am "Close wait-resolve on the transition that resolves a run's child wait, linking the park and the trigger (issue #70)"
  ```

  Then `cargo clean`.

---

### Task 5: The after-tally, the docs, and validation

**Files:**
- Modify: `docs/rakka-agent-observability-catalogue.md` (the class table at about `:233`, the `run-resume` paragraph at about `:237`, the call-site table at about `:268`, the identity table at about `:316`, the link table at about `:327`)
- Modify: `docs/rakka-agent-telemetry-validation-matrix.md` (the owed line at about `:214`)
- Modify: `CHANGELOG.md` (`## Unreleased` → `### Added`)
- Modify: `docs/rakka-compatibility.md`, if it records command-shape changes. Find how `ResolveCheckpoint`'s `telemetry` was recorded and follow that; if it was not recorded, add nothing.
- Create: `tmp/2026-11-26-assignment-records-context-on-refusal.md` (an issue draft, not filed)
- Modify: `docs/superpowers/specs/2026-11-26-child-wait-segments-design.md` (an as-built section)

- [ ] **Step 1: The after-tally.** Apply `tally.patch` and run `RAKKA_SEGMENT_TALLY=<workspace>/tally-after.txt cargo test -p rakka-agent --all-features > <workspace>/tally-after-run.txt 2>&1; echo "exit=$?"` in the background. Revert the patch.
  Expected: `exit=0`. Then:
  - `grep -P '\trun-resume$' tally-before.txt | sort | uniq -c` equals the same over `tally-after.txt`, apart from lines belonging to tests this branch added;
  - the after file's `wait` and `wait-resolve` lines are counted.

  Write the totals into the ledger. If the `run-resume` lines differ for any pre-existing test, stop and treat it as a defect: the anti-goal is broken.

- [ ] **Step 2: Docs, as the spec's section 7 lists them.**
  - **The catalogue.** Add the `wait` and `wait-resolve` rows ("17.4's checkpoint/timer park — no 17.6 row"). Add both to the run entity's call-site row. Add the identity row ("the run's context and the fan-in group's turn and timestamp; stored on nothing"). Add the `parked-wait` link row and widen `resume-request`'s "Written on" and "Points at". In the `run-resume` paragraph, add one sentence on the child-wait links. Add spec 5.8's note on links to a span nobody exported.
  - **The matrix.** Its owed line becomes a met row naming `child_wait_segments.rs` and the three `workflow_tool.rs` tests.
  - **The CHANGELOG Added entry.** It covers spec 5.7: two new spans per child wait, with the measured figures from Step 1; `run-resume` unchanged in count, now linking for a child wait; and the two commands' new `telemetry` field, optional and wire-compatible. It also covers spec 5.8.

- [ ] **Step 3: The issue draft.** In `tmp/2026-11-26-assignment-records-context-on-refusal.md`, write up what the reviewer found. The `Assignment` arm in `run.rs` (`envelope.kind() == AgentExchangeKind::Assignment && envelope.has_telemetry()`) records the envelope's context whatever `accept_assignment` answered, including a `run-generation-conflict` refusal. Give the file:line, the consequence (a later park identity drifts, and a refused message rewrites a run's causal context), and that it is not fixed here. Not filed.

- [ ] **Step 4: The spec's as-built section.** Add section 9, "As built", to the design. It records:
  - the measured tally;
  - every ruling from the ledger;
  - which spec 6 rows became which tests, and which were dropped as unreachable, and why.

- [ ] **Step 5: Validate.**
  Run: `scripts/validate.sh > <scratchpad>/validate.txt 2>&1; echo "exit=$?"`. If a workspace test is killed under load, re-run that crate alone before blaming the branch.
  Expected: `exit=0`. Also `cargo doc -p rakka-agent --no-deps 2>&1 | grep -c warning` shows no new warnings from this branch's comments.

- [ ] **Step 6: Commit.**

  ```bash
  git add docs CHANGELOG.md
  git commit -m "Document the child-wait segments and their links (issue #70)"
  ```

  Then `cargo clean`. The `tmp/` draft is left uncommitted, as earlier drafts were.
