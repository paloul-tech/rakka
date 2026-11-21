# A Failed Straggler Cancel Leaves the Run Going — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A failed or exhausted `WorkflowCancelCall` no longer winds its run down; it settles its own effect and moves nothing else, as a successful one does (issue #197).

**Architecture:** One condition in the failure arm of `apply_effect_outcome` (`crates/rakka-agent/src/run.rs`) gains `WorkflowCancelCall` beside the three kinds already exempt from the wind-down. The settle pass already rests a turn a cancel held open (`AgentRun::needs_turn_rest`). Tests are whole-run walks in a new integration test file; docs follow.

**Tech Stack:** Rust 1.88 workspace, `tokio` tests, the crate's `testkit` (`ScriptedDispatcher`, `DeterministicModelAdapter`) and `tests/common` fixture.

**Spec:** `docs/superpowers/specs/2026-11-21-failed-straggler-cancel-carries-on-design.md`

## Global Constraints

- Branch `failed-straggler-cancel-carries-on` (already created off `main` at `616ee1a`). Commit there; do not push or open a PR in these tasks.
- No durable record changes shape: no new field, enum variant, status, error code or decision kind.
- The straggler's cell keeps `cancel = Some(AgentWorkflowCancelDisposition::Committed { effect })` after a failed cancel.
- Records an earlier build wound down are not repaired.
- Workspace lints: `missing_docs` and clippy `-D warnings`; every new test fn gets a doc comment saying what it proves.
- `scripts/validate.sh` is the final gate. Redirect it to a file and read `$?` (piping into `tail` reports `tail`'s code). If the one-shot workspace test is killed under load, run the crates one by one.
- Commit messages end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

- A run whose result was accepted while the straggler's cancel was still out, and whose cancel then fails, ends `Completed` under `result-accepted` (it waited for the cancel since #178). Pinned in Task 1, `an_accepted_run_waiting_for_its_cancel_ends_accepted_when_the_cancel_fails`.
- A run that was cancelled, whose straggler cancel then fails, still waits for the straggler's own result and ends `Cancelled`, as the `Refused` contract says. Pinned in Task 1, `a_cancelled_run_whose_cancel_fails_still_waits_for_its_straggler`.
- A straggler that never reports does not hold an accepted run's end. Every walk in Task 1 asserts `Completed` before the straggler's result is delivered.

---

### Task 1: A failed straggler cancel leaves the run going

**Files:**
- Create: `crates/rakka-agent/tests/failed_straggler_cancel.rs`
- Modify: `crates/rakka-agent/src/run.rs` (failure arm, ~`:5629-5798`)
- Modify: `crates/rakka-agent/src/effect.rs` (`moves_the_run` doc, ~`:1183-1187`)
- Modify: `crates/rakka-agent/src/workflow_tool.rs` (`AgentWorkflowCancelDisposition::Committed` doc, ~`:765-771`)

**Interfaces:**
- Consumes: `tests/common` (`Fixture`, `delegation_config_with_fan_in`, `fan_in_tool_id`, `goal_spec_draft`, `goal_spec_with_workflow`, `goal_task_creation_command`, `run_scope`, `task_definition`, `tool_call`, `workflow_tool_descriptor`, `agent_scope`, `TENANT`, `AGENT`, `WORKFLOW_TOOL`).
- Produces: nothing other tasks call.

- [ ] **Step 1: Write the test file from the filing probe**

Start from `tmp/2026-11-21-draft-probes/zz_probe_straggler_cancel.rs`, which runs at `616ee1a` and builds the world: two workflow invocations awaited under `AgentFanInPolicy::Any`, a cancellable descriptor, the first child `Completed` while the run is reached over an unreachable `AgentExchangeRouter::new()`, so the straggler's cancel and turn two's model call are both `Ready`. Keep its helpers: `StartingExecutor`, `ScriptedCancel`, `workflow_call`, `proposing_turn`, `rejected_turn`, `deliver_result`, `effects`, `answer_one`. Drop `picture` and every `println!`. Module doc: what the file proves and that it is issue #197.

`answer_one` gains a parameter `outcome: Option<AgentRunEffectOutcome>`: `None` answers through `fixture.dispatcher.answer(&effect)` as before, and `Some` records the given outcome instead.

Add:

```rust
enum Door { NoExecutor, Refused, Exhausted }
enum Order { ModelCallOut, ProposalOutThenAccepted, ProposalOutThenRejected }

/// The code each door leaves on the cancel's effect record.
fn door_code(door: Door) -> &'static str // "workflow-cancel-executor-missing",
                                         // "workflow-cancel-refused-by-registry",
                                         // "workflow-cancel-attempt-failed"
```

- `Door::NoExecutor` wires no cancel executor. `Door::Refused` wires `ScriptedCancel` answering `Refused { code: "workflow-cancel-refused-by-registry", .. }`. `Door::Exhausted` wires none and records `AgentRunEffectOutcome::exhausted("workflow-cancel-attempt-failed", "every attempt of the cancel failed")` through `answer_one(.., Some(..))`.
- `Order::ProposalOutThenRejected` scripts `first_turn`, `rejected_turn()`, `proposing_turn()`. The other two script `first_turn`, `proposing_turn()`.

A walk `async fn walk(door: Door, order: Order)` does this, asserting at each mark:

1. Builds the world and delivers the first child's `Completed` over the unreachable router.
2. For both `ProposalOut…` orders it answers the `ModelCall` over the unreachable router, so the proposal is made and held back.
3. It answers the `WorkflowCancelCall` over the unreachable router. **Assert:** the run snapshot's `terminal_reason` is `None`, and its `status` is not `AgentRunStatus::Cancelling`. The cancel effect's `status` is `AgentRunEffectStatus::Failed` (`Exhausted` for `Door::Exhausted`), and its `last_error_code` is `Some(door_code(door))`. The straggler's cell `cancel` matches `Some(AgentWorkflowCancelDisposition::Committed { .. })`.
4. For `Order::ModelCallOut` it now answers the `ModelCall` over the unreachable router.
5. `fixture.pump()` with the fixture's router. **Assert, before the straggler reports:**
   - The run is `AgentRunStatus::Completed`, and `terminal_reason.code()` is `"result-accepted"`.
   - The run's turn is `3` for `ProposalOutThenRejected`, else `2`.
   - The task snapshot's `status` is `AgentTaskStatus::Completed` and `accepted_result` is `Some`.
   - `rejection_count` is `1` for `ProposalOutThenRejected`, else `0`.
6. It delivers the straggler's `Completed` result over `fixture.router`. **Assert:** it applies, and the run stays `Completed`.

One `#[tokio::test]` per door × order, nine in all, named `{no_executor,refused,exhausted}_{while_the_model_call_is_out,while_the_proposal_is_out_then_accepted,while_the_proposal_is_out_then_rejected}`. Each has a one-line doc comment.

Four more tests in the same world:

- `a_turn_held_for_a_failed_cancel_rests_and_the_run_completes`:
  - Script `first_turn`, then a turn with one `common::tool_call("lookup")`, then `proposing_turn()`, with no cancel executor.
  - After the first child, answer the `ModelCall` and then the `ToolCall`, both over the unreachable router. **Assert:** phase `AgentLoopPhase::AwaitingTools`, turn `2`: the cancel holds the turn.
  - Answer the `WorkflowCancelCall`. **Assert:** `terminal_reason` is `None`.
  - `fixture.pump()`. **Assert:** run `Completed` / `"result-accepted"` at turn `3`, task `Completed`.
- `a_park_answered_with_a_failed_cancel_leaves_the_run_going`:
  - After the first child, record `AgentRunEffectOutcome::indeterminate("dispatcher-lost-after-started", "the recovery retry was refused")` for the cancel. **Assert:** run `WaitingForReconciliation`.
  - Apply `AgentRunEntityCommand::ResolveIndeterminateEffect`, built exactly as `answer_the_park_of` builds it in `tests/workflow_tool.rs:2280`, with `ConfirmedExecuted { outcome: failed("workflow-cancel-refused-by-registry", "the registry refused the cancel") }`. **Assert:** `terminal_reason` is `None`, status not `Cancelling`, cancel effect `Failed`.
  - Answer the `ModelCall`, then `fixture.pump()`. **Assert:** run `Completed` / `"result-accepted"`, task `Completed`.
- `an_accepted_run_waiting_for_its_cancel_ends_accepted_when_the_cancel_fails` (pin):
  - After the first child, answer the `ModelCall` over `fixture.router`, so the task accepts with the cancel still out. **Assert:** run `Cancelling` with reason `"result-accepted"`.
  - Answer the cancel, with no executor. **Assert:** run `Completed` with reason `"result-accepted"`.
- `a_cancelled_run_whose_cancel_fails_still_waits_for_its_straggler` (pin):
  - After the first child, apply `AgentRunEntityCommand::Cancel`, with an operation id built as `tests/communal_claim_append.rs:591` builds one and reason `"operator stopped the work"`.
  - `fixture.pump()`, with no cancel executor. **Assert:** run `Cancelling`, reason `"cancellation-requested"`, cancel effect `Failed`.
  - Deliver the straggler's `Completed` result, then `fixture.pump()`. **Assert:** run `AgentRunStatus::Cancelled`.

- [ ] **Step 2: Run the file against the unchanged code**

Run: `cargo test -p rakka-agent --test failed_straggler_cancel`
Expected: the nine walks, the held-turn test and the park-answer test FAIL (the run is `Cancelling` with `effect-failed` at mark 3). The two pins PASS. If a pin fails, stop: the world is not what the design says, so report it rather than adjusting the assertion.

- [ ] **Step 3: Exempt the kind in the failure arm**

In `apply_effect_outcome`, add `&& failed_kind != AgentRunEffectKind::WorkflowCancelCall` to the condition guarding `fence_unsent` and `EffectFailed` (`run.rs:5782`).

Extend the comment above that condition (`:5629-5653`), which lists the exceptions, with a fourth paragraph. It should say:
- a workflow cancel is a request with an observable outcome, not the run's work ([specification 8.7]);
- on a live run every cancel is a fan-in straggler's, and that fan-in has already decided;
- a cancel that did not reach its child leaves the child to report on its own, as `Unsupported` does and as `AgentWorkflowCancelFinding::Refused` promises;
- the failure stays on the effect record and the cell keeps `Committed`;
- a run already winding down has its reason recorded, and its own fence ran when the wind-down began (issue #197).

- [ ] **Step 4: Correct the two docs that state the old rule**

- `effect.rs` `moves_the_run`: replace "A workflow cancel that succeeds writes nothing either; one that fails winds the run down, which is not a status to rest on." with a sentence saying a workflow cancel writes nothing of the run's either, whether it succeeds or fails (issue #197).
- `workflow_tool.rs` `AgentWorkflowCancelDisposition::Committed`: add that delivery is read from the effect the variant names. A failed or exhausted delivery leaves that effect `failed` or `exhausted` with its code, and the variant stands.

- [ ] **Step 5: Run the file and the crate**

Run: `cargo test -p rakka-agent --test failed_straggler_cancel`
Expected: all 13 pass.

Run: `cargo test -p rakka-agent --all-features > "$SCRATCH/agent-tests.log" 2>&1; echo $?`
Expected: `0`. If an existing test asserted the old wind-down, read it before changing it. Change it only if it asserts exactly the behaviour the ruling removes, and list it in the commit message.

- [ ] **Step 6: Falsify**

Temporarily remove the new `!= WorkflowCancelCall` line, then run `cargo test -p rakka-agent --all-features` over the whole crate. Record how many tests fail and which ones. Expect the 11 non-pin tests in the new file, and say if any test elsewhere also fails. Restore the line and confirm with `git diff --stat` that only the intended files changed.

- [ ] **Step 7: Commit**

```bash
git add crates/rakka-agent/tests/failed_straggler_cancel.rs crates/rakka-agent/src/run.rs \
        crates/rakka-agent/src/effect.rs crates/rakka-agent/src/workflow_tool.rs
git commit -m "A failed straggler cancel leaves the run going (issue #197)"
```

The message body states the falsification count from Step 6.

### Task 2: Product docs, compatibility note, changelog, validation

**Files:**
- Modify: `docs/rakka-agents.md:200-202`
- Modify: `docs/rakka-compatibility.md` (new entry directly after the issue #193 entry, ~`:985`)
- Modify: `CHANGELOG.md` (`### Fixed`, directly after the issue #193 entry, ~`:591`)

**Interfaces:**
- Consumes: Task 1's behaviour.
- Produces: nothing.

- [ ] **Step 1: `docs/rakka-agents.md`**

The sentence "An answer that fails the model call, a tool call or a workflow cancel winds the run down, as that failure always does" changes: an answer that fails the model call or a tool call winds the run down. One that fails a workflow cancel does not: the failure stays on the cancel's effect record and the run goes on. Grep the file for other sentences saying a failed workflow cancel winds a run down (`grep -n -i "workflow cancel" docs/rakka-agents.md`) and correct any you find the same way.

- [ ] **Step 2: `docs/rakka-compatibility.md`**

Write one entry in the shape of the gap-slice-2 claim-append entry (`:119`). It covers:
- what changed: a definitively failed or exhausted `WorkflowCancelCall` no longer records `EffectFailed` or winds its run down;
- every way in: no executor, `Refused`, retries spent, and a `ConfirmedExecuted` reconciliation answer carrying a failure;
- what stays: the effect record keeps the code, and the cell keeps `Committed`;
- a cancelled run still waits for the straggler's own result;
- no durable field, status, error code or decision kind is added;
- records an earlier build wound down are not repaired;
- a deployment that relied on a failed cancel to end a run must cancel the run;
- in a mixed deployment, the binary that applies the cancel's outcome decides, and an earlier one still winds the run down.

- [ ] **Step 3: `CHANGELOG.md`**

Add one `rakka-agent` entry in the style of its neighbours: **a failed straggler cancel no longer fails the run** (issue #197), the lost-result orders it fixes, and a pointer to `docs/rakka-compatibility.md`.

- [ ] **Step 4: Validate**

Run: `scripts/validate.sh > "$SCRATCH/validate.log" 2>&1; echo $?`
Expected: `0`. Read the tail of the log for the test count. Then run `scripts/package-check.sh > "$SCRATCH/package.log" 2>&1; echo $?`, expecting `0`.

- [ ] **Step 5: Commit**

```bash
git add docs/rakka-agents.md docs/rakka-compatibility.md CHANGELOG.md
git commit -m "Document that a failed straggler cancel leaves the run going (issue #197)"
```
