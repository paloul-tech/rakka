# Issue #71 Item 4: Graph Cancellation Settles Its Outbox — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every transition that cancels a graph's leftover nodes settles those nodes' outbox entries before it is persisted: the run's `Cancel`, the graph completing, and the graph failing. `Cancel` does the whole graph cancellation and returns what it cancelled.

**Architecture:**
- One shared settle, rule 1 in one place, is used by the fleet and by a new bridge operation that takes a scheduler transition.
- `AgentRunCore::cancel` gains a graph path: compute the transition from the state, settle, then persist graph and status in one `Cancel` write.
- The core's `complete_graph_node` and the bridge's two failure entry points settle before the transition is persisted.
- `NodeCancelled` joins the runtime event vocabulary.

**Tech Stack:** Rust 1.88 workspace; crates `rakka-agent-workflow`, `rakka-workflow` (read only) and `rakka-a2a` (two call sites).

**Spec:** `docs/superpowers/specs/2026-11-27-graph-cancellation-design.md`

## Global Constraints

- No durable schema change. The graph state, run state and outbox entries keep their schema versions.
- Rule 1 for each entry, as the design names it:

  | Entry | What happens |
  |---|---|
  | `Dispatching` | `record_outbox_cancelled_in_flight(…, DISPATCH_CANCELLED_IN_FLIGHT_LINE)` |
  | `Pending` or `Failed` | `record_outbox_cancelled` |
  | terminal, or compacted away (absent) | skipped |

  It lives in one function. The fleet's `settle_cancelled_effects` calls it.
- Settle before persisting, on every path (design 5.3 and 5.3a).
- Only nodes a transition changed, and that its state shows `Cancelled`, have their entries settled.
- `rakka-workflow`'s inbox is not changed: rule 2 was declined.
- Each new `AgentRunCore` operation boxes its body (`Box::pin(async move { … })`), as every core operation does (issue #162).
- `missing_docs` is warned, and validation runs clippy with `-D warnings`. Never add `-D warnings` to the doc build.
- Run `scripts/validate.sh > <file> 2>&1; echo "exit=$?"`. Never pipe it.
- `cargo clean` between tasks.
- Commits end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Nothing is published, and nothing is pushed without the owner's word.

## Review Focus

1. **A settle that fails part-way** (a store error on the second entry). The error is returned, entries already settled stay settled, and a repeat settles the rest and reports only those. Owned by Task 1, Step 1.
2. **An entry compacted away.** It is skipped, not an error, as the fleet skips it. Owned by Task 1, Step 1.
3. **A replayed failure command on the bridge** (the inbox answers `Duplicate`). The transition is recomputed, the settle finds every entry terminal, and the outcome reports nothing settled. Owned by Task 3, Step 1.
4. **`Cancel` on a `Cancelling` run whose graph already completed in the same pass.** It takes today's `Cancelling → Cancelled` path. The siblings' entries were already settled when the graph completed (Task 3), so nothing is left due. Owned by Task 3, Step 1.
5. **The host's own sequence, unchanged.** Persisting `cancel_graph_run` through `update_graph_state` still ends the run `Cancelled` and settles nothing, and a later `Cancel` is refused as today. Owned by Task 2, Step 1.

---

### Task 1: One settle, rule 1 in one place

**Files:**
- Modify: `crates/rakka-agent-workflow/src/effect_bridge.rs` (the new types and operation, and `AgentGraphEffectCommandOutcome`)
- Modify: `crates/rakka-agent-workflow/src/dispatcher.rs:3270-3318` (`settle_cancelled_effects` calls the shared function)
- Modify: `crates/rakka-agent-workflow/src/lib.rs` (exports)
- Create: `crates/rakka-agent-workflow/tests/graph_cancellation.rs`

**Interfaces:**
- Produces:
  - `#[non_exhaustive] pub enum AgentOutboxSettlement { Cancelled, CancelledInFlight }`.
  - `#[non_exhaustive] pub struct AgentSettledOutboxEntry { pub effect_id: AgentEffectId, pub settlement: AgentOutboxSettlement }`.
  - `pub async fn settle_cancelled_outbox_entries<Store, Clock>(inbox: &mut AgentRunInbox<Store, Clock>, effect_ids: &[AgentEffectId], reason: &str) -> AgentInboxResult<Vec<AgentSettledOutboxEntry>>`, in `effect_bridge.rs`. It returns only entries it wrote. An absent entry is skipped.
  - `AgentGraphEffectBridge::settle_cancelled_nodes<Store, Clock>(&self, inbox, transition: &AgentGraphSchedulerTransition) -> AgentGraphEffectBridgeResult<Vec<AgentSettledOutboxEntry>>`. The effect ids are the `scheduled_effect_ids`, in node order, of each `transition.changed_node_ids` entry whose node state in `transition.state` is `Cancelled`.
  - `pub const GRAPH_NODE_CANCELLED_REASON: &str = "graph-node-cancelled"`.
  - `AgentGraphEffectCommandOutcome` gains `pub settled_entries: Vec<AgentSettledOutboxEntry>` (empty in this task) and `#[non_exhaustive]`.

- [ ] **Step 1: Write the failing tests** in the new `graph_cancellation.rs`. Build on the fixture style of `effect_bridge.rs`, with a `TestInbox` and `effect_plan`. Use a two-branch plan: `input` → `effect` (ToolCall) and `input` → `terminal`, or whatever shape keeps `effect` waiting while the graph is cancelled.
  - `settling_a_cancel_transition_cancels_its_nodes_pending_entry`:
    - schedule `effect`, then compute `AgentGraphScheduler::new().cancel_graph_run(&plan, state, now)` (not persisted);
    - `settle_cancelled_nodes` returns `[effect → Cancelled]`;
    - the entry is `OutboxStatus::Cancelled`;
    - a `dispatch_due_outbox` pass with a counting dispatcher dispatches 0.
  - `an_entry_mid_dispatch_is_cancelled_with_its_attempt_counted`: `mark_outbox_dispatching` first. The settlement is `CancelledInFlight`, the entry is `Cancelled`, and its attempt count is 1 with last error `DISPATCH_CANCELLED_IN_FLIGHT_LINE`.
  - `a_terminal_or_absent_entry_is_left_alone`: an entry `Dispatched` before the cancel, and an effect id with no entry, are both absent from the result and unchanged.
  - `only_the_nodes_the_transition_cancelled_are_settled`:
    - a node resolved earlier, whose entry is still cancellable (a checkpoint decided while its approval request was pending);
    - a transition that cancels a different node;
    - the resolved node's entry stays `Pending`.
  - `a_transition_that_cancelled_nothing_settles_nothing`: an empty `Vec`.
  - `a_settle_that_fails_part_way_converges` (Review Focus 1):
    - two cancelled nodes, with a workflow store that fails its second `compare_and_set` once (a wrapper like `run_core.rs`'s `FailingRunStore`);
    - the first call errs;
    - the second returns only the remaining entry;
    - both entries end `Cancelled`.

- [ ] **Step 2: Run them to verify they fail.**
  Run: `cargo test -p rakka-agent-workflow --all-features --test graph_cancellation`
  Expected: FAIL to compile, because `settle_cancelled_nodes` and `AgentOutboxSettlement` do not exist.

- [ ] **Step 3: Implement.**
  - Move the fleet's per-entry rule into `settle_cancelled_outbox_entries`, keeping its behaviour.
  - `settle_cancelled_effects` calls it with its own reason, and keeps its own return shape.
  - Add `settle_cancelled_nodes`.
  - Add the outcome field. Each `accept_and_apply_*` constructs it with an empty `settled_entries`.

- [ ] **Step 4: Run them to verify they pass, plus the fleet's cancellation suites.**
  Run: `cargo test -p rakka-agent-workflow --all-features --test graph_cancellation --test dispatcher_fleet_cancellation --test dispatcher_fleet_settle --test effect_bridge`
  Expected: PASS.

- [ ] **Step 5: Falsify.** Break each guard, run Step 4's command, and restore it:
  - settle every node's entries, not only the changed and `Cancelled` ones (`only_the_nodes…` must fail);
  - cancel an entry mid-dispatch with `record_outbox_cancelled` (the in-flight test must fail);
  - error on an absent entry (`a_terminal_or_absent…` must fail).

- [ ] **Step 6: Lint and commit.**

```bash
cargo fmt --all && cargo clippy -p rakka-agent-workflow --all-targets --all-features -- -D warnings
git add crates/rakka-agent-workflow
git commit -m "Settle a transition's cancelled nodes' outbox entries under the fleet's rule (issue #71 item 4)"
```

  Then `cargo clean`.

### Task 2: `Cancel` does the whole graph cancellation

**Files:**
- Modify: `crates/rakka-agent-workflow/src/graph_scheduler.rs:593-618` (`cancel_graph_state`; `cancel_graph_run` calls it)
- Modify: `crates/rakka-agent-workflow/src/runner.rs` (`cancel_with_graph_state`)
- Modify: `crates/rakka-agent-workflow/src/run_core.rs:737-753` (`cancel`), `crates/rakka-agent-workflow/src/runtime.rs:561-566,875-876` (the `Cancel` reply)
- Modify: `crates/rakka-agent-workflow/src/runtime_events.rs` (`NodeCancelled` in the enum, `all()`, the label table, `from_label`, `requires_node_id`)
- Modify: `crates/rakka-a2a/src/host.rs:797` (the reply type) and `crates/rakka-a2a/src/task.rs:536-555` (`NodeCancelled => None`, private like every node event)
- Modify: `crates/rakka-agent-workflow/src/lib.rs`
- Test: `crates/rakka-agent-workflow/tests/graph_cancellation.rs`, `crates/rakka-agent-workflow/tests/runtime_events.rs`, `crates/rakka-agent-workflow/tests/graph_scheduler.rs`

**Interfaces:**
- Consumes: Task 1's `settle_cancelled_nodes`, `AgentSettledOutboxEntry`.
- Produces:
  - `AgentGraphScheduler::cancel_graph_state(&self, state: AgentGraphRunState, now) -> AgentGraphSchedulerResult<AgentGraphSchedulerTransition>`. It checks the schema version, answers an already-`Cancelled` graph unchanged, and refuses another terminal status with `TerminalGraph`.
  - `AgentStepRunner::cancel_with_graph_state(&mut self, graph_state, now) -> AgentRunEngineResult<AgentRunTransition>`. From `Cancelling` only. Kind `Cancel`, status `Cancelled`, `current_step_id = None`, and `completed_at = Some(now)`.
  - `#[non_exhaustive] pub struct AgentRunCancellation { pub transition: AgentRunTransition, pub cancelled_node_ids: Vec<AgentCompiledNodeId>, pub settled_entries: Vec<AgentSettledOutboxEntry> }`.
  - `AgentRunCore::cancel(now) -> AgentRunRuntimeResult<AgentRunCancellation>`, and `AgentRunActorCommand::Cancel { now, reply_to: ReplyTo<AgentRunRuntimeResult<AgentRunCancellation>> }`.
  - `AgentRuntimeEventKind::NodeCancelled`, labelled `node-cancelled`.

- [ ] **Step 1: Write the failing tests.** Use `run_core.rs`'s fixtures (`RuntimeFixture`, `core`, `a_core_with_its_effect_scheduled`, `CountingDispatcher`), copied into `graph_cancellation.rs` or moved into a shared `tests/` helper.
  - `a_cancelled_graph_run_sends_none_of_its_queued_effects`, the gap, written first. Take a core with its effect scheduled, call `request_cancellation`, then `cancel`, then `parts_mut().inbox.inner_mut().dispatch_due_outbox(&mut CountingDispatcher)`.
    - The dispatcher counts 0.
    - The entry is `Cancelled`.
    - The run is `Cancelled`, and `transition.kind == AgentRunTransitionKind::Cancel`.
    - `cancelled_node_ids` holds `effect` and `terminal`, as the scheduler cancels them.
    - `settled_entries == [effect → Cancelled]`.
  - `cancel_settles_a_checkpoints_approval_request`: a second node waits on a human checkpoint, opened through `parts_mut()` and the bridge's `open_node_human_checkpoint`. Its approval entry is settled too and listed.
  - `a_crash_between_the_settle_and_the_run_write_converges`:
    - a run store whose next `compare_and_set` fails once (extend `FailingRunStore` with a `fail_writes` flag);
    - the first `cancel` errs, the run is still `Cancelling`, and the entry is `Cancelled`;
    - the second `cancel` returns the same `cancelled_node_ids` and an empty `settled_entries`;
    - the run is `Cancelled`.
  - `a_run_without_a_graph_cancels_as_before`, using `workflow()` and `accepted_run_state`: empty lists, and today's transition.
  - `cancel_on_a_cancelled_run_is_refused`: today's error.
  - `the_hosts_own_cancel_sequence_still_works` (Review Focus 5):
    - persist `cancel_graph_run` through `parts_mut().runner.update_graph_state`;
    - the run is `Cancelled` and the entry still `Pending`, which is today's gap on that path;
    - `cancel` is refused.
  - `the_actor_answers_cancel_as_the_core_does`: the actor's reply equals the core's for the same stores and inputs.
  - In `graph_scheduler.rs`: `cancel_graph_state_matches_cancel_graph_run`. Over a state with waiting nodes and a running loop instance (reuse that file's loop plan), both give equal transitions. Over a `Completed` graph, both refuse with `TerminalGraph`.
  - In `runtime_events.rs`: `node_cancelled_is_a_node_event`. It is in `all()`, `from_label("node-cancelled")` round-trips, and a draft without a node id is refused.

- [ ] **Step 2: Run them to verify they fail.**
  Run: `cargo test -p rakka-agent-workflow --all-features --test graph_cancellation --test graph_scheduler --test runtime_events`
  Expected: FAIL to compile (`AgentRunCancellation`, `cancel_graph_state` and `NodeCancelled` are missing). Write the gap test against today's API in a scratch copy first, and record that it dispatches 1: that is the proof the gap exists.

- [ ] **Step 3: Implement**, in the design's order (5.3):
  1. `refresh_if_stale`;
  2. on a `Cancelling` run with a non-terminal graph, `cancel_graph_state` (not persisted);
  3. `settle_cancelled_nodes` on the core's inbox;
  4. `cancel_with_graph_state`;
  5. return `AgentRunCancellation`.

  Every other run calls `runner.cancel` and returns empty lists. Then adapt `rakka-a2a/src/host.rs` to read `.transition`, and add the `NodeCancelled` arm in `rakka-a2a/src/task.rs`.

- [ ] **Step 4: Run them to verify they pass**, plus the suites that touch cancel.
  Run: `cargo test -p rakka-agent-workflow --all-features --test graph_cancellation --test graph_scheduler --test runtime_events --test run_core --test runtime_actor --test step_runner && cargo test -p rakka-a2a --all-features`
  Expected: PASS.

- [ ] **Step 5: Falsify.** Break each guard, run Step 4's first command, and restore it:
  - skip the settle (the gap test must fail);
  - settle after `cancel_with_graph_state` (the crash test must fail);
  - record the write as `GraphUpdated` (the gap test's kind assertion must fail).

- [ ] **Step 6: Lint and commit.**

```bash
cargo fmt --all && cargo clippy --workspace --all-targets --all-features -- -D warnings
git add crates/rakka-agent-workflow crates/rakka-a2a
git commit -m "Cancel a graph run whole: its nodes, their outbox entries, and the run, returning what it cancelled (issue #71 item 4)"
```

  Then `cargo clean`.

### Task 3: Completing and failing graphs settle their cancelled siblings

**Files:**
- Modify: `crates/rakka-agent-workflow/src/run_core.rs:508-527` (`complete_graph_node`)
- Modify: `crates/rakka-agent-workflow/src/runtime.rs:280-300` (`AgentGraphRuntime::complete_node`'s doc says it does not settle)
- Modify: `crates/rakka-agent-workflow/src/effect_bridge.rs:1153-1213` (`accept_and_apply_effect_failed`, `accept_and_apply_reconciliation_decision`)
- Test: `crates/rakka-agent-workflow/tests/graph_cancellation.rs`

**Interfaces:**
- Consumes: Task 1's `settle_cancelled_nodes` and `AgentGraphEffectCommandOutcome::settled_entries`.
- Produces: the core's `complete_graph_node` keeps its signature and return type. The two bridge entry points fill `settled_entries`.

- [ ] **Step 1: Write the failing tests.**
  - `a_graph_completed_while_a_sibling_waits_settles_the_siblings_entry`:
    - a plan where `terminal` is reachable from `input` while `effect` waits on its scheduled entry;
    - the core's `complete_graph_node(&plan, "terminal")` ends the graph `Completed` and `effect` `Cancelled`;
    - its entry is `Cancelled`;
    - a dispatch pass afterwards sends 0.
  - `a_graph_failed_by_an_effect_failure_settles_the_siblings_entry`:
    - two sibling effects, both scheduled;
    - `accept_and_apply_effect_failed` with `Exhausted` for one;
    - before the owner persists anything, the other's entry is already `Cancelled`, and `outcome.settled_entries` lists it.
  - `an_abandon_decision_settles_the_siblings_entry`: the same through `accept_and_apply_reconciliation_decision` with `Abandon`, on a parked node (the `Reconcile` disposition), reusing `failure_injection.rs`'s park setup.
  - `a_replayed_failure_command_settles_nothing_more` (Review Focus 3): apply the same failure command twice. The second outcome's `settled_entries` is empty.
  - `cancel_after_the_graph_completed_in_the_same_pass` (Review Focus 4): `request_cancellation` on a run whose graph is `Completed`, then `cancel`. It takes today's path with empty lists, and no entry is due.

- [ ] **Step 2: Run them to verify they fail.**
  Run: `cargo test -p rakka-agent-workflow --all-features --test graph_cancellation`
  Expected: the three settle tests FAIL, with entries still `Pending` and `settled_entries` empty. Review Focus 4's test may already pass; say so in the commit message.

- [ ] **Step 3: Implement.**
  - **`complete_graph_node`:** compute the transition with `AgentGraphScheduler::complete_node` from the refreshed graph state, call `settle_cancelled_nodes`, then persist through `runner.update_graph_state`, keeping today's phase recording.
  - **The two bridge entry points:** after `accept_command` and the transition, call `self.settle_cancelled_nodes(inbox, &transition)`, and put the result in the outcome.

- [ ] **Step 4: Run them to verify they pass, plus the bridge and core suites.**
  Run: `cargo test -p rakka-agent-workflow --all-features --test graph_cancellation --test effect_bridge --test failure_injection --test run_core --test human_checkpoints`
  Expected: PASS.

- [ ] **Step 5: Falsify.** Skip the settle in `complete_graph_node`, then in each bridge entry point. Each path's own test must fail. Restore each one.

- [ ] **Step 6: Lint and commit.**

```bash
cargo fmt --all && cargo clippy -p rakka-agent-workflow --all-targets --all-features -- -D warnings
git add crates/rakka-agent-workflow
git commit -m "Settle the outbox entries of siblings a completing or failing graph cancels (issue #71 item 4)"
```

  Then `cargo clean`.

### Task 4: Docs, drafts, and validation

**Files:**
- Modify: `CHANGELOG.md` (`## Unreleased`; add `### Changed` and `### Validation` headings)
- Modify: `docs/rakka-compatibility.md` (a new issue entry after issue #25's)
- Modify: `docs/plans/compiled_execution_with_graph_schdlr/compiled-graph-operational-runbooks.md` (its cancelled-node and stuck-entry guidance)
- Create (uncommitted): `tmp/2026-11-27-step-run-cancel-leaves-entries-due.md`, `tmp/2026-11-27-iterator-bound-leaves-siblings-due.md`
- Modify: `docs/superpowers/specs/2026-11-27-graph-cancellation-design.md` (section 9, "As built")

- [ ] **Step 1: CHANGELOG.**
  - Under `### Changed`, one **Behaviour-breaking, `rakka-agent-workflow`** entry:
    - `Cancel` settles and ends a graph run, and its reply is `AgentRunCancellation`;
    - completing or failing graphs settle cancelled siblings' entries;
    - `AgentGraphEffectCommandOutcome::settled_entries`;
    - `NodeCancelled`;
    - `settle_cancelled_nodes` and the owner-composed-write contract.
  - Under `### Validation`, the `graph_cancellation.rs` suite.

- [ ] **Step 2: The compatibility entry.**
  - **What changes.**
  - **What a host meets:** the design's 5.6. That covers compile breaks on the `Cancel` reply, `AgentRuntimeEventKind` and the bridge outcome's literal, and the contract that the host's own `fail_node` and `fail_waiting_node` writes must call `settle_cancelled_nodes` first.
  - **Mixed builds:** no durable shape changes. An older build reads every row a newer one writes, and a cancelled entry is a status it already knows.

- [ ] **Step 3: The runbook.** It now says which transitions settle entries, and that an entry of a cancelled node that is still due means an owner-composed write skipped the settle.

- [ ] **Step 4: The two `tmp/` drafts.** Each gives file:line, the consequence, and a fix sketch. Neither is filed.

- [ ] **Step 5: The design's section 9, "As built".** Every ruling from the ledger, which section 6 rows became which tests, and the falsification results.

- [ ] **Step 6: Validate.**
  Run: `scripts/validate.sh > <scratchpad>/validate.txt 2>&1; echo "exit=$?"`. If a workspace test is killed under load, re-run that crate alone.
  Expected: `exit=0`. Also `cargo doc -p rakka-agent-workflow --no-deps --all-features 2>&1 | grep -c warning` reports nothing new from this branch's comments.

- [ ] **Step 7: Commit.**

```bash
git add CHANGELOG.md docs
git commit -m "Document graph cancellation settling its outbox (issue #71 item 4)"
```

  Then `cargo clean`. The `tmp/` drafts stay uncommitted.
