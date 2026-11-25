# AgentRunCore Implementation Plan (issue #71, item 1)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A public, owner-held `AgentRunCore` that does everything
`AgentRunActor` does, and lets its owner compose upstream's transitions on
the same runner and inbox. A host that already serializes its writes can
then drive a run with one writer and no `Recover`.

**Architecture:**
- The actor's state and handler bodies move into
  `crates/rakka-agent-workflow/src/run_core.rs`. `AgentRunActor` holds one
  core and delegates each arm to it; the command enum is unchanged.
- The core adds `parts_mut()` (a split borrow of its runner and inbox) and a
  public `refresh_if_stale()`.
- Behaviour parity is held by the existing actor tests plus a registry
  parity test.

**Tech Stack:** Rust 1.88, tokio, rakka-core actors, `rakka-persistence`'s
`InMemoryDurableStateStore`, `rakka-workflow`'s `ManualWorkflowClock`.

**Spec:** `docs/superpowers/specs/2026-11-24-agent-run-core-design.md`
(commit `c921e664`). Section numbers below (§) refer to it.

## Global Constraints

- **Lints:** MSRV 1.88; `unsafe_code = "forbid"`; every public item has a
  doc comment; `clippy --all-targets --all-features -D warnings` is clean.
- **Errors:** every error stays an existing `AgentRunRuntimeError`. No error
  code changes. A core never recovered answers the runner's
  `not-recovered`.
- **No change to `AgentRunActorCommand`:** no arm is added, removed or
  reshaped.
- **No change to the actor's observable behaviour** (§4 anti-goals):
  - each reply's value and error;
  - each refresh-failure reply;
  - recovery on `started` and `restarted`;
  - the registry's records and the phase names `recover`, `snapshot`,
    `accept-command`, `start`, `start-graph`, `mark-graph-ready`,
    `start-graph-node`, `complete-graph-node`,
    `schedule-graph-node-effect`, `begin-step`, `succeed-step`,
    `fail-step`, `wait`, `resume`, `complete`, `fail-run`,
    `request-cancellation`, `cancel`, `begin-compensation`,
    `schedule-effect` and `due-effects`;
  - metrics.
- **No durable record changes shape.** No schema version moves.
- **Validation:** `scripts/validate.sh` and `scripts/package-check.sh` exit
  0, the debug-build stack-margin guard included. Write their output to a
  file and capture `$?`; never pipe validate into `tail`.
- **Host repository:** read-only (`/Users/paloul/Projects/paloul.rakka.host`).
- **Disk:** run `cargo clean` only at task boundaries, never mid-task.

## Review Focus

1. **An owner re-created over the same stores**, as a passivated or
   restarted host entity is, then calling `recover()`: it reaches the same
   run state, the same graph projection and the same due effects. Test:
   Task 1, `a_core_recreated_over_the_same_stores_recovers_the_run`.
2. **An operation on a core never recovered:** the runner's `not-recovered`
   error, no panic, and nothing written. Test: Task 1,
   `an_operation_on_a_core_never_recovered_is_refused`.
3. **A composed write between core operations** is not recorded in the
   snapshot registry, and the owner's next core operation records the state
   as it then stands. Test: Task 2,
   `a_composed_write_is_recorded_by_the_next_core_operation`.
4. **A genuine second writer on the run store**, not only the workflow
   store: the runner goes stale, and the next core operation refreshes it.
   Test: Task 2,
   `a_composed_write_lost_to_another_writer_is_refreshed_by_the_next_operation`,
   which covers both stores.
5. **Debug-build stack depth** of the actor's handler now that it awaits
   core futures. No existing guard measures this handler:
   `examples/coordination-capability-acceptance/tests/stack_margin.rs`
   measures the agent dispatcher's walk. The actor's tests and the
   sharded-entity tests run it on tokio's default debug test threads. Box
   the core calls if anything overflows, or if the handler's future grows
   past what it was (the #162 rule). Check: Task 3, Step 4.

---

### Task 1: The core, with the actor delegating to it

**Files:**
- Create: `crates/rakka-agent-workflow/src/run_core.rs`
- Modify:
  - `crates/rakka-agent-workflow/src/runtime.rs`: the `AgentRunActor`
    struct, constructors, accessors and handler, `:581-1157`;
  - `crates/rakka-agent-workflow/src/lib.rs`: the `pub mod` list, the
    runtime re-exports at `:283`, and the list at `:424`.
- Test: `crates/rakka-agent-workflow/tests/run_core.rs` (new)

**Interfaces:**
- Produces, in `run_core.rs`, exported from the crate root and the list at
  `lib.rs:424`:
  - `pub struct AgentRunCore<RunStore, WorkflowStore, Clock = SystemWorkflowClock>`
    with the bounds `RunStore: DurableStateStore<AgentRunState>`,
    `WorkflowStore: DurableStateStore<WorkflowState>` and
    `Clock: WorkflowClock`. Its fields are `runner`, `inbox`,
    `graph_runtime` and `snapshots`, as in §5.1.
  - **Constructors:** the actor's six, with the same parameters: `new`,
    `with_metrics`, `with_clock`, `with_clock_and_metrics`, `from_parts`
    and `with_snapshot_registry`.
  - **Accessors:** `runner(&self) -> &AgentStepRunner<RunStore>`,
    `inbox(&self) -> &AgentRunInbox<WorkflowStore, Clock>` and
    `graph_runtime(&self) -> AgentGraphRuntime`.
  - **Operations:** one `pub async fn …(&mut self, …) ->
    AgentRunRuntimeResult<T>` per row of §5.2. Each takes the arm's fields,
    with `plan: &AgentCompiledExecutionPlan` where the arm holds an `Arc`.
    `T` is the arm's reply type:

    | Method | `T` |
    |---|---|
    | `recover`, `snapshot` | `AgentRunActorSnapshot` |
    | `accept_command` | `AgentInboxAcceptance` |
    | `start` and the step/lifecycle methods | `AgentRunTransition` |
    | `start_graph` | `AgentRunTransition` |
    | `mark_graph_ready`, `start_graph_node`, `complete_graph_node` | `AgentGraphRuntimeTransition` |
    | `schedule_graph_node_effect` | `AgentGraphRuntimeEffectOutcome` |
    | `schedule_effect` | `AgentOutboxAcceptance` |
    | `due_effects` | `Vec<AgentDueEffect>` |

  - `async fn refresh_if_stale(&mut self) -> AgentRunRuntimeResult<()>`,
    private in this task and made `pub` in Task 2.
- Produces on `AgentRunActor`: `pub const fn from_core(core: AgentRunCore<…>) -> Self`
  and `pub const fn core(&self) -> &AgentRunCore<…>`.

- [ ] **Step 1: Write the failing tests** in `tests/run_core.rs`.

  Reuse runtime_actor.rs's fixtures by copying them into the new file:
  - `RuntimeFixture`, `workflow()`, `graph_accepted_run_state`,
    `effect_graph_plan` and `effect_request`;
  - `FailingRunStore` and the `spawn_actor` and ask helpers.

  Add `type TestCore = AgentRunCore<RunStore, WorkflowStore, ManualWorkflowClock>;`
  and the helper `fn core(fixture: &RuntimeFixture, run_id: AgentRunId) -> TestCore`,
  built with `with_clock_and_metrics`.

  - `the_core_and_the_actor_record_the_same_snapshots`. Drive one scripted
    effect-graph sequence twice, over separate fixtures and separate
    `AgentWorkflowSnapshotRegistry`s: once through an actor with
    `with_snapshot_registry`, once through a core with the same registry
    wiring.
    - **The sequence:** `recover`, `start_graph` at 100, `mark_graph_ready`
      at 110, `start_graph_node("input")` at 120,
      `complete_graph_node("input")` at 130, `mark_graph_ready` at 140,
      `start_graph_node("effect")` at 150,
      `schedule_graph_node_effect(effect_request(run, "effect", 160))`,
      `due_effects`, then `complete_graph_node("input")` at 170, which must
      fail.
    - **Assert:**
      - `registry_a.runtime_snapshot() == registry_b.runtime_snapshot()`;
      - `registry_a.recovery_snapshot() == registry_b.recovery_snapshot()`;
      - `registry_a.outbox_snapshot() == registry_b.outbox_snapshot()`;
      - the failing call's error, recorded in both, carries the phase
        `"complete-graph-node"`.
  - `a_core_recovery_relinks_an_effect_that_exhausted_while_it_was_down`.
    Mirror `runtime_actor.rs:1254`, with a core in place of each actor: the
    first core drives to the `effect` node `Running`, and a standalone inbox
    commits the effect and exhausts it. A second core over the same stores
    then calls `recover()`. Assert what the actor test asserts: the node
    lists the effect and the projection matches.
  - `a_core_recreated_over_the_same_stores_recovers_the_run` (Review
    Focus 1). Core A drives to the scheduled effect and is dropped. Core B
    over the same stores calls `recover()`. Assert that its
    `snapshot().await?` equals core A's last snapshot, by `run_state`,
    `graph` and `due_effect_count == 1`.
  - `an_operation_on_a_core_never_recovered_is_refused` (Review Focus 2).
    A fresh core calls `start_graph(...)` without `recover()`. Assert
    `error.code() == "not-recovered"` and that the run store holds no
    record (`fixture.run_store.load(&id).await?.is_none()`).
  - `a_failed_refresh_runs_nothing_and_records_no_operation_phase`. Mirror
    `failed_stale_recovery_keeps_refusing_until_the_store_recovers`
    (`runtime_actor.rs:726`) on a core over `FailingRunStore`:
    - make the runner stale through a lost compare-and-set, then fail
      the store's next load;
    - `begin_step` answers the load error;
    - the registry records the error under `"recover"` and nothing under
      `"begin-step"`;
    - the run record is unchanged.

  `an_actor_hosts_a_core_built_elsewhere`: build a core, call
  `AgentRunActor::from_core(core)`, spawn it, and ask `Snapshot`. Assert
  that `snapshot.run_id` is the core's run id, and that a `StartGraph`
  through the actor is visible from a new core recovered over the same
  stores.

- [ ] **Step 2: Run them, and watch them fail.**

  Run: `cargo test -p rakka-agent-workflow --all-features --test run_core`
  Expected: compile errors. `AgentRunCore` and `AgentRunActor::from_core`
  are not found.

- [ ] **Step 3: Implement the core in `src/run_core.rs`.**
  - Move these bodies out of `AgentRunActor` unchanged:
    `refresh_if_stale`, `recover_components` (as `recover`, which records
    under `"recover"`), `reconcile_recovered_graph_effects`, the private
    `snapshot` read, `record_snapshot_result`, `record_current_snapshot`
    and `record_operation_result`.
  - Give each operation method the shape of its handler arm:
    `self.refresh_if_stale().await?`, then the call, then
    `self.record_operation_result(phase, &result)`, then return `result`.
  - **Exceptions:**
    - `recover` does not refresh first;
    - `snapshot` refreshes, then reads, then records under `"snapshot"`
      through `record_snapshot_result`.
  - **The module doc** says the core is for an owner that is already a
    single writer, and that the actor is one such owner (§7).

- [ ] **Step 4: Make the actor a wrapper in `src/runtime.rs`.**
  - `AgentRunActor` holds `core: AgentRunCore<RunStore, WorkflowStore, Clock>`.
  - Its constructors build the core through the core's constructors.
  - `runner()`, `inbox()` and `graph_runtime()` delegate. Add `from_core`
    and `core`.
  - `started` and `restarted` call `self.core.recover()`.
  - **The handler:** each arm becomes
    `let _reply_dropped = reply_to.reply(self.core.<method>(…).await);`,
    passing `&plan` for an `Arc` plan.
  - **Delete** the handler's leading `refresh_if_stale` block and
    `reply_refresh_failure`. The core's methods refresh first and answer
    the same `Err`.
  - Export `AgentRunCore` at `lib.rs:283` (beside `AgentRunActor`) and in
    the list at `:424`, and add `mod run_core;` to the module list.

- [ ] **Step 5: Run the new file and the actor's files, and watch them pass.**

  Run:
  `cargo test -p rakka-agent-workflow --all-features --test run_core --test runtime_actor --test runtime_snapshots --test failure_injection`
  Expected: all pass. `runtime_actor`, `runtime_snapshots` and
  `failure_injection` pass unmodified.

- [ ] **Step 6: Run the whole crate, the actor's other users, and clippy.**

  Run, each to a log file:
  - `cargo test -p rakka-agent-workflow --all-features`
  - `cargo test -p rakka-a2a --all-features`
  - `cargo test -p rakka-example-clustered-agent-workflow-http-grpc`
  - `cargo clippy -p rakka-agent-workflow --all-targets --all-features -- -D warnings`

  Expected: 0 failed, and clippy clean.

- [ ] **Step 7: Commit.**

  ```bash
  git add crates/rakka-agent-workflow/src/run_core.rs crates/rakka-agent-workflow/src/runtime.rs crates/rakka-agent-workflow/src/lib.rs crates/rakka-agent-workflow/tests/run_core.rs
  git commit -m "Move the run actor's internals into a public AgentRunCore the actor wraps (issue #71)"
  ```

### Task 2: Composition access: `parts_mut` and a public refresh

**Files:**
- Modify: `crates/rakka-agent-workflow/src/run_core.rs`
- Test: `crates/rakka-agent-workflow/tests/run_core.rs`

**Interfaces:**
- Consumes: Task 1's `AgentRunCore` and its operations.
- Produces:
  - `pub struct AgentRunParts<'a, RunStore, WorkflowStore, Clock>` with
    public fields:
    - `pub runner: &'a mut AgentStepRunner<RunStore>`
    - `pub inbox: &'a mut AgentRunInbox<WorkflowStore, Clock>`
    - `pub graph_runtime: AgentGraphRuntime`
  - `pub fn parts_mut(&mut self) -> AgentRunParts<'_, RunStore, WorkflowStore, Clock>`
  - `pub async fn refresh_if_stale(&mut self) -> AgentRunRuntimeResult<()>`
    (now public)
  - Both are exported from the crate root and the list at `lib.rs:424`.

- [ ] **Step 1: Write the failing tests** in `tests/run_core.rs`.

  Copy `command_metadata` and `artifact` from `effect_bridge.rs:980-1023`,
  and add a recording `OutboxDispatcher` whose `dispatch` returns success
  and counts entries.

  - **`the_hosts_composed_writes_need_no_recover`.** One core, no
    `recover()` between steps:
    1. Through the core: `start_graph` at 100, `mark_graph_ready` at 110,
       `start_graph_node("input")` at 120, `complete_graph_node("input")`
       at 130, `mark_graph_ready` at 140, `start_graph_node("effect")` at
       150, and `schedule_graph_node_effect(effect_request(run, "effect", 160))`.
    2. Composed through `let parts = core.parts_mut();`:
       - `parts.inbox.inner_mut().dispatch_due_outbox(&mut dispatcher).await?`;
       - read the graph state from `parts.runner.state()?`;
       - build `effect_completed_command(command_metadata(&plan, run, "cmd-effect-completed", 200), effect_id, Some(artifact(...)))`;
       - `parts.graph_runtime.effect_bridge().accept_and_apply_effect_completed(&plan, graph, command, parts.inbox).await?`;
       - `parts.runner.update_graph_state(outcome.transition.state, AgentTimestampMillis::new(200)).await?`.
    3. Through the core: `mark_graph_ready` at 210,
       `start_graph_node("terminal")` at 220, and
       `complete_graph_node("terminal")` at 230.

    Assert:
    - every call is `Ok`;
    - `dispatcher.count == 1`;
    - the final `core.snapshot().await?.graph` shows all three nodes
      `Completed` and `due_effect_count == 0`.
  - **`a_second_inbox_and_runner_lose_the_compare_and_set_the_core_does_not`.**
    The hazard control: the same steps 1 and 2, with step 2 made through a
    second `AgentRunInbox` and `AgentStepRunner` over the same stores
    (recovered once before step 1). Then the core's
    `mark_graph_ready(…, 210)` must answer an error whose message contains
    "revision". This shows the first test reaches the hazard the host lives
    with.
  - **`parts_mut_hands_out_the_cores_own_components`.** Accept a command
    through `core.parts_mut().inbox.accept_command(cmd)`. Assert that
    `core.inbox().inner().recoverable_inbox()?.len()` grew by one without a
    `recover()`, and that `core.snapshot().await?.recoverable_command_count`
    agrees.
  - **`a_composed_write_lost_to_another_writer_is_refreshed_by_the_next_operation`**
    (Review Focus 4). Two cases, each from a core at the scheduled effect:
    - *(a) Workflow store:* a second `AgentRunInbox` over the same store
      accepts a command. The core's composed
      `parts.inbox.accept_command(other)` then answers a revision conflict,
      and `core.inbox().is_stale()` holds. The next `core.due_effects()` is
      `Ok`, with one entry.
    - *(b) Run store:* a second `AgentStepRunner` persists a graph
      transition. The core's composed
      `parts.runner.update_graph_state(...)` answers a revision conflict,
      and `core.runner().is_stale()` holds. The next
      `core.snapshot().await` is `Ok` and shows the second runner's
      transition.
  - **`refresh_if_stale_recovers_only_a_stale_core`.** On a fresh, recovered
    core, `refresh_if_stale()` is `Ok` and records nothing in the registry.
    After case (a)'s lost write, `refresh_if_stale()` clears `is_stale()`
    and records under `"recover"`.
  - **`a_composed_write_is_recorded_by_the_next_core_operation`** (Review
    Focus 3). With a registry:
    1. after `schedule_graph_node_effect`, read
       `registry.outbox_snapshot()`, which shows due 1;
    2. dispatch the due outbox through `parts_mut()` with a dispatcher that
       succeeds. The registry still shows due 1;
    3. call `core.snapshot().await?`. The registry now shows due 0.

- [ ] **Step 2: Run them, and watch them fail.**

  Run: `cargo test -p rakka-agent-workflow --all-features --test run_core`
  Expected: compile errors. `parts_mut` is not found, and
  `refresh_if_stale` is private.

- [ ] **Step 3: Implement `parts_mut` and the public `refresh_if_stale`** in
  `src/run_core.rs`, with rustdoc stating §5.4's contract:
  - start a composed write with `refresh_if_stale()`;
  - compute it from state read after that refresh;
  - a lost compare-and-set leaves the component stale;
  - composed writes are not recorded.

  Export `AgentRunParts`.

- [ ] **Step 4: Run them, and watch them pass.**

  Run: `cargo test -p rakka-agent-workflow --all-features --test run_core`
  Expected: all pass. The hazard control's error contains "revision".

- [ ] **Step 5: Run the crate and clippy, as Task 1 Step 6 did.** Expected:
  0 failed, and clippy clean.

- [ ] **Step 6: Commit.**

  ```bash
  git add crates/rakka-agent-workflow/src/run_core.rs crates/rakka-agent-workflow/src/lib.rs crates/rakka-agent-workflow/tests/run_core.rs
  git commit -m "Let an AgentRunCore's owner compose writes on its own runner and inbox (issue #71)"
  ```

### Task 3: Docs, breaks and validation

**Files:**
- Modify:
  - `crates/rakka-agent-workflow/src/runtime.rs`: the
    `AgentRunActorCommand` doc names the core;
  - `docs/rakka-api-boundary-inventory.md`: the `rakka-agent-workflow` row
    at `:59`;
  - `docs/rakka-agents.md`: the "What ships" row at `:37`;
  - `docs/rakka-compatibility.md`: a new entry, last in its list, before
    "Run the default compatibility matrix";
  - `CHANGELOG.md`: under the current section's Added.

**Interfaces:** consumes Tasks 1 and 2. Produces nothing new in code.

- [ ] **Step 1: Write the docs.**
  - **`AgentRunActorCommand`'s doc:** a write the enum has no arm for is
    made through an owner-held `AgentRunCore`, not through a second inbox
    and runner.
  - **Inventory row:** add "an owner-held run core (`AgentRunCore`,
    `AgentRunParts`) that the run actor wraps".
  - **`rakka-agents.md`, the "What ships" row:** one clause on the same.
  - **Compatibility entry:**
    - additive;
    - the new items are `AgentRunCore`, `AgentRunParts`, and
      `AgentRunActor::{from_core, core}`;
    - no arm, error code, phase name, metric or durable record changes;
    - the actor's refresh-failure reply is unchanged, now answered by the
      core;
    - the single-writer contract (§5.4), including "compute from state
      read after the refresh".
  - **CHANGELOG:** one Added bullet naming #71's item 1, the core, and
    why: an owner that is already a single writer can drive a run with no
    second inbox and runner.

  Check the doc tests: `cargo test -p rakka-agent --all-features --test compatibility_currency --test product_doc_currency`.
  Expected: pass.

- [ ] **Step 2: Run the breaks**, each alone in `src/run_core.rs` or
  `src/runtime.rs`, restored from the git index afterwards. Run
  `--test run_core --test runtime_actor --test runtime_snapshots`. Each must
  fail at least one test:
  - **B1:** `begin_step` skips its `refresh_if_stale()`.
  - **B2:** `mark_graph_ready` records under `"mark-graph-ready-x"`.
  - **B3:** the actor's `CompleteGraphNode` arm calls
    `core.start_graph_node`.
  - **B4:** `recover` skips `reconcile_recovered_graph_effects`.
  - **B5:** `parts_mut` hands out a freshly built, recovered inbox instead
    of `&mut self.inbox`. Write a helper that builds one, so the break
    compiles.
  - **B6:** `started` does not recover.

  Record each one's failing tests in the ledger.

- [ ] **Step 3: Run the full validation.**

  Run:
  - `scripts/validate.sh > <ledger dir>/validate.log 2>&1; echo $?`
  - `scripts/package-check.sh > <ledger dir>/package.log 2>&1; echo $?`

  Expected: both `0`.

- [ ] **Step 4: Check the stack margin** (Review Focus 5).
  - Compare the actor handler's future size before and after the change:
    `std::mem::size_of_val(&actor.handle(ctx, msg))` from a scratch test, or
    `-Zprint-type-sizes` if available. Record both numbers in the ledger.
  - If the future grew, or anything in `validate.log` overflowed a stack,
    box the core calls in the handler (`Box::pin(self.core.x(..)).await`)
    and rerun Step 3.
  - `stack_margin.rs` must still pass in `validate.log`.

- [ ] **Step 5: Commit.**

  ```bash
  git add crates/rakka-agent-workflow/src/runtime.rs docs/rakka-api-boundary-inventory.md docs/rakka-agents.md docs/rakka-compatibility.md CHANGELOG.md
  git commit -m "Document AgentRunCore (issue #71)"
  ```

The #71 comment with the host's migration notes and the item 4 correction
(§8) is not a plan step. It is posted at the finish, with the owner's
approval, because it is outward-facing.
