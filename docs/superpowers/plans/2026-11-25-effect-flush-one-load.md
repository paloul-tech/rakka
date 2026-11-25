# One Workflow-Record Load per Flush Round Implementation Plan (issue #25)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A run's effect flush loads the workflow record once per round,
whatever the number of `Ready` effects. The load is counted in the budget,
measured both ways, and measured on PostgreSQL.

**Architecture:**
- `AgentRunEffectSink` gains a provided `dispatch_all`, which hands over
  several effects in order, reporting each acceptance.
- `WorkflowAgentRunEffectSink` overrides it: one recovered inbox, the effects
  scheduled against it, and a revision conflict reloaded and retried.
- `AgentRunEntityStore::dispatch_effects` makes one `dispatch_all` call per
  round, and closes each effect's `EffectSchedule` segment at its own
  acceptance.
- The testkit store counts loads.

**Tech Stack:** Rust 1.88, tokio, `rakka-agent` testkit
(`CrashingStateStore`), `rakka-persistence-postgres` plus `tokio-postgres`
for the gated test.

**Spec:** `docs/superpowers/specs/2026-11-25-effect-flush-one-load-design.md`
(commits `18d2a80f` and `ac29d32b`). Section numbers (§) refer to it.

## Global Constraints

- **Lints:** MSRV 1.88; `unsafe_code = "forbid"`; every public item has a
  doc comment; clippy `-D warnings` is clean.
- **The flush does what it did** (§4 anti-goals):
  - `mark_ready` commits before any sink write;
  - every `Ready`, unresolved effect is re-handed over each round;
  - a winding-down or ended run hands over only the
    `exempt_from_wind_down_fence` kinds;
  - the first error that is not a revision conflict ends the flush, and the
    effects after it are not handed over in that round;
  - error codes are unchanged (`outbox-schedule-failed` and the
    `sink_error` mapping).
- **No implementer breaks.** `dispatch_all` is a provided method.
- **The conflict bound** is three re-applications per effect, the existing
  `INBOX_WRITE_REAPPLICATIONS` in `crates/rakka-agent/src/dispatch.rs`.
- **Segments:** each segment keeps its `now` anchor (`AgentSegmentTimer::start(now)`).
  Effect `i`'s duration runs from the flush's start for `i = 0`, otherwise
  from effect `i - 1`'s acceptance.
- **The PostgreSQL test** is gated on `RAKKA_POSTGRES_TEST_DSN`. It says it
  skipped without the variable. Its new dependencies are dev-dependencies
  only. Wall time is printed, never asserted.
- **Validation:** `scripts/validate.sh` and `scripts/package-check.sh` exit 0.
  Write their output to a file and capture `$?`.
- **Disk:** run `cargo clean` only at task boundaries.

## Review Focus

1. **A wind-down mid-flush:** a run that is winding down, with both exempt
   and fenced `Ready` effects. Only the exempt ones are handed over, in one
   load. Test: Task 3,
   `a_winding_down_run_hands_over_only_its_exempt_effects_in_one_load`.
2. **A sink whose `dispatch_all` reports an index out of order or twice:** a
   third-party sink's bug. The flush closes each effect's segment once and
   ignores an index it has already closed or that is out of range. Test:
   Task 3, `a_sink_that_reports_an_acceptance_twice_closes_one_segment`.
3. **A batch whose record vanished between rounds** (the lost-row repair):
   the next round's single load finds no record, and every row is scheduled
   again. Test: Task 3,
   `a_lost_record_is_put_back_for_every_effect_in_one_load`.
4. **A conflict on the batch's first write:** the reload happens before any
   acceptance is reported, and no acceptance is reported twice. Test:
   Task 2, `a_conflict_on_the_first_write_is_retried_and_reports_each_acceptance_once`.
5. **Debug-stack headroom of the flush:** the smallest `RUST_MIN_STACK` for
   `effect_dispatch`, before and after. Check: Task 5, Step 3.

---

### Task 1: Count workflow-store loads, and pin today's budget

**Files:**
- Modify: `crates/rakka-agent/src/testkit.rs`
  (`CrashingStateStore`, around `:3896-4075`)
- Modify: `crates/rakka-agent/tests/effect_dispatch.rs`
  (the budget constants at `:2105-2112`, and the test at `:2115`)

**Interfaces:**
- Produces:
  - `CrashingStateStore::loads(&self) -> usize`;
  - `CrashingStateStore::reset_loads(&self)`;
  - every call to `load` increments the counter, whatever it answers,
    before its hook runs;
  - `const TURN_BUDGET_WORKFLOW_STORE_LOADS: usize`.

- [ ] **Step 1: Extend the budget test.**

  In `per_turn_durable_write_count_and_latency_measurement`:
  - next to `fx.workflow_store.reset_writes()`, add
    `fx.workflow_store.reset_loads()`;
  - after the run completes, read
    `let workflow_loads = fx.workflow_store.loads();`, add it to the
    `println!`, and assert it equals `TURN_BUDGET_WORKFLOW_STORE_LOADS`;
  - declare the constant with a placeholder of `0` and a doc comment saying
    it counts the workflow store's loads in one clean turn.

- [ ] **Step 2: Run it, and watch it fail.**

  Run: `cargo test -p rakka-agent --all-features --test effect_dispatch per_turn_durable_write_count_and_latency_measurement -- --nocapture`
  Expected: compile errors, since `reset_loads` and `loads` are not found.

- [ ] **Step 3: Add the load counter to `CrashingStateStore`.**

  It is an `Arc<AtomicUsize>` field beside `writes`. Clone it through the
  same `Clone` path as `writes`, and increment it at the top of `load`.

- [ ] **Step 4: Measure, and pin today's value.**

  Run Step 2's command. Expected: the assertion fails with the measured load
  count in the `println!`. Set the constant to that value, and record it in
  the ledger as "base workflow-store loads per turn". Rerun. Expected: pass.

- [ ] **Step 5: Run the crate's tests that use the testkit store.**

  Run: `cargo test -p rakka-agent --all-features --test effect_dispatch --test dispatch_missing_row --test dispatch_lost_fleet_writes`
  Expected: pass.

- [ ] **Step 6: Commit.**

  ```bash
  git add crates/rakka-agent/src/testkit.rs crates/rakka-agent/tests/effect_dispatch.rs
  git commit -m "Count the workflow store's loads in the per-turn budget (issue #25)"
  ```

### Task 2: `dispatch_all`, and the workflow sink's one-load override

**Files:**
- Modify: `crates/rakka-agent/src/effect.rs` (`AgentRunEffectSink`, around `:3007`)
- Modify: `crates/rakka-agent/src/dispatch.rs` (`WorkflowAgentRunEffectSink`, `:400-500`)
- Test: `crates/rakka-agent/tests/effect_flush_one_load.rs` (new)

**Interfaces:**
- Consumes: Task 1's `loads()` and `reset_loads()`.
- Produces: a provided trait method on `AgentRunEffectSink`:

  ```rust
  fn dispatch_all<'a>(
      &'a self,
      scope: &'a AgentRunScope,
      effects: &'a [AgentEffect],
      accepted: &'a mut (dyn FnMut(usize) + Send),
  ) -> AgentEffectFuture<'a, ()>
  ```

  - **The default** calls `self.dispatch(scope, &effects[i])` in order and
    `accepted(i)` after each success. It returns the first error.
  - **The override on `WorkflowAgentRunEffectSink`:**
    - one `AgentRunInbox` and one `recover()`;
    - per effect: `ticket_outbox_command`, then
      `inbox.inner_mut().schedule_outbox(command)`, then `accepted(i)`;
    - on an error whose `code()` is `"revision-conflict"`: `recover()`
      again and re-apply the same effect, at most `INBOX_WRITE_REAPPLICATIONS`
      times;
    - any other error, or a conflict past the bound, maps exactly as
      `dispatch` maps it, and is returned;
    - an empty `effects` returns `Ok(())` without building an inbox.

- [ ] **Step 1: Write the failing tests** in `tests/effect_flush_one_load.rs`.

  Build three effects from a real run, as `ready_tool_ticket` in
  `tests/dispatch_missing_row.rs:89` builds its fixture: an
  `AuthorityFixture::new(adapter, AgentToolAuthority::new(tool_registry_for_spec(TOOL, spec)), None)`
  whose turn 1 calls `TOOL` three times (`call-1`, `call-2` and `call-3`),
  then `start()` and `pump_until_tool_ticket()`. That file's
  `delete_workflow_record` loads the record once itself, so reset the load
  counter after calling it. Take
  `fx.effect_at(slot).expect(..).to_workflow_effect(&run_scope())` for
  slots 1 to 3. The sink is
  `WorkflowAgentRunEffectSink::new(fx.workflow_store.clone(), fx.wf_clock.clone())`
  over a **fresh** run scope's record (a `run_scope()` whose record holds
  none of these rows). If the fixture's own record already holds them,
  delete it first, as `dispatch_missing_row.rs:109` does.

  - **`dispatch_all_loads_the_record_once_for_every_effect`.**
    `reset_loads()`, then `dispatch_all` over the three. Assert:
    - `loads() == 1`;
    - `writes() == 3`;
    - the accepted indices are `[0, 1, 2]`.

    A second `dispatch_all` over the same three, all duplicates now, gives
    `loads() == 1` (after a reset), `writes() == 0` and the indices
    `[0, 1, 2]`.
  - **`a_conflict_mid_batch_is_retried_and_every_effect_is_accepted`.**
    Arm `fx.workflow_store.crash_at(2, CrashPoint::ConflictBeforeWrite)`,
    then `dispatch_all` over the three. Assert:
    - `Ok`;
    - the indices are `[0, 1, 2]`;
    - `loads() == 2`;
    - all three rows exist (`OutboxStatus::Pending`);
    - `assert_crash_fired(2, ConflictBeforeWrite)`.
  - **`a_conflict_on_the_first_write_is_retried_and_reports_each_acceptance_once`**
    (Review Focus 4): the same, armed at write 1. The indices are exactly
    `[0, 1, 2]`, with no duplicates.
  - **`a_conflict_past_the_bound_ends_the_batch`.**
    `conflict_from(2)`, so every write from the second on loses. Assert:
    - `Err` with `code() == "outbox-schedule-failed"`;
    - the indices are `[0]`;
    - `loads() == 1 + INBOX_WRITE_REAPPLICATIONS` (that is, 4);
    - rows 2 and 3 are absent.
  - **`a_store_failure_ends_the_batch_at_the_effect_it_hit`.**
    `crash_at(2, CrashPoint::BeforeWrite)`. Assert:
    - `Err` with the sink's code;
    - the indices are `[0]`;
    - `loads() == 1`, so a non-conflict error is not retried;
    - row 1 exists, rows 2 and 3 are absent.
  - **`the_default_dispatch_all_goes_effect_by_effect`.** A wrapper sink that
    implements only `dispatch` and forwards it to an inner
    `WorkflowAgentRunEffectSink`, as rakka.host's `RosterEffectSink` does.
    Its `dispatch_all` over the three gives:
    - `Ok` and the indices `[0, 1, 2]`;
    - `loads() == 3`, one per effect, which is the host follow-up §7
      describes.

- [ ] **Step 2: Run them, and watch them fail.**

  Run: `cargo test -p rakka-agent --all-features --test effect_flush_one_load`
  Expected: compile errors, since `dispatch_all` is not found.

- [ ] **Step 3: Implement the provided method in `effect.rs` and the
  override in `dispatch.rs`,** as the Interfaces block says. The rustdoc on
  the trait method says:
  - effects are handed over in order;
  - acceptance is reported per index, once;
  - the first error ends the call;
  - a sink that wraps another should forward `dispatch_all`, or it keeps
    per-effect cost.

- [ ] **Step 4: Run them, and watch them pass.** Same command. Expected:
  pass.

- [ ] **Step 5: Run the crate's sink users and clippy.**

  Run:
  - `cargo test -p rakka-agent --all-features --test effect_dispatch --test dispatch_missing_row --test effect_flush_one_load`
  - `cargo clippy -p rakka-agent --all-targets --all-features -- -D warnings`

  Expected: pass, and clippy clean.

- [ ] **Step 6: Commit.**

  ```bash
  git add crates/rakka-agent/src/effect.rs crates/rakka-agent/src/dispatch.rs crates/rakka-agent/tests/effect_flush_one_load.rs
  git commit -m "AgentRunEffectSink::dispatch_all: the workflow sink loads the record once per batch (issue #25)"
  ```

### Task 3: The flush hands a round's effects over in one call

**Files:**
- Modify: `crates/rakka-agent/src/run.rs` (`dispatch_effects`, `:10411-10498`)
- Test: `crates/rakka-agent/tests/effect_flush_one_load.rs`

**Interfaces:**
- Consumes: Task 2's `dispatch_all`.
- Produces: no new API.

- [ ] **Step 1: Write the failing tests** in `tests/effect_flush_one_load.rs`,
  driving the run itself (`fx.settle()`, or the fixture's settle helper).
  The fixture is the same three-tool-call run, with no worker pumped, so
  the three effects stay `Ready` and unresolved.

  - **`a_settle_hands_several_ready_effects_over_with_one_load`.**
    `reset_loads()`, then settle three times with nothing else happening.
    Assert `loads() == 3` (one per settle, each settle stopping after one
    round). The base, measured here first, makes 9: record it in the
    ledger.
  - **`a_lost_record_is_put_back_for_every_effect_in_one_load`** (Review
    Focus 3):
    1. delete the run's workflow record as `dispatch_missing_row.rs:109`
       does, then `fx.wf_clock.advance(1_000)`;
    2. `reset_loads()`, then settle;
    3. assert all three rows exist again as `Pending`, and `loads() == 1`.

    Then the same with one row removed rather than the whole record: load
    the record, remove one outbox entry, and compare-and-set it back. If
    `WorkflowState`'s outbox cannot be edited from a test, rule and ledger
    it.
  - **`a_sink_failure_mid_flush_ends_it_and_the_next_settle_hands_over_all`.**
    Delete the record, then `crash_at(2, CrashPoint::BeforeWrite)`, then
    settle the run directly (`fx.fx.run()`, `recover(now)`,
    `settle_side_effects(&fx.fx.router, now)`), because
    `AuthorityFixture::settle` (`common/mod.rs:3343`) expects success.
    Assert:
    - the settle's reply is an error with `effect-sink-failed`, as the
      entity maps a sink error;
    - row 1 exists, rows 2 and 3 are absent;
    - the run's three effects are still `Ready`.

    Then `survive()` and settle again: all three rows exist.
  - **`each_effect_closes_one_schedule_segment_at_its_acceptance`.** Wire an
    `InMemoryAgentSegmentSink`, as `tests/telemetry_segments.rs:32` does.
    Settle once with the three effects `Ready`. Assert:
    - exactly three `EffectSchedule` segments come from that settle, all
      `AgentSegmentOutcome::Ok`;
    - each `start == now`, the anchor;
    - their `end`s are non-decreasing.

    With the failure of the previous test, assert one `Ok` segment, then one
    `Error` with `error_type == Some("rakka.agent.outbox")`, and no third.
  - **`a_winding_down_run_hands_over_only_its_exempt_effects_in_one_load`**
    (Review Focus 1):
    1. a run with the three tool effects `Ready` and one exempt effect
       `Ready` (a memory promotion; see `private_memory_promotion.rs` for
       how one is made `Ready`);
    2. cancel the run;
    3. delete the record and settle.

    Assert: only the exempt effect's row is re-created, and
    `loads() == 1`.
  - **`a_sink_that_reports_an_acceptance_twice_closes_one_segment`**
    (Review Focus 2). A test sink whose `dispatch_all` dispatches each
    effect and calls `accepted(i)` twice for `i = 0`, then once with an
    index past the end. It wraps an `InMemoryAgentRunEffectSink`. Install
    it with `Fixture::with_sink` (`common/mod.rs:906`); the run's model
    effect is enough to have one `Ready`. Settle. Assert:
    - exactly one `EffectSchedule` segment per effect;
    - no panic.

- [ ] **Step 2: Run them, and watch them fail.**

  Run: `cargo test -p rakka-agent --all-features --test effect_flush_one_load`
  Expected:
  - the one-load tests fail, with 9 against 3, and with 3 against 1;
  - the segment and double-report tests may pass at the base, since the
    loop closes one segment per effect. Record which pass.

- [ ] **Step 3: Rewrite the re-drive loop in `dispatch_effects`.**
  1. Build `records: Vec<AgentEffect>` and the matching `kinds` from the
     filtered `ready` (the same filter as today).
  2. If it is empty, skip.
  3. Otherwise, start one `AgentSegmentTimer::start(now)`, and call
     `self.effects.dispatch_all(&self.scope, &records, &mut accepted)`.
     - **The closure:** for an index `i` that equals the number of segments
       closed so far, close the current timer as `EffectSchedule { kinds[i] }`,
       push it to a local `Vec<AgentTelemetrySegment>`, and start a new
       timer at `now`.
     - **Any other index is ignored** (Review Focus 2).
  4. After the call:
     - on `Err`, close the current timer as failed under
       `rakka.agent.outbox` with the error's code, if an effect remains
       unclosed;
     - call `self.close_segment(..)` for each segment, in order;
     - then propagate the error.

  The closure must not borrow `self`; it pushes to locals only.

- [ ] **Step 4: Run them, and watch them pass.** Same command. Expected:
  pass.

- [ ] **Step 5: Run the budget, the crate and clippy.**
  - Run `effect_dispatch`'s budget test. If `TURN_BUDGET_WORKFLOW_STORE_LOADS`
    moved, set it to the new value, update the table in
    `examples/durable-agent-acceptance/README.md` (Task 5), and record both
    numbers in the ledger. If it did not move, record that.
  - Run `cargo test -p rakka-agent --all-features --no-fail-fast` (to a log)
    and clippy. Expected: 0 failed.

- [ ] **Step 6: Commit.**

  ```bash
  git add crates/rakka-agent/src/run.rs crates/rakka-agent/tests/effect_flush_one_load.rs crates/rakka-agent/tests/effect_dispatch.rs
  git commit -m "The run's flush hands a round's effects to the sink in one call (issue #25)"
  ```

### Task 4: The PostgreSQL measurement (gated)

**Files:**
- Modify: `crates/rakka-agent/Cargo.toml`. Add dev-dependencies
  `rakka-persistence-postgres` (path, as `rakka-agent-workflow` names it)
  and `tokio-postgres` (workspace).
- Test: `crates/rakka-agent/tests/effect_flush_postgres.rs` (new)

**Interfaces:**
- Consumes: Task 2's `dispatch_all`, and Task 3's fixture shape.
- Produces: no API. A private `JsonWorkflowStateCodec` implementing
  `rakka_persistence::StateCodec<WorkflowState>` with `serde_json`, and a
  private `CountingStore<S>` wrapping any `DurableStateStore<S>` that counts
  `load` calls.

- [ ] **Step 1: Write the gated test**
  `per_round_workflow_loads_and_wall_time_on_postgres`.
  - **The gate.** Read `RAKKA_POSTGRES_TEST_DSN`. Without it, print a skip
    line and return.
  - **The store.** Connect with `tokio_postgres::connect(.., NoTls)`, spawn
    the connection, build
    `PostgresDurableStateStore::new(client, JsonWorkflowStateCodec)`,
    `migrate()`, and wrap it in `CountingStore`.
  - **The effects.** Take three effects from the in-memory three-tool-call
    run (Task 2's helper, made shared or copied).
  - **The scope.** For each history size `h` in `[0, 50, 200]`, use a fresh
    run scope. Its run id carries `h` and the current time in milliseconds,
    so a rerun never finds the previous run's record in the shared table.
    - Seed `h` finished rows through an `AgentRunInbox` over the counting
      store: `schedule_outbox`, then `record_outbox_success`
      (`rakka-workflow/src/inbox.rs:549`), for synthetic ids.
    - Print the record's row count after seeding, so the posted numbers say
      what was measured.
  - **The measurement.** Schedule the three rows once (the first flush).
    Then, with the rows present, which is the steady state:
    - **before:** `reset`, then `dispatch` each effect, three loads. Measure
      the wall time over 20 repetitions, and report the median;
    - **after:** `reset`, then `dispatch_all`, one load, measured the same
      way.
  - **The output.** Print one line per `h`:
    `history={h} rows={n} before: loads=3 median={..} after: loads=1 median={..}`.
  - **The assertions:** loads only, 3 and 1.

- [ ] **Step 2: Run it without the gate.**

  Run: `cargo test -p rakka-agent --all-features --test effect_flush_postgres -- --nocapture`
  Expected: pass, with a skip line.

- [ ] **Step 3: Run it against the local Docker PostgreSQL.**

  Run, with output to a log:
  `RAKKA_POSTGRES_TEST_DSN=postgres://postgres:postgres@localhost:5432/postgres cargo test -p rakka-agent --all-features --test effect_flush_postgres -- --nocapture`
  - Expected: pass, with three lines.
  - Record the lines in the ledger, with `rustc --version` and
    `SELECT version()`. The credentials are the documented default. If they
    differ, find the container's settings with `docker inspect`, never
    guessing a password into a log.

- [ ] **Step 4: Commit.**

  ```bash
  git add crates/rakka-agent/Cargo.toml crates/rakka-agent/tests/effect_flush_postgres.rs Cargo.lock
  git commit -m "Measure the flush's per-round workflow loads on PostgreSQL (issue #25)"
  ```

### Task 5: Docs, breaks and validation

**Files:**
- Modify:
  - `docs/rakka-agents.md`: where the flush or the effect sink is described.
    Grep for `effect sink`, `flush` and `Ready`;
  - `docs/rakka-compatibility.md`: a new last entry, before "Run the
    default compatibility matrix";
  - `CHANGELOG.md`: Changed and Added;
  - `examples/durable-agent-acceptance/README.md`: the budget table, adding a
    workflow-store loads row with Task 1's value, and Task 3's if it moved.

- [ ] **Step 1: Write the docs.**
  - **The compatibility entry:**
    - one workflow-record load per flush round;
    - the provided `dispatch_all`, and that a wrapping sink should forward
      it;
    - the conflict retry, with its bound;
    - that `EffectSchedule` segments keep their anchor, and that durations
      after the first exclude the shared load;
    - the testkit's `loads()` and `reset_loads()`;
    - no error code changes.
  - **CHANGELOG:**
    - Changed: one load per round;
    - Added: `dispatch_all` and the load counter.
  - **README:** the loads row, the reproduce command, and the PostgreSQL
    command.

  Run: `cargo test -p rakka-agent --all-features --test compatibility_currency --test product_doc_currency`
  Expected: pass.

- [ ] **Step 2: Run the breaks**, each alone, with sources restored from the
  git index (stage first). Run `--test effect_flush_one_load --test
  effect_dispatch`. Each must fail at least one test:
  - **B1:** the override recovers per effect (move `recover()` inside the
    loop);
  - **B2:** the flush filters out effects already `Ready` (re-driving only
    newly ready ones);
  - **B3:** the flush's closure ignores every acceptance;
  - **B4:** the override goes on after a non-conflict error;
  - **B5:** the override does not retry a revision conflict.

  Record each one's failing tests in the ledger.

- [ ] **Step 3: Run the full validation, and measure the stack.**
  - Run `scripts/validate.sh` and `scripts/package-check.sh`, each to a log,
    capturing `$?`. Expected: both `0`.
  - Build `effect_dispatch` and `effect_flush_one_load`. Find the smallest
    `RUST_MIN_STACK` at which each binary passes (bisect in 8 KiB steps).
    Build first: rustc itself fails under a small stack. Do it at this head,
    and at the plan's base commit with a temporary worktree and a shared
    `CARGO_TARGET_DIR`. Record both. If the head needs more, box the
    `dispatch_all` call and rerun.

- [ ] **Step 4: Commit.**

  ```bash
  git add docs/rakka-agents.md docs/rakka-compatibility.md CHANGELOG.md examples/durable-agent-acceptance/README.md
  git commit -m "Document the one-load flush (issue #25)"
  ```

These are not plan steps. At the finish, with the owner's approval because
they are outward-facing, comment on #25 with:
- spot 2 already being gone;
- Task 4's numbers;
- the host follow-up (§7).
