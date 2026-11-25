# One workflow-record load per flush round (issue #25)

Design record, 2026-11-25. Issue: #25. Branch: `effect-flush-one-load`.

## 1. Intent

Issue #25 (July 2026) flagged two spots that multiply store traffic on every
settle pass of every active run. Its 2026-09-14 comment asks that, before a
consumer fixes a memory-sweep interval against a real store, someone either
measures the per-pass cost on PostgreSQL and records it on the issue, or
lands a cut.

**Where things stand on `main` (`a55689b3`):**
- **Spot 2 is gone.** The wind-down fence `fence_run` built and recovered a
  fresh inbox per effect. It was replaced by the agent dispatcher's per-run
  sweep, which reads nothing extra on an ordinary pass.
- **Spot 1 stands, and it is reads, not writes.**
  - `AgentRunEntityStore::dispatch_effects` (`crates/rakka-agent/src/run.rs`,
    around `:10411`) hands every `Ready`, unresolved effect to the sink, up to
    `AGENT_RUN_MAX_PENDING_EFFECTS = 8`.
  - It does this in every round of a settle, and a settle runs up to
    `AGENT_RUN_MAX_SETTLE_ROUNDS = 4` rounds. A run that is only waiting
    stops after one.
  - `WorkflowAgentRunEffectSink::dispatch` (`crates/rakka-agent/src/dispatch.rs:470`)
    builds an `AgentRunInbox` and `recover()`s it for each effect. That is a
    full load and decode of the run's workflow record, which grows with the
    run's history.
  - The row is almost always there already, so `schedule_outbox` answers
    `Duplicate` before any compare-and-set.
  - So the cost is **one record load per `Ready` effect per round**.
- **Nothing measures it.** The per-turn budget test
  (`per_turn_durable_write_count_and_latency_measurement`,
  `crates/rakka-agent/tests/effect_dispatch.rs:2115`) counts compare-and-set
  writes only. Also, one clean turn has a single `Ready` effect at a time,
  where the cut saves nothing.

## 2. The owner's decisions (2026-11-25)

- **"Done" is both:** add a load count to the budget first, so the cut is
  measured both ways; then cut; then record the PostgreSQL numbers on #25.
- **The cut is approach 1:** one record load per flush round, shared across
  the run's effects.

## 3. Approaches considered

| | What | Trade-off |
|---|---|---|
| **1** | **The sink takes all of a run's effects at once. The workflow sink loads the record once and schedules each effect against it.** | **Up to 8 loads per round become 1. Every `Ready` effect is still re-sent every round, so the run's re-flush still puts a lost row back (issues #153 and #154). Costs: a new sink method (with a default, so existing sinks keep compiling), and care to keep each effect's telemetry segment.** |
| 2 | Remember in memory which effects the outbox accepted since the entity started, and skip them | Zero loads in steady state. But it breaks the lost-row repair: a row lost after acceptance is not re-sent until a restart. And for a ticket no dispatcher registered yet, nothing else puts it back (the sweep's `(None, None)` arm relies on "the run's own flush lands, or is sent again"), so the run waits until a restart. |
| 3 | Record acceptance durably on the run record | Exact, but it needs a durable field, an extra write per effect, and a rebuilt lost-row repair. The largest change. |

## 4. Goals, non-goals, anti-goals

**Goals**
- **G1.** One workflow-record load per flush round per run, whatever the
  number of `Ready` effects. Today it is one per effect.
- **G2.** The load is counted.
  - The testkit store counts loads, as it counts writes.
  - The per-turn budget pins workflow-store loads.
  - A new in-memory test pins loads per settle for a run with several
    `Ready` effects.
  - Each count is measured at the base before the change, and after it.
- **G3.** A PostgreSQL measurement: loads and wall time per settle, for a run
  with several `Ready` effects, before and after. It is recorded on #25.

**Non-goals**
- Approaches 2 and 3.
- Spot 2, which is already gone. It is said so on #25.
- The agent dispatcher's own reads. Its per-pass registration reads the run's
  outbox once per pass, and that does not depend on how many effects there
  are.
- The rakka.host app's `RosterEffectSink`. The host forwards the new method
  in its own sessions (§7).

**Anti-goals**
- **Any change to what the flush does.**
  - `mark_ready` still commits before any sink write (spec 8.7's fence
    foundation).
  - Every `Ready`, unresolved effect is still handed to the sink every round.
  - A winding-down or ended run still hands over only the
    `exempt_from_wind_down_fence` kinds.
  - The first sink error still ends the flush with that error, and the
    effects after it are not handed over in that round (as `dispatched?`
    does today).
  - No sink error code changes. `outbox-schedule-failed` and the
    `sink_error` mapping stay.
- **Losing the lost-row repair.** A row the outbox lost is re-created on the
  next round, as today.
- **A sink trait change that breaks an implementer.**

## 5. The design

### 5.1 The sink method

`AgentRunEffectSink` (`crates/rakka-agent/src/effect.rs:3007`) gains a
**provided** method:

```rust
/// Hands several effects to the outbox, in order, each idempotently on its
/// effect id, calling `accepted(i)` after effect `i` is durably accepted.
/// Stops at the first error and returns it; effects after it are not
/// handed over.
fn dispatch_all<'a>(
    &'a self,
    scope: &'a AgentRunScope,
    effects: &'a [AgentEffect],
    accepted: &'a mut (dyn FnMut(usize) + Send),
) -> AgentEffectFuture<'a, ()>;
```

- **The default** calls `self.dispatch(scope, effect)` for each effect in
  order, and `accepted(i)` after each success. An implementer that does
  nothing gets today's behaviour exactly.
- **The name and signature are the builder's to refine** (§9). What is fixed:
  - the method is provided, so implementers do not break;
  - effects are handed over in order, stopping at the first error;
  - acceptance is reported per effect, so that each effect's segment ends at
    its own acceptance.

### 5.2 The workflow sink's override

`WorkflowAgentRunEffectSink::dispatch_all`:
1. builds one `AgentRunInbox` and `recover()`s it once;
2. then for each effect, builds `ticket_outbox_command` and calls
   `inbox.inner_mut().schedule_outbox(command)`, with the same error mapping
   as `dispatch`;
3. calls `accepted(i)` after each.

`dispatch` stays as it is, for a single effect.

**A lost compare-and-set** on a new row's schedule mid-batch fails as today:
the error is returned and the batch stops. The next round, or the next pass,
loads afresh. A later effect's `Duplicate` answer in the same batch comes
from the record as loaded, which is what today's per-effect load answers too.

### 5.3 The flush

In `dispatch_effects`, the re-drive loop over `ready` becomes one
`dispatch_all` call over the filtered effects, built with
`to_workflow_effect`.

**Telemetry:** each effect still gets one `EffectSchedule` segment that ends
after its durable acceptance (spec 17.6 and 17.9: "`PRODUCER`, after durable
acceptance").
- Effect `i`'s segment starts when the flush starts (for `i = 0`) or when
  effect `i - 1` was accepted. So the shared load is counted in the first
  effect's segment.
- On an error, the segment of the effect that failed closes as failed, with
  the error's code under `rakka.agent.outbox`, as today.
- The segments close in effect order, after `dispatch_all` returns. The
  closure collects them in a local list, since it cannot borrow the entity.
  Their timings are taken in the callback.
- Segment counts and outcomes are unchanged. Only the start point of effects
  after the first moves.

### 5.4 Counting loads (`rakka_agent::testkit`)

`CrashingStateStore` gains a load counter beside its write counter: `loads()`
and `reset_loads()`, both additive. Every `load` counts, whatever it answers.

## 6. Proof

### 6.1 Budget: measured both ways

- **Measured at the base first.** Add `loads()`, then pin
  `TURN_BUDGET_WORKFLOW_STORE_LOADS` at the value measured at the base,
  before the change, and record that number in the ledger. That commit's
  tests pass.
- **After the cut,** the constant moves to the value measured after it,
  together with `examples/durable-agent-acceptance/README.md`, which records
  the budget. A one-effect turn may not move at all: say so if it does not.

### 6.2 New in-memory tests (`crates/rakka-agent/tests/`)

- **Several effects:** a run with several `Ready` effects that are not yet
  resolved, for example a model turn that proposes three tool calls with
  nothing dispatched. Settle it repeatedly, with nothing else happening. The
  workflow store's loads per settle are **1**: 3 at the base, and the test
  fails there first.
- **One row lost, one put back:** the lost-row repair still holds with
  several effects. When one effect's row is lost (the workflow record is
  deleted, as in `dispatch_missing_row.rs`), the next settle puts every row
  back in one load.
- **One failure stops the batch:** a sink failure on effect `k` ends the
  flush with that error. Effects after `k` are not handed over, the run
  record is unchanged, and the next settle hands over all of them.
- **Segments:** with several effects, one `EffectSchedule` segment per
  effect, each `ok`, in effect order. With a failure, the failed effect's
  segment is `failed` under `rakka.agent.outbox`.
- **The default method:** a sink that implements only `dispatch` (a wrapper,
  as the host's is) still works through `dispatch_all`'s default, effect by
  effect.

### 6.3 PostgreSQL (gated)

- **The test:** a test gated on `RAKKA_POSTGRES_TEST_DSN` drives the
  several-effects run over a `PostgresDurableStateStore` for the workflow
  record, wrapped in a counter of loads.
  - It prints loads and wall time per settle, and asserts loads per settle
    is 1.
  - Wall time is printed, never asserted.
- **Before and after:** run it at the base, where the assertion fails and
  the numbers are printed, and after the change. Record both on #25 with
  the toolchain and the Postgres version.
- **Location:** the builder's choice (§9), with these constraints:
  - no non-dev dependency on a published crate is added;
  - it passes silently, saying it skipped, without the variable, as the other
    gated tests do.
- **Database:** the local Docker `postgres` on port 5432 serves. No
  `vector` extension is needed.

### 6.4 Breaks

Each is applied alone, and each must be caught:
- the workflow sink's `dispatch_all` recovers per effect, so the load count
  is back to one per effect;
- the flush skips the re-drive of effects already `Ready`, so the lost-row
  repair is gone;
- `accepted` is not called, so the segments are wrong;
- the batch goes on after an error.

### 6.5 Validation

`scripts/validate.sh` and `scripts/package-check.sh`, and the debug-stack
headroom of the flush. The new boxed future is small, but check with the
method #217's review used: the smallest `RUST_MIN_STACK` for `effect_dispatch`
before and after.

## 7. For the host (follow-up, not built here)

`RosterEffectSink` (in `paloul.rakka.host`, `rakka-host-runtime/src/roster.rs:890`)
implements only `dispatch`. With the provided default, its `dispatch_all`
loops its own `dispatch`: a roster touch, then the inner sink's per-effect
load, for each effect. So the host gets no saving until it forwards the new
method: one `touch`, then `self.inner.dispatch_all(..)`. That also cuts its
roster touches from one per effect to one per round. This goes on #25 with
the measurements.

## 8. Docs

- **`docs/rakka-agents.md`:** where the flush or the sink is described, say
  that a flush round loads the record once.
- **`docs/rakka-compatibility.md`:** an additive entry. It covers:
  - the provided `dispatch_all`;
  - a wrapping sink should forward it, or it keeps per-effect cost;
  - the `EffectSchedule` start-point note (§5.3);
  - the testkit's `loads()` and `reset_loads()`.
- **`CHANGELOG.md`:** Changed (one load per round) and Added (the method and
  the counter).
- **`examples/durable-agent-acceptance/README.md`:** the budget, if it
  moves.

## 9. Left to the builder

- The method's final name and exact signature, within §5.1.
- Where the PostgreSQL test lives, within §6.3.
- The test fixtures and file names.

## 10. Calls made in this design (yours to reverse)

1. **A provided method** rather than a required one, so the host and any
   other sink keep compiling. The cost is that a wrapper that does not
   forward it gets no saving.
2. **The shared load is counted in the first effect's segment.** Later
   segments start at the previous acceptance.
3. **The testkit's counter counts every load.**
4. **Spot 2 is closed by noting it on #25,** not by code.
