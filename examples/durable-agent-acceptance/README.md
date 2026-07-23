# Durable Agent Acceptance

The M1 acceptance walk: one sharded Rakka Agent demonstrating every bullet of
the initial acceptance statement
([spec 22](../../docs/plans/rakka-agent/spec.md#22-initial-acceptance-statement))
with the deterministic model adapter. Everything is in-process and
deterministic: real `ClusterSharding` over all three entity types (agent,
task, run), the in-process A2A service core (no HTTP), the production effect
dispatcher fleet, and in-memory durable stores.

## Run

```sh
cargo run -p rakka-example-durable-agent-acceptance
cargo test -p rakka-example-durable-agent-acceptance
```

The integration test runs the same walk the binary prints and asserts the
transcript below verbatim against the `const` the binary prints from — and a
second test extracts this README's fenced block and compares it to the same
`const` — plus the typed facts behind every line, so this documented stdout
cannot rot.

## Expected stdout

```text
ok  1/18 instantiated with versioned settings: revision 1
ok  2/18 duplicate A2A sends mapped to one task task-be0e74d4fdbd9e6f and its initial run
ok  3/18 the typed result passed rule answer-present before the task completed
ok  4/18 admission fails closed: not-admitted before the decision, not-admitted again after a widening definition
ok  5/18 budgets settled durably: 2 loop iterations, 2 model calls, 3 effect attempts, escrow returned
ok  6/18 fully passivated (0 resident entities) and still addressable: the describe ask re-materialized the owner
ok  7/18 both model turns executed through dispatcher worker-1
ok  8/18 the session view assembled 1 correlated trace segments by run id
ok  9/18 bounded metrics observed: rakka.agent.recovery.events, rakka.agent.run.transitions, rakka.agent.telemetry.flush.failures
ok 10/18 short-term session context persisted: 3 entries, 2 immutable snapshots
ok 11/18 each effectful call is its own durable effect: model and tool ticketed separately
ok 12/18 the checkpoint-required tool parked the run WaitingForApproval, passivated
ok 13/18 recovered after dispatcher loss (worker died mid-attempt) and owner loss (decision write killed, redelivered)
ok 14/18 the ambiguous non-idempotent tool parked one Indeterminate outcome; invoked exactly once, never re-invoked
ok 15/18 resumed only after the deduplicated reconciliation decision; its replay answered Duplicate
ok 16/18 the authoritative snapshot answered from durable state with no telemetry wired into the query
ok 17/18 default telemetry carries no prompt, tool payload, memory content, or credential material
ok 18/18 the unavailable decision sink blocked nothing: the run completed, flush failures are a bounded metric, owed events are visible
```

## What each line demonstrates

Each line is one bullet of the spec 22 statement, in order. Where the
statement's proof is deeper than one example can honestly carry, the owning
test in `crates/rakka-agent/tests/` (or `crates/rakka-a2a/tests/`) is listed
— the example demonstrates the composed behavior; the suite proves it
exhaustively under fault injection.

| Line | Statement bullet | Owning proof beyond the example |
| --- | --- | --- |
| 1 | instantiated with versioned settings | `agent_entity.rs` |
| 2 | one A2A task, one durable `AgentTaskId`, one initial run | `agents_surface.rs` (scenario 1, swept) |
| 3 | one versioned typed result validated before completion | `task_results.rs` (scenario 40, swept) |
| 4 | fail-closed admission; widening update rejected | `autonomy_admission.rs` (scenario 53, swept) |
| 5 | bounded budgets reserved and settled durably | `escrow_ledger.rs` (scenarios 52/61, swept) |
| 6 | addressable while fully passivated, no resident resources | `goal_passivation.rs` (scenario 35) |
| 7 | a bounded Rig-shaped model turn through a dispatcher | `effect_dispatch.rs` |
| 8 | correlated trace segments assemble into one session view | `trace_scenarios.rs` (scenarios 23-25, swept); scenario 21 in `operational_query.rs`/`decision_events.rs` |
| 9 | bounded metrics without high-cardinality IDs | `agent_metrics.rs` (scenario 25) |
| 10 | short-term session context persisted | `session_memory.rs` (scenarios 14/16/17, swept) |
| 11 | each effectful tool call a separate durable effect | `run_entity.rs` |
| 12 | pauses and passivates at an approval gate | `checkpoint_run.rs` (scenario 3, swept) |
| 13 | recovers after owner and dispatcher pod loss | the owner-kill sweeps across the whole suite; scenarios 5-9 |
| 14 | ambiguous non-idempotent marked indeterminate, no auto re-invoke | `effect_dispatch.rs` (scenario 9) |
| 15 | resumes only after an authenticated, deduplicated reconciliation | `checkpoint_reconciliation.rs` (scenarios 3/11, swept) |
| 16 | authoritative snapshot correct without telemetry | `operational_query.rs` (scenario 56, swept) |
| 17 | no content or credentials in default telemetry | `trace_scenarios.rs` (scenario 25; sentinels asserted here too) |
| 18 | correct when export unavailable, loss bounded and visible | `trace_scenarios.rs` (scenario 26) |

## Durable-write budget

The M1 per-turn durable-write budget is pinned as exact assertions in
`crates/rakka-agent/tests/effect_dispatch.rs`
(`per_turn_durable_write_count_and_latency_measurement`): one clean model
turn — creation through accepted result and settled escrow — costs

| Store | Compare-and-set writes |
| --- | --- |
| run store | 10 |
| task store | 8 |
| workflow outbox | 3 |

The fleet store is deliberately unbudgeted: its lease bookkeeping scales
with worker churn, not turns. Wall-clock latency is informational and never
asserted; measured on a release build over in-memory stores:

```text
one full turn (accept -> model effect -> ticket -> claim -> invoke -> deliver
-> propose -> accept): run-store writes = 10, task-store writes = 8,
workflow-store writes = 3, wall time = ~2.0ms
```

Reproduce with:

```sh
cargo test -p rakka-agent --release --test effect_dispatch \
  per_turn_durable_write_count_and_latency_measurement -- --nocapture
```

A deliberate pipeline change that moves a count must re-derive the budget —
update the test's constants and this table together.
