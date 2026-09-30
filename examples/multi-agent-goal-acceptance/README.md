# Multi-Agent Goal Acceptance (M4)

One durable collaborative goal, end to end: the multi-agent goal milestone of
`docs/plans/rakka-agent/spec.md` (section 22), demonstrated with deterministic
model adapters over real `ClusterSharding`, the in-process `rakka-a2a` service
core, the production effect dispatcher, the communal knowledge graph, and a
compiled workflow child's durable inbox — all in-memory, no external services.

The walk: a root goal with a configured evaluator delegates bounded work to
two specialist Rakka Agents through real A2A sends, invokes one compiled
workflow through its versioned tool descriptor, waits fully passivated,
survives root and child pod loss, records one attributable communal claim,
reaches `Satisfied` only through the evaluator, and reconstructs the whole
tree afterwards through the authorized goal view.

Cancellation propagation is deliberately not a walk beat: scenarios 29, 31,
and 33 prove it in `rakka-agent`'s own suite, and the goal view's
cancellation surface is pinned by `tests/goal_view.rs` there.

Run it:

```sh
cargo run -p rakka-example-multi-agent-goal-acceptance
```

`tests/acceptance.rs` runs the same flow and asserts this transcript
verbatim, plus the typed facts behind every line.

## Expected stdout

```text
ok  1/18 three agents instantiated; the goal-bearing root created: goal Active with an evaluator, two specialist skills, one versioned workflow tool, one shared knowledge space
ok  2/18 one turn committed the fan-out atomically: 2 delegation records, 1 workflow invocation, a closed 3-member fan-in group
ok  3/18 both sends created real children through rakka-a2a: two distinct child tasks, each with its own generation-1 run and verified provenance
ok  4/18 a replayed delegation send converged on the same child: the deduplication key answered, no second child
ok  5/18 the workflow start accepted the derived StartRun into the compiled refund v1 child's durable inbox: invocation id = child run id = deduplication key
ok  6/18 the fan-out persisted and the root waits with nothing resident: AwaitingChildren, 0 outstanding effects, 0 resident entities
ok  7/18 ROOT pod loss: the passivated root re-materialized on the next ask; a killed result write was redelivered by the child's re-driven settle and converged on one recorded result
ok  8/18 the translator's terminal task owed its delegation result; recorded on the root without resolving the 3-member group
ok  9/18 the translator's claim appended into the shared space with attributable provenance (goal, task, run, delegation stamped); its replay answered the original claim
ok 10/18 CHILD pod loss: the summarizer's worker died after invoking the non-idempotent tool; recovery parked one Indeterminate outcome — invoked exactly once, never re-invoked
ok 11/18 the deduplicated reconciliation decision resolved the ambiguity; the summarizer completed and its result recorded on the root
ok 12/18 a workflow start replayed after both losses adopted the SAME child run — one durable StartRun ever — and the compiled step executed exactly once
ok 13/18 a direct criteria decision was refused task-goal-decision-unattested: with an evaluator configured, Satisfied has one door
ok 14/18 the EvaluateGoal effect ran the configured evaluator over durable evidence; the goal-evaluation exchange recorded Satisfied fenced to the assigned root run
ok 15/18 the application relayed the deduplicated workflow result; the group resolved all-settled and fan-in resumed the model deterministically
ok 16/18 the root proposed its own validated result: run and task Completed, escrow returned across root and children
ok 17/18 the authorized goal view reconstructed the goal, 3 tasks, 3 runs, 2 delegations, 1 workflow link, 1 evaluation, and its evidence from durable state alone
ok 18/18 no private content leaked: planted sentinels appear in no claim, no goal view, no snapshot, no metric
```
