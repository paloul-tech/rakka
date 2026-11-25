# One writer for a host-driven run: `AgentRunCore` (issue #71, item 1)

Design record, 2026-11-24. Issue: #71 (item 1 of six). Branch: `agent-run-core`.

## 1. Intent

Issue #71's item 1 asks for three new `AgentRunActorCommand` arms: outbox
dispatch, branch complete and child settlement. **Their purpose is to retire
the rakka.host app's second writer.**
- The host hosts upstream's `AgentRunActor` as a child of its own sharded
  workflow entity.
- The actor has no command for most of what the host must write. So the host
  builds a **second `AgentRunInbox` and a second `AgentStepRunner` over the
  same two stores**, writes through them, and then sends the actor `Recover`
  so its cached copy re-reads the store.
- This is correct only because the entity's mailbox orders every write. The
  host calls it its largest standing risk.
- The host's seam is `RunSubstrate`, in
  `paloul.rakka.host/crates/rakka-host-workflow/src/driver/substrate.rs`, at
  `e1a1906`.

Three arms would not retire it. The second writer now makes about fifteen
kinds of write: dispatch the due outbox, complete a branch, fail a node, fail
a waiting node, open a human checkpoint, record a local output, cancel the
graph, apply a human decision, accept and apply an effect callback, persist a
recovered-effects transition, re-offer and exhaust outbox entries, and accept
and complete signals. Every one is a call into upstream's public building
blocks:
- `AgentRunInbox::accept_command`;
- `DurableInbox::{dispatch_due_outbox, record_outbox_failure, record_outbox_exhausted, transition_inbox}`;
- the effect bridge's `accept_and_apply_*` and `open_node_human_checkpoint`,
  which take `&mut AgentRunInbox<Store, Clock>`;
- the scheduler's pure transitions;
- `AgentStepRunner::update_graph_state`.

## 2. The owner's decisions (2026-11-24)

- **What "done" means.** Retire the second writer. Closing the item's text
  with three arms is not the goal.
- **How. Approach C:** no child actor for the host. Upstream moves the
  actor's internals into a public, owner-held `AgentRunCore`, and the actor
  becomes a thin wrapper over it. The host's entity holds one core and calls
  it directly. Its mailbox is already the single writer, so there is one
  writer by construction, not by care.

## 3. Approaches considered

| | What | Trade-off |
|---|---|---|
| A | One "work" command: the host sends a closure that the actor runs against its own runner and inbox (a typed helper downcasts) | Smallest upstream change. A store-type mismatch fails at run time. Closures must be `'static`, so the host's borrowing `NodeEffectDispatcher<'_>` must become owned. A general escape hatch on the actor |
| B | About seven typed commands (apply a graph transition, accept and apply a graph command, open a checkpoint, dispatch the due outbox, a guarded outbox write, complete an inbox command, read the workflow state) | Every write is named and typed. Each new host write needs a new command, which is how the list reached fifteen. The same `'static` dispatcher cost |
| **C** | **The host's entity owns an `AgentRunCore`; the actor wraps the same core** | **One writer by construction. Fully typed. The borrowing dispatcher works as it is. No per-pass `Recover` and no mailbox hop. Costs: a refactor of an actor every rakka user runs, and a larger host change (it drops the child actor)** |

`AgentRunActorCommand` is one non-generic enum while the actor is generic
over its stores, so no command can hand an owner the actor's typed inbox.
That constraint shaped A and B.

## 4. Goals, non-goals, anti-goals

**Goals**
- G1. A public `AgentRunCore<RunStore, WorkflowStore, Clock>` offering every
  operation the actor offers today. Each operation has the actor's recovery,
  its refresh of a stale component before the operation, its snapshot and
  error recording under the same phase names, and the same results and
  errors.
- G2. Mutable access to the core's runner and inbox, so an owner can compose
  upstream's transitions (§5.4) against the **same instances** the core's
  own operations use. No second instance and no re-read dance.
- G3. `AgentRunActor` becomes a wrapper over one `AgentRunCore`, with
  behaviour unchanged.
- G4. Proof that the host's pattern works: an owner-held core, with composed
  writes between core operations and no `Recover`, never loses a
  compare-and-set to itself.

**Non-goals**
- No new `AgentRunActorCommand` arms. The enum does not change, so nothing
  breaks for its users.
- **The host is not changed here.** Rakka delivers the core; the host
  migrates in its own sessions, as with every earlier host-facing change.
  Migration notes go on #71 (§8).
- **#71's item 6 (expected revision and provenance on `PublishDefinition`
  and `Admit`) is left out** (my call, reversible). The host's own
  recommendation says such a guard "would not remove the split but would
  make its `Bundle` arm rare", and the host already checks an expected
  revision on its catalog row before it publishes.
- Items 2, 4 and 5 stay open on #71.
- Timers: the host keeps its own per-run timer store, which is not one of
  the two run stores.

**Anti-goals**
- Any change to the actor's observable behaviour. These depend on it:
  upstream's sharded run entity (`crates/rakka-agent-workflow/src/sharding.rs`),
  `rakka-a2a`'s host and stores, and the `clustered-agent-workflow-http-grpc`
  example. "Observable" covers:
  - each reply's value and error;
  - each refresh-failure reply;
  - recovery on `started` and `restarted`;
  - the snapshot registry's records and phase names;
  - metrics.
- A core that is safe only if its owner remembers a ritual, such as
  recovering after its own writes. Its own writes go through the instances
  it holds, so there is nothing to remember.

## 5. The design

### 5.1 The type

```rust
pub struct AgentRunCore<RunStore, WorkflowStore, Clock = SystemWorkflowClock>
where
    RunStore: DurableStateStore<AgentRunState>,
    WorkflowStore: DurableStateStore<WorkflowState>,
    Clock: WorkflowClock,
{
    runner: AgentStepRunner<RunStore>,
    inbox: AgentRunInbox<WorkflowStore, Clock>,
    graph_runtime: AgentGraphRuntime,
    snapshots: Option<AgentWorkflowSnapshotRegistry>,
}
```

- It lives in a new module of `rakka-agent-workflow`, exported beside the
  actor in `lib.rs`: both the `runtime` re-export list (around `:283`) and
  the list around `:424`. The `rakka` facade's `agent_workflow` module
  re-exports by glob.
- Its constructors mirror the actor's six: `new`, `with_metrics`,
  `with_clock`, `with_clock_and_metrics`, `from_parts` and
  `with_snapshot_registry`, with the same parameters and defaults.

### 5.2 Operations: one method per actor arm

Each is `async fn …(&mut self, …) -> AgentRunRuntimeResult<T>`. `T` and
the phase name are the arm's.

| Arm | Core method | Phase recorded |
|---|---|---|
| `Recover` | `recover()` | `recover` |
| `Snapshot` | `snapshot()` | `snapshot` |
| `AcceptCommand` | `accept_command(command)` | `accept-command` |
| `Start` | `start(initial_state)` | `start` |
| `StartGraph` | `start_graph(initial_state, &plan, now)` | `start-graph` |
| `MarkGraphReady` | `mark_graph_ready(&plan, now)` | `mark-graph-ready` |
| `StartGraphNode` | `start_graph_node(&plan, node_id, now)` | `start-graph-node` |
| `CompleteGraphNode` | `complete_graph_node(&plan, node_id, now)` | `complete-graph-node` |
| `ScheduleGraphNodeEffect` | `schedule_graph_node_effect(&plan, request)` | `schedule-graph-node-effect` |
| `BeginStep` | `begin_step(now)` | `begin-step` |
| `SucceedStep` | `succeed_step(success, now)` | `succeed-step` |
| `FailStep` | `fail_step(error_code, now)` | `fail-step` |
| `Wait` | `wait(reason, now)` | `wait` |
| `Resume` | `resume(now)` | `resume` |
| `Complete` | `complete(now)` | `complete` |
| `FailRun` | `fail_run(error_code, now)` | `fail-run` |
| `RequestCancellation` | `request_cancellation(reason_code, reason_summary, now)` | `request-cancellation` |
| `Cancel` | `cancel(now)` | `cancel` |
| `BeginCompensation` | `begin_compensation(now)` | `begin-compensation` |
| `ScheduleEffect` | `schedule_effect(effect)` | `schedule-effect` |
| `DueEffects` | `due_effects()` | `due-effects` |

Every method except `recover` first refreshes a stale component, exactly
as the actor's handler does before every non-`Recover` command:
- **If the refresh fails**, the method returns that error, records nothing
  under its own phase, and does not run. This is what `reply_refresh_failure`
  answers today. The refresh's own recovery records under `recover`, as it
  does now.
- **`recover`** is the actor's `recover_components`, unchanged. It recovers
  the runner, then the inbox, then re-links outbox effects that no graph node
  lists (`reconcile_recovered_graph_effects`, crash-window repair).

The actor passes `&plan` where it holds an `Arc<AgentCompiledExecutionPlan>`;
the core takes a borrow.

### 5.3 The actor over the core

- `AgentRunActor` holds one `AgentRunCore`. Its constructors build the core,
  and `runner()`, `inbox()` and `graph_runtime()` delegate to it.
- **New on the actor:**
  - `from_core(core)`, so an owner that built a core can still host it as an
    actor;
  - `core()`, read-only.
- **The handler:** each arm calls its core method and replies with the
  result. `started` and `restarted` call `core.recover()`.
- **The refresh.** Since the core's methods refresh first themselves, the
  handler's separate refresh and `reply_refresh_failure` go: the method's
  `Err` is the same reply. `Recover` still recovers without refreshing
  first.

### 5.4 Composition access (what retires the second writer)

- **`runner()`, `inbox()` and `graph_runtime()`:** read access, as on the
  actor. The host's reads (the due instant, the recovered effects, the
  parent scope) read `inbox().inner().state()` and
  `runner().state()`.
- **`parts_mut(&mut self) -> AgentRunParts<'_, RunStore, WorkflowStore, Clock>`:**
  a split borrow with public fields
  `runner: &mut AgentStepRunner<RunStore>`,
  `inbox: &mut AgentRunInbox<WorkflowStore, Clock>` and
  `graph_runtime: AgentGraphRuntime`. An owner can then call the bridge with
  `parts.inbox` and persist with `parts.runner`, which is what every
  `RunSubstrate` write does today:
  - `parts.inbox.inner_mut().dispatch_due_outbox(&mut dispatcher)`, with the
    host's borrowing dispatcher as it is;
  - `bridge.accept_and_apply_effect_completed(&plan, state, command, parts.inbox)`
    followed by `parts.runner.update_graph_state(next, now)`;
  - `AgentGraphScheduler` transitions persisted through
    `parts.runner.update_graph_state`;
  - the guarded `record_outbox_failure` and `record_outbox_exhausted`;
  - `transition_inbox` and `accept_command`.
- **`refresh_if_stale(&mut self) -> AgentRunRuntimeResult<()>`**, now
  public. Every composed write starts with it.
  - A composed write that loses a compare-and-set to a genuine second writer
    (for an agent run, the agent dispatcher writes the run's inbox store,
    issue #81) leaves that component stale. The next core operation, or the
    owner's next `refresh_if_stale`, recovers it.
  - Self-inflicted losses cannot happen. There is one instance of each
    component, and each write moves the revision it holds.
  - **A composed write is computed from state read after
    `refresh_if_stale()`** (my call, from the builder check). The refresh
    discards the stale copy and re-reads, so a write computed from a read
    taken before it would succeed over the other writer's change that the
    compare-and-set would otherwise have refused. The host's substrate
    methods take a graph state the caller read earlier. That flaw exists on
    today's route too; the core's contract states the stricter rule.
- **Composed writes are not recorded** in the snapshot registry: they are
  the owner's operations, not the core's. The owner's next core operation or
  `snapshot()` records the state as it then stands.

### 5.5 Lifecycle

- An owner calls `recover()` before the first operation, and again whenever
  it is itself re-created. The actor does this in `started` and `restarted`.
- An operation on a core never recovered fails as its component fails today
  (for example, the runner's not-recovered error). The core adds no implicit
  recovery.

## 6. Proof

### 6.1 Parity: the actor behaves as before

These pass unchanged:
- `crates/rakka-agent-workflow/tests/runtime_actor.rs`;
- `runtime_snapshots.rs`, which pins the phase records;
- the sharding tests;
- `rakka-a2a`'s tests;
- the example's end-to-end check.

### 6.2 The core, new tests (`crates/rakka-agent-workflow/tests/`)

- **The host's pattern, end to end.** An owner holds one core over the
  testkit stores and drives a compiled graph run:
  1. start, mark ready, start a node, and schedule its effect, all through
     the core;
  2. then, composed through `parts_mut()` with **no `recover` in between**:
     dispatch the due outbox with a recording dispatcher, accept and apply
     `EffectCompleted` through the bridge, and persist with
     `update_graph_state`;
  3. then complete the next node through the core.

  Every write succeeds, and the durable records end where the actor-plus-
  second-writer route ends. The same sequence through a second inbox and
  runner without a `Recover` loses a compare-and-set, which shows the test
  reaches the hazard.
- **A genuine second writer.** A composed write that loses its
  compare-and-set to another writer of the workflow store leaves the inbox
  stale. The next core operation refreshes it and succeeds against durable
  state.
- **Recovery on the core** re-links an orphaned effect, as the actor's
  recovery does. Its test moves over or is mirrored.
- **Phase records:** the same scripted sequence records the same phases
  through the core as through the actor.
- **A refresh failure** answers the error, records nothing under the
  operation's phase, and runs nothing.

### 6.3 Breaks

Each is applied alone, and each must be caught:
- a core method that skips its refresh;
- a changed phase name;
- an actor arm wired to the wrong core method;
- `recover` that skips the re-link;
- `parts_mut` returning a fresh instance instead of the core's own.

### 6.4 Validation

`scripts/validate.sh` and `scripts/package-check.sh` must pass, including the
debug-build stack-margin guard. If delegation grows the handler's future, box
the core calls (the #162 rule).

## 7. Docs

- **Rustdoc on the new module:** what the core is for (an owner that is
  already a single writer, such as a sharded entity), the contract of §5.4
  and §5.5, and that the actor is one such owner.
- **`AgentRunActorCommand`'s doc** names the core as the route for writes
  the enum has no arm for.
- **`docs/rakka-api-boundary-inventory.md`:** the `rakka-agent-workflow`
  row gains the core.
- **`docs/rakka-agents.md`:** one sentence in the "What ships" table's
  `rakka-agent-workflow` row, or nearby.
- **`docs/rakka-compatibility.md`:** an additive entry. The core and the
  actor's `from_core` and `core` are new; no type, arm, error code, phase or
  durable record changes.
- **`CHANGELOG.md`:** under Added.

## 8. For the host (follow-up, not built here)

To be posted on #71 when this lands, together with a correction to the
2026-11-20 status comment. `AgentGraphScheduler::cancel_graph_run` exists;
what item 4 lacks is the outbox clearing and the actor's per-node walk.

The host's migration:
- The entity holds an `AgentRunCore` instead of a child actor.
- Its eleven command sends become method calls.
- `DurableRunSubstrate`'s methods compose through `parts_mut()` after
  `refresh_if_stale()`.
- Every `Recover`-after-write goes.
- **State is read after the refresh** (§5.4). Each substrate method reads the
  graph state it transforms after `refresh_if_stale()`, not from the caller.
- **The guarded outbox writes** (the host's "R6" guard). Today they read an
  entry's status on a freshly loaded inbox and skip an entry another writer
  moved. On the core's cached inbox, a moved entry instead fails the
  compare-and-set, and the next pass refreshes and reads it right. Where the
  host wants the old skip in the same pass, `parts_mut().inbox.recover()`
  re-reads first.
- **No type-erased handle upstream.** The host's driver is not generic over
  its stores, and a `Clone` store bound cannot be a trait object. The host
  wraps the generic core in its own object-safe trait, as `RunSubstrate`
  does today. Rakka adds no erased handle.

Item 1 is then done another way, as item 3 was.

## 9. Left to the builder

- The module's file name, the name of the parts struct, and test file
  names.
- How the actor's handler and the core share the arm-to-phase mapping.
- Whether the actor's recovery test moves to the core or is mirrored.
- Wording of the docs in §7.

## 10. Calls made in this design (yours to reverse)

1. **Rakka only.** The host's migration is not done here.
2. **Item 6 left out** of this round.
3. **The core's method names** are the arms' names in snake_case.
4. **`parts_mut()`** as a split borrow, and a public `refresh_if_stale()`.
5. **Composed writes are not recorded** in the snapshot registry.
6. **`from_core` and `core`** are added to the actor.
7. **The snapshot type keeps its name** (`AgentRunActorSnapshot`), since
   renaming it would break its users for nothing.
8. **A composed write is computed from state read after the refresh.** This
   is the core's stated contract (§5.4), from the builder check.
9. **No type-erased core handle in rakka.** The host wraps the core in its
   own trait (§8), from the builder check.

The builder check found nothing for the owner to decide. It confirmed the
following against the code:
- the six constructors;
- the 21 arms and their phases;
- the refresh rules;
- that a composed write's lost compare-and-set marks its component stale (one
  place in each store wrapper);
- that `dispatch_due_outbox` marks an entry `Dispatching` before it calls the
  dispatcher, so a stale inbox cannot start an outside call;
- that every host write, the child settlement included, fits `parts_mut()`.
