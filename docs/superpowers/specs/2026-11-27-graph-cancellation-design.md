# Issue #71 item 4: a graph run's cancellation, done upstream

## 1. Why

Cancelling a compiled graph run upstream moves the run's status and nothing else:

- **The core's `cancel` (and the actor's `Cancel`)**
  (`crates/rakka-agent-workflow/src/run_core.rs:737`,
  `crates/rakka-agent-workflow/src/runner.rs:836`) moves a run from
  `Cancelling` to `Cancelled`. It touches no graph node.
- **`AgentGraphScheduler::cancel_graph_run`** (`graph_scheduler.rs:593`) marks
  the graph's unresolved nodes and loop instances `Cancelled`. It is a pure
  calculation that the caller has to persist, and it leaves the outbox alone.
- **The outbox sends what is due, whatever the run's state.** The run's
  durable outbox is a separate document from its run state.
  `DurableInbox::dispatch_due_outbox` (`crates/rakka-workflow/src/inbox.rs:749`)
  sends every due entry and checks neither the run nor the graph. The graph
  bridge refuses new work on a terminal graph, but it never sees a dispatch.

So upstream, a cancelled run's queued effects still reach the dispatcher.

rakka.host guards against this itself, at `732d382`:

- **Its driver** cancels in five steps (`rakka-host-workflow/src/driver/mod.rs:493-551`):
  1. ask child workflows to cancel;
  2. apply and persist `cancel_graph_run`;
  3. narrate one `node_cancelled` per node the transition cancelled, worked
     out by reading the snapshot afterwards;
  4. record the cancelled checkpoints;
  5. send the actor's `Cancel` only if the run is not yet terminal.
- **Its dispatcher** (`rakka-host-workflow/src/dispatch.rs:251-271`) refuses
  an entry of an ended run, or of a cancelled node, with the transient code
  `node_run_terminal`. Its spike recorded the gap as "check 7": such an entry
  "stays due forever because nothing clears it". Each refusal spends a retry
  attempt until the entry exhausts.

Issue #71 item 4 asks for cancellation to happen upstream: clear the cancelled
nodes' outbox entries, and have the actor's `Cancel` mark the remaining nodes
and report them per node.

## 2. The owner's decisions (2026-11-27)

- **A: `Cancel` does the whole job.** From `Cancelling`, it cancels the
  graph's unresolved nodes, settles their outbox entries, ends the run, and
  returns which nodes and entries it cancelled, so the owner can narrate from
  the result. The runtime event vocabulary gains a node-cancelled kind.
  Rejected:
  - B, an outbox-clearing operation alone, which leaves the host's walk and
    narration in place;
  - C, upstream also emitting the events. Upstream emits no runtime events
    anywhere today.
- **Rule 1 for a node's entries, the fleet's rule.** It is the one
  `AgentDispatcherFleet`'s `settle_cancelled_effects` applies
  (`crates/rakka-agent-workflow/src/dispatcher.rs:3270-3318`, issue #209):
  - an entry mid-dispatch is cancelled with its attempt counted
    (`record_outbox_cancelled_in_flight`, under
    `DISPATCH_CANCELLED_IN_FLIGHT_LINE`), so it still says the attempt
    started;
  - a pending or retry-scheduled entry is cancelled
    (`record_outbox_cancelled`);
  - an entry that already finished, or was compacted away, is left alone.

  Rejected:
  - 2, also changing `record_outbox_failure` so that a failure reported on a
    cancelled entry keeps it cancelled. That is a `rakka-workflow` behaviour
    change;
  - 3, leaving entries that are mid-dispatch untouched.

- **Every path that cancels leftover nodes, not only `Cancel`.** The builder
  check found that the scheduler also cancels a graph's unresolved nodes when
  the graph ends another way, and their entries stay due too:
  - `complete_node`, reaching a Terminal node, ends the graph `Completed`
    (`graph_scheduler.rs:360`);
  - `fail_waiting_node` and `fail_node` end it `Failed` (`:782`, `:819`).

  The owner chose to settle the cancelled nodes' entries on all of these
  paths. The alternative, `Cancel` only with an issue draft for the rest, was
  rejected.

**What rule 1 means in the core (builder check).** The fleet settles only a
`Dispatching` entry that no worker holds a lease on. The core sees no leases,
so it cancels any `Dispatching` entry of a changed node. In the core's
topology, a single owner both dispatches and cancels in sequence, so such an
entry is one a crashed drive left behind. A second writer dispatching outside
that topology is outside the contract, and rule 2 (declined) was the
protection for it.

## 3. Goals, non-goals, anti-goals

- **Goals:**
  - Every transition that cancels a graph's leftover nodes settles those
    nodes' outbox entries before it is persisted. That covers the run's
    cancel, the graph completing, and the graph failing, on every upstream
    path. An owner that composes its own write can do the same through one
    public operation (5.2).
  - After `Cancel` on a run with a graph, none of the graph's unresolved
    nodes is left unresolved, and no outbox entry of a node the cancel
    resolved is due.
  - The run ends `Cancelled`.
  - The caller learns, from the result alone, which nodes and which entries
    the cancel changed.
  - A repeated `Cancel`, or one interrupted by a crash, converges on the same
    end.
- **Non-goals:**
  - emitting runtime events upstream;
  - cancelling child workflows, which is the host's step 1;
  - durable timers, which live on the owner's store;
  - hardening `record_outbox_failure` (rule 2, declined);
  - changing `request_cancellation`;
  - the outbox entries of a run with no graph (step runs' `schedule_effect`
    entries). Their gap is the same, and it is recorded in section 7;
  - an iterator that exceeds its bound (`start_iterator_iteration`). It fails
    the graph without cancelling its siblings, which is a different gap; it
    is recorded in section 7. The effect bridge refuses iterator nodes today
    (#71 item 2);
  - making owner-composed writes settle on their own. The host calls the
    scheduler's `fail_node` and `fail_waiting_node` directly and persists
    through its own substrate. Upstream cannot step into that write, so it
    offers the operation and documents the contract (5.6);
  - cleaning up the entries of runs an earlier build already cancelled.
- **Anti-goals:**
  - **An entry whose dispatch finished, rewritten as unsent.** Rule 1 leaves
    terminal entries alone.
  - **A run that ends before its entries are settled.** Once a crash leaves
    the run `Cancelled` with entries due, a repeated `Cancel` is refused and
    nothing settles them. So the entries settle first (5.3).
  - **The host's existing sequence breaking silently.** Every change it meets
    is either a compile error or behaviour its sequence already tolerates
    (5.6).
  - **Settling an entry the cancel did not resolve.** Only entries of nodes
    this cancel moves are touched. A node that completed, failed or was
    skipped earlier keeps whatever its entries say.

## 4. Approaches considered

The approach is the owner's A (section 2). Within it, one choice was left: how
`Cancel` reaches the graph, since `cancel_graph_run` takes the compiled plan
and `Cancel` carries only `now`.

- **Keep `Cancel { now, reply_to }` and cancel from the state (chosen).**
  `cancel_graph_run` uses the plan only to validate that the plan and the
  state match (`validate_plan_state`). Cancelling unresolved nodes and loop
  instances reads the graph state alone (`cancel_unresolved_nodes`,
  `cancel_unresolved_loop_instances`). A scheduler operation that cancels
  from the state lets `Cancel` keep its fields. It keeps the state's
  schema-version check, which is the part of the validation that guards a
  write.
- **Give `Cancel` a plan.** This breaks every sender, including `rakka-a2a`'s
  workflow host (`crates/rakka-a2a/src/host.rs:797`), which starts no graph
  runs and has no plan to hand. Rejected.
- **Add a separate `CancelGraph` arm and leave `Cancel` as it is.** A caller
  that keeps sending `Cancel` to a graph run keeps today's gap, silently.
  Rejected.

## 5. The design

### 5.1 The scheduler

`AgentGraphScheduler` gains an operation that cancels a graph run from its
state alone:

- It rejects a state of a newer schema version, as `validate_plan_state` does.
- It answers a graph that is already `Cancelled` with no change, and refuses
  one that reached another terminal status (`TerminalGraph`), as
  `cancel_graph_run` does.
- It cancels the unresolved nodes and loop instances, and sets
  `terminal_status = Cancelled`.

`cancel_graph_run` keeps its signature and its plan validation, and calls the
new operation for the rest, so the two cannot drift.

The transition reports the nodes it changed, as `cancel_graph_run`'s does
today (`AgentGraphSchedulerTransition`'s changed node ids).

### 5.2 What a node's entries are, and the one operation that settles them

A node's outbox entries are those named by its `scheduled_effect_ids`, under
`OutboxMessageId::new(effect_id)`:
- the effects it scheduled;
- the approval request its human checkpoint opened. `record_open_checkpoint`
  pushes that effect id onto the same list (`effect_bridge.rs:2578-2606`).

**One public operation settles them.** It takes the run's inbox and a
scheduler transition, which has not yet been persisted. It finds the nodes
the transition changed that the transition's state shows `Cancelled`, and
settles their entries under rule 1. It returns what it settled, each entry
with what was done to it. It is idempotent, because a terminal entry is
skipped.

Every upstream path below uses it. An owner that persists a transition it
computed itself calls it before that write. Rule 1 lives in this one place,
shared with the fleet's `settle_cancelled_effects`.

### 5.3 `AgentRunCore::cancel`, in order

From `Cancelling`, on a run whose graph state is present and not terminal:

1. **Refresh** (`refresh_if_stale`).
2. **Work out the graph transition** from the refreshed state (5.1). This is
   pure; nothing is written yet.
3. **Settle the outbox entries** of the nodes the transition cancelled,
   through 5.2's operation, reading the inbox's current state.
   - **Why first.** Settling is idempotent: an entry that is already terminal
     is skipped. A crash after this step leaves the run `Cancelling` with its
     entries settled, and a repeated `Cancel` picks up from there. In the
     other order, a crash leaves the run `Cancelled` with entries due, and
     nothing settles them.
   - **What a settled node sees.** Between steps 3 and 4, a node can be
     `Waiting` on an entry that will now never be sent. That is harmless: the
     run is `Cancelling`, and step 4 resolves the node.
4. **Persist the graph state and the `Cancelled` status in one runner write**,
   recorded as an `AgentRunTransitionKind::Cancel` transition. The run gets
   what `update_graph_state` gives a terminal graph: `current_step_id`
   cleared and `completed_at` set.
   - Today, `update_graph_state` turns a cancelled graph into a `Cancelled`
     run but records it as `GraphUpdated` (`runner.rs` around `:930`).
     `Cancel` records the cancel as a cancel.
   - A lost compare-and-set leaves the core stale, as every core write does.
     The call errs, and a repeated `Cancel` re-settles (a no-op) and writes
     again.
5. **Return** the transition, the nodes changed (in the scheduler's order),
   and the entries settled, each with what was done to it.

**Every other run takes today's path:**
- a run with no graph state;
- a run whose graph is already terminal;
- a run that is not `Cancelling`.

That path is the runner's `Cancelling → Cancelled` transition, with the same
refusals, and it returns no nodes and no entries.

The actor's `Cancel` arm delegates to the core, as every arm does, so it
inherits all of this.

### 5.3a The other paths that cancel leftover nodes

The same order holds on each: compute the transition, settle with 5.2's
operation, then persist.

- **The core's `complete_graph_node`** (`run_core.rs:516`, today through
  `AgentGraphRuntime::complete_node`, which computes and persists in one
  call). When the completed node is Terminal, the graph ends `Completed` and
  its siblings are cancelled. The core holds the inbox, so it computes the
  transition, settles, and then persists.
  - `AgentGraphRuntime::complete_node`, a public helper that has only the
    runner, keeps its behaviour, and its documentation says it does not
    settle.
  - The core's return type is unchanged. The cancelled siblings are in the
    transition's graph state, as today.
- **The bridge's failure paths.** `accept_and_apply_effect_failed`, and
  `accept_and_apply_reconciliation_decision` on `Abandon`, can fail a waiting
  node and with it the graph, through `fail_waiting_node`
  (`effect_bridge.rs:1618`, `:1772`).
  - Each already accepts its command into the inbox it is handed, and
    returns a transition for the owner to persist.
  - Each now settles the cancelled nodes' entries after computing the
    transition and before returning, so the owner's persist comes after.
  - The outcome reports what was settled.
  - A crash between the settle and the owner's persist leaves the command
    accepted in the inbox. Its re-application recomputes the transition,
    settles nothing new, and persists.
- **Owner-composed writes.** A transition from `fail_node`, or from any
  scheduler call the owner makes itself, is the owner's to settle with 5.2's
  operation before persisting. The product doc and the #71 migration notes
  state this contract.

### 5.4 The result

`cancel` and the actor's `Cancel` reply return a new public type in place of
the bare `AgentRunTransition`:
- the transition;
- the cancelled node ids;
- the settled entries, each as its effect id and whether it was cancelled
  pending or cancelled in flight.

The type is `#[non_exhaustive]`, so it can grow. Changing the reply type is a
compile error for every `Cancel` sender. That is intended, and the actor
command enum is exhaustive for the same reason.

### 5.5 The runtime event vocabulary

`AgentRuntimeEventKind` gains `NodeCancelled` (`node-cancelled`), placed
beside `NodeSkipped` and `NodeFailed`. It joins `all()`, the label table, and
whatever the existing mapping tests and the observability docs enumerate.

The enum is exhaustive, so a consumer's exhaustive `match` on it stops
compiling. That is stated in the compatibility note.

Upstream still emits nothing. The kind is there so the owner's narration of a
cancelled node has an upstream name.

### 5.6 What rakka.host meets

- **It stops compiling** where it matches on the `Cancel` reply or on
  `AgentRuntimeEventKind`, and it adapts there.
- **Its sequence works unchanged until it adapts.** Its step 2 still persists
  `cancel_graph_run` through its own substrate, and that ends the run
  `Cancelled` through `update_graph_state`. Its step 5 is skipped for a
  terminal run, as now. So nothing changes for it until it replaces steps 2
  and 3 with `Cancel` and narrates from the returned nodes.
- **Its two dispatcher guards** become a second line it can keep, on every
  path upstream settles.
- **Its own scheduler calls** (`fail_node` and `fail_waiting_node` in its
  substrate, nodes and checkpoint drivers) settle only when it adds 5.2's
  operation before each write. Until then, those runs keep the gap, and its
  guards still cover them.

The migration notes go on #71 when this lands, as item 1's did.

## 6. Proof

Integration tests go in `crates/rakka-agent-workflow/tests/`, one concern per
file, using the existing graph and outbox fixtures.

| Case | Asserted |
| --- | --- |
| **The gap, first (red before the change).** A graph run with a scheduled effect is cancelled, then `dispatch_due_outbox` runs with a recording dispatcher | before: the dispatcher receives the entry. After: it receives nothing, and the entry is `Cancelled` |
| A graph run with nodes waiting on effects, a human checkpoint, and a loop instance is cancelled | every unresolved node and loop instance is `Cancelled`; their entries are `Cancelled`; the run is `Cancelled` with a `Cancel` transition; the returned lists match exactly |
| An entry mid-dispatch when the cancel runs | cancelled in flight, its attempt counted under the fleet's line, and reported as such |
| A node resolved earlier whose entry could still be cancelled: a checkpoint decided while its approval request was still pending | untouched, and not in the returned lists (a `Dispatched` entry would prove nothing here, since rule 1 never touches one) |
| A crash after the settle and before the run write (a store write fault) | the run is still `Cancelling` with its entries settled; a second `Cancel` completes it and returns the nodes again, with no entry settled twice |
| A run with no graph state; a run whose graph already completed | today's behaviour and today's refusals, with empty lists |
| `Cancel` on a `Cancelled` run | refused as today |
| A graph that completes through the core's `complete_graph_node` while a sibling has an effect due | the sibling is `Cancelled` and its entry `Cancelled`; a dispatch pass afterwards sends nothing |
| A graph that fails through `accept_and_apply_effect_failed`, and through an `Abandon` reconciliation decision, while a sibling has an effect due | the same; the outcome reports the settled entry, and the owner's persist comes after the settle |
| 5.2's operation on a transition that cancelled nothing | settles nothing, and returns an empty list |
| The actor's `Cancel` | the same result as the core's |
| `cancel_graph_run` and the new scheduler operation | the same state for the same input |
| `NodeCancelled` | in `all()`, labelled `node-cancelled`, and in whatever the vocabulary's own tests enumerate |

**Falsification.** Each guard is broken, then restored, and its test must
fail:
- settle nothing;
- settle after the run write (the crash test must fail);
- cancel entries mid-dispatch without counting the attempt;
- settle the entries of every node, not only the changed ones;
- skip the settle in `complete_graph_node`, then in each bridge failure path
  (each path's test must fail);
- record the transition as `GraphUpdated`.

## 7. Docs

- **`CHANGELOG.md` (Unreleased).** One entry, marked behaviour-breaking for
  `Cancel`'s reply type and the event enum.
- **`docs/rakka-compatibility.md`.** An entry in the issue list: what
  changes, what a host meets (5.6), and mixed builds. Nothing durable
  changes shape: the graph state, the run state and the outbox entries all
  keep their schema versions, so builds before and after read the same rows.
- **The product doc for the agent-workflow runtime.** Wherever it describes
  cancellation, it now says what `Cancel` settles. The builder finds the doc
  that does.
- **Two issue drafts in `tmp/`, not filed,** for the owner to file or drop:
  - **The step-run gap.** A cancelled run with no graph keeps its
    `schedule_effect` entries due.
  - **The iterator bound.** `start_iterator_iteration` fails the graph and
    leaves its siblings unresolved, with their entries due.

## 8. Left to the builder

- The scheduler operation's name, and the result type's name and fields,
  within 5.1 and 5.4.
- 5.2's operation: its name, and where it lives, on the effect bridge or as
  a free function beside it. Also how the bridge's outcome reports what it
  settled, given whether that outcome struct can take a new field without
  breaking a consumer's destructuring.
- Whether the settle reuses `settle_cancelled_effects`'s body through a
  shared helper or mirrors it. One rule must not become two.
- How the crash test injects the fault, using the store fault kit the
  workflow tests already use.
- Where `NodeCancelled` sits in the label table and in the docs that list
  the kinds.

## 9. As built

Commits `e5ac1389` (the settle), `0bdff8c3` (`Cancel`), `efd62ea4` (graphs
that end), and the docs commit on top.

**Section 8's choices.**
- **Names.** The scheduler operation is `AgentGraphScheduler::cancel_graph_state`,
  and `cancel_graph_run` checks the plan, then calls it. The runner's write is
  `AgentStepRunner::cancel_with_graph_state`. The result is
  `AgentRunCancellation { transition, cancelled_node_ids, settled_entries }`,
  `#[non_exhaustive]`.
- **5.2's operation** is `AgentGraphEffectBridge::settle_cancelled_nodes`. The
  rule itself is the free function `settle_cancelled_outbox_entries` beside it,
  and the fleet's `settle_cancelled_effects` now calls it, so there is one copy
  of the rule. Each settled entry is reported as `AgentSettledOutboxEntry`
  with an `AgentOutboxSettlement` (`Cancelled`, `CancelledInFlight`). A
  pending entry is cancelled under `GRAPH_NODE_CANCELLED_REASON`
  (`graph-node-cancelled`).
- **The bridge's outcome.** `AgentGraphEffectCommandOutcome` had public fields
  and was not `#[non_exhaustive]`. It gains `settled_entries` and becomes
  `#[non_exhaustive]`. No consumer in this repository or in rakka.host
  constructs or destructures it.
- **The crash fault** is a store wrapper in the test file whose `n`th
  compare-and-set fails once. The workflow tests had no shared fault kit for
  run and workflow stores together.
- **`NodeCancelled`** sits after `NodeSkipped`. No doc lists the event kinds.
- **The core's completion** settles through a crate-private
  `AgentGraphRuntime::complete_node_settling`. The public `complete_node`,
  which has only the runner, is unchanged, and its doc says it does not
  settle.

**Calls made while building** (each in the ledger):
- `cancel_with_graph_state` refuses a graph that is not `Cancelled`
  (`InvalidRunState`), so a run never ends cancelled over a running graph.
  This guard was not in the design, and it has its own test.
- `cancelled_node_ids` lists the changed nodes the cancel's state shows
  `Cancelled`, not the bare changed list. Today the two are the same.
- **Section 6's "a run whose graph already completed" row** cannot be
  reached as written. A graph that completes through `update_graph_state`
  ends the run `Completed`, and `request_cancellation` refuses an ended run.
  The test drives the order that can happen: a cancel is requested, then the
  graph completes (the run ends `Completed`, as before), then `Cancel` is
  refused, with nothing left due.
- **Section 6's combined row** (effects, a checkpoint and a loop instance in
  one cancelled run) is split in two. The run-level tests cover effects and a
  checkpoint. The loop instance is covered by
  `cancel_graph_state_matches_cancel_graph_run`, which holds the state-only
  cancel equal to `cancel_graph_run` over a running loop iteration. The run
  core has no iterator operation.
- **No product doc** describes graph-run cancellation. What `Cancel` settles is
  in `docs/rakka-compatibility.md` and in the graph runbook's new section,
  "Effects Due Under Cancelled Nodes".

**Section 6's rows and their tests** (`crates/rakka-agent-workflow/tests/`):

| Row | Test |
| --- | --- |
| The gap, first | `graph_cancellation.rs::a_cancelled_graph_run_sends_none_of_its_queued_effects`. Before the change it failed with one dispatch |
| Effects and a checkpoint cancelled | `cancel_settles_a_checkpoints_approval_request` |
| A loop instance | `graph_scheduler.rs::cancel_graph_state_matches_cancel_graph_run` |
| An entry mid-dispatch | `an_entry_mid_dispatch_is_cancelled_with_its_attempt_counted` |
| A resolved node with a still-cancellable entry | `only_the_nodes_the_transition_cancelled_are_settled` |
| A crash between the settle and the run write | `a_crash_between_the_settle_and_the_run_write_converges`, whose repeat runs on a new core that recovers the same stores |
| A run with no graph | `a_run_without_a_graph_cancels_as_before` |
| A run whose graph already completed | `cancel_after_the_graph_completed_in_the_same_pass` |
| `Cancel` on a `Cancelled` run | `cancel_on_a_cancelled_run_is_refused` |
| A graph completing with a sibling due | `a_graph_completed_while_a_sibling_waits_settles_the_siblings_entry` |
| A graph failing, both bridge paths | `a_graph_failed_by_an_effect_failure_settles_the_siblings_entry`, `an_abandon_decision_settles_the_siblings_entry` |
| 5.2's operation on a transition that cancelled nothing | `a_transition_that_cancelled_nothing_settles_nothing` |
| The actor's `Cancel` | `the_actor_answers_cancel_as_the_core_does` |
| The two scheduler operations | `cancel_graph_state_matches_cancel_graph_run` |
| `NodeCancelled` | `runtime_events.rs::node_cancelled_is_a_node_event` |

The plan's Review Focus added:
- `a_settle_that_fails_part_way_converges`;
- `a_terminal_or_absent_entry_is_left_alone`;
- `a_replayed_failure_command_settles_nothing_more`;
- `the_hosts_own_cancel_sequence_still_works`.

**Falsification.** Each break was applied, the suites run, and the break
reverted:

| Break | Tests that failed |
| --- | --- |
| Settle every node, not the changed and `Cancelled` ones | `only_the_nodes_the_transition_cancelled_are_settled`, `a_transition_that_cancelled_nothing_settles_nothing` |
| Cancel an entry mid-dispatch without counting the attempt | `an_entry_mid_dispatch_is_cancelled_with_its_attempt_counted` |
| Refuse an absent entry | `a_terminal_or_absent_entry_is_left_alone`, `a_settle_that_fails_part_way_converges` |
| Settle nothing in `Cancel` | the gap test, the checkpoint test and the crash test |
| Settle after the run write | the crash test |
| Record the write as `GraphUpdated` | the gap test |
| Drop the `Cancelled`-graph guard | `the_runner_cancels_with_a_graph_only_when_it_is_cancelled` |
| Skip the settle in `complete_graph_node` | the completion test, `cancel_after_the_graph_completed_in_the_same_pass` |
| Skip it in `accept_and_apply_effect_failed` | the effect-failure test, the replay test |
| Skip it in `accept_and_apply_reconciliation_decision` | the `Abandon` test |

**Drafts in `tmp/`, not filed:** `2026-11-27-step-run-cancel-leaves-entries-due.md`
and `2026-11-27-iterator-bound-leaves-siblings-due.md`.
