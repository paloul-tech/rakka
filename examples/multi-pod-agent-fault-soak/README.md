# Multi-Pod Agent Fault and Soak Harness

Slice 6.1 of the [`rakka-agent` plan](../../docs/plans/rakka-agent/implementation-plan.md).
Two real OS processes, one shared durable directory, and the durable agent
entities killed at every write they make.

## What it proves that no other test can

Every other agent proof in this repository runs in one process over one
in-memory store. That proves entities *re-materialize*: the fixture drops the
entity facade and builds a new one, so "every call is already a restart". What
it cannot reach is the claim
[specification 15](../../docs/plans/rakka-agent/spec.md) actually makes —
durable state must be sufficient to recover an agent, task, or run **on a
different pod, without node-local memory**. A store that lives in the dying
process's memory cannot test that, because the store dies too.

Three things here are firsts for the repository:

1. **All five sharded entity classes register through their *remote*
   registrations, and all five are exercised.** `init_agent_entity_remote_sharding`
   and its four siblings were defined, exported, and called nowhere before this
   example. Four are addressed by exchange envelope through the router. The
   fifth takes a serializable `AgentEntityCommand` instead, with payload codecs
   the application must register — so it is the one that can be registered and
   silently unreachable. The reference world replays the agent's instantiation
   through its shard and asserts that exactly one pod's command crossed the
   wire; without the codecs, the remote arm cannot encode and the harness
   fails.
2. **A real agent entity's exchanges travel the production
   `ShardedExchangeRoute`.** Every acceptance example uses the testkit's
   `LocalShardedExchangeRoute`, whose own documentation says it is "the local
   arm ... without the `rakka-remote` ask client the other arm needs".
3. **The durable store is outside every pod.** Each committed revision is a
   file, and the commit is a `hard_link` claim: two pods racing one
   compare-and-set cannot both win, so stale-owner rejection is real rather
   than simulated.

## Run

```sh
cargo run -p rakka-example-multi-pod-agent-fault-soak
```

Expected output. The write counts and the window totals vary run to run — the
counter behind them is incremented by the sharded entity actors as well as the
drive loop, so it moves with TCP timing, membership convergence, and which pod
wins the seed compare-and-set. What does not vary is the shape: a reference
line, one line per sweep row, and a total in which every window fired.

```
Rakka multi-pod agent fault harness
reference: two pods completed the task; pod-a wrote tasks=3 runs=9, pod-b wrote tasks=4 runs=0; the agent entity was commanded across the wire
  PodA tasks: 6 windows, ordinals 1-3
  PodA runs: 18 windows, ordinals 1-10
  PodB tasks: 6 windows, ordinals 1-3
  PodB runs: 0 windows — unreachable: pod B reaches its run writes only after taking over, which no world that arms pod B does
swept 30 pod-loss windows; every one fired and converged from the shared record (25 moved a shard to the survivor, 5 were finished by the surviving owner without one)
```

**A crash marker says a pod died, not that a shard moved.** The two numbers in
the summary are different claims. A window that *moved a shard* downed the
departed pod, took over its shards, and re-materialized its entities on the
survivor — the whole of specification 15. A window finished *without* a
takeover is still a real recovery, but the armed pod had already done its part
before dying, so the survivor completed from the shared record without ever
needing the dead pod's shards. Both converge; only the first exercises shard
movement, so the harness holds the takeover count to a floor rather than
counting markers and calling them recoveries.

Each row also carries a floor, because `PodB runs: 0 windows` is legitimately
zero for a *single-arming* world: a bare "did anything fire?" guard cannot tell
an intended zero from a regression that stopped a row dead. Pod B drives the run
only once pod A is gone, and pod A only goes when *it* is the armed pod.

**The second owner's own writes get their own world.** Every other row kills the
pod that natively owns what it is writing; that one kills the pod that
*inherited* it. Pod A is armed at its first task-store write, so it never drives
anything and pod B must take the run's shard over; pod B is armed at its `nth`
run-store write, and since it makes none until it owns that shard, every ordinal
is a write it made as the second owner, redriving a record it did not create.
A third pod replaces it, downs both departed pods, and finishes. Pods A and B
see only each other, so while they run the cluster is the same two-node shape
every other row sweeps; the replacement joins knowing all three, which is what
lets it down them.

It runs once per store the second owner touches after inheriting the shard — its
run state and its effect outbox — so the boundary specification 18's directive
names is swept on the inheriting pod as well as on the original owner.

Set `RAKKA_MULTI_POD_VERBOSE=1` to see each pod's exit line — which entities it
owned, whether it took over from a departed peer, how many rounds lost their
compare-and-set to the other pod, and what the durable record said when it
stopped.

Drive-loop errors are surfaced without the flag, because a loop that errored
every round is the diagnosis for a world that failed to converge. A round that
loses its compare-and-set to the other pod is not one of them: two pods writing
one record is the topology here, so those are counted into `lost-writes=` and
never printed. Printing them would bury the errors that matter under the ones
that do not.

Through the gate, alongside the repository's other multi-process check:

```sh
RAKKA_RUN_MULTI_PROCESS_COMPATIBILITY=1 \
  cargo test -p rakka-testkit --test compatibility_matrix -- --nocapture
```

## The shape of one run

The driver picks two loopback ports and re-execs itself twice — the idiom
`examples/multi-node-sharding` established here. Each pod boots an
`ActorSystem`, a `TcpRemoteTransport`, and `ClusterSharding`, registers all five
entity classes for remote hosting, and installs one production
`ShardedExchangeRoute` per class.

Both pods then seed: they instantiate the agent and create the task. The
commands deduplicate on derived operation ids, so two pods issuing them produce
one agent and one task — which is also what an ingress redelivering to whichever
pod is up actually does.

Both pods are `kill_on_drop` and both of the driver's waits are bounded, so an
early return anywhere reaps them rather than leaving them running to their own
deadline in a directory the driver has stopped watching. Each pod's exit status
is checked: the surviving pod is the one that performs the recovery a window
exists to prove, so one that panicked or was killed leaves a converged record
nothing produced on purpose. Which pod is the survivor comes from the arming
rather than from which one reported — an armed pod can reach its write inside
`system.shutdown()`, after its work is done and its line already flushed, so
"both pods reported" is a real and convergent outcome.

A pod that takes over its peer's shards restarts its own deadline at that
moment. Measured from boot, the later its peer died the less time it had for the
recovery being tested, and running out reported as "the task did not converge on
Completed" — pointing at the agent domain rather than at the harness's budget.

**There is exactly one way this harness skips**, and it is settled before any
world runs: if the sandbox refuses a loopback bind at all, the driver says so
and stops. Everything after that point fails loudly with its own message. This
used to be eight unrelated failures inside `boot_pod` — codec registration, the
runtime build, `ClusterSharding`, and each of the five entity registrations —
funnelled through `.ok()?` into one `None` reported as "loopback binding is
unavailable" with an exit code of zero, which both gate tests accepted as a
pass. A pod that finds its port taken between the driver choosing it and the
pod binding it is neither a skip nor a failure: the driver re-runs that world on
fresh ports.

The reference world asserts that the task and the run are owned by *different*
pods before any of this is believed. A shard-count or hashing change that
co-located them would take the cross-wire property away silently — every other
assertion in the harness still passes with both entities on one pod.

Each pod drives only the entities whose shards it own. In practice the task and
the run land on different pods, so the task's owed run-creation exchange has to
cross the wire; the run's result proposal has to cross back. The model call goes
to an adapter that appends to a shared ledger file **before** it answers, which
is [specification 18](../../docs/plans/rakka-agent/spec.md)'s window: the
external system has committed and no receipt exists anywhere.

When one pod exits — killed, or finished — the driver announces its departure
and the survivor calls `mark_down` on it. `mark_down`, not `mark_leaving`: a
killed pod never got to leave, and a downing decision is what an operator or a
failure detector produces for one. The shards move, the entities re-materialize
from the shared directory, and the survivor finishes the work.

## The sweep

The sweep replays the world once per `(pod, store, write ordinal, window)`,
arming that pod to `abort()` at exactly that write. `abort()` rather than a
panic: a panic unwinds, runs destructors, and lets the harness observe an
orderly failure, and a pod loss is none of those.

**Each row bounds itself.** A pod that reaches its armed write records the
window in a `crashed` marker before aborting, so the driver can tell a window
that fired from one whose armed write the flow never reached; a row walks its
ordinals until two *consecutive* ordinals fire nothing. Two, not one: the write
counters move with TCP timing and membership convergence, so an armed pod can
miss ordinal `n` and still reach `n + 1`, and stopping at the first miss would
truncate the row and report the rest as swept. An ordinal walked past this way
is reported as a gap rather than absorbed. Reading the row's length
off the crash-free reference run instead would measure one world and arm
another — only an armed world ever loses a pod, downs it, and recovers its
entities on the survivor — and the counts drift between runs on one machine
regardless. Ordinal `n` would then name whatever the reference's `n`th write
happened to be, and every ordinal past the armed world's own last write would
be a world that killed nothing and converged trivially. A window in the total
is a window that fired.

A world whose armed write is never reached is still a crash-free world, so it
is still asserted; it is simply not counted as a pod-loss window.

Each window asserts, from the shared directory after every pod is gone:

- the surviving pod itself saw the task reach a terminal status, so a converged
  record cannot be credited to a pod that stopped for some other reason;
- the task's durable status is `Completed`;
- the external ledger was reached, and reached for exactly **one logical turn**
  — a pod killed after the external commit may legitimately cause a *retry* of
  that turn, but never a second turn under a different identity.

## Deliberate limits

- **The shared directory stands in for a shared durable backend.** Specification
  15 forbids pod-local state as the production source of truth; a shared volume
  is not what it means by "pod-local", but neither is it a database. Production
  is PostgreSQL through `PostgresDurableStateStore`, which is already generic
  over the state type, so that is a substitution rather than a rewrite.
- **Departure is announced, not detected.** A real deployment learns it from
  etcd, DNS, or the Kubernetes API. Announcing it makes shard movement happen at
  a determined moment instead of a timing-dependent one — and until the survivor
  reconciles, it correctly refuses to drive shards it does not own.
- **History sinks are per-pod.** Task, team, and conversation history is bounded
  observability behind an authorized cursor, never the correctness source, and
  this harness asserts convergence only from durable entity state. A file-backed
  history that passes `assert_task_history_store_contract` is owed work.
- **Team and conversation entities are registered but not exercised by the
  workload.** Registering them is what proves the remote registration path; a
  coordination workload across pods is owed work.
