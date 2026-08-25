# Agent Domain Fault-Injection Matrix

Status: implemented (slice 6.1).

This document maps the durable boundaries of `rakka-agent` to the tests that
kill something at each one. The goal is not to claim exactly-once external
effects. It is to say precisely where durable recovery is proven, at what
fidelity, and what is still inferred rather than demonstrated.

The companion document for the workflow substrate is
[Phase 7.1 Failure-Injection Suite](plans/agentic-workflow/phase-7-1-failure-injection-suite.md);
the reliability contract both rest on is
[`rakka-v1-reliability-boundaries.md`](rakka-v1-reliability-boundaries.md).

## Two fidelities, and why both exist

Agent fault injection runs at two fidelities, and they prove different things.

**In-process** kills return an error from a `CrashingStateStore` and rebuild the
entity facade from the same in-memory store. That is a faithful model of an
owner dying, because a sharded entity is materialized on its owner, transitions,
and passivates — nothing but the store survives between calls, so every call is
already a restart. It covers every window cheaply and runs in ordinary
validation.

**Multi-pod** kills call `std::process::abort()` in a real OS process whose
durable store is a directory outside it. That is the only fidelity at which
[specification 15](plans/rakka-agent/spec.md)'s actual requirement can be
tested — durable state sufficient to recover *on a different pod, without
node-local memory* — because an in-memory store dies with the pod that held it.
It is gated, because it spawns processes.

Neither subsumes the other. The in-process matrix has the coverage; the
multi-pod harness has the fidelity.

## Multi-pod matrix

Harness: [`examples/multi-pod-agent-fault-soak`](../examples/multi-pod-agent-fault-soak).

| Failure | Expected result | Proof |
| --- | --- | --- |
| Owner pod dies before a durable write | The transition is lost; the surviving pod re-derives it from the shared record and the task still completes. | Sweep window `before-write`, every task-store and run-store write the armed pod reaches |
| Owner pod dies after a durable write, before acting on it | The record says one thing and nobody was told; the surviving pod finds it and finishes. | Sweep window `after-write`, every task-store and run-store write the armed pod reaches |
| Owner pod dies after the external system commits, before any receipt exists | The external ledger holds the commit; recovery produces at most a *retry of the same logical turn*, never a second turn under a different identity. | The ledger adapter commits before it answers; every window asserts one distinct ledger entry |
| The pod owning the task dies | The run's owner downs it, takes over the task's shard, and drives the task to `Completed`. | `Armed::PodB` rows of the sweep |
| The pod owning the run dies | Symmetrically, the task's owner takes over the run. | `Armed::PodA` rows of the sweep |
| Two pods race one compare-and-set | One wins; the loser gets a revision conflict rather than a lost update. | The shared store's `hard_link` commit claim |
| Two pods both accept the same creation | One agent and one task, not two — the commands deduplicate on derived operation ids. | Both pods seed every run |
| A pod is gone but not yet downed | The survivor refuses to drive shards it does not own rather than becoming a second writer. | Ownership gate in `flow::drive` |

Specification 18 scenarios this re-proves at multi-pod fidelity: **1** (the
creation-deduplication half), **2**, and **60**. That list is the authority for
slice 6.4's done-when: every other shipped scenario is proven in-process, and a
scenario moves here only when its claim cannot be reached by an in-process kill. It also satisfies section 18's
closing fault-injection directive — kill the owner at every durable effect
boundary, including after a test external system commits but before it returns
the receipt.

Each sweep row bounds itself: a pod that reaches its armed write records it
before aborting, and the row walks its ordinals until two consecutive ordinals
fire nothing, so every window in the reported total is one that fired and a
single missed ordinal is reported as a gap rather than truncating the row. Each
row carries a floor, so a row that collapses fails naming itself instead of
reading as a short one. The totals themselves vary run to run, because the write
counters move with TCP timing, membership convergence, and which pod wins the
seed compare-and-set.

All five entity classes share the drive loop's logical clock rather than the wall
clock four of them default to, so the sharded actors and the drive loop no longer
stamp one durable record with values ~1.75e12 apart. Every pod's exit status is
checked, both children are `kill_on_drop` with both driver waits bounded, and a
pod restarts its own deadline when it takes over its peer's shards rather than
measuring it from boot. Drive-loop errors are reported rather than discarded,
with a round that loses its compare-and-set to the other pod counted rather than
printed — two pods writing one record is the documented topology here, not a
fault, and printing it would bury the errors that are.

The harness skips in exactly one case — the sandbox refusing a loopback bind,
settled once before any world runs. Every other failure keeps its message and
fails, including the eight wiring failures inside `boot_pod` that used to be
reported as that skip with an exit code of zero.

The reference world also replays the agent's instantiation through its *shard*,
asserting that exactly one pod's command crossed the wire. The agent class is
the only one of the five addressed by a serializable command rather than an
exchange envelope, so it is the only one whose remote registration can be made
and never exercised — and whose required payload codecs can be absent with
nothing noticing.

A crash marker records that a pod died, not that a shard moved, and the harness
reports the two separately. A window that moved a shard downed the departed pod,
took over its shards, and re-materialized its entities on the survivor; a window
that did not is still a real recovery, but its armed pod had finished its part
before dying, so the survivor never needed the dead pod's shards. The takeover
count is held to a floor, and the reference world asserts the task and the run
are owned by different pods before any of it is believed.

Deliberate limits are documented in the harness README: the shared directory
stands in for a shared durable backend (production is PostgreSQL), departure is
announced rather than detected, history sinks are per-pod, and the team and
conversation entities are registered but not exercised by the workload.

The agent store and the durable workflow outbox are armed alongside the task and
run stores, so the effect boundary specification 18's directive names is swept
rather than unreachable. And the second owner's *own* recovery writes have their
own world: every other row kills the pod that natively owns what it is writing,
while that one kills the pod that inherited it — pod A dies at its first
task-store write so pod B must take the run's shard over, pod B is armed at its
`nth` write, and a third pod replaces it, downs both, and finishes. It runs once
per store the second owner touches after inheriting the shard — run state and
effect outbox — so specification 18's named boundary is swept on the inheriting
pod as well as on the original owner.

## In-process matrix added by slice 6.1

| Failure | Expected result | Test |
| --- | --- | --- |
| Owner dies at any write of a fan-out, child result, or fan-in resolution | One resolved group, one result per cell, two logical children however often a send retried. | `cargo test -p rakka-agent --test fan_in_recovery` |
| A `DelegationResult` envelope is lost, its reply is lost, or it is delivered twice | One resolved group either way. | `cargo test -p rakka-agent --test fan_in_recovery the_delegation_result_survives_every_delivery_fault` |
| Owner dies at any write of a cancellation propagation | Every child terminal under the requested reason, every chase accepted, the root finalized only after its ledger closed, and no propagation leg replays a send. | `cargo test -p rakka-agent --test cancellation_recovery` |
| Coordinator dies inside the compare-and-set that spends a delegation ceiling | The quota is charged once, one child is admitted, and the refusal the model corrects course from survives exactly once. | `cargo test -p rakka-agent --test delegation_limits the_descendants_ceiling_stays_exact_across_every_coordinator_loss` |
| A tool or credential is revoked while a run waits on a checkpoint | The attempt the later resume produces is refused before the checkpoint gate is consulted, even under an approval granted after the revocation. | `cargo test -p rakka-agent --test wait_invalidation` |
| A guardrail policy is selected, or the deployed chain upgraded, while a run waits | The resumed attempt refuses rather than running under the wrong policy or chain revision. | `cargo test -p rakka-agent --test wait_invalidation` |
| An agent is suspended while a run waits | The resumed attempt defers without spending the intent's budget, and the decision taken during the suspension survives it. | `cargo test -p rakka-agent --test wait_invalidation an_agent_suspended_during_the_approval_wait_defers_the_resumed_attempt` |
| A definition is narrowed while a task is blocked | The assignment decision taken when the wait ends is refused, and the task stays assignable rather than failing. | `cargo test -p rakka-agent --test wait_invalidation a_definition_narrowed_while_a_task_is_blocked_refuses_its_later_assignment` |
| A revocation and an owner loss interleave | Both survive: the revocation is part of the durable record the new owner reads. | `cargo test -p rakka-agent --test wait_invalidation a_revocation_during_the_wait_survives_every_owner_loss` |

## Bounded behaviour under repetition

| Bound | Test |
| --- | --- |
| An agent's durable record does not grow with the number of tasks it has served | `cargo test -p rakka-agent --test agent_soak` |
| The metric series set is fixed however many transitions are recorded — no identifier reaches a label | same |
| Each task's materialized record stays inside `AGENT_TASK_MATERIALIZED_MAX_BYTES` | same |
| Every exchange journal settles empty | same (the drive loop's exit condition) |
| One model call per task, however many tasks ran before it | same |

Scale it with `RAKKA_AGENT_SOAK_ITERATIONS`; at 500 iterations the agent record
and the series count are unchanged while observation volume grows twentyfold.

## Repeatable commands

```sh
cargo test -p rakka-agent --test fan_in_recovery
cargo test -p rakka-agent --test cancellation_recovery
cargo test -p rakka-agent --test delegation_limits
cargo test -p rakka-agent --test wait_invalidation
cargo test -p rakka-agent --test agent_soak
RAKKA_AGENT_SOAK_ITERATIONS=500 cargo test -p rakka-agent --test agent_soak
```

Gated multi-pod path:

```sh
RAKKA_RUN_MULTI_PROCESS_COMPATIBILITY=1 \
  cargo test -p rakka-testkit --test compatibility_matrix -- --nocapture
```

Or the harness directly, which needs no gate:

```sh
cargo run -p rakka-example-multi-pod-agent-fault-soak
```

## Production interpretation

Passing these means the durable boundaries are test-backed, at the fidelity each
table names. It does not remove the need for:

- downstream idempotency keys for external effects, and reconciliation for
  ambiguous ones;
- a real shared durable backend — the multi-pod harness's shared directory is a
  stand-in for PostgreSQL, not a substitute for testing against it;
- membership and downing policy tuned for the deployment, since the harness
  announces departure rather than detecting it;
- Kubernetes-level pod eviction, rollout, and drain testing;
- security validation (slice 6.2) and telemetry/Collector validation (slice 6.3).
