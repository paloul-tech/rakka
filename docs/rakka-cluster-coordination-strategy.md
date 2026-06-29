# Rakka Cluster Coordination Strategy: External Arbiter (etcd / Kubernetes)

Status: Proposed (pending review)
Date: 2026-06-28

## Scope

This is a framework-level direction for how Rakka should handle cluster
membership consistency, failure detection, and shard hand-off — the areas where
Rakka differs from Akka Cluster. It refines the post-v1 roadmap item
*"Durable shard coordinator backend or consensus integration"*
(`rakka-v1-known-limitations-roadmap.md`) and follows from the Akka comparison in
`examples/clustered-agent-workflow-http-grpc/doc/akka-comparison.md` and the
`rakka-akka-core-gap-report.md`.

It is broader than any one example. The decisions here apply to Rakka's
clustering/sharding layers (`rakka-cluster`, `rakka-sharding`) and to how
applications are expected to deploy them.

## Operating assumption

For Rakka's target deployments, **a strongly-consistent external coordinator
(etcd) and Kubernetes are effectively always present.** This assumption is the
basis of the strategy below. Where it does not hold (no external arbiter; membership
sourced from internal gossip), the guarantees in this document do not apply and
the gaps revert to those described in the Akka comparison.

## Summary (the decision)

Rakka should **not** re-implement Akka's self-contained consensus stack — gossip
membership, Distributed Data (ddata), phi-accrual failure detection, and a
Split-Brain Resolver. Akka builds those because it assumes **no** external
coordinator. Rakka can assume one, so it should **lean on etcd/Kubernetes as the
external consistency substrate** and add only the one thing the external arbiter
cannot provide.

Net:

- **Split-brain resolver — build nothing.** Adopt the external arbiter as a
  first-class, documented membership contract. With membership defined by a live
  etcd lease, two partitions cannot form two clusters, so an internal SBR is
  redundant and would conflict with etcd.
- **Peer-reachability failure detection — build one thing: self-fencing.** This
  is the only real gap etcd does not close. A node that cannot communicate with
  its peers over `rakka-remote` should fence *itself* (drop its etcd lease / fail
  readiness), converting a reachability fault into a *consistent* membership
  change.
- **Graceful hand-off — build nothing required; rely on durability.** Rakka's
  durable run + inbox/outbox + revision compare-and-set already make a transient
  double-writer safe, which is the failure mode Akka's stop-the-world hand-off
  exists to prevent. Bounded rebalancing is optional polish.

This is one real engineering item (self-fencing), one documentation/packaging
item (the external-arbiter contract), and a deliberate decision **not** to build
the rest.

## Implementation status

All four agreed actions have landed (branch `rakka-vs-akka-sharding-review`):

- **Bounded rebalance** — `DeterministicModuloShardAllocationStrategy::with_max_simultaneous_rebalance`
  in `rakka-sharding` (deterministic, converges; default unbounded).
- **Self-fencing** — `SelfFenceDetector` / `SelfFenceConfig` / `SelfHealth` in
  `rakka-cluster` (hysteretic policy core). Actuator is
  `rakka_discovery_etcd::EtcdDiscoverySession::leave`.
- **External-arbiter contract + CAS single-writer guarantee** — documented in
  `rakka-v1-reliability-boundaries.md`.
- **External-arbiter provider** — new `rakka-discovery-etcd` adapter crate
  (cache-backed `DiscoveryProvider` + leased registration), wired into the `rakka`
  facade behind the opt-in `discovery-etcd` feature. Kubernetes reuses the existing
  `rakka-k8s` DNS discovery and downward-API identity helpers.

Remaining integration (application glue, not framework): feed the
`SelfFenceDetector` from a peer-reachability signal (for example repeated
remote-ask timeouts) and call `EtcdDiscoverySession::leave` when it fences. The
framework pieces for that wiring are all in place.

## Background: the three deltas from Akka

Akka Cluster Sharding provides, and Rakka does not:

1. **A graceful shard hand-off protocol** — the coordinator buffers messages for a
   moving shard, the old owner stops its entities and acks, then the new owner
   activates. Combined with `down-removal-margin`, this guarantees the old owner
   has stopped before the new one starts.
2. **Peer-to-peer failure detection** — phi-accrual detectors measure whether
   *peers* can reach a node, independent of any external system.
3. **A Split-Brain Resolver** — after a stability window, deterministically downs
   one side of a partition (keep-majority, static-quorum, keep-oldest, down-all,
   lease-majority) so two halves never run two coordinators / two writers.

The full analysis is in the example's `akka-comparison.md`. The rest of this
document is the recommended Rakka response to each, under the operating
assumption.

## Decision 1 — Split-brain: adopt the external arbiter, build no SBR

**Decision.** Make "membership is defined by a consistent external arbiter" a
first-class, supported Rakka contract. Do not implement an internal split-brain
resolver.

**Rationale.** When a node is a member iff it holds a live etcd lease, a network
partition cannot produce two independent membership views: both halves consult the
same etcd, and the half that loses etcd loses its leases and self-removes. The
failure modes degrade safely:

- Minority partition loses etcd → its nodes' leases lapse → removed (correct).
- etcd globally unreachable → no lease renewals → no new ownership decisions
  (fail-stop / frozen, not split-brain). Existing durable state is untouched.

An internal keep-majority/static-quorum/lease-majority resolver would be redundant
with etcd and could make conflicting downing decisions against it.

**Actions.**

- Promote the example's etcd discovery into a supported `rakka-cluster`
  `DiscoveryProvider` (etcd, and a Kubernetes-API variant), rather than
  example-only code.
- Document the contract explicitly: *membership MUST come from a consistent
  external arbiter; internal SBR is intentionally omitted; without such an
  arbiter, partition safety is not guaranteed.*

## Decision 2 — Peer reachability: add self-fencing (the one real addition)

**Problem.** etcd liveness ("can the node reach etcd and renew its lease") is not
the same as peer reachability ("can peers reach this node over `rakka-remote`").
A node can be etcd-healthy yet partitioned from peers (NetworkPolicy, partial
partition, saturation). etcd keeps routing work to it; the remote asks time out
(`entity-no-route: remote ask timed out`). This is the gap the external arbiter
does **not** close.

**Decision.** Add a lightweight self-health signal derived from `rakka-remote`
transport state (connect/idle timeouts, sustained ask failures). On sustained
peer-communication failure — with hysteresis to avoid flapping — the node fences
**itself**: it releases its etcd lease and/or fails its Kubernetes readiness
probe, so the external arbiter removes it from membership. This keeps a single
arbiter while closing the reachability gap, and is far lighter than Akka's
bidirectional phi-accrual detector.

**Hard design rule (invariant).** Peer-reachability signals must **never**
directly edit the local up-set used to compute shard ownership. Ownership must
remain a pure function of the externally-arbitrated membership; otherwise
independent nodes disagree on owners and routing breaks (the exact failure
observed when per-node coordinators race a shared store). Peer-reachability may
only:

1. trigger self-fence (drop the etcd lease / fail readiness), or
2. make ingress fail fast / retry instead of hanging.

**Actions.**

- Surface a transport-health signal in `rakka-remote` / `rakka-cluster`.
- Provide a self-fence hook (release external-arbiter registration; integrate with
  coordinated shutdown and the K8s readiness/drain path).
- Make remote-ask routing fail fast with a clear error on owner unreachability.

## Decision 3 — Graceful hand-off: rely on durability, bound rebalancing

**Decision.** Treat the durable layer — revision compare-and-set plus idempotent
inbox/outbox — as **the** single-writer guarantee, not cluster topology. Do not
make an Akka-style stop-the-world hand-off a correctness requirement. Optionally
add bounded rebalancing for operational smoothness.

**Rationale.** Akka needs hand-off because its entities are in-memory; a
double-writer corrupts state. Rakka is durable-first: during a shard move, if both
the old and new owner briefly drive the same entity, the loser's compare-and-set
fails and idempotent effects dedupe. The outcome is wasted/retried work, not
corruption. This is consistent with `rakka-v1-reliability-boundaries.md`
(at-most-once delivery; stronger guarantees built from durable state) and with the
durable coordinator rationale (`rakka-akka-parity-phase-4-durable-coordinator-rationale.md`).

**Actions.**

- Document that single-writer correctness rests on revision-CAS + idempotency,
  and ensure every durable/outbox write stays CAS-guarded and idempotent.
- Keep the cooperative drain for planned scale-in (preStop drain + `leave_local` +
  lease revoke); it already provides a graceful hand-off for the common
  Kubernetes case.
- Optionally bound the deterministic rebalance (cap simultaneous shard moves) to
  avoid thundering recovery at large scale. A true quiesce-before-activate barrier
  is a nice-to-have, not required.

## What Rakka will and will not build

**Will build / formalize**

- A supported external-arbiter discovery/membership provider (etcd + Kubernetes).
- A peer-reachability-driven self-fencing mechanism with hysteresis.
- Documentation of the external-arbiter contract and the CAS-based single-writer
  guarantee.
- Optionally, bounded rebalancing.

**Will not build (under the operating assumption)**

- An internal split-brain resolver (keep-majority / static-quorum / etc.).
- A gossip membership / Distributed Data replication layer.
- A phi-accrual bidirectional failure detector.
- A mandatory stop-the-world shard hand-off barrier.

## Relationship to the durable shard coordinator

This strategy does not retire the durable shard coordinator
(`rakka-akka-parity-phase-4-durable-coordinator-rationale.md`); that remains the
right mechanism for control-plane ownership state with revision fencing. The
constraint to keep in mind is that Rakka's current fenced coordinator **couples
coordination with hosting** (`register_region` requires holding the coordinator
lease), so it suits a single-coordinator topology, not symmetric every-node
hosting. If a single authoritative coordinator is ever required — for example to
use `LeastShardAllocationStrategy` or remembered-entities safely — it should be
implemented as a **leader-elected singleton coordinator decoupled from region
hosting**, using the same external substrate (an etcd / Kubernetes Lease for the
election) rather than an in-process election. That is a separate, larger
`rakka-sharding` change and is out of scope for this strategy.

## Roadmap impact

This refines the post-v1 roadmap item *"Durable shard coordinator backend or
consensus integration"* (`rakka-v1-known-limitations-roadmap.md`): the
recommended direction is **integration with an external arbiter (etcd /
Kubernetes), not an in-process consensus backend**, plus the self-fencing addition
and the documented external-arbiter contract.

## References

- `examples/clustered-agent-workflow-http-grpc/doc/akka-comparison.md` — detailed
  Akka↔Rakka analysis this strategy follows from.
- `docs/rakka-akka-core-gap-report.md` — framework-level Akka gap review.
- `docs/rakka-akka-parity-phase-4-durable-coordinator-rationale.md` — why the
  durable coordinator exists.
- `docs/rakka-v1-reliability-boundaries.md` — delivery and single-writer
  boundaries.
- `docs/rakka-v1-known-limitations-roadmap.md` — the roadmap item this refines.
- Akka: cluster sharding, cluster-sharding-concepts, split-brain-resolver,
  coordination (linked from the example's `akka-comparison.md`).
