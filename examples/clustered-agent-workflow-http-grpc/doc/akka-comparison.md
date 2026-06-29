# Akka comparison: cluster sharding, coordination, rebalancing, and split‑brain

> **Scope.** A design review of how **Akka Cluster Sharding** approaches shard
> coordination, the `ShardCoordinator` state, shard rebalancing, and split‑brain
> resolution, evaluated against how **Rakka** (`rakka-sharding` + `rakka-cluster`)
> implements the same concerns — through the lens of this example's
> requirements. This is an architecture review, not a feature gap list: in several
> places Rakka's simpler model is the *better* fit for this workload, and in a few
> places it is genuinely weaker. The companion doc
> [`kubernetes-etcd-discovery.md`](kubernetes-etcd-discovery.md) covers the
> deployment; the "Shard coordination" section there records why the fenced
> PostgreSQL coordinator is intentionally not used.
>
> Akka sources reviewed:
> [cluster-sharding](https://doc.akka.io/libraries/akka-core/current/typed/cluster-sharding.html),
> [cluster-sharding-concepts](https://doc.akka.io/libraries/akka-core/current/typed/cluster-sharding-concepts.html),
> [split-brain-resolver](https://doc.akka.io/libraries/akka-core/current/split-brain-resolver.html),
> [coordination](https://doc.akka.io/libraries/akka-core/current/coordination.html).
> Rakka sources reviewed: `crates/rakka-sharding/src/{coordinator,allocation,coordinator_lease,coordinator_store,runtime,node_runtime}.rs`,
> `crates/rakka-cluster/src/membership.rs`.

## The lens: this example's requirements

- **Dynamic autoscaling and downscaling** — membership grows and shrinks at
  runtime with no fixed replica count (driven by etcd discovery).
- **Symmetric topology** — every node hosts a slice of the runs, any node accepts
  ingress (HTTP/gRPC) and routes to the owner over `rakka-remote`.
- **Single‑writer per run** — at most one active run actor cluster‑wide, the
  correctness basis for the durable run/inbox/outbox model.
- **Durable recovery on owner change** — a run resumes on its new owner after
  scale‑in or pod failure (shared PostgreSQL store).
- **Kubernetes‑native** — pods come and go; no external consensus inside the app.

## 1. Sharding topology and the coordinator

**Akka.** A `ShardRegion` runs on every node and routes messages; entities map to
shards by `abs(entityId.hashCode) % number-of-shards` (default **1000**). The
**`ShardCoordinator` is a cluster singleton** — exactly one instance, on the
oldest member. A region that doesn't know a shard's home asks the coordinator
(`GetShardHome`), **buffers** messages until told, then forwards. The crucial
property: **one allocation authority is decoupled from per‑node hosting**.

**Rakka.** A `ShardRegion` runs per node; entity→shard uses a stable FNV hash
(`ShardId::for_entity`), shard count is configurable (this example uses 64). There
is **no singleton coordinator**. Instead, the `ClusterNodeRuntime` offers two
shapes, and `register_region` calls `ensure_coordinator`, which couples
coordinator and hosting:

- **Per‑node coordinator** (default, used here): each node runs its own
  `ShardCoordinator` and reconciles ownership locally.
- **Shared store + fenced lease**: a single `ShardCoordinatorStore` +
  `ShardCoordinatorLease`. Because `ensure_coordinator` requires holding the
  lease, only the lease holder can register a region — so this shape collapses
  hosting onto one node and cannot host symmetrically (see the deployment doc).

**Evaluation.** Akka's singleton‑coordinator + regions‑everywhere is the gold
standard: one authority *and* symmetric hosting. Rakka cannot express that
combination — you get one authority *or* symmetric hosting, not both. For this
example that is not fatal, because correctness is recovered a different way
(Section 2).

## 2. Coordinator state and how ownership stays consistent

**Akka.** Default coordinator state lives in **Distributed Data (ddata)**:
shard→region map replicated with `WriteMajorityPlus`/`ReadMajorityPlus`, in memory
(not on disk). When the coordinator node dies, the new singleton recovers the
majority‑replicated state; unknown shards buffer until recovery finishes — never
two active coordinators. (A deprecated event‑sourced `persistence` mode also
exists.) Consistency is internal — no external system required.

**Rakka.** Ownership is a `ShardOwnershipSnapshot` (revisioned). State is either
**in‑memory per node**, or a `ShardCoordinatorStore` (`InMemory`/`Postgres`) using
**revision compare‑and‑set**, optionally fenced by a `LeaseToken`. There is no
gossip/ddata layer. In the per‑node shape there is *no shared coordinator state at
all*; instead, consistency is achieved by **determinism + a consistent membership
feed**:

- Allocation is `DeterministicModuloShardAllocationStrategy` —
  `sorted_routable_nodes[shard % n]`. Given the same up‑set, every node computes
  the **same** owner for every shard, independently.
- The up‑set comes from **etcd** (`apply_discovery` in
  `src/etcd_discovery.rs`). etcd (Raft) is strongly consistent, so all nodes
  converge on the same membership and therefore the same ownership.

**Evaluation.** Akka replicates *state*; Rakka (here) replicates *the inputs*
(membership) and recomputes state deterministically. With a consistent membership
source this is sound and far simpler — no ddata, no coordinator failover. The
trade is that Rakka leans on an **external** consistent store (etcd) where Akka is
self‑contained. Note that `LeastShardAllocationStrategy` (Rakka has one) is **not**
safe across independent per‑node coordinators — it depends on per‑node shard
counts that can differ — so it implies the single‑coordinator shape.

## 3. Rebalancing and handoff

**Akka.** Pluggable `ShardAllocationStrategy`; default
**`LeastShardAllocationStrategy`** (assign new shards to the least‑loaded node,
move shards off heavy nodes), **bounded** per round by `rebalance-absolute-limit`
/ `rebalance-relative-limit`. **Handoff is graceful**: the coordinator tells
regions to buffer the affected shard, the current owner **stops its entities
(PoisonPill) and acks**, then the coordinator publishes the new home and flushes
buffers. Entity state is *not* migrated — recovery at the new home requires
persistence.

**Rakka.** `coordinator.reconcile(membership)` (in `coordinator.rs`) runs two
passes: (1) allocate orphaned/unowned shards via `allocate_shard`, then (2) apply
`allocation_strategy.rebalance(...)`. For deterministic‑modulo, `rebalance()`
returns **every** shard whose current owner ≠ deterministic owner, so a scale
event re‑homes all mis‑placed shards at once — **unbounded**, with **no handoff
protocol**. A "Move" simply changes where future asks resolve; the new owner
recovers run state from the durable store. There is no buffer / stop‑old‑before‑
start‑new / ack barrier, and no `down-removal-margin` analogue.

**Evaluation.** Two real deltas:
1. **No graceful handoff.** Akka guarantees the old owner stopped before the new
   one starts. Rakka does not — during a rebalance/membership‑propagation window,
   two nodes can briefly drive the same run. Correctness then rests on the durable
   **revision‑CAS** rejecting the second writer plus idempotent outbox dedup. Safe
   in practice for this durable model, but it is a *backstop, not a guarantee*.
2. **Unbounded rebalance.** Deterministic‑modulo moves all mis‑placed shards
   simultaneously (no `max_simultaneous_rebalance` on that path). Fine at this
   scale; with many shards or large state it would cause thundering recovery.

## 4. Split‑brain and failure detection

**Akka.** A **Split‑Brain Resolver** downing provider: after `stable-after`
(default **20s**) of no membership change, it deterministically downs one side.
Strategies — **keep‑majority** (default), **static‑quorum**, **keep‑oldest**,
**down‑all**, **lease‑majority**. `down-removal-margin` prevents a new
singleton/shard from starting until the old one is definitely stopped (the safety
property for "one writer"). **lease‑majority** uses the **Lease** API as a
tie‑breaker. Failure detection is **peer‑to‑peer** (phi accrual).

**Rakka.** `ClusterMembership` (`membership.rs`) has `Unreachable`/`Down` states
with `failure_timeout` and `down_after_unreachable` (timeout‑based downing), but
there is **no split‑brain resolver** — no quorum/majority/keep‑oldest/down‑all,
and no general Lease API for membership. In this example, downing is effectively
**delegated to etcd**: `apply_discovery` feeds the authoritative up‑set, so a node
that cannot renew its etcd lease is dropped from membership. etcd is the partition
arbiter.

**Evaluation.** Using a strongly‑consistent external store as the membership
arbiter is a legitimate substitute for an internal SBR (similar in spirit to Akka
Cluster Bootstrap relying on an external system). It holds **only while etcd is
the membership source**. Two caveats:
- **etcd liveness ≠ peer reachability.** Membership means "can reach etcd," not
  "peers can reach me." A node that reaches etcd and clients but is partitioned
  from its peers stays "up," yet its `rakka-remote` asks fail —
  `entity-no-route: remote ask timed out`. Akka's detector measures peer
  reachability; this example does not feed peer reachability into membership.
- If membership ever came from internal gossip instead of etcd, there is **no SBR
  to fall back on** — two live halves would result.

## 5. Side‑by‑side

| Capability | Akka | Rakka | Verdict for this example |
|---|---|---|---|
| One authority **and** symmetric hosting | ✅ singleton + regions | ❌ mutually exclusive | Gap — uses per‑node coordinators |
| Coordinator state | ✅ ddata majority (internal) | ⚠️ external store CAS, or recomputed | OK via etcd‑consistent membership |
| Deterministic, coordination‑free allocation | ➖ | ✅ deterministic‑modulo | **Rakka strength** here |
| Bounded, graceful rebalance + handoff | ✅ limits + stop/ack | ❌ unbounded, no handoff | Gap (mitigated by durable CAS) |
| Single‑writer under churn | ✅ singleton + down‑removal‑margin | ⚠️ at‑most‑once + revision CAS + determinism | Common‑case yes; backstopped |
| Split‑brain resolution | ✅ SBR strategies + lease | ❌ none; etcd is the arbiter | Acceptable while etcd is the source |
| Peer‑to‑peer failure detection | ✅ phi accrual | ⚠️ etcd liveness drives membership | Reachability gap |
| Remember‑entities / passivation policies | ✅ ddata/eventsourced, LRU/LFU/idle | ➖ not equivalent | Not needed (runs are durable, externally triggered) |
| Durable entity state by design | ➖ needs persistence add‑on | ✅ run + inbox/outbox + CAS built in | **Rakka strength** |

## 6. Review against the requirements

**What fits well.**
- **Deterministic‑modulo + etcd is the right architecture for this workload.**
  Every node derives identical ownership from the same etcd up‑set, so no
  in‑process consensus or singleton coordinator is needed. This is why the example
  scales 2↔4↔8 cleanly and routes consistently.
- **Durability is stronger than stock Akka.** Akka entities are in‑memory and need
  a persistence add‑on to survive handoff; Rakka's run + inbox/outbox + revision‑
  CAS store is the source of truth by design, so "recover on a new owner" is
  intrinsic and the CAS is a genuine second‑writer backstop.
- **etcd as the membership arbiter** is a reasonable stand‑in for SBR for a
  Kubernetes deployment.

**Where it is genuinely weaker (risks to know).**
1. **Single‑writer is "best‑effort + CAS," not guaranteed** — no handoff barrier
   / `down-removal-margin` equivalent.
2. **etcd liveness ≠ inter‑node reachability** — the most realistic production
   failure (etcd‑live but peer‑partitioned) is not reflected in membership.
3. **No split‑brain resolver in Rakka itself** — safe only because etcd is
   authoritative today.
4. **Rebalance is abrupt and unbounded** on the deterministic‑modulo path.
5. **The fenced coordinator is unusable for symmetric hosting** (documented
   finding) — it would give Akka‑grade fencing only by collapsing to one host.

**Recommendations (priority order).**
- **Keep deterministic‑modulo + etcd**; do not pursue the fenced lease for this
  topology.
- **Close the reachability gap**: feed `rakka-remote` peer reachability (or a
  health probe) into the up‑set, or have the ingress treat repeated
  `remote ask timed out` as "owner unreachable" rather than trusting etcd
  liveness alone.
- **Treat the durable layer as the real guarantee**: single‑writer = revision‑CAS
  + idempotency keys, not topology. Ensure every AgentEffect/outbox write stays
  CAS‑guarded and idempotent so a transient double‑drive is harmless.
- **If Akka‑grade guarantees are ever required**, the change is in `rakka-sharding`,
  not the example: **decouple coordinator from region** (a leader‑elected singleton
  coordinator via the existing `ShardCoordinatorLease`, with host/proxy regions on
  all nodes reading its published assignments) and add a **handoff barrier**. That
  is also the prerequisite for using `LeastShardAllocationStrategy` safely.

## 7. Bottom line

For a symmetric, autoscaled, durably‑backed workload, Rakka's deterministic‑modulo
coordinator driven by etcd is a sound and simpler‑than‑Akka design, and its
built‑in durability is an advantage over stock Akka. The honest deltas from Akka
are **no graceful handoff, no peer‑reachability failure detection, and no
split‑brain resolver** — all currently compensated for by making etcd the single
source of membership truth and by the durable revision‑CAS. Those compensations
hold for the deployment in this example; they would not hold if membership ever
came from internal gossip instead of an external consistent store.
