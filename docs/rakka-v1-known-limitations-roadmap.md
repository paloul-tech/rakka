# Rakka V1 Known Limitations and Roadmap

This document records the important limits of the v1 release candidate and the most natural post-v1 work.

## Known Limitations

- Core actor and remote entity delivery are at-most-once by default.
- Durable workflow inbox/outbox reliability is opt-in.
- Exactly-once external side effects are not guaranteed.
- Rakka internal remoting is trusted-cluster traffic, not a public API.
- v1 does not include built-in TLS/mTLS, certificate lifecycle management, or service-mesh policy.
- v1 does not include a durable distributed consensus backend for shard coordination. The intended direction is an external arbiter (etcd/Kubernetes), not in-process consensus; see `rakka-cluster-coordination-strategy.md`.
- Kubernetes examples are reviewable manifests and scripts, not a full operator or Helm lifecycle.
- Process actors run child processes inside Rakka node containers; per-actor sidecars are future work.
- Rakka is not an OS sandbox for child processes.
- HTTP/gRPC adapters are integration surfaces, not a full web framework or auth platform.
- Protobuf compatibility is policy-driven; v1 does not automatically diff descriptors.
- Observability exporters provide Prometheus/OpenTelemetry-oriented primitives, not hosted dashboards or vendor agents.
- The repository is MIT-licensed (`LICENSE`), and every crate manifest declares `license = "MIT"`. Declaring a license is not permission to publish.
- Contributions are accepted under the same MIT terms as the project (inbound = outbound), as `CONTRIBUTING.md` states. The repository requires no contributor license agreement (CLA) and no Developer Certificate of Origin (DCO) sign-off.
- Packaging checks are validation-only and offline-only. They do not publish crates or upload artifacts.

## Agent Domain

The agent domain (`docs/rakka-agents.md`) ships outside the publishable crate set, and its open items are recorded where they are checkable rather than repeated here:

- `docs/rakka-agent-security-validation-matrix.md`, "Owed": the limits of communal search by meaning (below), the cross-tenant cost of the PostgreSQL search index, and the copies of a claim that erasure does not reach.
- `docs/rakka-agents.md`, "Search by meaning is opt-in": ranking communal claims by meaning (issue #140) is an exact scan of one tenant and space, with no approximate index and no way for the shipped statement to use one; it has no relevance floor, so the nearest admitted claims are answered however far they are; the snapshot records the retriever's version and not the embedding model; no metric counts embeddings that failed at append; nothing bounds an embedder call in time; a claim whose classification the deployment did not state for embedding is not found by meaning; an artifact-backed claim is embedded from its subject and predicate alone; and the query a turn sends to the embedder is not classified. The private tier's `reindex` still stops at a memory its embedder refuses, which the communal pass does not.
- `docs/rakka-agents.md`, "The dispatcher fleet record": the fleet index is one durable record per database, written whole on every claim and settle; a prune bounds it to live entries plus the finished ones inside its horizon, and nothing splits it per run or per shard. A worker that stalls past its lease inside a claim can still start an attempt on an unfinished ticket another worker is running, and nothing renews a lease during a call, so a lease has to be longer than the longest call.
- `docs/rakka-agent-telemetry-validation-matrix.md`, "Owed" and "Inferred": the segment classes with no production call site, and tail sampling untested against a running two-replica gateway.
- `docs/rakka-agent-fault-injection-matrix.md`: under "Multi-pod matrix", the team and conversation entities the multi-pod workload registers but does not exercise; under "Production interpretation", a PostgreSQL arm for the shared substrate and detected rather than announced departure.
- `docs/rakka-agent-recovery-scenarios.md`, "What the roster does not claim".

## Post-V1 Roadmap

Likely post-v1 work:

- Public API stabilization pass after user review.
- Durable shard coordinator backend or consensus integration. See `rakka-cluster-coordination-strategy.md` for the recommended direction: lean on an external etcd/Kubernetes arbiter rather than an in-process consensus backend, plus peer-reachability self-fencing.
- TLS/mTLS integration guidance for internal remoting.
- Operator or Helm-style packaging for Kubernetes.
- Generated application templates for HTTP/gRPC/actor/entity/workflow/process services.
- Additional durable stores beyond PostgreSQL.
- More production observability examples, dashboards, and alert guidance.
- Descriptor-based Protobuf compatibility checks.
- Per-actor sidecar or external workload ownership model.
- Release process finalization once the publishing policy is explicit.

## Review Questions

- Which APIs should be promoted from v1 draft to semver-stable first?
- Which coordination backend is the right next step for durable shard ownership?
- Which deployment target should receive the first production packaging story: raw manifests, Helm, or an operator?
