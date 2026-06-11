# Rakka V1 Known Limitations and Roadmap

This document records the important limits of the v1 release candidate and the most natural post-v1 work.

## Known Limitations

- Core actor and remote entity delivery are at-most-once by default.
- Durable workflow inbox/outbox reliability is opt-in.
- Exactly-once external side effects are not guaranteed.
- Rakka internal remoting is trusted-cluster traffic, not a public API.
- v1 does not include built-in TLS/mTLS, certificate lifecycle management, or service-mesh policy.
- v1 does not include a durable distributed consensus backend for shard coordination.
- Kubernetes examples are reviewable manifests and scripts, not a full operator or Helm lifecycle.
- Process actors run child processes inside Rakka node containers; per-actor sidecars are future work.
- Rakka is not an OS sandbox for child processes.
- HTTP/gRPC adapters are integration surfaces, not a full web framework or auth platform.
- Protobuf compatibility is policy-driven; v1 does not automatically diff descriptors.
- Observability exporters provide Prometheus/OpenTelemetry-oriented primitives, not hosted dashboards or vendor agents.
- The repository does not declare a license yet; release packaging must not claim one.
- Packaging checks are validation-only and offline-only. They do not publish crates or upload artifacts.

## Post-V1 Roadmap

Likely post-v1 work:

- Public API stabilization pass after user review.
- Durable shard coordinator backend or consensus integration.
- TLS/mTLS integration guidance for internal remoting.
- Operator or Helm-style packaging for Kubernetes.
- Generated application templates for HTTP/gRPC/actor/entity/workflow/process services.
- Additional durable stores beyond PostgreSQL.
- More production observability examples, dashboards, and alert guidance.
- Descriptor-based Protobuf compatibility checks.
- Per-actor sidecar or external workload ownership model.
- Release process finalization once the repository license and publishing policy are explicit.

## Review Questions

- Which APIs should be promoted from v1 draft to semver-stable first?
- Which coordination backend is the right next step for durable shard ownership?
- Which deployment target should receive the first production packaging story: raw manifests, Helm, or an operator?
- What license and contribution policy should the repository declare before any public release?
