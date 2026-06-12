# Rakka Plans

This directory contains historical and active implementation plans. These files are useful for understanding how Rakka was built, but they are not the primary user-facing documentation.

Use the product docs in `docs/` for current behavior, validation, examples, compatibility, reliability boundaries, and release-candidate review.

## Plan Files

- `rakka-v1-implementation-plan.md`: original v1 implementation plan.
- `rakka-phase-3-continuation-plan.md`: Phase 3 remote, cluster, and sharding continuation plan.
- `rakka-phase-4-continuation-plan.md`: Phase 4 process actor and durable workflow continuation plan.
- `rakka-phase-5-continuation-plan.md`: Phase 5 stream, HTTP/gRPC, Kubernetes, and metrics continuation plan.
- `rakka-v1-hardening-plan.md`: V1 hardening slice plan.
- `rakka-akka-parity-implementation-plan.md`: post-v1 Akka Core parity and API simplification plan.
- `rakka-akka-parity-phase-4d-follow-up-plan.md`: detailed durable coordinator follow-up plan for persistent stores, leases, remembered entities, and recovery-after-movement examples.

When a plan creates durable user-facing behavior, update the relevant product doc in `docs/` as the source of truth.
