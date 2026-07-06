# Changelog

All notable Rakka changes should be recorded here before a release candidate is cut.

The format follows Keep a Changelog style sections, and versioning is expected to follow SemVer once v1 release candidates begin.

## Unreleased

### Added

- V1 hardening foundations for TCP remoting, cluster runtime integration, compatibility matrix tests, generated HTTP/gRPC contract examples, observability exporters, Kubernetes local-cluster scenarios, security defaults, and repository validation scripts.
- `scripts/validate.sh` as the required local validation entry point.
- `scripts/package-check.sh` as the offline package metadata and publishability check entry point.
- V1 release-candidate review docs for reliability boundaries, N/N+1 rolling updates, known limitations, post-v1 roadmap, and final review checklist.
- Phase 5 agent workflow autonomy APIs: effect target catalog and policy validation, A2A peer adapter contracts, dispatcher registry routing, A2A peer/webhook/push dispatch classes, and deterministic dispatcher cancellation marking.

### Changed

- Workspace MSRV is raised from Rust 1.80 to Rust 1.85, required by the published A2A SDK crates used by the Phase 0 clustered sharded A2A agents example.
- The workspace `axum` dependency is upgraded from 0.7 to 0.8. `axum` types appear in the `rakka-http` public API, and axum 0.8 replaces the `/:param` route syntax with `/{param}` and changes websocket text/close payloads to `Utf8Bytes`.
- Workspace crates now share release metadata and internal path dependencies include explicit versions for packaging.
- CI separates required validation from optional PostgreSQL and local Kubernetes integration jobs.
- Historical implementation plans now live under `docs/plans/` instead of the product-doc root.
- `rakka-workflow` outbox entries support a terminal `Cancelled` status: `DurableInbox::record_outbox_cancelled` settles an undelivered entry before dispatch, cancelled entries are never due again, and compaction reclaims them like other terminal entries.
- Agent dispatcher cancellation is durable end to end: `AgentDispatcherWorker::cancel_run_dispatches` settles cancelled effects at the outbox layer, in-flight entries carry a typed `cancellation_requested` flag that blocks re-claiming after lease expiry, and worker refresh finalizes expired cancellation-requested entries as cancelled instead of redelivering them.
- Agent dispatch target classification only honors `target_class` attribute and target-type refinements that are compatible with the effect kind, so a mislabeled effect can no longer be routed to a dispatcher that deterministically rejects it.
- Agent autonomy policy classification (`AgentAutonomyTargetClass::for_effect`) is derived from the dispatcher's `AgentDispatchTargetClass::classify`, removing the name-substring webhook/push heuristics so policy admission and dispatch routing always agree on an effect's class.

### Security

- Security and operational defaults are documented in `docs/rakka-v1-security-operational-defaults.md`.
- Internal remoting is documented as trusted-cluster traffic and production exposure remains out of scope for v1 without an operator-provided security layer.

### Validation

- Required validation: `scripts/validate.sh`.
- Packaging validation: `scripts/package-check.sh` in Cargo offline mode.
- Release-candidate review: `docs/rakka-v1-release-candidate-review.md`.
- Optional PostgreSQL validation: `RAKKA_POSTGRES_TEST_DSN=postgres://postgres:postgres@localhost:5432/postgres cargo test -p rakka-persistence-postgres`.
- Optional Kubernetes validation: `RAKKA_K8S_RUN_LOCAL_CLUSTER=1 RAKKA_K8S_IMAGE=<image> examples/kubernetes/local-cluster-scenario.sh`.

## 0.1.0-v1-rc.0 Draft

This section is reserved for the first reviewable v1 release-candidate notes.

### Release Checklist

- Run `scripts/validate.sh` from a clean checkout.
- Run `scripts/package-check.sh` from a clean checkout.
- Review `docs/rakka-v1-release-packaging.md`.
- Confirm optional PostgreSQL and Kubernetes checks were either run or explicitly deferred.
- Update this changelog with user-facing changes, known limitations, and migration notes.
