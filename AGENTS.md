# AGENTS.md

## 1. System & Tech Stack

- Rakka is a Rust 2021 Cargo workspace for an Akka-inspired actor framework: typed actors, actor refs/paths, supervision, cluster membership, remoting, sharding, durable state/event sourcing, durable workflow inbox/outbox, bounded streams, process actors, HTTP/gRPC adapters, Kubernetes health/drain hooks, metrics, and operational snapshots. See `README.md`, `CLAUDE.md`, and `docs/rakka-actor-framework-spec.md`.
- The repo is a v1 release-candidate foundation plus active `rakka-agent-workflow` work for durable agent/compiled-workflow execution. Agent workflow plans live under `docs/plans/agentic-workflow/` and `docs/plans/compiled_execution_with_graph_schdlr/`.
- MSRV is Rust `1.85` from the workspace manifest; `rust-toolchain.toml` uses stable with `clippy` and `rustfmt`. gRPC/protobuf crates and examples require `protoc`.
- Main dependencies by surface: Tokio runtime, `prost`/`tonic` for Protobuf/gRPC, `axum` for HTTP, `tokio-postgres` for PostgreSQL adapters, `etcd-client` for etcd discovery, `serde`/`serde_json`, and `tracing`.
- Application code should prefer the top-level `rakka` crate and `rakka::prelude`; component crates remain public for foundations, adapters, advanced wiring, and tests. See `docs/rakka-api-boundary-inventory.md` and `docs/rakka-v1-api-review.md`.

## 2. Critical Commands & Tests

- Run from the workspace root. Canonical required validation:
  - `scripts/validate.sh`
  - `scripts/package-check.sh`
- `scripts/validate.sh` runs: `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --all-features -- -D warnings`, `cargo test --workspace --all-features`, no-default feature checks for `rakka-stream` and `rakka-process`, `cargo doc --workspace --all-features --no-deps`, and a safe Kubernetes dry run.
- `scripts/package-check.sh` is offline validation only. It runs `cargo package` checks with `--offline`, verifies publishable crate file lists, fully packages `rakka-core`, and confirms examples have `publish = false`.
- Useful focused checks:
  - `cargo test -p <crate>`
  - `cargo test -p <crate> --test <test_file> -- <test_name> --nocapture`
  - `cargo test -p rakka-testkit --test compatibility_matrix -- --nocapture`
  - `cargo test -p rakka-example-generated-contracts --test generated_contracts -- --nocapture`
  - `cargo test -p rakka-core --test observability_exporters`
  - `cargo test -p rakka-http --test observability_routes`
- Optional gated checks need external services or mutable infrastructure:
  - `RAKKA_POSTGRES_TEST_DSN=postgres://postgres:postgres@localhost:5432/postgres cargo test -p rakka-persistence-postgres`
  - `RAKKA_POSTGRES_TEST_DSN=postgres://postgres:postgres@localhost:5432/postgres cargo test -p rakka-sharding-postgres`
  - `RAKKA_RUN_MULTI_PROCESS_COMPATIBILITY=1 cargo test -p rakka-testkit --test compatibility_matrix optional_multi_process_compatibility_example_is_gated -- --nocapture`
  - `RAKKA_ETCD_TEST_ENDPOINTS=http://127.0.0.1:2379 cargo test -p rakka-discovery-etcd --test etcd_discovery -- --nocapture`
  - `RAKKA_K8S_RUN_LOCAL_CLUSTER=1 RAKKA_K8S_IMAGE=<image> examples/kubernetes/local-cluster-scenario.sh`
- Examples are runnable packages and double as behavioral documentation; expected output is in `README.md` and example READMEs.

## 3. Architecture & Code Map

- `crates/rakka`: facade crate, feature-gated module re-exports, curated `rakka::prelude`.
- `crates/rakka-core`: actor runtime, actor refs/context/system, supervision, paths, receptionist, routers, coordinated shutdown, metrics, errors.
- `crates/rakka-persistence`, `crates/rakka-persistence-postgres`: durable state, event/snapshot stores, behavior facades, in-memory and PostgreSQL persistence.
- `crates/rakka-cluster`, `crates/rakka-remote`, `crates/rakka-discovery-etcd`: membership, discovery, protocol compatibility, Protobuf/TCP remoting, known-peer transport, etcd external-arbiter discovery.
- `crates/rakka-sharding`, `crates/rakka-sharding-postgres`: entity identity, shard ownership/coordinator, local/remote routes, remembered entities, PostgreSQL coordinator store/lease.
- `crates/rakka-workflow`: durable inbox/outbox reliability substrate: deduplication, retries, recovery, clocks, workflow telemetry.
- `crates/rakka-agent-workflow`: additive agent workflow facade/runtime: runs, steps, effects, graph scheduler, compiled plans, timers, dispatchers, query, retention, audit, OpenTelemetry/Kubernetes helpers.
- `crates/rakka-stream`, `crates/rakka-process`: bounded streams and supervised child-process actors/process-backed entities.
- `crates/rakka-http`, `crates/rakka-grpc`, `crates/rakka-k8s`: edge adapters and Kubernetes operation surfaces.
- `crates/rakka-testkit`: cross-crate probes, assertions, compatibility fixtures, and repository hygiene helpers.
- `examples/*`: unpublished end-to-end examples. `examples/kubernetes/` contains reviewable StatefulSet/service/probe/drain/observability manifests and scripts.
- `docs/*.md` are the source of truth for current behavior, compatibility, reliability, security, packaging, API boundaries, and release review. `docs/plans/` contains historical and active plans; when a plan creates durable user-facing behavior, update the relevant product doc.

## 4. Coding Conventions

- Workspace lints are strict: `unsafe_code = "forbid"`, `missing_docs = "warn"`, `clippy::all = "warn"`; validation promotes clippy warnings to errors.
- Public APIs need documentation comments. Keep examples and docs compiling under the strict workspace settings.
- Return `RakkaResult<T>` / `RakkaError` at public boundaries where appropriate. Errors carry a stable `Subsystem`, stable kebab-case `code`, and message; compatibility-sensitive codes must not churn.
- Actor implementations use `Actor::handle<'a>(&'a mut self, ctx, msg) -> ActorFuture<'a>` with `actor_future(async move { ... Ok(ActorAction::Continue) })`. `actor_fn` is synchronous; use manual actors, `Behavior`, `setup`, `ctx.ask`, or `ctx.pipe_to_self` for async work.
- Prefer `rakka::prelude` and facade APIs in new application-facing examples. Use lower-level crate APIs only when the boundary is explicit or no facade exists.
- Integration tests live in `crates/<crate>/tests/*.rs`; unit tests are inline under `#[cfg(test)]`. Prefer `rakka-testkit` probes/assertions over sleeps.
- Keep optional integration layers feature-gated through `crates/rakka/Cargo.toml`; maintain no-default checks for `rakka-stream` and `rakka-process`.
- Streams and mailboxes are bounded by design. Preserve back-pressure, cancellation, drain, and message ownership on failure.
- Metrics labels must be bounded and operationally meaningful; avoid raw ids, actor paths, prompts, payloads, command args, temp paths, and full error text.

## 5. Strict Boundaries

- Core actors, remoting, and sharding are at-most-once by default. Stronger behavior belongs to durable state plus durable inbox/outbox, idempotency keys, retries, and recovery. See `docs/rakka-v1-reliability-boundaries.md`.
- Do not claim exactly-once external side effects. External systems must be idempotent or reconciled by application logic.
- Internal remoting is trusted cluster traffic, not a public client protocol. External clients should use HTTP/gRPC or explicit application protocols. v1 does not provide built-in TLS/mTLS or certificate lifecycle management.
- N/N+1 rolling updates require mutual cluster protocol compatibility, additive Protobuf schema changes, compatible manifest/API metadata, and fail-closed readiness for incompatible nodes. See `docs/rakka-compatibility.md` and `docs/rakka-v1-rolling-update-upgrade.md`.
- Kubernetes schedules pods and enforces network/security policy; Rakka manages actor placement, drain, readiness, and handoff inside that environment. Do not load-balance internal actor remoting through a normal public service.
- Process actors run child processes inside the Rakka node container in v1. They require absolute executables, explicit allowlists, no inherited environment by default, and explicit stdio/env/cwd choices. Rakka is not an OS sandbox.
- For `rakka-agent-workflow` and compiled graph execution: Rakka owns the product-neutral runtime IR, durable graph run state, deterministic scheduler, durable effect bridge, runtime events, recovery, passivation, drain, metrics, tracing, and snapshots. The application backend owns editor DSL/UI, compiler, deployments, triggers, auth/billing/tenant policy, credential storage, and product adapters.
- Never persist resolved credentials or secret material in compiled plans, graph state, outbox entries, runtime events, logs, metrics, snapshots, or query indexes. Store logical credential binding refs only; resolve credentials at dispatch time.
- Runtime events and observability projections are not the correctness source. Durable run state and durable inbox/outbox state are.
- Do not publish crates, packages, images, generated bundles, releases, or artifacts to any registry without explicit user approval for that exact action. Package checks are validation only and offline only.

## 6. Definition of Done Checklist

- Identify the affected API tier: facade, foundation, adapter, or test/support; preserve the documented ownership boundary.
- Implement the smallest scoped change that follows existing crate/module patterns and feature gates.
- Add or update focused tests for the behavior touched; use testkit helpers where available.
- Run the narrowest relevant test first, then run `scripts/validate.sh` before considering the work complete when feasible.
- Run `scripts/package-check.sh` for release, packaging, manifest, feature-boundary, or public API work.
- Run or explicitly defer gated PostgreSQL, etcd, multi-process, generated-contract, or Kubernetes checks when the touched code depends on them.
- Update `README.md`, relevant `docs/*.md`, and `CHANGELOG.md` when behavior, compatibility, reliability, security defaults, release packaging, or durable user-facing semantics change.
- Keep examples unpublished (`publish = false`) and keep generated contract code out of the repository.
- Confirm no secrets, unbounded labels, raw high-cardinality ids, or resolved credential values were added to state, logs, metrics, snapshots, indexes, or tests.
- Leave the repo with clear validation results and no accidental publishing, registry upload, or unrelated file churn.
