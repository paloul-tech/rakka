# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Rakka is a Rust actor framework (Akka-inspired) shipped as a Cargo workspace. It provides typed local actors, durable state / event sourcing, cluster membership, Protobuf remoting, sharding, supervised child-process actors, durable workflow inbox/outbox reliability, bounded streams, HTTP/gRPC edge adapters, and Kubernetes operation. The repository is a v1 release-candidate foundation plus active work on `rakka-agent-workflow` (a durable execution kernel for compiled agent workflows).

MSRV is Rust 1.85 (`rust-toolchain.toml` pins stable + clippy + rustfmt). gRPC crates/examples require `protoc` (`protobuf-compiler`) on the build host.

## Essential Commands

The canonical validation entry point — run this before considering work done:

```sh
scripts/validate.sh        # fmt --check, clippy -D warnings, workspace tests, no-default-feature checks, doc build, k8s dry-run
scripts/package-check.sh   # offline crate packaging / publishability check (validation only, never publishes)
```

Granular commands:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features

# Single crate / single test file / single test
cargo test -p rakka-agent-workflow
cargo test -p rakka-agent-workflow --test graph_scheduler
cargo test -p rakka-agent-workflow --test graph_scheduler -- some_test_name --nocapture

# Minimal-feature compile checks (these are part of validation; keep them green)
cargo check -p rakka-stream --no-default-features
cargo check -p rakka-process --no-default-features
```

Examples are runnable and self-contained (most need no external services); each documents its expected stdout in `README.md`:

```sh
cargo run -p rakka-example-minimal-system
cargo run -p rakka-example-multi-node-sharding -- --networked-loopback
cargo run -p rakka-example-multi-pod-agent-fault-soak      # multi-pod agent fault sweep, ~2 min
```

### Gated / optional tests

These are skipped by default and require env vars and/or external services:

```sh
RAKKA_POSTGRES_TEST_DSN=postgres://postgres:postgres@localhost:5432/postgres cargo test -p rakka-persistence-postgres
RAKKA_POSTGRES_TEST_DSN=postgres://postgres:postgres@localhost:5432/postgres cargo test -p rakka-agent-postgres
# The memory conformance suite's retriever clauses need the `vector` extension; without
# it they announce the clauses they skipped. Set this to make that a failure instead:
RAKKA_POSTGRES_TEST_DSN=... RAKKA_POSTGRES_PGVECTOR_REQUIRED=1 cargo test -p rakka-agent-postgres
RAKKA_POSTGRES_TEST_DSN=postgres://postgres:postgres@localhost:5432/postgres cargo test -p rakka-agent-knowledge-graph-postgres
RAKKA_RUN_MULTI_PROCESS_COMPATIBILITY=1 cargo test -p rakka-testkit --test compatibility_matrix -- --nocapture   # both multi-process gates
RAKKA_K8S_SCENARIO_DRY_RUN=1 examples/kubernetes/local-cluster-scenario.sh         # preview, no cluster touched
RAKKA_K8S_VALIDATE_MANIFESTS=1 cargo test -p rakka-k8s optional_kubectl_manifest_validation_is_gated -- --nocapture
```

## Architecture

### Crate layering (strict dependency DAG, low → high)

```
rakka-core                      foundation: Actor/ActorRef/ActorContext, ActorSystem, supervision,
                                paths, receptionist, routers, coordinated shutdown, metrics, errors
  ├─ rakka-cluster              membership, discovery, downing, clustered receptionist
  │    └─ rakka-remote          envelopes, serialization registry, transport traits, TCP
  │         └─ rakka-sharding   entity identity, shard ownership/coordinator, local/remote routes
  ├─ rakka-persistence          durable state + typed event/snapshot stores + behavior facades
  │    └─ rakka-workflow        durable inbox/outbox reliability primitives
  ├─ rakka-stream               bounded stream primitives
  ├─ rakka-process              supervised child-process actors / process-backed entities
  ├─ rakka-http / rakka-grpc    edge adapters (axum / tonic)
  ├─ rakka-k8s                  health, drain, DNS discovery, manifest helpers
  ├─ rakka-*-postgres           PostgreSQL adapters for persistence / sharding
  └─ rakka-agent-workflow       durable agent/compiled-workflow execution kernel (see below)
       └─ rakka-agent           durable agent domain: entities, loop, model adapter, effects,
                                budgets, checkpoints, memory (rakka-agent-postgres = PostgreSQL/
                                pgvector adapters; rakka-agent-knowledge-graph = communal claims,
                                trust transitions, promotion gate, portable graph SPI + conformance;
                                rakka-agent-knowledge-graph-postgres = the graph's relational backend)

rakka                          top-level facade crate + curated `rakka::prelude`; re-exports
                               component crates behind cargo features (default = all)
```

Application code should depend on `rakka` and import from `rakka::prelude`; component crates remain available for advanced wiring and tests. Each crate carries an **API boundary tier** documented in `docs/rakka-api-boundary-inventory.md`: Facade (preferred), Foundation (public building blocks), Adapter (edge integration), Test/support (`rakka-testkit`).

### Delivery & reliability model (the central design constraint)

Core actor, remote, and sharded message delivery is **at-most-once**. Any stronger guarantee (dedup, retry, exactly-once *intent*) must be built from durable state + durable inbox acceptance + durable outbox effects + idempotency keys + recovery — this is what `rakka-workflow` provides and `rakka-agent-workflow` builds on. Do not assume delivery reliability that the layer below does not promise. See `docs/rakka-v1-reliability-boundaries.md`.

### `rakka-agent-workflow`

This crate is the durable execution kernel for **compiled workflow execution plans with a deterministic graph scheduler** (active branch work; spec/plan in `docs/plans/compiled_execution_with_graph_schdlr/`). Key boundary, which must be preserved:

- **Rakka owns**: the product-neutral compiled execution IR, durable graph run state, the deterministic per-run graph scheduler, the bridge from runtime nodes to durable `AgentEffect` outbox work, normalized trigger metadata, runtime event records, actor-backed/sharded run execution, recovery/passivation/drain, bounded metrics, tracing context, snapshots.
- **Rakka does NOT own** (these live in a separate application backend): the visual editor/DSL/compiler, auth/billing/tenant policy, trigger registration & webhook routing, credential/secret storage.
- **Hard rule**: never persist resolved credentials/secret material in compiled plans, durable graph state, outbox entries, runtime events, logs, metrics, snapshots, or query indexes. Runtime events are observability, never the correctness source — durable run + inbox/outbox state is.

## Conventions

- **Workspace lints are strict.** `unsafe_code = "forbid"`, `missing_docs = "warn"`, `clippy::all = "warn"` — but validation runs `clippy ... -D warnings`, so every public item needs a doc comment and any warning fails the build. Lints are inherited via `[lints] workspace = true` in each crate manifest.
- **Errors**: return `RakkaResult<T>` / `RakkaError`. Every error carries a `Subsystem` (stable kebab-case id, e.g. `sharding`) plus a stable `code` string and message. Construct via the subsystem helpers (e.g. `RakkaError::core("ask-failed", msg)`). Codes are part of the compatibility surface — keep them stable.
- **Actor handler idiom**: `Actor::handle<'a>(&'a mut self, ctx, msg) -> ActorFuture<'a>` where `ActorFuture` is a boxed pinned `Send` future; build the body with `actor_future(async move { ... Ok(ActorAction::Continue) })`.
- **Tests**: integration tests live in `crates/<crate>/tests/*.rs` (one concern per file); unit tests are inline `#[cfg(test)]`. Examples double as end-to-end checks and must set `publish = false`.
- **Features**: optional crates are wired through the `rakka` facade's feature flags; when adding cross-crate optional functionality, propagate the feature (note the `rakka-agent-workflow?/...` passthroughs in `crates/rakka/Cargo.toml`).

## Publishing policy

Do **not** publish any crate, container image, release artifact, or generated bundle to any registry without explicit user approval for that exact action. `scripts/package-check.sh` and `cargo package` are validation-only; package checks always run with `--offline`.

## Docs

- `docs/*.md` — product documentation; the **source of truth** for current behavior, compatibility, reliability boundaries, security/operational defaults, and the API boundary inventory.
- `docs/plans/` — implementation plans (historical and active). Active focus: `docs/plans/compiled_execution_with_graph_schdlr/` and `docs/plans/agentic-workflow/`.
- When a plan introduces durable user-facing behavior, update the relevant `docs/` product doc, and record user-facing changes in `CHANGELOG.md`.
