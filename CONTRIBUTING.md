# Contributing to Rakka

Thanks for your interest in Rakka! Bug reports, fixes, documentation, examples,
and new features are all welcome. This guide covers how to set up, what to run
before you open a pull request, and the conventions the codebase follows.

## Before you start

- **Found a bug?** Open an issue with the smallest reproduction you can. A failing
  test or a short example is best.
- **Planning a larger change?** Open an issue to discuss it first, especially if
  it touches a public API, a durable record format, or a reliability guarantee.
  Rakka makes explicit promises about what survives a crash, and design changes
  go more smoothly when they're agreed on before the code is written.
- **Small fixes** (typos, docs, clear bugs) can go straight to a pull request.

## Setup

You need:

- **Rust 1.88 or newer.** `rust-toolchain.toml` pins stable with `clippy` and
  `rustfmt`, so `rustup` picks it up automatically.
- **`protoc`** (`protobuf-compiler`) for the gRPC crates and examples.
- **Optional:** Docker, to run the PostgreSQL-backed tests locally (see
  [Gated tests](#gated-tests)).

Then build and test the workspace:

```sh
cargo build --workspace
cargo test --workspace --all-features
```

## Working on a change

You rarely need the whole workspace while iterating. Run just the crate, test
file, or test you're working on:

```sh
cargo test -p rakka-core
cargo test -p rakka-agent-workflow --test graph_scheduler
cargo test -p rakka-agent-workflow --test graph_scheduler -- some_test_name --nocapture
```

The examples double as end-to-end checks. The [examples index](examples/README.md)
lists each one with the output it should print.

## Validate before you open a pull request

Run the full validation suite from the repository root:

```sh
scripts/validate.sh
scripts/package-check.sh
```

`scripts/validate.sh` is what CI's required job runs. It checks:

- formatting (`cargo fmt --all -- --check`)
- clippy across every target and feature, with warnings as errors
- the full workspace test suite
- that the crates with optional features still compile with no default features
- the documentation build (`cargo doc --workspace --all-features --no-deps`)
- a Kubernetes scenario dry-run that touches no cluster

`scripts/package-check.sh` checks that the publishable crates package correctly.
It runs offline and never publishes anything.

CI also checks the workspace on the minimum supported Rust version, runs the
PostgreSQL integration suites against a `pgvector/pgvector:pg16` service on every
pull request, and dry-runs the Kubernetes scripts.

## Gated tests

Some tests need an external service or infrastructure they may change, so they
skip unless you set an environment variable. You only need these when your
change touches the area they cover.

**PostgreSQL.** Start a local database with the `vector` extension, the same image
CI uses:

```sh
docker run -d --name rakka-postgres -p 5432:5432 \
  -e POSTGRES_PASSWORD=postgres pgvector/pgvector:pg16
```

Then run the suites for the adapters you changed:

```sh
export RAKKA_POSTGRES_TEST_DSN=postgres://postgres:postgres@localhost:5432/postgres
cargo test -p rakka-persistence-postgres
cargo test -p rakka-sharding-postgres
cargo test -p rakka-agent-postgres
cargo test -p rakka-agent-knowledge-graph-postgres
```

Without the `vector` extension, the `rakka-agent-postgres` retrieval tests report
which checks they skipped. Set `RAKKA_POSTGRES_PGVECTOR_REQUIRED=1` to make that a
failure instead.

**Multi-process compatibility.** Launches real node processes on loopback,
including the multi-pod agent fault sweep:

```sh
RAKKA_RUN_MULTI_PROCESS_COMPATIBILITY=1 \
  cargo test -p rakka-testkit --test compatibility_matrix -- --nocapture
```

**etcd discovery.** Needs a reachable etcd:

```sh
RAKKA_ETCD_TEST_ENDPOINTS=http://127.0.0.1:2379 \
  cargo test -p rakka-discovery-etcd --test etcd_discovery -- --nocapture
```

**Kubernetes and the OpenTelemetry Collector.** The manifest validation,
Collector configuration, and local-cluster commands are listed under
[Kubernetes checks](examples/README.md#kubernetes-checks) in the examples index.

**OTLP export to a live Collector.** By default the export test uses an
in-process OTLP receiver. To export to a real Collector instead:

```sh
RAKKA_AGENT_OTEL_COLLECTOR_ENDPOINT=http://127.0.0.1:4317 \
  cargo test -p rakka-example-agent-otlp-export-acceptance --test exporter_failure -- --nocapture
```

**Live model providers.** The durable agent walk can run against a real model
provider instead of the deterministic adapter. The
[durable agent acceptance README](examples/durable-agent-acceptance/README.md)
lists the supported providers and variables.

## Conventions

**Lints are strict.** The workspace forbids `unsafe` code and warns on missing
docs and clippy findings, and validation treats every warning as an error. In
practice, every public item needs a doc comment.

**Errors.** Return `RakkaResult<T>` and build errors with the subsystem helpers,
for example `RakkaError::core("ask-failed", message)`. Every error carries a
subsystem and a stable kebab-case code. Codes are part of the compatibility
surface: don't rename or reuse one, and record new ones as described in
[the API review](docs/rakka-v1-api-review.md#error-codes) and
[the compatibility notes](docs/rakka-compatibility.md).

**Delivery guarantees.** Actor, remote, and sharded delivery is at-most-once. If
your change needs a stronger guarantee, build it from durable state, the durable
inbox and outbox, and idempotency keys, and don't assume more than the layer
below promises. [The reliability boundaries](docs/rakka-v1-reliability-boundaries.md)
spell out what is and isn't guaranteed.

**No secrets in durable state.** Never write resolved credentials or secret
material into durable records, outbox entries, runtime events, logs, metrics,
snapshots, or query indexes. Durable state refers to credentials only by
logical binding.

**Tests.** Put integration tests in `crates/<crate>/tests/`, one concern per
file, and unit tests inline under `#[cfg(test)]`. A bug fix should come with a
test that fails without it.

**Examples.** Examples are workspace packages with `publish = false`. A new
example needs a row in the [examples index](examples/README.md), which the
repository hygiene test enforces, and a record of what it prints: in the index,
or in its own README.

**Features.** Optional functionality is wired through the `rakka` facade's
feature flags. When you add a cross-crate optional feature, pass it through the
facade in `crates/rakka/Cargo.toml`.

**Docs and changelog.** The files in `docs/` are the source of truth for how
Rakka behaves. If your change alters behavior users rely on, update the relevant
document, and record the change in [CHANGELOG.md](CHANGELOG.md) under
Unreleased. Implementation plans live in `docs/plans/`.

## Pull requests

- Keep each pull request focused on one change, and split unrelated work into
  separate pull requests.
- Write commit messages that explain why the change is needed, not just what
  changed.
- In the description, say what you changed, why, and how you tested it,
  including any gated tests you ran.
- Make sure `scripts/validate.sh` passes. CI runs it on every pull request.

## Releases and publishing

Maintainers cut releases. Contributors never publish crates, container images,
or release artifacts, and `scripts/package-check.sh` and `cargo package` are
validation only. [Release packaging](docs/rakka-v1-release-packaging.md)
describes the process.

## License

Rakka is licensed under the [MIT License](LICENSE). By contributing, you agree
that your contributions are licensed under the same terms.
