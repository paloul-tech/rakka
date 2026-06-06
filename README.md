# Rakka

Rakka is a Rust actor framework planned around typed actors, durable state, Rakka-owned cluster coordination, Kubernetes operation, Protobuf remoting, and supervised child-process actors.

The current repository state is Phase 2: a first local actor kernel plus durable state APIs, an in-memory durable state store, and a PostgreSQL durable state store plugin.

## Useful Commands

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo doc --workspace --all-features --no-deps
cargo run -p rakka-example-minimal-system
cargo run -p rakka-example-durable-counter
```

The minimal example should print:

```text
Rakka Phase 1 actor replied with pong on tokio.
```

The durable counter example should print:

```text
Rakka durable counter recovered value 2.
```

The PostgreSQL plugin has an optional round-trip test. It is skipped unless `RAKKA_POSTGRES_TEST_DSN` is set:

```sh
RAKKA_POSTGRES_TEST_DSN=postgres://user:password@localhost:5432/rakka cargo test -p rakka-persistence-postgres
```
