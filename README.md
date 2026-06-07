# Rakka

Rakka is a Rust actor framework planned around typed actors, durable state, Rakka-owned cluster coordination, Kubernetes operation, Protobuf remoting, and supervised child-process actors.

The current repository state includes Phase 3 foundations: local typed actors, durable state APIs, in-memory and PostgreSQL durable state stores, cluster membership/discovery foundations, Protobuf remote envelopes, and deterministic cluster sharding.

See `docs/rakka-phase-3-remote-sharding.md` for the current remote entity routing flow and the boundary between production foundations and deterministic test scaffolding.

## Useful Commands

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo doc --workspace --all-features --no-deps
cargo run -p rakka-example-minimal-system
cargo run -p rakka-example-durable-counter
cargo run -p rakka-example-multi-node-sharding
```

The minimal example should print:

```text
Rakka Phase 1 actor replied with pong on tokio.
```

The durable counter example should print:

```text
Rakka durable counter recovered value 2.
```

The multi-node sharding example should print:

```text
Rakka multi-node sharding routed add-apple to cart-N on rakka-1#uid-b.
node-a local entity count: 0
node-b local entity count: 1
```

The PostgreSQL plugin has an optional round-trip test. It is skipped unless `RAKKA_POSTGRES_TEST_DSN` is set:

```sh
RAKKA_POSTGRES_TEST_DSN=postgres://user:password@localhost:5432/rakka cargo test -p rakka-persistence-postgres
```
