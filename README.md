# Rakka

Rakka is a Rust actor framework planned around typed actors, durable state, Rakka-owned cluster coordination, Kubernetes operation, Protobuf remoting, and supervised child-process actors.

The current repository state is Phase 1: a first local actor kernel with typed actors, bounded mailboxes, `tell`, `ask`, timers, child spawning, watching, dead letters, and supervision basics.

## Useful Commands

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo doc --workspace --all-features --no-deps
cargo run -p rakka-example-minimal-system
```

The minimal example should print:

```text
Rakka Phase 1 actor replied with pong on tokio.
```
