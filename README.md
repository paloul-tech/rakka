[![CI](https://github.com/paloul-tech/rakka/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/paloul-tech/rakka/actions/workflows/ci.yml)

![Rakka - Rust Actor Framework](media/rakka-banner.png)

# Rakka

**Typed, durable, clustered actors for Rust, inspired by Akka.**

Rakka helps you build stateful services that keep running when nodes restart, pods move,
or a network link drops. Write plain typed actors, then add persistence, sharding,
clustering, and Kubernetes operation as you need them. Everything is async on Tokio,
and the workspace forbids `unsafe` code.

> **Status:** Rakka is a v1 release-candidate foundation. The core APIs are in place and
> heavily tested, but they may still change before 1.0, and the crates are not on
> crates.io yet. [Known limitations and roadmap](docs/rakka-v1-known-limitations-roadmap.md).

## Highlights

- **Typed actors.** Actor refs with `tell` and `ask`, supervision, a receptionist, routers,
  and coordinated shutdown. Every message is type-checked at compile time.
- **Durable state and event sourcing.** Akka-style `EventSourcedBehavior` and durable-state
  facades, with snapshots. Stores are in-memory or PostgreSQL.
- **Clustering and sharding.** Membership, discovery (DNS, etcd), Protobuf remoting over
  TCP, and sharded entities that recover their state when their shard moves to another node.
- **Reliable workflows.** Durable inbox and outbox primitives with idempotency keys, retries,
  and recovery. Use them where at-most-once delivery isn't enough.
- **Streams and child processes.** Bounded, backpressured streams, plus supervised actors
  that own and restart external processes.
- **Ready for production.** HTTP (axum) and gRPC (tonic) adapters, Kubernetes health and
  drain hooks, Prometheus and OpenTelemetry metrics, and reviewable manifests.
- **Durable AI agents.** An optional agent layer: goal-driven agents, tasks, and runs whose
  model calls, tool calls, and messages are durable effects. It adds model providers, MCP
  tools, A2A, memory, and a knowledge graph. [Read more](docs/rakka-agents.md).

## A quick look

```rust
use std::time::Duration;
use rakka::prelude::*;

// An actor's protocol is a plain enum. Each request that expects an answer
// carries a typed `ReplyTo` channel, so the reply type is checked at compile time.
enum Echo {
    Ping { reply_to: ReplyTo<&'static str> },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The actor system hosts actors and owns their lifecycle.
    let system = ActorSystem::new("hello");

    // Spawn an actor named "echo" from a closure. The actor handles one message
    // at a time, and `spawn` returns a typed `ActorRef<Echo>` handle to it.
    let echo = system.spawn(
        "echo",
        actor_fn(|_ctx: &mut ActorContext<Echo>, msg: Echo| match msg {
            Echo::Ping { reply_to } => {
                // `reply` fails only if the caller stopped waiting,
                // so it's safe to ignore here.
                let _ = reply_to.reply("pong");
                // Keep the actor running for the next message.
                // Returning `ActorAction::Stop` would stop it instead.
                Ok(ActorAction::Continue)
            }
        }),
    )?;

    // `ask` sends a message and waits for its reply. It returns an error
    // if no reply arrives within the timeout.
    let reply = echo
        .ask(|reply_to| Echo::Ping { reply_to }, Duration::from_secs(1))
        .await?;
    println!("echo replied: {reply}");

    // Stop every actor through coordinated shutdown and wait for them to finish.
    system.terminate().await?;
    Ok(())
}
```

## Getting started

Rakka needs Rust 1.88 or newer. The gRPC crates also need `protoc` (`protobuf-compiler`).

Add the facade crate from Git:

```toml
[dependencies]
rakka = { git = "https://github.com/paloul-tech/rakka" }
tokio = { version = "1", features = ["full"] }
```

Import from `rakka::prelude`. The default features turn on the actor, persistence,
cluster, remoting, sharding, workflow, stream, process, HTTP, gRPC, and Kubernetes layers.
Turn off `default-features` to pick only the layers you need. The agent layer is opt-in
through the `agent` feature and its companion features (`agent-rig`, `agent-mcp`, `a2a`, and others).

## Examples

Every example runs from the workspace root with no external services unless noted:

| Example | What it shows |
| --- | --- |
| `cargo run -p rakka-example-minimal-system` | An actor system, a spawned actor, and a request/reply |
| `cargo run -p rakka-example-event-sourced-counter` | Event sourcing with snapshots and recovery |
| `cargo run -p rakka-example-sharded-cart-persistence` | A sharded entity that moves nodes and recovers its state |
| `cargo run -p rakka-example-multi-node-sharding -- --networked-loopback` | Two nodes routing to sharded entities over TCP |
| `cargo run -p rakka-example-durable-workflow` | Inbox dedup, outbox retry, and recovery |
| `cargo run -p rakka-example-streams` | Bounded streams with backpressure |
| `cargo run -p rakka-example-edge-gateway` | HTTP and gRPC in front of actors |
| `cargo run -p rakka-example-durable-agent-acceptance` | A durable agent run from start to finish |

The [examples index](examples/README.md) lists every example and how to run it, including the Kubernetes checks and a multi-pod fault soak.

## How delivery works

Like Akka, Rakka delivers actor, remote, and sharded messages **at most once**. When you
need stronger guarantees, build them explicitly from durable state, the durable inbox and
outbox, and idempotency keys. The `rakka-workflow` crate provides those building blocks.
[The reliability boundaries](docs/rakka-v1-reliability-boundaries.md) spell out what is
guaranteed and what is left to your application.

## Crates

Application code should depend on **`rakka`**. The component crates are available
for advanced wiring.

| Area | Crates |
| --- | --- |
| Core | `rakka-core`, `rakka` (facade and prelude) |
| Persistence | `rakka-persistence`, `rakka-persistence-postgres` |
| Cluster | `rakka-cluster`, `rakka-remote`, `rakka-sharding`, `rakka-sharding-postgres`, `rakka-discovery-etcd` |
| Reliability and I/O | `rakka-workflow`, `rakka-stream`, `rakka-process` |
| Edge and ops | `rakka-http`, `rakka-grpc`, `rakka-k8s` |
| Agents | `rakka-agent-workflow`, `rakka-agent`, `rakka-agent-postgres`, `rakka-agent-knowledge-graph`, `rakka-agent-knowledge-graph-postgres`, `rakka-agent-mcp`, `rakka-a2a` |
| Testing | `rakka-testkit` |

## Documentation

**Using Rakka**
- [Akka migration notes](docs/rakka-akka-parity-migration-notes.md): Rakka for Akka users
- [API boundary](docs/rakka-api-boundary-inventory.md): which crates and items are stable surface
- [Remoting and sharding](docs/rakka-phase-3-remote-sharding.md), [processes and workflows](docs/rakka-phase-4-process-workflow.md), [HTTP, gRPC, streams, and Kubernetes](docs/rakka-phase-5-integration-surfaces.md)

**Running Rakka in production**
- [Reliability boundaries](docs/rakka-v1-reliability-boundaries.md) and [security defaults](docs/rakka-v1-security-operational-defaults.md)
- [Compatibility and version skew](docs/rakka-compatibility.md) and [rolling updates](docs/rakka-v1-rolling-update-upgrade.md)
- [Observability exporters](docs/rakka-v1-observability-exporters.md)
- [Release-candidate review](docs/rakka-v1-release-candidate-review.md) and [known limitations](docs/rakka-v1-known-limitations-roadmap.md)

**Durable agents**
- [Rakka Agents overview](docs/rakka-agents.md) and the [agent specification and plan](docs/plans/rakka-agent/)
- Validation: [recovery scenarios](docs/rakka-agent-recovery-scenarios.md), [fault injection](docs/rakka-agent-fault-injection-matrix.md), [security](docs/rakka-agent-security-validation-matrix.md), [telemetry](docs/rakka-agent-telemetry-validation-matrix.md), [observability catalogue](docs/rakka-agent-observability-catalogue.md)
- [Agent workflow plans](docs/plans/agentic-workflow/): spec, Kubernetes topology, runbooks, and production-candidate support material

## Contributing

Contributions are welcome! [CONTRIBUTING.md](CONTRIBUTING.md) covers setup, the gated
tests, and the conventions the codebase follows. Before you open a pull request, run
the validation suite:

```sh
scripts/validate.sh
scripts/package-check.sh   # offline packaging check; never publishes
```

## License

Rakka is licensed under the [MIT License](LICENSE).
