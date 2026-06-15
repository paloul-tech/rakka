# Rakka Akka Parity Phase 6: Streams Facade And Testkit

Status: implemented through Slice 6I stream testkit probes
Date: 2026-06-14

Phase 6 introduces Akka-shaped stream names over Rakka's existing bounded stream
runtime. The goal is a familiar first path for users who know Akka Streams,
while keeping Rakka's Rust-native lifecycle and back-pressure model visible.

## Parity Boundary

Rakka is targeting an Akka-shaped bounded-stream subset in Phase 6, not full
Akka Streams graph parity.

Included in the Phase 6 target:

- `Source<T>`, `Flow<I, O>`, and `Sink<T, M>` facade vocabulary.
- Explicit bounded `StreamRunSettings`.
- Core operators such as `map`, `filter`, `map_async`, `take`, `merge`,
  `broadcast`, `fold`, `run_collect`, and `run_foreach`.
- Actor and entity boundaries with explicit back-pressure and acknowledgement
  protocols.
- Stream testkit probes for item, demand, completion, cancellation, and failure
  assertions.

Not included in the Phase 6 target:

- Full Akka Streams graph DSL.
- Custom GraphStage APIs.
- Reactive Streams publisher/subscriber compatibility.
- Distributed stream materialization.
- Transparent cluster deployment of arbitrary stream stages.

## Current Baseline

The low-level bounded runtime remains available:

```rust
let (sink, source) = rakka_stream::bounded_channel::<u64>(16)?;
sink.send(1).await?;
sink.drain()?;
assert_eq!(source.next().await?, Some(1));
assert_eq!(source.next().await?, None);
```

That runtime owns the precise lifecycle semantics:

- bounded buffer capacity;
- pressure through `try_send` and `send`;
- graceful drain;
- immediate close;
- cancellation with a reason;
- typed stream errors.

## Facade Vocabulary

Slice 6A added the public facade names:

```rust
use rakka_stream::{Flow, Sink, Source, StreamRunSettings};

let settings = StreamRunSettings::default()
    .with_stream_name("orders")
    .with_cancellation_reason("orders stream cancelled");

let source = Source::<u64>::empty().with_settings(settings.clone());
let flow = Flow::<u64, u64>::identity();
let sink = Sink::<u64, ()>::ignore();
let runnable = source.to(sink);

assert!(flow.is_identity());
assert_eq!(runnable.source_settings().stream_name(), Some("orders"));
```

## Materialization Basics

Slice 6B makes finite and low-level bounded sources runnable through terminal
sinks:

```rust
let collected = Source::from_iter([1, 2, 3])
    .run_collect()
    .await?;
assert_eq!(collected, vec![1, 2, 3]);

let sum = Source::from_iter([1, 2, 3])
    .run_with(Sink::fold(0, |sum, item| sum + item))
    .await?;
assert_eq!(sum, 6);
```

The facade can also bridge existing low-level bounded primitives:

```rust
let (sink, source) = rakka_stream::bounded_channel(8)?;
sink.send("work").await?;
sink.drain()?;

let items = Source::from_stream_source(source).run_collect().await?;
assert_eq!(items, vec!["work"]);
```

The consuming conversion helpers keep adapter migrations terse:

```rust
let items = source.into_source().run_collect().await?;
let written = Source::from_iter(items).run_with(sink.into_sink()).await?;
```

## Linear Operators

Slice 6C adds first-pass linear operators:

```rust
let values = Source::from_iter([1, 2, 3, 4])
    .map(|item| item * 2)
    .filter(|item| *item > 4)
    .take(2)
    .run_collect()
    .await?;

assert_eq!(values, vec![6, 8]);
```

`Flow::identity()` and `Flow::from_fn(...)` can be applied with `via`:

```rust
let values = Source::from_iter([1, 2, 3])
    .via(Flow::from_fn(|item| format!("item-{item}")))
    .run_collect()
    .await?;
```

`take(n)` completes as soon as `n` elements are emitted. When it stops early it
cancels its upstream source boundary, which wakes blocked bounded producers.

## Async Operators

Slice 6D adds ordered `map_async`, matching Akka's default `mapAsync`
behavior:

```rust
let values = Source::from_iter([1, 2, 3])
    .map_async(2, |item| async move {
        enrich(item).await
    })?
    .run_collect()
    .await?;
```

At most `parallelism` mapper futures are in flight at once, and outputs are
emitted in source order even when later futures complete first. A parallelism
of zero is rejected with `StreamError::Operator`.

The same behavior is available as a flow:

```rust
let flow = Flow::from_async_fn(4, |item| async move {
    format!("item-{item}")
})?;

let values = Source::from_iter([1, 2, 3])
    .via(flow)
    .run_collect()
    .await?;
```

When downstream cancels early, for example through `take(n)`, in-flight async
mapper tasks are aborted where the Tokio runtime permits it. Mapper task
failures surface as source-side `StreamRunError::Source` values containing
`StreamError::Operator`.

`map_async_unordered` is intentionally left as a follow-up so the first-path
API remains ordered and easy to reason about.

## Fan-in And Fan-out

Slice 6E adds bounded `merge` and `broadcast` facade operators.

`merge` combines two sources, preserving order within each input while allowing
interleaving between inputs:

```rust
let values = Source::from_iter([1, 2])
    .merge(Source::from_iter([3, 4]))
    .run_collect()
    .await?;
```

For more inputs, use `merge_all` or `merge_all_with_settings`:

```rust
let values = Source::merge_all([
    Source::from_iter(["a"]),
    Source::from_iter(["b", "c"]),
])
.run_collect()
.await?;
```

`broadcast` returns one bounded branch source per requested branch:

```rust
let mut branches = Source::from_iter([1, 2, 3])
    .broadcast(2)?;

let right = branches.pop().expect("right branch");
let left = branches.pop().expect("left branch");
let (left, right) = tokio::join!(left.run_collect(), right.run_collect());
```

Every item is forwarded to every live branch. A full live branch backpressures
the upstream broadcast, matching the safer Akka-style default. If a branch is
cancelled or dropped, it is removed from the broadcast so the remaining live
branches can continue.

## Actor Boundaries

Slice 6F adds actor source and sink facade boundaries.

For fire-and-fail-fast delivery, `Sink::actor_ref` forwards stream items
directly into an actor mailbox:

```rust
let delivered = Source::from_iter(commands)
    .run_with(Sink::actor_ref(worker))
    .await?;
```

For explicit back-pressure, use `Sink::actor_ref_with_ack`. The target actor
receives `ActorSinkMessage<T, Ack>` and must reply with the configured ack value
before the stream pulls the next item:

```rust
let sink = Sink::actor_ref_with_ack(
    worker,
    AckProtocol::new("ack").with_timeout(Duration::from_secs(1)),
);

let delivered = Source::from_iter(commands)
    .run_with(sink)
    .await?;
```

Failures before delivery, such as a full or closed actor mailbox, surface as
`StreamRunError::Actor` with `ActorStreamError<T>` so the undelivered item can
be inspected or recovered.

Actor-backed sources expose a typed actor ref plus a bounded source:

```rust
let (actor_ref, source) = Source::actor_ref(&system, "commands", 32)?;
actor_ref.tell(Command::Start)?;
let first = source.take(1).run_collect().await?;
```

The acked source variant accepts `ActorSourceMessage<T, Ack>` and replies only
after the bounded source accepts the element:

```rust
let (actor_ref, source) = Source::actor_ref_with_ack(
    &system,
    "acked-commands",
    32,
    AckProtocol::new("ack"),
)?;
```

## Entity Boundaries

Slice 6G adds sharded entity sink integration behind the existing
`rakka-stream` `adapters` feature.

Use `Sink::entity_ref` when the caller already has a `ShardRegion<M>` and
logical `EntityRef<M>`:

```rust
let delivered = Source::from_iter(commands)
    .run_with(Sink::entity_ref(region, entity_ref))
    .await?;
```

Use `Sink::sharded_entity_ref` with the higher-level sharding facade reference:

```rust
let delivered = Source::from_iter(commands)
    .run_with(Sink::sharded_entity_ref(cart))
    .await?;
```

No-route and delivery failures surface as `StreamRunError::Entity` and reuse
`EntitySinkError<M>`, preserving the undelivered stream item. Region semantics
remain owned by sharding: owner refresh, passivation buffering, shard handoff
buffering, and routing failure behavior are whatever the provided `ShardRegion`
would normally do for `tell`.

Entity sources are intentionally not implicit. When an entity needs to emit a
stream, use the actor-backed source boundary from Slice 6F or send to an
explicit `Source::actor_ref`/`Source::actor_ref_with_ack` boundary. This keeps
streams from pretending arbitrary entities are queryable sources.

## Process IO Boundaries

Slice 6H migrates process pipe adapters onto the facade without removing the
low-level pipe-control APIs.

Use the facade constructors when a managed process pipe is simply part of a
stream pipeline:

```rust
let (stdout, stdout_pump) = Source::process_stdout(
    &mut process,
    ProcessOutputConfig::default(),
)?;

let chunks = stdout.run_collect().await?;
let bytes_read = stdout_pump
    .expect("stdout pump")
    .await
    .expect("pump task")?;
```

For stdin, the facade owns the writer boundary and closes it when the stream
finishes or the sink is dropped:

```rust
let written = Source::from_iter(chunks)
    .run_with(Sink::process_stdin(&mut process)?)
    .await?;
```

Adapters that already own low-level process handles can migrate with consuming
conversions:

```rust
let (stdout, stdout_pump) = process_output.into_source();
let stdin = process_input.into_sink();
```

`ProcessOutputStream::into_source()` returns the pump handle alongside the
facade `Source<Vec<u8>>` so callers can still observe read completion or read
errors. `ProcessInputSink::into_sink()` is consuming rather than borrowed
because facade sinks are owned, `'static` stream boundaries.

## Stream Testkit Probes

Slice 6I adds reusable probes in `rakka-testkit` for source, sink, lifecycle,
error, cancellation, and ack-demand assertions.

Use a source probe to drive a facade source manually:

```rust
let (source, source_probe) = StreamTestKit::source_probe::<String>()?;
let run = tokio::spawn(async move { source.run_collect().await });

source_probe.send_next("one".to_owned()).await?;
source_probe.send_complete()?;

assert_eq!(run.await??, vec!["one".to_owned()]);
```

Use a sink probe to assert elements and terminal signals without sleeps:

```rust
let (sink, mut sink_probe) = StreamTestKit::sink_probe::<String>()?;
let run = tokio::spawn(async move {
    Source::from_iter(["one".to_owned()]).run_with(sink).await
});

sink_probe.request(1)?;
assert_eq!(sink_probe.expect_next().await?, "one");
sink_probe.expect_complete().await?;
assert_eq!(run.await??, 1);
```

Use a demand probe with `Sink::actor_ref_with_ack` when the test needs to prove
the stream does not over-pull while an element ack is withheld:

```rust
let (actor, mut probe) = StreamTestKit::demand_probe(&system, "demand", "ack")?;
let run = tokio::spawn(async move {
    Source::from_iter([1_u64, 2])
        .run_with(Sink::actor_ref_with_ack(
            actor,
            AckProtocol::new("ack"),
        ))
        .await
});

probe.expect_init().await?;
assert_eq!(probe.expect_next().await?, 1);
probe.expect_no_message(Duration::from_millis(50)).await?;
probe.request(1)?;
```

Every async probe assertion has a deterministic timeout. Defaults come from
`StreamTestKit`, and explicit `*_within` variants are available where the test
needs a custom timeout.

## Coordinated Shutdown Drain

Phase 7 connects bounded stream lifecycles to coordinated shutdown. Register
owned stream handles during application wiring so shutdown can reject new
items, let existing consumers flush buffered items, and report the drain task
in the `drain-http-grpc-and-streams` phase:

```rust
use rakka_core::{CoordinatedShutdown, CoordinatedShutdownReason, ShutdownOutcome};
use rakka_stream::{bounded_channel, register_stream_sink_drain};

let shutdown = CoordinatedShutdown::new();
let (sink, _source) = bounded_channel::<String>(16)?;

register_stream_sink_drain(&shutdown, "drain-orders-stream", sink.clone())?;

let report = shutdown
    .run(CoordinatedShutdownReason::user_request())
    .await?;

assert_eq!(report.outcome(), ShutdownOutcome::Complete);
```

Closed and already-cancelled stream drains are treated as completed shutdown
work, which keeps repeated application shutdown idempotent. Drains still report
real lifecycle failures, such as a stream that is already draining when the
task tries to drain it again.

## Runnable Example

`examples/streams` is the self-contained Phase 6 adoption example. It covers
finite source operators, `Sink::actor_ref_with_ack`, `Source::process_stdout`,
and `StreamTestKit` source probes:

```bash
cargo run -p rakka-example-streams
```

Expected output:

```text
Finite stream operators produced [6, 8].
Acked actor sink delivered ["init", "apple", "banana", "complete"].
Process stdout facade source read "child-stream-output".
Stream testkit probe collected ["probe-one", "probe-two"].
```

## Design Rules

- All facade materialization must remain bounded by `StreamRunSettings`.
- `StreamSink<T>` and `StreamSource<T>` stay public for low-level integrations.
- Stream completion and cancellation must propagate through every operator.
- Actor and entity integrations must preserve message ownership on failure
  wherever the public API can return the message.
- Testkit probes should replace sleeps in stream tests once demand and
  materialization APIs exist.
