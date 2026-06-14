# Rakka Akka Parity Phase 6: Streams Facade And Testkit

Status: implemented through Slice 6F actor source and sink boundaries
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

Entity boundaries and test probes arrive in later slices.

## Design Rules

- All facade materialization must remain bounded by `StreamRunSettings`.
- `StreamSink<T>` and `StreamSource<T>` stay public for low-level integrations.
- Stream completion and cancellation must propagate through every operator.
- Actor and entity integrations must preserve message ownership on failure
  wherever the public API can return the message.
- Testkit probes should replace sleeps in stream tests once demand and
  materialization APIs exist.
