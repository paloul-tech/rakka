# Rakka Akka Parity Phase 6: Streams Facade And Testkit

Status: implemented through Slice 6C linear operators
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

Async operators, actor/entity boundaries, fan-in/fan-out, and test probes
arrive in later slices.

## Design Rules

- All facade materialization must remain bounded by `StreamRunSettings`.
- `StreamSink<T>` and `StreamSource<T>` stay public for low-level integrations.
- Stream completion and cancellation must propagate through every operator.
- Actor and entity integrations must preserve message ownership on failure
  wherever the public API can return the message.
- Testkit probes should replace sleeps in stream tests once demand and
  materialization APIs exist.
