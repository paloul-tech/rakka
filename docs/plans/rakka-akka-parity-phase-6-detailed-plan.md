# Rakka Akka Parity Phase 6 Detailed Plan

Status: implemented through Slice 6E
Date: 2026-06-14

## Purpose

This plan expands Phase 6 from
`docs/plans/rakka-akka-parity-implementation-plan.md` into implementation
slices for an Akka-shaped streams facade and stream testkit.

The current `rakka-stream` crate already provides bounded stream primitives:

- `BoundedStream<T>`, `StreamSink<T>`, and `StreamSource<T>`.
- Explicit bounded-buffer back-pressure with `send` and `try_send`.
- Lifecycle states for open, draining, completed, closed, and cancelled.
- Actor, entity, stream-to-stream, broadcast, fan-in, and process IO adapters.
- Basic testkit helpers in `rakka-testkit` for collection, lifecycle, and
  buffered-depth assertions.

Phase 6 should keep that foundation, but add the API shape users expect from
Akka Streams: `Source<T>`, `Flow<I, O>`, `Sink<T>`, operator chains, explicit
materialization, actor/entity integrations, and test probes.

## Evaluation

The Phase 6 description is intentionally compact and asks for one architectural
decision: whether Rakka claims full Akka Streams parity or a smaller
bounded-stream story.

Recommendation: claim an Akka-shaped bounded-stream subset in Phase 6, not full
Akka Streams parity.

That means:

- Provide familiar names and workflows: `Source`, `Flow`, `Sink`, `via`, `to`,
  `run_collect`, `run_foreach`, and probe-based tests.
- Preserve Rakka's explicit bounded-stream lifecycle and typed error model.
- Avoid a full graph interpreter, custom GraphStage API, materialized-value
  algebra, or reactive-streams publisher/subscriber compatibility in this
  phase.
- Keep room for a future Phase 6 follow-up to add graph DSL or interop if the
  facade proves stable.

The main design pressure is Rust ergonomics. Akka Streams can hide a lot behind
Scala's type system and runtime graph interpreter. Rakka should start with
owned, `Send + 'static` closures and explicit run settings so common stream
pipelines stay pleasant:

```rust
let values = Source::from_iter([1, 2, 3, 4])
    .map(|n| n * 2)
    .filter(|n| *n > 4)
    .take(2)
    .run_collect()
    .await?;
```

## Target Outcome

Users should be able to write compact, recognizable stream code over Rakka's
bounded runtime:

```rust
let source = Source::from_iter(work_items)
    .map(|item| item.normalize())
    .map_async(4, |item| async move { enrich(item).await })
    .filter(|item| item.is_ready());

let count = source
    .run_with(Sink::fold(0usize, |count, _item| count + 1))
    .await?;
```

Actor and entity integrations should be explicit about back-pressure:

```rust
Source::from_iter(commands)
    .run_with(Sink::actor_ref_with_ack(worker, AckProtocol::default()))
    .await?;

Source::from_iter(entity_commands)
    .run_with(Sink::entity_ref(region, entity_ref))
    .await?;
```

Tests should use probes instead of sleeps:

```rust
let (source, mut probe) = StreamTestKit::source_probe::<Work>();
let run = source.via(flow).run_with(Sink::collect());

probe.send_next(Work::new("a")).await?;
probe.send_complete()?;

assert_eq!(run.await?, vec![expected]);
```

## Non-goals

- Full Akka Streams graph DSL.
- Custom GraphStage or stage interpreter APIs.
- Reactive Streams publisher/subscriber compatibility.
- Distributed stream materialization.
- Transparent stream deployment across cluster nodes.
- Persistent exactly-once stream processing.
- Backward-incompatible removal of `StreamSink<T>` and `StreamSource<T>`.

## Guiding Decisions

- Keep `StreamSink<T>` and `StreamSource<T>` as the low-level bounded runtime.
- Add `Source<T>`, `Flow<I, O>`, and `Sink<T, M>` as facade types over bounded
  primitives and async tasks.
- Use explicit bounded capacities through `StreamRunSettings`; avoid unbounded
  defaults.
- Preserve message ownership on failures where possible.
- Make cancellation and completion propagate through every operator.
- Keep actor and entity back-pressure protocols explicit.
- Put stream test probes in `rakka-testkit`, but keep runtime probe plumbing in
  `rakka-stream` only when needed by public APIs.
- Move HTTP, gRPC, and process adapters onto the facade only when it reduces
  duplicate lifecycle semantics.

## Slice 6A: Parity Boundary And Facade Vocabulary

Goal: define the Phase 6 stream parity boundary and introduce stable facade
names without changing runtime behavior.

Status: implemented.

Scope:

- Add a stream facade module in `rakka-stream`.
- Add public types:
  - `Source<T>`;
  - `Flow<I, O>`;
  - `Sink<T, M>` or an equivalent materialized-result shape;
  - `RunnableStream<M>` if needed to represent `source.to(sink)`.
- Add `StreamRunSettings` with:
  - default bounded capacity;
  - operator output capacity;
  - cancellation reason labels;
  - optional task naming or stream name for metrics.
- Re-export stable facade types through `rakka::stream` and the curated prelude
  when the API shape is settled.
- Keep low-level `BoundedStream`, `StreamSink`, and `StreamSource` available.
- Document the bounded-subset decision in a new Phase 6 stream guide.

Acceptance criteria:

- Existing `rakka-stream` tests pass unchanged.
- Facade types compile and have complete rustdoc.
- Users can construct empty facade values without running a stream.
- The guide clearly states that Phase 6 is Akka-shaped bounded-stream parity,
  not full Akka Streams graph parity.

Implementation status:

- Added the `rakka-stream` facade vocabulary module with `Source<T>`,
  `Flow<I, O>`, `Sink<T, M>`, `RunnableStream<M>`, and
  `StreamRunSettings`.
- Added bounded-capacity validation and diagnostic stream naming/cancellation
  labels through `StreamRunSettings`.
- Re-exported the facade vocabulary from `rakka-stream`, `rakka::stream`, and
  the curated `rakka::prelude`.
- Added a Phase 6 streams guide documenting the bounded-subset parity boundary.
- Added stream facade tests proving the vocabulary can be constructed without
  materializing runtime behavior.

Review commands:

```bash
cargo fmt --all -- --check
cargo test -p rakka-stream
cargo doc -p rakka-stream --no-deps
```

## Slice 6B: Source, Sink, And Materialization Basics

Goal: make simple streams runnable through the facade.

Status: implemented.

Scope:

- Add source constructors:
  - `Source::empty()`;
  - `Source::single(item)`;
  - `Source::from_iter(items)`;
  - `Source::from_stream_source(source)`;
  - `Source::queue(capacity)` or `Source::bounded(capacity)` if useful.
- Add sink constructors:
  - `Sink::ignore()`;
  - `Sink::collect()`;
  - `Sink::foreach(fn)`;
  - `Sink::fold(initial, fn)`;
  - `Sink::from_stream_sink(sink)`.
- Add run methods:
  - `source.run_collect()`;
  - `source.run_foreach(fn)`;
  - `source.run_with(sink)`;
  - `source.to(sink).run()`, if `RunnableStream` is introduced.
- Convert terminal `StreamError` into facade-level stream run errors without
  losing the original lifecycle reason.
- Add examples in tests for the intended first-path API.

Acceptance criteria:

- A finite source collects in source order.
- `Sink::foreach` observes every element once.
- `Sink::fold` returns the expected accumulator.
- `Source::from_stream_source` preserves closed and cancelled errors.
- Default capacities are bounded and observable.

Implementation status:

- Added source constructors for empty, single-item, iterator-backed, low-level
  `StreamSource<T>`-backed, and bounded queue sources.
- Added sink constructors for ignore, collect, foreach, fold, and low-level
  `StreamSink<T>` forwarding.
- Added `Source::run_with`, `Source::run_collect`, `Source::run_foreach`, and
  `RunnableStream::run`.
- Added `StreamRunError<T>` and `StreamRunResult<T, M>` so source lifecycle
  errors and sink send errors remain typed during materialization.
- Added facade tests covering finite source collection, foreach, fold,
  `source.to(sink).run()`, low-level source/sink wrapping, and source/sink
  lifecycle errors.
- Updated the Phase 6 streams guide with runnable materialization examples.

Review commands:

```bash
cargo test -p rakka-stream --test stream_core
cargo test -p rakka-stream stream_facade
```

## Slice 6C: Linear Operators

Goal: add the core single-input single-output operators from the Phase 6
deliverables.

Status: implemented.

Scope:

- Add `Source` and `Flow` operators:
  - `map`;
  - `filter`;
  - `take`;
  - `fold` where it reads naturally as a terminal sink or source operator;
  - `via(flow)`;
  - `Flow::from_fn`;
  - `Flow::identity`.
- Decide whether operator closure errors are modeled as:
  - panic-free infallible closures first; or
  - `try_map` and `try_filter` variants returning typed errors.
- Preserve element ordering.
- Ensure `take(n)` cancels upstream after enough items and drains downstream
  consistently.
- Add metrics/tracing labels for operator lifecycle if existing stream status
  metrics are insufficient.

Acceptance criteria:

- Operators compose fluently on `Source`.
- `via(Flow::identity())` is behaviorally neutral.
- `map` and `filter` preserve order.
- `take` completes after the requested count and does not wait for upstream
  completion.
- Cancellation from downstream wakes blocked upstream producers.

Implementation status:

- Refactored `Source<T>` onto an internal source-stage trait so operators can
  change item types without changing the public facade shape.
- Added `Source::map`, `Source::filter`, `Source::take`, and `Source::via`.
- Added `Flow::from_fn` alongside `Flow::identity`.
- Kept operator closures infallible in this slice; fallible variants remain a
  later extension if needed.
- Made `take(n)` cancel upstream when the requested count is reached so bounded
  queue producers are woken instead of waiting behind a completed consumer.
- Added tests for operator composition, order preservation, identity flow,
  mapped flow, and `take(0)` upstream cancellation.
- Updated the Phase 6 streams guide with linear operator examples.

Review commands:

```bash
cargo test -p rakka-stream stream_facade_linear
cargo clippy -p rakka-stream --all-targets -- -D warnings
```

## Slice 6D: Async Operators And Back-pressure Semantics

Goal: add `map_async` while proving bounded back-pressure, cancellation, and
error propagation.

Status: implemented.

Scope:

- Add `map_async(parallelism, fn)`.
- Preserve Akka-like ordered output for `map_async` by default.
- Reject zero parallelism with a typed error.
- Decide whether to add `map_async_unordered` now or leave it as a follow-up.
- Ensure every async operator respects `StreamRunSettings` capacities.
- Add cancellation propagation from:
  - downstream sink cancellation;
  - upstream source close/cancel;
  - operator task failure;
  - timeout or dropped materialized task handle.
- Add stable error variants for operator failure and task join failure.

Acceptance criteria:

- `map_async(1, ...)` behaves like sequential `map`.
- `map_async(n, ...)` preserves output order even when futures complete out of
  order.
- Bounded downstream pressure limits upstream consumption.
- Cancelling a run stops in-flight operator tasks where possible and marks the
  stream cancelled.
- Operator errors surface to the materialized result.

Implementation status:

- Added ordered `Source::map_async(parallelism, fn)` returning a typed
  `StreamResult<Source<_>>`.
- Added `Flow::from_async_fn(parallelism, fn)` for the same ordered async
  behavior through `via(flow)`.
- Added `StreamError::Operator` with stable `operator-error` code for invalid
  async operator configuration and task join failures.
- Rejected zero parallelism at construction time with a typed operator error.
- Kept `map_async_unordered` out of this slice so ordered Akka-like
  `mapAsync` remains the default first-path API.
- Implemented bounded in-flight work: the stage pulls and spawns at most
  `parallelism` mapper futures ahead of downstream demand.
- Propagated cancellation through `take` and dropped materialized source
  stages by aborting in-flight async mapper tasks where Tokio permits it.
- Added focused tests for sequential parity, ordered out-of-order completion,
  zero parallelism, bounded in-flight work, flow usage, task failure surfacing,
  and cancellation of in-flight async tasks.
- Updated the Phase 6 streams guide with async operator examples and the
  ordered/unordered decision.

Review commands:

```bash
cargo test -p rakka-stream stream_facade_async
cargo test -p rakka-testkit stream_probe
```

## Slice 6E: Fan-in And Fan-out Operators

Goal: expose Akka-recognizable `merge` and `broadcast` facade operators over
the existing bounded stream adapter foundations.

Status: implemented.

Scope:

- Add `Source::merge(left, right)` or `source.merge(other)`.
- Add `Source::merge_all(iter)` if the two-source API is too narrow.
- Add `source.broadcast(count)` or a small `Broadcast<T>` facade that returns
  multiple `Source<T>` branches.
- Reuse or adapt existing `fan_in_streams` and `broadcast_streams` behavior.
- Define completion rules:
  - merge completes when all inputs complete;
  - merge fails or cancels when any input fails unless a supervision option
    says otherwise;
  - broadcast completes all branches when the source completes;
  - cancellation of one branch is explicit and tested.
- Add bounded per-branch capacities.

Acceptance criteria:

- Merge forwards all items from every input without duplication.
- Broadcast forwards every item to every live branch.
- A full branch applies back-pressure to the broadcast source.
- Cancellation and source failure are observable and deterministic.
- Existing low-level `fan_in_streams` and `broadcast_stream` tests still pass.

Implementation status:

- Added `Source::merge(other)` for two-source fan-in.
- Added `Source::merge_all(sources)` and
  `Source::merge_all_with_settings(settings, sources)` for multi-source
  fan-in and explicit operator capacity.
- Implemented merge with bounded internal output capacity from
  `StreamRunSettings::operator_buffer_capacity`.
- Added `Source::broadcast(branches)` returning bounded branch `Source<T>`
  values for `T: Clone`.
- Rejected zero broadcast branches with typed `StreamError::Operator`.
- Implemented broadcast so every item is sent to every live branch, a full
  live branch backpressures upstream, and a cancelled or dropped branch is
  removed from the fan-out.
- Added focused facade tests for merge completion, empty merge, merge source
  failure, broadcast duplication, invalid branch counts, full-branch
  back-pressure, and cancelled branch dropout.
- Updated the Phase 6 stream guide with fan-in/fan-out examples and
  cancellation/back-pressure notes.

Review commands:

```bash
cargo test -p rakka-stream --test stream_adapters
cargo test -p rakka-stream stream_facade_fanout
```

## Slice 6F: Actor Source And Sink With Explicit Ack

Goal: add Akka-style actor stream boundaries with a Rakka-native acknowledgement
protocol.

Scope:

- Keep existing `ActorSink` and `spawn_actor_source` simple adapters.
- Add acked facade APIs:
  - `Sink::actor_ref(actor_ref)`;
  - `Sink::actor_ref_with_ack(actor_ref, protocol)`;
  - `Source::actor_ref(capacity)`;
  - `Source::actor_ref_with_ack(capacity, protocol)`.
- Define an `AckProtocol` or equivalent with:
  - init message;
  - ack message;
  - complete message;
  - failure/cancel message;
  - optional demand or credits.
- Preserve message ownership on full or closed mailbox errors.
- Make back-pressure explicit:
  - unacked actor sink sends at most one item or one configured window;
  - source actor only accepts according to bounded capacity or ack protocol.
- Add tests for full mailbox, closed actor, missing ack, duplicate ack, and
  cancellation.

Acceptance criteria:

- Actor sink delivers items in order when acks arrive.
- Actor sink does not overrun the configured ack window.
- Failure before delivery returns or records the undelivered item.
- Actor source exposes a typed `ActorRef<M>` and bounded `Source<M>`.
- Cancellation sends the configured cancel/failure signal when provided.

Review commands:

```bash
cargo test -p rakka-stream actor_ack
cargo test -p rakka-core --test local_actor_runtime
```

## Slice 6G: Entity Source And Sink Integration

Goal: integrate the stream facade with sharded entity references without making
`rakka-stream` own sharding semantics.

Scope:

- Keep `rakka-stream`'s optional `adapters` feature for sharding integration.
- Add facade sink constructors:
  - `Sink::entity_ref(region, entity_ref)`;
  - `Sink::entity_ref_with_ack(region, entity_ref, protocol)` if the ack model
    can be reused.
- Add entity source support only where the entity can explicitly emit to a
  stream actor/source boundary. Avoid pretending that arbitrary entities are
  queryable streams.
- Preserve current `EntitySinkError` message ownership behavior.
- Test no-route, delivery failure, owner movement, passivation, and successful
  delivery.

Acceptance criteria:

- A source can run into a sharded `EntityRef` sink.
- Missing owner and delivery failures preserve the undelivered message.
- Entity sink behavior remains deterministic over `ShardRegion` snapshots.
- The API does not introduce a dependency cycle between stream and sharding.

Review commands:

```bash
cargo test -p rakka-stream --features adapters
cargo test -p rakka-sharding
```

## Slice 6H: Adapter Migration Onto The Facade

Goal: use the new facade where it simplifies HTTP, gRPC, process IO, and actor
adapter lifecycle semantics.

Scope:

- Review current HTTP/gRPC streaming adapters for duplicate bounded lifecycle
  handling.
- Review `ProcessOutputStream` and `ProcessInputSink` for facade wrappers:
  - `Source::process_stdout`;
  - `Source::process_stderr`;
  - `Sink::process_stdin`.
- Keep low-level process functions for callers that need direct pipe control.
- Add conversion APIs rather than wholesale rewrites if that keeps risk low:
  - `ProcessOutputStream::into_source()`;
  - `ProcessInputSink::as_sink()`;
  - `StreamSource<T>::into_source()`;
  - `StreamSink<T>::into_sink()`.
- Update docs to use facade examples when they are clearer.

Acceptance criteria:

- Existing HTTP, gRPC, and process stream tests pass unchanged.
- At least one process IO example uses the facade.
- Adapter cancellation and drain semantics remain unchanged.
- No adapter introduces unbounded buffering.

Review commands:

```bash
cargo test -p rakka-http
cargo test -p rakka-grpc
cargo test -p rakka-stream --test stream_adapters
cargo test -p rakka-process
```

## Slice 6I: Stream Testkit Probes

Goal: provide reusable probes for stream demand, item assertions, completion,
cancellation, and failure.

Scope:

- Add `rakka-testkit` stream probe types:
  - `StreamTestKit`;
  - `TestSourceProbe<T>`;
  - `TestSinkProbe<T>`;
  - `TestDemandProbe` if demand is separated from item assertions.
- Add source probe methods:
  - `send_next`;
  - `send_complete`;
  - `send_error` or `cancel_with`;
  - `expect_cancelled`.
- Add sink probe methods:
  - `request(n)` when the facade supports demand;
  - `expect_next`;
  - `expect_next_n`;
  - `expect_no_message`;
  - `expect_complete`;
  - `expect_error`;
  - `cancel`.
- Keep the existing helper functions:
  - `collect_stream_source`;
  - `expect_stream_source_items`;
  - `wait_for_stream_depth`;
  - `assert_stream_lifecycle`.
- Add deterministic timeouts to every async assertion.

Acceptance criteria:

- Tests can drive a source manually and assert sink observations without
  sleeps.
- Demand probes prove that an actor sink with ack does not over-pull.
- Completion, cancellation, and failure assertions produce useful diagnostics.
- The testkit helpers are documented and covered by integration tests.

Review commands:

```bash
cargo test -p rakka-testkit --test integration_helpers
cargo test -p rakka-stream
cargo doc -p rakka-testkit --no-deps
```

## Slice 6J: Docs, Examples, And Migration Notes

Goal: make the stream facade understandable and easy to adopt.

Scope:

- Add `docs/rakka-akka-parity-phase-6-streams.md`.
- Update `docs/rakka-akka-parity-migration-notes.md` with stream migration
  examples.
- Add or update examples:
  - finite source operators;
  - actor sink with ack;
  - process stdout facade source;
  - stream testkit probe usage.
- Update `README.md` only if the facade is stable enough to advertise.
- Update the main implementation plan with Phase 6 completion notes as slices
  land.

Acceptance criteria:

- Docs explain the bounded-stream parity boundary.
- Examples run locally without external services.
- Migration notes show low-level `StreamSource` to facade conversion.
- The full validation gate passes.

Review commands:

```bash
cargo run -p rakka-example-streams
cargo test -p rakka-testkit
cargo doc --workspace --all-features --no-deps
```

## Full Validation Gate

Use this gate before marking Phase 6 complete:

```bash
cargo fmt --all -- --check
cargo test -p rakka-stream
cargo test -p rakka-testkit
cargo test -p rakka-http
cargo test -p rakka-grpc
cargo test -p rakka-process
cargo test -p rakka-sharding
cargo check --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo doc --workspace --all-features --no-deps
```

## Risks And Mitigations

- API overreach: start with an Akka-shaped bounded subset and document
  non-goals clearly.
- Rust closure complexity: require owned `Send + 'static` closures first, then
  add borrowed or fallible variants only when a concrete use case needs them.
- Hidden unbounded buffering: route all facade materialization through
  `StreamRunSettings` and bounded channels.
- Cancellation leaks: every operator task must have tests for downstream
  cancellation and upstream failure.
- Actor ack ergonomics: keep the ack protocol explicit and typed, with a
  no-ack actor sink available for simple fire-and-fail-fast use.
- Dependency cycles: keep optional sharding/process integrations behind
  existing feature gates and avoid moving sharding types into `rakka-core`.
