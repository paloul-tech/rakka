# Rakka Akka Parity Phase 2 Actor Facade

Status: First implementation slice
Date: 2026-06-11

Phase 2 makes simple actors easier to write and moves common interaction
patterns into `ActorContext`. The low-level `Actor` trait remains supported for
stateful or fully custom actors, while new examples should prefer the top-level
facade crate and curated prelude.

## Import Style

Use the facade prelude for application code:

```rust
use rakka::prelude::*;
```

Reach into `rakka::actor` only for less common core types, and into adapter
modules such as `rakka::http` or `rakka::grpc` for integration APIs.

## Choosing An Actor Shape

Use `actor_fn` for simple synchronous handlers:

```rust
let echo = system.spawn("echo", actor_fn(|_ctx, msg| {
    match msg {
        EchoMessage::Ping { reply_to } => {
            let _ = reply_to.reply("pong");
            Ok(ActorAction::Continue)
        }
    }
}))?;
```

Use `setup` when construction needs the actor context:

```rust
let actor = system.spawn("configured", setup(|ctx| {
    let path = ctx.path().to_string();
    Ok(MyBehavior { path })
}))?;
```

Use a manual `Actor` implementation when the handler naturally awaits while
borrowing actor state or the context:

```rust
impl Actor for Worker {
    type Msg = WorkerCommand;

    fn handle<'a>(
        &'a mut self,
        ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        actor_future(async move {
            self.handle_command(ctx, msg).await?;
            Ok(ActorAction::Continue)
        })
    }
}
```

## Context Idioms

Prefer context-owned APIs instead of ad hoc tasks and local registries:

- `ctx.spawn`, `ctx.spawn_anonymous`, `ctx.children`, and `ctx.child` for child
  actor lifecycle and lookup.
- `ctx.watch`, `ctx.watch_with`, and `ctx.unwatch` for DeathWatch.
- `ctx.start_timer_once`, `ctx.cancel_timer`, and `ctx.is_timer_active` for
  keyed timers.
- `ctx.set_receive_timeout` and `ctx.cancel_receive_timeout` for idle receive
  timeouts.
- `ctx.message_adapter` when another actor protocol should feed into the
  current actor.
- `ctx.ask`, `ctx.ask_with_status`, and `ctx.pipe_to_self` for async interaction
  that preserves actor message ordering.
- `ctx.trace_context` for actor-scoped log and tracing fields.

## Testkit Probes

`rakka-testkit` includes reusable Phase 2 context probes:

- `TestProbe::expect_message_eq` and `TestProbe::expect_no_message`.
- `expect_terminated` for DeathWatch-style termination assertions.
- `spawn_actor_context_probe` with `ActorContextProbeCommand` and
  `ActorContextProbeEvent` for timers, receive timeouts, watch/unwatch, context
  ask, and pipe-to-self.
- `spawn_stop_probe` and `spawn_echo_probe` as small target actors for context
  probe tests.

Example:

```rust
let system = ActorSystem::new("phase-2-test");
let mut events = TestProbe::<ActorContextProbeEvent>::spawn(&system, "events")?;
let probe = spawn_actor_context_probe(&system, "context", events.actor_ref())?;

probe.tell(ActorContextProbeCommand::StartTimer {
    key: "tick".to_owned(),
    delay: Duration::from_millis(10),
})?;

events
    .expect_message_eq(
        ActorContextProbeEvent::TimerFired("tick".to_owned()),
        Duration::from_secs(1),
    )
    .await?;
```

## Async Closure Facade Tradeoff

The current `actor_fn` accepts a synchronous closure that returns `ActorResult`.
This keeps the common path compact and inference-friendly. Async work is still
available through manual `Actor` implementations, `Behavior`, `setup`,
`ctx.ask`, and `ctx.pipe_to_self`.

A fully async closure facade is possible as an additive API, but it runs into
Rust higher-ranked lifetime complexity. The core actor handler borrows both the
actor state and `ActorContext` for some lifetime `'a` and returns
`ActorFuture<'a>`. A closure-friendly async API wants to express:

```rust
for<'a> FnMut(&'a mut ActorContext<M>, M) -> Future<Output = ActorResult> + 'a
```

The unpleasant part is that each lifetime `'a` can produce a different future
type. Stable Rust cannot name that family of `impl Future` return types in a
simple closure bound. The usual workarounds make the call site noisier:

- Require users to return `ActorFuture<'_>` and call `Box::pin`.
- Require helper functions instead of inline async closures.
- Require a `'static` future, which prevents borrowing `ctx` or actor state
  across `.await`.
- Add extra adapter traits that improve internals but make compiler diagnostics
  and type inference harder to understand.

Adding a separate `actor_async` helper would not need to break existing code.
Changing `actor_fn` itself to become async-only or to require boxed future
annotations could break current synchronous handlers and would make the simple
case less pleasant. The safer path is to keep `actor_fn` stable and prototype an
additive async helper only if its call site stays readable.
