# Entity Activation Lifecycle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A local sharded entity never loses a message across its stop, idle passivation costs one timer per entity, a remote ask the owner cannot deliver is answered with a typed rejection, and a run actor that loses a compare-and-set recovers before its next command (issues #112 and #111, batch 1).

**Architecture:** `LocalEntityRoute` becomes a per-entity state machine (active or stopping) whose delivery, stop decision and stop all run under its registry lock, with a bounded hold queue drained into the next incarnation by a DeathWatch-driven task. `rakka-core` marks an actor stopping before its `stopped` hook so a late `tell` fails closed. `rakka-remote` gains an additive failure field on the envelope that the owner's ask inbound uses to answer a failed local delivery, and the TCP session survives. The agent run actor's runner and durable inbox mark themselves stale on a revision conflict and the actor recovers them at the start of its next command.

**Tech Stack:** Rust 1.88, Tokio, prost (hand-written envelope struct), serde; `cargo test` per crate; `scripts/validate.sh`.

**Spec:** `docs/superpowers/specs/2026-10-28-entity-activation-lifecycle-design.md`

## Global Constraints

- MSRV 1.88; workspace lints apply: every public item documented, `cargo clippy --workspace --all-targets --all-features -- -D warnings` clean, `unsafe_code = "forbid"`.
- `rakka-sharding` must not depend on `rakka-persistence`.
- No new persisted field, schema version, or durable record anywhere.
- Remote envelope changes are additive only: new tag 9, existing tags 1 to 8 unchanged, `DEFAULT_REMOTE_ENVELOPE_VERSION` stays `1`.
- Stable codes do not change. New codes: `remote-ask-decode`, `remote-ask-entity-type-mismatch`. Carried rejection codes are the existing HTTP/gRPC strings: `entity-no-route`, `entity-mailbox-full`, `entity-mailbox-closed`, `entity-not-local`, `entity-spawn-failed`, `entity-remote-encode`, `entity-remote-send`, `entity-shard-handoff`, `entity-shard-buffer-full`, `entity-rejected`.
- Error types the host classifies stay as they are: `DurableError::RevisionConflict` under `AgentRunEngineError::Persistence`, `WorkflowError::RevisionConflict` under `AgentInboxError::Workflow` / `AgentOutboxError::Workflow`.
- Held messages are bounded by the entity actor's configured mailbox capacity (`ActorOptions`, `crates/rakka-core/src/supervision.rs:29`, read through the route's `actor_options`), and `RemoteFailure::message` by `REMOTE_FAILURE_MESSAGE_MAX_BYTES = 512`.
- Never hold a `std::sync::Mutex` guard across an `.await`. Lock order inside the route: `actors` registry, then `shard_states`; the actor system's live-actor registry is taken only inside `spawn_actor_with_options`, after the route's registry.
- Commit after every task with the attribution trailer `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`. Run `cargo clean` only at the boundary between tasks, never mid-task.

## Review Focus

Inputs the spec implies but no proof below exercises directly. Each line's test is added to the owning task.

1. A delivery arrives while the next incarnation is being spawned by the watcher (the watcher holds the registry lock, so the delivery waits and then finds an active slot): it must be delivered after every held message. Task 2, test `delivery_racing_the_watcher_lands_after_held_messages`.
2. The actor system is terminating when the watcher tries to respawn: held messages are dropped with one warning and the slot is removed, nothing panics and no task leaks. Task 2, test `held_messages_are_dropped_when_the_system_is_terminating`.
3. An idle timer task whose entity was explicitly passivated and then re-activated under a new uid must not passivate the new incarnation on the old deadline. Task 3, test `idle_timer_from_a_previous_incarnation_does_not_stop_the_next`.
4. A failure reply arriving for a request that already timed out (the pending entry is gone) must be an `UnknownRequestId` endpoint error, not a panic, and must not leak. Task 5, test `late_failure_reply_for_unknown_request_is_reported_not_panicked`.
5. A revision conflict on the inbox followed by a recovery that itself fails (store error) must leave the actor stale and every command refused with the recovery error until a recovery succeeds. Task 9, test `failed_stale_recovery_keeps_refusing_until_the_store_recovers`.

---

### Task 1: Core marks a stopping actor and fails late tells closed

**Files:**
- Modify: `crates/rakka-core/src/actor.rs` (ActorCell at `:1523-1560`, `ActorRef::tell` `:508-535`, `ActorRef::stop` `:561-575`, `ActorRef::is_terminated` `:617`, `run_actor_task` `:1372-1454`)
- Test: `crates/rakka-core/tests/local_actor_runtime.rs`

**Interfaces:**
- Produces: `ActorRef::is_stopping(&self) -> bool` (public). `tell`/`ask` return `TellError::Closed`/`AskError::MailboxClosed` and publish a `DeadLetter { reason: MailboxClosed }` once the actor has left its mailbox loop. `stop()` returns `Ok(())` on a stopping actor. Envelopes still queued when the loop exits are drained and published as dead letters with `MailboxClosed`.

- [ ] **Step 1: Write the failing tests**

Add to `crates/rakka-core/tests/local_actor_runtime.rs` a `GatedStopActor` whose `handle` returns `Ok(ActorAction::Stop)` on `GatedMessage::Stop`, pushes `GatedMessage::Record(String)` into a shared `Arc<Mutex<Vec<String>>>`, and whose `stopped` hook sends `()` on an `mpsc::UnboundedSender<()>` and then awaits `Arc<tokio::sync::Notify>::notified()`.

```rust
#[tokio::test]
async fn tell_during_stopped_hook_fails_closed_and_publishes_dead_letter() {
    // spawn, tell Stop, await the "entered" signal, then:
    assert!(actor.is_stopping());
    assert!(!actor.is_terminated());
    assert!(matches!(actor.tell(GatedMessage::Record("late".into())), Err(TellError::Closed(GatedMessage::Record(_)))));
    assert!(matches!(actor.stop(), Ok(())));
    // one dead letter with DeadLetterReason::MailboxClosed and recipient == actor.path()
    // release.notify_one(); wait_until_terminated(&actor).await; handled log == []
}

#[tokio::test]
async fn messages_queued_behind_a_self_stop_become_dead_letters() {
    // tell Stop, Record("behind-1"), Record("behind-2") back to back (mailbox capacity >= 3);
    // await "entered"; release; wait_until_terminated.
    // assert exactly two dead letters, both MailboxClosed; handled log == []
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p rakka-core --test local_actor_runtime tell_during_stopped_hook messages_queued_behind -- --nocapture`
Expected: FAIL, `is_stopping` is not a method, and after stubbing it the first test's `tell` returns `Ok`.

- [ ] **Step 3: Implement the stopping flag**

In `ActorCell` add `stopping: AtomicBool` (initialised false in `ActorCell::new`), `pub(crate) fn mark_stopping(&self)` and `pub(crate) fn is_stopping(&self) -> bool`. Add `pub fn is_stopping(&self) -> bool` on `ActorRef` with the doc line "Returns true once the actor has left its mailbox loop and will not handle another message." In `tell`, treat `self.is_terminated() || self.is_stopping()` as the closed case (dead letter, `TellError::Closed`). In `stop`, return `Ok(())` for the same condition. In `run_actor_task`, immediately after the `if let Some(reason) = lifecycle_failure(...) { ... } else { ... loop ... }` block and before `actor.stopped(...)`: `cell.mark_stopping(); receiver.close();` then `while let Ok(envelope) = receiver.try_recv() { cell.mark_dequeued(); if matches!(envelope, Envelope::User(_)) { publish DeadLetter { recipient: cell.path.clone(), message_type: cell.message_type.to_string(), reason: DeadLetterReason::MailboxClosed } via cell.dead_letters.send } }`.

- [ ] **Step 4: Run the core tests**

Run: `cargo test -p rakka-core`
Expected: PASS, including the existing `sending_to_stopped_actor_publishes_dead_letter`.

- [ ] **Step 5: Commit**

```bash
git add crates/rakka-core/src/actor.rs crates/rakka-core/tests/local_actor_runtime.rs
git commit -m "Fail a tell to an actor that has left its mailbox loop instead of losing the message"
```

---

### Task 2: The local route holds messages across a stop

**Files:**
- Modify: `crates/rakka-sharding/src/local.rs` (whole file: `LocalEntityHandle` `:97-121`, `entity_count`/`entity_actor`/`reap_terminated_entities`/`passivate_entity`/`passivate_shard` `:180-230`, `actor_for` `:262-317`, `activate_existing_or_spawn` `:319-330`, `schedule_idle_passivation` `:355-380`, `remove_terminated_entity` `:382-395`, `deliver` `:451-489`, `passivate_entity_in`/`passivate_shard_in`/`passivate_entity_if_idle` `:522-592`)
- Modify: `crates/rakka-sharding/src/facade.rs:2079-2085` (`LocalRouteControl::passivate_entity`)
- Create: `crates/rakka-sharding/tests/entity_lifecycle.rs`

**Interfaces:**
- Consumes: Task 1's `ActorRef::is_stopping`, `TellError::Closed` on a stopping actor, `ActorRef::when_terminated`, `ActorRef::uid`.
- Produces (public on `LocalEntityRoute<M, A, F>`): `pub fn passivate_entity_with(&self, entity_id: &EntityId, stop_message: Option<M>) -> bool` (true when the entity is now stopping or was already stopping); `passivate_entity(&self, entity_id) -> bool` keeps its signature and calls `passivate_entity_with(entity_id, None)`; `entity_count`, `entity_actor`, `reap_terminated_entities`, `passivate_shard` keep their signatures. Private: `enum LocalEntitySlot<M> { Active { context: LocalEntityContext, actor_ref: ActorRef<M>, idle_deadline: Option<Instant>, timer_armed: bool }, Stopping { context: LocalEntityContext, actor_ref: ActorRef<M>, held: VecDeque<M>, reactivate: bool } }`; `enum PassivationOutcome { Stopping, AlreadyStopping, Absent, Abandoned }`; `fn passivate_slot(&self, actors: &mut BTreeMap<EntityId, LocalEntitySlot<M>>, entity_id: &EntityId, stop_message: Option<M>) -> PassivationOutcome`; `fn spawn_incarnation(&self, context: &LocalEntityContext) -> Result<ActorRef<M>, EntityDeliveryFailure>`; `fn spawn_stop_watcher(&self, entity_id: EntityId, actor_ref: ActorRef<M>)`; `fn on_incarnation_stopped(&self, entity_id: &EntityId, uid: ActorUid)`. The registry type becomes `Arc<Mutex<BTreeMap<EntityId, LocalEntitySlot<M>>>>`; `next_idle_token` and `LocalEntityHandle` are removed. Task 3 replaces the idle timer; in this task `schedule_idle_passivation` keeps spawning one sleeper per delivery but the sleeper calls `passivate_slot` under the lock after checking the slot is `Active` with the same uid and that no delivery arrived since (compare `idle_deadline`).

- [ ] **Step 1: Write the failing tests**

Create `crates/rakka-sharding/tests/entity_lifecycle.rs` with: `enum LifecycleCommand { Record(String), Get(ReplyTo<Vec<String>>), Passivate }`; a `LifecycleEntity { context: LocalEntityContext, incarnation: usize, items: Vec<String>, log: Arc<Mutex<Vec<(usize, String)>>>, stop_gate: Option<StopGate>, handle_gate: Option<Arc<Notify>> }` where `StopGate { entered: mpsc::UnboundedSender<()>, release: Arc<Notify> }`; `Record` pushes to `items` and logs `(incarnation, value)`, `Get` replies `items.clone()`, `Passivate` returns `Ok(ActorAction::Stop)`; the `stopped` hook sends on `entered` and awaits `release.notified()` when a gate is set; `handle` awaits `handle_gate.notified()` before `Record` when set. A `fixture(idle: Option<Duration>, options: ActorOptions) -> (LocalEntityRoute<..>, ShardRegion<LifecycleCommand>, EntityRef, Arc<Mutex<Vec<..>>>, StopGate, AtomicUsize incarnations)` builds a one-node membership (`ClusterMembership` as `membership_with_up_nodes` does in `sharding_foundation.rs:312`), a coordinator reconciled once, and `ShardRegion::from_snapshot`.

```rust
#[tokio::test]
async fn message_held_during_idle_stop_is_handled_by_next_incarnation() {
    // idle 20 ms; tell Record("first"); await entered (the idle stop is in its hook);
    entity.tell(&region, Record("held")).unwrap();            // red today: SpawnFailed
    let reply = entity.ask(&region, Get, 1s);                  // sent while still in the hook
    release.notify_one();
    assert_eq!(reply.await.unwrap(), vec!["held"]);
    assert_eq!(log, [(1, "first"), (2, "held")]);
    assert_eq!(route.entity_count(), 1);
}

#[tokio::test]
async fn held_messages_keep_arrival_order_ahead_of_later_ones() {
    // hold Record a, b, c during the hook; release; wait until route.entity_actor(&id).is_some();
    // tell Record d; Get == [a, b, c, d]
}

#[tokio::test]
async fn held_messages_are_bounded_by_mailbox_capacity() {
    // ActorOptions with mailbox capacity 2; hold two; the third:
    assert!(matches!(entity.tell(&region, Record("third")), Err(EntityTellError::Delivery { failure: EntityDeliveryFailure::MailboxFull, message: Record(v) }) if v == "third"));
}

#[tokio::test]
async fn explicit_passivation_queues_stop_message_and_stop_together() {
    // no idle; tell Record("a"); route.passivate_entity_with(&id, Some(Passivate)) == true;
    // the log ends with (1, "passivate") recorded by the first incarnation before the gate;
    // during the hook: passivate_entity_with again == true (AlreadyStopping), tell Record("b") held;
    // release; Get == ["b"]; incarnations == 2
}

#[tokio::test]
async fn self_passivating_entity_answers_two_consecutive_asks_without_buffering() {
    // facade: ClusterSharding::get + Entity::of(...).without_buffering() with a gated FacadeLifecycleEntity;
    // ask Get; sharding.passivate_entity_id(&key, &id) == Ok(true); await entered;
    // ask Get again (held); release; second ask == Ok(vec![])
}

#[tokio::test]
async fn stop_that_cannot_be_queued_leaves_entity_active() {
    // mailbox capacity 1, handle_gate set, idle 20 ms; tell Record("blocked") (in handle, waiting on the gate);
    // tell Record("queued") fills the mailbox; sleep 60 ms (the idle deadline passed, stop() == MailboxFull);
    assert_eq!(route.entity_count(), 1); assert!(route.entity_actor(&id).is_some());
    // handle_gate.notify_waiters() twice; wait_for_entity_count(0) within 200 ms;
    // tell Record("after") == Ok; entity_count == 1; log == [(1,"blocked"),(1,"queued"),(2,"after")]
}

#[tokio::test]
async fn self_stopping_entity_drops_queued_messages_as_dead_letters_and_holds_later_ones() {
    // subscribe_dead_letters; tell Passivate, Record("behind-1"), Record("behind-2"); await entered;
    // tell Record("after") == Ok (held via Closed); release; Get == ["after"];
    // exactly two dead letters with DeadLetterReason::MailboxClosed
}

#[tokio::test]
async fn shard_transfer_refuses_delivery_after_state_flip_and_holds_after_reacquire() {
    // tell Record("a"); route.mark_shard_transferring(shard) == 1; await entered;
    // tell Record("b") == Err(Delivery { failure: ShardHandoff { state: Transferring, .. }, .. });
    // route.mark_shard_acquired(shard); tell Record("c") == Ok (held); release; Get == ["c"]
}

#[tokio::test]
async fn remembered_replay_during_stop_reactivates_entity() {
    // tell Record("a"); route.passivate_entity(&id); await entered;
    // <LocalEntityRoute as EntityRoute<_>>::activate_entity(&route, &entity_type, &id, shard) == Ok(true);
    // release; wait until route.entity_actor(&id).is_some(); incarnations == 2; Get == []
}

#[tokio::test]
async fn delivery_racing_the_watcher_lands_after_held_messages() {
    // hold Record("h1"), Record("h2"); spawn a task that tells Record("late") the instant release fires;
    // release; Get == ["h1", "h2", "late"]
}

#[tokio::test]
async fn held_messages_are_dropped_when_the_system_is_terminating() {
    // hold Record("h1"); system.shutdown() (or terminate) before release; release;
    // wait_for_entity_count(0); no panic; a later tell fails SpawnFailed
}
```

- [ ] **Step 2: Run the new test file to verify it fails**

Run: `cargo test -p rakka-sharding --test entity_lifecycle`
Expected: compile error on `passivate_entity_with`; after a stub, `message_held_during_idle_stop_is_handled_by_next_incarnation` fails at the `tell` with `SpawnFailed("... actor-path-in-use ...")`.

- [ ] **Step 3: Implement the slot state machine in `local.rs`**

Replace `LocalEntityHandle` with `LocalEntitySlot<M>` and the registry type; remove `next_idle_token`. Implement, under the registry lock as the spec's section 5.1 orders them: `deliver` (re-check `shard_handoff_state` after taking the registry lock; the four slot cases; `Err(Closed)` converts to `Stopping` and spawns the watcher or resolves inline when the old actor is terminated), `passivate_slot` (stop message then `stop()`; `Abandoned` on either `MailboxFull`; `Stopping` spawns the watcher), `spawn_stop_watcher` (`tokio::spawn` of `actor_ref.when_terminated().await` then `route.on_incarnation_stopped(&entity_id, uid)`; the route is `Clone`), `on_incarnation_stopped` (respawn and drain in order when `held` is non-empty or `reactivate`, else remove; on spawn failure remove and `eprintln!`-free: add `tracing.workspace = true` to `crates/rakka-sharding/Cargo.toml` and emit `tracing::warn!` with `entity_type`, `entity_id`, `dropped = held.len()`), `passivate_entity_with`, `passivate_shard` via `passivate_slot` per `Active` slot of the shard, `activate_existing_or_spawn` setting `reactivate` on a `Stopping` slot, and the reaping functions skipping `Stopping` slots. Keep `observe_activation` calls on every spawn, outside the lock where it is today. The slot stores `context: LocalEntityContext` so a respawn needs no entity type from the caller.

- [ ] **Step 4: Point the facade control at the new entry**

In `crates/rakka-sharding/src/facade.rs:2079-2085` replace the body with `self.route.passivate_entity_with(entity_id, self.stop_message_factory.as_ref().map(|factory| factory()))`.

- [ ] **Step 5: Run the sharding crate's tests**

Run: `cargo test -p rakka-sharding`
Expected: PASS, including `local_entity_route_idle_timeout_passivates_and_recreates_entity`, `local_entity_route_recreates_entity_after_self_passivation`, `cluster_sharding_facade_buffers_messages_during_explicit_passivation`, and `ownership_refresh_passivates_entities_for_shards_lost_to_joining_node` in `sharding_foundation.rs`. If `wait_for_entity_count(&route, 0)` in the idle test now needs more than its 200 ms because the slot is removed by the watcher, raise that helper's attempts, not the production code.

- [ ] **Step 6: Run clippy on the crate**

Run: `cargo clippy -p rakka-sharding --all-targets --all-features -- -D warnings`
Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add crates/rakka-sharding/Cargo.toml crates/rakka-sharding/src/local.rs crates/rakka-sharding/src/facade.rs crates/rakka-sharding/tests/entity_lifecycle.rs
git commit -m "Hold messages across a local entity's stop and deliver them to its next incarnation (issue #112 H1, H2)"
```

---

### Task 3: One idle timer per entity

**Files:**
- Modify: `crates/rakka-sharding/src/local.rs` (`schedule_idle_passivation` and the `Active` slot's `idle_deadline`/`timer_armed`)
- Test: `crates/rakka-sharding/tests/entity_lifecycle.rs`

**Interfaces:**
- Consumes: Task 2's slot and `passivate_slot`.
- Produces: `pub fn armed_idle_timers(&self) -> usize` on `LocalEntityRoute` (count of `Active` slots with `timer_armed`), documented as a diagnostic. `fn note_delivery(&self, actors: &mut BTreeMap<..>, entity_id: &EntityId)` sets `idle_deadline = Instant::now() + timeout` and spawns `fn run_idle_timer(self, entity_id: EntityId, uid: ActorUid, deadline: Instant)` only when `timer_armed` is false.

- [ ] **Step 1: Write the failing tests**

```rust
#[tokio::test]
async fn idle_timer_is_one_task_per_entity() {
    // idle 200 ms; 1_000 tells of Record to one entity;
    assert_eq!(route.armed_idle_timers(), 1);
    // after wait_for_entity_count(0): armed_idle_timers() == 0
}

#[tokio::test]
async fn idle_deadline_extends_on_each_delivery() {
    // idle 50 ms; tell at t0, t0+30 ms, t0+60 ms; at t0+90 ms entity_count() == 1;
    // wait_for_entity_count(0) succeeds before t0+200 ms
}

#[tokio::test]
async fn idle_timer_from_a_previous_incarnation_does_not_stop_the_next() {
    // idle 100 ms; tell Record("a") at t0; at t0+20 ms passivate_entity_with(&id, None) with the gate held;
    // tell Record("b") (held); release; the second incarnation starts at ~t0+30 ms;
    // at t0+110 ms (old deadline passed) entity_count() == 1 and entity_actor(&id).is_some();
    // wait_for_entity_count(0) succeeds by t0+300 ms
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p rakka-sharding --test entity_lifecycle idle_`
Expected: FAIL, `armed_idle_timers` does not exist; after a stub the first test counts more than one.

- [ ] **Step 3: Implement the per-entity timer**

Replace `schedule_idle_passivation` with `note_delivery` and `run_idle_timer` as the spec's section 5.1 pseudo-code: sleep until the deadline; under the lock, if the slot is `Active` with the same uid and its `idle_deadline` has passed, call `passivate_slot(.., None)`; on `Abandoned` push the deadline out by one timeout and loop; on any other outcome clear `timer_armed` and exit; if the deadline moved, sleep to the new one; if the slot is gone or belongs to another uid, exit without touching the other incarnation's flag.

- [ ] **Step 4: Run the crate's tests and clippy**

Run: `cargo test -p rakka-sharding && cargo clippy -p rakka-sharding --all-targets --all-features -- -D warnings`
Expected: PASS and clean.

- [ ] **Step 5: Commit**

```bash
git add crates/rakka-sharding/src/local.rs crates/rakka-sharding/tests/entity_lifecycle.rs
git commit -m "Arm one idle timer per local entity instead of one sleeping task per delivery (issue #112 H3)"
```

---

### Task 4: Stress proof across the hold

**Files:**
- Test: `crates/rakka-sharding/tests/entity_lifecycle.rs`

**Interfaces:**
- Consumes: Tasks 2 and 3.

- [ ] **Step 1: Write the stress test**

```rust
#[tokio::test]
async fn no_message_is_lost_across_repeated_idle_stops() {
    // idle 1 ms; 50 entities; 10_000 tells of Record spread round-robin from 4 spawned tasks;
    // each tell is counted as sent; an Err(MailboxFull) is counted as refused;
    // after wait_for_entity_count(0): log.len() + refused == 10_000
}
```

- [ ] **Step 2: Run it**

This is the secondary check the spec's section 6 names; the deterministic proofs in Task 2 are the ones that fail on `8445ee4`. If it is flaky under load, reduce to 2 000 tells and keep the invariant.

Run: `cargo test -p rakka-sharding --test entity_lifecycle no_message_is_lost -- --nocapture`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/rakka-sharding/tests/entity_lifecycle.rs
git commit -m "Prove no message is lost across repeated idle stops"
```

---

### Task 5: An additive failure field on the remote envelope

**Files:**
- Modify: `crates/rakka-remote/src/proto.rs:3-21` (new `ProtoRemoteFailure`, tag 9 on `ProtoRemoteEnvelope`)
- Modify: `crates/rakka-remote/src/envelope.rs` (`RemoteEnvelope` `:201-260`, conversions `:286-330`)
- Modify: `crates/rakka-remote/src/request.rs` (`RemoteRequestError` `:22-55`, `Display` `:57-82`, `TypedPendingRemoteReply::complete` `:266-285`)
- Modify: `crates/rakka-remote/src/lib.rs:29-33,55-57` (exports)
- Test: `crates/rakka-remote/tests/remote_boundary.rs`

**Interfaces:**
- Produces: `pub struct RemoteFailure { code, message }` with `pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self` (message truncated on a char boundary to `pub const REMOTE_FAILURE_MESSAGE_MAX_BYTES: usize = 512`), `pub fn code(&self) -> &str`, `pub fn message(&self) -> &str`; `pub const REMOTE_FAILURE_MESSAGE_TYPE_ID: &str = "rakka.remote.RemoteFailure"`; `RemoteEnvelope.failure: Option<RemoteFailure>` (default `None` from `new`), `pub fn with_failure(self, failure: RemoteFailure) -> Self`, `pub fn failure_reply(request_id: impl Into<String>, failure: RemoteFailure) -> Self` (destination `Reply { request_id }`, `request_id` set, metadata `RemoteEnvelopeMetadata::protobuf(REMOTE_FAILURE_MESSAGE_TYPE_ID, 1)`, empty payload); `RemoteRequestError::Rejected { code: String, message: String }` displayed as `remote request rejected by owner ({code}): {message}`; `complete` sends `Err(Rejected { .. })` and returns `Ok(())` when `envelope.failure` is `Some`.

- [ ] **Step 1: Write the failing tests**

In `crates/rakka-remote/tests/remote_boundary.rs`:

```rust
#[test]
fn failure_reply_round_trips_through_protobuf_at_tag_9() {
    let envelope = RemoteEnvelope::failure_reply("req-1", RemoteFailure::new("entity-shard-handoff", "draining"));
    let bytes = ProtobufEnvelopeCodec::encode(&envelope).unwrap();
    let decoded = ProtobufEnvelopeCodec::decode(&bytes).unwrap();
    assert_eq!(decoded.failure.as_ref().map(RemoteFailure::code), Some("entity-shard-handoff"));
    assert_eq!(decoded.metadata.message_type_id, REMOTE_FAILURE_MESSAGE_TYPE_ID);
    assert!(decoded.payload.is_empty());
    // a tag-9-free envelope (an existing fixture) decodes with failure == None
}

#[tokio::test]
async fn failure_reply_completes_pending_request_with_rejected() {
    // registry.register::<CartReply>("req-1"); registry.complete_reply(failure_reply("req-1", ..)) == Ok(());
    // pending.wait(1s) == Err(RemoteRequestError::Rejected { code: "entity-shard-handoff", message: "draining" });
    // registry.pending_count() == 0
}

#[test]
fn failure_message_is_bounded() {
    let failure = RemoteFailure::new("x", "é".repeat(600));
    assert!(failure.message().len() <= REMOTE_FAILURE_MESSAGE_MAX_BYTES);
    assert!(failure.message().is_char_boundary(failure.message().len()));
}

#[test]
fn late_failure_reply_for_unknown_request_is_reported_not_panicked() {
    // registry.complete_reply(failure_reply("never-registered", ..)) == Err(RemoteRequestError::UnknownRequestId { .. })
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p rakka-remote --test remote_boundary failure_`
Expected: compile errors on `RemoteFailure`, `failure_reply`, `Rejected`.

- [ ] **Step 3: Implement the field, the type, the variant and the completion check**

Add `ProtoRemoteFailure` (`#[prost(string, tag = "1")] code`, `#[prost(string, tag = "2")] message`) and `#[prost(message, optional, tag = "9")] failure` to `ProtoRemoteEnvelope`; map it in both conversions. Add `RemoteFailure` with serde derives matching `RemoteEnvelopeMetadata`'s, the constants, the `RemoteEnvelope` field and constructors, the `Rejected` variant, and the check at the top of `TypedPendingRemoteReply::complete`. Export `RemoteFailure`, `REMOTE_FAILURE_MESSAGE_MAX_BYTES`, `REMOTE_FAILURE_MESSAGE_TYPE_ID` from `lib.rs`.

- [ ] **Step 4: Run the remote crate's tests and clippy**

Run: `cargo test -p rakka-remote && cargo clippy -p rakka-remote --all-targets --all-features -- -D warnings`
Expected: PASS and clean. Also `cargo check --workspace --all-features` to catch any struct literal of `RemoteEnvelope` (none were found in the workspace).

- [ ] **Step 5: Commit**

```bash
git add crates/rakka-remote/src/proto.rs crates/rakka-remote/src/envelope.rs crates/rakka-remote/src/request.rs crates/rakka-remote/src/lib.rs crates/rakka-remote/tests/remote_boundary.rs
git commit -m "Carry an optional failure on a remote reply envelope and surface it as a typed rejection"
```

---

### Task 6: The owner answers a remote ask it could not deliver

**Files:**
- Modify: `crates/rakka-sharding/src/routing.rs:20-76` (`EntityDeliveryFailure::code`), `:78-128` (`EntityTellError::code`)
- Modify: `crates/rakka-sharding/src/remote.rs:144-211` (`RemoteEntityAskInboundError::code`), `:575-654` (`RemoteEntityAskInbound::handle`)
- Test: `crates/rakka-sharding/tests/sharding_foundation.rs` (next to `remote_entity_ask_timeout_removes_pending_request` `:3021`)
- Test: `crates/rakka-sharding/tests/network_runtime.rs` (next to `networked_runtime_routes_remote_ask_reply_over_tcp` `:216`)

**Interfaces:**
- Consumes: Task 5's `RemoteEnvelope::failure_reply`, `RemoteFailure`, `RemoteRequestError::Rejected`; Task 2's `tracing` dependency in `rakka-sharding`.
- Produces: `pub const fn code(&self) -> &'static str` on `EntityDeliveryFailure` (the ten strings in Global Constraints), `pub fn code(&self) -> &'static str` on `EntityTellError<M>` (`NoRoute` → `entity-no-route`, `Delivery` → the failure's code) and on `RemoteEntityAskInboundError<M>` (`Decode` → `remote-ask-decode`, `EntityTypeMismatch` → `remote-ask-entity-type-mismatch`, `Delivery` → the tell error's code, `MissingRequestId` → `remote-ask-missing-request-id`, `MissingSource` → `remote-ask-missing-source`, `InvalidSource` → `remote-ask-invalid-source`, `UnexpectedDestination` → `remote-ask-unexpected-destination`). `handle` answers every failure that occurs after the request id and requester are known with `RemoteEnvelope::failure_reply(request_id, RemoteFailure::new(error.code(), error.to_string())).with_source(local_node_id)`; when that send succeeds it logs `tracing::warn!(code, entity_type, entity_id, "remote ask rejected")` and returns `Ok(())`; when the send fails it returns the original error.

- [ ] **Step 1: Write the failing tests**

In `sharding_foundation.rs`, modelled on `remote_entity_ask_routes_reply_back_to_requesting_node` (`:2934`):

```rust
#[tokio::test]
async fn remote_entity_ask_rejected_at_owner_answers_with_delivery_code() {
    // owner's LocalEntityRoute: route.mark_shard_draining(shard_of(entity));
    // asker: client.ask(.., Duration::from_secs(5)) wrapped in tokio::time::timeout(1s)
    // == Ok(Err(RemoteEntityAskError::Reply { error: RemoteRequestError::Rejected { code, message } }))
    // with code == "entity-shard-handoff"; client.requests().pending_count() == 0
}
```

In `network_runtime.rs`, modelled on the TCP ask test; `AskCartEntity`'s `route_b` is kept in a variable so the test can call `route_b.mark_shard_draining(..)`:

```rust
#[tokio::test]
async fn networked_runtime_answers_rejected_remote_ask_and_keeps_connection() {
    // node_b: route_b.mark_shard_draining(shard) before the ask;
    // first ask == Err(Reply { Rejected { code: "entity-shard-handoff", .. } }) within 1 s of a 5 s timeout;
    // let connections_after_rejection = node_b.transport_snapshot().inbound_connections();
    // route_b.mark_shard_owning(shard); second ask == Ok(CartReply { summary: "<id>:total" });
    // wait_for(|| node_b.transport_snapshot().inbound_envelopes() >= 2);
    // assert_eq!(node_b.transport_snapshot().inbound_connections(), connections_after_rejection);
    // assert_eq!(node_b.transport_snapshot().inbound_failures(), 0);
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p rakka-sharding --test sharding_foundation remote_entity_ask_rejected && cargo test -p rakka-sharding --test network_runtime networked_runtime_answers_rejected`
Expected: FAIL, the first with `Reply { Timeout }` after 1 s elapses (the outer timeout fires), the second the same way (and, if the first ask is let run to its timeout, `inbound_connections() == 2`).

- [ ] **Step 3: Implement the codes and the failure reply**

Add the three `code()` methods. In `handle`, split the body after `requester` is parsed into `fn handle_request(&self, envelope, request_id: &str, requester: &NodeId) -> Result<(), RemoteEntityAskInboundError<M>>` holding today's logic; the outer `handle` matches its result and, on `Err(error)`, builds and sends the failure reply as the Interfaces block says.

- [ ] **Step 4: Run the sharding crate's tests and clippy**

Run: `cargo test -p rakka-sharding && cargo clippy -p rakka-sharding --all-targets --all-features -- -D warnings`
Expected: PASS and clean. `remote_entity_ask_timeout_removes_pending_request` must still pass (a silent entity still times out).

- [ ] **Step 5: Commit**

```bash
git add crates/rakka-sharding/src/routing.rs crates/rakka-sharding/src/remote.rs crates/rakka-sharding/tests/sharding_foundation.rs crates/rakka-sharding/tests/network_runtime.rs
git commit -m "Answer a remote ask the owner could not deliver with a typed rejection and keep the connection (issue #112 H2)"
```

---

### Task 7: The A2A router classifies a rejection

**Files:**
- Modify: `crates/rakka-a2a/src/router.rs:162-184` (`remote_ask_error`)
- Test: inline `#[cfg(test)] mod tests` in `crates/rakka-a2a/src/router.rs`

**Interfaces:**
- Consumes: Task 5's `RemoteRequestError::Rejected`.
- Produces: `const RETRYABLE_REMOTE_REJECTION_CODES: [&str; 7] = ["entity-no-route", "entity-not-local", "entity-shard-handoff", "entity-mailbox-full", "entity-shard-buffer-full", "entity-mailbox-closed", "entity-spawn-failed"]`; a `Rejected { code, message }` whose code is listed maps to `RakkaA2AHandlerError::Unavailable { message: format!("owner rejected ask ({code}): {message}") }`, any other code to `OwnerAsk` with the same message text.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn rejected_remote_ask_with_delivery_code_is_unavailable() {
    let error = remote_ask_error(RemoteEntityAskError::Reply { error: RemoteRequestError::Rejected { code: "entity-shard-handoff".into(), message: "draining".into() } });
    assert!(matches!(error, RakkaA2AHandlerError::Unavailable { message } if message.contains("entity-shard-handoff")));
}

#[test]
fn rejected_remote_ask_with_decode_code_is_owner_ask() {
    // code "remote-ask-decode" => RakkaA2AHandlerError::OwnerAsk { .. }
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p rakka-a2a --lib rejected_remote_ask`
Expected: FAIL, both map to `OwnerAsk` through the catch-all.

- [ ] **Step 3: Add the explicit arm**

Insert a `RemoteRequestError::Rejected { code, message }` arm before the catch-all.

- [ ] **Step 4: Run the crate's lib tests and clippy**

Run: `cargo test -p rakka-a2a --lib && cargo clippy -p rakka-a2a --all-targets --all-features -- -D warnings`
Expected: PASS and clean.

- [ ] **Step 5: Commit**

```bash
git add crates/rakka-a2a/src/router.rs
git commit -m "Classify an owner's remote ask rejection as retryable when the request never entered a mailbox"
```

---

### Task 8: The durable inbox marks itself stale on a revision conflict

**Files:**
- Modify: `crates/rakka-workflow/src/inbox.rs` (struct `:300-312`, `with_clock` `:330-340`, `recover` `:372-385`, compare-and-set sites `:418`, `:444`, `:487`, `:719`)
- Test: `crates/rakka-workflow/tests/durable_inbox.rs` (next to `revision_conflict_surfaces_as_workflow_failure` `:187`)

**Interfaces:**
- Produces: `DurableInbox::is_stale(&self) -> bool` (public, `#[must_use]`); private `fn persistence_failure(&mut self, error: DurableError) -> WorkflowError` that sets `self.stale = true` when `error` is `DurableError::RevisionConflict { .. }` and returns `map_durable_error(&self.workflow_id, error)`; `recover()` sets `stale = false` after a successful load.

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn revision_conflict_marks_inbox_stale_until_recover() {
    // as revision_conflict_surfaces_as_workflow_failure, then:
    assert!(stale.is_stale());
    assert!(!first.is_stale());
    stale.recover().await.unwrap();
    assert!(!stale.is_stale());
    let accepted = stale.accept(InboxCommand::new("message-2", "Command", b"second".to_vec())).await.unwrap();
    assert!(matches!(accepted, InboxAcceptance::Accepted { revision, .. } if revision == Revision::new(2)));
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p rakka-workflow --test durable_inbox revision_conflict_marks_inbox_stale`
Expected: compile error, `is_stale` not found.

- [ ] **Step 3: Implement the flag**

Add the `stale: bool` field (false in `with_clock`), `is_stale`, `persistence_failure`, replace the four `.map_err(|error| map_durable_error(&self.workflow_id, error))` closures on compare-and-set results with `.map_err(|error| self.persistence_failure(error))` (the `load` in `recover` keeps `map_durable_error`), and clear the flag in `recover`.

- [ ] **Step 4: Run the crate's tests and clippy**

Run: `cargo test -p rakka-workflow && cargo clippy -p rakka-workflow --all-targets --all-features -- -D warnings`
Expected: PASS and clean.

- [ ] **Step 5: Commit**

```bash
git add crates/rakka-workflow/src/inbox.rs crates/rakka-workflow/tests/durable_inbox.rs
git commit -m "Mark a durable inbox stale when its compare-and-set loses, until it recovers"
```

---

### Task 9: The run actor recovers after a lost compare-and-set

**Files:**
- Modify: `crates/rakka-agent-workflow/src/runner.rs` (struct `:310-320`, `new` `:325-340`, `recover` `:359-367`, `start` `:380-396`, `persist_transition` `:1058-1071`, `persistence_error` `:1091-1096`)
- Modify: `crates/rakka-agent-workflow/src/inbox.rs:286` (`AgentRunInbox::is_stale`)
- Modify: `crates/rakka-agent-workflow/src/runtime.rs` (`recover_components` `:700-710`, `handle` `:841-1070`, module docs at the top)
- Test: `crates/rakka-agent-workflow/tests/step_runner.rs`
- Test: `crates/rakka-agent-workflow/tests/runtime_actor.rs`

**Interfaces:**
- Consumes: Task 8's `DurableInbox::is_stale`.
- Produces: `AgentStepRunner::is_stale(&self) -> bool`; `persistence_error` becomes `fn persistence_error(&mut self, error: DurableError) -> AgentRunEngineError` and sets `stale` on `RevisionConflict` (used at `start`, `persist_transition`, and `recover`, where a load error never is a conflict); `recover()` clears it. `AgentRunInbox::is_stale(&self) -> bool` forwards. `AgentRunActor`: private `async fn refresh_if_stale(&mut self) -> AgentRunRuntimeResult<()>` (calls `recover_components` when `self.runner.is_stale() || self.inbox.is_stale()`), and private `fn reply_refresh_failure(message: AgentRunActorCommand, error: AgentRunRuntimeError)` that answers every variant's `reply_to` with `Err(error.clone())`. `handle` begins: if the message is not `Recover`, `if let Err(error) = self.refresh_if_stale().await { Self::reply_refresh_failure(msg, error); return Ok(ActorAction::Continue); }`.

- [ ] **Step 1: Write the failing runner tests**

In `step_runner.rs`, using `start_and_begin` (`:349`) and a shared `TestStore`:

```rust
#[tokio::test]
async fn lost_compare_and_set_marks_runner_stale_and_recover_clears_it() {
    // runner_a = start_and_begin(..); runner_b = AgentStepRunner::new(same workflow, run id, store); runner_b.recover();
    // runner_a.succeed_step(AgentStepSuccess::advance(AgentStepId::new("review"), inline_payload("ready")), ts(300)) ok  (revision 3)
    // runner_b.succeed_step(..) == Err(AgentRunEngineError::Persistence { error: DurableError::RevisionConflict { expected, actual, .. }, .. }) with expected == Revision::new(2), actual == Revision::new(3)
    assert!(runner_b.is_stale());
    runner_b.recover().await.unwrap();
    assert!(!runner_b.is_stale());
    // runner_b.state() now shows revision 3's status; its next valid transition succeeds at revision 4
}

#[tokio::test]
async fn lost_start_marks_runner_stale() {
    // two fresh runners over one store, both recovered; a.start(..) ok; b.start(..) == Err(Persistence { RevisionConflict { expected: INITIAL, actual: 1 } }); b.is_stale()
}
```

- [ ] **Step 2: Write the failing actor tests**

In `runtime_actor.rs`, with two actors spawned from one `RuntimeFixture` (the stores are shared clones):

```rust
#[tokio::test]
async fn actor_recovers_from_lost_compare_and_set_before_next_command() {
    // A: accept + start + BeginStep; B (spawned after A's start, so B recovered A's record): BeginStep
    // => Err(AgentRunRuntimeError::RunEngine { error: AgentRunEngineError::Persistence { error: DurableError::RevisionConflict { .. }, .. } })
    // B Snapshot => run_state revision == A's latest revision (fresh, not cached)
    // B SucceedStep => Ok(transition) at the next revision; A's next BeginStep now conflicts and A's retry succeeds
}

#[tokio::test]
async fn inbox_conflict_through_the_effect_bridge_recovers_before_next_command() {
    // graph run on both actors (start_graph + mark_graph_ready helpers); A ScheduleGraphNodeEffect ok;
    // B ScheduleGraphNodeEffect => Err(AgentRunRuntimeError::GraphEffectBridge { .. }) whose inner error is the inbox/outbox RevisionConflict;
    // B DueEffects => Ok(list) that contains A's effect (fresh inbox record)
}

#[tokio::test]
async fn deadline_fail_run_succeeds_after_a_lost_compare_and_set() {
    // after B's refused BeginStep: B FailRun { error_code: "deadline" } => Ok(transition) with next_status Failed
}

#[tokio::test]
async fn failed_stale_recovery_keeps_refusing_until_the_store_recovers() {
    // use the failure-injection store from tests/failure_injection.rs (or a FailingRunStore wrapper in this file whose load fails while a flag is set);
    // B refused once; set the flag; B SucceedStep => Err(RunEngine { Persistence { error: <the load error>, .. } }); clear the flag; B SucceedStep => Ok
}
```

- [ ] **Step 3: Run the new tests to verify they fail**

Run: `cargo test -p rakka-agent-workflow --test step_runner lost_ && cargo test -p rakka-agent-workflow --test runtime_actor recovers_from_lost inbox_conflict deadline_fail_run failed_stale_recovery`
Expected: compile error on `is_stale`; after stubbing, `actor_recovers_from_lost_compare_and_set_before_next_command` fails at B's `SucceedStep` with a second `RevisionConflict`.

- [ ] **Step 4: Implement the runner flag, the inbox forward, and the actor refresh**

As the Interfaces block says. `reply_refresh_failure` is one `match` over every `AgentRunActorCommand` variant that does `let _reply_dropped = reply_to.reply(Err(error.clone()))`. Update `runtime.rs` module docs with one paragraph: a refused compare-and-set marks the component stale and the actor recovers all components before its next command; the refused command fails with its typed error and the caller retries.

- [ ] **Step 5: Run the crate's tests and clippy**

Run: `cargo test -p rakka-agent-workflow && cargo clippy -p rakka-agent-workflow --all-targets --all-features -- -D warnings`
Expected: PASS and clean.

- [ ] **Step 6: Commit**

```bash
git add crates/rakka-agent-workflow/src/runner.rs crates/rakka-agent-workflow/src/inbox.rs crates/rakka-agent-workflow/src/runtime.rs crates/rakka-agent-workflow/tests/step_runner.rs crates/rakka-agent-workflow/tests/runtime_actor.rs
git commit -m "Recover a run actor that lost a compare-and-set before its next command (issue #111 Ask 1)"
```

---

### Task 10: Docs, compatibility and changelog

**Files:**
- Modify: `docs/rakka-akka-parity-phase-4-cluster-sharding.md` ("Passivation And State" `:54-66`, "Handoff And Passivation Buffering" `:68-100`, new section "One activation per entity is not guaranteed" after "Durable Coordinator" `:236-285`)
- Modify: `docs/rakka-v1-reliability-boundaries.md` (sharding guarantees `:74-85`, non-guarantees `:87-96`)
- Modify: `docs/rakka-compatibility.md` ("Remote Protobuf Schemas" `:48-67`)
- Modify: `CHANGELOG.md` (`### Added` `:9`, `### Changed` `:486`, `### Fixed` `:543`)

**Interfaces:**
- Consumes: the names and codes produced by Tasks 1 to 9.

- [ ] **Step 1: Write the sharding doc changes**

"Passivation And State" gains four paragraphs: delivery, the stop decision and the stop run under one lock, a message that arrives while an entity stops is held (bounded by the mailbox capacity) and delivered to the next incarnation in order; idle passivation arms one timer per entity; a stop message may be delivered more than once when the mailbox was full at passivation, so stop messages must be idempotent; an entity that returns `ActorAction::Stop` itself drops what is already queued in its mailbox as dead letters, which `rakka-process`'s `Passivate` action and the `rakka-persistence` actor facades do, so prefer the facade's passivation or the idle timer. "Handoff And Passivation Buffering" gets one sentence: the region buffer is no longer needed to survive an entity's own stop window. The new section carries the five bullets of the spec's section 5.5.

- [ ] **Step 2: Write the reliability-boundaries, compatibility and changelog changes**

Reliability boundaries: add the guarantee "A message delivered through a region to a local entity while that entity passivates is handled by its next incarnation or refused with a typed error; it is not dropped." and the non-guarantee "No wiring gives one activation per entity during a join; see the sharding doc's 'One activation per entity is not guaranteed'." Compatibility: under "Remote Protobuf Schemas" record `ProtoRemoteEnvelope` tag 9 `failure` (`code`, `message`), the type id `rakka.remote.RemoteFailure`, that an older asker decodes it as `RemoteRequestError::Decode` rather than a timeout, the rule that an optional field with a new tag may be added to the envelope within an envelope version, and the two new codes. Changelog: Fixed entries for H1/H2, the leaked actor on a full mailbox, the lost tell during `stopped`, the dropped TCP session on a rejected remote ask, and the stale run actor; Added for `RemoteFailure`/`Rejected`, `is_stopping`, `armed_idle_timers`, `is_stale`; Changed for one idle timer per entity and the docs answer to #111 Ask 2. Each entry names its issue.

- [ ] **Step 3: Run the doc-holding tests and the doc build**

Run: `cargo test --workspace --all-features doc 2>&1 | tail -20 && cargo doc --workspace --all-features --no-deps 2>&1 | tail -3`
Expected: every doc-parsing test passes; the doc build is warning-free.

- [ ] **Step 4: Commit**

```bash
git add docs/rakka-akka-parity-phase-4-cluster-sharding.md docs/rakka-v1-reliability-boundaries.md docs/rakka-compatibility.md CHANGELOG.md
git commit -m "Document the entity activation lifecycle, the remote rejection, and the join answer for issue #111 Ask 2"
```

---

### Task 11: Whole-workspace validation

**Files:** none new.

- [ ] **Step 1: Format and clippy**

Run: `cargo fmt --all -- --check && cargo clippy --workspace --all-targets --all-features -- -D warnings`
Expected: clean.

- [ ] **Step 2: Run the full validation entry point**

Run: `scripts/validate.sh > /tmp/validate.log 2>&1; echo exit=$?` (capture the exit code directly; piping into `tail` reports `tail`'s code). If the workspace test run is killed under load, run `cargo test -p <crate>` for `rakka-core`, `rakka-sharding`, `rakka-remote`, `rakka-a2a`, `rakka-workflow`, `rakka-agent-workflow`, and `rakka-agent`, then re-run `validate.sh` once more.
Expected: `exit=0`.

- [ ] **Step 3: Package check**

Run: `scripts/package-check.sh`
Expected: exit 0, nothing published.

- [ ] **Step 4: Fix anything that failed, re-run, commit the fixes**

```bash
git commit -am "Address validation findings"
```
