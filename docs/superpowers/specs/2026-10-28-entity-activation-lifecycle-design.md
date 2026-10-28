# Entity activation lifecycle: hold messages across a stop, answer a failed remote ask, recover a run actor that lost a compare-and-set

Status: written 2026-10-28, awaiting the owner's review. Branch `entity-activation-lifecycle`, off `main` at `8445ee4`.

Briefs: issue #112 (the rakka.host idle-passivation brief: H1, H2, H3) and issue #111 (the stale-run-actor brief: Ask 1 and Ask 2). Both were drafted against `8445ee4`, which is also the host's pin and current `main`. The host's priority comment asks for these two together as its batch 1, and for #110 alone as batch 2. This document covers batch 1 only.

Owner ruling (2026-10-28): #111 Ask 2 gets the docs-only answer (section 5.5). No join handoff and no follower path are built in this change.

Calls made after the builder check (2026-10-28), each marked where it lands; the owner can veto any of them:

1. A remote ask that the owner answers with a rejection is a handled envelope, not an endpoint failure: the inbound handler returns `Ok` once the failure reply is sent, so the requester's TCP session survives (section 5.3, hazard six).
2. Entities that stop themselves by returning `ActorAction::Stop`, which includes `rakka-process` and the `rakka-persistence` actor facades, keep a documented residual: what is already queued in their mailbox becomes dead letters. They are not reworked here (section 5.2).
3. `rakka-a2a` maps a rejection whose code says the request never entered a mailbox to its retryable `Unavailable` summary, and every other rejection to `OwnerAsk` (section 5.3).
4. The remote envelope version stays 1. Tag 9 is additive, and the compatibility doc states the rule that an optional field may be added to the envelope within a version (section 5.3).

## 1. Intent

Every one of the host's six entity types now leaves memory when idle, so every quiet entity crosses the stop window once per idle period. Today that window can drop a tell, answer an ask `ReplyDropped`, refuse a delivery `SpawnFailed`, or make a forwarded ask wait out its full timeout. A run actor that loses one compare-and-set during a join never writes again. The host retries around all of it; this change gives it native fixes so it can drop those retries and move its pin.

The change has four parts that share one theme, the lifecycle of a local entity incarnation:

1. the local route holds messages across a stop and delivers them to the next incarnation (#112 H1, H2, and two hazards the brief missed);
2. one idle timer per entity instead of one sleeping task per delivery (#112 H3);
3. the owner answers a remote ask whose local delivery failed, instead of letting the asker time out (#112 H2, remote half);
4. a run actor that loses a compare-and-set recovers before its next command (#111 Ask 1).

Section 6 answers #111 Ask 2 in the docs.

## 2. The hazards, verified at `8445ee4`

Paths are under `crates/`.

- **H1, a message queued behind a stop is lost.** `LocalEntityRoute::deliver` (`rakka-sharding/src/local.rs:451-489`) looks the actor up under the registry lock, drops the lock, then `tell`s. `passivate_entity_if_idle` (`:567-592`) takes the lock, removes the entry, drops the lock, then queues `Envelope::Stop`. If the stop is queued between the lookup and the tell, the message lands behind the stop. The mailbox loop (`rakka-core/src/actor.rs:1397`) breaks on `Stop` and never reads it. A tell is lost silently; an ask answers `ReplyDropped`.
- **H2, a message during the stop window is refused.** The entry is removed before the actor has stopped. The next delivery finds no entry and spawns under the same name; `register_actor_cell` (`rakka-core/src/system.rs:806-819`) refuses `actor-path-in-use` because the old cell is still live. The caller gets `SpawnFailed`. The same window exists on explicit passivation (`passivate_entity_in`, `local.rs:522-537`) unless the region was built `with_buffering`; the host's finished-run reads hit it routinely.
- **H3, one sleeping task per delivery.** `schedule_idle_passivation` (`local.rs:355-380`) spawns a task that sleeps the whole timeout on every delivery; stale ones are discarded only when they wake.
- **A fourth hazard the brief missed: a stop that cannot be queued leaks a live actor.** `passivate_entity_if_idle` removes the entry and then calls `stop()`, ignoring its result. `ActorRef::stop` fails `MailboxFull` when the mailbox is full (`actor.rs:561-575`). The actor stays alive, unknown to the registry, and every later activation of that entity fails `actor-path-in-use` until it dies on its own. `passivate_entity_in` and `passivate_shard_in` have the same shape.
- **A fifth, in core: a tell during the `stopped` hook vanishes.** After the mailbox loop breaks, `run_actor_task` (`actor.rs:1372-1454`) runs `actor.stopped(...)` and only then `cell.mark_terminated`. `ActorRef::tell` checks `is_terminated` and then `try_send`; the sender is open until the task returns. A message told during the hook is accepted, never read, and published nowhere. Widening the registry lock alone would leave this residue for an actor that stops itself.
- **Remote asks.** `RemoteEntityAskInbound::handle` (`rakka-sharding/src/remote.rs:575-654`) returns a local delivery failure to the endpoint, which maps it to `HandlerRejected` and sends nothing back. The asker's `pending.wait(timeout)` ends in `Reply { Timeout }`.
- **A sixth hazard the brief missed: that failure also ends the TCP session.** The TCP inbound loop (`rakka-remote/src/network.rs:1347-1350`) propagates the endpoint's error with `?`, records `endpoint-error`, and closes the connection. So every remote ask that meets the owner's stop window today costs the requester its connection to that owner; the requester's peer task reconnects on its next send. The host meets that window routinely.
- **#111 Ask 1.** `AgentStepRunner::persist_transition` (`rakka-agent-workflow/src/runner.rs:1058-1071`) replaces its cached record only on success, and every transition takes its expected revision from that record; `start` (`:390`) has a second compare-and-set against `Revision::INITIAL`. `DurableInbox` (`rakka-workflow/src/inbox.rs`) does the same at four compare-and-set sites (`:418`, `:444`, `:487`, `:719`). The run actor recovers only in `started`, `restarted`, and on an explicit `Recover`. So after one refused write every later write is refused too, including the deadline's `fail_run`. The brief counts three cached revisions; there are two, because the inbox and the outbox live in one `WorkflowState` record.

## 3. Goals, non-goals, anti-goals

Goals:

- A message delivered through the local route to an entity the route is stopping is handled by that entity's next incarnation, in arrival order, or the caller gets a typed error. It is never silently lost.
- A delivery never fails `SpawnFailed` because of a stop the route itself initiated.
- Idle passivation carries one timer task per active entity, not one per delivery.
- A passivation the route cannot carry out leaves the entity active and registered, never leaked.
- A remote ask whose local delivery failed at the owner is answered with a typed rejection carrying the stable delivery code, at once, and the requester keeps its connection to that owner.
- After a compare-and-set is refused for a revision conflict, the run actor computes its next command from durable state. The refused command still fails with today's typed error.
- The docs say plainly that no wiring gives one activation per entity during a join, and what the contract requires instead.

Non-goals:

- No join handoff, no follower path for the lease, no facade export of `rakka-sharding-postgres` (owner ruling).
- No change to the region buffer (`ShardBufferConfig`, `passivation_buffer_duration`); it keeps covering shard handoff gaps and composes with the new hold.
- No reply for a remote *tell* whose local delivery failed; a tell has no reply channel.
- No internal retry of a refused command inside the run actor; the caller retries.
- No change to `rakka-agent`'s entities; they already re-materialize from the store on every delivery.
- No rework of `rakka-process` or the `rakka-persistence` actor facades, which stop entities by returning `ActorAction::Stop`; they carry the documented residual of section 5.2.
- #110 in every part, including its `Cancelling` addendum. Batch 2.

Anti-goals, the things that would make this a failure even if the tests pass:

- Holding a held message forever. Every held message is delivered or dropped with a warning that names the count, within one incarnation's lifetime.
- Delivering a held message out of order with a later one, or twice.
- Blocking the registry lock across an `await`, or taking it inside an actor task in a way that could deadlock with the actor system's own registry.
- Changing a stable error code, a public error variant's meaning, or the remote envelope's existing field numbers.
- A new persisted field or schema version anywhere. Nothing in this change is durable.

## 4. Constraints

- MSRV 1.88, workspace lints: every public item documented, clippy clean under `-D warnings`, `unsafe_code = "forbid"`.
- `rakka-sharding` must not depend on `rakka-persistence` (crate cycle, see the sharding doc).
- The remote envelope is a hand-written prost struct (`rakka-remote/src/proto.rs`, tags 1 to 8). A change must be additive under `docs/rakka-compatibility.md` "Remote Protobuf Schemas": new tag, safe default, ignored by older nodes.
- Idle passivation already requires a Tokio runtime (`tokio::spawn` in `schedule_idle_passivation`), and the actor system spawns every actor task with `tokio::spawn`. The route may assume a current runtime wherever it can spawn an actor.
- Stable codes already exist for every local delivery failure in the HTTP and gRPC adapters (`rakka-http/src/error.rs:160-185`): `entity-no-route`, `entity-mailbox-full`, `entity-mailbox-closed`, `entity-not-local`, `entity-spawn-failed`, `entity-remote-encode`, `entity-remote-send`, `entity-shard-handoff`, `entity-shard-buffer-full`, `entity-rejected`. The remote rejection carries these same strings.
- The host classifies a lost compare-and-set by type: `DurableError::RevisionConflict` under the runner, `WorkflowError::RevisionConflict` under the inbox, outbox, and effect bridge. Those types and their codes do not change.

## 5. Design

### 5.1 The local route owns each incarnation's lifecycle

`LocalEntityRoute` (`rakka-sharding/src/local.rs`) replaces `LocalEntityHandle` with a slot that is either active or stopping:

```rust
enum LocalEntitySlot<M> {
    Active {
        actor_ref: ActorRef<M>,
        shard_id: ShardId,
        idle_deadline: Option<Instant>, // None when idle passivation is off
        timer_armed: bool,
    },
    Stopping {
        actor_ref: ActorRef<M>, // the incarnation being stopped
        shard_id: ShardId,
        held: VecDeque<M>,
        reactivate: bool, // a remembered-entity replay asked for the next incarnation
    },
}
```

The `idle_token` counter goes away. An incarnation is identified by `ActorRef::uid()`.

**Delivery** (`deliver`) does everything under the registry lock. The shard handoff check runs again under that lock, because `shard_states` and `actors` are separate mutexes: without the re-check, a delivery that passed the check could take the registry lock after `passivate_shard` ran, be held, and reactivate the entity on a shard this node has lost. Lock order inside the route is registry, then `shard_states`; nothing takes them in the other order.

1. No slot, or an `Active` slot whose actor `is_terminated()`: spawn the next incarnation, insert `Active`, tell, arm the idle timer. This is today's behaviour. `spawn_actor_with_options` is synchronous and does not block; it registers the cell and spawns the task. The actor's `started` runs in that task, never under the route's lock.
2. `Active` and live: `tell`.
   - `Ok`: set `idle_deadline = now + timeout`; arm the timer if it is not armed.
   - `Err(Full)`: return `MailboxFull` with the message, as today.
   - `Err(Closed)`: the actor is stopping on its own (section 5.2) or terminated between the check and the send. Convert the slot to `Stopping` with this message held and start the watcher. If the actor is already terminated, resolve inline as in step 4.
3. `Stopping` and the old actor is still live: push the message onto `held` if `held.len()` is below the actor options' mailbox capacity; otherwise return `MailboxFull` with the message. Return `Ok`.
4. `Stopping` and the old actor has terminated (its watcher has not run yet): resolve inline. Spawn the next incarnation, deliver `held` in order and then this message, set the slot `Active`, arm the timer. The watcher later finds a different uid and does nothing.

Lock order is route registry, then the actor system's live-actor registry (inside `spawn_actor_with_options`), never the reverse. The actor task's `unregister_actor_cell` takes only the system registry.

**Stopping an incarnation** is one function, used by the idle timer, explicit passivation, and shard transfer: `passivate_slot(entity_id, stop_message: Option<M>) -> PassivationOutcome { Stopping, AlreadyStopping, Absent, Abandoned }`, under the lock:

1. `Stopping` slot: `AlreadyStopping`. No slot, or `Active` with a terminated actor: remove, `Absent`.
2. `Active` and live: tell the stop message if there is one, then `actor_ref.stop()`. If either fails `MailboxFull`, leave the slot `Active` and return `Abandoned`; the idle timer re-arms for one more timeout, and an explicit passivation returns `false`. A stop message may therefore be delivered more than once to an entity whose mailbox was full at the moment of passivation; stop messages must be idempotent, and the sharding doc says so.
3. On success, set the slot `Stopping { held: empty, reactivate: false }` and spawn the watcher.

The watcher is one task per stop, not per message:

```rust
tokio::spawn(async move {
    old_actor_ref.when_terminated().await;
    route.on_incarnation_stopped(entity_id, old_uid);
});
```

`when_terminated` registers a DeathWatch recipient, and `ActorCell::watch` (`rakka-core/src/actor.rs:1646-1683`) notifies at once when the actor has already terminated, so the watcher cannot miss a termination that happened before it registered.

**After termination** (`on_incarnation_stopped`), under the lock: if the slot is `Stopping` with the same uid, then when `held` is non-empty or `reactivate` is set, spawn the next incarnation, tell each held message in order, set the slot `Active`, arm the timer, and observe the activation; otherwise remove the slot, as today. If the spawn fails (the system is terminating), remove the slot and log a warning with the held count. A `Closed` while draining means the new incarnation died in `started`; the remaining held messages are dropped with the same warning. A `Full` cannot happen, because `held` is bounded by the mailbox capacity and the new mailbox is empty.

**The idle timer** is one task per active entity. On each delivery the deadline moves; the task is spawned only when `timer_armed` is false:

```text
loop {
    sleep_until(deadline)
    lock
    match slot {
        Active with my uid, idle_deadline Some(d):
            if d <= now:
                match passivate_slot(entity_id, None):
                    Abandoned => idle_deadline = now + timeout; deadline = that; continue
                    _ => timer_armed = false; break
            else: deadline = d; continue
        _ => timer_armed = false (if the slot is Active with another uid, leave its own flag alone); break
    }
}
```

A public diagnostic `armed_idle_timers()` (count of `Active` slots with `timer_armed`) lets a test assert "one timer per entity".

**Shard transfer** (`mark_shard_transferring` → `passivate_shard`) flips the shard state first and then stops each `Active` slot of the shard through `passivate_slot(entity_id, None)`, counting the ones that entered `Stopping`. A delivery that re-checks the shard state under the registry lock after the flip is refused `ShardHandoff`, so `held` does not grow; one that took the registry lock before the flip reaches the still-active actor ahead of its stop. The watcher removes each slot when its actor terminates. If the shard is re-acquired first and a message arrives while a slot is `Stopping`, it is held and the next incarnation gets it, which is the right outcome.

**Explicit passivation.** `LocalEntityRoute::passivate_entity(entity_id)` becomes a wrapper over `passivate_entity_with(entity_id, stop_message: Option<M>) -> bool`, true for `Stopping` or `AlreadyStopping`. The facade's `LocalRouteControl::passivate_entity` calls it with `stop_message_factory.map(|f| f())`, so the stop message and the stop are queued in one critical section; today the stop message is told outside the lock. `ClusterSharding::passivate_entity_id` keeps its region-buffer pause; the two compose. An entity that passivates itself after every answer, with no region buffering, now answers two consecutive asks.

**Reaping and counting.** `reap_terminated_entities`, `remove_terminated_entity`, and `entity_count` remove only `Active` slots whose actor has terminated. A `Stopping` slot belongs to its watcher. `entity_actor(entity_id)` returns the actor ref of an `Active` slot only.

**Remembered entities.** `activate_existing_or_spawn` on a `Stopping` slot sets `reactivate = true` and returns `Ok(true)`; the activation observer fires when the watcher spawns the next incarnation.

### 5.2 Core: an actor that is stopping refuses new messages

`ActorCell` gains `stopping: AtomicBool`. `run_actor_task` sets it as soon as the mailbox loop exits, before `actor.stopped(...)`, on every exit path including a failed `started`. It then calls `receiver.close()`, so a `try_send` that races the flag fails `Closed` rather than landing in a channel nobody reads, and drains whatever `receiver.try_recv()` still holds: each `Envelope::User` is dequeued and published as a dead letter with `DeadLetterReason::MailboxClosed`.

`ActorRef::tell` and `ActorRef::ask` fail `Closed` (and publish the dead letter) when the cell is stopping or terminated. `ActorRef::stop` returns `Ok(())` when the cell is stopping. `ActorRef::is_stopping()` is public and documented as "the actor has left its mailbox loop and will not handle another message".

The residual, and who carries it. A route-initiated stop has no residual: the `Stop` envelope is queued behind whatever the route already sent, and nothing the route sends afterwards enters that mailbox. An entity that stops itself by returning `ActorAction::Stop` from `handle` leaves its loop at once, so whatever was already queued behind the message it was handling becomes dead letters, and a message that reaches the route after the flag is held for the next incarnation (delivery step 2's `Closed` arm). Today those queued messages vanish silently; after this change they are visible. Two shipped crates stop entities that way: `rakka-process` (`ProcessBackedEntityAction::Passivate`, `rakka-process/src/sharded.rs:399`, which builds a bare route and has no other passivation API) and the `rakka-persistence` actor facades (`actor.rs:356`, `event_sourced.rs:709`). They are not reworked in this change; the sharding doc states the residual, says that stopping through the facade or the idle timer has none, and the proofs include a self-stopping entity. Giving those crates a route-level passivation entry is a follow-up candidate for the report-back.

### 5.3 The owner answers a remote ask that it could not deliver

`rakka-remote`:

- `RemoteFailure { code: String, message: String }`, with the message bounded (the builder picks the bound; 512 bytes is the suggestion) and truncated on construction.
- `RemoteEnvelope` gains `pub failure: Option<RemoteFailure>`, default `None`, with a `with_failure` builder and a `RemoteEnvelope::failure_reply(request_id, failure)` constructor whose metadata is `message_type_id = "rakka.remote.RemoteFailure"`, `schema_version = 1`, the protobuf codec id, and an empty payload. `ProtoRemoteEnvelope` gains `failure: Option<ProtoRemoteFailure>` at tag 9, with `code` at tag 1 and `message` at tag 2 inside. Existing tags and meanings do not move.
- `RemoteRequestError::Rejected { code, message }`. `TypedPendingRemoteReply::complete` checks `envelope.failure` before decoding: a failure completes the pending reply with `Err(Rejected)` and returns `Ok(())` to the endpoint.
- `RemoteEntityAskError::Reply { error: Rejected { .. } }` is what the asker sees. `code()` on the rejection returns the carried code.

`rakka-sharding/src/remote.rs`: once `RemoteEntityAskInbound::handle` knows the request id and the requester, every later failure (`EntityTypeMismatch`, `Decode`, `Delivery`) sends a failure reply to the requester through the transport. When that send succeeds, the handler logs the rejection at warn level with the code, entity type, and entity id, and returns `Ok(())`: the envelope was handled, the asker has its answer, and the requester's TCP session survives (hazard six). When the failure reply itself cannot be sent, the handler returns the original error as today, and the endpoint's failure path applies. The codes: `EntityDeliveryFailure::code()` and `EntityTellError::NoRoute → "entity-no-route"`, the same strings the HTTP and gRPC adapters emit; `"remote-ask-decode"` for a decode failure; `"remote-ask-entity-type-mismatch"` for the mismatch.

`rakka-a2a/src/router.rs:162-184` maps the new variant explicitly instead of through its catch-all: a `Rejected` whose code is one of `entity-no-route`, `entity-not-local`, `entity-shard-handoff`, `entity-mailbox-full`, `entity-shard-buffer-full`, `entity-mailbox-closed`, or `entity-spawn-failed` becomes `RakkaA2AHandlerError::Unavailable`, the retryable summary the compatibility doc commits to clients, because each of those says the request never entered a mailbox. Every other code becomes `OwnerAsk`. The carried code and message go into the summary's message text. The examples that match on `RemoteRequestError` (`clustered-counter-http`, `clustered-counter-grpc`, `clustered-agent-workflow-http-grpc`) gain a `Rejected` arm mapping to their entity-rejected code.

Rolling upgrade: the remote envelope version stays `DEFAULT_REMOTE_ENVELOPE_VERSION = 1`. The handshake fails closed on a version mismatch, so a bump would stop mixed-version nodes from talking at all, which is the wrong trade for an optional field. An asker on an older binary ignores tag 9, tries to decode an empty payload under an unknown type id, and gets `Reply { Decode }` instead of `Reply { Timeout }`. An owner on an older binary never sends the field; a newer asker then behaves exactly as today. The compatibility doc records the field, this behaviour, and the rule it rests on: an optional field with a new tag may be added to the remote envelope within an envelope version; the version moves only for a change the additive rules forbid.

### 5.4 The run actor recovers after a lost compare-and-set

`AgentStepRunner` (`rakka-agent-workflow/src/runner.rs`) and `DurableInbox` (`rakka-workflow/src/inbox.rs`) each gain a `stale: bool` and a public `is_stale()`. The flag is set when a compare-and-set fails with `DurableError::RevisionConflict`, at `persist_transition` and at `start` for the runner (a lost `Start` against `Revision::INITIAL` wedges the loser the same way) and at the four sites for the inbox through one shared helper. Any other persistence error leaves it alone. `recover()` clears it. `AgentRunInbox::is_stale()` forwards to the wrapped inbox.

`AgentRunActor::handle` (`rakka-agent-workflow/src/runtime.rs:841`) starts with:

```text
let stale = self.runner.is_stale() || self.inbox.is_stale();
let refreshed = if stale { self.recover_components().await.map(|_snapshot| ()) } else { Ok(()) };
```

Every command arm except `Recover` runs its operation only when `refreshed` is `Ok`, and otherwise replies with the recovery error, cloned (`AgentRunRuntimeError` is `Clone`). `Recover` always recovers. `recover_components` already records the `recover` snapshot result, so the recovery is observable where recovery already is. A recovery that fails leaves the flags set; the next command tries again.

What stays the same: the refused command's reply, its error type and code, `AgentRunActorSnapshot`, and every public signature. The effect bridge needs no change: its `Inbox` and `Outbox` errors wrap the inbox's own conflict, which sets the inbox's flag.

A direct user of `AgentStepRunner` or `DurableInbox` outside the actor can read `is_stale()` and call `recover()`; the components do not reload themselves inside a mutating call. That keeps a refused reply free of I/O and keeps the fix in one place.

### 5.5 Where the answer to #111 Ask 2 goes

`docs/rakka-akka-parity-phase-4-cluster-sharding.md` gains a section "One activation per entity is not guaranteed" stating:

- Each node reconciles ownership from its own membership view, and a region takes a gained shard without a handshake. During a join, a rebalance, or a transient membership disagreement, two nodes can briefly each own one shard and activate the same entity.
- The coordinator lease does not close that window. It gates who may register a region and make coordinator decisions, so it fits a single-coordinator topology; a node that does not hold it cannot host the entity type at all (`CoordinatorLeaseRejected`). Nothing carries the holder's ownership to other nodes.
- A shared coordinator store without a lease is not a supported topology. Per-node coordinators race on the store, and a node whose persist loses the compare-and-set keeps its in-memory ownership unpublished until membership changes again.
- The contract is the one `docs/rakka-v1-reliability-boundaries.md` already states: single-writer correctness rests on the revision compare-and-set and idempotent effects. Every durable write an entity makes must be conditioned on the revision it read. A write that takes a fresh revision unconditionally is outside the contract and can overwrite newer state during a join.
- Entity types that keep state in an actor must cope with two activations: recover from the store, and treat a refused compare-and-set as "reload and retry", which is what the run actor now does (section 5.4).

`docs/rakka-v1-reliability-boundaries.md` gets the same statement in the sharding section's non-guarantees, in one line, pointing at the sharding doc.

### 5.6 Docs, changelog, compatibility

- `docs/rakka-akka-parity-phase-4-cluster-sharding.md`: "Passivation And State" describes the hold across a stop, the one-timer-per-entity behaviour, the idempotent stop message rule, the "passivate through the facade" rule, and the remote ask rejection.
- `docs/rakka-v1-reliability-boundaries.md`: the sharding guarantees gain "a message delivered during an entity's passivation is handled by its next incarnation or refused with a typed error, never dropped" and the non-guarantees gain the join line above.
- `docs/rakka-compatibility.md`: the envelope's tag 9, the `rakka.remote.RemoteFailure` type id, the old-asker behaviour, and the new codes `remote-ask-decode` and `remote-ask-entity-type-mismatch`.
- `CHANGELOG.md`: Fixed entries for #112 H1, H2, H3 and the two extra hazards, Added for the remote rejection, Fixed for #111 Ask 1, Changed for the docs answer to Ask 2.
- Module docs of `rakka_agent_workflow::runtime` describe the stale recovery.

## 6. Proofs

Each hazard gets a deterministic test that fails on `8445ee4` and passes after, with the stress test as a secondary check only.

`rakka-sharding/tests/sharding_foundation.rs` (or a new `tests/entity_lifecycle.rs`):

- **H1 and H2, deterministic.** An entity whose `stopped` hook blocks on a gate the test holds. Deliver a message, let idle passivation queue the stop, wait until the actor is in its hook, send an ask, release the gate. The ask is answered by the next incarnation and `entity_count` ends at 1. Before the change this ask fails `SpawnFailed` or `ReplyDropped`.
- **Order.** Hold three messages during the gate; the next incarnation handles them in order and before a fourth sent after release.
- **The bound.** With mailbox capacity 2, a third held message returns `MailboxFull` with the message.
- **The fourth hazard.** An entity with mailbox capacity 1 whose handler blocks; the idle deadline passes; the entity stays registered and active; after the handler unblocks, the next deadline passivates it. Before the change, a later delivery fails `actor-path-in-use`.
- **H3.** 1 000 tells to one entity with a long idle timeout: `armed_idle_timers()` is 1. After the timeout: 0 and the entity is gone.
- **Explicit passivation without buffering.** An entity that calls the facade's `passivate_entity_id` on itself after each answer, region built `without_buffering`: two consecutive asks both succeed.
- **Shard transfer.** Entities of a lost shard enter `Stopping` and are gone after termination; a message held while a shard is re-acquired reaches the next incarnation.
- **Stress, secondary.** 10 000 tells across 50 entities with a 1 ms idle timeout and a shared handled counter: handled plus returned errors equals sent.
- **Self-stopping entity.** An entity that returns `ActorAction::Stop` with two more messages queued behind the one it handles: both arrive as dead letters with `MailboxClosed`; an ask sent after it left its loop is held and answered by the next incarnation.
- **Stopped-hook tell, core** (`rakka-core/tests/local_actor_runtime.rs`): a `tell` during `stopped` returns `Closed` and publishes a dead letter; `stop` during `stopped` returns `Ok`.
- **Remote rejection** (`rakka-sharding/tests/network_runtime.rs` and `sharding_foundation.rs` with the in-memory transport): an ask to an owner whose shard is `Draining` answers `Reply { Rejected { code: "entity-shard-handoff" } }` well within the timeout; over TCP the requester's next ask to the same owner reuses the connection, asserted through the transport's inbound-connection count staying at one; a success reply without tag 9 still decodes; a reply with tag 9 and an empty payload decodes as `Rejected` on the new asker; `rakka-a2a`'s router maps a handoff rejection to `Unavailable` and a decode rejection to `OwnerAsk`.
- **#111 Ask 1** (`rakka-agent-workflow/tests/step_runner.rs`, `rakka-workflow/tests/durable_inbox.rs`, `rakka-agent-workflow/tests/runtime_actor.rs`): two runners over one store, A writes, B's write is refused `RevisionConflict` and B `is_stale()`, and the same for a lost `start`; the same for the inbox through `accept_command`; two `AgentRunActor`s over one store where B's `StartGraphNode` is refused typed, B's next command succeeds, B's `Snapshot` after the refusal shows A's revision, and a deadline's `FailRun` on B after a refusal succeeds; a conflict through `ScheduleGraphNodeEffect` (the bridge's `Outbox` arm) also marks the inbox stale.

Validation: `scripts/validate.sh` green, per-crate runs for the sharding and agent-workflow crates where the workspace run is killed under load, `cargo clean` at the task boundary.

## 7. Left to the builder

- Exact names and placement of the slot type, the passivation outcome enum, and the helper functions; whether `held` lives in a `VecDeque<M>` or a small struct with a count.
- Whether `is_stopping` is exposed on `ActorRef` only or also on `ActorContext`.
- The message bound for `RemoteFailure` and how the HTTP and gRPC adapters map `RemoteRequestError::Rejected` (they already map `RemoteEntityAskError` somewhere; the builder keeps that mapping consistent with the carried code).
- Whether `RemoteEnvelope` struct literals exist outside `rakka-remote` (tests) that the new field breaks, and fixing them.
- Test helper shapes, including the gated `stopped` hook entity.
- Whether the watcher uses `when_terminated` or a new synchronous watch registration on `ActorRef`; the behaviour is fixed, the mechanism is not.

## 8. Report-back obligations

The host asked, per hazard and per ask: what changed, the commit, the tests, and any place where the code at `main` contradicted the brief. For #112 that includes the fourth, fifth, and sixth hazards above and the self-stop residual that `rakka-process` carries. For #111 that includes "two cached revisions, not three", the `start` site, the coordinator persist-conflict behaviour in shared-store mode, and the Ask 2 answer. Both reports go on the issues when the PR merges, with the pin to move to.
