# Continuous-goal acceptance (M3)

The executable acceptance statement of the continuous-goal milestone
(`docs/plans/rakka-agent/spec.md`, "Continuous Goal Milestone (M3)"): one
stable goal and root control task that stays logically active while fully
passivated, admits finite epochs only from versioned, deduplicated durable
wake occurrences, coalesces instead of overlapping, survives pod loss and
shard movement mid-flow, defers on an exhausted goal window and retries
itself with durable controller-originated re-wakes, backs off on failure and
escalates into suspension, is fenced against stale schedule revisions and
stale owners, renews, retires, and answers the authoritative operational
query from durable state alone.

Everything is in-process and deterministic: durable in-memory stores, the
real wake scanner over the real durable wake-timer index, entity stores
rebuilt from durable state at every step (every step is already a restart),
crash injection on the durable writes, and the deterministic model adapter
for the epoch that runs end to end.

```sh
cargo run -p rakka-example-continuous-goal-acceptance
```

`tests/acceptance.rs` runs the same walk and asserts the transcript below
verbatim — the README, the `EXPECTED_TRANSCRIPT` const, and the binary's
stdout are one source.

## Expected stdout

```text
ok  1/16 the continuous root is durable and passivatable: controller state persisted, no resident actor, loop, or timer
ok  2/16 a scheduled occurrence admitted one derived epoch task and run; the epoch ran to completion and its result released the occurrence
ok  3/16 the replayed admission answered Duplicate from the durable record: one admission, one epoch
ok  4/16 overlap is forbidden: the next occurrence admitted and the one behind it coalesced durably
ok  5/16 the owner died mid-settlement; the rebuilt owner replayed the same exchange and converged: one release, one promotion
ok  6/16 a downtime backlog of 3 missed occurrences admitted one coalesced representative and absorbed 2 as missed
ok  7/16 the failed epoch backed off; the durable backoff re-wake retried and admitted the occurrence parked behind it
ok  8/16 a second consecutive failure escalated: the goal auto-suspended durably
ok  9/16 the stale-revision resume was fenced with wake-stale-lifecycle-revision; the current-revision resume reactivated the goal and cleared the backoff
ok 10/16 the schedule update to revision 2 took the windowed policy into force and fenced a stale revision-1 delivery terminally
ok 11/16 the exhausted goal window deferred the next occurrence and parked a durable window-turn re-wake at the boundary
ok 12/16 the window turned: the parked re-wake's scan promoted the deferred occurrence; the ledger survived every rebuild
ok 13/16 the renewal extended expiry to 50000000; a stale former owner's write was fenced with revision-conflict and re-recovery converged
ok 14/16 each epoch is its own derived finite task and run with a bounded occurrence input; continuity lives only in the root's durable controller
ok 15/16 the goal retired after its 9th admitted occurrence; a later delivery was barred and its entry marked terminal
ok 16/16 the operational query answered from durable state alone: schedule revision 2, lifecycle retired, 9 admitted, 2 missed, 1 coalesced, 1 fenced, no pending wake
```
