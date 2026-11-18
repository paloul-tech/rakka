# The stack a dispatch walk needs in a debug build

Issue #162. A record of what was measured, what the cause was, the rule the
fix follows, what a review of it corrected, and what it found and left.

Every figure here is from a debug build on aarch64 macOS. Mine are from
Rust 1.99.1 and 1.100.0, which agree where both were run. The review's are
from Rust 1.100.0, and are marked as the review's.

## 1. What was wrong

`examples/coordination-capability-acceptance` walks a whole coordination
transcript on one thread. That walk needed 1,956 KiB of a test thread's
2 MiB stack (the review's figure, to 4 KiB; mine, to 64 KiB, was between
1,920 and 1,984). During #157 one async function added on the dispatch path
overflowed it.

## 2. Where the stack went

Measured by overflowing the walk on a smaller stack (`RUST_MIN_STACK`),
taking the crashed thread's call chain from the operating system's crash
report, and reading each function's frame size from its prologue. At the
point of overflow the frames summed to the stack, so the method accounts
for all of it.

The chain nests about twenty polls: the test, the walk, a dispatch pass, a
handoff executor, the in-process A2A service, the task entity, its
choreography host, the store. Depth was not the problem. These frames were:

| Frame | Before |
|---|---|
| the example's walk function | 613 KiB |
| `AgentTaskEntityStore::apply_once` | 308 KiB |
| `AgentTaskEntityStore::settle_side_effects` | 184 KiB |
| `RakkaAgentA2AService::send_message_normalized` | 123 KiB |
| the test function and its async block | 201 KiB |
| six frames of the dispatcher | 201 KiB |

## 3. Why

Two facts about a debug build:

- **A function's frame keeps stack for every future it awaits, about twice
  the future's size.** `settle_side_effects` awaits 14 futures that sum to
  88.8 KiB, and its frame was 183.8 KiB. `apply_once` awaits 21 that sum
  to 137.6 KiB, and its frame was 308.1 KiB. Why it is two and not one was
  not established.
- **A future holds every future below it inline.** So a large future low
  in a chain is carried by every future above it.

Together: one large future low in a chain is paid for at every await of
every function above it, twice.

The large future was the choreography host's. `AgentExchangeHost::accept`,
`settle`, `initiate` and the two `record_*` operations each hold the
participant's state three times across their one write: the record they
read, the copy they changed, and the copy handed to the store. A task's
state is 2.2 KiB, so each of those futures was 6.8 KiB
(`-Zprint-type-sizes`). Every method of the task entity that reaches the
host was 6.8 KiB larger for it.

## 4. Approaches

1. **Give the test a bigger stack.** Hides it. An application's debug build
   runs the same chain on a Tokio worker thread, which also has 2 MiB.
2. **Make the futures smaller where they are large:** stop holding the state
   three times. Right in principle and a real saving, but it is a rewrite of
   the five operations that every durable transition goes through.
3. **Box the future where it is made,** at the doors that many callers go
   through. The future is as large as before, on the heap; what the callers
   carry is a pointer.

Chosen: 3. Each box is a thin public function over the unchanged body, and
changes no behaviour.

## 5. What was boxed, and what each buys

"Lost" is how much more stack the walk needs with that group put back as it
was on main, the rest left boxed. The review measured it, to 4 KiB.

| Door | Lost without it |
|---|---|
| `AgentExchangeHost::accept`, `settle`, `initiate`, `record_unsettleable_refusal`, `record_delivery_failure` | 480 KiB |
| `RakkaAgentA2AService::send` and `send_message` | 192 KiB |
| the example's `run_acceptance` | 96 KiB |
| `AgentRunEffectDispatcher::pump_run` and `register_run` (the per-run half) | 80 KiB |
| `apply` on the agent, run, task, team and conversation entity stores | 16 KiB |

The five `apply` boxes buy the least on the walk. Measured on three more
test binaries, to 8 KiB, they are worth 32 KiB on `effect_dispatch`
(856 to 824), 32 on `private_memory_promotion` (840 to 808) and 16 on
`delegation_dispatch` (712 to 696). They are kept for the caller that sends
many commands: a run command's future is 5.3 KiB unboxed and 264 bytes
boxed, and some tests already box `run.apply(..)` by hand for that reason.

A hosted entity's command future does not hold the entity's state. The
host's operations hold it. Only `AgentEntityStore`, which has no host,
holds its record in the futures under `apply` (7.1 KiB).

## 6. Result

Smallest stack the walk passes with, as the boxes went in (mine, in 64 KiB
steps), and the review's own measurement of the same four states:

| | Passes with | Fails with | The review, to 4 KiB |
|---|---|---|---|
| Before | 1984 KiB | 1920 KiB | 1956 KiB |
| After the choreography host | 1280 KiB | 1216 KiB | 1236 KiB |
| After the entity, service and dispatcher doors | 896 KiB | 832 KiB | 868 KiB |
| After the walk's own door | 832 KiB | 768 KiB | 772 KiB |

To 8 KiB the walk now passes with 776 KiB, under both compilers. A release
build of it passes with 192 KiB. The review cross-built for x86_64 Linux
and ran it under emulation: 792 KiB.

The walk was not the only deep path, which the first version of this
record did not know. Other test binaries of `rakka-agent`, on main and now:

| Test binary | main | now | Measured by |
|---|---|---|---|
| `dispatch_failure_outcomes` | 1576 KiB | 1064 KiB | me, to 8 KiB |
| `wait_invalidation` | 1656 KiB | 1000 KiB | me, to 8 KiB |
| `dispatch_finished_row` | 1432 KiB | 968 KiB | me, to 8 KiB |
| `dispatch_owed_outcome` | 1528 KiB | 936 KiB | me, to 8 KiB |
| `effect_dispatch` | 1508 KiB | 820 KiB | the review, to 4 KiB |
| `private_memory_promotion` | 1476 KiB | 804 KiB | the review, to 4 KiB |
| `delegation_dispatch` | 1348 KiB | 692 KiB | the review, to 4 KiB |

Every test target in the workspace (333 binaries, all features, doctests
left out) was then run on a 1 MiB stack. One test overflowed, in
`dispatch_failure_outcomes`; section 10 is about it. On 896 KiB, the four
binaries at the top of the table did.

## 7. The guards

**`examples/coordination-capability-acceptance/tests/stack_margin.rs`**
runs the walk on a thread with 1 MiB, half a default stack. It fails by
overflowing, which names its thread, and its own doc says how to find what
grew. It holds one line for one path. The walk can grow by about 240 KiB
before it fails, and the review showed what that means: it fails with the
host's boxes removed, and stays green with any one of the other four
groups removed. It holds nothing on a path the walk does not take.

**`the_send_futures_stay_near_their_measured_size`**, in
`crates/rakka-a2a/src/agents/service.rs`, holds the size of what the two
request entries box: 10 KiB against a measured 9,136 bytes.
**`the_send_entries_box_what_they_send`**, in
`crates/rakka-a2a/tests/service_shape.rs`, holds the box: each entry's own
future is 40 bytes, and one that awaits its work in place fails.

The second pair replaces a guard this change had blinded; see section 11.

## 8. The rule

A future of kilobytes that many callers await is boxed at a function every
one of them goes through: a thin public function that awaits
`Box::pin(...)` of the unchanged body. The function that makes the box
still keeps stack for the future it boxes, so the box helps its callers
and not itself.

## 9. Not done

- The three copies of the state inside each host operation (approach 2).
- Frames that are large for what they hold by value, not for what they
  await: `attempt_invocation` 80 KiB, `send_message_normalized` 71 KiB,
  `execute_claim` 68 KiB, `apply_once` 59 KiB, and the example's own walk
  function, 205 KiB. Shrinking those means splitting functions.
- Doors of the same kind that the walk does not go through, and whose
  worth was not measured: `accept` and `settle_side_effects` on the entity
  stores; `team_command`, `conversation_command`, `manage_agent` and
  `cancel_task` on the A2A service; the request futures of `rakka-a2a`'s
  handler, 17 KiB, the largest left after the walk's own.
  `AgentEntityStore` is boxed at `apply`, above the functions that hold
  its record, so `apply`'s own frame is still 81 KiB.
- The tests that box `run.apply(..)` by hand now box a 264-byte wrapper.
  Harmless, and left.
- **A margin for every path.** Running the test step of validation with a
  reduced `RUST_MIN_STACK` would hold one line for every test at no cost in
  time. Two things stand in the way: the test in section 10 does not pass
  at 1 MiB, and `rustc` reads the same variable, so the step would have to
  build first and leave doctests out. It is the owner's call, and not
  made here.

## 10. Found, and not fixed here: one claim nests once per attempt

The deepest test left is
`an_unknown_outcome_the_reconciler_finds_not_executed_is_reinvoked_under_the_executors_code`.
Its profile shows the same four frames three times over:

```text
attempt_invocation      80.4 KiB
settle_unknown_outcome  16.0 KiB
reconcile_ambiguous     50.8 KiB
retry_ambiguous         27.0 KiB
```

That is 174 KiB a lap, and the test's effect has three attempts. When a
reconcileable effect's attempt ends with an unknown outcome and the
reconciler finds it not executed, `retry_ambiguous` invokes the next
attempt by calling `attempt_invocation`, under the same claim and inside
the attempt before it. The comment at the box in `settle_unknown_outcome`
says so: "Each lap spends an attempt first, so the depth is bounded by the
effect's budget."

Nothing bounds the budget. `AgentEffectSpec::validate` asks for at least
one attempt, and for exactly one of a non-idempotent effect. So a
reconcileable effect with N attempts, each ending unknown and found not
executed, nests N laps in one poll. From the measurement above that is
about 540 + 174 x N KiB in a debug build, which passes 2 MiB at nine
attempts. That figure is computed, not run, and a release build was not
measured.

It is not fixed here because the fix is a change to the claim executor's
control flow (the retry handing "again" back to the function that holds
the claim, a loop where there is a call), or a bound on the budget, or
both. This change is wrappers that alter no behaviour, and should stay
reviewable as that.

**Since fixed.** Issue #165 made the attempts of one claim a loop. Its
record is `2026-11-17-reconcile-retry-loop-design.md`.

## 11. The review

A fresh reviewer read the branch and measured its claims. It found no
change of behaviour: every `_unboxed` body is the old body, nothing runs
before the first poll, and the futures keep their auto traits. What it
found wrong, and what changed:

1. **A guard that could no longer fail.** `service_shape.rs` asserted that
   `send`'s future stayed under 20 KiB. Once `send` boxed its work that
   measured the box, 40 bytes, whatever the work held. Now two tests
   (section 7), each seen to fail: 4,600 bytes held across an await in a
   send leaf trips the first, and an entry that awaits in place trips the
   second. The first version of this record also said earlier fixes were
   made "without a measurement"; that one had been measured, and had left
   this guard.
2. **The recipe in the guard's doc did not sort by size.** Replaced, and
   run as written.
3. **"The deepest chain of polls in the repository" was not true** once
   the walk shrank. Sections 6 and 7 say what is measured, and the guard's
   doc says what it does not hold.
4. **A frame keeps each awaited future twice, not once,** and
   `settle_side_effects` awaits 14 futures, not "about 25". Section 3.
5. **"Within 64 KiB" was 92.** The texts say "within 100 KiB".
6. **The comment on the `apply` boxes said the future holds the entity's
   state.** It does only where there is no host. Section 5.
7. **"One allocation beside a durable write"** is not true of a replayed
   exchange, a refusal that writes nothing, or a request refused before it
   sends. The texts say one allocation a call.
8. Doors left unboxed, the hand-boxed test call sites and the coarseness
   of the walk's guard are in sections 7 and 9.
