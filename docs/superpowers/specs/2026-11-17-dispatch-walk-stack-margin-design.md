# The stack a dispatch walk needs in a debug build

Issue #162. A record of what was measured, what the cause was, and the rule
the fix follows, because the same thing has been fixed by hand three times
before without anyone measuring it.

## 1. What was wrong

`examples/coordination-capability-acceptance` walks a whole coordination
transcript on one thread. In a debug build that walk needed between 1920 and
1984 KiB of a test thread's 2 MiB stack. During #157 one async function
added on the dispatch path overflowed it.

## 2. Where the stack went

Measured by overflowing the walk on a smaller stack (`RUST_MIN_STACK`),
taking the crashed thread's call chain from the operating system's crash
report, and reading each function's frame size from its prologue. At the
point of overflow the frames summed to the stack, so the method accounts
for all of it.

The chain is 20 polls deep: the test, the walk, a dispatch pass, a handoff
executor, the in-process A2A service, the task entity, its choreography
host, the store. Depth was not the problem. Six frames were:

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

- **A function's frame keeps a slot for every future it awaits.** The future
  is built in the frame and then moved into the function's own state, and
  a debug build does not reuse the slot. A function that awaits forty
  futures has forty slots.
- **A future holds every future below it inline.** So a large future low in
  a chain is carried by every future above it.

Together: one large future low in a chain is paid for at every await site
of every function above it.

The large future was the choreography host's. `AgentExchangeHost::accept`,
`settle`, `initiate` and the two `record_*` operations each hold the
participant's state three times across their one write: the record they
read, the copy they changed, and the copy handed to the store. A task's
state is 2.2 KiB, so each of those futures was 6.8 KiB
(`-Zprint-type-sizes`). Every method of every entity store awaits one, and
was 6.8 KiB larger for it. `settle_side_effects` awaits about 25 such
methods: 184 KiB.

## 4. Approaches

1. **Give the test a bigger stack.** Hides it. An application's debug build
   runs the same chain on a Tokio worker thread, which also has 2 MiB.
2. **Make the futures smaller where they are large:** stop holding the state
   three times. Right in principle and a real saving, but it is a rewrite of
   the five operations that every durable transition goes through.
3. **Box the future where it is made,** at the doors that many callers go
   through. The future is as large as before, on the heap; what the callers
   carry is a pointer.

Chosen: 3. Each box is two lines beside a durable write, and changes no
behaviour.

## 5. What was boxed

Each is a thin public function that boxes the unchanged body.

| Door | Why there |
|---|---|
| `AgentExchangeHost::accept`, `settle`, `initiate`, `record_unsettleable_refusal`, `record_delivery_failure` | The root: they hold the entity state. Every entity operation awaits them |
| `apply` on the agent, run, task, team and conversation entity stores | An entity command's one door. Every sender awaits it |
| `RakkaAgentA2AService::send` and `send_message` | A request's one door. A handoff inside a dispatch pass calls it in-process |
| `AgentRunEffectDispatcher::pump_run` and `register_run` (the per-run half) | A caller that pumps many runs awaits one for each |
| the example's `run_acceptance` | The walk's own future is 34 KiB |

## 6. Result

Smallest stack the walk passes with, debug build, Rust 1.100.0, aarch64
macOS, in 64 KiB steps:

| | Passes with | Fails with |
|---|---|---|
| Before | 1984 KiB | 1920 KiB |
| After the choreography host | 1280 KiB | 1216 KiB |
| After the entity, service and dispatcher doors | 896 KiB | 832 KiB |
| After the walk's own door | 832 KiB | 768 KiB |

What is left is not futures. The example's walk function is still 205 KiB
of frame, and `attempt_invocation`, `execute_claim`,
`send_message_normalized` and `apply_once` are 60 to 80 KiB each: values
held by value in long functions, which a debug build also gives a slot
each. Shrinking those means splitting functions, and is not done here.

## 7. The guard

`examples/coordination-capability-acceptance/tests/stack_margin.rs` runs
the walk on a thread with 1 MiB, half a default stack. It fails by
overflowing, which names its thread, and its own doc says how to find what
grew. A change that eats the margin fails there while a default stack still
has its other half.

## 8. The rule

A future that holds an entity's state, or a whole request, is boxed where
it is made: `Box::pin(...).await` at the lowest function that holds it.

## 9. Not done

- The three copies of the state inside each host operation (approach 2).
- The by-value structs in the long functions of the dispatcher, the A2A
  service and the task entity.
- Other platforms were not measured before the change. CI runs x86_64
  Linux on a floating stable toolchain.
