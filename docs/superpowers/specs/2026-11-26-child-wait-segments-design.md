# A run's child wait is parked and resolved as spans that link (issue #70)

Design. Branch `child-wait-segments`, off `main` at `3c478460`.

## 1. Why

Specification 17.4 lists "checkpoint/timer park and later resume" among the
bounded trace segments an agent session is made of. 17.5 says that after a
long wait the runtime SHOULD start a new bounded segment and link it to the
span that parked the wait and to the span of whatever caused the resume
(timer, human, authorization, callback, A2A trigger). 17.11 makes the same
link a MUST for checkpoints.

**For checkpoints this holds today.** Three segments carry it:

- `checkpoint-open` closes on the commit that parks, under a durable span
  identity;
- `checkpoint-resolve` closes on the call whose transition committed the
  resolution, whatever the rest of the pass does;
- `run-resume` closes when the same command also leaves the run free.

Both of the last two link to the park (`parked-checkpoint`) and to the
resolving request (`resume-request`).

**For a run waiting on its children there is nothing like it:**

- **The park leaves no segment.** A turn rests on `AwaitingChildren` when its
  fan-in group is closed and unresolved (`turn_rest` in
  `crates/rakka-agent/src/run.rs`). No segment closes for that commit.
- **The end of the wait leaves nothing that links.** Three things end it:
  - a child's `DelegationResult` exchange, which reaches
    `AgentRunEntity::accept`;
  - `RecordWorkflowResult`, a workflow child's result;
  - `FireFanInDeadline`, the deadline timer.

  `run-resume` cannot carry the link. Since #191 it closes only when the
  command's pass leaves the run ended or held by nothing. The pass that
  resolves a group records the turn and goes on to the next model call, so
  the run ends that pass on its next wait. `workflow_tool.rs`
  (`a_command_that_rests_the_run_on_its_children_closes_no_resume_segment`)
  asserts exactly this. An exchange closes no `run-resume` in any case: the
  observability catalogue says "An exchange delivered to the run closes
  none".
- **Two of the three triggers carry no trace context.** The exchange's
  envelope carries the child's context. `RecordWorkflowResult` and
  `FireFanInDeadline` have no `telemetry` field, so a host's relay or
  scheduler span cannot be linked even when the host has one.

`docs/rakka-agent-telemetry-validation-matrix.md` records this as owed and
points to #70.

**A run never waits on a timer by itself.** It never enters
`AgentRunStatus::WaitingForTimer`; `query.rs` says so. The timers a run sees
act on another wait:

- the fan-in deadline ends a child wait;
- checkpoint deadlines escalate or expire a checkpoint park.

A checkpoint timer can also cause a child park. A turn that awaits its
children while a member's send waits on an approval rests on `AwaitingTools`.
If that approval expires, `FireCheckpointTimers` fails the send and rests the
turn on its children. For a run, then, "timer and child waits" means the child
wait and the timers that start or end one.

## 2. The owner's decisions (2026-11-26)

- **Scope: child waits only** (option A). Not model or tool waits, and not the
  task's wake wait between continuous-goal epochs. The wake wait would also
  need the task entity's `wake-admit` emission built first.
- **Identity: derived at both ends, stored nowhere** (approach 1 of section
  4). No durable field is added.
- **The link rides a resolution segment of its own** (option c). A
  `wait-resolve` segment, the child wait's counterpart to
  `checkpoint-resolve`, closes on the transition that resolves the group and
  carries the links. `run-resume` keeps #191's rule.

## 3. Goals, non-goals, anti-goals

**Goals:**

- A committed transition that parks a run on a closed, unresolved fan-in
  group closes one `wait` segment, exported under a derived durable span
  identity.
- A committed transition that resolves the group the run was parked on closes
  one `wait-resolve` segment, whatever the rest of the pass does. It links the
  park (`parked-wait`). It also links the trigger (`resume-request`) when the
  trigger carries a trace context.
- When `run-resume` closes, under its existing rule, for a command that found
  the run parked on a group, it carries the same two links.
- `RecordWorkflowResult` and `FireFanInDeadline` accept an optional trace
  context, wire-compatible both ways.

**Non-goals:**

- Model and tool waits (`AwaitingModel`, `AwaitingTools`) keep the links they
  have today.
- The handoff wait gets no `wait` or `wait-resolve`. `turn_rest` also rests a
  run on `AwaitingChildren` while a handoff awaits its target, but that is not
  a child wait.
- `run-resume` gets no change to when it closes. An exchange still closes no
  `run-resume`, and the catalogue sentence saying so stays true.
- A cancellation exchange, the task's wake wait, and a trace context on
  `FireCheckpointTimers` are out of scope.
- The assignment arm that records an envelope's context whatever the
  acceptance answered is left as it is (section 5.8).

**Anti-goals:**

- Any durable state, decision or status reading a segment. Segments are
  observability (17.1).
- A new durable field on the run, the loop state or the fan-in group.
- A `wait` or `wait-resolve` for a transition that did not park or resolve:
  a partial result, a replay, a duplicate or a refusal. A segment claims a
  transition that happened.
- A change to how many `run-resume` segments any existing test sees.

## 4. Approaches considered

### 4.1 Where the park's identity comes from

1. **Derive it at both ends; store nothing.** Chosen by the owner. It rests
   on two facts, checked at `3c478460`:
   - **The group stays on the record until it is replaced.** Only a later
     committed delegation replaces it, and a resolved group is absorbing. No
     delegation can be committed while the run waits on the group, because
     no turn runs. So whatever reads the group at or after its resolution
     finds the group the park was derived from.
   - **The run's context changes only when an `Assignment` is applied**
     (`record_telemetry` in run.rs, the `Assignment && has_telemetry` arm of
     the exchange participant). A child's result does not replace it.

     The arm records the envelope's context **whatever `accept_assignment`
     answered**. That includes an idempotent re-acceptance and a
     `run-generation-conflict` refusal. A re-drive resends the stored
     envelope, so its context is the one already held. A misrouted or
     mismatched `Assignment` carrying another context would rewrite it, and
     section 5.8 says what that costs.
2. **Store the identity on the fan-in group,** as `AgentCheckpoint::telemetry`
   does for a checkpoint park. It is robust if either fact stops holding, but
   it adds a durable field, a compatibility note and a rollback cost.
3. **Re-parent the resume under the park** with
   `agent_durable_resume_telemetry_context`. Rejected: the agent domain keeps
   one parent per activation and records causes as links.

### 4.2 Which segment carries the link

- **(c) A `wait-resolve` segment closed on the resolving transition.** Chosen
  by the owner. It mirrors `checkpoint-resolve`, leaves #191/#193's
  `run-resume` rule alone, and also covers a group that resolves while a
  reconciliation park still holds the run.
- **(a) Close `run-resume` whenever a result takes the run off its child
  wait.** It needs no new class, but it reverses #191's rule for child waits
  only.
- **(b) Keep the rule as it is.** The link would almost never appear.

## 5. The design

### 5.1 Two segment classes

- **`AgentSegmentOperation::Wait { kind: &'static str }`**, class label
  `wait`. The span name is `rakka.agent.run.wait`, kind `INTERNAL`.
- **`AgentSegmentOperation::WaitResolve { kind: &'static str }`**, class
  label `wait-resolve`. The span name is `rakka.agent.run.wait.resolve`, kind
  `INTERNAL`.

`kind` is a bounded vocabulary of one value in this slice, `children`. A later
slice can add `handoff`, `effect` or `wake` without a new class.

Attributes:

- **Both classes:** `rakka.agent.wait.kind`.
- **`wait`:** the group's policy label.
- **`wait-resolve`:** the group's bounded resolution code, the five codes
  `METRIC_AGENT_FAN_IN_RESOLUTIONS` already counts.

Reuse an existing attribute key where one already carries the value. Every new
key goes into `AGENT_SPAN_ATTRIBUTE_KEYS`, because the adapter drops a key
missing from that allowlist. Ids, member names and millisecond deadlines stay
out (17.6).

Widths:

- **`wait`** is zero-width at the parking commit's injected `now`, as
  `checkpoint-open` is. No span object is held across the wait (17.4, 17.11).
- **`wait-resolve`** measures the call that committed the resolution, the way
  `checkpoint-resolve` measures its own.

### 5.2 Where parks and resolutions are found

**The terms.** "Parked on G" means all three of:

- the phase is `AgentLoopPhase::AwaitingChildren`;
- `loop_state.awaits_fan_in()` holds: the group is closed and unresolved;
- the group's identity is G, its `(opened_turn, opened_at)`. This is the pair
  `fan_in_resolution` already treats as a group's identity.

**The rule.** For every committed transition T of the run, compare the state
before T with the state after it:

- **Park.** If the run was not parked on G before T and is parked on G after
  it, T closes `wait`.
- **Resolution.** If the run was parked on G before T and is not after it, T
  closes `wait-resolve`.

The rule covers every way a transition commits: each command arm's
transition, an exchange's acceptance, and each crank step. That includes the
`FireCheckpointTimers` arm (section 1), which today diffs nothing for
`checkpoint-open` either.

**Where it lives.** Put it where every committed transition passes, rather
than at each arm. The obvious places are around a command arm's transition
before its settle pass, around the exchange's acceptance in `accept`, and
around each `advance_once` commit in the crank. The builder confirms by search
that no other path commits a run transition. The `checkpoint-open` close is
neither moved nor widened in this slice.

**What closes nothing.** A replay answers from the operation log and commits
no transition, so it closes neither segment. A duplicate and a refusal close
neither either. So does a partial result that leaves the group unresolved, and
any transition that leaves the run parked on the same G.

**A resolution with no outside trigger.** A group can resolve inside a crank
step, when a member's send fails definitively. The step closes `wait-resolve`
with only the park link.

**A cancelling parent.** On a parent that is winding down, a resolution does
not move the phase. The group stops awaiting, though, so the run is no longer
parked on G and the transition closes `wait-resolve`. The wait ended, even
though the run does not go on.

### 5.3 The park's identity

`agent_durable_span_identity(run_context, &["wait", "children",
<opened_turn>, <opened_at>])`:

- `run_context` is `loop_state.telemetry()`;
- the two group fields are rendered as decimal strings.

The derivation lives in one function that the park and the resolution both
call, for example `AgentFanInCell::parked_span_identity(run_context)`. A run
with no trace context derives no identity. Its `wait` then exports under a
minted id, and its `wait-resolve` carries no `parked-wait`, as a checkpoint
with an empty effect context does today.

### 5.4 The resolution's links and context

- **Parent.** `wait-resolve` is a child of the run's context
  (`close_segment_under` with the loop telemetry), like every run segment.
- **`parked-wait`.** The link points at G's derived identity.
- **`resume-request`.** The link points at the trigger's context, when it has
  one. The trigger is the command or exchange whose own transition resolved
  G:
  - for a `DelegationResult`, its envelope's `telemetry()`;
  - for the two commands, their new `telemetry` field.

  A crank step has no trigger. The existing kind is reused, and its catalogue
  meaning widens to "the incoming request, exchange or timer that caused the
  resume" (17.5's list).
- **`run-resume`.** Its rule is unchanged. When it closes for a command that
  found the run parked on G, it adds the same two links, the way it adds the
  checkpoint's links today. In practice that is a cancelled parent's last
  child, or a turn that ends on its proposal.
- **A checkpoint resolution in the same call.** A command that resolves a
  checkpoint keeps its current links. If the builder finds a command that both
  resolves a checkpoint and resolves a group in its own transition, they raise
  it rather than guess.

### 5.5 The two commands' trace context

- **The field.** `AgentRunEntityCommand::RecordWorkflowResult` and
  `::FireFanInDeadline` each gain `#[serde(default)] telemetry:
  AgentTelemetryContext`, as `ResolveCheckpoint` gained one in gap slice 1.
  It is admitted through `sanitize_agent_telemetry_context` where it is read,
  and it is never persisted.
- **Constructors.** `record_workflow_result` keeps its signature and leaves
  the context empty. The builder adds a way to set the field, for example a
  `with_telemetry` method on the command.
- **Wire compatibility.** The command enum has no `deny_unknown_fields`, so an
  older build ignores the key, and a command written without it decodes to an
  empty context. A test holds the second with JSON literals.
- **Struct literals.** Every test and example that builds these variants as a
  literal is updated. Gap slice 1 hit 22 such sites.

### 5.6 The link vocabulary

- `LINK_KIND_PARKED_WAIT` = `parked-wait` joins `AGENT_TELEMETRY_LINK_KINDS`.
  The writer-bijection test in `crates/rakka-agent/tests/telemetry_context.rs`
  must see it written.
- `resume-request` gains a writer on `wait-resolve`.

### 5.7 What a deployment sees

- **Two new spans per child wait:** `rakka.agent.run.wait` at the park and
  `rakka.agent.run.wait.resolve` at the resolution.
- **`rakka.agent.run.resume` closes exactly as before.** When it closes for a
  command that ended a child wait, it now links the park and the trigger.
- **Nothing durable changes.** No durable field, status, decision kind or
  error code is added. Model, tool, handoff and checkpoint waits are
  unchanged.

### 5.8 Links that point at a span nobody exported

A `parked-wait` link can point at an id no span was exported under. It
happens when the park was committed:

- by a build from before this change;
- while no segment sink was wired;
- or under a run context that an `Assignment` later rewrote (section 4.1).

A tail sampler that drops the park span has the same effect, and does it
routinely. A link to an absent span is valid OpenTelemetry, and storing the
identity (approach 2) would remove only the first of these causes. The
catalogue and the CHANGELOG say so.

The assignment arm's write on a refusal is not fixed here: it is a durable
behaviour change of its own. The builder drafts an issue for it in `tmp/`, for
the owner to file or drop.

## 6. Proof

Integration tests go in `crates/rakka-agent/tests/`, one concern per file. A
new file (for example `child_wait_segments.rs`) drives runs through the
testkit's fan-in fixtures with an `InMemoryAgentSegmentSink`.

| Case | Asserted |
| --- | --- |
| A turn awaits two delegations under `All` | one `wait` (kind `children`), exported under the derived identity |
| The first child's result arrives | no `wait-resolve`, no second `wait` |
| The second child's result arrives with a trace context | one `wait-resolve`, linking `parked-wait` (equal to the `wait` span's exported id) and `resume-request` (the child's context). No `run-resume`, as today |
| The same, with the run passivated and re-materialized between park and result | the same park identity is linked |
| A duplicate or replayed result | no segment |
| `FireFanInDeadline`, with and without a context, ends the wait | `wait-resolve` links the park, and `resume-request` only when a context was given |
| `RecordWorkflowResult` ends a workflow child's wait | the same |
| A member's send fails definitively and resolves the group in a crank step | `wait-resolve` with `parked-wait` only |
| A group resolves while a reconciliation park holds the run | `wait-resolve` closes and links the park |
| An approval on a member's send expires (`FireCheckpointTimers`) and the turn rests on its children | one `wait` |
| A cancelled parent's last workflow child reports and the run ends | `wait-resolve`, and a `run-resume` carrying the same two links |
| A handoff wait | no `wait`, no `wait-resolve` |
| A run with no trace context | both segments close; `wait-resolve` carries no `parked-wait`; nothing fails |
| JSON for both commands without `telemetry` | decodes to an empty context |

**Measured before anything is written about the change.** Issue #191's
lesson applies. The builder logs every `run-resume`, `wait` and
`wait-resolve` the crate's tests close, before and after the change. The
`run-resume` tally must not move (section 3's last anti-goal). The CHANGELOG
states the measured `wait`/`wait-resolve` figures, not an estimate.

**Falsification.** Each guard is broken on purpose, then restored, and its
proof must fail:

- derive the resolution's identity from a different input (for example the
  turn plus one);
- skip the park detection on one path (the `FireCheckpointTimers` arm, or the
  crank);
- close `wait-resolve` without the "was parked on G before" check;
- close `wait-resolve` on a transition that leaves G unresolved.

## 7. Docs

- **`docs/rakka-agent-observability-catalogue.md`:**
  - the class table gains `wait` and `wait-resolve`;
  - the production call-site table lists both under the run entity;
  - the identity table gains the park row: "the run's context and the fan-in
    group's turn and timestamp; stored on nothing";
  - the link table gains `parked-wait` and widens `resume-request`;
  - the `run-resume` paragraph gains the sentence on the child-wait links;
  - section 5.8's note is added.
- **`docs/rakka-agent-telemetry-validation-matrix.md`:** the owed line that
  points to #70 becomes a met row naming the proofs.
- **`CHANGELOG.md`:** an Added entry (5.7, with the measured figures) and a
  note that the two commands' new field is optional and wire-compatible.
- **`docs/rakka-compatibility.md`:** an entry for the optional field, if that
  document records command-shape changes. The builder follows how earlier
  optional command fields were recorded.

## 8. Left to the builder

- Attributes beyond `rakka.agent.wait.kind`, within 5.1's bounds.
- Where the identity function lives (5.3).
- How the commands' context is set (5.5).
- Where exactly the before/after comparison sits, within 5.2's rule.
- The test file layout, one concern per file.
- Any exhaustive `AgentSegmentOperation` list the compiler or the mapping
  tests point to.
