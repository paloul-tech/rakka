# A run records its trace context only when its assignment creates it (issue #224)

A short design: a small hardening change.

## Why

A run's trace context lives in its loop state (`AgentLoopState::telemetry`).
Every segment the run closes reads it, and so does every derived span
identity, such as a child wait's park identity
(`AgentFanInCell::parked_span_identity`). It is written in one place outside
tests, at the end of the run participant's `apply`
(`crates/rakka-agent/src/run.rs:8100`). That write runs for every
`Assignment` exchange that carries a context, whatever `accept_assignment`
answered:

- **A refused generation** (`run-generation-conflict`, `run.rs:2011`) makes
  the refused message's context the run's.
- **The same generation again** (`run.rs:2018`) is answered with the original
  acceptance, and its context replaces the run's.

Specification 17.5 says an entity never invents a context. It should not
adopt one it refused, or one from a message it had already acted on.

**Neither path is reachable through upstream's own task entity.**
- A run's id is `{task}-gen-{generation}` (`task.rs:4941`), so an assignment
  for another generation addresses another run. `run-generation-conflict`
  needs an envelope the task never builds; the one test that reaches it
  edits the record (`tests/goal_view.rs:889`).
- A re-driven assignment is the stored envelope, unchanged
  (`choreography.rs`, `drive_pending_exchanges`), so a replay carries the
  original context.
- rakka.host builds no assignment exchanges.

## The owner's decisions (2026-11-27)

- **Build it as hardening,** rather than closing #224 as not reachable. The
  rule then holds by construction, not by how the task happens to build its
  assignments today.
- **Record the context where the run is created.**

## Goals, non-goals, anti-goals

- **Goal:** a run's trace context is the context of the assignment that
  created it, and no later exchange changes it.
- **Non-goals:**
  - records written before this change keep the context they hold;
  - no other exchange starts recording a context (a child's delegation result
    still links into the parent's spans rather than replacing its chain,
    specification 17.4).
- **Anti-goal:** changing which runs are admitted. The size check that admits
  a run must answer exactly as today.

## Approaches

| Approach | Trade-off | |
| --- | --- | --- |
| **A. Record the context in `accept_assignment`'s creating branch** | the rule sits beside the one event it belongs to; a refusal or a replay never reaches it | **chosen** |
| B. Keep the write at the end of `apply`, but only when this call created the run | same behaviour, but needs a before-and-after comparison, and keeps the rule apart from the creation | rejected |
| C. Let a replay fill in a context when the run has none | adopts a later message's context mid-run, which is what #224 removes, and a replay is the stored envelope anyway | rejected |

## The design

- **`accept_assignment`'s creating branch** records the envelope's context into
  the new run's loop state, through `AgentLoopState::record_telemetry`, when
  the envelope carries one (`has_telemetry`). It does so after the run passes
  its size check, as today's write does, so admission is unchanged (my call:
  recording it before the check would count the context against the reserve,
  and could refuse a run that is admitted today).
- **The write at the end of `apply` is removed.** Its comment, which states
  the rule and its specification references, moves to the new site.
- **A run created by an assignment with no context stays without one.** No
  later exchange fills it in.
- **Nothing durable changes shape.** No field, schema version or error code is
  added.

## Proof

| Case | Asserted |
| --- | --- |
| **A refused generation.** A run accepts generation 1 under context A. A hand-built generation-2 assignment under context B is refused `run-generation-conflict` | the run's `loop_state().telemetry()` is still A |
| **The same generation re-sent past the journal window** under context B | answered with the original acceptance; the context is still A |
| **A child wait across a refusal.** A wait parked under A; a refused assignment under B; then the group resolves | `wait-resolve`'s `parked-wait` link names the span the `wait` segment exported |
| **The first acceptance** (trace scenario 23, `tests/trace_scenarios.rs:85`) | still records the assignment's context, unchanged |

**Falsification:** restore the write at the end of `apply`; the first three
cases must fail, and the fourth must still pass.

## Docs

- **`CHANGELOG.md` (Unreleased):** an entry under `### Fixed`, saying the paths
  were not reachable through upstream's own task entity.
- **`docs/rakka-compatibility.md`:** an entry; mixed builds read and write the
  same rows.
- **`docs/rakka-agent-observability-catalogue.md:351`:** the "under a run
  context that a later `Assignment` rewrote" cause of an unexported
  `parked-wait` link applies only to records written before this change.

## Left to the builder

- How the tests build their envelopes: decode the original assignment
  (`AgentRunAssignment`, public), change its generation or its context,
  re-encode, and deliver it under a new operation id; for the replay, the
  original operation id after the journal's applied record is removed, as
  `child_wait_segments.rs::forget_exchanges` does.
- Where the first two tests live. The third extends
  `tests/child_wait_segments.rs`.

## As built

Commits `08a6cb18` (the change and its tests) and the docs commit on top.

- **The change.** `accept_assignment`'s creating branch records the envelope's
  context, when it carries one, after `run.check_bounds(..)` and before the
  run is stored. The write at the end of `apply` is gone, with a one-line
  comment left in its place pointing to the new site.
- **The tests' assignments** come from a new `tests/common` helper,
  `assignment_envelope(fixture, generation)`, addressed to the generation-1
  run whatever the generation. `forget_exchanges` moved to `tests/common`
  unchanged, and `run_context` reads the run's context from a recovered
  facade.

**Calls made while building:**
- **A run created without a context** got its own test,
  `a_run_created_without_a_context_takes_none_from_a_later_assignment`. It
  covers both a repeat and a refused generation.
- **The replay test** asserts the reply was applied, not answered from the
  journal. The falsification below shows the transition ran: restoring the
  old write fails it.
- **The catalogue's last sentence** said storing the park identity "would
  remove only the first" cause. That was wrong before this change, since
  storing would have removed the rewrite cause, which is not the first. With
  this change it would remove none of the causes for a park committed now,
  and the sentence now says so.
- **The compatibility doc's issue #70 entry** stated the same "rewrote" cause
  and is narrowed the same way. The released 0.2.0 changelog text is left as
  it was.

**The proof rows and their tests:**

| Row | Test |
| --- | --- |
| A refused generation | `assignment_context.rs::a_refused_generation_leaves_the_runs_trace_context` |
| The same generation re-sent past the journal window | `the_same_generation_sent_again_leaves_the_runs_trace_context` |
| A run created without a context | `a_run_created_without_a_context_takes_none_from_a_later_assignment` |
| A child wait across a refusal | `child_wait_segments.rs::a_refused_assignment_between_the_park_and_the_resolution_keeps_the_park_link` |
| The first acceptance | `trace_scenarios.rs::ingress_context_flows_to_every_effect_and_survives_owner_loss`, unchanged |

**Red, then falsification:**
- On the code before the change, all four new tests failed on their context
  assertions: the run took the other message's context, and the
  `wait-resolve` link named a span nobody exported. The 12 existing
  child-wait tests passed.
- Restoring the old write fails exactly the four new tests, and trace
  scenario 23 still passes.
- Removing the new write, so that nothing records a context, fails scenario
  23 and nine other traced tests.
