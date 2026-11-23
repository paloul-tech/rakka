# The wind-down fence parks an idempotent call that may have executed (issue #206)

2026-11-22. Short design. The owner ruled on the issue's open question before
the build and approved this design. The owner reviews at the pull request.

## 1. What was wrong

The wind-down fence cancels a ticket "that provably never started" and tells
the run `Cancelled`. In `execute_claim` (`crates/rakka-agent/src/dispatch.rs`)
a row is treated as ambiguous only when it is `Dispatching`, or `Failed` for a
reconcileable effect. Every other row on a winding-down run, for a kind the
fence does not exempt, is cancelled.

An idempotent call's `Failed` row has attempts that started. The dispatcher's
own rule, `possibly_executed` in `attempt_invocation`, says that for an
idempotent effect "every non-first attempt follows a possibly-delivered one",
and a refusal of such an attempt parks the call instead of failing it. The
fence drew the opposite line for the same row. A row reaches the fence this way
by three routes:

1. **A lost attempt that was counted.** A worker dies after `Started`; the
   next claim counts the loss (`retry_ambiguous`, row `Failed, 1`) and then
   ends before its retry: it crashes, or (since #196) its authority answers
   after the lease and it ends out of lease. The run is then cancelled.
2. **An ordinary failed attempt.** The executor returns a retryable failure
   (row `Failed, 1`), and the run is cancelled before the retry. The row
   keeps only its last failure line, and `timed_out` is not stored, so this
   cannot be told apart from route 1.
3. **The fleet's own cancellation.** `AgentDispatcherWorker::cancel_run_dispatches`
   cancels the outbox rows of unleased entries itself (`Failed, 1` becomes
   `Cancelled, 1`), and the per-run sweep then tells the winding-down run
   `Cancelled`. rakka.host does not call it (its `docs/facts/pin-2a43cbb.md`).

What follows a `Cancelled` delegation send, handoff send or workflow start is
worse than the evidence: its cell is settled `Failed { run-winding-down }`
("the winding-down parent never spawned the child", `run.rs`), and the
wind-down chases only `ChildCreated` / `Started` cells with a cancel. A child
the earlier attempt created is never cancelled. Both kinds are idempotent by
default.

Specification 8 (`docs/plans/rakka-agent/spec.md`, "If any started
consequential effect has an unknowable outcome...") requires such a run to
stay `WaitingForReconciliation` until the outcome is known or decided.

## 2. The owner's ruling (2026-11-22)

**Park for a decision.** A winding-down run's idempotent call with at least
one started attempt is parked `Indeterminate`, with a reconciliation
checkpoint, charged the attempts that started, instead of being cancelled.
Nothing is invoked after the cancel.

Rejected: retrying with the same key (what the dispatcher does for an
idempotent `Dispatching` row on a winding-down run; after an ordinary failure
the retry may be the first real execution, made after the cancel), and keeping
the cancel with a documented caveat (short of specification 8).

## 3. The change

**One predicate.** A function in `dispatch.rs`, "an earlier attempt of this
call may have executed": the intent is `Idempotent` and at least one of its
attempts has started. `attempt_invocation`'s `possibly_executed` becomes a
call to it (`started = attempt - 1`, so `attempt > 1` is unchanged), and the
three places below call it, so the refusal's line and the fence's cannot drift
apart.

**Three places that tell a winding-down run its call was cancelled**, each
changed to park where the predicate holds:

| Place | Today | After |
|---|---|---|
| The claim's fence, `execute_claim`'s wind-down branch | cancels the ticket, tells `Cancelled` with the row's count | where the predicate holds on the row's count: `park_indeterminate` with that count; the ticket ends as every park's does (row and fleet entry cancelled `indeterminate`) |
| `owed_by_finished_row`, the reader of a finished row, used by `conclude_on_untold_row` | a `Cancelled` row under the fence is `NeverStarted` | for an idempotent row that counts an attempt: `Unknown` under the new code |
| The per-run sweep's arm for a fenced `Cancelled` row (`settle_waited_effects`) | reads only the count from `owed_by_finished_row`, delivers `Cancelled` itself | delivers the verdict through `tell_owed`, so the same row reads the same from either side |

The second and third cover route 3 and a row an earlier build's fence
cancelled and died before telling the run.

**The new stable code:** `run-cancelled-after-attempt`, as the effect's
`last_error_code` and the park segment's error. The park's message is the
existing `INDETERMINATE_OUTCOME_MESSAGE` ("the attempt's outcome could not be
established; an explicit reconciliation decision is owed"), which `tell_owed`
already uses.

## 4. Calls made in the design (each the owner's to reverse)

1. **The lost-worker rule is unchanged.** An idempotent `Dispatching` row on
   a winding-down run still retries with the same key
   (`recover_ambiguous`, documented at `docs/rakka-compatibility.md:235`). So
   an attempt lost after `Started` retries after the cancel if no claim
   counted it first, and parks if one did. Both satisfy specification 8 (the
   outcome is learned, or decided). The issue did not ask about it.
2. **No lease check before the fence's park**, as there is none before the
   fence's cancel: nothing is asked between the claim's read of the row and
   the decision.
3. **No repair** of a run already told `Cancelled`: its effect is resolved,
   and nothing durable tells it from a call that never started.
4. **The other classes are unchanged.** Read-only: cancelled, as nothing
   consequential is abandoned. Reconcileable: its `Failed` row is already
   ambiguous and asks the reconciler. Non-idempotent: its spec is held to one
   attempt (`effect.rs`), so it never has a `Failed` row with a retry left.
   The kinds the fence exempts are untouched.
5. **A fenced `Cancelled` row of an idempotent call reads as a park whatever
   wrote it.** After this change nothing in this build writes such a row
   before telling the run (the claim's fence parks, a park and a refusal tell
   the run first), so the readers' rule reaches only route 3 and an earlier
   build's row.

## 5. What a deployment sees

- A cancelled run holding such a call rests `WaitingForReconciliation`, with
  an `IndeterminateEffectReconciliation` checkpoint, where it ended
  `Cancelled`. `ResolveCheckpoint` / `ResolveIndeterminateEffect` answer it,
  as any park.
- The call reads `Indeterminate`, `last_error_code` `run-cancelled-after-attempt`,
  charged the attempts that started (the row's count), as the fence's cancel
  was (issue #182).
- `AgentDispatchPass::parked_indeterminate` counts one where `cancelled` did.
  The fleet entry's `last_error_code` is `indeterminate`, where it was
  `run-cancelled`.
- A delegation send, handoff send or workflow start stays pending until the
  decision; a decision of "executed" lets the run's usual cancel chase the
  child (held by a test).
- #182's test `a_call_the_fence_cancels_before_its_retry_is_charged_the_attempt_it_made`
  (`effect_dispatch.rs`) is turned around: the call is parked, charged one.

## 6. Mixed builds and rollback

Nothing durable is added; the code is a new string in an existing field. An
earlier-build worker that claims the ticket cancels it as before. Rolled back,
a parked call stays parked, and an earlier build resolves it as any park.

## 7. Tests

A new file, `crates/rakka-agent/tests/wind_down_possibly_executed.rs`, each
reading the run's status, the call's status, code and charge, the pass's
counts and the tool's invocation count:

- Route 1, as the issue reproduces it: a lost attempt counted by a claim whose
  refusal comes after the lease, then a cancel.
- Route 1 with a worker that dies right after the count.
- Route 2: an ordinary failure, then a cancel (the turned-around #182 test
  stays in `effect_dispatch.rs`).
- Route 3: `cancel_run_dispatches` after a failed attempt, then a pump.
- An earlier build's row: a `Cancelled, 1` row with a live fleet entry and an
  untold run (the claim's replay), and with a finished entry (the sweep).
- A workflow start or delegation send through the park and a decision of
  "executed", to the chase.
- Guards: a read-only call with a failed attempt is still cancelled; an
  idempotent call with no attempt is still cancelled with none charged.

Each fails on `main` except the guards. The breaks to run one at a time: each
of the three places restored, the predicate's `>= 1` moved, and the refusal
site's call to the predicate.

## 8. Documents

`docs/rakka-agents.md` (the dispatch rules and the fence), a
`docs/rakka-compatibility.md` entry (what changed, what a deployment sees, the
new stable code), `CHANGELOG.md`, and the `dispatch.rs` module and fence
comments that say "provably never started".
