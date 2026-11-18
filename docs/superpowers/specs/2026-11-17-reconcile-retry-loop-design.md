# One claim's attempts, one after another

Issue #165. Found while measuring stack use for #162, whose record
(`2026-11-17-dispatch-walk-stack-margin-design.md`, section 10) has the
profile.

## 1. What was wrong

A reconcileable effect's attempt ends with an unknown outcome. The
dispatcher asks the reconciler in the same pass. A finding of
`NotExecuted` burns the attempt, and if the budget permits another, the
next attempt runs now, under the same claim.

It ran *inside* the attempt before it:

```text
attempt_invocation        the attempt
  settle_unknown_outcome  its outcome is unknown
    reconcile_ambiguous   the reconciler finds it not executed
      retry_ambiguous     the attempt is burned, the budget permits another
        attempt_invocation
          ...
```

Four futures deeper for each attempt, 174 KiB of stack in a debug build.
Nothing bounds the budget above: `AgentEffectSpec::validate` asks for at
least one attempt. So the depth of one claim was a number a deployment's
configuration chose.

## 2. Approaches

1. **A loop.** The function that decides a retry hands the next attempt
   back, and the function that holds the claim makes it. Depth does not
   grow with attempts. No behaviour changes.
2. **An upper bound on `max_attempts`.** The depth stays, with a ceiling.
   It is a compatibility decision: a stored spec above the bound needs an
   answer, and the number has to be chosen for the smallest stack a
   deployment runs on.
3. **Both.**

Chosen: 1.

## 3. The change

`crates/rakka-agent/src/dispatch.rs`.

- `ClaimStep` is how one step of a claim ends: `Concluded` with the
  claim's conclusion, or `Invoke { claim, attempt }` with another attempt
  to make.
- `retry_ambiguous` returns `Invoke` where it called `attempt_invocation`.
- The five functions on the cycle return a `ClaimStep`:
  `attempt_invocation`, `settle_unknown_outcome`, `reconcile_ambiguous`,
  `retry_ambiguous` and `recover_ambiguous`.
- `execute_claim` makes the attempts in a loop until a step concludes.
- Four of the five no longer take the run's state, which they only passed
  down to the next attempt.
- The box in `settle_unknown_outcome` is gone. It existed to give a
  recursive future a size.

Each attempt is made with what the nested call was given: the same claim,
the same intent, the run state read once when the claim began, and the
attempt number read from the row after the burn. The order of durable
writes is the same.

## 4. My calls

1. **A loop, and no bound.** With the loop the budget no longer sets a
   depth, which was the reason for a bound.
2. **The box is removed, not kept.** Measured both ways: keeping it saves
   16 KiB on the path of a reconcileable effect's unknown outcome (696 KiB
   against 712 KiB for that test binary) and nothing on the other three
   binaries measured. That is one allocation and a comment for 16 KiB on a
   path that is not the deepest.
3. **The guard is a thread of 2 MiB and a budget of 64,** not a tighter
   stack. It holds that a claim does not nest. The margin of a dispatch
   walk has its own guard (#162).

## 5. Measured

Debug build, aarch64 macOS, Rust 1.100.0. Smallest `RUST_MIN_STACK` that
passes, to 8 KiB.

| Test binary | `main` (`2a43cbb`) | This change |
|---|---|---|
| `dispatch_failure_outcomes` | 1,064 KiB | 712 KiB |
| `dispatch_finished_row` | 968 KiB | 888 KiB |
| `wait_invalidation` | 1,000 KiB | 1,000 KiB |
| `dispatch_owed_outcome` | 936 KiB | 936 KiB |
| the acceptance walk of `coordination-capability-acceptance` | 776 KiB | 776 KiB |

`dispatch_finished_row` drops because a recovery no longer holds an
attempt's future inside its own.

The new test's claim of 64 attempts passes on a thread of 768 KiB and
overflows one of 704 KiB: what the three-attempt test needs. On `main` it
overflows a thread of 2 MiB; the 11 MiB it would need there is computed
from the profile (about 540 + 174 x N KiB), not run.

A release build was not measured, before or after.

## 6. Tests

`tests/dispatch_failure_outcomes.rs`:

- `a_claim_that_spends_a_large_budget_needs_no_more_stack_than_one_attempt`:
  a reconcileable effect of 64 attempts, each ending unknown and found not
  executed, in one pass on a thread of 2 MiB. It asserts every attempt ran
  under the one claim and the row ended `Exhausted`. Seen to fail first: on
  `main` it overflows that thread and the process aborts, naming it.
- The whole `rakka-agent` suite passes unchanged, among it the
  three-attempt re-invocation, the findings of `Executed` and `Unknown`,
  and a lost worker's recovery.

The loop was then broken on purpose, four ways, and each break failed
tests:

| Break | Tests that failed |
|---|---|
| a retry concludes the claim and hands no attempt back | 15 |
| the loop makes one attempt and stops | 2 |
| the attempt handed back is numbered one too high | 4 |
| the attempt a lost worker's recovery hands back is dropped | 13 |

The first and the last also leave one test of `dispatch_finished_row`
waiting for a worker that never comes. That binary was stopped by hand to
finish the run.

## 7. Not done

- **No bound on `max_attempts`.**
- **Attempts under one claim are not paced.** A re-invocation after a
  finding of not executed runs at once, as it did, and a budget of N can
  make N invocations in one pass under one lease. This change does not
  touch that.
