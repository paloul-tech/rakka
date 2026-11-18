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

Four futures deeper for each attempt: 174 KiB of stack in a debug build,
13 KiB in a release build. Nothing bounds the budget above:
`AgentEffectSpec::validate` asks for at least one attempt. So the depth of
one claim was a number a deployment's configuration chose. On a 2 MiB
thread the ninth such attempt overflowed in a debug build and the 145th
in a release build, and a stack overflow aborts the process.

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
- Five functions return a `ClaimStep`: the four on the cycle
  (`attempt_invocation`, `settle_unknown_outcome`, `reconcile_ambiguous`,
  `retry_ambiguous`) and `recover_ambiguous`, which enters it.
- `execute_claim` makes the attempts in a loop until a step concludes.
- Four of the five no longer take the run's state, which they only passed
  down to the next attempt.
- `ClaimStep` is `#[must_use]`: an attempt handed back and then dropped
  would end a claim with no conclusion, and the compiler now refuses it.
- The box in `settle_unknown_outcome` is gone. It existed to give a
  recursive future a size.

Each attempt is made with what the nested call was given: the same claim,
the same intent, the run state read once when the claim began, and the
attempt number read from the row after the burn. The order of durable
writes is the same.

## 4. My calls

1. **A loop, and no bound.** With the loop the budget no longer sets a
   depth, which was the reason for a bound.
2. **The box is removed, not kept.** Without it `reconcile_ambiguous`'s
   future sits in `attempt_invocation`'s, which costs 2,592 bytes of stack
   under every attempt of every safety class in a debug build, and 5,600
   at the reconciler (the review measured both by address). Against that
   the loop took a recovery's copy of an attempt out of `execute_claim`'s
   future, so no path is deeper than on `main` by more than 704 bytes.
   That is not worth an allocation and a comment.
3. **The guard asserts the property, not a margin.** The executor records
   where the stack is at each call, and the test asserts that 64 attempts
   are all made within 4 KiB of one another. It fails as an assertion, in
   a debug build and in a release build, whatever the frames come to
   cost. The margin of a dispatch walk has its own guard (#162).

## 5. Measured

Debug build, aarch64 macOS, Rust 1.100.0. Smallest `RUST_MIN_STACK` that
passes. A thread's stack is a whole number of pages, and a page here is
16 KiB, so each figure is good to one page and a difference of 16 KiB
between two builds means only that their needs fall in adjacent pages.

| Test binary | `main` (`2a43cbb`) | This change |
|---|---|---|
| `dispatch_failure_outcomes` | 1,064 KiB | 712 KiB |
| `dispatch_finished_row` | 968 KiB | 888 KiB |
| `wait_invalidation` | 1,000 KiB | 1,000 KiB |
| `dispatch_owed_outcome` | 936 KiB | 936 KiB |
| the acceptance walk of `coordination-capability-acceptance` | 776 KiB | 776 KiB |

`dispatch_finished_row` drops because a recovery no longer holds an
attempt's future inside its own.

One claim, by the number of attempts it makes, each ending unknown and
found not executed. The review measured these, on a thread of its own:

| Attempts | `main`, debug | This change, debug | `main`, release | This change, release |
|---|---|---|---|---|
| 1 | 744 KiB | 728 KiB | 168 KiB | 152 KiB |
| 3 | 1,096 KiB | 728 KiB | 184 KiB | 152 KiB |
| 64 | 11,720 KiB | 728 KiB | 984 KiB | 152 KiB |
| 1,024 | not run | not run | 13,624 KiB | 152 KiB |

By the address of a local at each attempt, the nest was 178,464 bytes a
lap in a debug build, and the loop is at the same address at every
attempt.

## 6. Tests

`tests/dispatch_failure_outcomes.rs`:

- `a_claim_makes_every_attempt_of_a_large_budget_at_one_stack_depth`: a
  reconcileable effect of 64 attempts, each ending unknown and found not
  executed, in one pass. It asserts every attempt ran under the one claim,
  the row ended `Exhausted`, and the stack was at one depth at every call.
  Seen to fail first on `main`'s code, by name and not by an overflow:
  11,243,232 bytes deeper at the last attempt than at the first in a debug
  build (63 laps of 178,464), and 848,736 in a release build.
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

The first and the last also left one test of `dispatch_finished_row`
waiting for a worker that never came, and the binary was stopped by hand
to finish the run. That test's wait is bounded now, so such a break fails
it by name.

## 7. The review

A fresh reviewer read the commit and ran it against `main`.

**Nothing but depth changes.** It drove 337 scenarios through both
versions and compared, for each, every pass report or error, every
durable record, every segment, the metrics and the call counts: fresh
claims of several budgets with each reconciler answer at each query, the
worker killed at each window of a second attempt, a lease lapsing during
a call, another writer finishing the row, a lost worker's recovery under
each safety class, and four sweeps in which each write of a pass is lost
or loses its compare-and-set. With one wall-clock field masked the two
traces are byte for byte the same. Not in the trace, and read only: the
credential resolver, an authority refusal on a later attempt, a model
call's policy conflict, and a guardrail revision mismatch.

**The loop is bounded by the row's budget,** as the nest was: an attempt
is handed back only after a burn that did not exhaust the row.

What it found wrong, and what changed:

1. **The guard only guarded a debug build.** The first test ran 64
   attempts on a 2 MiB thread and waited for an overflow. In a release
   build the nest needs under half of that, so the test passed against the
   nested code there. Its name said more than it asserted, too. It is
   replaced by the review's own proposal (section 4, call 3).
2. **"To 8 KiB" and "16 KiB" claimed a precision the method does not
   have.** Section 5 says one page. The cost of the removed box was not
   16 KiB; section 4, call 2, has what the review measured by address.
3. **The changelog gave the debug threshold as if it were the exposure.**
   A deployment runs a release build, where it took 145 such attempts and
   not 9. Both are there now.
4. **"Nothing else changes" hid one thing,** which is the first item of
   section 8.
5. "The five functions on the cycle": four are on it. Section 3.
6. **A dropped `ClaimStep::Invoke` compiled clean.** `#[must_use]` added.
7. **Two of the four breaks hung a test binary** where they should have
   failed it. The wait in `dispatch_finished_row` is bounded.

## 8. Not done

- **A claim whose lease has lapsed goes on invoking for the rest of its
  budget.** Not new: `main` does the same, and the traces are identical.
  But on `main` the overflow was, in effect, the ceiling on how long one
  claim could run: 8 attempts in a debug build, 144 in a release build.
  With the loop a budget of any size runs to its end under one lease that
  nothing renews, with no pause between attempts. Once the lease has
  lapsed another worker may claim the ticket, find it not executed, and
  invoke beside this one. The review's proposal: before each attempt
  after the first, ask the fleet whether the claim is still current, and
  end the claim when it is not. That changes what a claim does, so it is
  left for a decision and not made here.
- **No bound on `max_attempts`.**
- **Attempts under one claim are not paced.** A re-invocation after a
  finding of not executed runs at once, as it did.
