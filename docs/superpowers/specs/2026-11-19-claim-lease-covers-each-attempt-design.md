# A claim's later attempts need a lease that covers them (issue #171)

Design and record, 2026-11-19. Branch `claim-current-before-each-attempt`,
off `main` at `321f218`. The brief is the rakka.host's.

## 1. What was asked

One dispatch claim makes every attempt a reconcileable effect's budget
permits, one after another, for as long as each ends unknown and the
reconciler finds it not executed. Nothing renews the lease of the claim
being run, and the claim is checked once, before it starts. So a budget
whose attempts together outlast the lease is spent to its end under a
claim whose lease is over.

The brief asks that, before each attempt after the first, the claim ask
whether it is still current and end when it is not. It asks one question:
should a claim that is still current renew its own lease? And it says what
it needs from either answer: no two workers invoking at once, and the
attempts already made still counting against the budget.

The brief was read from the code, not run. Two things were measured here.

**Two workers do invoke at once.** Four attempts of 25 s under a 60 s
lease, with a second worker that looks for work as each call ends. The
first worker starts its third attempt 50 s into the lease. At 75 s, with
that attempt still in flight, the second worker takes the lapsed ticket, is
told by the reconciler that nothing executed, and invokes beside it. The
first worker's pass then fails `claim-fenced`.

**A claim that takes over a retry-scheduled reconcileable row spends an
attempt before it invokes.** A budget of six, with every call refused and
every finding "not executed", makes three calls. That is on `main` and is
not this issue (section 8), but it decides where the check goes.

## 2. Approaches

1. **Ask, before each later attempt, whether the claim is current.** What
   the brief asks. It stops the fourth attempt in the brief's example
   (four of 60 s under 180 s), and leaves the third in flight when the
   lease lapses: it starts just inside the lease and ends just outside it,
   by the time the reconciler took.
2. **Ask whether the lease is still active for the length of the next
   attempt.** The same question about the attempt's end, from the same
   record and clock.
3. **Renew the lease before each later attempt.** A fleet write for each
   attempt, and one claim keeps its ticket for a whole budget, with every
   ticket behind it in the batch and its place under a concurrency limit.
4. **Document that a claim's attempts must fit one lease,** and let a
   deployment enforce it. The exposure stays wherever it is not enforced.

Chosen: 2.

## 3. The change

- `AgentDispatcherFleet::is_current_claim_for(claim, for_ms)`, new, in
  `rakka-agent-workflow`: whether the claim is current and its lease still
  active `for_ms` from now. It reads the handle's own copy of the record
  and its clock, as `is_current_claim` does, and the lease it reads is the
  entry's, which a renewal moves.
- In `rakka-agent`'s dispatcher, where an attempt made under this claim is
  found not executed and the budget has another: the claim asks that
  question for the effect's `timeout_ms`, and ends when the answer is no.
  It ends before it records the attempt it lost.
- The attempt that is made is bounded from the instant the claim asked, not
  from its own start.
- A claim that ends this way ends its pass's batch, and the pass gives back
  the claims it had not started.
- `AgentDispatchPass::lapsed` counts the claim.
- A reconcileable effect that the wind-down's fence exempts, found not
  executed by a claim that did not make the attempt, is retried while its
  run winds down, where it was cancelled.

No durable field, status, error code or metric is added.

## 4. My calls

1. **The lease must cover the attempt, not only its first instant.** That
   is stricter than the brief's words and is what its stated need takes:
   see approach 1. An effect that declares no `timeout_ms` is asked only
   whether the lease has lapsed. **Yours to reverse:** passing zero gives
   the brief's check exactly.
2. **No renewal.** The answer to the brief's question. A budget that
   outlasts the lease is spent over several claims, which the brief says
   is fine by the host. A renewal would add a write for each attempt and
   hold everything behind the claim for the length of a budget.
3. **The claim ends before it records the attempt it lost, and writes
   nothing.** The row stays `Dispatching`, which is what a worker lost
   after `Started` leaves, so the claim that follows needs no new path: it
   asks the reconciler about that attempt and goes on from the answer. Had
   the claim recorded the attempt first, the claim that follows would have
   spent one more attempt before invoking (section 1), and a lapsed worker
   would have written to a row that is no longer its own.
4. **The attempt's bound is counted from the instant the claim asked.**
   Between asking and invoking, the dispatcher records the attempt before,
   authorizes the next and writes its `Started`. Counted from the
   attempt's own start, that time would carry its deadline past the lease
   the claim had just been told covered it. So it comes out of the
   attempt's time, and the deadline the executor is handed is inside the
   lease.
5. **A claim that ends this way ends its batch, and the pass gives back
   what it had not started.** The claims behind it are renewed by a
   settle, and this claim wrote none: they hold what is left of the lease
   it ran out of. Started there, a claim's first attempt, which is not
   asked, would be in flight at the lapse. Given back, they are claimable
   at once under a full lease. It is the write a failed pass already
   makes.
6. **It is counted in `lapsed`.** The brief suggests it and the host
   already reads that field. A new field would need every reader taught.
   The meaning widens from "not started" to "ended for want of lease", and
   the doc says so. The claims given back are not counted anywhere.
7. **The attempt that spends the budget is settled by the claim that made
   it,** whatever is left of the lease. Nothing follows it for the lease to
   cover, and ending there would only delay telling the run.
8. **A claim's first attempt is not asked.** The rule is for what a claim
   does beyond what it was claimed for. Applied to the first attempt, a
   lease no longer than an attempt's bound would never run anything.
9. **"Not executed, and the run is winding down" no longer cancels an
   effect the wind-down is waiting for.** This one goes beyond the brief.
   A reconcileable compensation whose claim ran out of lease was cancelled
   by the claim that followed, with half its budget unspent, because that
   claim read the wind-down as "the run wants nothing further". On `main`
   the same reading already met a compensation whose worker was lost after
   `Started`; the change would have made it the ordinary path. It is
   corrected where it is read, for both.
10. **One new public method on the fleet handle.** The dispatcher could
    have read the entry's lease itself; asking the handle keeps one clock
    and one definition of "current".

## 5. What a reader sees

| State | Before | Now |
|---|---|---|
| An attempt is found not executed, budget is left, and the lease covers another | the next attempt, at once, bounded from its own start | the next attempt, at once, bounded from the instant the claim asked |
| The same, and the lease has lapsed or will before another attempt's bound | the next attempt anyway, to the end of the budget; the pass fails `claim-fenced` | the claim ends, writes nothing, counts in `lapsed` |
| The claims the pass had not started | run after it, unless the pass failed | given back, claimable at once; the pass ends |
| The ticket after such a claim | n/a | `Claimed` until the lease lapses; then any worker asks the reconciler and goes on from its answer |
| An exempt reconcileable effect found not executed while its run winds down | cancelled `run-cancelled` | retried under its budget |
| The attempt that spends the budget | `Exhausted`, run told | unchanged |
| A claim's first attempt | made | unchanged |
| An effect with no `timeout_ms` | as row 2 | a later attempt is made unless the lease has already lapsed |

Three things follow that I want named:

- **A ticket waits out the rest of its lease.** A claim that ends this way
  leaves the entry `Claimed`. That wait is at most one attempt's bound.
- **The attempt a claim leaves is recorded as one whose worker was lost,**
  not under the code its executor gave. The run's record still carries the
  code of the attempt that spent the budget.
- **A lease no longer than an attempt's bound means one attempt to a
  claim.** The budget is still spent.

## 6. What it does not close

- A first attempt that outlives its lease, as before.
- An attempt of an effect with no declared bound that starts inside the
  lease and outlives it.
- An executor that does not keep the `deadline_at` it is handed.
- Workers whose clocks disagree, as for every lease.
- What the next claim's reconciler answers. Told "not executed" again, it
  records the attempt once and goes on. Told that nothing can be
  established, it records a failed attempt and a later claim asks again,
  and that later claim spends one more before it invokes (section 9). So
  the budget is spent exactly only when each finding is "not executed".
- A run that is cancelled between the two claims. The claim that follows
  cancels an effect the wind-down's fence applies to, as a wind-down does.

## 7. Tests

`crates/rakka-agent/tests/dispatch_attempt_lease.rs`, new, twelve tests. An
executor moves the shared clock by the time each call takes, so where the
clock stands when a call starts is the evidence.

- A claim whose lease has lapsed makes no further attempt (no bound
  declared: three calls, not four).
- An attempt the lease does not cover is not started (bound declared: two
  calls, and none in flight at the lapse).
- No second worker invokes while an attempt is in flight: the measurement
  of section 1, with a rival worker looking for work as each call ends.
- A later attempt's deadline falls inside the lease, with an authority that
  takes two seconds between the claim's asking and the attempt's start.
- The claims behind a claim that ran out of lease are given back, and a
  second worker takes one at once under a full lease.
- No second worker invokes beside the claim behind one that ran out of
  lease.
- The next claim goes on: four attempts, four calls, five reconciler
  queries.
- A budget's last attempt is settled under the claim that made it, with
  less lease left than an attempt's bound.
- A bound longer than the lease means one attempt to a claim.
- A budget that fits its lease is spent under one claim.
- A compensation goes on under the claim that follows.
- A compensation whose worker was lost goes on too.

Ten fail on `main`; the last-attempt and fits-its-lease tests hold what
must not change.

`crates/rakka-agent-workflow/tests/dispatcher_fleet_leases.rs`, three
tests of the fleet's question: what is left of a lease, a renewed claim,
and a claim another worker took over.

## 8. The review

A fresh reviewer read the first commit alone, with probes of its own. It
found three things the first build got wrong, each shown by a test that is
now in the file:

- **The claim behind the one that ended was started on what was left of
  the lease.** Nothing had renewed it, a first attempt is not asked, and a
  second worker invoked beside it. The change had moved the double
  invocation from one ticket to the next. Call 5.
- **The check and the attempt's start were two instants.** An authority
  that took two seconds carried a later attempt's deadline one second past
  its lease. Call 4.
- **A compensation was cancelled by the claim that followed.** Call 9.

And statements that were too strong: that no later attempt could be in
flight at the lapse, and that the budget is always spent exactly
(section 6).

## 9. Found, not done

- **A reconcileable effect retried after a recorded failure spends two
  attempts for each call.** A claim that finds such a row retry-scheduled
  asks the reconciler, and on "not executed" records one more failed
  attempt before it invokes, though the attempt before was already
  counted. A budget of six makes three calls, and the run's record names a
  lost dispatcher where none was lost. On `main`; the reviewer reproduced
  it; drafted for the owner, not filed.
- **A read-only effect that the fence exempts, whose worker is lost during
  a wind-down, is discarded.** The same reading as call 9, in the
  read-only arm. By reading only; no such effect is configured by default.
- No bound on `max_attempts`, and no pacing between a claim's attempts,
  as #168 left them.
