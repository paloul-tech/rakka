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
   record and clock. No attempt after the first is in flight at the lapse.
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
  question for the effect's `timeout_ms`, and ends as `Lapsed` when the
  answer is no. It ends before it records the attempt it lost.
- `AgentDispatchPass::lapsed` counts it.

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
   nothing.** The row stays `Dispatching`. The claim that follows asks the
   reconciler about that attempt, records it and makes the next. So the
   budget counts each attempt once. Had the claim recorded it first, the
   claim that follows would have spent one more attempt before invoking
   (section 1), and a lapsed worker would have written to a row that is no
   longer its own.
4. **It is concluded as `Lapsed` and counted in `lapsed`.** The brief
   suggests it and the host already reads that field. A new field would
   need every reader taught. The meaning widens from "not started" to
   "ended for want of lease", and the doc says so.
5. **The attempt that spends the budget is settled by the claim that made
   it,** whatever is left of the lease. Nothing follows it for the lease to
   cover, and ending there would only delay telling the run.
6. **A claim's first attempt is not asked.** The rule is for what a claim
   does beyond what it was claimed for. Applied to the first attempt, a
   lease no longer than an attempt's bound would never run anything.
7. **One new public method on the fleet handle.** The dispatcher could have
   read the entry's lease itself; asking the handle keeps one clock and one
   definition of "current".

## 5. What a reader sees

| State | Before | Now |
|---|---|---|
| An attempt is found not executed, budget is left, and the lease covers another | the next attempt, at once | unchanged |
| The same, and the lease has lapsed or will before another attempt's bound | the next attempt anyway, to the end of the budget; the pass fails `claim-fenced` | the claim ends, writes nothing, counts in `lapsed`; the pass goes on |
| The ticket after such a claim | n/a | `Claimed` until the lease lapses; then any worker asks the reconciler and goes on |
| The attempt that spends the budget | `Exhausted`, run told | unchanged |
| A claim's first attempt | made | unchanged |
| An effect with no `timeout_ms` | as row 2 | a later attempt is made unless the lease has already lapsed |

Three things follow that I want named:

- **A ticket waits out the rest of its lease.** A claim that ends this way
  leaves the entry `Claimed`. That wait is at most one attempt's bound.
  Releasing the claim would be a write, and a renewal by another name.
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

## 7. Tests

`crates/rakka-agent/tests/dispatch_attempt_lease.rs`, new, seven tests. An
executor moves the shared clock by the time each call takes, so where the
clock stands when a call starts is the evidence.

- A claim whose lease has lapsed makes no further attempt (no bound
  declared: three calls, not four).
- An attempt the lease does not cover is not started (bound declared: two
  calls, and none in flight at the lapse).
- No second worker invokes while an attempt is in flight: the measurement
  of section 1, with a rival worker looking for work as each call ends.
- The next claim goes on and the budget is spent exactly: four attempts,
  four calls, five reconciler queries.
- A budget's last attempt is settled under the claim that made it, with
  less lease left than an attempt's bound.
- A claim's first attempt is made whatever its bound, and the budget is
  spent one attempt to a claim.
- A budget that fits its lease is spent under one claim.

Five fail on `main`; the last-attempt and fits-its-lease tests hold what
must not change.

`crates/rakka-agent-workflow/tests/dispatcher_fleet_leases.rs`, three
tests of the fleet's question: what is left of a lease, a renewed claim,
and a claim another worker took over.

## 8. Found, not done

- **A reconcileable effect retried after a recorded failure spends two
  attempts for each call.** A claim that finds such a row retry-scheduled
  asks the reconciler, and on "not executed" records one more failed
  attempt before it invokes, though the attempt before was already
  counted. A budget of six makes three calls. On `main`; drafted for the
  owner, not filed.
- No bound on `max_attempts`, and no pacing between a claim's attempts,
  as #168 left them.
