# Batch claims: renewal in the settle, and release on a failed pass (issue #157)

Date: 2026-11-17. Branch `batch-claim-renewal-and-release`, off main `c4be3d9`.

## 1. What was asked, and what was found

Issue #157 is the host's second fleet brief. It was drafted against `ff9a495`,
the host's pin. It makes three asks and puts one question:

| | The ask | Where main (`c4be3d9`) stands |
|---|---|---|
| 1 | A settle that survives another worker's write | **Landed in #146, `6fca814`.** The host's pin predates it |
| 2 | A lease per claim, not per batch, or a renewal | Not built |
| 3 | A pass that fails releases the claims it did not start | Not built |
| Q | Is a delivered claim meant to count against the class limit until its lease lapses? | Answered in section 7 |

**Ask 1.** `AgentDispatcherFleet::update_current_claim` reads the record again
and re-applies a settle that lost its compare-and-set, up to three more times,
under the claim's own fence. The tests that pin it pass on main:
`dispatcher_fleet_settle.rs` (5) and `dispatch_fleet_prune.rs` (4). This
change adds the host's own staging as a test and builds nothing more for it.

**Ask 3.** `AgentRunEffectDispatcher::claim_into` returns on the first error
from a claim. The claims behind it stay `Claimed` under the batch's lease, and
nothing in the fleet can put a claim back.

**Ask 2.** `claim_due` leases every claim of a batch at one instant, the pass
runs them one after another, and nothing renews.

## 2. Goals, and what this is not

- A pass that works through its batch keeps the batch: a later claim is not
  re-claimed by another worker while the pass is alive and making progress.
- A lease has to cover the longest single call, not the sum of a batch.
- A pass that fails gives back what it never started, at once.
- **Nothing runs twice.** Every new write keeps the fence: this worker, this
  fencing token, a lease still active.
- **No durable shape changes.** No field, status or schema version is added.

Not in this change: the batch size, the claim order, the default limit, the
record's one-record shape, a heartbeat during a call, and backoff or jitter
on a re-applied write.

It would be a failure if a busy fleet wrote the fleet record more often than
it does today. The host measured that record losing writes under load.

## 3. Approaches for Ask 2

1. **Renew in the settle.** Each settle a pass writes also extends the leases
   of the claims that pass still holds. No extra write. Each claim starts
   with a full lease, counted from the settle before it.
2. **A renewal write at each claim's start.** One more compare-and-set of the
   whole record per claim after the first. To let the lease shrink below a
   batch's total time it must renew every remaining claim each time, which
   is approach 1 with an extra write.
3. **Leave Ask 2.** Build Ask 3 alone.

Dropped before these: claiming one ticket at a time. It makes the lease start
at execution by construction, but it changes what a batch and the claim order
are, which the brief says it does not ask for.

**The owner picked approach 1** (2026-11-17).

## 4. The design

### 4.1 The fleet handle (`rakka-agent-workflow`)

Two public methods on `AgentDispatcherFleet`, and one change to every settle.

**`hold_claims(claims)`** names the claims this handle's worker has taken and
not yet started. It replaces what was held. It reads and writes nothing.

**Every settle renews what is held.** `complete_claim`,
`record_claim_failure` and `settle_claim_from_row` all go through
`update_current_claim`. In the same write, each held claim other than the one
being settled is looked up in the record the write is applied to. If the entry
is still that claim's (same worker, same fencing token, lease active now), its
lease expiry becomes now plus the handle's lease duration, never earlier than
it was. A held claim that lapsed, was taken over, finished or is gone is left
as it is. A settle that loses its write is read again and re-applied, and the
renewal is computed again from the fresh record.

**`release_claims(claims)`** puts claims back to claimable.

- The record is read fresh on every attempt.
- Each claim that is still current has its entry set to `Pending` with no
  lease. Nothing else on the entry changes: `due_at`, `attempts`,
  `last_error_code`, `last_fencing_token`, `cancellation_requested`.
- A claim that is not current is skipped.
- One write for all of them, and none when nothing is current.
- A lost write is read again and re-applied, up to three more times.
- It answers how many entries it released.

A released entry has no lease, so a late write under the released token is
answered `claim-fenced`. The next claim of the entry takes the next token.

### 4.2 The pass (`rakka-agent`)

`claim_into` becomes, for each claim of the batch in order:

1. **Ask the handle whether the claim is still current**, from the copy of
   the record the handle holds. No read. A claim that is not current is
   counted in `AgentDispatchPass::lapsed` and not started. Nothing is written
   for it: its lease is over, so any worker can claim it.
2. **Hold the claims behind it.**
3. **Execute it.**

When the batch ends, however it ends, nothing is held any more.

**On an error** the pass releases the claims it did not reach and returns the
error it had. A release that itself fails is logged and not returned. The
claim that failed keeps its lease.

**On a probe's kill** (`ClaimConclusion::Died`) nothing is released. A killed
worker cannot release anything, and the probe stands for one.

**Why the check in step 1 is exact for a lapse.** While a lease is active,
only its own worker's writes move it, and the handle's copy holds every write
the handle made. So the copy's expiry for a claim this worker holds is the
record's. Once that expiry has passed, the claim is not started, whether or
not another worker has taken the ticket.

### 4.3 The workflow-tier worker

`AgentDispatcherWorker::run_once` runs the same kind of loop. It holds the
rest of its batch across each dispatch, and on an error other than
`claim-unavailable` it releases what it did not start before it returns the
error. `dispatch_claim` is unchanged.

### 4.4 What a pass reports

`AgentDispatchPass` gains two counted fields:

- `lapsed`: claims the pass took and did not start, because their lease had
  lapsed before the pass reached them.
- `concurrency_limited`: due tickets the claim skipped because their class or
  target was at its limit. The fleet already counted it; the pass dropped it
  (the brief's section 1.5).

The fleet counter `rakka.agent_workflow.dispatcher.fleet` gains two label
pairs: `operation=renew, outcome=renewed`, counting entries renewed, and
`operation=release, outcome=released`, counting entries released. `detail` is
`none` for both. No label carries a run, effect, tenant or worker id.

## 5. Compatibility

- **The record.** Nothing new is written. A renewed entry is a claimed entry
  with a later `lease_expires_at`. A released entry is `Pending` with no
  lease and a `last_fencing_token` above zero, which is what a registration
  has always made of a lapsed claim.
- **An `ff9a495` worker beside a newer one.** It leaves a renewed lease alone
  and claims a released entry, as it would any lease and any pending entry.
  Its own batches behave as before: no renewal, no release, and a settle
  that does not re-read.
- **A rollback** is clean.
- **Source.** `AgentDispatchPass` gains two public fields. Field access and
  `..AgentDispatchPass::default()` are unaffected. A struct literal or a
  pattern that names every field stops compiling.

## 6. Decisions

**The owner's (2026-11-17):** approach 1 for Ask 2; no interim note on #157,
one report when it merges; build straight through and review at the pull
request.

**Calls I made, for the owner to check:**

1. A released entry is `Pending`, not what it was before the claim. The claim
   does not keep what it replaced, and a registration already turns a lapsed
   claim into `Pending`.
2. The claim that failed is not released. Its lease is its backoff, and a
   ticket that fails every pass would otherwise be claimed first by every
   pass and fail the batch behind it each time.
3. A claim whose lease lapsed is skipped without a write.
4. The claim being executed is not renewed by anything. A call longer than
   the lease lapses as it does today, and takes the unstarted rest of its
   batch with it.
5. `hold_claims` is explicit. `claim_due` does not hold its batch by itself,
   so a caller of the fleet that never calls it sees no change.
6. `run_once` gets the same treatment (4.3), though the host does not call it.
7. The two new fields on `AgentDispatchPass`, and the two metric label pairs.
8. A failed release is logged, not returned.

## 7. The answer to the host's question

No. The limit is meant to bound calls in flight. An entry counts from its
claim until it is settled or its lease lapses, because the settle is the only
thing in the record that says a call ended. A delivered claim that still
counts is a claim whose settle was lost. With the re-read of `6fca814` that
takes four lost writes running, or a worker lost between the delivery and the
settle. It then holds its slot for the rest of one lease.

What also holds a slot is a batch's claimed and unstarted rest. That is what
a batch is: the slot is taken at the claim. With this change a failed pass
gives those slots back at once. The batch size is the lever for the rest.

## 8. Build order

1. `rakka-agent-workflow`: `release_claims`; `hold_claims` and the renewal;
   `run_once`. Tests in `tests/dispatcher_fleet_leases.rs`.
2. `rakka-agent`: the pass loop and the two fields. Tests in
   `tests/dispatch_batch_leases.rs`, the host's staging among them.
3. Product docs (`docs/rakka-agents.md`, `docs/rakka-compatibility.md`),
   `CHANGELOG.md`, validation.

Each behaviour gets its failing test first, and each guard is falsified once
it passes.

## 9. Not done by this change

- No heartbeat during a call (call 4 above).
- A registration and a claim that lose their write still fail the pass. They
  fail before any claim is taken, so nothing is stranded.
- The claim that failed waits out its lease.
- A pass whose future is dropped releases nothing, like a killed worker.
- A delivered ticket whose settle was lost for good is still ended
  `Cancelled` under `ticket-stale` or `intent-already-resolved` at its
  re-claim, not `Completed`.
