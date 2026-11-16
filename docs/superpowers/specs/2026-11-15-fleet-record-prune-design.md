# Fleet record prune and fleet metrics: design (issue #145)

Date: 2026-11-15. Base: `main` at `ff9a495`. Branch: `fleet-record-prune`.

## 1. Intent

The dispatcher fleet keeps one durable record per database, and nothing ever removes an entry from it. Every claim and every settle writes the whole record back. On the host's staging environment the record reached 11.6 MB of finished entries, a 3.7 MB record made a node answer a command late, and the only remedy is to stop every node and delete the row.

Issue #145 (the host's fleet-record prune brief) asks for two changes:

1. A way to remove finished entries while workers run.
2. The fleet's metrics reachable from `AgentRunEffectDispatcher`.

It also asks three questions that the report on #145 must answer (section 10).

**Owner decisions, 2026-11-15:**

- An **explicit prune with a horizon**, over removal as part of the fleet's own writes.
- **The settle re-reads the record** when it loses its write (section 6.5). This is the first item of the host's announced second brief; it is taken now because a prune is one more writer.
- **Two existing defects are fixed in this change** (sections 6.6 and 6.7). A design review of this document found them; neither is caused by the prune.

## 2. The code at `ff9a495`, verified

Paths are under `crates/`.

**The record.** `AgentDispatcherFleetState` (`rakka-agent-workflow/src/dispatcher.rs:985`) is a map of `AgentDispatchEntry` by `AgentDispatchId`, plus `updated_at`. Entries are inserted or replaced at `:1389` (register), `:1485` (claim) and `:1725` (settle), and updated in place by the two cancellation paths. Nothing removes one. Every write is a compare-and-set of the whole record at the revision the handle last read (`persist`, `:1732`).

**An entry's clock.** An entry carries `updated_at`, `completed_at` and `exhausted_at` (`:581-585`). Every transition into a finished status sets `updated_at` (`mark_completed`, `mark_exhausted`, `mark_cancelled`, `:756-808`). Nothing touches a finished entry afterwards: `upsert_from_due_effect` returns it unchanged (`:691`), and the cancellation paths only count it. So a finished entry's `updated_at` is the time it finished.

**What a finished entry does today.** A registration leaves a finished entry untouched (`:691`). If an effect's outbox row is due while its fleet entry is finished, the entry is what stops the effect being registered and claimed again.

**The host's tier, `AgentRunEffectDispatcher`** (`rakka-agent/src/dispatch.rs`). It settles a fleet entry at seven places: `:3279`, `:3482`, `:3609`, `:3726`, `:3927`, `:3976`, `:4667`. Five can finish an entry (`:3279`, `:3482`, `:3609`, `:3976`, `:4667`), and each writes the run's outbox first. `:3726` and `defer_dispatch` (`:3927`) only reschedule. It never calls the fleet's cancellation methods; its wind-down fence (`fence_run`) writes the outbox and the run, not the fleet.

**The workflow tier, `AgentDispatcherWorker`** (`rakka-agent-workflow/src/dispatcher.rs:2027`):

- `dispatch_claim` (`:2268`) writes the outbox first, then the fleet.
- `cancel_run_dispatches` (`:2209`) writes the **fleet first**: `AgentDispatcherFleet::cancel_run_dispatches` (`:1527`) marks unclaimed entries `Cancelled`, and only then are the outbox rows settled.
- `refresh_run` (`:2170`) calls `finalize_run_cancellations` (`:1589`), also **fleet first**, then settles.
- A crash between the two writes is repaired by `refresh_run` itself: it lists the run's `Cancelled` fleet entries and settles any outbox row still cancellable (`:2183-2194`). **The repair reads the finished entry back.** If that entry were removed before the repair ran, the row would still be due, a fresh `Pending` entry would be registered, and a cancelled effect would be dispatched.
- Only tests call the worker's cancellation. The host calls neither path.

**Stale registrations.** A registration's compare-and-set is fenced on a fleet revision, but its list of due effects can be older than that revision:

- `register_run_into` (`rakka-agent/src/dispatch.rs:2666`) reads the run's outbox, and only then reads the fleet record (`:2680`).
- `refresh_run` reads the inbox (`:2181`) before a handle that holds no record first reads the fleet (`:2182` into `:1593-1595`). A handle that already holds a record registers against the revision it cached earlier, which is safe.

If another worker claimed, executed and settled the effect between the two reads, and a prune then removed the entry, the registration would insert a fresh `Pending` entry for a finished effect. Today the finished entry closes this door.

**Outbox rows.** `rakka-workflow/src/inbox.rs` and `model.rs`:

- A finished row (`Dispatched`, `Exhausted`, `Cancelled`) is not due (`model.rs:612`).
- `mark_outbox_dispatching` (`inbox.rs:530`), `record_outbox_success`, `record_outbox_failure` and `record_outbox_exhausted` set the row's status **whatever it held**. Only `record_outbox_cancelled` checks (`is_cancellable`).
- A dispatch ticket's outbox id folds in the effect's generation (`dispatch_ticket_id`, `rakka-agent/src/effect.rs`), and scheduling an id the outbox already holds answers `Duplicate`. So no new ticket reuses a finished row's id.

**Settles write against a cached revision.** `update_current_claim` (`dispatcher.rs:1696`) writes at the revision the handle cached, without re-reading. Any write from another handle in between makes it lose:

| Tier | What a lost settle does today |
|---|---|
| `AgentRunEffectDispatcher` | The outbox row is already written and the outcome delivered. The pass returns `revision-conflict` and abandons the rest of its batch. After the lease lapses the ticket is re-claimed and settled `Cancelled` with `intent-already-resolved` (`dispatch.rs:2794`). Nothing is invoked twice. |
| `AgentDispatcherWorker` | The row is `Dispatched` and the entry stays `Claimed`. After the lease lapses it is re-claimed, `mark_outbox_dispatching` sets `Dispatching` over `Dispatched`, and the effect is **dispatched a second time**. A crash between the two writes has the same result. |

**A stale worker can reopen a finished row.** In `AgentRunEffectDispatcher`, `write_outbox` (`dispatch.rs:2521`) re-reads the inbox and applies its write with no check of the row's status. A call that outlives its lease and then fails writes `Failed` over a row another worker already finished as `Dispatched`. The row is due again. Today the finished fleet entry absorbs the re-registration, and the row stays due for good. The workflow tier is not exposed: its writes go through the inbox handle it recovered before dispatching, so a stale write loses that record's own compare-and-set.

**The agent tier's last line.** `execute_claim` settles a ticket whose intent is already resolved without invoking (`dispatch.rs:2794-2798`), and the success path delivers the outcome (`:3270`) before it finishes the row (`:3277`).

**Metrics.** The fleet records the counter `rakka.agent_workflow.dispatcher.fleet` and the gauges `.in_flight` and `.backlog` (`dispatcher.rs:42-49`, `record_metric`, `record_gauges`). `AgentRunEffectDispatcher` builds its fleet with a no-op recorder in `new` (`dispatch.rs:2203`) and in `rebuild_fleet` (`:2295`), and `with_metrics` (`:2461`) sets only the dispatcher's own recorder.

**Other readers.** Nothing in Rakka reads fleet entries except the fleet and its own health snapshot (`AgentDispatcherSnapshot`).

**The precedent.** `AgentWakeTimerStore::prune_terminal(before)` (`rakka-agent/src/wake_timers.rs:495`) removes finished entries last touched before a horizon, in one write.

**An unused error variant.** `AgentDispatcherError::ClaimUnavailable` (`claim-unavailable`) is declared and never constructed. Section 6.7 uses it.

## 3. Goals, non-goals, anti-goals

**Goals**

- A prune that removes finished entries older than a horizon, safe while workers run, at any horizon.
- Three rules, true at every site in both tiers after this change:
  1. **A fleet entry is finished only after its outbox row is finished or absent.**
  2. **A registration's due list is never older than the fleet revision it writes against.**
  3. **A finished outbox row stays finished.** No dispatcher write moves it, and no attempt starts on it in the workflow tier.
- A settle that survives another handle's write.
- The dispatcher's recorder handed to its fleet, and a gauge that shows the record's entry count by status.
- No durable field, no schema version, no new error code.

**Non-goals**

- Splitting the record per run or per shard. Section 10 gives the answer for the host.
- Removal as part of the fleet's own writes, or a retention setting.
- The second item of the host's second brief: one lease instant for a whole batch.
- **Issue #134.** A claim on an `Exhausted` row whose intent is unresolved still starts an attempt in `AgentRunEffectDispatcher`. Refusing to start it would leave that intent unresolved with nothing to claim it, which #134 must decide together with the park. This design does not guard the `Dispatching` write in that tier.
- **A stale worker's write to an unfinished row.** A worker whose lease lapsed can still burn an attempt on a ticket another worker is running. Fencing outbox writes on the claim is a larger change.
- A gauge of the record's stored size. The fleet does not see encoded bytes; the host keeps its own.
- Issues #25, #123 and #132, which touch the same files.

**Anti-goals**

- A prune that can move, remove or re-open a live entry.
- A prune whose safety depends on the horizon being long enough.
- A metric label that carries a run, effect, tenant or worker id.

## 4. Constraints

- **From the brief:** safe while workers run; a pending, claimed or retry-scheduled entry is untouched, and so is a claimed entry whose lease has lapsed; say what an older binary reads; bounded labels only; say whether each change is additive.
- **From the repository:** every public item documented, `clippy -D warnings`, error codes are a compatibility surface, user-facing changes go in `CHANGELOG.md` and the product docs.
- **The fleet record is the hottest shared record.** A prune that finds nothing to remove writes nothing.
- **`rakka-workflow` is not changed.** Its outbox transitions stay as they are; the guards live in the two dispatchers.

## 5. Approaches

**How entries are removed**

| Approach | What it is | Trade-off |
|---|---|---|
| **A. Explicit prune with a horizon** (chosen) | `prune_terminal(before)`, called by the application | One whole-record write per call. Matches the wake-timer prune. Adds no settings surface. Someone has to call it. |
| B. Removal in the fleet's own writes | A retention on the fleet settings; old finished entries dropped whenever the record is written anyway | No extra write and no scheduling. Every worker must agree on the retention, and off by default protects nobody new. |

The owner picked A. The entry-count gauge covers A's risk that nobody calls it.

**How the prune is made safe**

| Approach | What it is | Why chosen or not |
|---|---|---|
| **1. Make the three rules true, then prune freely** (chosen) | Reorder the fleet-first cancellation paths, order the registration's reads, and stop the dispatchers rewriting a finished row | The prune needs no horizon floor and no knowledge of outboxes. Each rule is one sentence the host can rely on. |
| 2. A horizon floor | Refuse a horizon shorter than some bound | Safety would rest on timing: nothing bounds how long a stalled task waits between two reads, or how long a cancelled run goes unrefreshed. |
| 3. A durable "outbox unsettled" mark on the entry | The fleet-first paths set it, a later write clears it, the prune skips it | A new durable field that an older binary strips when it rewrites the record, and a second write on every cancellation anyway. |

**What a prune does to work in flight**

| Approach | What it is | Why chosen or not |
|---|---|---|
| **The settle re-reads on a lost write** (chosen by the owner) | `update_current_claim` re-reads and re-applies under the same fence | One function, both tiers. A prune no longer costs in-flight work a lease. |
| Document "prune when the fleet is quiet" | No code | "Safe while workers run" would carry an asterisk, and in the workflow tier a prune could cause a second dispatch. |

## 6. Design

### 6.1 Why a prune is safe at any horizon

Take an effect whose fleet entry is finished.

- By rule 1, its outbox row is finished or absent.
- By rule 3, that row stays finished: no settle write moves it, and the only write that could, the `Dispatching` mark, needs a claim, which needs an entry.
- By rule 2, no registration can carry a due list from before the entry finished.

So no later registration can produce an entry for that effect, whether or not the finished entry is still in the record. The prune may remove it at once.

Sections 6.3 to 6.7 make the three rules true. Section 6.2 is the prune.

### 6.2 The prune

In `rakka-agent-workflow`:

```rust
impl<Store, Clock> AgentDispatcherFleet<Store, Clock> {
    /// Removes finished entries that finished before `before`.
    pub async fn prune_terminal(
        &mut self,
        before: AgentTimestampMillis,
    ) -> AgentDispatcherResult<AgentDispatcherPrune>;
}

/// What one prune did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentDispatcherPrune {
    /// Entries this call removed.
    pub pruned: usize,
    /// Entries left in the record, live and finished.
    pub remaining: usize,
}
```

- **Eligible:** `entry.status.is_terminal() && entry.updated_at < before`. Nothing else is read. A fenced entry (`cancellation_requested`) that is not yet finished is not eligible.
- **Any horizon is accepted,** including one in the future, which removes every finished entry.
- **It reads the record fresh** on every attempt, never from the handle's cached copy.
- **Nothing eligible means no write.** The revision does not move.
- **A lost compare-and-set is retried.** The call re-reads and re-applies up to 3 more times, recognizing the loss by its stable code (`revision-conflict`), as `write_outbox` does in `rakka-agent`. After that it returns the conflict.
- **The state's `updated_at`** is set from the fleet's clock.
- **Metrics:** the counter `rakka.agent_workflow.dispatcher.fleet` with `operation=prune`, `outcome=pruned`, `detail=none`, and the removed count as its value; then the gauges.

In `rakka-agent`:

```rust
impl AgentRunEffectDispatcher<..> {
    /// Removes finished entries from this dispatcher's fleet record.
    pub async fn prune_fleet(
        &mut self,
        before: AgentTimestampMillis,
    ) -> AgentDispatchResult<AgentDispatcherPrune>;
}
```

It calls the fleet's `prune_terminal` and nothing else. An operator command needs no dispatcher: `AgentDispatcherFleet::new(store)` over the fleet store reaches the same method. `AgentDispatcherWorker` already exposes its fleet through `fleet_mut()`.

`AgentDispatcherPrune` is exported from `rakka-agent-workflow` and re-exported wherever `AgentDispatcherFleetState` is.

### 6.3 Rule 1. Cancellation in the workflow tier: fence, settle, finish

The existing flag `cancellation_requested` already means "never claimable again" (`is_claimable_at`, `dispatcher.rs:636`). The reorder uses it as the fence, so no durable field is added.

**The fleet's methods**

```rust
/// Fences every unfinished entry of a run against being claimed.
pub async fn request_run_cancellation(
    &mut self,
    run_id: &AgentRunId,
) -> AgentDispatcherResult<AgentDispatcherCancellationRequest>;

/// Finishes the named entries of a run as cancelled.
pub async fn finalize_run_cancellations(
    &mut self,
    run_id: &AgentRunId,
    settled: &[AgentEffectId],
) -> AgentDispatcherResult<Vec<AgentEffectId>>;
```

```rust
/// What one cancellation request fenced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentDispatcherCancellationRequest {
    /// Run whose entries were considered.
    pub run_id: AgentRunId,
    /// Timestamp of the request.
    pub requested_at: AgentTimestampMillis,
    /// Effects of fenced entries with no active lease: settle these at the
    /// outbox, then finalize them.
    pub awaiting_settlement: Vec<AgentEffectId>,
    /// Entries with an active lease, fenced and left to their worker.
    pub in_flight_entries: usize,
    /// Entries already finished before the request.
    pub already_terminal_entries: usize,
}
```

- `request_run_cancellation` sets `cancellation_requested` on every unfinished entry of the run. **Nothing becomes finished.** An entry with an active lease also gets `last_error_code = "cancellation-requested"`, as today; an entry without one keeps the code it had. One write, and none when nothing changed. `awaiting_settlement` lists every fenced entry of the run with no active lease, including ones an earlier request fenced.
- `finalize_run_cancellations` finishes an entry only when it is in `settled`, belongs to the run, has `cancellation_requested`, is unfinished, and has no active lease. It answers the effect ids it finished. One write, and none when it finished nothing.
- `AgentDispatcherFleetState::cancellations_awaiting_settlement(&self, run_id, now) -> Vec<AgentEffectId>` gives the same list as `awaiting_settlement`, from a state already read.
- **`AgentDispatcherFleet::cancel_run_dispatches` is removed.** Its contract was to finish entries before their rows.

**Why the list is stable.** An entry in the list is fenced and has no active lease. A fenced entry cannot be claimed, so it cannot gain a lease between the settle and the finalize. An entry that was in flight at the request is not in the list; if its lease lapses later, the next refresh picks it up.

**The worker**

`AgentDispatcherWorker::cancel_run_dispatches` keeps its signature and its result type, `AgentDispatcherCancellation`:

1. `request_run_cancellation`.
2. Settle each `awaiting_settlement` row at the outbox with the reason `run-cancelled`, skipping a row that is not cancellable, as `settle_cancelled_effects` does today.
3. `finalize_run_cancellations` with those ids.

`cancelled_entries` and `cancelled_effect_ids` report what step 3 finished. `in_flight_entries` and `already_terminal_entries` come from step 1.

`AgentDispatcherWorker::refresh_run`:

1. If the fleet handle holds no record, read the fleet record (section 6.4).
2. Recover the run's inbox.
3. Take `cancellations_awaiting_settlement` for the run.
4. Settle those rows with the reason `dispatch-cancelled`.
5. `finalize_run_cancellations` with those ids.
6. **Kept for records an earlier binary wrote:** settle any row still cancellable whose fleet entry is already `Cancelled`, as today.
7. Read the due effects and register them.

**A crash at any point** leaves fenced, unfinished entries. They are never claimable and never prunable, and the next `refresh_run` of that run settles and finishes them. If the run is never refreshed again they stay; today an in-flight cancellation whose lease lapsed waits the same way.

**Cost:** two fleet writes per cancellation that has unclaimed entries, where today there is one.

### 6.4 Rule 2. A registration reads the fleet before the outbox

**`AgentRunEffectDispatcher`.** `register_run_into` keeps its first outbox read, which decides whether the fleet is read at all. When that read finds due effects, it reads the fleet record and then **reads the run's outbox again**, and registers what the second read finds due. If the second read finds nothing due, it registers nothing and writes nothing. `pump_run` registers through the same function. Cost: one more read of one run's inbox record on a pass that has due effects.

**`AgentDispatcherWorker`.** `refresh_run` reads the fleet record first when its handle holds none (step 1 of 6.3). A handle that already holds a record keeps it: the record was read before the inbox, and a write against a revision that has since moved loses.

In both, the registration's compare-and-set is fenced on a fleet revision that was read before the due list. Either the fleet did not move after that read, and the due list is newer than every finished entry and every prune it could reflect; or it moved, and the write loses.

### 6.5 The settle re-reads when it loses its write

`update_current_claim` (`complete_claim` and `record_claim_failure` both go through it):

1. Take the handle's record, reading it if there is none.
2. Find the entry. If it is absent, answer `ClaimFenced` (section 6.8).
3. Check the claim is current: this worker, this fencing token, a lease active now. If not, answer `ClaimFenced`.
4. Apply the update and write at that record's revision.
5. If the write loses with `revision-conflict`, read the record again and go to step 2, up to 3 more times. After that, return the conflict.

The fence is in the record, so a re-read cannot settle a claim that another worker took over: step 3 refuses it. Any other error returns at once.

`claim_due`, `register_due_effects` and the cancellation writes are not retried. A lost claim or registration is taken up by the next pass.

### 6.6 Rule 3 in `AgentRunEffectDispatcher`: a settle never rewrites a finished row

`write_outbox` already re-reads the inbox on every attempt. For the three settle writes, `Succeeded`, `Failed` and `Exhausted`, it now looks at the row in that same read:

- **The row is finished** (`Dispatched`, `Exhausted` or `Cancelled`): nothing is written, and the caller is told the row's status.
- Otherwise the write applies as today.

The check and the write use the same inbox revision, so no write can slip between them.

The `Cancelled` write is already guarded by the substrate. The `Scheduled` write creates a row. **The `Dispatching` write is not guarded** (non-goals, #134).

**What the caller does when the row was already finished.** Its claim is over. One shared step replaces the rest of the site's settle:

1. Bring the fleet entry in line with the row through `settle_claim_from_row` (below). An answer of `claim-fenced` is expected for a worker whose lease lapsed, and is not an error.
2. Deliver nothing further to the run. Whoever finished the row owns its outcome.
3. Conclude the claim as settled, so the pass goes on to its next claim.

This applies at each site that writes `Succeeded`, `Failed` or `Exhausted`: the success path, `record_attempt_failure`, `retry_ambiguous`, the reconciled-outcome path, and the `Unknown`-finding path. The success path has already delivered its outcome by then, as today; the run's own guard decides between two deliveries.

**The shared fleet method**, in `rakka-agent-workflow`:

```rust
/// Settles a current claim to match an outbox row that is already
/// finished, or absent.
pub async fn settle_claim_from_row(
    &mut self,
    claim: &AgentDispatchClaim,
    row: Option<&OutboxEntry>,
) -> AgentDispatcherResult<AgentDispatchEntry>;
```

| Row | The entry becomes | `last_error_code` |
|---|---|---|
| `Dispatched` | `Completed` | unchanged |
| `Exhausted` | `Exhausted`, with the row's attempt count | `ticket-row-settled` |
| `Cancelled` | `Cancelled` | `ticket-row-settled` |
| absent | `Cancelled` | `ticket-row-missing` |
| not finished | refused with `InvalidEntry` (`invalid-dispatch-entry`) | |

It goes through `update_current_claim`, so it is fenced and re-reads like every other settle.

### 6.7 Rule 3 in `AgentDispatcherWorker`: no attempt on a finished row

`dispatch_claim` reads the claim's outbox row after it recovers the inbox and before it marks anything:

- **The row is absent or finished:** nothing is dispatched. The claim is settled with `settle_claim_from_row`, the fleet counter records `operation=dispatch`, `outcome=skipped`, `detail=ticket-row-settled` or `ticket-row-missing`, and the call answers `AgentDispatcherError::ClaimUnavailable` (`claim-unavailable`).
- Otherwise it proceeds as today.

`run_once` treats `ClaimUnavailable` from `dispatch_claim` as a claim that needed no dispatch: it goes on to the next claim, and the claim is not among the cycle's completions.

This closes the second dispatch after a crash between the outbox write and the fleet write, and after a settle that exhausted its re-reads. Today an absent row fails the call on every lease; it now settles the entry.

### 6.8 A claim whose entry is gone is fenced

After this change, an absent entry is a normal state of a finished one. A worker still holding a lapsed claim on it must get the answer it gets today.

- `AgentDispatcherFleet::is_current_claim` answers `Ok(false)` when the entry is absent.
- `update_current_claim` answers `ClaimFenced` (`claim-fenced`) when the entry is absent.

Today both answer `DispatchNotFound`, which no path could reach for a claim the fleet issued. Nothing is written in either case.

### 6.9 The fleet's metrics from the dispatcher

- `AgentRunEffectDispatcher::with_metrics(metrics)` stores the recorder and rebuilds the fleet with it. `rebuild_fleet` uses the stored recorder, so `with_fleet_settings`, the claim-filter builder and `with_metrics` compose in any order.
- `new` keeps a no-op recorder for both, as today.
- An application that already calls `with_metrics` starts receiving the fleet's three series and the new gauge.

**The new gauge:** `rakka.agent_workflow.dispatcher.entries`, constant `METRIC_AGENT_DISPATCHER_ENTRIES`, one sample per `AgentDispatchStatus` with the label `status` (six values, from `AgentDispatchStatus::as_label`). A status with no entries is recorded as zero. It is recorded wherever the in-flight and backlog gauges are, and after a prune.

No label on any fleet series carries a run, effect, tenant or worker id; the existing cardinality test is extended to the new series.

### 6.10 What a finished entry is after this change

History only. It carries the settle reason of a cancelled ticket (`last_error_code`), it counts in the fleet's health snapshot, and nothing else reads it.

A prune removes the settle reason with the entry. The reason has no other durable copy: the outbox row keeps only its cancelled status. The horizon is how long it stays readable.

## 7. Rollout and compatibility

- **Durable shape:** unchanged. A binary at `36d742b`, `86cf087` or `ff9a495` reads a pruned record as a record with fewer entries. A test pins the bytes of a one-entry record to prove no field moved.
- **Rollback:** clean. Nothing this change writes is new to an earlier binary.
- **Before the first prune:**
  1. Every node is on the new build. An earlier `AgentRunEffectDispatcher` registers with the old read order and can rewrite a finished row, and an earlier `AgentDispatcherWorker` cancels fleet-first. All three are safe only while finished entries stay.
  2. A deployment that cancels through `AgentDispatcherWorker` refreshes every run that holds a `Cancelled` fleet entry once on the new build. An earlier binary can have left such an entry with its row unsettled, and the repair reads the entry.
  3. A deployment that must prune during a roll keeps the horizon at an hour or more. That narrows the windows and does not close them.
- **Source changes in `rakka-agent-workflow`:** `AgentDispatcherFleet::cancel_run_dispatches` is removed; `finalize_run_cancellations` takes the settled ids; `request_run_cancellation`, `prune_terminal`, `settle_claim_from_row`, `AgentDispatcherPrune`, `AgentDispatcherCancellationRequest`, `cancellations_awaiting_settlement` and `METRIC_AGENT_DISPATCHER_ENTRIES` are new.
- **Source changes in `rakka-agent`:** `prune_fleet` is new.
- **Behaviour changes:**
  - A settle that loses its write re-reads and re-applies. A pass that used to fail with `revision-conflict` and abandon its batch now usually completes, and the entry ends in its true status where it ended `Cancelled`.
  - A settle write that finds its row already finished writes nothing, and the pass goes on. The worker's fleet entry follows the row.
  - `AgentDispatcherWorker::dispatch_claim` on a finished or absent row dispatches nothing and answers `claim-unavailable`, where it dispatched again or failed with the outbox's not-found error.
  - `with_metrics` on `AgentRunEffectDispatcher` now also wires the fleet.
  - A claim on an absent entry answers `claim-fenced` where it answered `dispatch-not-found`.
  - The worker's cancellation writes the fleet twice. An unclaimed entry of a cancelled run is fenced and unfinished between the two writes, where it was `Cancelled` at once.
- **New stable strings:** the metric name, its `status` label, the counter's `operation=prune` and `outcome=skipped`, and the detail codes `ticket-row-settled` and `ticket-row-missing` on a fleet entry and the counter. `ticket-row-missing` already exists as a settle reason in `rakka-agent`.
- **No new error code, no new durable field, no schema version.** `claim-unavailable` exists and is first used here.

## 8. Proofs

Each is a test. Where a proof names a guard, the builder removes the guard and shows the test fails.

**The prune**

1. Removes finished entries older than the horizon, of all three finished statuses, and keeps a finished entry at or after it.
2. Keeps a pending entry, a retry-scheduled entry, a claimed entry with a live lease, a claimed entry whose lease has lapsed, and a fenced unfinished entry, at a horizon in the future.
3. Writes nothing and leaves the revision alone when nothing is eligible.
4. Retries a lost compare-and-set and succeeds; returns the conflict after the bound.
5. A one-entry record's bytes are the bytes `ff9a495` writes.

**The settle**

6. A prune from **another handle** lands between a worker's claim and its settle. The settle succeeds and the entry ends `Completed`, in both tiers. *Guard: the re-read.*
7. The same with another worker's claim as the foreign write.
8. A settle whose claim was taken over between the first write and the re-read answers `claim-fenced` and changes nothing.
9. A lapsed claim on a pruned entry answers `claim-fenced` and writes nothing.
10. A settle that loses more times than the bound returns the conflict.

**Rule 1**

11. For each of the five finishing sites of `AgentRunEffectDispatcher`: at the moment the fleet entry is finished, the outbox row is finished or absent.
12. Workflow tier, crash after the fence and before the settle: the entry survives a prune at a future horizon, is not claimable, and the next `refresh_run` settles the row and finishes the entry. The effect is never dispatched. *Guard: the fence-settle-finish order.*
13. Workflow tier, crash after the settle and before the finalize: the same outcome.
14. An entry in flight at the request is not finished by that request's finalize, even if its lease lapses between the two writes.
15. A `Cancelled` entry with a still-cancellable row, as an earlier binary leaves it, is settled by `refresh_run`.

**Rule 2**

16. `AgentRunEffectDispatcher`: another worker finishes the effect and a prune removes its entry between the first outbox read and the fleet read. The registration inserts no entry. *Guard: the second outbox read.*
17. `AgentDispatcherWorker`, on a handle with no record: the same interleaving between the inbox read and the first fleet read. The registration inserts no entry and nothing is dispatched twice. *Guard: reading the fleet first.*
18. After a prune of a finished entry, pumping its run again registers nothing, in both tiers.

**Rule 3**

19. `AgentRunEffectDispatcher`: worker A's call outlives its lease; worker B re-claims, completes and finishes the row; A's call then fails. The row is still `Dispatched`, A's pass does not fail, and A delivers no failure outcome. After a prune, pumping the run registers nothing. *Guard: the row check in `write_outbox`.*
20. The same for A failing definitively (`Exhausted`) and for A succeeding late (`Succeeded`).
21. `settle_claim_from_row` gives each row of the table in 6.6, and refuses an unfinished row.
22. `AgentDispatcherWorker`: a crash after the outbox success and before the fleet write. After the lease lapses the claim is taken again, nothing is dispatched, the entry ends `Completed`, and the call answers `claim-unavailable`. *Guard: the row check in `dispatch_claim`.*
23. `dispatch_claim` on an absent row settles the entry `Cancelled` with `ticket-row-missing` and answers `claim-unavailable`.
24. `run_once` goes on past a `claim-unavailable` claim and dispatches the rest of its batch.
25. Issue #134's interleaving still behaves as it does at `ff9a495`: the `Dispatching` write on an `Exhausted` row is not refused. This pins the non-goal.

**The metrics**

26. A dispatcher built with `with_metrics` records the fleet's three series and the entries gauge, whatever the order of its builders.
27. The entries gauge reports each status, zeros included, and falls after a prune.
28. The prune counter carries `operation=prune` and the removed count; a skipped dispatch carries `outcome=skipped` and its detail.
29. No fleet series carries an id as a label.

**End to end**

30. In an example that already runs the real dispatcher, with two dispatchers over one fleet record: runs complete, one dispatcher prunes at a future horizon while the other has a claim in flight, that run completes with its entry `Completed`, and no tool is invoked twice.

## 9. Docs and changelog

- `docs/rakka-agents.md`: the prune, the three rules, and what a prune removes.
- `docs/rakka-compatibility.md`: an entry for #145 with section 7's content.
- `docs/rakka-agent-observability-catalogue.md` and the metric catalogue in `rakka-agent-workflow/src/metrics.rs`: the new gauge, the prune counter and the skipped-dispatch outcome.
- `docs/rakka-v1-known-limitations-roadmap.md`: one fleet record per database, rewritten whole, bounded by a prune to live entries plus the horizon's finished ones; a stale worker can still burn an attempt on an unfinished row.
- `docs/rakka-api-boundary-inventory.md`: the new public items.
- `CHANGELOG.md`: Added, Changed, Fixed and Removed entries.
- Any test or document that holds the Collector allowlist or the metric catalogue as data is updated with the new series.

## 10. Answers for the host's report

1. **Is outbox-first a rule the prune can rely on at every site?** Yes, after this change, in both tiers. It was already true at every finishing site of `AgentRunEffectDispatcher`, which are five, not seven. It was not true of the workflow tier's two cancellation paths, which the host does not call; they are reordered. Two further doors were closed that the brief did not name: a registration built from an outbox read older than its fleet read, and a stale worker's late write that turned a finished row back to due. A prune is safe at any horizon.
2. **The cancelled ticket's `last_error_code`.** It is removed with the entry and has no other durable copy. The horizon is how long it stays readable. Nothing was added to keep it longer.
3. **Is one record, rewritten whole, the shape Rakka intends to keep?** *(Confirmed by the owner with this design, 2026-11-15.)* One record stays for now and no split is planned. A prune bounds the record to live entries plus the finished entries inside the horizon; on a busy fleet that can still be megabytes. Rakka will decide on a split per run or per shard after the host's three-node measurement and its second brief.

The report also states:

- The commit, and the public names of section 6.
- What a `36d742b` binary reads after a prune: fewer entries, nothing else.
- **The first item of the host's second brief is done here:** a settle re-reads when it loses its write (6.5). The second item, one lease instant per batch, is not.
- The three steps before the first prune (section 7).
- Each place the code differed from the brief.

## 11. Left to the builder

- Private helper names, and where the retry bound's constant lives.
- How `write_outbox` tells its caller the row was already finished, and the shape of the shared step in 6.6.
- Which existing example carries proof 30.
- Test file placement, following one concern per file.
- Whether `AgentDispatcherCancellation` needs a doc change for fields whose meaning is unchanged.
- The wording of every doc comment and document entry.

## 12. After the merge

- The report on #145, with section 10's answers and the items the brief's section 7 lists.
- A note on #134: settle writes no longer rewrite a finished row, the `Dispatching` write is still unguarded in `AgentRunEffectDispatcher`, and proof 25 pins that.
- Memory: the decisions and any trap met while building.
- A note on #134 that its unconfirmed step is confirmed: the fleet does offer an entry whose row is already `Exhausted`, and the tool is invoked again.

## 13. As built

Where the build moved from this design. The whole-branch review found the first two.

- **Section 6.6, "whoever finished the row owns its outcome", was wrong for one case.** A late worker's failure is written to a row that is not finished yet, and that worker is fenced at the fleet before it delivers. The worker that still holds the claim then finds the row `Exhausted` under it. As designed it delivered nothing, and the run waited on the effect for good, where `ff9a495` resolved it. As built, a worker that still holds the claim and finds its row `Exhausted` tells the run: `Exhausted` from a failed attempt, `Indeterminate` from a reconciliation that established nothing. A worker whose lease lapsed still delivers nothing. `the_worker_holding_the_claim_delivers_the_exhaustion_of_a_row_exhausted_under_it` holds it.
- **Section 6.1's argument for rule 3 does not hold in `AgentRunEffectDispatcher`.** It says the `Dispatching` mark "needs a claim, which needs an entry". That dispatcher does not check its claim again before the mark, so a worker that stalls past its lease before its durable `Started` can mark a finished row `Dispatching` and invoke, as it could at `ff9a495`. If that attempt fails the row is due again, and after a prune the ticket is registered again and settled `Cancelled` without being invoked. So in that tier the promise is "never run again because of a prune", not "never registered again". The documents say so. The workflow tier keeps the stronger promise: its row check and its mark share one inbox revision.
- **Proof 11 covers four of the five finishing sites.** The reconciler's `Executed` finding has no order pin.
- **Proof 16 does not assert `registered == 0`.** The other dispatcher's pass delivers the tool's result, the run schedules its next ticket, and the second outbox read rightly registers that one. The test asserts the finished ticket has no entry.
- **Proof 30 holds the claim in flight with the kill-switch probe,** not a held call. A prune between a live claim and its settle is proven in `rakka-agent`'s own fixture (`a_prune_from_another_dispatcher_does_not_cost_a_claim_its_settle`).
- **The fleet counter's `cancel` operation** records `outcome=requested` and `outcome=cancelled`; `outcome=finalized, detail=lease-expired` is gone.
- **Two public items the design did not name:** `AgentDispatchStatus::ALL` and `AgentDispatchEntry::awaits_cancellation_settlement`.
- **Test support:** `fleet_support::ContendedStore` stands in for the `HookedStore` of the plan.
