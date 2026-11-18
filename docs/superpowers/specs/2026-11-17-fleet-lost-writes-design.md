# A lost write to the fleet record: re-read for the registration and the claim, a count and a pause

Issues #159 and #160, both found by the host's review of #158 (issue #157).
One change, because both issues are about the same loop.

## 1. Where it stands

The dispatcher fleet record is one record, written whole by compare-and-set.
A pass writes it as a registration, a claim, and one settle for each claim.

| Write | Loses its compare-and-set | Since |
|---|---|---|
| Settle | reads again, re-applies, up to 3 more times | #146 |
| Release | the same | #158 |
| Prune | the same | #146 |
| Registration | returns `revision-conflict`; the pass fails | |
| Claim | returns `revision-conflict`; the pass fails | |

Nothing counts a re-application or a write that ran out of them, and the
tries run back to back.

The host measured, at `32b1ebc`, three nodes and 12 tasks in flight for 15
minutes: 942 passes failed on `revision-conflict` (0.53 for each model
call), and 3 of 3 004 finished entries were ended by a re-claim. It reads
the 3 as settles that lost four writes running, and cannot prove it.

## 2. Goals

- A registration or a claim that loses its write is re-applied, under the
  settle's bound. A pass no longer fails on one lost write.
- Every lost write is counted, by which write it was and what followed.
- Re-applications after the first wait a short random time.
- Nothing runs twice, and no finished effect is registered again.
- No durable field, status, schema version or error code changes.

Not goals: the bound (3), the cancellation's two writes, the record's
one-record shape.

## 3. The registration's lost write is a fence

This shapes the design. A registration is given a list of a run's due
effects. `register_run_into` and `refresh_run` read the fleet record first
and the list second, on purpose (issue #145): a list older than the record
can name an effect another worker has since run, settled, and had pruned,
and registering it would put a finished effect back. Read in that order,
either nothing wrote the record after the read, or the write loses.

So a registration cannot be re-applied to a fresher record with the list it
was given. **Its re-application has to read the list again, after the
record.** The fleet cannot do that: it is handed the list. The caller can.

## 4. Approaches

1. **Re-read inside `register_due_effects`, with the same list.** Smallest,
   and wrong: it removes the fence of section 3.
2. **The fleet takes a source of the list** and calls it after each read.
   The order becomes structural. It needs a generic or boxed async callback
   across two crates and two inbox types, for two callers.
3. **`register_due_effects` stays one try; its two callers loop** (record,
   then list, then register), and ask the fleet what follows a lost write,
   so the count, the bound and the pause are the fleet's. The claim, which
   needs nothing but the record, loops inside the fleet.

Chosen: 3. It keeps the fence where it is and adds one public method.

## 5. Design

### 5.1 One rule for every write that is re-applied

For a registration, a claim, a settle, a release and a prune:

- Only a lost compare-and-set (`revision-conflict`) is re-applied. Any other
  failure is returned as it is and is not counted.
- A write is re-applied at most 3 times (`FLEET_WRITE_REAPPLICATIONS`,
  unchanged). The loss after that returns `revision-conflict`.
- Every attempt is computed whole from the record it read, with the clock
  read again.

### 5.2 The claim

`claim_due` loops. Each attempt takes the due entries, the class and target
limits, the batch and the fencing tokens from the record it read, and its
own `claimed_at`. A lost attempt's claims are dropped: its write did not
land, so no lease was taken, and nothing is handed to the caller. The
attempt that ends the call is the one whose counts are returned and
recorded. A re-read that finds nothing to claim writes nothing.

### 5.3 The registration

`register_due_effects` is unchanged: one try, against the handle's copy.

`AgentRunEffectDispatcher::register_run_into` and
`AgentDispatcherWorker::refresh_run` loop. Each attempt reads the fleet
record, then the run's due list, then registers. Between attempts they call
the new `AgentDispatcherFleet::registration_lost(reapplied, error)`, which
counts the loss, waits if this re-application waits, and answers `Ok` to go
again or the error to return.

`registered` is the count of the attempt that landed: what that write
changed. An effect another worker registered in between counts 0, and if
nothing is left to change, nothing is written (the host's input on #159).

### 5.4 The count

On the counter the fleet already records,
`rakka.agent_workflow.dispatcher.fleet`. Each lost compare-and-set is
counted once, by what followed it:

| `operation` | `outcome` | `detail` | Counts |
|---|---|---|---|
| `register`, `claim`, `settle`, `release`, `prune` | `reapplied` | `1`, `2`, `3` | a lost write that was read again; `detail` is which re-application of that write it led to |
| the same | `exhausted` | `none` | a lost write that returned `revision-conflict`, the bound spent |

`operation=settle` is new on this counter. All label values are fixed
strings: 20 series at most.

`reapplied` with `detail=1` is the number of writes read again at all, and
`exhausted` the number that ran out. `detail=3` is the number that came
within one loss of running out, which is the evidence the bound needs.

### 5.5 The pause

- The first re-application follows at once. A handle writes from the copy
  its last read left, which another worker's write makes old, so a first
  loss says the copy was old and nothing about contention.
- The second waits between 10 and 20 ms, the third between 20 and 40 ms,
  each uniformly at random. The whole wait is under 60 ms.
- The wait is before the read, so the read is as fresh as it can be when
  the write follows it.
- **It waits on the runtime's timer (`tokio::time`), never on the fleet's
  clock.** A deployment may hand the fleet a logical clock that moves only
  when it is told to (the host's input on #160). `rakka-agent-workflow`
  gains `tokio` as a direct dependency; `rakka-core` and `rakka-workflow`
  already have it.
- The randomness is the standard library's per-process hash seed. No crate
  is added.

What a pause does and does not do. A write is lost when another lands
between its read and its compare-and-set. A pause does not shorten that
window. It separates writers that lost to the same write and would
otherwise read again together and lose to each other. The window shrinks
with the record: pruning (#145) is what shortens it.

### 5.6 What gets more contended

A registration or a claim that lost used to leave the record alone until
its next pass. It now reads and writes again at once. That is more writes
against the same record, and the write it competes with is the settle,
whose loss is the one that costs something. The count is how to see it:
`operation=settle, outcome=exhausted` before and after.

## 6. Compatibility

- **Durable:** nothing. The record's bytes are what they were.
- **An earlier binary in the same fleet** fails its pass on a lost
  registration or claim as before, and does not count or wait.
- **Source:** one new public method,
  `AgentDispatcherFleet::registration_lost`. `rakka-agent-workflow` now
  needs a Tokio runtime with the timer enabled when a write is re-applied a
  second time.
- **Stable strings:** the label values `settle`, `reapplied`, `exhausted`,
  and `1`, `2`, `3` under `detail`.

## 7. Decisions

**The host's inputs (on the two issues), followed:** both writes; the
registered count is the landed write's; the pause is on a runtime timer and
small against a 6 s lease; labels by `operation` with an outcome for a
re-applied write and one for an exhausted one.

**Calls I made, for review:**

1. The registration's loop is in its two callers, not in the fleet
   (section 3).
2. Each lost compare-and-set is counted, not each write, with the
   re-application's number in `detail`. The host asked for "a write that
   landed after a re-read"; `reapplied, detail=1` is "a write that was read
   again", which differs by the few whose re-read ended some other way.
3. The series are on the existing fleet counter, so a deployment that
   already exports it gets them with no change.
4. The first re-application does not wait.
5. The bound stays 3. The count is what would justify moving it.
6. The pause's bounds are constants, not settings. The settings are part of
   a fleet's serialized configuration.
7. Release and prune are counted and wait like the rest.
8. The cancellation's two writes are left as they are: one try, not
   counted. No pass makes them.

## 8. Testing

Test-first, and each behaviour falsified by a mutation.

- Fleet: a claim re-applied from the record it read again (another worker's
  claim, a limit, the tokens); a claim that runs out; the count for each of
  the five writes, both outcomes; the pause's three cases on a paused
  runtime clock, with the fleet's clock standing still; `refresh_run`'s
  registration re-applied, reading its list again.
- Pass: a registration and a claim that each lose once and go on; a
  re-applied registration that must not put back an effect finished and
  pruned in between; one that counts 0 for an effect another worker
  registered; a registration that runs out.

## 9. Not done by this change

- The cancellation's two writes (`request_run_cancellation`,
  `finalize_run_cancellations`) are still one try.
- Nothing shortens the window a write can be lost in.
- A delivered ticket whose settle ran out is still ended `Cancelled` at its
  re-claim.
