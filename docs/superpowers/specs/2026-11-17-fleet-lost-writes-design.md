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
- A re-application waits a short random time first.
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
`rakka.agent_workflow.dispatcher.fleet`. Each lost compare-and-set of the
five writes is counted once, by what followed it:

| `operation` | `outcome` | `detail` | Counts |
|---|---|---|---|
| `register`, `claim`, `settle`, `release`, `prune` | `reapplied` | `1`, `2`, `3` | a lost write that was read again; `detail` is which re-application of that write it led to |
| the same | `revision-conflict` | `none` | a lost write that was returned under that code, the bound spent |

`operation=settle` is new on this counter. All label values are fixed
strings: 20 series at most.

`reapplied` with `detail=1` is the number of writes read again at all, and
`revision-conflict` the number that ran out. `detail=3` is the number that
came within one loss of running out, which is the evidence the bound needs.

### 5.5 The pause

- A re-application waits a random time before it reads again: between 5
  and 10 ms before the first, 10 and 20 ms before the second, 20 and 40 ms
  before the third. One write asks for under 70 ms in all.
- **A settle's first re-application follows at once.** A settle is written
  from the copy its claim left, which is as old as the call it settles, so
  its first loss says the copy was old and nothing about contention. Every
  other write follows a read, so its first loss is already a write that got
  in between. A settle asks for under 60 ms in all.
- The wait is before the read, so the read is as fresh as it can be when
  the write follows it.
- **It never waits on the fleet's clock.** A deployment may hand the fleet
  a logical clock that moves only when it is told to (the host's input on
  #160).
- **What does the waiting is replaceable, and the default needs no
  runtime.** `AgentFleetWritePause::Thread`, the default, starts a thread
  that sleeps and wakes the task. `AgentFleetWritePause::with` takes the
  application's own wait. No dependency is added.
- The randomness is the standard library's hash keys, seeded for each
  thread and moved on for each draw. No crate is added.

What a pause does and does not do. A write is lost when another lands
between its read and its compare-and-set. A pause does not shorten that
window. It separates writers that lost to the same write and would
otherwise read again together and lose to each other. The window shrinks
with the record: pruning (#145) is what shortens it.

### 5.6 What gets busier

A registration or a claim that lost used to leave the record alone until
its next pass. It now reads the whole record and tries again within tens of
milliseconds. A compare-and-set that loses moves nothing, so what a settle
can lose to is still only a write that lands, and the writes that land are
the ones that would have landed a pass later. They land sooner and closer
together, and there are more reads and more attempts.

A settle that waits does so while its claim's lease runs. A lease with
under 60 ms left can lapse in the wait, and the settle is then answered
`claim-fenced`.

None of this is measured under load. The count is how to see it:
`operation=settle, outcome=revision-conflict` before and after.

## 6. Compatibility

- **Durable:** nothing. The record's bytes are what they were.
- **An earlier binary in the same fleet** fails its pass on a lost
  registration or claim as before, and does not count or wait.
- **Source:** `AgentDispatcherFleet::registration_lost`;
  `AgentFleetWritePause` and `AgentFleetPauseFuture`;
  `AgentDispatcherFleet::with_write_pause` and `set_write_pause`;
  `AgentRunEffectDispatcher::with_fleet_write_pause`. No signature changes
  and no dependency is added.
- **Stable strings:** the label values `settle`, `reapplied`, and `1`, `2`,
  `3` under `detail`; `revision-conflict` as an `outcome`.

## 7. Decisions

**The host's inputs (on the two issues), followed:** both writes; the
registered count is the landed write's; the pause never waits on the
fleet's clock, is small against a 6 s lease, and can be replaced by the
application; labels by `operation` with an outcome for a re-applied write
and one for a write that ran out.

**Calls I made, for review:**

1. The registration's loop is in its two callers, not in the fleet
   (section 3).
2. Each lost compare-and-set is counted, not each write, with the
   re-application's number in `detail`. The host asked for "a write that
   landed after a re-read"; `reapplied, detail=1` is "a write that was read
   again", which differs by the few whose re-read ended some other way.
3. The series are on the existing fleet counter, so a deployment that
   already exports it gets them with no change.
4. Only a settle's first re-application does not wait.
5. The bound stays 3. The count is what would justify moving it.
6. The pause's bounds are constants, not settings. The settings are part of
   a fleet's serialized configuration.
7. Release and prune are counted and wait like the rest.
8. The cancellation's two writes are left as they are: one try, not
   counted. No pass makes them.
9. The default wait is a thread, not the runtime's timer (section 11).
10. A write that ran out is counted as `outcome=revision-conflict`, the
    code it returns, not `exhausted`, which this counter already uses for a
    ticket out of attempts.

## 8. Testing

Test-first, and each behaviour falsified by a mutation.

- Fleet: a claim re-applied from the record it read again (another worker's
  claim, a limit, the tokens); a claim that runs out; the count for each of
  the five writes, both outcomes; the waits each write asks for, through a
  wait that records them; the default wait on a bare executor, with the
  fleet's clock standing still; `refresh_run`'s registration re-applied,
  reading the record and then its list again.
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

## 10. As built

Where the build moved from the sections above.

1. **The wait is in whole milliseconds.** The first build drew
   microseconds. Timers are seldom finer than a millisecond and round up,
   so a wait of 39.9 ms took 40. The waits asked for are 5 to 9, 10 to 19
   and 20 to 39 ms.
2. **`claim_due` is two functions.** `claim_due_once` is one attempt from
   the handle's copy, the old body unchanged; `claim_due` is the loop. The
   counts of an attempt are recorded inside the attempt, after its write,
   so a lost attempt records none.
3. **One private rule, one public door.** `after_failed_write` decides and
   counts, `reapply` adds the wait, and the four loops inside the fleet
   call `reapply`. `registration_lost` is `reapply` under
   `operation=register`, for the two callers that loop outside the fleet.
4. **The bound is the label table.** `FLEET_REAPPLICATION_LABELS` has one
   label for each re-application the bound allows, and a loss is
   re-applied while there is a label for it. The bound cannot be changed
   without the labels.
5. **Both cold paths are boxed.** The wait is a boxed future by its type,
   and the call to `registration_lost` in `register_run_into` is boxed. A
   pass's future is already within about 100 KiB of a test thread's stack
   in one example (`coordination-capability-acceptance`), and neither path
   runs unless a write was lost. Measured on this branch before the
   review's changes: the example passes with 1984 KiB and fails with 1920,
   the same bracket as main.
6. **`refresh_run` repeats only its registration.** The cancellation steps
   before it ran against the first read. What a re-read shows newly fenced
   is finished by the next refresh.
7. **A fleet record that does not exist yet cannot be contended** by the
   test stores: there is no revision to move. The registration tests
   register another run first.

## 11. The review

One fresh reviewer read the branch at `cbdf94f` and wrote seven probe
tests. No Critical finding. What it found, and what was done:

| Finding | Outcome |
|---|---|
| **Important.** The pause was `tokio::time::sleep`. Outside a Tokio runtime, or on one built without its timer, the second lost write in a row panicked: a settle, a release and a prune had re-applied without a timer before. Proved | Fixed. The wait is replaceable and the default is a thread, which needs no runtime. The `tokio` dependency is gone. `the_default_wait_needs_no_runtime_and_lasts_the_time_asked` and `every_write_can_run_out_with_no_runtime_under_it` run on a bare executor |
| **Important.** The product doc said a settle that ran out leaves a delivered ticket that its re-claim ends `Cancelled`. That is one of three cases: after a call that spent its attempts the run is not told until the re-claim, which ends the entry `Exhausted`. Proved | Fixed in the product doc and the compatibility entry, and three tests hold the three cases |
| **Important.** "A first loss says the copy was old" is true of a settle only. A prune, a release, the dispatcher's registration and its claim all write right after a read, so their first loss is contention, and they re-applied with no spread | Fixed. Every write but the settle waits before its first re-application too |
| Minor. No test held the order of the two reads in `refresh_run`'s re-application: swapped, every test passed | Test added, `a_refresh_made_again_reads_the_record_before_it_reads_its_list` |
| Minor. `outcome=exhausted` already meant a ticket out of attempts on this counter | Renamed to `outcome=revision-conflict` |
| Minor. "Every lost compare-and-set is counted" was wider than the code: `register_due_effects` alone counts nothing | Docs narrowed to the five writes, and say so |
| Minor. The stated cost had the mechanism wrong: a compare-and-set that loses moves nothing, so a settle has no more writes to lose to, only sooner ones | Section 5.6 and the docs rewritten. The reviewer's narrow case, a settle whose lease lapses in its wait, is added |
| Minor. Doc sentences wider than the code: "unlike every other write", "reads the fleet record once", "per-process seed", "under 60 ms" as wall time | Each corrected |
| Minor. Test nits: a vacuous assertion; no test for a cancellation requested between a lost registration and its re-read; none for the runtime | The first removed, the other two added |
| Minor. `registration_lost` keeps its bound in a counter the caller holds; a caller that passes the same number every time is never told to stop | Kept, and its doc now says so. A handle cannot tell one registration's tries from the next one's |

Not changed by the review and still true: the reviewer's
`concurrency_limited` assertion that does not discriminate is left in, with
its neighbours that do.
