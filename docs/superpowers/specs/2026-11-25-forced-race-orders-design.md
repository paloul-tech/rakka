# The knowledge graph's PostgreSQL race proofs force every order they claim (issue #136)

Short design. Branch `forced-race-orders`, off `main` at `9f9c9be6`.

## 1. Why

`crates/rakka-agent-knowledge-graph-postgres/tests/postgres_erasure_proofs.rs`
holds five proofs that depend on timing to reach the case they are about:

| Proof | How it reaches its case | What a slow or fast machine does to it |
| --- | --- | --- |
| `postgres_concurrent_tombstone_and_transition_stay_coherent_when_dsn_is_set` | 48 runs; the transition held back by a nominal `(run % 8) * 40` µs | One order may never occur; the proof still passes |
| `postgres_concurrent_delete_and_transition_stay_coherent_when_dsn_is_set` | The same | The same |
| `postgres_same_operation_races_converge_every_time_when_dsn_is_set` | 64 runs; the second racer held back by `(run % 16) * 25` µs | Nothing asserts a run landed anywhere but "serial" |
| `postgres_a_duplicate_append_cannot_restore_a_claim_deleted_while_it_waited_when_dsn_is_set` | Sleeps `BLOCKED_WRITER_PAUSE` (200 ms) for the duplicate to reach a lock | A duplicate slower than that sees committed state; the proof passes having proved the serial case |
| `postgres_an_operation_id_in_flight_for_another_claim_is_a_conflict_when_dsn_is_set` | The same pause | The same |

Issue #136 names the first two. The owner chose all five (2026-11-25).

A runtime timer is far coarser than the microsecond steps the code names, so
each step is really "up to about a millisecond, by timer phase". Across the
#68 builder's and reviewer's runs, 3 to 15 of 48 ended "transition first".

## 2. What was verified before designing

- **Every write is one statement that locks the claim row first.**
  `TRANSITION_SQL`, `TOMBSTONE_SQL` and `DELETE_SQL` each fence on
  `transition_count` in their first data-modifying CTE; every other CTE
  reads that one (`FROM upd`, `EXISTS (SELECT 1 FROM del)`), so the claim
  row is the first row each locks. Their reads (`WRITE_START_SQL`,
  `CLAIM_HISTORY_SQL`) are plain `SELECT`s, which a row lock does not block.
- **PostgreSQL lets the first writer queued on a held row lock go first.**
  The first waiter takes the tuple's lock and waits on the holder's
  transaction; a second waits on that tuple lock. Probed on the local 17.5
  (2026-11-25): a session holds `SELECT … FOR UPDATE`, two fenced `UPDATE`s
  queue in turn, the holder commits; six runs, alternating which queued
  first, and each time the first applied and the second matched nothing.
- **`pg_locks` shows both waits.** The probe counted two ungranted locks
  before each release (by PostgreSQL's design, the first waiter's on the
  holder's transaction id and the second's on the tuple). The helper
  `until_waiting_for_a_lock` in `postgres_damaged_history_proofs.rs` polls
  for exactly that, and `PostgresKnowledgeGraphStore::from_shared_client`
  lets a test read a store session's `pg_backend_pid()` first.
- **The case "a transition lands between an erasure's two reads" is already
  forced**, for both erasures, by
  `postgres_a_transition_between_an_erasures_two_reads_is_a_race_when_dsn_is_set`
  (a table lock in a private schema).
- **CI's `postgres` job runs these files on every pull request** against
  `pgvector/pgvector:pg16`, so the forced proofs do not sit skipped.

## 3. Goals, non-goals, anti-goals

- **Goal:** each interleaving these five proofs claim is forced at the
  database and asserted, on every run, on any machine.
- **Goal:** a proof that cannot reach its interleaving fails, rather than
  passing on the serial case.
- **Goal:** each forced proof is shown to fail when the guard it is about is
  removed, except where no such guard exists (stated in the proof).
- **Non-goal:** any change to product code. Test and docs only.
- **Non-goal:** the other proof files' timing. Only this file's five.
- **Anti-goal:** a sleep or a nominal step anywhere a proof waits for a
  writer to reach a lock.
- **Anti-goal:** a branch that is asserted only when it happens to occur.

## 4. Approaches

- **A. Hold the row (or, where the window is between two reads, a table)
  from a second session, and wait on `pg_locks` until each writer is
  queued.** The issue's own recipe; the `pg_locks` wait is already used by
  the #137/#138 proofs. No product change. **Chosen.**
- **B. A test-only pause hook inside the store** (a `cfg(test)` or feature
  that stops an operation between its read and its write). Exact, but adds
  test seams to product code, and an integration test cannot reach a
  `cfg(test)` item without a feature that ships in the crate.
- **C. Keep the loops and assert a minimum of each order.** Still timing:
  a floor that holds here can fail on a loaded CI runner, and raising the
  run count only makes that less likely.

## 5. The design

### 5.1 Shared helpers (in the test file)

- **Every forced proof runs in a private schema of its own** (amended while
  planning). Every other proof in the file calls `migrate()` on the shared
  schema at its start, and a repeat migration takes a brief `SHARE` lock on
  the claim table (`CREATE INDEX IF NOT EXISTS`). Such a lock request,
  queued ahead of a writer, can let the two writers reach the row together
  after the release, or leave a proof waiting on a migrator that waits on
  the proof. No other proof migrates or writes a private schema, so the
  only locks in it are the proof's own.
- A world of three store sessions (the first writer, the second writer, the
  holder), each with its backend id, and an observer session, all in that
  schema; dropped at the end.
- `until_blocked_by(observer, waiter, blocker)`: polls
  `$2 = ANY(pg_blocking_pids($1))` until it holds, panicking after a bound
  (about ten seconds) with both ids in the message. This is stricter than
  the damaged-history file's "any ungranted lock": a first writer blocked by
  the holder is at the row and holds its tuple lock, so it is first in the
  queue.
- `hold_claim_row(holder, scope, claim_id)`: `BEGIN` plus
  `SELECT 1 FROM rakka_agent_knowledge_claim WHERE … FOR UPDATE`; released
  by `COMMIT`.
- `queued_in_order(first, second)`: given two store calls on the first and
  second sessions, holds the row, starts `first`, waits until it is blocked
  by the holder, starts `second`, waits until it is blocked by the first,
  commits, and returns both answers. Both have read the claim before either
  writes.

### 5.2 Tombstone against transition, and delete against transition

Two proofs replace the two 48-run loops, one per erasure, each running both
orders on a fresh claim:

| Erasure | Queued first | Asserted |
| --- | --- | --- |
| tombstone | transition | The transition answers `Disputed`. The tombstone lands on retry: the stub lists the transition, its count is 1, and the transition's ledger row is `erased` with no result |
| tombstone | tombstone | The transition answers `claim-tombstoned`. The stub is `Proposed` with no history, and the transition has no ledger row |
| delete | transition | The transition answers `Disputed`. The delete lands on retry: no claim row and no transition row remains, and both the append's and the transition's ledger rows are `erased` |
| delete | delete | The transition answers `claim-not-found` and has no ledger row; the append's ledger row is `erased` |

Every case also asserts that no row of the scope holds any of the four
distinctive statement strings, as the loops did.

### 5.3 Same operation, two sessions

Three proofs replace the 64-run loop (the third added after the review; see
section 7):

- **Stopped at its first read.** For each of transition, tombstone and
  delete, in a private schema: the holder locks the claim table
  (`ACCESS EXCLUSIVE`), so one session's write-start read waits before it
  takes its snapshot; the holder runs the same request inside its
  transaction and commits. The waiting caller then sees the ledger row and
  the claim together and answers the holder's result. Asserted: the two
  answers are equal.
- **Queued at the write.** For each of transition, tombstone and delete:
  both sessions run the same request, queued in order on the held row. The
  second's fenced write matches nothing, and it answers from the ledger the
  first wrote. Asserted: both answers are equal (the one outcome, the one
  stub, success twice).
- **Between an erasure's two reads.** For tombstone and delete, in a private
  schema: a holder session locks the transition table (`ACCESS EXCLUSIVE`),
  so one session's erasure answers its first read and waits at its history
  read; the holder runs the same request through a store on its own
  session, inside its transaction, and commits. The waiting session finds
  the claim moved, reads again, and answers the holder's result. Asserted:
  the two answers are equal. Transition has one read, so it has no such
  window.

The private schema is dropped at the end. As in the damaged-history file, a
proof that panics leaves it behind.

### 5.4 The two pause-based proofs

`BLOCKED_WRITER_PAUSE` is removed. Each proof moves into a private world,
runs its raw transaction on the holder session, and, where it slept, waits
with `until_blocked_by` until the store's session is blocked by the holder:
the duplicate append on the original's uncommitted claim row, the second
delete or tombstone on the uncommitted ledger row. Their assertions do not
change.

### 5.5 Falsification

Each forced proof is run against a temporary break of the guard it is
about, reverted after. The break must fail the proof on every run:

| Break | Must fail |
| --- | --- |
| `TOMBSTONE_SQL` without its `transition_count` fence | tombstone, transition queued first |
| `TRANSITION_SQL` without its fence | tombstone, tombstone queued first |
| `DELETE_SQL` without its fence | delete, transition queued first |
| `WRITE_START_SQL` without its ledger join, so a retry sees no ledger row | same operation queued at the write |
| An erasure that treats a moved claim as damaged instead of reading again | same operation between the two reads |
| `write_start` split into a ledger read and then a claim read (the shape the #68 design's 12.3 fixed) | same operation stopped at its first read |
| `APPEND_SQL`'s ledger insert with `ON CONFLICT DO NOTHING` again | duplicate append |
| `TOMBSTONE_SQL`'s and `DELETE_SQL`'s ledger insert with `ON CONFLICT DO NOTHING` | operation id in flight for another claim |

Delete queued first has no such guard: a deleted row matches no `UPDATE`,
whatever its condition says. The proof says so in its comment.

Each forced proof is also run 20 times in a row on both databases, to show
the queue order holds.

### 5.6 Docs

- `docs/rakka-agent-security-validation-matrix.md`: the race rows name the
  new proofs and say both orders are forced; the same-operation row says
  where each racer is stopped. The dated attestation is re-run on
  PostgreSQL 16.14 with pgvector 0.8.5 (with
  `RAKKA_POSTGRES_PGVECTOR_REQUIRED=1`) and on 17.5, and its per-file
  counts updated.
- The #68 design's section 12.5 says the deterministic version "is not
  built (deferred)"; a line points to this issue.
- `CHANGELOG.md`, under Validation: one line.

## 6. Calls made by the builder

- The two 48-run loops and the 64-run loop are removed, not kept. With 5.2,
  5.3 and the damaged-history proof, every interleaving they could reach is
  forced somewhere, and a loop whose branches are asserted elsewhere cannot
  fail for a reason of its own. As first built, this was true only of the
  interleavings today's code has: see section 7.
- One proof per erasure, each running both orders, rather than one proof
  per order. It keeps the file's existing shape (one proof per race).
- The helpers are copied into this file rather than shared. Test files in
  the crate share no module today, and adding one changes nothing a reader
  needs.

## 7. After the whole-branch review

One fresh reviewer read the branch at `5c78f533`: no critical finding, one
important, five minor. The builder re-graded one minor to important.

| Finding | Fix | Proof |
| --- | --- | --- |
| Removing the 64-run loop removed the only proof that fails if `write_start` reads the ledger and the claim in two statements again (the #68 design's 12.3 defect). Section 6's claim held only for today's one-statement read: neither new same-operation proof stops a caller before that read. With the read split, the branch's suite passed and the base's loop failed, ten runs of ten. | A third same-operation proof stops the caller at its first read (5.3). | `postgres_a_same_operation_racer_stopped_at_its_first_read_answers_the_winners_result_when_dsn_is_set`: against a split read it failed 6 runs of 6 (3 per database), every operation refused as 12.3 describes, while the other 17 proofs passed; against the branch it passed 20 runs of 20 on each database. |
| **Re-graded from minor.** The matrix row "ends as the writer that reached the claim's row first left it" is false when the transition reaches the row first: the race ends with a stub, or no claim, once the erasure reads again. | The row says the race ends as if the two had run one after the other, in the order they reached the row. | Docs only. |

Deferred minors, not fixed: the between-reads proof checks that the racer
is blocked by the holder, not that it is at its history read; a writer that
fails or finishes at once costs the full poll bound before the proof says
so; the bound is about 16 seconds as measured, not ten, and the diagnostic
labels an idle session's last statement "running"; a failed run's schema
name is not printed.
