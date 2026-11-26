# Forced race orders (issue #136) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** every interleaving the five timing-dependent proofs in `postgres_erasure_proofs.rs` claim is forced at the database and asserted on every run.

**Architecture:** test-only. Each forced proof gets a private schema with three store sessions and an observer. Writers queue on a claim row that the holder session keeps locked, in a chosen order. The proof checks `pg_blocking_pids` until each writer is blocked by the session it should be blocked by. Each proof is then shown to fail when the safeguard it checks is temporarily removed.

**Tech Stack:** Rust 1.88, `tokio`, `tokio_postgres`, PostgreSQL 16 (pgvector 0.8.5) and 17.5.

**Spec:** `docs/superpowers/specs/2026-11-25-forced-race-orders-design.md`

## Global Constraints

- No product code changes. Every break in a falsification step is reverted before its task commits; at the end `git diff --stat main -- crates/rakka-agent-knowledge-graph-postgres/src` prints nothing.
- No `tokio::time::sleep` and no nominal step anywhere a proof waits for a writer to reach a lock. The one sleep left in the file is the poll interval inside `until_blocked_by`.
- Every proof is gated on `RAKKA_POSTGRES_TEST_DSN` and passes silently without it (`let Some(world) = lock_world("…").await else { return };`).
- `cargo fmt --all -- --check` and `cargo clippy -p rakka-agent-knowledge-graph-postgres --all-targets --all-features -- -D warnings` stay clean.
- Databases: PG 17.5 at `postgres://postgres:postgres@localhost:5432/postgres` (the long-running container, no pgvector); PG 16 + pgvector 0.8.5 as a throwaway: `docker run -d --name rakka-pgvector-check -e POSTGRES_PASSWORD=postgres -p 5433:5432 pgvector/pgvector:pg16`, then `docker exec rakka-pgvector-check psql -U postgres -c "create extension if not exists vector;"`; `docker rm -f rakka-pgvector-check` when Task 4 is done.
- A proof that panics leaves its schema behind. After every falsification step, drop the `rakka_knowledge_graph_<label>_%` schemas this branch's labels left (list them first with `\dn rakka_knowledge_graph_*`). Never drop a schema whose label this branch did not create.
- `cargo clean` at each task boundary, after the commit (disk pressure).
- `scripts/validate.sh`: redirect to a file and read `$?` directly, never through a pipe.

## Review Focus

1. **Proofs running in parallel in one test binary** (the default, and how CI runs). A `migrate()` from another proof must not change a forced order. Owned by Task 1 (private schemas); the test is Task 1 Step 6 (the whole file, ten times in a row).
2. **A writer that never queues**: a product change that stops taking the row lock, or the wrong session handed to `queued_in_order`. The proof must fail within its bound, naming both backend ids, and not hang the suite. Owned by Task 1; the test is Task 1 Step 5, last row.
3. **No DSN**: every proof returns silently. Owned by every task; the test is Task 4 Step 5 (`validate.sh` runs without the DSN).
4. **PostgreSQL 16 against 17**: the queue order must hold on both. Owned by Tasks 1–3; each runs its forced proofs 20 times on each database.
5. **A failed run leaves a private schema behind**: harmless on CI's fresh database, but it piles up on the shared local one. Owned by every falsification step (drop what the run left).

---

### Task 1: The world, the helpers, and the two erasure-against-transition proofs

**Files:**
- Modify: `crates/rakka-agent-knowledge-graph-postgres/tests/postgres_erasure_proofs.rs` (helpers after `ledger_row`, about `:455`; the two proofs at `:1021-1118` and `:1375-1465`; the constants at `:144-148`)

**Interfaces:**
- Produces (all private to the test file):
  - `struct Session { client: Arc<Client>, store: PostgresKnowledgeGraphStore, pid: i32 }`
  - `struct LockWorld { dsn: String, schema: String, first: Session, second: Session, holder: Session, observer: Client }`
  - `async fn lock_world(label: &str) -> Option<LockWorld>`: `None` without the DSN. Creates `unique_schema(label)`, opens three sessions with `client_in`, reads each one's `pg_backend_pid()`, builds each store with `PostgresKnowledgeGraphStore::from_shared_client`, migrates once through `first.store`, and opens `observer` with `client_in`.
  - `async fn drop_world(world: LockWorld)`: `DROP SCHEMA … CASCADE` from a fresh `connect(&world.dsn)`.
  - `async fn until_blocked_by(observer: &Client, waiter: i32, blocker: i32)`: polls `SELECT $2 = ANY(pg_blocking_pids($1))` every 10 ms, up to 1,000 times; on running out, panics with `"session {waiter} was never blocked by session {blocker}"`.
  - `async fn hold_claim_row(holder: &Client, scope: &KnowledgeSpaceScope, claim_id: &ClaimId)`: `BEGIN`, then `SELECT 1 FROM rakka_agent_knowledge_claim WHERE tenant = $1 AND space = $2 AND claim_id = $3 FOR UPDATE`, asserting exactly one row.
  - `async fn queued_in_order<A, B>(world: &LockWorld, scope: &KnowledgeSpaceScope, claim_id: &ClaimId, first: impl Future<Output = A> + Send + 'static, second: impl Future<Output = B> + Send + 'static) -> (A, B)` where `A, B: Send + 'static`. `first` must run on `world.first.store`, `second` on `world.second.store`. In order: `hold_claim_row` on the holder, `tokio::spawn(first)`, `until_blocked_by(first.pid, holder.pid)`, `tokio::spawn(second)`, `until_blocked_by(second.pid, first.pid)`, `COMMIT` on the holder, then await both handles.
  - `enum Order { TransitionFirst, ErasureFirst }`, which Task 2 does not use.

- [ ] **Step 1: Write the two proofs**, replacing `postgres_concurrent_tombstone_and_transition_stay_coherent_when_dsn_is_set` and `postgres_concurrent_delete_and_transition_stay_coherent_when_dsn_is_set`, and deleting `TOMBSTONE_RACE_RUNS` and `DELETE_RACE_RUNS`:
  - `postgres_a_tombstone_and_a_transition_queued_on_one_claim_end_as_they_queued_when_dsn_is_set` (label `queued_tombstone`)
  - `postgres_a_delete_and_a_transition_queued_on_one_claim_end_as_they_queued_when_dsn_is_set` (label `queued_delete`)

  Both use `#[tokio::test(flavor = "multi_thread", worker_threads = 2)]`. Each loops over both `Order`s, with a fresh `ConformanceScopes::unique(…)` scope and a fresh `distinctive_claim` appended through `world.first.store`. The writer queued first runs on `world.first.store`, the other on `world.second.store`. The transition is `transition_request(&scope, &claim.claim_id, "dispute", ClaimTrustStatus::Disputed)` with `ClaimPromotionPolicy::ungated()`. After `queued_in_order`, assert exactly the rows of spec 5.2 for that erasure and order, each message prefixed with the order:

  ```rust
  // tombstone, TransitionFirst
  assert_eq!(transitioned.expect("the transition lands").claim.trust(), ClaimTrustStatus::Disputed);
  // stub == get(); stub.is_tombstoned(); stub.trust() == Disputed;
  // stub.transition_count() == 1; history.transitions.len() == 1;
  // ledger_row(dispute.operation_id) == Some(("erased", empty))
  // tombstone, ErasureFirst
  // transitioned.unwrap_err().code() == "claim-tombstoned";
  // stub.trust() == Proposed; history empty; ledger_row(dispute) == None
  // delete, TransitionFirst
  // transitioned Ok with trust Disputed; get() == None;
  // every scope row starts with b"op|"; ledger_row(claim.operation_id).0 == "erased";
  // ledger_row(dispute.operation_id) == Some(("erased", empty))
  // delete, ErasureFirst
  // transitioned.unwrap_err().code() == "claim-not-found"; get() == None;
  // every scope row starts with b"op|"; ledger_row(claim.operation_id).0 == "erased";
  // ledger_row(dispute.operation_id) == None
  // every case:
  assert_eq!(statement_strings_in(&scope_rows(&world.observer, &scope).await), Vec::<&str>::new());
  ```

  The erasure itself must succeed in every case (`tombstone_request(&scope, &claim.claim_id, "withdraw")`, `delete_request(&scope, &claim.claim_id, "erase", None)`). The delete proof's doc comment states that delete-first has no fence to falsify: a deleted row matches no `UPDATE`, whatever its condition says. Each proof's doc comment says which case of #136 it forces and that "a transition lands between an erasure's two reads" is forced in `postgres_damaged_history_proofs.rs`. `drop_world` at the end.

- [ ] **Step 2: Run against both databases**

  Run: `RAKKA_POSTGRES_TEST_DSN=<dsn> cargo test -p rakka-agent-knowledge-graph-postgres --test postgres_erasure_proofs queued_on_one_claim -- --nocapture`, with each of the two DSNs.
  Expected: `2 passed`.

- [ ] **Step 3: Run each proof 20 times on each database**

  Run: `for i in $(seq 20); do RAKKA_POSTGRES_TEST_DSN=<dsn> cargo test -q -p rakka-agent-knowledge-graph-postgres --test postgres_erasure_proofs queued_on_one_claim || { echo "FAILED on run $i"; break; }; done`
  Expected: no `FAILED` line, on either database.

- [ ] **Step 4: Format and lint**

  Run: `cargo fmt --all && cargo clippy -p rakka-agent-knowledge-graph-postgres --all-targets --all-features -- -D warnings`
  Expected: no warnings.

- [ ] **Step 5: Falsify** (PG 17.5 is enough; five runs per break; revert each break before the next)

  | Break | Run | Expected |
  | --- | --- | --- |
  | Drop `AND transition_count = $8` from `TOMBSTONE_SQL` (`src/erasure.rs`) | the tombstone proof | fails in `TransitionFirst` |
  | Drop `AND transition_count = $8` from `TRANSITION_SQL` (`src/lib.rs`) | the tombstone proof | fails in `ErasureFirst` |
  | Drop `AND transition_count = $5` from `DELETE_SQL` | the delete proof | fails in `TransitionFirst` |
  | Hand `queued_in_order` a `first` built on `world.second.store` (test edit) | the tombstone proof | panics within about 10 s, naming both backend ids |

  Then `git diff --stat -- crates/rakka-agent-knowledge-graph-postgres/src` must print nothing. Drop the schemas the failed runs left. Write each break's observed result down in the commit message body.

- [ ] **Step 6: Run the whole file ten times on PG 17.5** (the proofs alongside every other proof's `migrate()`)

  Run: `for i in $(seq 10); do RAKKA_POSTGRES_TEST_DSN=<17.5 dsn> cargo test -q -p rakka-agent-knowledge-graph-postgres --test postgres_erasure_proofs || { echo "FAILED on run $i"; break; }; done`
  Expected: no `FAILED` line.

- [ ] **Step 7: Commit**

  ```bash
  git add crates/rakka-agent-knowledge-graph-postgres/tests/postgres_erasure_proofs.rs
  git commit -m "Force both orders of an erasure racing a transition on PostgreSQL (issue #136)"
  ```
  Then `cargo clean`.

---

### Task 2: The two same-operation proofs

**Files:**
- Modify: `crates/rakka-agent-knowledge-graph-postgres/tests/postgres_erasure_proofs.rs` (replace `postgres_same_operation_races_converge_every_time_when_dsn_is_set`, about `:1568-1636`; delete `SAME_OPERATION_RACE_RUNS`)

**Interfaces:**
- Consumes from Task 1: `LockWorld`, `lock_world`, `drop_world`, `until_blocked_by`, `queued_in_order`, `Session`.

- [ ] **Step 1: Write `postgres_a_same_operation_racer_queued_behind_the_winner_answers_its_result_when_dsn_is_set`** (label `queued_same_op`, multi-thread flavor). For each of transition (`"dispute"` to `Disputed`), tombstone (`"withdraw"`) and delete (`"erase"`, `None`), on a fresh `conformance_claim` in one scope: the same request on `world.first.store` (first) and `world.second.store` (second), via `queued_in_order`. Assert:

  ```rust
  assert_eq!(first.expect("the first lands"), second.expect("the second answers")); // the one outcome / the one stub; delete: () twice
  ```

  The doc comment says what the second does: its fenced write matches nothing, and its retry answers from the ledger the first wrote.

- [ ] **Step 2: Write `postgres_a_same_operation_racer_stopped_between_its_two_reads_answers_the_winners_result_when_dsn_is_set`** (label `between_reads_same_op`). For tombstone and delete, each on a fresh claim:
  - `world.holder.client.batch_execute("BEGIN; LOCK TABLE rakka_agent_knowledge_claim_transition IN ACCESS EXCLUSIVE MODE")`;
  - spawn the request on `world.first.store`;
  - `until_blocked_by(&world.observer, world.first.pid, world.holder.pid)`;
  - run the same request on `world.holder.store`, inside the holder's transaction;
  - `COMMIT`;
  - await the spawned answer.

  Assert the two answers are equal (the stub; `Ok(())` twice). The doc comment says transition has one read, so it has no such window, and that the waiting erasure's history read finds the fence moved (`Moved`) and reads again.

- [ ] **Step 3: Run against both databases, then 20 times on each**

  Run: Task 1 Step 2's and Step 3's commands with filter `same_operation_racer`.
  Expected: `2 passed`; no `FAILED` line.

- [ ] **Step 4: Format and lint** (Task 1 Step 4's command)

- [ ] **Step 5: Falsify** (as in Task 1 Step 5)

  | Break | Run | Expected |
  | --- | --- | --- |
  | Add `AND FALSE` to `WRITE_START_SQL`'s ledger join condition (`ON op.tenant = $1 … AND FALSE`), so a retry sees no ledger row | the queued-behind proof | fails for all three operations (run it once per operation by commenting the others out, or read the first failure's message and check the loop order) |
  | In both `tombstone_claim` and `delete_claim`, replace `ErasableHistory::Moved => continue,` with `ErasableHistory::Moved => return Err(ClaimError::HistoryDamaged { claim_id: request.claim_id.clone() }),` | the between-reads proof | fails with `claim-history-damaged` |

  Revert; `git diff --stat -- crates/rakka-agent-knowledge-graph-postgres/src` prints nothing. Drop the schemas left behind.

- [ ] **Step 6: Commit**

  ```bash
  git commit -am "Force the two orders of a same-operation race on PostgreSQL (issue #136)"
  ```
  Then `cargo clean`.

---

### Task 3: The two pause-based proofs wait for the lock

**Files:**
- Modify: `crates/rakka-agent-knowledge-graph-postgres/tests/postgres_erasure_proofs.rs` (`BLOCKED_WRITER_PAUSE` at about `:1638-1641`; `postgres_a_duplicate_append_cannot_restore_a_claim_deleted_while_it_waited_when_dsn_is_set`; `postgres_an_operation_id_in_flight_for_another_claim_is_a_conflict_when_dsn_is_set`)

**Interfaces:**
- Consumes from Task 1: `lock_world`, `drop_world`, `until_blocked_by`, `Session`.

- [ ] **Step 1: Move both proofs into a `lock_world`** (labels `duplicate_append` and `id_in_flight`, multi-thread flavor). The raw transaction (`earlier_append`, the raw `DELETE` and ledger `UPDATE`, the raw ledger `INSERT`) runs on `world.holder.client`. The store under test is `world.first.store`, and `scope_rows` reads through `world.observer`. Replace each `tokio::time::sleep(BLOCKED_WRITER_PAUSE)` with `until_blocked_by(&world.observer, world.first.pid, world.holder.pid).await`. Delete `BLOCKED_WRITER_PAUSE` and its doc comment. Leave every assertion as it is. Each doc comment says the proof goes on only once the store's session is blocked by the holder, so it can no longer pass on the committed state.

- [ ] **Step 2: Run against both databases, then 20 times on each**

  Run: Task 1 Step 2's and Step 3's commands with filter `while_it_waited` and then `in_flight_for_another_claim`.
  Expected: `1 passed` each; no `FAILED` line.

- [ ] **Step 3: Format and lint** (Task 1 Step 4's command)

- [ ] **Step 4: Falsify**

  | Break | Run | Expected |
  | --- | --- | --- |
  | Add `ON CONFLICT (tenant, space, operation_id) DO NOTHING` to `APPEND_SQL`'s `ins_op` | the duplicate-append proof | fails on every run: the duplicate returns the restored claim |
  | Add the same clause to `DELETE_SQL`'s `ins_op` | the in-flight proof | fails in its delete iteration |
  | Add the same clause to `TOMBSTONE_SQL`'s `ins_op` only | the in-flight proof | passes its delete iteration, fails in its tombstone iteration |

  Revert; `git diff --stat -- crates/rakka-agent-knowledge-graph-postgres/src` prints nothing. Drop the schemas left behind.

- [ ] **Step 5: Check the anti-goal**

  Run: `grep -n "sleep" crates/rakka-agent-knowledge-graph-postgres/tests/postgres_erasure_proofs.rs`
  Expected: exactly one hit, inside `until_blocked_by`.

- [ ] **Step 6: Commit**

  ```bash
  git commit -am "Wait for the lock, not a pause, in the two blocked-writer proofs (issue #136)"
  ```
  Then `cargo clean`.

---

### Task 4: Docs, the attestation, and validation

**Files:**
- Modify: `docs/rakka-agent-security-validation-matrix.md` (the erasure-proofs table near `:387-405`; the attestation paragraph near `:452-458`)
- Modify: `docs/superpowers/specs/2026-11-13-knowledge-graph-retention-tombstone-deletion-design.md` (section 12.5, last paragraph)
- Modify: `CHANGELOG.md` (`### Validation` under `## Unreleased`, first bullet)

- [ ] **Step 1: Matrix rows**
  - Replace the row "A tombstone or delete racing a transition ends in one of the two serial outcomes" with "A tombstone or delete racing a transition ends as the writer that reached the claim's row first left it; both orders are forced on every run", naming the two Task 1 proofs.
  - Replace "Two callers racing the same operation both get its one result" with "Two callers racing the same operation both get its one result, whether the second is queued behind the first's write or stopped between an erasure's two reads", naming the two Task 2 proofs.
  - Leave the other rows as they are.

- [ ] **Step 2: Re-run the attestation**

  Run, once per database (the 16.14 throwaway with `RAKKA_POSTGRES_PGVECTOR_REQUIRED=1`; 17.5 without it): `RAKKA_POSTGRES_TEST_DSN=<dsn> cargo test -p rakka-agent-knowledge-graph-postgres > <scratchpad>/attest-<db>.txt 2>&1; echo "exit=$?"`
  Expected: `exit=0` on both. From each file's `test result: ok. N passed` line, take the per-file counts. On 17.5, count the `did not run` announcements: `grep -c "did not run" <scratchpad>/attest-17.txt`.

- [ ] **Step 3: Rewrite the attestation paragraph** with today's date (2026-11-25), the measured per-file counts (the erasure file should go from 16 to 17; say so only if measured), and the measured 17.5 skip count. If any other file's count differs from the old paragraph for a reason outside this branch (for example #139's backend proof), use the measured number and do not explain it in the paragraph.

- [ ] **Step 4: Section 12.5 and the changelog**
  - Section 12.5: after "…is not built (deferred).", add: "Built later by issue #136: both orders of each race are forced at the database, and the 48-run loops are gone (`docs/superpowers/specs/2026-11-25-forced-race-orders-design.md`)."
  - CHANGELOG, first bullet under Validation: "`rakka-agent-knowledge-graph-postgres`: the erasure proofs force every race order they claim (issue #136). A tombstone or delete racing a transition, two callers racing one operation, and the two proofs that waited on a blocked writer now queue each writer on a held lock and check `pg_blocking_pids` before going on, each in a private schema. Before, the two erasure races mixed their orders with a sub-millisecond sleep and asserted each order only if it happened, and the other three could pass having reached only the serial case."

- [ ] **Step 5: Validate**

  Run: `scripts/validate.sh > <scratchpad>/validate.txt 2>&1; echo "exit=$?"` (without the DSN; if a workspace test is killed under load, re-run that crate alone before blaming the branch)
  Expected: `exit=0`. Then `git diff --stat main -- crates/rakka-agent-knowledge-graph-postgres/src` prints nothing, and `docker rm -f rakka-pgvector-check`.

- [ ] **Step 6: Commit**

  ```bash
  git add docs/rakka-agent-security-validation-matrix.md docs/superpowers/specs/2026-11-13-knowledge-graph-retention-tombstone-deletion-design.md CHANGELOG.md
  git commit -m "Name the forced race proofs and re-run the graph attestation (issue #136)"
  ```
  Then `cargo clean`.
