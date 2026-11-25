# A sweep reads past a row it cannot use: Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The private pgvector `reindex` and the two knowledge-graph passes read past a row whose id this build refuses, and `reindex` pages past a row whose write the database refuses for its data (issues #199, #200).

**Architecture:** Each pass keeps a raw-text cursor inside the call and reads on until it has handled `limit` rows whose id it reads, or the rows run out; refused-id rows are counted and do not count toward `limit`. `store_vector` reports a write refused for the row's data (SQLSTATE class 22/23, or a revision outside `BIGINT`) apart from any other failure, and `reindex` counts the first. `embedder_identity` refuses version 0 and a model holding a NUL, so a class 22/23 error can only come from the row.

**Tech Stack:** Rust 1.88, `tokio-postgres`, PostgreSQL 16 + pgvector 0.8.

**Spec:** `docs/superpowers/specs/2026-11-25-sweep-reads-past-refused-rows-design.md`

## Global Constraints

- MSRV 1.88. Workspace lints: `missing_docs`, `clippy::all` under `-D warnings`, `unsafe_code = "forbid"`. Every new item, private ones included, gets a doc comment in the crate's plain style.
- No public type, field or method is added or changed in shape. `ReindexPage`, `ClaimEmbeddingPage`, `ClaimSearchIndexPage` keep their fields.
- Error codes are a compatibility surface: `memory-embedding-invalid`, `memory-backend-failed`, `claim-backend-failed` keep their meaning.
- `index_memory` still returns a write refused for one memory as its error (`memory-backend-failed`).
- Gated runs: a throwaway `pgvector/pgvector:pg16` on port 5433, with `RAKKA_POSTGRES_PGVECTOR_REQUIRED=1` so a skip fails. Start it once before Task 1:
  ```sh
  docker run -d --name rakka-pgvector-check -e POSTGRES_PASSWORD=postgres -p 5433:5432 pgvector/pgvector:pg16
  docker exec rakka-pgvector-check psql -U postgres -c "create extension if not exists vector;"
  export RAKKA_POSTGRES_TEST_DSN=postgres://postgres:postgres@localhost:5433/postgres RAKKA_POSTGRES_PGVECTOR_REQUIRED=1
  ```
  Every `cargo test` command in this plan runs with those two variables set; a fresh shell must set them again. Remove the container after Task 4 (`docker rm -f rakka-pgvector-check`).
- `cargo clean` at every task boundary (owner rule, disk pressure), never while a cargo run is live.
- `scripts/validate.sh`: redirect to a file and read `$?`; never pipe it into `tail`. If the workspace test step is killed, run per crate.
- Never add `-D warnings` to the doc build; check new doc comments with `cargo doc -p <crate> --no-deps` locally.
- Do not edit `../paloul.rakka.host`.
- Commit messages end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Test ids holding `|` sort by the column's collation. The private `memory_id` column uses the database default, and `claim_id` is `COLLATE "C"`. Every test that plants one asserts the server's order first.

## Review Focus

1. `index_memory` for a memory whose write the table refuses still answers `memory-backend-failed`: it is how a caller learns why. Task 2, `pgvector_reindex_pages_past_a_memory_the_derived_table_refuses_when_dsn_is_set`.
2. A tombstoned or expired row with a refused id is never read, so it is never counted. Task 1, the `withdrawn` case.
3. A readable row the pass skips by policy (redacted), straight after a run of refused ids, still carries the cursor and counts toward `limit`. Task 1, the `redacted` case.
4. The identity check refuses a NUL and no other control character. A model named `model\tname` still indexes. Task 2, the identity test's last block.
5. A limit of 0 on the search-index pass (clamped to 1) still reads past a refused id to one claim. Task 3, the search-index test's second block.

---

### Task 1: The private pass reads past ids it refuses (#200)

**Files:**
- Modify: `crates/rakka-agent-postgres/src/retrieval.rs`: `reindex` (≈`:603-690`), its doc comment (≈`:543-602`), the module doc's reindex bullet (≈`:58-67`).
- Test: the same file's `mod tests`.

**Interfaces:**
- Consumes: nothing new.
- Produces: private `async fn unindexed_rows(&self, scope: &AgentScope, identity: &(MemoryEmbeddingRef, i32, i64), now_raw: i64, after_raw: &str, limit: usize) -> Result<Vec<tokio_postgres::Row>, MemoryError>`, which is the existing SELECT with `LIMIT` from `limit`. Test helpers `plant_refused_id` and `server_order`, which Task 2 does not need.

- [ ] **Step 1: Add the two test helpers beside `seed`**

```rust
/// Writes a copy of `source`'s row under `raw_id` past the store, as a
/// later build with a wider id alphabet would: an id this build refuses.
/// `tombstoned` writes the copy withdrawn.
async fn plant_refused_id(
    retriever: &PgvectorPrivateMemoryRetriever,
    owner: &AgentScope,
    source: &str,
    raw_id: &str,
    tombstoned: bool,
)
```
It asserts `AgentPrivateMemoryId::new(raw_id).is_err()`, then runs:
`INSERT INTO rakka_agent_private_memory (tenant, agent, memory_id, operation_id, revision, expires_at, tombstoned, record) SELECT tenant, agent, $3, operation_id || $3, revision, expires_at, $5, record FROM rakka_agent_private_memory WHERE tenant = $1 AND agent = $2 AND memory_id = $4`. Then it asserts one row was written.

```rust
/// The memory ids of one agent in the database's own order.
async fn server_order(retriever: &PgvectorPrivateMemoryRetriever, owner: &AgentScope) -> Vec<String>
```

- [ ] **Step 2: Write the failing tests**

```rust
#[tokio::test]
async fn pgvector_reindex_reads_past_ids_this_build_refuses_when_dsn_is_set() {
    // Issue #200's rows: a page made only of a refused id used to answer
    // no cursor, which ended the sweep before `mem-d` and `mem-e`.
    for limit in [1, 2] {
        let embedder = Arc::new(PickyEmbedder::refusing(None, Refusal::Errors));
        let Some((store, retriever)) = vector_world_over(embedder.clone()).await else { return };
        let owner = scope(&unique(), "support");
        seed(&store, &owner, &[("a", EMBEDDABLE), ("b", EMBEDDABLE), ("d", EMBEDDABLE), ("e", EMBEDDABLE)]).await;
        plant_refused_id(&retriever, &owner, "mem-b", "mem-c|x", false).await;
        assert_eq!(server_order(&retriever, &owner).await, ["mem-a", "mem-b", "mem-c|x", "mem-d", "mem-e"]);

        let expected = if limit == 1 {
            vec![page(1, 0, 0, Some("mem-a")), page(1, 0, 0, Some("mem-b")),
                 page(1, 0, 1, Some("mem-d")), page(1, 0, 0, Some("mem-e")), page(0, 0, 0, None)]
        } else {
            vec![page(2, 0, 0, Some("mem-b")), page(2, 0, 1, Some("mem-e")), page(0, 0, 0, None)]
        };
        assert_eq!(sweep(&retriever, &owner, limit).await.expect("the sweep ends"), expected, "limit {limit}");
        assert_eq!(embedded(&retriever, &owner).await, ["mem-a", "mem-b", "mem-d", "mem-e"]);
        assert_eq!(embedder.calls(), 4, "limit {limit}: no model call for the refused id");
        // Each refused row is counted once a sweep.
        assert_eq!(sweep(&retriever, &owner, limit).await.expect("ends"), vec![page(0, 0, 1, None)]);
    }
}

#[tokio::test]
async fn pgvector_reindex_reads_past_every_arrangement_of_refused_ids_when_dsn_is_set() {
    // (case, seeded, planted (source, raw id, tombstoned), redacted seed,
    //  limit, server order, pages, embedded, model calls)
}
```
Write the second test as a table, one fresh world per row. Seeded memories are `EMBEDDABLE`. A redacted seed is `memory(..)` with `classification = MemoryClassification::Redacted`, upserted with `PrivateMemoryExpectation::Absent`.

| case | seeded | planted (from `mem-a` unless noted) | limit | pages | embedded | calls |
|---|---|---|---|---|---|---|
| pair | a, b, e | `mem-c\|1`, `mem-c\|2` from `mem-b` | 2 | `(2,0,0,b)`, `(1,0,2,None)` | a, b, e | 3 |
| first | b, c | `mem-a\|x` from `mem-b` | 1 | `(1,0,1,b)`, `(1,0,0,c)`, `(0,0,0,None)` | b, c | 2 |
| last | a, b | `mem-z\|x` | 1 | `(1,0,0,a)`, `(1,0,0,b)`, `(0,0,1,None)` | a, b | 2 |
| run | a, d | `mem-c\|1` … `mem-c\|5` | 1 | `(1,0,0,a)`, `(1,0,5,d)`, `(0,0,0,None)` | a, d | 2 |
| withdrawn | a, b | `mem-a\|x`, tombstoned | 1 | `(1,0,0,a)`, `(1,0,0,b)`, `(0,0,0,None)` | a, b | 2 |
| redacted | a, e, redacted d | `mem-c\|1`, `mem-c\|2` | 1 | `(1,0,0,a)`, `(0,1,2,d)`, `(1,0,0,e)`, `(0,0,0,None)` | a, e | 2 |

Each row first asserts `server_order` is its seeded and planted ids in the order the table implies.

- [ ] **Step 3: Run them and see them fail**

Run: `cargo test -p rakka-agent-postgres --lib retrieval::tests::pgvector_reindex_reads_past -- --nocapture`
Expected: both FAIL on the page list. On `main`, the `limit 1` sweep's third page is `(0,0,1,None)`.

- [ ] **Step 4: Implement the page rule in `reindex`**

Move the SELECT into `unindexed_rows`. The loop:

```rust
let mut after_raw = after.map_or_else(String::new, |id| id.as_str().to_string());
let (mut handled, mut ended) = (0usize, false);
while handled < limit {
    let want = limit - handled;
    let rows = self.unindexed_rows(scope, &identity, now_raw, &after_raw, want).await?;
    let read = rows.len();
    for row in rows {
        let raw: String = row.get("memory_id");
        let parsed = AgentPrivateMemoryId::new(raw.as_str());
        after_raw = raw;
        let Ok(memory_id) = parsed else { failed += 1; continue };
        handled += 1;
        last = Some(memory_id);
        // decode / redacted / artifact / embed / store_vector: unchanged
    }
    if read < want { ended = true; break; }
}
// next: if ended { None } else { last }
```

Replace the comment at the id parse with one that says what is now true. Rewrite the `# A single unreadable record cannot stall the rebuild` section of the `reindex` doc. Drop "the cursor advances on each row's own id before anything fallible runs". Say that a row whose id this build refuses is counted in `failed`, read past, and not counted toward `limit`. Say the call reads on until it has handled `limit` rows whose id it reads, or the rows run out, so `next` is always an id it read. One call may read more than `limit` rows, but it makes at most `limit` model calls.

- [ ] **Step 5: Run the crate's pgvector tests**

Run: `cargo test -p rakka-agent-postgres --lib retrieval::tests`
Expected: all pass, including the #151 tests and `pgvector_reindex_pages_past_an_unreadable_record_when_dsn_is_set`.

- [ ] **Step 6: Falsify.** Revert the loop to read one page only, keeping the new id handling. Both new tests must fail. Restore.

- [ ] **Step 7: Commit**: `The private reindex reads past ids it refuses (issue #200)`.

---

### Task 2: The private pass pages past a write refused for its data (#199)

**Files:**
- Modify: `crates/rakka-agent-postgres/src/retrieval.rs`: `embedder_identity` (≈`:349`), `index_decoded`, `store_vector` (≈`:425-480`), `reindex`'s write and its `# Errors` doc, `ReindexPage::failed`, `index_memory`'s `# Errors`.
- Test: the same file's `mod tests`.

**Interfaces:**
- Consumes: Task 1's `reindex` loop.
- Produces: private `fn is_row_data_sqlstate(code: &str) -> bool`, which is true when `code` starts with `"22"` or `"23"`. Private `enum StoreFailure { RowRefused(MemoryError), Backend(MemoryError) }` with `fn into_error(self) -> MemoryError`. `store_vector(..) -> Result<IndexOutcome, StoreFailure>`.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn a_write_error_is_the_rows_own_data_only_in_sqlstate_classes_22_and_23() {
    for code in ["22000", "22003", "22021", "22P02", "23000", "23502", "23505", "23514", "23P01"] {
        assert!(is_row_data_sqlstate(code), "{code}");
    }
    for code in ["", "2", "21000", "24000", "2F000", "08006", "40001", "42501", "42P01", "53100", "57014", "XX000"] {
        assert!(!is_row_data_sqlstate(code), "{code}");
    }
}
```

```rust
/// Raises `sqlstate` on every write of `memory_id`'s derived row in this
/// test's own tenant, through a function and trigger named for the tenant.
/// Answers the SQL that removes both; run it before asserting.
async fn raise_on_derived_write(
    retriever: &PgvectorPrivateMemoryRetriever,
    owner: &AgentScope,
    memory_id: &str,
    sqlstate: &str,
) -> String
```
The helper runs one `batch_execute`. Its name is `rakka_test_raise_{tenant}`, where the tenant is `unique()`'s `t<nanos>`, safe as both identifier and literal. `CREATE FUNCTION {name}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'raised by a test' USING ERRCODE = TG_ARGV[0]; END $$;` then `CREATE TRIGGER {name} BEFORE INSERT OR UPDATE ON rakka_agent_private_memory_embedding FOR EACH ROW WHEN (NEW.tenant = '{tenant}' AND NEW.memory_id = '{memory_id}') EXECUTE FUNCTION {name}('{sqlstate}');`. It answers `DROP TRIGGER {name} ON rakka_agent_private_memory_embedding; DROP FUNCTION {name}();`.

```rust
#[tokio::test]
async fn pgvector_reindex_counts_a_write_refused_for_its_data_and_returns_any_other_when_dsn_is_set() {
    for (sqlstate, counted) in [
        ("22021", true), ("22003", true), ("23514", true), ("23505", true),
        ("21000", false), ("24000", false), ("40001", false), ("42501", false), ("57014", false), ("XX000", false),
    ] {
        let Some((store, retriever)) = vector_world().await else { return };
        let owner = scope(&unique(), "support");
        seed(&store, &owner, &[("a", EMBEDDABLE), ("b", EMBEDDABLE), ("c", EMBEDDABLE)]).await;
        let cleanup = raise_on_derived_write(&retriever, &owner, "mem-b", sqlstate).await;
        let answer = retriever.reindex(&owner, None, now(300), 10).await;
        retriever.client.batch_execute(&cleanup).await.expect("remove the trigger");
        if counted {
            assert_eq!(counts(&answer.expect("counted, not returned")), page(2, 0, 1, None), "{sqlstate}");
            assert_eq!(embedded(&retriever, &owner).await, ["mem-a", "mem-c"], "{sqlstate}");
        } else {
            assert_eq!(answer.expect_err("returned").code(), "memory-backend-failed", "{sqlstate}");
            assert_eq!(embedded(&retriever, &owner).await, ["mem-a"],
                "{sqlstate}: what was stored before stays, and nothing after it is tried");
        }
    }
}

#[tokio::test]
async fn pgvector_reindex_pages_past_a_memory_the_derived_table_refuses_when_dsn_is_set() {
    // Issue #199: a NUL in the digest is kept by the store and refused by
    // the derived table's TEXT column (SQLSTATE 22021).
    // Owner one: a, b (digest "00\u{0}00"), c; sweep with limit 1:
    //   [(1,0,0,a), (0,0,1,b), (1,0,0,c), (0,0,0,None)]; embedded [a, c];
    //   a second sweep: [(0,0,1,b), (0,0,0,None)].
    // Owner two, same seeds, limit 10: [(2,0,1,None)]; embedded [a, c].
    // index_memory(owner one, mem-b) -> Err, code "memory-backend-failed".
}

#[tokio::test]
async fn pgvector_an_identity_the_table_cannot_store_is_refused_before_the_model_when_dsn_is_set() {
    let declared = DeterministicEmbedder::new().embedding_ref();
    for identity in [
        MemoryEmbeddingRef { version: AgentRevisionNumber::new(0), ..declared.clone() },
        MemoryEmbeddingRef { model: "model\u{0}name".to_string(), ..declared.clone() },
    ] {
        // PickyEmbedder::refusing(None, Refusal::Errors) with `identity`; seed a.
        // index_memory(mem-a), reindex(None, 10), retrieve(MemoryRetrievalQuery::new(EMBEDDABLE)):
        // each Err with code "memory-embedding-invalid"; embedder.calls() == 0; embedded() empty.
    }
    // Only a NUL is refused: model "model\tname" at version 1 indexes mem-a
    // (IndexOutcome::Indexed { .. }).
}
```

Remove `pgvector_reindex_returns_a_write_failure_and_keeps_what_it_stored_when_dsn_is_set`. Its NUL digest is now counted, and the codes the boundary test does not count are its successor. Move `pgvector_index_memory_refuses_an_unstorable_identity_before_the_model_when_dsn_is_set` unchanged.

- [ ] **Step 2: Run them and see them fail**

Run: `cargo test -p rakka-agent-postgres --lib retrieval::tests -- --nocapture`
Expected: the unit test does not compile (`is_row_data_sqlstate` missing). Once it is stubbed to `false`, the boundary test fails on `22021`, the NUL test fails with `memory-backend-failed`, and the identity test fails with `calls() == 1` on version 0.

- [ ] **Step 3: Implement**

- `embedder_identity`: after the empty-model and zero-dimension check, add two refusals as `MemoryError::InvalidEmbeddingRef`: `reference.version.get() == 0` (message: the embedder declares version 0; stored versions start at 1) and `reference.model.contains('\0')` (message: the embedder's model holds a NUL, which the derived table cannot store). Extend its doc.
- `store_vector`: a revision outside `i64` becomes `StoreFailure::RowRefused`. The SQL error becomes `RowRefused(map_error(e))` when `e.as_db_error()` has a code for which `is_row_data_sqlstate(db.code().code())` is true, and `Backend(map_error(e))` otherwise. An `encode_vector` failure is `Backend`.
- `index_decoded`: `.map_err(StoreFailure::into_error)`.
- `reindex`: `Ok(_) => indexed += 1`, `Err(StoreFailure::RowRefused(_)) => failed += 1`, `Err(StoreFailure::Backend(error)) => return Err(error)`.
- Docs. In `reindex`'s `# Errors`, replace "A write that fails because of one record's own data is returned like any other, and every retry meets that record again." Say instead that a write the database refuses for the row's data (SQLSTATE class 22 or 23), or a record revision outside `BIGINT`, is counted in `failed` and paged past, and that every other write failure is returned. Add version 0 and a NUL in the model to the list of identities refused before the page. Add "a memory whose derived row the database refuses for its data" to `ReindexPage::failed`. Add the identity refusals to `index_memory`'s `# Errors`.

- [ ] **Step 4: Run the crate's tests**

Run: `cargo test -p rakka-agent-postgres`
Expected: all pass. The run must not print `skipping: the test database lacks the pgvector extension`.

- [ ] **Step 5: Falsify, one at a time, restoring after each:**
  1. `is_row_data_sqlstate` always `false`: the boundary and NUL tests fail.
  2. Always `true`: the boundary test fails on the codes it must return.
  3. Prefix `"2"`: the unit test fails on `21000`, `24000` and `2F000`.
  4. Drop the version-0 check: the identity test fails.
  5. Drop the NUL-model check: the identity test fails.

- [ ] **Step 6: Commit**: `The private reindex pages past a write refused for its data (issue #199)`.

---

### Task 3: The knowledge-graph passes read past claim ids they refuse

**Files:**
- Modify: `crates/rakka-agent-knowledge-graph-postgres/src/semantic.rs`: `unembedded` (≈`:515-566`).
- Modify: `crates/rakka-agent-knowledge-graph-postgres/src/search.rs`: `index_claims` (≈`:267-334`) and its doc (≈`:241-266`), `ClaimSearchIndexPage::skipped` and `::next` (≈`:190-204`).
- Modify: `crates/rakka-agent-knowledge-graph/src/store.rs`: the `unembedded_claims` doc (≈`:951-966`).
- Modify: `crates/rakka-agent-knowledge-graph/src/semantic.rs`: `ClaimEmbeddingPage::undecodable` and `::next` (≈`:253-263`).
- Test: `crates/rakka-agent-knowledge-graph-postgres/tests/postgres_semantic_search_proofs.rs`, `.../tests/postgres_search_index_proofs.rs`.

**Interfaces:**
- Consumes: nothing from Tasks 1–2. Same page rule.
- Produces: nothing public.

- [ ] **Step 1: Add, in each of the two test files, the same two helpers**

```rust
/// Writes a copy of `source`'s row under `raw_id` past the store, as a
/// later build with a wider id alphabet would: a claim id this build refuses.
async fn plant_refused_claim_id(raw: &Client, scope: &KnowledgeSpaceScope, source: &ClaimId, raw_id: &str)
```
It asserts `ClaimId::new(raw_id).is_err()`, then runs `INSERT INTO rakka_agent_knowledge_claim (tenant, space, claim_id, operation_id, subject, predicate, object_node, trust, transition_count, record, tombstoned, search_tokens, search_classification, search_confidence_bps, search_read_capabilities) SELECT tenant, space, $3, operation_id || $3, subject, predicate, object_node, trust, transition_count, record, tombstoned, search_tokens, search_classification, search_confidence_bps, search_read_capabilities FROM rakka_agent_knowledge_claim WHERE tenant = $1 AND space = $2 AND claim_id = $4`. Then it asserts one row was written.

```rust
/// The claim ids of one space in the database's own order.
async fn claim_order(raw: &Client, scope: &KnowledgeSpaceScope) -> Vec<String>
```

- [ ] **Step 2: Write the failing tests**

```rust
// postgres_semantic_search_proofs.rs
#[tokio::test]
async fn postgres_the_embedding_pass_reads_past_a_claim_id_this_build_refuses_when_dsn_is_set() {
    // pgvector_dsn(..), PrivateSchema::migrated(&dsn, "sem_refused_id"),
    // ConformanceScopes::unique("pg-sem-refused-id").primary,
    // ConceptEmbedder::new(&[&["car", "automobile"]]).
    // Append three claims (`append(&store, scope, "claim-{i}", "the car is here")`), ids sorted c0 < c1 < c2.
    // Plant "{c0}|x" from c0 and "{c2}|x" from c2; assert claim_order == [c0, c0|x, c1, c2, c2|x].
    // Walk embed_claims with limit 1, at most 8 pages, recording (embedded, failed, undecodable, next):
    assert_eq!(pages, vec![
        (1, 0, 0, Some(c0.clone())),
        (1, 0, 1, Some(c1.clone())),
        (1, 0, 0, Some(c2.clone())),
        (0, 0, 1, None),
    ]);
    // schema.drop().await
}

// postgres_search_index_proofs.rs
#[tokio::test]
async fn postgres_the_indexing_pass_reads_past_a_claim_id_this_build_refuses_when_dsn_is_set() {
    // dsn(), PrivateSchema::migrated(&dsn, "search_refused_id"),
    // scope ConformanceScopes::unique("pg-index-refused-id").primary,
    // earlier_claims(&raw, &scope, 3) sorted c0 < c1 < c2; plant c0|x and c2|x; assert claim_order.
    // Walk index_claims with limit 1, recording (indexed, skipped, next):
    //   [(1,0,c0), (1,1,c1), (1,0,c2), (0,1,None)]
    // Second block, a fresh scope "pg-index-refused-zero": earlier_claims 2 sorted d0 < d1, plant d0|x;
    //   index_claims(Some(&d0), 0) == (indexed 1, skipped 1, next Some(d1)).
    // schema.drop().await
}
```

- [ ] **Step 3: Run them and see them fail**

Run: `cargo test -p rakka-agent-knowledge-graph-postgres --test postgres_semantic_search_proofs reads_past -- --nocapture` and the same with `--test postgres_search_index_proofs`.
Expected: both FAIL. The second page of each is an error from `ClaimId::new`.

- [ ] **Step 4: Implement the page rule in both `unembedded` and `index_claims`**

Use the same loop shape as Task 1 Step 4. Keep each function's existing clamp, and its query (with `LIMIT` from `want`). A refused claim id is counted in `undecodable` (respectively `skipped`) and continues, with the raw text as the cursor. A readable id counts toward `limit` and becomes `last`, and then the row is handled as today. `next` is `if ended { None } else { last }`, with no conversion at the end. Update the four docs listed under **Files** to say: a row whose claim id this build refuses is counted, read past, and not counted toward `limit`, and `next` is always a claim id. In the SPI doc, phrase it as what a backend that can hold such a row does. The in-memory store cannot hold one.

- [ ] **Step 5: Run both crates' tests**

Run: `cargo test -p rakka-agent-knowledge-graph-postgres` and `cargo test -p rakka-agent-knowledge-graph`
Expected: all pass, including `postgres_the_indexing_pass_clamps_its_limit_when_dsn_is_set` and `postgres_the_indexing_pass_skips_a_damaged_record_when_dsn_is_set`, with no pgvector skip announced.

- [ ] **Step 6: Falsify.** Restore `page.next = last.map(ClaimId::new).transpose()?` in each function in turn. Its test must fail. Restore.

- [ ] **Step 7: Commit**: `The knowledge-graph passes read past claim ids they refuse (siblings of issue #200)`.

---

### Task 4: Product docs, changelog, full validation

**Files:**
- Modify: `docs/rakka-v1-known-limitations-roadmap.md:28`. After the #151 sentence, add: the private `reindex` also reads past a row whose id it refuses and pages past a memory whose derived row the database refuses for its data, and the communal passes read past a claim id they refuse (issues #199, #200). Neither says which cause a count holds.
- Modify: `docs/rakka-compatibility.md`, after the #151 entry (≈`:637`). Add a bullet for issues #199 and #200 with the design's section 5 table and its two consequences. Add: no type, field, code or schema is added.
- Modify: `CHANGELOG.md`. Under `### Fixed`, add an entry naming `rakka-agent-postgres` and `rakka-agent-knowledge-graph-postgres`, with the four rows of what changed and the design record's path. Under `### Migration notes`, add: an embedder that declares version 0 must declare 1 or more, since `reindex`, `index_memory` and `retrieve` now refuse it as `memory-embedding-invalid`.

- [ ] **Step 1: Write the three doc changes.**
- [ ] **Step 2: Check the doc comments:** `cargo doc -p rakka-agent-postgres -p rakka-agent-knowledge-graph -p rakka-agent-knowledge-graph-postgres --no-deps`. Expected: no new warnings from changed files.
- [ ] **Step 3: Full gated runs.** Run `cargo test -p rakka-agent-postgres`, `cargo test -p rakka-agent-knowledge-graph-postgres` and `cargo test -p rakka-agent-knowledge-graph` with the DSN on 5433 and `RAKKA_POSTGRES_PGVECTOR_REQUIRED=1`. Expected: all pass. That includes the memory conformance suite (`tests/memory_conformance.rs`), whose attestation in `docs/rakka-agent-security-validation-matrix.md` must still hold. Then drop any `rakka_knowledge_graph_*` schema these runs left on 5433, and `docker rm -f rakka-pgvector-check`.
- [ ] **Step 4: Validate:** `scripts/validate.sh > tmp/2026-11-25-validate.log 2>&1; echo "exit=$?"` (`tmp/` is git-ignored). Expected: `exit=0`.
- [ ] **Step 5: Commit**: `Document the sweeps that read past refused rows (issues #199, #200)`.
