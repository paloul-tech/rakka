# Communal Read Path Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A wired run's context snapshot carries the communal claims the run is allowed to read, found by an indexed keyword search and checked against the authoritative record and the memory-ingress guardrails.

**Architecture:** `rakka-agent` owns the ports, the mirror types, the read grant, the assembly and the snapshot record; it never names a graph type. `rakka-agent-knowledge-graph` adds a keyword-search store operation and the adapters that implement the ports. `rakka-agent-knowledge-graph-postgres` keeps the search index as columns on the claim row. `rakka-a2a` carries read capabilities across the wire.

**Tech Stack:** Rust 1.88 workspace, `tokio`, `serde`/`serde_json`, `tokio-postgres`, PostgreSQL (GIN over `TEXT[]`, one `plpgsql` trigger, no extension).

**Spec:** `docs/superpowers/specs/2026-11-14-communal-read-path-design.md` (issue #69). Section numbers below are that document's. Where this plan and the spec disagree, the spec wins.

## Global Constraints

- Branch `communal-read-path`, off main `86cf087`. Commit locally after each task. Do not push, and do not publish anything.
- Every commit message ends with `Co-Authored-By: Claude Fable 5.2 <noreply@anthropic.com>` and names `(issue #69)`.
- Toolchain: `export PATH="$HOME/.rustup/toolchains/stable-aarch64-apple-darwin/bin:$PATH"` before any cargo command.
- `cargo clean` at every task boundary. Run tests per crate, never the whole workspace in one command, until Task 19.
- Clippy runs with `-D warnings`. Every public item has a doc comment.
- Error codes, metric names, label values, wire field names and the PostgreSQL object names below are a compatibility surface. Use them exactly.
- Every new durable field is omitted when empty. A record written by this version without the feature in use has the bytes it has today. Task 1's pins hold that to the end.
- No resolved credential or secret material enters a snapshot, a log or a metric. Raw claim text, search words, claim ids and space ids are never metric labels and are never logged.
- `rakka-agent` must not depend on `rakka-agent-knowledge-graph`.
- TDD: every test is written first and watched to fail for the stated reason. A test that passes before its code exists is a finding about the test.
- **PostgreSQL tasks (6 to 9) are not complete unless their tests ran with `RAKKA_POSTGRES_TEST_DSN` set.** Without it those tests pass silently and prove nothing. Use `RAKKA_POSTGRES_TEST_DSN=postgres://postgres:postgres@localhost:5432/postgres`.
- If an existing test fails for a reason this plan did not predict, stop and report it. Do not fix it and continue.
- In zsh, quote globs (`--include='*.rs'`).

## Review Focus

Inputs the spec implies and its listed proofs do not exercise. Each has a test in the task named.

1. **A query with no words** (punctuation only, or empty after clamping): a search answers no hits and no error on both stores, and the PostgreSQL statement is valid with an empty word array. Task 5 (clause) and Task 8.
2. **Non-ASCII text** (`"Straße ÉCOLE 東京"`): both stores produce the same words and the same hits; PostgreSQL compares stored words bytewise. Task 5 (golden test and clause), run on PostgreSQL in Task 8.
3. **A claim with few or no words:** a claim whose object is held by artifact reference is indexed by its subject and predicate only, and a row whose word array is empty is indexed (not `NULL`), never counted `unindexed`, never a hit. Task 7 and Task 8.
4. **A version 2 snapshot through the PostgreSQL snapshot store** (`rakka-agent-postgres`): it persists and loads, a version 1 snapshot still loads, and no code in that crate needs to change. Task 13.
5. **Two nodes running `index_claims` over the same space at once:** every claim ends indexed once, neither call errors, and the sum of `indexed` is at least the number of claims. Task 9.

---

## File Structure

| File | Responsibility |
|---|---|
| `crates/rakka-agent/src/communal.rs` (new) | Mirror types, query, policy, the three ports, the bundle, the read grant and its reference source, the communal half of assembly, its report. |
| `crates/rakka-agent/src/definition.rs` | `read_capabilities` on the envelope, its dimension and narrowing. |
| `crates/rakka-agent/src/delegation.rs`, `task.rs`, `run.rs`, `query.rs` | `read_capabilities` beside every `knowledge_spaces`; the run passes its envelope to assembly and emits the counters. |
| `crates/rakka-agent/src/memory.rs`, `schema.rs` | Snapshot record, version rule, `with_communal_retrieval`. |
| `crates/rakka-agent/src/retrieval.rs` | `assemble_context` gains the envelope; the tokenizer becomes public. |
| `crates/rakka-agent/src/tools.rs`, `guardrails.rs`, `observability.rs` | Attestation, `with_claim`, two counters. |
| `crates/rakka-a2a/src/agents/collaboration.rs` | `read-capabilities` on both envelopes. |
| `crates/rakka-agent-knowledge-graph/src/search.rs` (new) | `ClaimSearch`, the words, the scorer, the report. |
| `crates/rakka-agent-knowledge-graph/src/store.rs`, `conformance.rs` | Capability, trait method, in-memory search, clauses. |
| `crates/rakka-agent-knowledge-graph/src/communal_retrieval.rs` (new) | The two adapters and the trust conversions. |
| `crates/rakka-agent-knowledge-graph-postgres/src/search.rs` (new) | Index column derivation, the search statement, the probe, `index_claims`. |
| `crates/rakka-agent-knowledge-graph-postgres/src/lib.rs`, `erasure.rs` | Migration, append writes the index, tombstone clears it. |
| `crates/rakka-agent-knowledge-graph-postgres/tests/postgres_search_index_proofs.rs` (new) | Every proof in spec 8.4 that is not a conformance clause. |

---

### Task 1: Pin today's bytes

**Files:**
- Create: `crates/rakka-agent/tests/communal_read_golden_bytes.rs`
- Modify: `crates/rakka-a2a/tests/collaboration_surface.rs`

These are characterization tests. They pass on `86cf087` and must pass after every later task. They are the one exception to "watch it fail": prove each can fail by changing one expected byte, watching the failure, and restoring it.

- [ ] **Step 1: Write the pins in `communal_read_golden_bytes.rs`**
  - `a_version_1_snapshot_keeps_its_bytes_and_digest`: assemble a snapshot with one session entry and no retrieval through `assemble_session_context` with fixed ids and `AgentTimestampMillis::new(10)`; assert `serde_json::to_string(&snapshot)` equals a string literal captured from this commit, and `snapshot.content_digest` equals a captured literal.
  - `an_authority_envelope_keeps_its_bytes`: an `AgentAuthorityEnvelope::empty()` with one knowledge space and one tool; assert its JSON equals a captured literal.
  - `a_delegation_record_and_provenance_keep_their_bytes`: one `AgentDelegationRecord` and one `AgentTaskDelegationProvenance` with one knowledge space; assert each JSON equals a captured literal.
- [ ] **Step 2: Add to `collaboration_surface.rs`** `the_collaboration_envelopes_keep_their_bytes_without_read_capabilities`: one `AgentCollaborationMetadata` and one `AgentHandoffCollaborationMetadata` built from records with no read capability; assert each JSON equals a captured literal.
- [ ] **Step 3: Run** `cargo test -p rakka-agent --test communal_read_golden_bytes` and `cargo test -p rakka-a2a --test collaboration_surface`. Expected: PASS. Then falsify each once as described above.
- [ ] **Step 4: Commit** `Pin the snapshot, envelope and delegation bytes before the communal read path (issue #69)`.

### Task 2: Read capabilities on the authority envelope (spec 6.1)

**Files:**
- Modify: `crates/rakka-agent/src/definition.rs` (`AgentAuthorityEnvelope` :645, `AgentEnvelopeDimension` :542, narrowing :747-803), `crates/rakka-agent/src/admission.rs` (two struct literals)
- Test: `crates/rakka-agent/tests/definition_setup_envelope.rs`, `crates/rakka-agent/tests/autonomy_admission.rs`

**Interfaces:**
- Produces: `AgentAuthorityEnvelope::read_capabilities: BTreeSet<AgentCapabilityId>` with `#[serde(default, skip_serializing_if = "BTreeSet::is_empty")]`; `AgentEnvelopeDimension::ReadCapability`, label `"widened-read-capability"`.

- [ ] **Step 1: Write failing tests**
  - `definition_setup_envelope.rs`: `a_setup_that_adds_a_read_capability_is_refused` asserts the violation's dimension label is `"widened-read-capability"`; `a_setup_may_narrow_read_capabilities` asserts a subset is accepted; `read_capabilities_authorize_nothing_but_reads` asserts a tool dispatch for a capability named only in `read_capabilities` is still refused.
  - `autonomy_admission.rs`: `a_definition_that_gains_a_read_capability_is_widened_until_readmitted` asserts the admission refusal is the existing `Widened` one, and that re-admission clears it.
- [ ] **Step 2: Run** `cargo test -p rakka-agent --test definition_setup_envelope --test autonomy_admission`. Expected: FAIL to compile, no field `read_capabilities`.
- [ ] **Step 3: Implement.** Add the field after `knowledge_spaces`, the variant after `KnowledgeSpace`, the label, and one `collect_subset_violations` call directly after the knowledge-space one (:775). Follow whatever size bound the neighbouring sets have. Fix the two literals in `admission.rs`.
- [ ] **Step 4: Run** the two test files and `--test communal_read_golden_bytes`. Expected: PASS.
- [ ] **Step 5: Commit** `Grant read capabilities on the authority envelope (issue #69)`.

### Task 3: Read capabilities through delegation and handoff (spec 6.2)

**Files:**
- Modify: `crates/rakka-agent/src/delegation.rs` (`AgentDelegationTarget` :237, `AgentDelegationRecord` :492, `AgentTaskDelegationProvenance` :877, `AgentRunDelegationEnvelope` :1018), `crates/rakka-agent/src/task.rs` (`AgentTaskHandoff` :2380, `AgentTaskHandoffRequest` :2471, `delegation_envelope_for` :5044, `delegation_envelope_base` :5085, :6202), `crates/rakka-agent/src/run.rs` (:3190), `crates/rakka-agent/src/query.rs` (`AgentGoalDelegationEdgeView` :394, :416), `crates/rakka-agent/src/tools.rs`, `fan_in.rs`, `effect.rs` (struct literals)
- Modify (struct literals only): `crates/rakka-agent/tests/common/mod.rs`, `task_bounded_state.rs`, `delegation_limits.rs`, `task_entity.rs`, `goal_view.rs`, `fan_out_fan_in.rs`, `communal_claim_append.rs`, `handoff_cancellation.rs`, `delegation_record.rs`
- Test: `crates/rakka-agent/tests/delegation_record.rs`, `handoff_record.rs`, `goal_view.rs`

**Interfaces:**
- Produces: field `read_capabilities` on each record, typed as the table in spec 6.2 and with the same serde attributes as the `knowledge_spaces` field beside it; `AgentDelegationTarget::with_read_capability(self, capability: AgentCapabilityId) -> Self`; `AgentRunDelegationEnvelope::read_capabilities: Option<BTreeSet<AgentCapabilityId>>`.

- [ ] **Step 1: Write failing tests,** each mirroring the knowledge-space test that sits beside it:
  - `delegation_record.rs`: `a_child_gets_the_targets_read_capabilities_intersected_with_the_parents_grant`; `a_child_of_a_parent_with_lineage_and_no_statement_gets_none`; `a_root_delegation_gets_the_targets_statement`; `a_delegated_childs_envelope_carries_its_provenance_read_capabilities`; `a_provenance_recorded_before_the_field_decodes_to_no_read_capabilities` (decode a JSON literal without the field; assert the run envelope's set is `Some(empty)`); `a_goal_root_and_a_plain_task_have_no_read_capability_statement` (assert `None`).
  - `handoff_record.rs`: one test per arm of `delegation_envelope_for` (`Some` intersect, `None` with lineage, `None` at root).
  - `goal_view.rs`: `a_delegation_edge_shows_its_read_capabilities`.
- [ ] **Step 2: Run** `cargo test -p rakka-agent --test delegation_record --test handoff_record --test goal_view`. Expected: FAIL to compile.
- [ ] **Step 3: Implement.** Copy each knowledge-space derivation at its three sites for the new field. `AgentDelegationToolCall` and `AgentGoalContractView` (`query.rs:1396`) do not change. `capability_scopes` is not touched.
- [ ] **Step 4: Run** `cargo test -p rakka-agent` (the whole crate: many literals changed). Expected: PASS, including Task 1's pins.
- [ ] **Step 5: Commit** `Pass read capabilities through delegation and handoff (issue #69)`.

### Task 4: Read capabilities on the A2A wire (spec 6.2)

**Files:**
- Modify: `crates/rakka-a2a/src/agents/collaboration.rs` (`AgentCollaborationMetadata` :139, :244, :292; `AgentHandoffCollaborationMetadata` :347, :404, :426)
- Test: `crates/rakka-a2a/tests/collaboration_surface.rs`, `handoff_surface.rs`; fix literals in `ingress_egress_guardrails.rs`

**Interfaces:**
- Consumes: Task 3's fields.
- Produces: `read_capabilities: Vec<String>` on both envelopes, wire name `read-capabilities`, same serde attributes as `knowledge_spaces`.

- [ ] **Step 1: Write failing tests**
  - `collaboration_surface.rs`: `read_capabilities_cross_from_the_record_to_the_provenance`; `an_invalid_read_capability_id_fails_the_send`; `an_envelope_carrying_read_capabilities_is_refused_by_the_earlier_shape` (deserialize the JSON into a local copy of the `86cf087` struct with `deny_unknown_fields`; assert the error).
  - `handoff_surface.rs`: `read_capabilities_cross_from_the_handoff_to_the_request`.
- [ ] **Step 2: Run** `cargo test -p rakka-a2a --test collaboration_surface --test handoff_surface`. Expected: FAIL to compile.
- [ ] **Step 3: Implement,** validating into `AgentCapabilityId` at the receiving surface exactly where `KnowledgeSpaceId::new` runs today.
- [ ] **Step 4: Run** `cargo test -p rakka-a2a`. Expected: PASS, including Task 1's envelope pin.
- [ ] **Step 5: Commit** `Carry read capabilities in the A2A collaboration envelopes (issue #69)`.

### Task 5: Keyword search in the graph store (spec 6.3)

**Files:**
- Create: `crates/rakka-agent-knowledge-graph/src/search.rs`
- Modify: `crates/rakka-agent-knowledge-graph/src/store.rs` (`KnowledgeGraphCapability` :61, trait :700, in-memory store :920), `conformance.rs`, `lib.rs`; `crates/rakka-agent/src/retrieval.rs` (:894), `crates/rakka-agent/src/lib.rs`
- Test: `crates/rakka-agent-knowledge-graph/tests/knowledge_graph_conformance.rs`, unit tests in `search.rs`

**Interfaces:**
- Produces, in `rakka-agent`: `pub fn tokenize_search_text(text: &str) -> BTreeSet<String>` (the existing private `tokenize`, renamed and re-exported at the crate root; its body does not change).
- Produces, in `search.rs`, all re-exported from the crate root:
  - `pub const CLAIM_SEARCH_TEXT_MAX_BYTES: usize = 1024;` `pub const CLAIM_SEARCH_MAX_HITS: usize = 64;`
  - `ClaimSearch::new(text: &str, limit: usize) -> Self`, builders `with_trust(BTreeSet<ClaimTrustStatus>)`, `with_classifications(BTreeSet<MemoryClassification>)`, `with_min_confidence_bps(u16)`, `with_reader(BTreeSet<AgentCapabilityId>)`; accessors `text() -> &str`, `query_tokens() -> &BTreeSet<String>`, `trust()`, `classifications()`, `min_confidence_bps() -> u16`, `reader()`, `limit() -> usize`; `admits(&self, claim: &Claim) -> bool`. A new search admits no trust state and no classification until told.
  - `pub fn claim_search_tokens(claim: &Claim) -> BTreeSet<String>`: words of the subject, the predicate, and the object (node id; or `memory_embedding_text` (`crates/rakka-agent/src/retrieval.rs:374`) of an inline value; nothing for an artifact).
  - `pub fn claim_search_query_tokens(text: &str) -> BTreeSet<String>`
  - `pub fn claim_search_relevance_bps(query_tokens: &BTreeSet<String>, claim: &Claim) -> u16`: `overlap * 10_000 / query_tokens.len()`, zero for an empty query.
  - `pub struct ClaimSearchHit { pub claim: Claim, pub relevance_bps: u16 }`, `pub struct ClaimSearchReport { pub hits: Vec<ClaimSearchHit>, pub unindexed: bool }`
- Produces, in `store.rs`: `KnowledgeGraphCapability::KeywordSearch` (label `"keyword-search"`); `fn search<'a>(&'a self, scope: &'a KnowledgeSpaceScope, search: &'a ClaimSearch) -> ClaimFuture<'a, ClaimSearchReport>` with a default body returning `ClaimError::CapabilityUnsupported` for `KeywordSearch`.
- Produces, in `conformance.rs`: `pub async fn keyword_search(store: &dyn KnowledgeGraphStore, scopes: ConformanceScopes)`, called from `check_knowledge_graph_contract` only when the store reports the capability.

- [ ] **Step 1: Write the golden test** in `search.rs`: `the_word_rule_is_frozen`.

```rust
assert_eq!(words("Hello, WORLD! hello"), ["hello", "world"]);
assert_eq!(words("dark-mode_v2 x9"), ["dark", "mode", "v2", "x9"]);
assert_eq!(words("Straße ÉCOLE 東京"), ["straße", "école", "東京"]);
assert_eq!(words("  ...  "), [] as [&str; 0]);
```

`words` collects `claim_search_query_tokens` in order. Add `a_claims_words_are_its_subject_predicate_and_object` for an edge claim, an inline-value claim and an artifact-value claim.
- [ ] **Step 2: Write the conformance clause `keyword_search`,** asserting in one scope:
  - hits are in descending `relevance_bps`, ties by ascending claim id; `limit` is honoured; no hit has `relevance_bps == 0`;
  - a claim excluded by trust, by classification, by confidence, and by a missing reader capability takes no slot, with `limit` admitted claims behind each;
  - a claim requiring nothing is found by a reader holding nothing; a claim requiring two capabilities is not found by a reader holding one;
  - a tombstoned stub is not a hit, and a search for the words its statement held finds nothing;
  - the other tenant's scope and the other space answer `hits.is_empty() && !unindexed`;
  - a matching claim is found after 300 non-matching claims were appended before it;
  - a punctuation-only query answers no hits and no error (Review Focus 1);
  - a claim holding `"Straße ÉCOLE 東京"` is found by the query `"école"` with the score the scorer gives (Review Focus 2);
  - `!report.unindexed` after every append made through the store.
- [ ] **Step 3: Add** `a_store_without_the_capability_refuses_search` to `knowledge_graph_conformance.rs`: a test double with `capabilities()` = core answers the error whose code is `"claim-capability-unsupported"`. Wire `keyword_search` into that file as its own `#[tokio::test]`.
- [ ] **Step 4: Run** `cargo test -p rakka-agent-knowledge-graph`. Expected: FAIL to compile.
- [ ] **Step 5: Implement.** The in-memory store reads its space, keeps claims that `admits` and score above zero, sorts by score descending then claim id, truncates to `limit`, and reports `unindexed: false` and the capability.
- [ ] **Step 6: Run** `cargo test -p rakka-agent-knowledge-graph` and `cargo test -p rakka-agent --lib retrieval`. Expected: PASS.
- [ ] **Step 7: Commit** `Add keyword search to the knowledge-graph store (issue #69)`.

### Task 6: PostgreSQL migration for the search index (spec 6.3.1)

**Files:**
- Modify: `crates/rakka-agent-knowledge-graph-postgres/src/lib.rs` (`MIGRATION_SQL` :210, `migrate` :542)
- Create: `crates/rakka-agent-knowledge-graph-postgres/tests/postgres_search_index_proofs.rs`

**Interfaces:**
- Produces: columns `search_tokens TEXT[] COLLATE "C"`, `search_classification TEXT`, `search_confidence_bps INTEGER`, `search_read_capabilities TEXT[] COLLATE "C"`, all nullable with no default; `pub const SEARCH_TOKENS_INDEX_NAME: &str = "rakka_agent_knowledge_claim_search_tokens"`; `pub const UNINDEXED_INDEX_NAME: &str = "rakka_agent_knowledge_claim_unindexed"`; `pub const SEARCH_TRIGGER_NAME: &str = "rakka_agent_knowledge_claim_clear_search"` (the function has the same name).

The exact DDL:

```sql
CREATE OR REPLACE FUNCTION rakka_agent_knowledge_claim_clear_search() RETURNS trigger
LANGUAGE plpgsql AS $clear$
BEGIN
    NEW.search_tokens := NULL;
    RETURN NEW;
END
$clear$;

-- inside the DO block, when pg_trigger holds no such trigger on the table:
CREATE TRIGGER rakka_agent_knowledge_claim_clear_search
    BEFORE INSERT OR UPDATE ON rakka_agent_knowledge_claim
    FOR EACH ROW
    WHEN (NEW.tombstoned AND NEW.search_tokens IS NOT NULL)
    EXECUTE FUNCTION rakka_agent_knowledge_claim_clear_search();

-- each its own statement, after the batch:
CREATE INDEX CONCURRENTLY IF NOT EXISTS rakka_agent_knowledge_claim_search_tokens
    ON rakka_agent_knowledge_claim USING GIN (search_tokens);
CREATE INDEX CONCURRENTLY IF NOT EXISTS rakka_agent_knowledge_claim_unindexed
    ON rakka_agent_knowledge_claim (tenant, space, claim_id)
    WHERE search_tokens IS NULL AND NOT tombstoned;
```

- [ ] **Step 1: Write failing tests** in the new file. Copy the harness (`dsn`, `client_in`, `unique_schema`, `table_filenodes`) and the earlier-schema technique from `postgres_erasure_proofs.rs`. Keep five literals named `MIGRATION_SQL_86CF087`, `APPEND_SQL_86CF087`, `TRANSITION_SQL_86CF087`, `TOMBSTONE_SQL_86CF087`, `DELETE_SQL_86CF087`, copied from `git show 86cf087:crates/rakka-agent-knowledge-graph-postgres/src/{lib,erasure}.rs`.
  - `postgres_the_search_migration_rewrites_nothing_when_dsn_is_set`: apply `MIGRATION_SQL_86CF087` in a fresh schema, insert rows with `APPEND_SQL_86CF087`, record `table_filenodes`, run `migrate()`, assert the claim table's file node is unchanged, the four columns exist and are `NULL` on every row, the trigger exists, and both indexes exist with `indisvalid`.
  - `postgres_a_second_search_migration_alters_nothing_when_dsn_is_set`: run `migrate()` twice; assert the catalog rows (`pg_attribute`, `pg_trigger`, `pg_index` oids) are identical.
  - `postgres_an_invalid_search_index_is_rebuilt_when_dsn_is_set`: after `migrate()`, produce a real invalid index: drop the index, open a second connection holding a transaction that has written to the claim table, start `CREATE INDEX CONCURRENTLY` with `SET statement_timeout = '200ms'`, let it time out, assert `indisvalid` is false, end the transaction, run `migrate()`, assert `indisvalid` is true.
  - `postgres_every_earlier_statement_still_works_after_the_search_migration_when_dsn_is_set`: after `migrate()`, run each of the four `_86CF087` write statements and `MIGRATION_SQL_86CF087` again; assert each succeeds.
- [ ] **Step 2: Run** `RAKKA_POSTGRES_TEST_DSN=... cargo test -p rakka-agent-knowledge-graph-postgres --test postgres_search_index_proofs`. Expected: FAIL, columns absent.
- [ ] **Step 3: Implement.** In `MIGRATION_SQL`: the columns in the `CREATE TABLE`; the function as a top-level statement; in the `DO` block, one `ALTER TABLE ... ADD COLUMN` ×4 when `search_tokens` is absent, and the trigger when absent. In `migrate`, after the validate statement and still under the advisory lock: for each index, read `indisvalid` through `to_regclass`; if it exists and is invalid, `DROP INDEX CONCURRENTLY`; then the `CREATE INDEX CONCURRENTLY IF NOT EXISTS`, each sent alone. Extend the doc comment on `MIGRATION_SQL` with the cost statement in spec 6.3.1.
- [ ] **Step 4: Run** the new file and `--test postgres_erasure_proofs --test postgres_backend_proofs --test postgres_conformance` with the DSN. Expected: PASS. If the suite stalls, a concurrent build is waiting on an open transaction in another test; find that transaction. Do not drop `CONCURRENTLY`.
- [ ] **Step 5: Commit** `Migrate the claim table for the search index (issue #69)`.

### Task 7: Write the index with the claim, clear it with the tombstone (spec 6.3.1)

**Files:**
- Create: `crates/rakka-agent-knowledge-graph-postgres/src/search.rs`
- Modify: `lib.rs` (`APPEND_SQL` :337, `append` :720), `erasure.rs` (`TOMBSTONE_SQL` :57)
- Test: `tests/postgres_search_index_proofs.rs`

**Interfaces:**
- Produces, `pub(crate)` in `search.rs`: `struct SearchColumns { tokens: Vec<String>, classification: &'static str, confidence_bps: i32, read_capabilities: Vec<String> }` and `fn search_columns(claim: &Claim) -> SearchColumns` (`tokens` from `claim_search_tokens`, sorted).

- [ ] **Step 1: Write failing tests**
  - `postgres_append_writes_the_index_columns_when_dsn_is_set`: after `append`, read the row; assert `search_tokens` equals `claim_search_tokens(&claim)`, and the three other columns equal the claim's fields.
  - `postgres_an_artifact_object_is_indexed_by_its_subject_and_predicate_when_dsn_is_set` (Review Focus 3): the row's `search_tokens` holds the subject's and predicate's words and nothing from the artifact reference.
  - `postgres_a_transition_leaves_the_index_columns_when_dsn_is_set`: the four columns are equal before and after `transition`.
  - `postgres_tombstone_clears_the_search_words_when_dsn_is_set`: after `tombstone`, `search_tokens IS NULL`.
  - `postgres_an_earlier_tombstone_statement_clears_the_search_words_when_dsn_is_set`: append through the store, run `TOMBSTONE_SQL_86CF087` with the parameters the store would pass, assert `search_tokens IS NULL`. Then, in a second schema, `DROP TRIGGER` first and assert the same statement leaves the words: this half asserts the words **remain**, which proves the trigger is what clears them.
  - `postgres_delete_leaves_no_row_when_dsn_is_set`: after `delete`, no row for the claim id.
- [ ] **Step 2: Run** with the DSN. Expected: FAIL, `search_tokens` is `NULL` after append.
- [ ] **Step 3: Implement.** `APPEND_SQL`'s claim insert names the four columns and takes four more parameters. `TOMBSTONE_SQL`'s update adds `search_tokens = NULL`. `TRANSITION_SQL` and `DELETE_SQL` do not change.
- [ ] **Step 4: Run** all four test files of the crate with the DSN. Expected: PASS.
- [ ] **Step 5: Commit** `Write a claim's search index with the claim and erase it with the claim (issue #69)`.

### Task 8: Search in PostgreSQL (spec 6.3.1)

**Files:**
- Modify: `crates/rakka-agent-knowledge-graph-postgres/src/search.rs`, `lib.rs` (`capabilities` :716, trait impl)
- Test: `tests/postgres_conformance.rs`, `tests/postgres_search_index_proofs.rs`

**Interfaces:**
- Consumes: Task 5's `ClaimSearch` accessors, `claim_search_relevance_bps`; Task 7's columns.
- Produces: `PostgresKnowledgeGraphStore` implements `search` and reports `KeywordSearch`.

The statement and the probe:

```sql
SELECT claim_id, record, tombstoned
FROM rakka_agent_knowledge_claim
WHERE tenant = $1 AND space = $2
  AND NOT tombstoned
  AND search_tokens && $3
  AND trust = ANY($4)
  AND search_classification = ANY($5)
  AND search_confidence_bps >= $6
  AND search_read_capabilities <@ $7
ORDER BY (SELECT count(*) FROM unnest($3::TEXT[]) AS q(token)
          WHERE q.token = ANY(search_tokens)) DESC,
         claim_id
LIMIT $8;

SELECT EXISTS (
    SELECT 1 FROM rakka_agent_knowledge_claim
    WHERE tenant = $1 AND space = $2 AND search_tokens IS NULL AND NOT tombstoned
) AS unindexed;
```

- [ ] **Step 1: Write failing tests**
  - `postgres_conformance.rs`: `keyword_search_ranks_admitted_claims_when_dsn_is_set`, calling `conformance::keyword_search`.
  - `postgres_search_index_proofs.rs`:
    - `postgres_a_claim_is_found_straight_after_its_append_when_dsn_is_set`.
    - `postgres_an_earlier_binarys_claim_is_reported_unindexed_when_dsn_is_set`: a row inserted by `APPEND_SQL_86CF087` is not a hit and `report.unindexed` is true; a tombstoned stub alone does not set `unindexed`.
    - `postgres_a_drifted_capability_column_cannot_leak_a_claim_when_dsn_is_set`: for a claim requiring a capability, `UPDATE ... SET search_read_capabilities = '{}'`; a reader holding nothing gets an error whose code is `"claim-backend-failed"`.
    - `postgres_a_drifted_classification_column_cannot_leak_a_claim_when_dsn_is_set`: the same with `search_classification` rewritten to an admitted label.
    - `postgres_a_query_without_words_is_a_valid_statement_when_dsn_is_set` (Review Focus 1): `ClaimSearch::new("...", 8)` answers `Ok` with no hits.
    - `postgres_a_row_with_no_words_is_indexed_and_never_a_hit_when_dsn_is_set` (Review Focus 3): set one row's `search_tokens` to `'{}'`; no query finds it and `unindexed` is false.
    - `postgres_search_uses_the_word_index_when_dsn_is_set`: with `SET enable_seqscan = off`, `EXPLAIN` of the statement names `rakka_agent_knowledge_claim_search_tokens`.
- [ ] **Step 2: Run** with the DSN. Expected: FAIL with `claim-capability-unsupported`.
- [ ] **Step 3: Implement.** Decode each row with `decode_claim_row`; if `!search.admits(&claim)` or the score is zero, return `drift_error`. Sort the decoded hits by the Rust score descending then claim id, so the answer's order is the shared scorer's whatever the database's count did. Send the probe as a second statement.
- [ ] **Step 4: Run** all four test files with the DSN. Expected: PASS.
- [ ] **Step 5: Commit** `Search claims in PostgreSQL through the index (issue #69)`.

### Task 9: The indexing pass (spec 6.3.1)

**Files:**
- Modify: `crates/rakka-agent-knowledge-graph-postgres/src/search.rs`, `lib.rs` (re-exports, crate docs)
- Test: `tests/postgres_search_index_proofs.rs`

**Interfaces:**
- Produces: `pub const CLAIM_SEARCH_INDEX_PAGE_MAX_ROWS: usize = 256;` `pub struct ClaimSearchIndexPage { pub indexed: u64, pub skipped: u64, pub next: Option<ClaimId> }`; `pub async fn index_claims(&self, scope: &KnowledgeSpaceScope, after: Option<&ClaimId>, limit: usize) -> ClaimResult<ClaimSearchIndexPage>`. `next` is the last claim id read when the page read `limit` rows, and `None` when it read fewer.

- [ ] **Step 1: Write failing tests**
  - `postgres_the_indexing_pass_makes_an_earlier_claim_searchable_when_dsn_is_set`: three rows from `APPEND_SQL_86CF087`; page with `limit` 2 until `next` is `None`; assert total `indexed == 3`, every claim is a hit, `unindexed` is false.
  - `postgres_the_indexing_pass_is_idempotent_when_dsn_is_set`: a second run answers `indexed == 0, skipped == 0, next == None`.
  - `postgres_the_indexing_pass_skips_a_damaged_record_when_dsn_is_set`: overwrite one row's `record` with `'not json'`; assert `skipped == 1`, the rows after it are indexed, and `unindexed` stays true.
  - `postgres_the_indexing_pass_clamps_its_limit_when_dsn_is_set`: `limit` 0 reads one row; `limit` 10_000 reads at most 256.
  - `postgres_a_tombstone_beats_the_indexing_pass_when_dsn_is_set`: run the pass's write statement directly against a row that was tombstoned after being read; assert `search_tokens IS NULL` and the statement reports zero rows.
  - `postgres_two_indexing_passes_at_once_index_every_claim_once_when_dsn_is_set` (Review Focus 5): 40 unindexed rows, two stores on two connections each paging to the end concurrently; assert neither errors, every row has non-`NULL` `search_tokens`, and the sum of `indexed` is at least 40.
  - `postgres_the_indexing_pass_stays_in_its_scope_when_dsn_is_set`: rows of another tenant and another space stay `NULL`.
- [ ] **Step 2: Run** with the DSN. Expected: FAIL to compile.
- [ ] **Step 3: Implement.** Read `claim_id, record` where `search_tokens IS NULL AND NOT tombstoned` after the cursor, in claim-id order. Write each with `UPDATE ... SET (four columns) WHERE tenant, space, claim_id AND search_tokens IS NULL AND NOT tombstoned`; count a row as indexed only when the update reports one row.
- [ ] **Step 4: Run** all four test files with the DSN. Expected: PASS.
- [ ] **Step 5: Commit** `Index claims that predate the search migration (issue #69)`.

### Task 10: Communal types, ports, policy and bundle (spec 6.4)

**Files:**
- Create: `crates/rakka-agent/src/communal.rs`
- Modify: `crates/rakka-agent/src/lib.rs` (module and re-exports), `memory.rs` (`AgentRunMemory` :2666)
- Test: unit tests in `communal.rs`

**Interfaces:**
- Produces, all public and re-exported:
  - Bounds: `AGENT_SNAPSHOT_COMMUNAL_CLAIM_MAX_ENTRIES: usize = 16`, `AGENT_SNAPSHOT_COMMUNAL_CLAIM_MAX_BYTES: usize = 64 * 1024`, `AGENT_COMMUNAL_RETRIEVAL_MAX_SPACES: usize = 16`, `AGENT_COMMUNAL_CLAIM_OBJECT_INLINE_MAX_BYTES: usize = 4096`, `AGENT_COMMUNAL_CLAIM_MAX_EVIDENCE: usize = 16`, `AGENT_COMMUNAL_CLAIM_MAX_READ_CAPABILITIES: usize = 16`.
  - `AgentCommunalClaimTrust { Proposed, Verified, Disputed, Retracted }`, `#[non_exhaustive]`, kebab-case, `as_label`.
  - `CommunalClaimObject { Node(String), Value(AgentTaskContent) }`, `CommunalClaimStatement { subject: String, predicate: String, object: CommunalClaimObject }` with `validate()` and `digest() -> AgentContentDigest`.
  - `CommunalClaimProvenance { agent: AgentId, goal: Option<AgentGoalId>, task: Option<AgentTaskId>, run: Option<AgentRunId>, delegation: Option<AgentDelegationId> }`.
  - `CommunalClaimRecord { claim_id: AgentCommunalClaimId, space: KnowledgeSpaceId, statement, statement_digest: AgentContentDigest, trust, provenance, evidence: Vec<ArtifactRef>, created_at: AgentTimestampMillis, confidence_bps: u16, classification: MemoryClassification, required_read_capabilities: BTreeSet<AgentCapabilityId> }` with `validate() -> Result<(), MemoryError>`.
  - `CommunalRetrievalQuery { text: String, limit: usize, min_confidence_bps: u16, classifications: BTreeSet<MemoryClassification>, trust: BTreeSet<AgentCommunalClaimTrust>, reader: BTreeSet<AgentCapabilityId> }` with `admits(&CommunalClaimRecord) -> bool`.
  - `RankedCommunalClaim { claim: AgentCommunalClaimId, relevance_bps: u16 }`, `CommunalRetrievalOutcome { ranked: Vec<RankedCommunalClaim>, index_watermark: Option<String>, index_incomplete: bool }`.
  - Traits `AgentCommunalClaimRetriever`, `AgentCommunalClaimAuthority`, `AgentCommunalReadGrantSource`, with the signatures in spec 6.4 and 6.5.
  - `AgentCommunalReadGrant { spaces: BTreeSet<KnowledgeSpaceId>, read_capabilities: BTreeSet<AgentCapabilityId>, definition_revision: AgentRevisionNumber, setup_revision: Option<AgentRevisionNumber> }`.
  - `CommunalRetrievalPolicy` with clamping builders and the defaults in spec 6.4's table; `max_resolutions()` = `max_results * 4` capped at 64.
  - `AgentCommunalRetrieval::new(retriever: Arc<dyn AgentCommunalClaimRetriever>, authority: Arc<dyn AgentCommunalClaimAuthority>, grants: Arc<dyn AgentCommunalReadGrantSource>, guardrails: AgentGuardrailChain, policy: CommunalRetrievalPolicy) -> Self`, accessors for each.
  - `AgentRunMemory::with_communal_retrieval(self, AgentCommunalRetrieval) -> Self`, `communal_retrieval(&self) -> Option<&AgentCommunalRetrieval>`.

- [ ] **Step 1: Write failing unit tests**
  - `the_default_policy_admits_only_verified`: `trust == {Verified}`, `classifications == {Unclassified}`, `max_results == 8`, `max_bytes == 65_536`, `max_spaces == 4`, `query_entries == 4`, `min_confidence_bps == 0`, `max_resolutions() == 32`.
  - `no_policy_can_admit_retracted`: `with_trust({Verified, Retracted})` leaves `{Verified}`.
  - `policy_knobs_clamp`: `max_results(0) → 1`, `max_results(99) → 16`, `max_spaces(99) → 16`, `max_bytes(usize::MAX) → 65_536`.
  - `a_query_admits_only_what_the_reader_may_read`: one assertion per arm of `admits`, including a `Retracted` record under a query whose trust set was built by hand to include it.
  - `a_record_over_a_bound_fails_validation`: 4097 inline bytes, 17 evidence references, 17 capabilities, confidence 10_001.
  - `the_trust_labels_match_the_graphs`: `"proposed"`, `"verified"`, `"disputed"`, `"retracted"`.
- [ ] **Step 2: Run** `cargo test -p rakka-agent --lib communal`. Expected: FAIL to compile.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** the same command. Expected: PASS.
- [ ] **Step 5: Commit** `Add the communal claim ports, policy and bundle (issue #69)`.

### Task 11: The read grant (spec 6.5)

**Files:**
- Modify: `crates/rakka-agent/src/communal.rs`
- Create: `crates/rakka-agent/tests/communal_read_grant.rs`

**Interfaces:**
- Consumes: `load_agent_entity_state` (`agent.rs:640`), `AgentRunSetupResolver` (`dispatch.rs:1815`), Task 3's envelope field.
- Produces: `AgentEntityCommunalReadGrants<Store>::new(store: Arc<Store>, policy: AgentSchemaPolicy) -> Self` where `Store: DurableStateStore<AgentEntityState>`, `with_setups(self, Arc<dyn AgentRunSetupResolver>) -> Self`, implementing `AgentCommunalReadGrantSource` with `backend_name() == "agent-entity"`; `AgentCommunalReadGrant::narrowed_by(&self, envelope: Option<&AgentRunDelegationEnvelope>) -> AgentCommunalReadGrant`.

- [ ] **Step 1: Write failing tests** in `communal_read_grant.rs`
  - `a_definition_alone_grants_its_spaces_and_read_capabilities`
  - `a_setup_narrows_the_definition`
  - `a_definition_narrowed_below_its_setup_still_binds` (the answer is the intersection)
  - `an_agent_that_does_not_exist_has_no_grant` (`None`)
  - `an_agent_the_dispatcher_would_refuse_has_no_grant`: use the lifecycle statuses `AgentEntityAuthority::authorize` (`dispatch.rs:1905`) refuses; `None` for each.
  - `a_delegation_statement_intersects_the_grant`; `lineage_without_a_statement_grants_nothing`; `a_root_without_a_statement_keeps_the_grant`; `no_envelope_keeps_the_grant`. Each asserts both sets.
- [ ] **Step 2: Run** `cargo test -p rakka-agent --test communal_read_grant`. Expected: FAIL to compile.
- [ ] **Step 3: Implement.** The lineage test for `narrowed_by` is the one the claim-append check uses at `run.rs:6603`; share it rather than copying it.
- [ ] **Step 4: Run** the same command. Expected: PASS.
- [ ] **Step 5: Commit** `Read a run's communal grant from its definition, setup and delegation (issue #69)`.

### Task 12: The attestation covers the communal bundle (spec 6.4)

**Files:**
- Modify: `crates/rakka-agent/src/tools.rs` (`with_memory_ingress` :1618, `attests` :1646, `declared_by` :1723)
- Test: `crates/rakka-agent/tests/memory_guardrail_chain_consistency.rs`

- [ ] **Step 1: Write failing tests**
  - `a_communal_bundle_with_another_chain_is_refused_beside_a_matching_private_bundle`: the error's code is `"guardrail-chain-mismatch"`.
  - `a_communal_only_memory_with_the_matching_chain_attests`
  - `attests_is_false_when_the_communal_bundle_drifted`
  - `a_memory_with_no_bundle_of_either_kind_is_still_refused`
- [ ] **Step 2: Run** `cargo test -p rakka-agent --test memory_guardrail_chain_consistency`. Expected: the first and third FAIL (attestation succeeds), the second FAILS (refused).
- [ ] **Step 3: Implement.** `declared_by` becomes the list of declarations of every bundle present. Attestation holds when the list is non-empty and every entry equals the authority's. The error reports the first that differs.
- [ ] **Step 4: Run** the same file and `--test memory_ingress_guardrails --test tool_authority`. Expected: PASS.
- [ ] **Step 5: Commit** `Attest the memory-ingress chain of every retrieval bundle (issue #69)`.

### Task 13: The snapshot record and its version rule (spec 6.7)

**Files:**
- Modify: `crates/rakka-agent/src/memory.rs` (`SnapshotBudget` :1071, `MemoryContextSnapshot` :1100, `compute_digest` :1167, record and `Deserialize` :1192-1235, constructor :1524), `crates/rakka-agent/src/schema.rs` (:154)
- Test: `crates/rakka-agent/tests/schema_compatibility.rs`, `compatibility_currency.rs`, new `crates/rakka-agent/tests/communal_snapshot_record.rs`; `crates/rakka-agent-postgres/tests/memory_conformance.rs`

**Interfaces:**
- Consumes: Task 10's types.
- Produces: `SnapshotCommunalClaim { claim_id, space, trust, statement, statement_digest, provenance, evidence, created_at, classification, confidence_bps, relevance_bps, transforms: Vec<SnapshotIngressRecord>, reports: Vec<SnapshotIngressRecord> }`; `SnapshotCommunalRetrieval { query: String, retriever: String, retriever_version: AgentRevisionNumber, index_watermark: Option<String>, spaces: Vec<KnowledgeSpaceId>, trust: BTreeSet<AgentCommunalClaimTrust>, classifications: BTreeSet<MemoryClassification>, min_confidence_bps: u16, index_incomplete: bool, ingress_revision: Option<AgentRevisionNumber>, definition_revision: AgentRevisionNumber, setup_revision: Option<AgentRevisionNumber> }`; `MemoryContextSnapshot::communal_claims: Vec<SnapshotCommunalClaim>`, `communal: Option<SnapshotCommunalRetrieval>`; `SnapshotBudget::communal_claim_bytes: usize`; `CURRENT_AGENT_MEMORY_CONTEXT_SNAPSHOT_SCHEMA_VERSION` = 2; `MemoryContextSnapshot::set_communal(&mut self, retrieval: SnapshotCommunalRetrieval, claims: Vec<SnapshotCommunalClaim>, bytes: usize)`, which sets the fields, moves the record to version 2 and recomputes the digest.

- [ ] **Step 1: Write failing tests** in `communal_snapshot_record.rs`
  - `a_snapshot_without_a_communal_search_is_version_1` (and Task 1's pin still holds).
  - `a_snapshot_with_a_communal_search_is_version_2`, including one that searched and selected nothing.
  - `the_version_2_digest_covers_the_communal_record`: changing `communal.query` changes `compute_digest()`; changing only `budget` does not.
  - `a_version_1_snapshot_with_communal_claims_is_refused`, `a_version_1_snapshot_with_a_communal_record_is_refused`, `a_version_2_snapshot_without_a_communal_record_is_refused`: each built as a JSON literal and decoded with `serde_json::from_str::<MemoryContextSnapshot>`; assert `Err`.
  - `the_86cf087_shape_cannot_read_a_claim_as_a_memory_id`: decode a version 2 snapshot with one claim into a local copy of the `86cf087` record struct; assert `Err`.
  - `relevance_is_clamped_to_10000`.
- [ ] **Step 2: Add to `crates/rakka-agent-postgres/tests/memory_conformance.rs`** (Review Focus 4) `postgres_a_version_2_snapshot_persists_and_loads_when_dsn_is_set` and `postgres_a_version_1_snapshot_still_loads_when_dsn_is_set`.
- [ ] **Step 3: Run** `cargo test -p rakka-agent --test communal_snapshot_record`. Expected: FAIL to compile.
- [ ] **Step 4: Implement.** The version is chosen when the record is built, as issue #119 staged the decision event's. The three refusals go in `impl Deserialize for MemoryContextSnapshot`. `SnapshotBudget` and the snapshot's new fields use serde attributes that leave a version 1 record's bytes unchanged; the existing `communal_claims` count in the budget stays.
- [ ] **Step 5: Run** `cargo test -p rakka-agent --test communal_snapshot_record --test communal_read_golden_bytes --test schema_compatibility --test compatibility_currency --test session_memory --test private_memory_retrieval` and `RAKKA_POSTGRES_TEST_DSN=... cargo test -p rakka-agent-postgres`. Expected: PASS. `compatibility_currency` and `schema_compatibility` pin the current version against `docs/rakka-compatibility.md`; update the document's snapshot row here with the version rule.
- [ ] **Step 6: Commit** `Record selected communal claims in the context snapshot (issue #69)`.

### Task 14: Assembly (spec 6.6)

**Files:**
- Modify: `crates/rakka-agent/src/communal.rs`, `retrieval.rs` (`assemble_context` :1074, `AssembledContext` :1018, its seven in-file callers), `guardrails.rs` (:246, :298), `run.rs` (:8707, the call only)
- Create: `crates/rakka-agent/tests/communal_claim_retrieval.rs`

**Interfaces:**
- Consumes: Tasks 10, 11, 13.
- Produces:
  - `assemble_context(memory: &AgentRunMemory, scope: &AgentRunScope, delegation: Option<&AgentRunDelegationEnvelope>, reference: &AgentContextSnapshotRef, turn: u64, policy_revision: AgentRevisionNumber, now: AgentTimestampMillis) -> Result<AssembledContext, MemoryError>`
  - `AssembledContext::communal: CommunalRetrievalReport`
  - `CommunalRetrievalReport { attempted: bool, degraded: bool, selected: usize, rejected: usize, unverified: usize, blocked: usize, transformed: usize, reported: usize, checkpoint_refused: usize, resolution_capped: bool, spaces_searched: usize, spaces_capped: bool, index_incomplete: bool }`
  - `AgentGuardrailContext::claim: Option<&'a AgentCommunalClaimId>`, `claim_space: Option<&'a KnowledgeSpaceId>`, `with_claim(self, claim: &'a AgentCommunalClaimId, space: &'a KnowledgeSpaceId) -> Self`
  - `pub fn derive_communal_query(window: &[SnapshotSessionEntry], policy: &CommunalRetrievalPolicy, reader: BTreeSet<AgentCapabilityId>) -> Option<CommunalRetrievalQuery>`

- [ ] **Step 1: Write failing tests** in `communal_claim_retrieval.rs`, against hand-written doubles of the three ports that record their calls. One test per line of spec 8.2's assembly list:
  - `a_granted_admitted_claim_is_embedded_from_the_authoritative_record`: every `SnapshotCommunalClaim` field but `relevance_bps` equals the authority's record, where the retriever's double disagrees with it.
  - `an_identity_the_authority_does_not_hold_is_unverified`
  - `retrieve_is_never_called_outside_the_effective_grant`; `a_claim_of_another_space_is_unverified`
  - `a_record_the_query_no_longer_admits_is_rejected`: three cases (trust moved to `Retracted`, classification not admitted, capability not held).
  - `a_record_naming_another_claim_or_space_is_rejected`
  - `a_blocked_claim_is_dropped`, `a_checkpoint_required_claim_is_dropped`, `a_transform_that_is_not_a_statement_is_dropped`, `a_transform_that_changes_a_node_object_is_dropped`, `a_transform_that_changes_an_artifact_reference_is_dropped`, `a_legal_transform_is_embedded_with_its_record_and_digest`
  - `the_guardrail_sees_the_statement_json_and_the_claim`: the stage double receives `{"subject","predicate","object"}` and a context whose `claim` and `claim_space` are set.
  - `the_selection_is_a_rank_prefix_under_max_results`, `..._under_max_bytes`; `the_walk_stops_at_max_resolutions`
  - `a_repeated_identity_is_rejected_once_embedded`
  - `rankings_merge_by_relevance_then_space_then_claim`
  - `spaces_are_searched_in_ascending_order_up_to_max_spaces`: five granted spaces, `max_spaces` 4, `spaces_capped` true, the fifth never retrieved.
  - `a_grant_source_error_degrades`, `one_failing_space_degrades_and_the_others_continue`, `an_authority_error_keeps_the_verified_prefix`: each returns `Ok`.
  - `an_incomplete_index_reaches_the_report_and_the_snapshot`
  - `an_unwired_run_writes_todays_bytes`, `a_run_with_no_query_text_writes_version_1`, `a_run_with_no_granted_space_writes_version_1`
  - `the_private_half_is_unchanged_by_a_communal_bundle`: the private selection and `RetrievalReport` are equal with and without it.
- [ ] **Step 2: Run** `cargo test -p rakka-agent --test communal_claim_retrieval`. Expected: FAIL to compile.
- [ ] **Step 3: Implement** the communal half as a function in `communal.rs` that `assemble_context` calls after the private half, following spec 6.6's five steps in order. `run.rs` passes `loop_state.delegation_envelope()`.
- [ ] **Step 4: Run** `cargo test -p rakka-agent --test communal_claim_retrieval --test private_memory_retrieval --test memory_ingress_guardrails --test communal_read_golden_bytes --test run_entity` and `cargo test -p rakka-agent --lib retrieval`. Expected: PASS.
- [ ] **Step 5: Commit** `Select communal claims into the context snapshot (issue #69)`.

### Task 15: Telemetry (spec 6.9)

**Files:**
- Modify: `crates/rakka-agent/src/observability.rs` (:1129, catalogue :1632), `lib.rs`, `run.rs` (:8726-8760), `docs/rakka-agent-observability-catalogue.md`
- Test: `crates/rakka-agent/tests/metric_catalogue.rs`, `agent_metrics.rs`

**Interfaces:**
- Produces: `METRIC_AGENT_COMMUNAL_RETRIEVALS = "rakka.agent.communal.retrievals"` (labels `backend`, `outcome` ∈ `retrieved`, `degraded`, `skipped`); `METRIC_AGENT_COMMUNAL_INGRESS_OUTCOMES = "rakka.agent.communal.ingress.outcomes"` (label `outcome` ∈ `blocked`, `transformed`, `reported`, `checkpoint-refused`, `rejected`, `unverified`, `resolution-capped`, `index-incomplete`, `spaces-capped`).

- [ ] **Step 1: Write failing tests** in `agent_metrics.rs`: `a_communal_retrieval_counts_under_its_own_names` (one run turn with a communal bundle; assert the two new counters and that the two private counters did not move); `communal_counters_carry_no_claim_text_or_ids` (no label value contains the claim's words, id or space). In `metric_catalogue.rs`, the catalogue lists both names.
- [ ] **Step 2: Run** `cargo test -p rakka-agent --test agent_metrics --test metric_catalogue`. Expected: FAIL.
- [ ] **Step 3: Implement,** emitting beside the private counters from `assembled.communal`, with `backend` read from the bundle's retriever. If the private retrieval emits a telemetry segment, emit its sibling with the tier named; if it does not, add none.
- [ ] **Step 4: Run** the two files and `--test telemetry_segments --test product_doc_currency`. Expected: PASS.
- [ ] **Step 5: Commit** `Count communal retrievals and their ingress outcomes (issue #69)`.

### Task 16: The graph crate's adapters (spec 6.8)

**Files:**
- Create: `crates/rakka-agent-knowledge-graph/src/communal_retrieval.rs`, `crates/rakka-agent-knowledge-graph/tests/communal_retrieval.rs`
- Modify: `crates/rakka-agent-knowledge-graph/src/lib.rs` (module, re-exports, non-goals at :54)

**Interfaces:**
- Consumes: Task 5's `search`, Task 10's ports.
- Produces: `KnowledgeGraphCommunalRetriever::new(store: Arc<dyn KnowledgeGraphStore>) -> ClaimResult<Self>` (refuses a store without `KeywordSearch` with `CapabilityUnsupported`; `retriever_version()` is `AgentRevisionNumber::new(1)`; `backend_name()` is the store's); `KnowledgeGraphCommunalAuthority::new(store: Arc<dyn KnowledgeGraphStore>) -> Self`; `impl From<ClaimTrustStatus> for AgentCommunalClaimTrust` and a fallible reverse, both by exhaustive `match`.

- [ ] **Step 1: Write failing tests**
  - `the_retriever_refuses_a_store_that_cannot_search`
  - `the_retriever_maps_the_query_onto_a_search`: a store double records the `ClaimSearch`; assert text, trust, classifications, floor, reader and limit.
  - `the_retriever_answers_identities_and_scores_only`; `an_unindexed_report_becomes_index_incomplete`
  - `the_authority_maps_a_live_claim_field_by_field`; `the_authority_answers_none_for_a_stub`; `the_authority_answers_none_for_another_scope`
  - `the_mirrored_bounds_equal_the_graphs`: the three `AGENT_COMMUNAL_CLAIM_*` constants equal `CLAIM_OBJECT_INLINE_MAX_BYTES`, `CLAIM_MAX_EVIDENCE_ARTIFACTS`, `CLAIM_MAX_ACL_CAPABILITIES`.
  - `the_trust_labels_round_trip` over `ClaimTrustStatus::ALL`.
- [ ] **Step 2: Run** `cargo test -p rakka-agent-knowledge-graph --test communal_retrieval`. Expected: FAIL to compile.
- [ ] **Step 3: Implement.** Remove "no communal retrieval path" and "no per-claim capability enforcement at read" from the crate's non-goals.
- [ ] **Step 4: Run** `cargo test -p rakka-agent-knowledge-graph`. Expected: PASS.
- [ ] **Step 5: Commit** `Serve the communal read ports from the knowledge graph (issue #69)`.

### Task 17: End to end (spec 8.3)

**Files:**
- Create: `examples/multi-agent-goal-acceptance/tests/communal_read_path.rs` (that example already depends on both crates and drives real run entities; reuse its wiring)

- [ ] **Step 1: Write the test** `a_run_reads_only_the_communal_claims_it_is_granted`: an in-memory graph store, both adapters, `AgentEntityCommunalReadGrants`, a policy admitting `Verified`. Append a claim requiring capability `cap-a` to space `s1` and move it to `Verified`. Three runs each take one model turn:
  - definition grants `s1` and `cap-a`: the persisted snapshot is version 2 and holds the claim with trust `verified`;
  - definition lacks `s1`: version 1, no claim;
  - definition grants `s1` without `cap-a`: version 2, no claim.
- [ ] **Step 2: Run** `cargo test -p rakka-example-multi-agent-goal-acceptance --test communal_read_path`. Expected: PASS. If it fails, the defect is in an earlier task: fix it there with a failing test in that task's file first.
- [ ] **Step 3: Run** `cargo run -p rakka-example-multi-agent-goal-acceptance` and compare with the stdout its `README.md` documents. Expected: unchanged.
- [ ] **Step 4: Commit** `Prove the communal read path end to end (issue #69)`.

### Task 18: Documents and changelog (spec 9)

**Files:**
- Modify: `docs/rakka-agents.md`, `docs/rakka-agent-security-validation-matrix.md`, `docs/rakka-compatibility.md`, `docs/rakka-api-boundary-inventory.md`, `docs/rakka-v1-known-limitations-roadmap.md`, `CHANGELOG.md`, the crate docs of `rakka-agent-knowledge-graph` and `rakka-agent-knowledge-graph-postgres`, and wherever the A2A collaboration envelopes are documented (`grep -rn 'knowledge-spaces' docs crates/rakka-a2a`)

- [ ] **Step 1: Run** the document tests first: `cargo test -p rakka-agent --test product_doc_currency --test compatibility_currency --test crate_shape` and `cargo test -p rakka-agent-knowledge-graph --test compatibility_currency`. Note which fail; each is a claim the documents now owe.
- [ ] **Step 2: Write.**
  - `rakka-agents.md`: the read path, the read-capability grant, the `Verified`-only default and how to admit `Proposed`, what a snapshot keeps (spec 6.10), `index-incomplete`.
  - Security matrix: the "communal memory treated as an injection source" row becomes a stated control naming the tests of Tasks 12 and 14; #69 leaves the owed list.
  - Compatibility: the roll order in spec 7 as six numbered steps, the snapshot's version rule, the A2A field's cross-version posture.
  - PostgreSQL graph crate docs: the migration and its lock cost, `index_claims`, the trigger, and that a tombstone and a delete clear the search index. Add the index to the list of what issue #68's erasure reaches.
  - API boundary inventory: every new public item, by tier.
  - Changelog: the feature, the `assemble_context` signature change, the new compatibility surface in spec 6.11.
- [ ] **Step 3: Run** Step 1's commands. Expected: PASS.
- [ ] **Step 4: Commit** `Document the communal read path (issue #69)`.

### Task 19: Whole-workspace validation

- [ ] **Step 1:** `cargo clean`, then `scripts/validate.sh > "$TMPDIR/validate-69.log" 2>&1; echo $? > "$TMPDIR/validate-69.exit"`. Read the exit file before recording anything. Expected: `0`. If the one-shot workspace test is killed, or `rakka-agent-mcp`'s `child_process_launcher` fails once, rerun that crate alone with `--all-features` before concluding anything.
- [ ] **Step 2:** `scripts/package-check.sh > "$TMPDIR/package-69.log" 2>&1; echo $?`. Expected: `0`.
- [ ] **Step 3:** With the DSN set, `cargo test -p rakka-agent-knowledge-graph-postgres` and `cargo test -p rakka-agent-postgres`. Expected: PASS, with no "skipped" announcement from the graph crate.
- [ ] **Step 4:** `cargo test -p rakka-agent --test communal_read_golden_bytes` and `cargo test -p rakka-a2a --test collaboration_surface`. Expected: PASS, with the literals of Task 1 untouched (`git diff 86cf087 -- crates/rakka-agent/tests/communal_read_golden_bytes.rs` shows only the file's creation).
- [ ] **Step 5: Commit** anything validation required, with a message that says what it fixed.

## After the merge (spec 11)

Not part of this plan's tasks: the report on issue #69 for the host, and the note on #140.
