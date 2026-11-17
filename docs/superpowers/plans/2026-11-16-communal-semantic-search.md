# Communal Claims Ranked By Meaning Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A deployment that opts in can rank a knowledge space's claims by meaning: each claim has one stored embedding, and a search compares the query's embedding with them under every guard a communal read already has.

**Architecture:** The store stays a pure store. Three optional SPI operations behind `semantic-search` take and return vectors; the in-memory store and PostgreSQL (pgvector, opt-in migration, exact scan) implement them. Three callers hold the embedder: the communal retriever, the claim-append executor, and a pass function a deployment calls. Context assembly, the snapshot shape and the retriever port do not change.

**Tech Stack:** Rust 1.88, `rakka-agent-knowledge-graph`, `rakka-agent-knowledge-graph-postgres` (`tokio-postgres`, pgvector), `rakka-agent`'s `AgentMemoryEmbedder`.

**Spec:** `docs/superpowers/specs/2026-11-16-communal-semantic-search-design.md` (issue #140). Section numbers below (6.1, proof 13, ...) are that document's.

## Global Constraints

- Before any `cargo`: `export PATH="$HOME/.rustup/toolchains/stable-aarch64-apple-darwin/bin:$PATH"`. Ignore the pre-existing `redundant_homepage` manifest warnings.
- `cargo clean` at every task boundary. `clippy -D warnings`. Every public item documented. `unsafe` forbidden.
- Commit messages name issue #140 and end with `Co-Authored-By: Claude Fable 5.2 <noreply@anthropic.com>`.
- No new error code and no new metric. Refusals reuse `claim-capability-unsupported` (naming `semantic-search`), `claim-embedding-invalid` (`ClaimError::InvalidEmbeddingRef`) and `claim-backend-failed`.
- **No existing signature changes and no existing SQL statement of the PostgreSQL crate changes.** `MIGRATION_SQL` and `MIGRATION_VALIDATE_SQL` must not mention `vector`. A store built without `with_semantic_search()` behaves exactly as today.
- No vector and no embedding text in any error message, log or `Debug` output. `ClaimEmbedding`'s `Debug` prints the reference and the vector's length only.
- The store calls no model and returns no vector. Storing an embedding changes no byte of a claim's record; `Claim.embedding` is not written.
- A failed embedding never fails a claim append.
- A `Redacted` claim is never sent to an embedder, whatever set a caller passes. A `Retracted` claim is not embedded.
- The classification set of the executor's `with_embedder` and of `embed_claims` is a required argument with no default.
- PostgreSQL tests: gated on `RAKKA_POSTGRES_TEST_DSN`. The always-on database on port 5432 has **no** pgvector. For pgvector runs start a throwaway `docker run -d --name rakka-140-pgvector -e POSTGRES_PASSWORD=postgres -p 5433:5432 pgvector/pgvector:pg16` and remove it when Task 6 and Task 8 are done. Do not use the containers on 25432 or 15432.
- Read `scripts/validate.sh`'s exit code from a file, never through a pipe. In zsh quote globs.
- After a compile-failure RED, falsify each guard once it is green: copy the file to the scratchpad, break the guard, see the named test fail, restore from the copy.

## Review Focus

Each line has a test in the task named.

1. A tombstone that commits between the pass's read of a claim and its embedding write: the embedding must not be stored. (Task 6, `postgres_a_tombstone_beats_a_late_embedding_when_dsn_is_set`; Task 2 clause 7 for the portable rule.)
2. A model or version change while claims hold vectors under the old reference: a search finds nothing from them and reports the index incomplete; it never ranks across references. (Task 2 clause 4; Task 5, `a_changed_embedder_finds_nothing_until_the_pass_has_run`.)
3. An embedder that answers a vector with no direction (all zeros): no error, never a hit, and in PostgreSQL the not-a-number distance does not break the statement or the order. (Task 2 clause 3; runs on PostgreSQL through Task 6's conformance.)
4. A query with empty or blank text: an empty ranking, and the embedder is not called. (Task 5, `a_blank_query_ranks_nothing_and_calls_no_model`.)
5. Two passes at once, and the pass beside the append executor, on the same claim: every claim ends with exactly one embedding and no call fails. (Task 6, `postgres_two_embedding_passes_at_once_embed_every_claim_when_dsn_is_set`.)

---

### Task 1: The portable types

**Files:**
- Create: `crates/rakka-agent-knowledge-graph/src/semantic.rs` (inline `#[cfg(test)] mod tests`)
- Modify: `crates/rakka-agent-knowledge-graph/src/lib.rs` (`pub mod semantic;` and re-exports of every public item below)
- Modify: `crates/rakka-agent-knowledge-graph/src/search.rs` (`ClaimSearchHit::relevance_bps` and `ClaimSearchReport` docs: also the answer of a meaning-based search; a hit's relevance is never zero in either)
- Modify: `crates/rakka-agent-knowledge-graph/src/claim.rs` (`Claim::embedding` and `with_embedding` docs: the appender's own note, not the state of any index; nothing in Rakka writes or reads it)

**Interfaces:**
- Consumes: `rakka_agent::{MemoryEmbeddingRef, memory_embedding_text, AGENT_MEMORY_EMBEDDING_MODEL_MAX_LENGTH}`, `crate::search::ClaimSearch`, `ClaimError::InvalidEmbeddingRef { message }`.
- Produces:

```rust
pub const CLAIM_EMBEDDING_MAX_DIMENSIONS: u32 = 16_000;
pub const CLAIM_EMBEDDING_PAGE_MAX_CLAIMS: usize = 64;

pub fn claim_embedding_text(claim: &Claim) -> Option<String>;

#[derive(Clone, PartialEq)]            // Debug by hand: reference + vector length
pub struct ClaimEmbedding { pub reference: MemoryEmbeddingRef, pub vector: Vec<f32> }
impl ClaimEmbedding { pub fn validate(&self) -> ClaimResult<()>; }

#[derive(Debug, Clone, PartialEq)]
pub struct ClaimSemanticSearch { /* private */ }
impl ClaimSemanticSearch {
    pub fn new(filters: ClaimSearch, query: ClaimEmbedding) -> Self;
    pub const fn filters(&self) -> &ClaimSearch;
    pub const fn query(&self) -> &ClaimEmbedding;
    pub const fn limit(&self) -> usize;              // the filters' limit
    pub fn admits(&self, claim: &Claim) -> bool;     // the filters' admits
}

pub fn claim_semantic_relevance_bps(query: &[f32], candidate: &[f32]) -> u16;
pub fn claim_semantic_distance_relevance_bps(cosine_distance: f64) -> u16;

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ClaimEmbeddingPage { pub claims: Vec<Claim>, pub undecodable: u64, pub next: Option<ClaimId> }
```

Rules the bodies must keep:
- `claim_embedding_text`: `None` for a tombstoned claim. Otherwise the subject, the predicate, and the object's text, joined by single spaces. A node object gives its node id; an inline value gives `memory_embedding_text`; an artifact-backed object gives nothing (the text is then `"<subject> <predicate>"`).
- `validate`: refuses (`claim-embedding-invalid`) an empty model or one longer than `AGENT_MEMORY_EMBEDDING_MODEL_MAX_LENGTH`, zero dimensions, more than `CLAIM_EMBEDDING_MAX_DIMENSIONS`, a vector whose length is not `reference.dimensions`, and a non-finite component. The message names the model and the two lengths, never a component.
- `claim_semantic_distance_relevance_bps(d)` = `((1 - d/2) * 10000).round()` clamped to `0..=10000`; a non-finite `d` gives 0.
- `claim_semantic_relevance_bps`: computed in `f64`; 0 when the lengths differ, when either vector has zero magnitude, or when the cosine is not finite; otherwise `claim_semantic_distance_relevance_bps(1 - cos)`.

- [ ] **Step 1: Write the failing tests** in `semantic.rs`'s test module, building claims the way `search.rs`'s test module does (`claim(discriminator, subject, predicate, object)`, `inline(value)`, `artifact()`):

```rust
#[test] fn a_claims_embedding_text_is_its_subject_predicate_and_object() {
    // node object            -> "shipment status depot"
    // inline string "at the depot" -> "shipment status at the depot"
    // inline json!({"b":1,"a":2}) -> r#"shipment status {"a":2,"b":1}"#
    // artifact-backed        -> "shipment status"
    // tombstoned stub        -> None
}
#[test] fn an_embedding_is_refused_unless_it_matches_its_reference() {
    // valid (dimensions 3, three finite values) -> Ok
    // each of: 2 values for 3 dimensions; f32::NAN; f32::INFINITY; dimensions 0 with an empty
    // vector; dimensions 16_001; model ""; model of AGENT_MEMORY_EMBEDDING_MODEL_MAX_LENGTH + 1
    //   -> Err whose code() == "claim-embedding-invalid"
    // format!("{embedding:?}") contains "3" and does not contain "0.25" (a component used)
}
#[test] fn relevance_is_the_private_tiers_mapping_of_cosine() {
    assert_eq!(claim_semantic_relevance_bps(&[1.0, 0.0], &[1.0, 0.0]), 10_000);
    assert_eq!(claim_semantic_relevance_bps(&[2.0, 0.0], &[1.0, 0.0]), 10_000);
    assert_eq!(claim_semantic_relevance_bps(&[1.0, 0.0], &[0.6, 0.8]), 8_000);
    assert_eq!(claim_semantic_relevance_bps(&[1.0, 0.0], &[0.0, 1.0]), 5_000);
    assert_eq!(claim_semantic_relevance_bps(&[1.0, 0.0], &[-1.0, 0.0]), 0);
    assert_eq!(claim_semantic_relevance_bps(&[1.0, 0.0], &[0.0, 0.0]), 0);
    assert_eq!(claim_semantic_relevance_bps(&[1.0, 0.0], &[1.0, 0.0, 0.0]), 0);
    for (distance, bps) in [(0.0, 10_000), (0.4, 8_000), (1.0, 5_000), (2.0, 0), (2.5, 0), (-0.1, 10_000)] {
        assert_eq!(claim_semantic_distance_relevance_bps(distance), bps);
    }
    assert_eq!(claim_semantic_distance_relevance_bps(f64::NAN), 0);
}
#[test] fn a_semantic_search_admits_and_limits_as_its_filters_do() {
    // filters ClaimSearch::new("", 1000) with trust {Verified}: limit() == 64 and a fresh
    // (Proposed) claim is not admitted; with trust {Proposed} and its classification, it is.
}
```

- [ ] **Step 2: Run to verify they fail.** Run: `cargo test -p rakka-agent-knowledge-graph --lib semantic` Expected: compile failure, unresolved names.
- [ ] **Step 3: Implement the module and the exports; reword the four docs.**
- [ ] **Step 4: Run to verify they pass.** Run: `cargo test -p rakka-agent-knowledge-graph --lib` Expected: PASS, 0 failed.
- [ ] **Step 5: Commit.** `git commit -m "Add the portable types of a meaning-based claim search (issue #140)"`

### Task 2: The store operations, the in-memory store, the conformance clauses

**Files:**
- Modify: `crates/rakka-agent-knowledge-graph/src/store.rs` (three trait methods with refusing default bodies; `Inner` gains the embeddings; the in-memory implementations; `capabilities()` adds `SemanticSearch`; the module doc's sentence that no backend serves the capability)
- Create: `crates/rakka-agent-knowledge-graph/src/conformance/semantic.rs`
- Modify: `crates/rakka-agent-knowledge-graph/src/conformance.rs` (`mod semantic;`, `pub use semantic::{semantic_search, semantic_search_refused};`, and `check_knowledge_graph_contract` runs `semantic_search` when the store reports the capability and `semantic_search_refused` when it does not)
- Test: `crates/rakka-agent-knowledge-graph/tests/knowledge_graph_conformance.rs`

**Interfaces:**
- Consumes: Task 1.
- Produces, on `KnowledgeGraphStore`:

```rust
fn search_semantic<'a>(&'a self, scope: &'a KnowledgeSpaceScope, search: &'a ClaimSemanticSearch)
    -> ClaimFuture<'a, ClaimSearchReport>;
fn store_claim_embedding<'a>(&'a self, scope: &'a KnowledgeSpaceScope, claim_id: &'a ClaimId,
    embedding: &'a ClaimEmbedding) -> ClaimFuture<'a, bool>;
fn unembedded_claims<'a>(&'a self, scope: &'a KnowledgeSpaceScope, reference: &'a MemoryEmbeddingRef,
    classifications: &'a BTreeSet<MemoryClassification>, after: Option<&'a ClaimId>, limit: usize)
    -> ClaimFuture<'a, ClaimEmbeddingPage>;
```

and `conformance::semantic_search(store, scopes)`, `conformance::semantic_search_refused(store, scopes)`.

The contract (spec 6.2), which the trait docs state and both backends keep:
- `search_semantic`: validates the query embedding first. A candidate is a live claim of the scope that `search.admits` and whose stored embedding has exactly the query's reference. Each candidate is scored with `claim_semantic_relevance_bps`; a score of 0 is not a hit. Hits are ordered with `rank_claim_search_hits`. `unindexed` is true when the scope holds a live claim the search admits that has no embedding under the query's reference.
- `store_claim_embedding`: validates first. Stores or replaces the claim's one embedding and answers `true`; answers `false` and stores nothing when the scope holds no such claim or the claim is tombstoned.
- `unembedded_claims`: the limit is clamped to `1..=CLAIM_EMBEDDING_PAGE_MAX_CLAIMS`. Lists, in ascending claim id after the cursor, the claims that are live, not `Retracted`, of a classification in the set, and without an embedding under the reference. `next` is the last claim read when the page was full, otherwise `None`. An empty set lists nothing.
- A tombstone and a delete remove the claim's embedding in the same operation.

- [ ] **Step 1: Write the conformance clauses.** `semantic_search_refused`: each of the three operations answers `Err` with `code() == "claim-capability-unsupported"` and a `to_string()` containing `semantic-search`. `semantic_search`, with reference `{ model: "conformance-embedder", dimensions: 3, version: 1 }`, query `[1,0,0]`, and a search admitting `Proposed` + `Unclassified` unless a sub-clause says otherwise (reuse `search.rs`'s shape: a private claim builder, `tombstone_request`, `transition_request`):

| Sub-clause (spec proof) | Arrange | Assert |
|---|---|---|
| Ranking (3) | claims embedded at `[1,0,0]`, `[0.6,0.8,0]`, `[0,1,0]` twice, `[-1,0,0]`, `[0,0,0]` | hit ids = first, second, then the two `[0,1,0]` claims by ascending id; relevance `[10000, 8000, 5000, 5000]`; the `[-1,0,0]` and `[0,0,0]` claims are not hits; `hits[0].claim` equals the stored claim; with limit 2 only the first two |
| Admission precedes ranking (2) | for each of: `Proposed` claim under a `Verified`-only search; `Sensitive`; confidence below the floor; a required read capability the reader lacks; tombstoned after embedding; a claim of the foreign scope. The excluded claim is embedded at exactly the query; `limit` admitted claims sit behind it at `[0.6,0.8,0]` | the hits are exactly the admitted claims; the excluded one is never a hit |
| Unembedded and other-reference claims (4) | an admitted claim with no embedding; then one stored under version 2; then under version 1 | not a hit and `unindexed == true` in the first two states; a hit and `unindexed == false` in the third |
| `unindexed` follows the filters (5) | only a `Sensitive` claim lacks an embedding; the search admits `Unclassified` | `unindexed == false`; a search that admits `Sensitive` reports `true` |
| Storing (6) | store the same embedding twice; replace under version 2; an absent claim id; a tombstoned claim; the primary's claim id in the foreign scope; a 2-value vector for 3 dimensions | `true`, `true`, one hit; a version 1 search no longer finds it; `false`; `false`; `false` and a foreign search has no hit; `Err` with `claim-embedding-invalid` |
| Erasure (7) | embed, then tombstone; embed another, then delete | neither is a hit; neither is listed by `unembedded_claims`; `store_claim_embedding` answers `false` for both |
| The page (8) | 5 unembedded claims, limit 2; 65 unembedded claims, limit 1000 and limit 0; one retracted; one `Sensitive` with the set `{Unclassified}`; the empty set | pages walk all 5 in ascending id through `next`, the last `next` is `None`; the 1000 page has 64 claims and a `next`; the limit 0 page has 1 claim; the retracted and the `Sensitive` claim are never listed; the empty set lists nothing; a claim embedded under the reference is not listed and one embedded under version 2 is |
| The record is untouched (8a) | `get` before and after `store_claim_embedding` | equal, and `embedding` on the claim is still `None` |

- [ ] **Step 2: Wire the clauses into the in-memory test file.** Add `semantic_search_admits_before_it_ranks` (in-memory store, `conformance::semantic_search`) and `a_store_without_the_capability_refuses_semantic_search` (the file's existing delegating wrapper that hides a capability, `conformance::semantic_search_refused`).
- [ ] **Step 3: Run to verify they fail.** Run: `cargo test -p rakka-agent-knowledge-graph --test knowledge_graph_conformance` Expected: compile failure (no such trait methods).
- [ ] **Step 4: Add the three trait methods with refusing default bodies.** Run the same command. Expected: `a_store_without_the_capability_refuses_semantic_search` PASS; `semantic_search_admits_before_it_ranks` FAIL with `claim-capability-unsupported`.
- [ ] **Step 5: Implement the in-memory store** (`embeddings: BTreeMap<String, BTreeMap<String, ClaimEmbedding>>` keyed like `claims`; removed in `tombstone` and `delete`; `capabilities()` reports `SemanticSearch`).
- [ ] **Step 6: Run to verify.** Run: `cargo test -p rakka-agent-knowledge-graph` Expected: PASS, 0 failed, including `scenario_20`'s whole-contract test.
- [ ] **Step 7: Falsify three guards**, one at a time, and see `semantic_search_admits_before_it_ranks` fail each time: drop the `admits` filter in `search_semantic`; skip the embedding removal in `tombstone`; drop the reference comparison in `search_semantic`. Restore.
- [ ] **Step 8: Commit.** `git commit -m "Serve semantic-search on the store SPI and the in-memory store (issue #140)"`

### Task 3: The embedding pass and the test embedder

**Files:**
- Create: `crates/rakka-agent-knowledge-graph/src/embedding_pass.rs`
- Modify: `crates/rakka-agent-knowledge-graph/src/conformance/semantic.rs` and `conformance.rs` (`ConceptEmbedder`, exported)
- Modify: `crates/rakka-agent-knowledge-graph/src/lib.rs` (`pub mod embedding_pass;`, re-exports)
- Test: `crates/rakka-agent-knowledge-graph/tests/claim_embedding_pass.rs` (new)

**Interfaces:**
- Consumes: Tasks 1 and 2; `rakka_agent::{AgentMemoryEmbedder, embed_memory_vector, tokenize_search_text}`.
- Produces:

```rust
// embedding_pass.rs
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ClaimEmbeddingPassPage { pub embedded: u64, pub failed: u64, pub undecodable: u64, pub next: Option<ClaimId> }

pub async fn embed_claims(store: &dyn KnowledgeGraphStore, embedder: &dyn AgentMemoryEmbedder,
    classifications: &BTreeSet<MemoryClassification>, scope: &KnowledgeSpaceScope,
    after: Option<&ClaimId>, limit: usize) -> ClaimResult<ClaimEmbeddingPassPage>;

/// Whether a caller holding `classifications` may send `claim` to an embedder: live, not
/// `Retracted`, not `Redacted`, and of a classification in the set.
pub(crate) fn may_embed(claim: &Claim, classifications: &BTreeSet<MemoryClassification>) -> bool;
/// Embeds one claim's text, validated. `None` on any model failure or invalid answer.
pub(crate) async fn embed_claim(embedder: &dyn AgentMemoryEmbedder, claim: &Claim) -> Option<ClaimEmbedding>;

// conformance (a test embedder that needs no model)
pub struct ConceptEmbedder { /* private */ }
impl ConceptEmbedder {
    /// One axis per concept, plus a last axis for a text that names no concept.
    pub fn new(concepts: &[&[&str]]) -> Self;        // model "concept-embedder", version 1
    pub fn with_version(self, version: u64) -> Self;
    pub fn failing_on(self, word: &str) -> Self;      // `embed` errs for a text holding the word
    pub fn answering_short_on(self, word: &str) -> Self; // answers one component too few
    pub fn calls(&self) -> Vec<String>;               // every text asked for, in order, shared across clones
}
impl AgentMemoryEmbedder for ConceptEmbedder { .. }
```

`ConceptEmbedder`: component `i` is the number of the text's words (`tokenize_search_text`) that belong to concept `i`; the last component is 1 when no word belongs to any concept, else 0. `Clone`; a `Mutex`, not an atomic `fetch_update` (MSRV 1.88).

`embed_claims`: removes `Redacted` from the set, reads one page of `unembedded_claims` under the embedder's reference, and for each claim: `embed_claim`; `None` adds to `failed`; otherwise `store_claim_embedding`, and `true` adds to `embedded` (a `false` is a claim withdrawn meanwhile: counted nowhere). `undecodable` and `next` are the store page's. A store error ends the call with that error.

- [ ] **Step 1: Write the failing tests** in `claim_embedding_pass.rs`, over `InMemoryKnowledgeGraphStore` and `ConceptEmbedder::new(&[&["car", "automobile"], &["boat", "ship"]])`:

```rust
#[tokio::test] async fn the_pass_embeds_what_is_missing_and_resumes_from_its_cursor()
// 5 claims; limit 2 -> embedded 2 + next; fed back twice more -> embedded 2, then 1 with next None;
// a search for the embedding of "automobile" then finds the claim that says "car".
#[tokio::test] async fn the_pass_is_idempotent()
// a second full walk: embedded 0, embedder.calls() unchanged in length.
#[tokio::test] async fn a_changed_reference_embeds_every_claim_again()
// same store, embedder.with_version(2): embedded == number of claims.
#[tokio::test] async fn a_claim_the_model_refuses_does_not_stop_the_claims_after_it()
// failing_on("boat") and, in a second case, answering_short_on("boat"), with the boat claim first by id:
// one call over all claims -> failed 1, embedded n-1, Ok; the boat claim is still listed by unembedded_claims.
#[tokio::test] async fn the_pass_sends_only_what_its_set_allows()
// Unclassified, Sensitive, Redacted and a retracted claim; set {Unclassified, Redacted}:
// embedder.calls() holds the Unclassified claim's text only.
#[tokio::test] async fn a_store_without_the_capability_ends_the_pass_with_its_refusal()
// the delegating wrapper from knowledge_graph_conformance.rs, copied: Err code "claim-capability-unsupported", no call.
```

- [ ] **Step 2: Run to verify they fail.** Run: `cargo test -p rakka-agent-knowledge-graph --test claim_embedding_pass` Expected: compile failure.
- [ ] **Step 3: Implement `ConceptEmbedder`, `may_embed`, `embed_claim`, `embed_claims`.**
- [ ] **Step 4: Run to verify they pass.** Same command. Expected: 6 passed.
- [ ] **Step 5: Falsify:** make `embed_claims` return the error on a model failure (test 4 fails); stop removing `Redacted` (test 5 fails). Restore.
- [ ] **Step 6: Commit.** `git commit -m "Add the claim embedding pass a deployment calls (issue #140)"`

### Task 4: The append executor embeds a fresh claim

**Files:**
- Modify: `crates/rakka-agent-knowledge-graph/src/append_executor.rs`
- Test: `crates/rakka-agent-knowledge-graph/tests/claim_append_executor.rs`

**Interfaces:**
- Consumes: `embedding_pass::{may_embed, embed_claim}`, `KnowledgeGraphStore::store_claim_embedding`, `ConceptEmbedder`.
- Produces: `KnowledgeGraphClaimAppendExecutor::with_embedder(self, embedder: Arc<dyn AgentMemoryEmbedder>, classifications: BTreeSet<MemoryClassification>) -> Self`.

After the store answers the append `Ok(appended)`, and only when an embedder is wired, the store reports `SemanticSearch` and `may_embed(&appended, &classifications)`: `embed_claim`, then `store_claim_embedding`. Every failure of that step, the store's included, is dropped. The finding is `Appended` exactly as today.

- [ ] **Step 1: Write the failing tests** (reuse the file's `append_request`, `intent`, `run_scope`, `provenance`):

```rust
#[tokio::test] async fn the_executor_embeds_a_fresh_claim()
// value "the car is at the depot"; after execute -> Appended; with no pass, search_semantic with the
// embedding of "automobile" has the claim as its first hit at 10_000 and unindexed == false.
#[tokio::test] async fn a_failed_embedding_does_not_fail_the_append()
// failing_on("car"), then answering_short_on("car"): the finding is Appended both times, the claim is
// stored, and unembedded_claims lists it.
#[tokio::test] async fn the_executor_sends_only_the_classifications_it_was_given()
// set {Unclassified}: a Sensitive append -> Appended, embedder.calls() empty.
// set {Unclassified, Redacted}: a Redacted append -> Appended, calls() empty.
// the empty set: an Unclassified append -> Appended, calls() empty.
#[tokio::test] async fn an_executor_without_an_embedder_embeds_nothing()
// KnowledgeGraphClaimAppendExecutor::new(store): Appended; unembedded_claims lists the claim.
#[tokio::test] async fn an_executor_over_a_store_without_the_capability_calls_no_model()
// the delegating wrapper that hides the capability (as in claim_embedding_pass.rs), with_embedder:
// Appended, embedder.calls() empty.
```

- [ ] **Step 2: Run to verify they fail.** Run: `cargo test -p rakka-agent-knowledge-graph --test claim_append_executor` Expected: compile failure (`with_embedder`).
- [ ] **Step 3: Implement `with_embedder` and the best-effort step.**
- [ ] **Step 4: Run to verify.** Same command. Expected: all pass, the four existing tests included; five new.
- [ ] **Step 5: Falsify:** propagate the embedding failure as an `Err` (test 2 fails); skip `may_embed` (test 3 fails). Restore.
- [ ] **Step 6: Commit.** `git commit -m "Embed a fresh claim at append, best effort (issue #140)"`

### Task 5: The retriever ranks by meaning

**Files:**
- Modify: `crates/rakka-agent-knowledge-graph/src/communal_retrieval.rs`, `src/lib.rs` (export the enum)
- Test: `crates/rakka-agent-knowledge-graph/tests/communal_retrieval.rs`

**Interfaces:**
- Consumes: Tasks 1 to 3; `embed_memory_vector`.
- Produces:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnowledgeGraphCommunalRanking { Keyword, Semantic }
impl KnowledgeGraphCommunalRetriever {
    pub fn with_embedder(self, embedder: Arc<dyn AgentMemoryEmbedder>) -> Self;
    pub fn ranking(&self) -> KnowledgeGraphCommunalRanking;
}
```

Rules:
- `with_embedder` keeps the embedder only when the store reports `SemanticSearch`; otherwise the retriever is unchanged. `ranking()` is `Semantic` exactly when an embedder was kept. `retriever_version()` is 2 for `Semantic` and 1 for `Keyword`.
- `Semantic` `retrieve`: a query whose text is empty or all whitespace answers an empty ranking with `index_incomplete: false` and calls no model *(my call; a word search of no words finds nothing either)*. Otherwise the query vector is `embed_memory_vector(embedder, &query.text)`, kept with its text in an `Arc<Mutex<Option<(String, Vec<f32>)>>>` and reused while the text is the same. The filters are built as today (`ClaimSearch::new(&query.text, query.limit)` with trust minus `Retracted`, classifications, confidence, reader). The answer maps as today; `index_incomplete` is the report's `unindexed`.
- A failed embedding returns that `MemoryError`. There is no fall back to `search`.

- [ ] **Step 1: Write the failing tests.** Extend `RecordingStore` to record `search_semantic` calls and add a constructor reporting both capabilities:

```rust
#[tokio::test] async fn a_retriever_given_an_embedder_and_a_capable_store_ranks_by_meaning()
// in-memory store: ranking() == Semantic, retriever_version() == 2;
// KnowledgeGraphCommunalRetriever::new(store) alone: Keyword, 1;
// RecordingStore::searching(..) (keyword only) with_embedder: Keyword, 1, and after a retrieve the
// store saw one `search` and the embedder no call.
#[tokio::test] async fn a_query_sharing_no_word_with_a_claim_finds_it_by_meaning()
// claim "the car is at the depot" embedded by embed_claims; query "automobile":
// the keyword retriever ranks nothing; the semantic one ranks the claim first with 10_000.
#[tokio::test] async fn one_query_is_embedded_once_across_spaces()
// retrieve in two spaces with one query -> embedder.calls().len() == 1; a second text -> 2.
#[tokio::test] async fn a_failed_query_embedding_fails_the_retrieval_and_falls_back_to_nothing()
// failing_on("automobile"); a claim that shares the word "automobile" is in the store:
// retrieve is Err(MemoryError), and RecordingStore saw no `search`.
#[tokio::test] async fn a_changed_embedder_finds_nothing_until_the_pass_has_run()
// claims embedded under version 1; retriever with_version(2): ranked empty, index_incomplete true;
// after embed_claims under version 2: the claim is ranked, index_incomplete false.
#[tokio::test] async fn a_blank_query_ranks_nothing_and_calls_no_model()
// text "" and "   ": ranked empty, index_incomplete false, calls() empty.
#[tokio::test] async fn the_semantic_retriever_maps_the_query_onto_a_search()
// RecordingStore with both capabilities: the recorded ClaimSemanticSearch carries the limit,
// trust without Retracted, classifications, confidence floor and reader of the query.
```

- [ ] **Step 2: Run to verify they fail.** Run: `cargo test -p rakka-agent-knowledge-graph --test communal_retrieval` Expected: compile failure.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run to verify.** Run: `cargo test -p rakka-agent-knowledge-graph` Expected: PASS, 0 failed.
- [ ] **Step 5: Falsify:** on an embedding error call `store.search` instead (test 4 fails); drop the memo (test 3 fails). Restore.
- [ ] **Step 6: Commit.** `git commit -m "Rank communal claims by meaning when an embedder is wired (issue #140)"`

### Task 6: PostgreSQL with pgvector

**Files:**
- Create: `crates/rakka-agent-knowledge-graph-postgres/src/semantic.rs`
- Modify: `crates/rakka-agent-knowledge-graph-postgres/src/lib.rs` (`mod semantic;`, re-exports, the `semantic_search: bool` field, `with_semantic_search`, `migrate`, `capabilities`, the three trait methods delegating to `semantic.rs`, the crate docs)
- Modify: `crates/rakka-agent-knowledge-graph-postgres/tests/postgres_conformance.rs`
- Create: `crates/rakka-agent-knowledge-graph-postgres/tests/postgres_semantic_search_proofs.rs`
- Modify: `.github/workflows/ci.yml` (the "Test PostgreSQL store" step's `env` gains `RAKKA_POSTGRES_PGVECTOR_REQUIRED: "1"`)

**Interfaces:**
- Consumes: Tasks 1 and 2; `decode_claim_row`, `drift_error`, `map_error`, `MIGRATION_LOCK_ID`, the existing `migrate` flow.
- Produces:

```rust
pub const EMBEDDING_TRIGGER_NAME: &str = "rakka_agent_knowledge_claim_clear_embedding";
pub const EMBEDDING_CHECK_NAME: &str = "rakka_agent_knowledge_claim_embedding_check";
pub const SEMANTIC_MIGRATION_SQL: &str;            // one batch, one transaction
pub const SEMANTIC_MIGRATION_VALIDATE_SQL: &str;   // a statement of its own
pub const CLAIM_SEMANTIC_SEARCH_SQL: &str;
pub const CLAIM_EMBEDDING_WRITE_SQL: &str;
impl PostgresKnowledgeGraphStore {
    pub fn with_semantic_search(self) -> Self;
    pub const fn serves_semantic_search(&self) -> bool;
}
```

What the SQL must be (the exact text is the builder's):
- `SEMANTIC_MIGRATION_SQL`: `CREATE EXTENSION IF NOT EXISTS vector;` then one guarded `DO` block, each part skipped when already present (found by name, as `MIGRATION_SQL` does): one `ALTER TABLE` adding `embedding vector`, `embedding_model TEXT`, `embedding_dimensions INTEGER`, `embedding_version BIGINT`, all nullable with no default; the check `EMBEDDING_CHECK_NAME`, `CHECK (embedding IS NULL OR (embedding_model IS NOT NULL AND embedding_version IS NOT NULL AND embedding_dimensions IS NOT NULL AND vector_dims(embedding) = embedding_dimensions)) NOT VALID`; a trigger function and the trigger `EMBEDDING_TRIGGER_NAME`, `BEFORE INSERT OR UPDATE ... FOR EACH ROW WHEN (NEW.tombstoned AND NEW.embedding IS NOT NULL)`, which sets the four columns to `NULL`. No index.
- `SEMANTIC_MIGRATION_VALIDATE_SQL`: validates the check when it is not yet valid.
- `migrate`: unchanged for a store without the flag. With it, after the base steps succeed and before the unlock, the two semantic batches run, the validation in a statement of its own. The unlock still always runs.
- `CLAIM_SEMANTIC_SEARCH_SQL`: selects `claim_id, record, tombstoned` and `(embedding <=> $N::text::vector)::float8 AS distance`; `WHERE` tenant, space, `NOT tombstoned`, `embedding IS NOT NULL`, the three reference columns equal to the query's, `trust = ANY`, `search_classification = ANY`, `search_confidence_bps >=`, `search_read_capabilities <@`; `ORDER BY distance, claim_id LIMIT`. Every row is decoded; a row whose record the search does not admit is refused with `drift_error`; relevance is `claim_semantic_distance_relevance_bps(distance)`; a relevance of 0 is dropped, not refused; the hits are re-ranked with `rank_claim_search_hits`.
- The `unindexed` probe: an `EXISTS` over the live rows of the scope that either have `search_tokens IS NULL` (their filters cannot be judged) or pass the search's four filters and have no embedding under the query's reference (`IS DISTINCT FROM` on the reference columns).
- `CLAIM_EMBEDDING_WRITE_SQL`: one `UPDATE` setting the four columns `WHERE tenant, space, claim_id AND NOT tombstoned`; the answer is whether it changed a row.
- The page: live rows, `trust <> 'retracted'`, `search_classification = ANY`, embedding null or reference distinct, `claim_id > $cursor`, ascending, limit. A row that cannot be decoded, is not the claim its row names, or whose record's classification or trust contradicts the columns is counted in `undecodable` and not listed (the record decides what may be sent to a model); the cursor moves past it.
- The `version` of a reference is bound as `BIGINT`; a version above `i64::MAX` is refused with `claim-embedding-invalid`.

Traps for the tests:
- The `vector` type and the `<=>` operator live in the schema the extension was created in. Before any private schema is made, create the extension from a session on the default search path, serialised across tests (a transaction holding `pg_advisory_xact_lock` on a test-only key). Private-schema sessions use `SET search_path TO <schema>, public`. Never let `CREATE EXTENSION` run first in a private schema: dropping that schema would drop the extension under every other test.
- Gate every test that needs the extension on `SELECT EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'vector')`. When it is absent: `eprintln!("SKIPPED: ...")` naming the tests not run, and a panic instead when `RAKKA_POSTGRES_PGVECTOR_REQUIRED=1`, as `rakka-agent-postgres/tests/memory_conformance.rs` does.
- The earlier binary is a store built **without** `with_semantic_search()` on a second client in the same schema: this change alters none of its statements (a Global Constraint).

- [ ] **Step 1: Write the failing tests.**

In `postgres_conformance.rs`: `semantic_search_ranks_admitted_claims_when_dsn_is_set` (a `with_semantic_search()` store, `conformance::semantic_search`), `a_store_without_the_flag_refuses_semantic_search_when_dsn_is_set` (`conformance::semantic_search_refused`), and `scenario_20_the_whole_contract_passes_with_semantic_search_when_dsn_is_set` (`check_knowledge_graph_contract` on the semantic store). The first and third are pgvector-gated.

In `postgres_semantic_search_proofs.rs` (spec proofs 18 to 22 and the Review Focus lines):

| Test | Gate | Assert |
|---|---|---|
| `postgres_the_base_migration_never_names_the_extension` | none (no database) | `MIGRATION_SQL` and `MIGRATION_VALIDATE_SQL` do not contain `vector` |
| `postgres_a_store_without_the_flag_adds_nothing_when_dsn_is_set` | DSN | private schema, plain `migrate`: no `embedding%` column, no `EMBEDDING_TRIGGER_NAME`; `capabilities()` lacks `SemanticSearch` |
| `postgres_the_semantic_migration_fails_whole_without_the_extension_when_dsn_is_set` | DSN and extension **absent** (announce a skip otherwise) | `migrate` of a `with_semantic_search()` store is `Err` with `claim-backend-failed`; no `embedding%` column; a plain store's `migrate` on the same schema then succeeds |
| `postgres_the_semantic_migration_rewrites_nothing_when_dsn_is_set` | pgvector | rows appended under a plain migration; `pg_relation_filenode` of the claim table is the same after the semantic `migrate`; every row reads back equal; a second semantic `migrate` leaves the four columns, the valid check and the one trigger as they were |
| `postgres_an_earlier_build_still_works_on_the_migrated_table_when_dsn_is_set` | pgvector | a plain store on a second client appends, transitions, searches by word, tombstones and deletes on the semantic-migrated table |
| `postgres_an_earlier_tombstone_clears_the_embedding_when_dsn_is_set` | pgvector | embed a claim; the plain store's `tombstone`: the four columns are `NULL`. A second claim: a raw `UPDATE ... SET tombstoned = TRUE`: `NULL`. A raw `UPDATE` writing an embedding to a tombstoned row leaves them `NULL` |
| `postgres_a_tombstone_beats_a_late_embedding_when_dsn_is_set` | pgvector | `CLAIM_EMBEDDING_WRITE_SQL`, run raw against a row tombstoned after it was read, changes 0 rows; `store_claim_embedding` answers `false` |
| `postgres_a_drifted_filter_column_cannot_leak_a_claim_when_dsn_is_set` | pgvector | a `Sensitive` embedded claim whose `search_classification` is doctored to `unclassified`: a search admitting `Unclassified` is `Err` (`claim-backend-failed`) and never answers it; `unembedded_claims` with `{Unclassified}` (after clearing its embedding) does not list it and counts 1 `undecodable` |
| `postgres_the_database_refuses_a_vector_of_the_wrong_length_when_dsn_is_set` | pgvector | a raw `UPDATE` with a 2-value vector and `embedding_dimensions = 3` fails with a check violation |
| `postgres_an_unindexed_claim_is_not_ranked_and_flags_the_search_when_dsn_is_set` | pgvector | a live row whose four search columns were set to `NULL` raw: not a hit, `unindexed == true`, not listed by `unembedded_claims`; after `index_claims` it is listed |
| `postgres_two_embedding_passes_at_once_embed_every_claim_when_dsn_is_set` | pgvector | 40 claims; two `embed_claims` walks joined, and `store_claim_embedding` for the same claims beside them: both walks `Ok`, every row has exactly the embedder's reference, a third walk embeds 0 |
| `postgres_a_claim_is_found_by_meaning_when_dsn_is_set` | pgvector | `ConceptEmbedder`; executor `with_embedder` over the PostgreSQL store appends "the car is at the depot"; the retriever `with_embedder` ranks it first for "automobile" |

- [ ] **Step 2: Start the throwaway pgvector database** (Global Constraints) and run to verify failure. Run: `RAKKA_POSTGRES_TEST_DSN=postgres://postgres:postgres@localhost:5433/postgres RAKKA_POSTGRES_PGVECTOR_REQUIRED=1 cargo test -p rakka-agent-knowledge-graph-postgres` Expected: compile failure (`with_semantic_search`).
- [ ] **Step 3: Implement** the flag, the migration, the three operations.
- [ ] **Step 4: Run with pgvector.** Same command as Step 2. Expected: PASS, 0 failed, no line beginning `SKIPPED` except the extension-absent test's.
- [ ] **Step 5: Run without pgvector.** Run: `RAKKA_POSTGRES_TEST_DSN=postgres://postgres:postgres@localhost:5432/postgres cargo test -p rakka-agent-knowledge-graph-postgres -- --nocapture`, output to a file in the workspace. Expected: PASS, 0 failed; every pgvector test prints a line beginning `SKIPPED`; `postgres_the_semantic_migration_fails_whole_without_the_extension_when_dsn_is_set` runs and passes.
- [ ] **Step 6: Run without a DSN.** Run: `cargo test -p rakka-agent-knowledge-graph-postgres` Expected: PASS.
- [ ] **Step 7: Falsify** against 5433, each seen to fail its named test: remove the trigger from the migration (`..._an_earlier_tombstone_clears_the_embedding_...`); drop `AND NOT tombstoned` from `CLAIM_EMBEDDING_WRITE_SQL` with the trigger also removed (`..._a_tombstone_beats_a_late_embedding_...`); drop the record re-check in the search (`..._a_drifted_filter_column_...`); move the classification filter out of the search's `WHERE` (the conformance clause). Restore. Drop any schema a failed run left behind.
- [ ] **Step 8: Commit.** `git commit -m "Serve semantic-search from PostgreSQL with pgvector, opt-in (issue #140)"`

### Task 7: End to end over sharded entities

**Files:**
- Modify: `examples/multi-agent-goal-acceptance/src/wiring.rs`
- Create: `examples/multi-agent-goal-acceptance/tests/communal_semantic_read_path.rs`
- Modify: `examples/multi-agent-goal-acceptance/README.md` (one paragraph naming the new test)

**Interfaces:**
- Consumes: Tasks 3 to 5; the helpers of `tests/communal_read_path.rs` (`snapshot_of_one_turn`'s steps, `reviewer`, `ask_retrying`, `pump`, `instantiate`) and of `flow.rs` (`claim_append_operation_id`, the `AgentRunEntityCommand::AppendClaim` shape at `flow.rs:754`).
- Produces: `World::with_semantic_read_path() -> World`: the communal read path as `with_communal_read_path` wires it, with the retriever built `.with_embedder(embedder.clone())` and `World::pipeline`'s claim-append executor built `.with_embedder(embedder, {Unclassified})`. `World` gains `pub embedder: Option<ConceptEmbedder>` (`None` in the other two worlds, whose behaviour and transcript must not change). The concepts: `&[&["car", "automobile"]]`.

- [ ] **Step 1: Write the failing test** `a_run_reads_a_claim_another_agent_wrote_in_other_words`:
  1. `World::with_semantic_read_path()`.
  2. A writer agent runs one task to its end (as `snapshot_of_one_turn` does). Inside the post-terminal window it is sent `AppendClaim` (space `shared`, subject `fleet-note`, predicate `reports`, value `"the car is at the depot"`, `Unclassified`), and its task and run are pumped again so the dispatcher runs the effect.
  3. The graph holds the claim; `unembedded_claims` under the embedder's reference does not list it (the append embedded it; no pass ran).
  4. The claim is promoted to `Verified` directly on the store, as `verified_claim` does.
  5. A reader agent, granted the space, runs one task whose input is `"where is the automobile"`. Its turn-1 snapshot has exactly that claim in `communal_claims`, `communal.retriever_version` is 2, and `communal.index_incomplete` is false.
  6. A second world built `with_communal_read_path()` (word search), driven the same way, selects no claim for the same input.
- [ ] **Step 2: Run to verify it fails.** Run: `cargo test -p rakka-example-multi-agent-goal-acceptance --test communal_semantic_read_path` Expected: compile failure (`with_semantic_read_path`).
- [ ] **Step 3: Implement the wiring.**
- [ ] **Step 4: Run to verify.** Run: `cargo test -p rakka-example-multi-agent-goal-acceptance` and `cargo run -p rakka-example-multi-agent-goal-acceptance`. Expected: all tests pass; the example's stdout still matches its README.
- [ ] **Step 5: Commit.** `git commit -m "Prove a claim is read by meaning across sharded agents (issue #140)"`

### Task 8: Documents and the full validation

**Files:**
- Modify: `docs/rakka-agents.md` (the communal read path: ranking by meaning, how to opt in, what is sent to the embedder — claims of the stated classifications, and up to 1,024 bytes of recent session text per searching turn — the exact scan and its cost, no relevance floor, and the deployment-applied approximate index; remove "There is no meaning-based search yet")
- Modify: `docs/rakka-compatibility.md` (an entry for #140 with the six-step roll order of spec section 7, what an earlier binary does, rollback, the fixed column, check and trigger names, and that the session's search path must reach the schema holding the `vector` extension)
- Modify: `docs/rakka-agent-security-validation-matrix.md` (remove the "Owed" item for meaning-based search; add rows: erasure reaches the embedding, filters before ranking, the record decides what is sent to a model; note the PostgreSQL run now requires pgvector in CI)
- Modify: `docs/rakka-v1-known-limitations-roadmap.md` (exact scan; no relevance floor; the embedding model is not recorded in a snapshot; no metric for embeddings that failed at append; no time bound on the embedder call; a claim outside the stated classifications is not found by meaning; the private tier's `reindex` still stops on a model error)
- Modify: `docs/rakka-api-boundary-inventory.md`, `CHANGELOG.md`
- Modify: the crate docs in `crates/rakka-agent-knowledge-graph/src/lib.rs` and `crates/rakka-agent-knowledge-graph-postgres/src/lib.rs`
- Modify: `docs/superpowers/specs/2026-11-16-communal-semantic-search-design.md` (a last section "As built", listing every ledgered ruling that moved the build from the design)
- Modify: `CLAUDE.md` (the gated-tests block: the pgvector line for `rakka-agent-knowledge-graph-postgres`)

- [ ] **Step 1: Find the tests that hold the documents to the code.** Run: `grep -rln "rakka-agents.md\|rakka-compatibility.md\|security-validation-matrix" crates/*/tests examples/*/tests | sort` and read what each asserts before editing.
- [ ] **Step 2: Write the documents.**
- [ ] **Step 3: Run the full validation.** `cargo clean`, then `scripts/validate.sh > "$SCRATCH/validate.log" 2>&1; echo $? > "$SCRATCH/validate.code"` and `scripts/package-check.sh > "$SCRATCH/package.log" 2>&1; echo $? > "$SCRATCH/package.code"`. Expected: both files hold `0`. If the one-shot workspace test is killed under load, run the workspace tests per crate and record each result.
- [ ] **Step 4: Run the gated PostgreSQL suites on the final tree**, both commands of Task 6 Steps 4 and 5, plus `RAKKA_POSTGRES_TEST_DSN=...5433... RAKKA_POSTGRES_PGVECTOR_REQUIRED=1 cargo test -p rakka-agent-postgres`. Expected: 0 failed. Then `docker rm -f rakka-140-pgvector`.
- [ ] **Step 5: Commit.** `git commit -m "Document communal claims ranked by meaning (issue #140)"`
