# The In-Memory Retriever Skips a Refused Memory — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `InMemoryPrivateMemoryRetriever` with an embedder leaves out a memory its embedder refuses, ranks the rest, and logs one content-free warning per retrieval that skipped something (issue #201).

**Architecture:** One change inside the embedder arm of `InMemoryPrivateMemoryRetriever::retrieve` (`crates/rakka-agent/src/retrieval.rs`): a per-memory `embed_memory_vector` error is counted and the loop continues; the query's embedding stays `?`. After the loop, one `tracing::warn!` when anything was skipped. Tests sit in the same file's `#[cfg(test)] mod tests`, beside the existing embedder test and the `memory_with_session` / `assemble_context` fixture.

**Tech Stack:** Rust 1.88, tokio tests, `tracing` 0.1.44 (already a dependency of `rakka-agent`; no new dependency).

**Spec:** `docs/superpowers/specs/2026-11-25-in-memory-retriever-skips-refused-memory-design.md`

## Global Constraints

- The warning never carries the error's message, a memory's text, or a memory id. Fields exactly: `tenant`, `agent`, `skipped`, `code`, `embedder`.
- `code` is the `MemoryError::code()` of the **first** skipped memory (admitted order, which is the store's memory-id order).
- A failure to embed the **query** still fails the retrieval with the embedder's error, unchanged.
- No public type, field, error code, or dependency is added. The pgvector backend is not touched.
- `scripts/validate.sh` exit 0 before the branch is done (redirect to a file, capture `$?`; never pipe it into `tail`).
- `cargo clean` before the first build (task boundary; disk pressure).

## Review Focus

1. **An outage that starts after the query** (embedder accepts the query, refuses every memory): `Ok` with no memories, one warning with `skipped` equal to the admitted count, and the assembly is *not* degraded. Pinned by `an_embedder_refusing_every_memory_answers_empty_and_warns_once`.
2. **The refused memory is the nearest to the query:** the healthy one is still returned, nothing else shifts. Pinned by the table test's first query (the refused text *is* the query).
3. **A retrieval that skipped nothing logs nothing.** Pinned by the control half of `a_skip_warns_once_with_the_count_and_no_content`.
4. **Mixed causes:** a misdescribed vector first, a refused one second → `skipped=2`, `code=memory-embedding-invalid`. Pinned in `a_skip_warns_once_with_the_count_and_no_content`.

---

### Task 1: Skip a memory the embedder refuses, and warn

**Files:**
- Modify: `crates/rakka-agent/src/retrieval.rs` — the `InMemoryPrivateMemoryRetriever` doc (~:707-720), the embedder arm of `retrieve` (~:833-851), and the `mod tests` module (helpers beside `seeded_store` ~:1491; tests after `an_embedder_ranks_by_cosine_and_a_dimension_mismatch_fails_closed` ~:1815)
- Modify: `CHANGELOG.md` — first bullet under `## Unreleased` → `### Fixed` (~:594)

**Interfaces:**
- Consumes: `embed_memory_vector(&dyn AgentMemoryEmbedder, &str) -> Result<Vec<f32>, MemoryError>`, `MemoryError::code(&self) -> &'static str`, `AgentScope::tenant()/agent()` (`.as_str()`), test fixtures `run_scope()`, `seeded_store(&AgentScope, &[(&str, &str)])` (memory id = `mem-<name>`, content = the text), `memory_with_session(Option<AgentMemoryRetrieval>)` (session query "what are the renewal contract terms"), `now()`, `crate::testkit::DeterministicEmbedder`.
- Produces: test-only helpers `RefusingEmbedder` and `CapturedEvents` (below). No public items.

- [ ] **Step 0: Clean the target directory**

Run: `cargo clean`
Expected: `Removed … files`

- [ ] **Step 1: Write the test helpers**

In `mod tests`:

```rust
/// Wraps `DeterministicEmbedder`; for one exact text it either errors with a
/// message that quotes the text, or answers a vector of the wrong length.
struct RefusingEmbedder {
    inner: DeterministicEmbedder,
    refused: Vec<(&'static str, Refusal)>,
}
#[derive(Clone, Copy)]
enum Refusal { Errors, Misdescribes }
impl RefusingEmbedder {
    fn new() -> Self;                                    // no refusals
    fn refusing(self, text: &'static str, how: Refusal) -> Self;
}
// impl AgentMemoryEmbedder: embedding_ref() = inner's; embed(text):
//   Errors       → Err(MemoryError::Backend { backend: "test-embedder".into(),
//                    message: format!("provider rejected input: {text}") })
//   Misdescribes → Ok(vec![1.0; 3])   (DeterministicEmbedder declares 8)
//   otherwise    → inner.embed(text)
```

```rust
/// A `tracing::Subscriber` that records each event's fields as
/// (name, rendered value) pairs, the message included as `message`.
#[derive(Clone, Default)]
struct CapturedEvents(Arc<Mutex<Vec<Vec<(String, String)>>>>);
impl CapturedEvents { fn events(&self) -> Vec<Vec<(String, String)>>; }
// impl tracing::Subscriber: enabled → true; new_span → Id::from_u64(1);
// record / record_follows_from / enter / exit → no-ops; event → visit fields
// with a `tracing::field::Visit` implementing record_debug (`{value:?}`) and
// record_str (the str as is), push the pairs.
```

Run a future under it with `tracing::instrument::WithSubscriber::with_subscriber(future, captured.clone())`.

- [ ] **Step 2: Write the failing tests**

```rust
#[tokio::test]
async fn a_memory_the_embedder_refuses_is_left_out_and_the_rest_rank() {
    // issue #201's table
    let scope = run_scope().agent_scope();
    let store = seeded_store(&scope, &[("refused", "renewal contract terms"),
                                       ("healthy", "renewal contract signed in march")]).await;
    let embedder = Arc::new(RefusingEmbedder::new()
        .refusing("renewal contract terms", Refusal::Errors));
    let retriever = InMemoryPrivateMemoryRetriever::new(store).with_embedder(embedder);
    for query in ["renewal contract terms", "something unrelated entirely"] {
        let outcome = retriever.retrieve(&scope, &MemoryRetrievalQuery::new(query), now())
            .await.expect("one refused memory no longer fails the retrieval");
        let ids: Vec<_> = outcome.memories.iter().map(|m| m.memory.memory_id.as_str()).collect();
        assert_eq!(ids, ["mem-healthy"], "query {query:?}");
    }
    // control: an agent holding only the healthy memory answers the same
}

#[tokio::test]
async fn a_misdescribed_vector_for_one_memory_is_left_out_the_same_way() {
    // as above with Refusal::Misdescribes; expect ids == ["mem-healthy"]
}

#[tokio::test]
async fn a_query_the_embedder_refuses_still_fails_the_retrieval() {
    // refuse the query text "renewal" (Errors); seed one healthy memory
    // expect_err; assert_eq!(error.code(), "memory-backend-failed");
}

#[tokio::test]
async fn the_assembly_selects_the_healthy_memory_and_is_not_degraded() {
    // seeded_store with the two memories above, refusing "renewal contract terms";
    // AgentMemoryRetrieval::new(Arc::new(retriever), store, AgentGuardrailChain::new(INITIAL));
    // memory_with_session(Some(retrieval)); assemble_context(.., turn 1, ..)
    assert!(!assembled.retrieval.degraded);
    assert_eq!(assembled.snapshot.private_memory.len(), 1);
    assert_eq!(assembled.snapshot.private_memory[0].memory_id.as_str(), "mem-healthy");
}

#[tokio::test]
async fn an_embedder_refusing_every_memory_answers_empty_and_warns_once() {
    // two memories, both refused (Errors); query "renewal" accepted
    // outcome.memories.is_empty(); captured.events().len() == 1 with skipped == "2";
    // through the assembly: !degraded, selected == 0
}

#[tokio::test]
async fn a_skip_warns_once_with_the_count_and_no_content() {
    // seed ("a-bad", "alpha renewal note") Misdescribes,
    //      ("b-refused", "beta renewal note") Errors, ("c-healthy", "gamma renewal note")
    // retrieve "renewal" under CapturedEvents
    let events = captured.events();
    assert_eq!(events.len(), 1);
    let fields: BTreeMap<_, _> = events[0].iter().cloned().collect();
    assert_eq!(fields["skipped"], "2");
    assert_eq!(fields["code"], "memory-embedding-invalid");
    assert_eq!(fields["embedder"], DeterministicEmbedder::MODEL);
    assert_eq!(fields["tenant"], scope.tenant().as_str());
    assert_eq!(fields["agent"], scope.agent().as_str());
    let rendered = format!("{events:?}");
    for leaked in ["provider rejected", "alpha renewal note", "beta renewal note", "mem-a-bad", "mem-b-refused"] {
        assert!(!rendered.contains(leaked), "the warning carries {leaked:?}");
    }
    // control: a second retriever with no refusals over the same store, same
    // query, under a fresh CapturedEvents → events().is_empty()
}
```

- [ ] **Step 3: Run the tests to see them fail**

Run: `cargo test -p rakka-agent --lib -- retrieval::tests:: 2>&1 | tail -30`
Expected: the first two and the assembly test fail with `memory-backend-failed` / `memory-embedding-invalid` from `expect`; the two warning tests fail (error, or zero events). `a_query_the_embedder_refuses_still_fails_the_retrieval` and every existing test pass.

- [ ] **Step 4: Implement the skip and the warning in `retrieve`**

In the `Some(embedder)` arm: keep `let query_vector = embed_memory_vector(..query.text()).await?;`. For each memory, `match embed_memory_vector(embedder.as_ref(), &text).await` — on `Err(error)`, `skipped += 1`, record `error.code()` if no code is held yet, drop the error, `continue`. After the loop, if `skipped > 0`:

```rust
tracing::warn!(
    tenant = scope.tenant().as_str(),
    agent = scope.agent().as_str(),
    skipped,
    code = first_code,
    embedder = reference.model.as_str(),
    "the in-memory retriever left out memories its embedder refused; \
     they are not ranked while the embedder refuses them"
);
```

A memory with no embedding text (`memory_embedding_text` → `None`) is still skipped by the existing `continue` and is **not** counted.

- [ ] **Step 5: Update the `InMemoryPrivateMemoryRetriever` doc**

Replace "returns every admitted candidate ranked, the way a vector index would" with: returns every admitted candidate it can embed, ranked, the way a vector index would. Add a paragraph: a memory its embedder refuses (an error, or a vector the wrong length or with a non-finite component) is left out of the ranking, as the pgvector backend in effect leaves out a memory its `reindex` could not embed; one refused memory used to fail every retrieval for that agent (issue #201). Each retrieval that left any out logs one warning with the count, the first error's code, and the embedder's model, never the error's message, which can quote the text. A query the embedder refuses still fails the retrieval. An embedder that accepts the query and then fails for every memory answers no memories, not an error, after one call per admitted memory (bounded by `AGENT_MEMORY_RETRIEVAL_SCAN_MAX_ENTRIES`).

- [ ] **Step 6: Add the CHANGELOG entry** as the first bullet under `### Fixed`:

```markdown
- `rakka-agent`: **the in-memory reference retriever ranks past a memory its embedder refuses** (issue #201). `InMemoryPrivateMemoryRetriever` with an embedder embeds each admitted memory at retrieval and returned the first error, so one memory its embedder refused failed every retrieval for that agent, whatever the query, and every turn of its runs got no private memory. Such a memory (an error from the embedder, or a vector of the wrong length or with a component that is not finite) is now left out of the ranking, as the pgvector backend in effect leaves it out, and the retrieval logs one warning with the count, the first error's code and the embedder's model; never the error's message, which can quote the text. A query the embedder refuses still fails the retrieval. No type, field or code is added. The record is `docs/superpowers/specs/2026-11-25-in-memory-retriever-skips-refused-memory-design.md`.
```

- [ ] **Step 7: Run the tests to see them pass**

Run: `cargo test -p rakka-agent --lib -- retrieval::tests:: 2>&1 | tail -30`
Expected: every `retrieval::tests::` test passes, the six new ones included.

- [ ] **Step 8: Falsify the warning test**

Temporarily add `message = %error` (the dropped error) to the `warn!`; run `a_skip_warns_once_with_the_count_and_no_content`; expected FAIL naming `"provider rejected"`. Revert, rerun, expected PASS.

- [ ] **Step 9: Lint and doc-check the crate**

Run: `cargo fmt --all -- --check && cargo clippy -p rakka-agent --all-targets --all-features -- -D warnings && cargo doc -p rakka-agent --no-deps 2>&1 | grep -n "retrieval.rs"`
Expected: fmt and clippy clean; the `grep` prints nothing (no doc warning in the changed file).

- [ ] **Step 10: Commit**

```bash
git add crates/rakka-agent/src/retrieval.rs CHANGELOG.md
git commit -m "The in-memory retriever ranks past a memory its embedder refuses (issue #201)"
```

### Task 2: Validate the branch

- [ ] **Step 1: Run the canonical validation**

Run: `scripts/validate.sh > "$SCRATCH/validate.log" 2>&1; echo "exit $?"`
Expected: `exit 0`. If the one-shot workspace test is killed under load, rerun the failing crate's tests alone and record both.

- [ ] **Step 2: Record the counts** (passed totals from the log, not the last `test result` line) in the PR body.
