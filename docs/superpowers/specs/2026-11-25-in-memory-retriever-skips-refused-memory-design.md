# The in-memory retriever skips a memory its embedder refuses (issue #201)

Short design. Branch `in-memory-retriever-skips-refused`, off `main` at
`9b2cf885`.

## 1. Why

`InMemoryPrivateMemoryRetriever` with an embedder embeds every admitted
memory at each retrieval and returns the first error
(`crates/rakka-agent/src/retrieval.rs`, the `embed_memory_vector(..).await?`
inside the loop). One memory the embedder refuses therefore fails every
retrieval for that agent, whatever the query. `select_private_memories`
turns the error into a degraded, empty selection, so every turn of that
agent gets no private memory for as long as the refused memory is admitted.

The pgvector backend does not behave this way. It embeds only the query at
retrieval; a memory its embedder refuses is counted and paged past by
`reindex` (issue #151) and is simply never ranked. The reference retriever
and the production backend give different answers for the same memories.

## 2. Goals, non-goals, anti-goals

- **Goal:** a memory the embedder refuses is left out of the ranking, and
  every other admitted memory is ranked as before.
- **Goal:** the skip leaves a trace: one content-free warning per retrieval
  that skipped something.
- **Non-goal:** a public report of skipped memories (a field on
  `MemoryRetrievalOutcome` or `RetrievalReport`, a metric). That is option C
  below, not chosen.
- **Non-goal:** any change to the pgvector backend.
- **Anti-goal:** the embedder's message, the memory's text, or its id in the
  warning. A model's error can quote the text it was given; that is why
  `reindex` drops it.
- **Anti-goal:** a failure to embed the *query* turning into an empty `Ok`.
  It stays an error, so an embedder that is down from the start still
  degrades the retrieval visibly.

## 3. Approaches

- **A. Skip silently, documented.** Matches the pgvector read path exactly
  (its retrieval never says what is unindexed). No public change. But the
  in-memory retriever has no reindex pass, so nothing anywhere would ever
  count the refused memory.
- **B. Skip, and log one warning per retrieval with the count.** No public
  change. Leaves a trace in the one setup that has no reindex to count the
  refusals. **Chosen by the owner.**
- **C. Skip, and carry an `index_incomplete` flag on the outcome and the
  report, emitted as an `index-incomplete` metric like the communal tier.**
  Adds a field to a struct with all-public fields, which breaks every
  literal (eleven in this repository, one in the host's
  `memory_conformance_postgres.rs`), and is honest only if pgvector computes
  the same flag. A bigger piece; not this issue.

## 4. The design

**The skip.** In the embedder arm of `retrieve`, a memory whose
`embed_memory_vector` call returns any error is left out of the ranking and
counted. That covers both an error from the embedder and the shared check's
`memory-embedding-invalid` refusal (wrong length, a non-finite component) —
the same set `reindex` counts into `failed`. The loop goes on to the next
memory.

**The query.** Unchanged: `embed_memory_vector` for the query text is still
`?`, so a refused or misdescribed query vector fails the retrieval, and the
existing `LyingEmbedder` unit test keeps passing as written.

**The warning.** After the loop, when at least one memory was skipped, one
`tracing::warn!` carrying:

- the scope's `tenant` and `agent` ids,
- `skipped`, the count,
- `code`, the stable code of the first skipped memory's error
  (`MemoryError::code()`), so an operator can tell a misdescribed vector
  from an embedder error,
- `embedder`, the embedder's declared model name,

and a fixed message. Never the error's message, a memory's text, or a
memory id. Following the `tracing::warn!` field style in `dispatch.rs`.

**The error message question from the issue.** Left as it is. After the
skip, the only embedder error the in-memory retriever returns is the
query's, which it returns word for word — exactly as the pgvector
retriever does (`self.embed_checked(query.text()).await?`). The only caller
in the crate, `select_private_memories`, discards it (`Err(_)`). Dropping
it in both backends would be a separate change to the production backend.

**A known edge, documented, not changed.** An embedder that accepts the
query and then fails for every memory (an outage that begins mid-scan)
answers `Ok` with fewer memories, or none, and is not counted as degraded.
It makes one failing call per remaining admitted memory, bounded by
`AGENT_MEMORY_RETRIEVAL_SCAN_MAX_ENTRIES`. This is the same choice `reindex`
made in #151; the warning is where it shows.

**Docs.** The `InMemoryPrivateMemoryRetriever` doc (which today says it
"returns every admitted candidate ranked") says a memory its embedder
refuses is left out, logged by count, and why. A `CHANGELOG.md` entry under
Unreleased. No product doc in `docs/` describes this retriever's embedder
path (checked), so none changes. The #151 design's §8 bullet is history and
stays as written.

**Tests** (in the world of `crates/rakka-agent/tests/private_memory_retrieval.rs`):

- The issue's table: an embedder wrapping `DeterministicEmbedder` that
  returns `MemoryError::Backend` for one exact text; an agent holding that
  memory and a healthy one answers `Ok` with the healthy memory for both
  queries; a control agent holding only the healthy one answers the same.
- A memory whose vector is misdescribed (wrong length for that one text) is
  skipped the same way.
- An embedder that refuses the query text still fails the retrieval.
- Through the assembly: the poisoned agent's snapshot selects the healthy
  memory and the report is not degraded.
- The warning fires once per retrieval with the count and the code, and
  carries neither the embedder's message nor the memory's text.

## 5. My calls (for the owner to check)

- The warning carries `tenant`, `agent`, `skipped`, `code`, `embedder`. The
  code of the *first* skipped memory only, not a count per code.
- No new conformance clause: a clause takes a retriever already built, so
  it cannot bind a refusing embedder generically; pgvector's side is held
  by its #151 tests.
- The test captures the warning with a small subscriber written against
  `tracing` itself rather than a new dev-dependency, unless that proves
  awkward, in which case the workspace's existing `tracing-subscriber` as a
  dev-dependency of `rakka-agent`.

## 6. Left to the builder

Names, the exact warning text, where the capture helper lives, and the test
file split.
