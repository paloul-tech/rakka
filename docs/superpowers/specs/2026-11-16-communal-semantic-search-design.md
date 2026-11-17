# Communal claims ranked by meaning

Issue #140. The follow-up the owner asked for on 2026-11-14 when choosing word-match search for issue #69.

Branch `communal-semantic-search`, off `main` at `84c5b17`.

## 1. Intent

A run can read communal claims since issue #69, and it finds them by shared words. A claim that says the same thing in other words is not found, and text with no word boundaries (Chinese, Japanese) or with decomposed accents matches badly. The owner accepted those limits on 2026-11-15 with this issue as the plan for recall.

After this change a deployment can rank a knowledge space's claims by meaning: each claim has a stored embedding, a search compares the query's embedding with them, and everything that already guards a communal read still applies. A deployment that does not opt in sees no change.

## 2. What exists today

- `KnowledgeGraphCapability::SemanticSearch` is declared in `crates/rakka-agent-knowledge-graph/src/store.rs`. No operation stands behind it and no backend reports it.
- `KnowledgeGraphStore::search` ranks by shared words (`ClaimSearch`, `ClaimSearchReport { hits, unindexed }`). It is optional, behind `keyword-search`.
- In PostgreSQL the word index is four nullable columns on the claim row (`search_tokens`, `search_classification`, `search_confidence_bps`, `search_read_capabilities`), filled inside the append. A trigger clears a tombstoned row's words. `index_claims` fills rows an earlier build appended.
- `KnowledgeGraphCommunalRetriever` (`communal_retrieval.rs`) serves `rakka-agent`'s port `AgentCommunalClaimRetriever` from any store with `keyword-search`. It reports retriever version 1.
- `KnowledgeGraphClaimAppendExecutor` (`append_executor.rs`) appends a run's claim as an idempotent, retried effect.
- `Claim.embedding: Option<MemoryEmbeddingRef>` is a field of the authoritative record. A caller may set it at append (`with_embedding`); nothing reads it, and a tombstone clears it. No vector is stored anywhere.
- A claim's classification is one of `Unclassified`, `Sensitive` and `Redacted`. A read policy admits `Unclassified` alone unless a deployment widens it.
- Context assembly in `rakka-agent` takes whatever ranking the retriever answers, resolves every ranked identity through the authority, re-checks it, evaluates it at memory ingress, and embeds it in the snapshot. It records the retriever's name and version and `index_incomplete`. **Assembly does not change.**
- The private tier already ranks by meaning: `AgentMemoryEmbedder` (a model identity `MemoryEmbeddingRef { model, dimensions, version }` and `embed(text)`), `embed_memory_vector` (the shared length and finiteness check), and `PgvectorPrivateMemoryRetriever` in `rakka-agent-postgres`, which ships an exact scan and maps cosine distance `d` to relevance `(1 - d/2) * 10000` basis points.
- The CI database image is `pgvector/pgvector:pg16`. A local database may have no `vector` extension.

## 3. Goals, non-goals, anti-goals

**Goals**

- One stored embedding per claim, scoped by tenant and knowledge space, with the model, dimension count and pipeline version beside it.
- A search operation behind `semantic-search`, on the portable store SPI, implemented by the in-memory store and by PostgreSQL with pgvector.
- A fresh claim is embedded at append, best effort. A pass a deployment calls embeds what is left. No loop in Rakka calls the pass. *(Owner's decision, 2026-11-16.)*
- A deployment states which classifications of claim may be sent to the embedder. There is no default. *(Owner's decision, 2026-11-16.)*
- The communal retriever ranks by meaning when it is given an embedder and its store reports the capability, and by words otherwise.
- Conformance clauses any backend runs.
- A deployment that does not opt in keeps its schema, its bytes and its behaviour. The base graph schema keeps working on a database with no `vector` extension.

**Non-goals**

- An approximate index (HNSW, IVFFlat). The shipped search is an exact scan. A deployment may add an approximate index as its own DDL. *(Owner's decision, 2026-11-16: approach 1.)*
- A relevance floor. A meaning-based search answers its nearest admitted claims, however far.
- Recording the embedding model in a context snapshot. The snapshot records the retriever's name and version, as today.
- Mixing word-match and meaning-based rankings in one search.
- A metric for embeddings that failed at append.
- A time bound on the embedder call. The embedder is the deployment's.
- Embedding artifact-backed object content. This crate never loads artifact bytes.
- Fixing the same wedge in the private tier's `reindex`, which stops on a model error.
- Keeping `Claim.embedding` in step with the stored embedding (6.2).
- Any change to context assembly, to the snapshot's shape, or to the `AgentCommunalClaimRetriever` port.

**Anti-goals**

- An embedding that outlives its claim's content: one that survives a tombstone or a delete, or that any binary, earlier or current, can write back afterwards.
- A filter applied after ranking, so that a claim the reader may not see takes a result slot or shapes the order.
- A ranked identity entering a snapshot without being resolved through the authoritative store.
- A store that calls a model.
- An embedding, or any vector, in a snapshot, a log, a metric or an error message.
- A failed embedding failing a claim append.
- A base migration that needs the `vector` extension.

## 4. Constraints

- **From the issue:** erasure reaches the embedding in the same operation; restrictions apply before ranking (specification 16); tenant and knowledge-space filters survive the index even at a cost (specification 13.6); ranking is not authority.
- **From the repository:** every public item documented; `clippy -D warnings`; error codes are a compatibility surface; `unsafe` forbidden; user-facing changes go in `CHANGELOG.md` and the product documents.
- **Hard rule:** no resolved credential or secret material is persisted. An embedder's credential is the deployment's and never reaches this crate.
- **Rolling updates:** an earlier binary keeps serving against a migrated table. It must not be able to break the rules above.
- **The migration-lock trap (issue #69):** a concurrent index build must not run while a session waits inside `pg_advisory_lock`. This design ships no new index, so it adds no build.

## 5. Approaches

**The overall shape**

| Approach | What it is | Why chosen or not |
|---|---|---|
| **1. The store stays a pure store; three callers hold the embedder** (chosen by the owner) | Optional SPI operations take and return vectors. The retriever, the append executor and the indexing pass call the model. | The declared capability is served, any backend can implement it, and conformance clauses have something portable to run against. |
| 2. Copy the private tier | A pgvector retriever in the PostgreSQL crate with its own table and its own embedder, implementing the retriever port directly | Less new surface. The capability stays unserved and nothing portable exists to write clauses against; the issue asks for both. |
| 3. Approach 1 with a shipped approximate index | HNSW over a fixed-dimension column | pgvector applies filters after the index picks candidates, so a selective filter silently loses results. That breaks "restrictions apply before ranking", and fixes one dimension in the schema. |

**A claim with no embedding yet**

| Approach | Trade-off |
|---|---|
| **Embed at append, best effort, with the pass as repair** (chosen by the owner) | A fresh claim is usually findable at once. The append effect now includes a model call. |
| The pass only | Simplest. A fresh claim is invisible to a meaning-based search until the deployment's next pass. |
| Fill the gap with word-match in the same search | Nothing is invisible. One ranking mixes two scales. |

**Where the embedding lives in PostgreSQL** *(my call)*

| Approach | Trade-off |
|---|---|
| **Nullable columns on the claim row, added by an opt-in migration** (chosen) | One statement filters and ranks with no join. The filter columns of the word index are already on the row. A trigger clears the embedding on tombstone, and a delete removes the row. An earlier binary names none of the columns. |
| A separate derived table, as the private tier has | The claim table is untouched even after opting in. Every search joins, and erasure needs two hooks. |

## 6. Design

### 6.1 The portable types (`rakka-agent-knowledge-graph`, a new module `semantic`)

- **`claim_embedding_text(claim) -> Option<String>`**: the text a claim is embedded from. It is the subject, the predicate and the object's text, joined by single spaces, in that order. A node object contributes its node id. An inline value contributes `memory_embedding_text`. An artifact-backed object contributes nothing, so such a claim is embedded from its subject and predicate, as the word index does. A tombstoned claim answers `None`. The query side embeds the query text as it is.
- **`ClaimEmbedding { reference: MemoryEmbeddingRef, vector: Vec<f32> }`**, with a `validate` that refuses a vector whose length is not `reference.dimensions`, a non-finite component, zero dimensions, or more than `CLAIM_EMBEDDING_MAX_DIMENSIONS` (16000, pgvector's own ceiling). It reuses the existing `claim-embedding-invalid` code.
- **`ClaimSemanticSearch`**: a `ClaimSearch` (its trust set, classifications, confidence floor, reader capabilities, limit and `admits`) together with the query's `ClaimEmbedding`.
- **`claim_semantic_relevance_bps(query, candidate) -> u16`**: cosine similarity mapped as the private tier maps it, `((1 + cos) / 2) * 10000` rounded, and 0 when either vector has zero magnitude. One function, so backends score alike.
- **The answer is the existing `ClaimSearchReport { hits, unindexed }`.** Hits are ordered by relevance, highest first, then by claim id. A candidate that scores 0 is not a hit: its vector, or the query's, has no direction, or the two point exactly opposite ways. That keeps the type's rule that a hit's relevance is never zero. *(My call.)*

### 6.2 The store SPI (`KnowledgeGraphStore`)

Three optional operations. A backend that reports `semantic-search` implements all three. Each default body answers `claim-capability-unsupported` naming `semantic-search`.

| Operation | What it does |
|---|---|
| `search_semantic(scope, &ClaimSemanticSearch) -> ClaimSearchReport` | Admits, then ranks. A candidate is a live claim of the scope that the search admits and whose stored embedding has the query's reference. `unindexed` is true when the scope holds a live claim the search admits, or cannot judge, that has no embedding under that reference. |
| `store_claim_embedding(scope, &ClaimId, &ClaimEmbedding) -> bool` | Stores or replaces the claim's one embedding. Answers `false` and stores nothing when the scope holds no such claim or the claim is tombstoned. It refuses an invalid embedding. It is idempotent. |
| `unembedded_claims(scope, &MemoryEmbeddingRef, classifications, after: Option<&ClaimId>, limit) -> ClaimEmbeddingPage` | One bounded page, in claim-id order, of the claims that should have an embedding under the reference and have none: none at all, or one from another model, dimension count or version. A claim is listed only when it is live, its trust is not `Retracted` (no search ever admits a retracted claim), and its classification is in the set given. `ClaimEmbeddingPage { claims, undecodable, next }`. `undecodable` counts records this build cannot decode; the cursor moves past them. The page is clamped to `CLAIM_EMBEDDING_PAGE_MAX_CLAIMS` (64). |

Rules every backend keeps:

- **Admission precedes ranking.** A claim the search does not admit is never a candidate.
- **One embedding per claim.** Storing under a new reference replaces the old one. A search under one reference never ranks a vector stored under another.
- **Erasure reaches the embedding.** A tombstone and a delete remove it in the same operation.
- **The store calls no model** and never returns a vector.
- **The record is not rewritten.** Storing an embedding changes no byte of the claim's authoritative record, so `Claim.embedding` is not kept in step: it stays whatever the appender set, which is usually nothing. Keeping it in step would make the embedding a second writer of the record, racing a trust transition, and would break the rule that a replayed append answers the claim as first stored. The field's documentation is reworded to say it is the appender's own note and not the state of any index. *(My call.)*

`InMemoryKnowledgeGraphStore` implements the three and reports the capability.

### 6.3 PostgreSQL (`rakka-agent-knowledge-graph-postgres`)

**Opting in.** `PostgresKnowledgeGraphStore::with_semantic_search(self) -> Self`. A store built that way reports `semantic-search`, and its `migrate` also applies the semantic migration. A store built without it refuses the three operations and its `migrate` is exactly today's. `capabilities()` reads no database, so the flag is the only source of the capability.

**The semantic migration**, applied after the base one, under the same migration lock:

- `CREATE EXTENSION IF NOT EXISTS vector`. Where the extension is unavailable, `migrate` fails with the server's message and nothing else is changed.
- Four nullable columns on `rakka_agent_knowledge_claim`: `embedding vector` (no fixed dimension), `embedding_model TEXT`, `embedding_dimensions INTEGER`, `embedding_version BIGINT`. Adding a nullable column with no default rewrites nothing.
- A check that a stored vector has the dimension count beside it, added without a long exclusive lock (not valid first, validated in a statement of its own, as issue #69 did).
- A row trigger that sets the four columns to null on any inserted or updated row whose `tombstoned` is true and whose `embedding` is not null. The rule therefore holds for an earlier binary's tombstone statement, which names none of them.
- **No index.** The claim table's primary key leads with tenant and space, which bounds every statement below to one knowledge space.

**Search.** One statement: the scope, `NOT tombstoned`, the embedding's three reference columns, then trust (a column every row has), then classification, confidence and read capabilities from the three columns the word index keeps, and only then `ORDER BY` cosine distance, claim id, with the limit. Every returned row is decoded and checked against its own record (`admits`); a row its record does not admit is refused as drift, as the word search does. Relevance is the shared mapping of the database's distance. The answer is re-ranked by the shared rule.

- A claim an earlier build appended has no filter columns until `index_claims` reaches it. It is not a candidate, and it makes `unindexed` true.
- Cost is linear in the claims of one knowledge space that pass the filters. The documents say so, and show the deployment-applied approximate index the private tier's documents show.

**Storing.** One `UPDATE` that sets the four columns `WHERE` the scope and claim id match `AND NOT tombstoned`. It answers whether a row was written. A tombstone that commits first leaves nothing to update; one that commits after clears the embedding through the trigger. An earlier binary cannot write the columns: it does not name them.

**The page.** One `SELECT` in claim-id order of live rows, not retracted, of the classifications given, whose embedding is null or whose reference differs. A row with no filter columns (appended before issue #69 and not yet reached by `index_claims`) is not listed: its classification cannot be judged without them.

### 6.4 The three callers of the embedder (`rakka-agent-knowledge-graph`)

- **The retriever.** `KnowledgeGraphCommunalRetriever::with_embedder(self, Arc<dyn AgentMemoryEmbedder>) -> Self`.
  - Given an embedder and a store that reports `semantic-search`, it ranks by meaning and reports retriever version 2. Otherwise it ranks by words and reports version 1, as today. The choice is made when it is built and never changes: a snapshot records one retriever version.
  - `ranking()` answers which of the two it is, so a deployment can assert it got what it wired.
  - It embeds the query with `embed_memory_vector`, and keeps the last query text with its vector, because assembly calls `retrieve` once per granted space with the same query.
  - A failed embedding fails that `retrieve`. Assembly already treats that as a degraded space and continues the turn. There is no per-search fallback to words.
  - `index_incomplete` is the store's `unindexed`.
- **The append executor.** `KnowledgeGraphClaimAppendExecutor::with_embedder(self, embedder, classifications: BTreeSet<MemoryClassification>) -> Self`. After the store answers the append (first time or replay), when the store reports `semantic-search` and the claim's classification is in the set (6.5), it embeds `claim_embedding_text` and stores the result. **Every failure of that step is swallowed**: the finding is still `Appended`. A claim it could not embed is left for the pass.
- **The pass.** `embed_claims(store, embedder, classifications, scope, after, limit) -> ClaimResult<ClaimEmbeddingPassPage { embedded, failed, undecodable, next }>`. It reads one page of `unembedded_claims`, embeds each claim and stores it. A deployment calls it until `next` is `None`. It is idempotent and safe while the store serves.
  - **A claim the model refuses does not stop the pass.** It is counted in `failed` and the cursor moves past it, so one claim a provider always rejects (a content filter, an input it finds too long) cannot keep every later claim of the space unembedded. The same holds for an answer of the wrong length or with a non-finite component. A non-zero `failed` means: run the pass again later. A claim that fails every time stays unembedded, and searches that admit it keep reporting `index_incomplete`. *(My call. The private tier's `reindex` stops on a model error and can be wedged this way.)*
  - A store failure ends the call with the store's error. What was already stored stays.
  - After a model or version change it re-embeds everything, since every claim is then unembedded under the new reference.

### 6.5 What is sent to the embedder

Embedding sends text to a model the deployment chose, which may be outside the deployment. Two kinds of text are sent.

**Claims.** The executor and the pass take the set of classifications they may embed.

- **There is no default. A deployment states the set.** It is a required argument of the executor's `with_embedder` and of `embed_claims`, so sending a class of claims to a model is always something the deployment wrote down. *(Owner's decision, 2026-11-16.)*
- An empty set is allowed and embeds nothing.
- A `Redacted` claim is never sent, whatever the set says. The private tier never embeds redacted content either. *(My call.)*
- A `Retracted` claim is not sent: no search ever admits one. One that is retracted after it was embedded keeps its vector, which no search can reach. *(My call.)*
- A claim outside the set has no embedding, so a meaning-based search does not find it. Word-match search would have. A deployment whose read policy admits a classification must also let it be embedded, or lose those claims when it switches the retriever to meaning.
- `unindexed` is judged against the search's own filters (6.2). So a search that does not admit `Sensitive` claims is not marked incomplete because `Sensitive` claims were left unembedded. One that does admit them is marked incomplete for as long as they are, which is the sign of the mismatch above.

**The query.** Wiring the embedder into the retriever sends the query text to the model on every turn that searches. The query is derived from the run's recent session entries, up to 1024 bytes, with no classification filter: it is conversation text, not a claim. The private tier's meaning-based retrieval already does the same with its own embedder. The documents say this plainly.

### 6.6 What a deployment sees

- Nothing, until it calls `with_semantic_search` on the store and `with_embedder` on the retriever.
- After it does: claims ranked by meaning; `retriever_version` 2 in the snapshot's communal record; `index_incomplete` while claims the search admits have no embedding.
- A fresh claim is findable once its append effect has embedded it. If that failed, it is findable after the next pass.
- A model change makes every claim unembedded under the new reference. Searches find nothing and report `index_incomplete` until the pass has run.

## 7. Rollout and compatibility

- **Additive.** Three SPI operations with default bodies, one module of types, two builders, one function, one PostgreSQL builder and migration. No existing signature changes.
- **Durable shape.** The base schema and every existing record are unchanged. The four columns exist only where a store opted in.
- **Snapshots.** No field is added. A snapshot for whose turn a meaning-based retriever ran records `retriever_version: 2`.
- **Earlier binaries** against an opted-in table: their inserts, tombstones and deletes name none of the four columns. A tombstone clears the embedding through the trigger. They never search by meaning.
- **Rollback.** Clean. An earlier build ignores the columns. The embeddings stay and are still cleared by the trigger.
- **Roll order:**
  1. Have the `vector` extension available on the graph's database.
  2. Run `migrate` from a store built `with_semantic_search`, to its end.
  3. Run `index_claims` for any space with claims from before issue #69, as that issue's step 3 says.
  4. Wire the embedder into the append executor on every node, with the classifications it may embed. They should cover every classification the read policy admits.
  5. Run `embed_claims` for each space, with the same classifications, until it answers no `next`.
  6. Wire the embedder into the retriever. Before step 5 finishes this is safe: searches report `index_incomplete`.
- **New stable strings:** the three operation names in refusals are not new codes; `semantic-search` already exists. The PostgreSQL column and trigger names are fixed once shipped, because `migrate` finds them by name.
- No new error code and no new metric.

## 8. Proofs

**Conformance clauses** (`conformance/semantic.rs`), run by a backend that reports the capability, with a deterministic embedder that needs no model:

1. A store without the capability refuses all three operations with `claim-capability-unsupported`.
2. Admission precedes ranking: a claim excluded by trust, classification, confidence, a required read capability, a tombstone or its scope takes no hit slot, even when it is the nearest.
3. Hits are ordered by relevance and then claim id, and the limit is kept.
4. A claim with no embedding, or with one under another reference, is not a hit and makes `unindexed` true. After its embedding is stored it is a hit and `unindexed` is false.
5. `unindexed` follows the search's filters: an unembedded claim the search does not admit does not set it.
6. `store_claim_embedding` is idempotent, replaces an embedding under another reference, answers `false` for an absent or tombstoned claim and for a claim of another scope, and refuses an invalid embedding.
7. A tombstone removes the embedding: the claim is no longer found, is not listed by `unembedded_claims`, and cannot be given an embedding again. A delete does the same.
8. `unembedded_claims` pages in claim-id order, keeps its clamp, resumes from its cursor, and lists no retracted claim and no claim outside the classifications given.
8a. Storing an embedding changes no byte of the claim's record: `get` answers the claim exactly as before.

**The callers**, against the in-memory store:

9. The retriever with an embedder and a capable store ranks by meaning and reports version 2. With no embedder, or with a store that lacks the capability, it ranks by words and reports version 1.
10. A query that shares no word with a claim finds it by meaning.
11. The retriever embeds one query once across several spaces.
12. A failed query embedding fails the `retrieve` and falls back to nothing.
13. The append executor embeds a fresh claim; a search finds it with no pass.
14. An embedder that fails, or answers a wrong-length vector, does not fail the append. The claim is left unembedded.
15. The executor and the pass send no claim outside their classification set, never a `Redacted` one, and never a `Retracted` one.
16. `embed_claims` embeds what is missing, is idempotent, re-embeds after a reference change, and resumes from its cursor. A claim the embedder refuses is counted in `failed` and does not stop the claims after it.

**PostgreSQL**, gated on `RAKKA_POSTGRES_TEST_DSN`, with the pgvector clauses gated the way the private tier's are (`RAKKA_POSTGRES_PGVECTOR_REQUIRED=1` makes a skip a failure):

17. The conformance clauses pass.
18. A store built without `with_semantic_search` migrates on a database with no `vector` extension and refuses the three operations.
19. The semantic migration rewrites nothing, is repeatable, and leaves every statement of an earlier build working.
20. An earlier build's tombstone statement clears the embedding.
21. A filter column that drifted from its record cannot put a claim in front of a reader the record would refuse.
22. A stored vector whose length differs from its dimension count is refused by the database.

**End to end**, in `examples/multi-agent-goal-acceptance` over real sharded entities:

23. One agent appends a claim, which the append effect embeds. The claim is promoted to `Verified` directly on the store, as the existing `communal_read_path.rs` does, because the example's read policy admits verified claims only. Another agent, granted its space, then gets it in its turn's snapshot from a query that shares no word with it, with `retriever_version` 2.

## 9. Documents

- `docs/rakka-agents.md`: the communal read path gains meaning-based ranking, what is sent to the embedder, and its limits.
- `docs/rakka-compatibility.md`: an entry with the roll order and what an earlier binary does.
- `docs/rakka-agent-security-validation-matrix.md`: erasure reaches the embedding; filters before ranking; the attestation re-run with pgvector.
- `docs/rakka-v1-known-limitations-roadmap.md`: the exact scan, no relevance floor, the model not recorded in a snapshot.
- `docs/rakka-api-boundary-inventory.md`, `CHANGELOG.md`, and the crate documentation of both graph crates.

## 10. Left to the builder

- Names of private helpers, and the exact SQL.
- How the deterministic test embedder maps text to a vector, as long as two texts with the same meaning in the proofs are near and unrelated ones are far.
- Whether the in-memory store keeps vectors beside its claims or in a map of their own.
- The size of the retriever's query memo, as long as one assembly embeds one query once.

## 11. As built

Where the build moved from the sections above. Each is a decision made while building and recorded in the build's ledger.

- **An approximate index is not usable, not merely not shipped (sections 3, 6.3).** The design said a deployment may add one as its own DDL. pgvector indexes only a fixed-dimension expression, and the shipped statement orders by the bare column, so the planner would not use such an index. The documents say that ranking through an approximate index is not available in this release, and do not show index DDL. The private tier's documents have the same gap.
- **The relevance mapping is two functions (6.1).** `claim_semantic_distance_relevance_bps(distance)` is the one mapping; `claim_semantic_relevance_bps(query, candidate)` computes the cosine distance and calls it. PostgreSQL maps the database's distance with the first, so both backends score alike.
- **The in-memory store has a diagnostic `has_embedding` (6.2).** The SPI cannot show that an erased claim's vector is gone, only that no search reaches it: a stub is never admitted and a deleted claim is absent. The conformance clause therefore passed with the removal deleted. The accessor and `erasure_drops_the_embedding_itself` close that; PostgreSQL proves the same on its columns.
- **The `Redacted` rule is held twice (6.5).** The set handed to the store has `Redacted` removed, and the record is checked again just before its text leaves. Either layer alone keeps the tests green; both are kept, because a backend's listing is a nomination by an index.
- **A blank query ranks nothing and calls no model (6.4).** The design did not say. `index_incomplete` is false for such a turn.
- **The PostgreSQL check is stricter (6.3).** The four columns are all null or all set, and the vector has the dimension count beside it. A reference with no vector, or a vector with no reference, cannot exist, so the tombstone trigger keys on `embedding` alone.
- **A row with no filter columns flags a search incomplete only when its trust is one the search admits (6.2, 6.3).** Trust is a base column, so that much of "cannot judge" can be judged.
- **`undecodable` also counts a row whose record contradicts its columns (6.2, 6.3).** The page is what a caller sends to a model, so it lists by the record: a row whose record's classification is outside the set, or is retracted, is not listed and is counted there.
- **A reference whose pipeline version exceeds a `BIGINT` is refused** with `claim-embedding-invalid` on PostgreSQL.
- **`rakka-agent` is a dependency of the PostgreSQL graph crate**, default features off, where it was a dev-dependency: the new SPI operations carry its classification and embedding-reference types.
- **CI requires pgvector.** The PostgreSQL job sets `RAKKA_POSTGRES_PGVECTOR_REQUIRED=1`. One proof runs only where the extension is absent (the opt-in migration failing whole and releasing its lock) and announces a skip in CI.
- **Proof 17 to 22 use a store built without the flag as the earlier binary.** That is exact because no existing statement of the crate changed.
- **Proof 23 grants the writer its space.** The dispatcher refuses an append into a space its agent was never given (`widened-knowledge-access`). Being granted, the writer's own turn searches the space, so the embedder also receives the writer's query; the test asserts both texts. The reader asks "automobile whereabouts", which shares no word with the claim, so the word-ranked control selects nothing.
- **The PostgreSQL found-by-meaning proof embeds through the pass**, not the append executor, whose only store call is `store_claim_embedding`.
