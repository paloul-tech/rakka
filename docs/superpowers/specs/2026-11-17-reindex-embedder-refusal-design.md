# A private memory reindex is not stopped by a memory its embedder refuses

Issue #151.

## 1. What was wrong

`PgvectorPrivateMemoryRetriever::reindex`
(`crates/rakka-agent-postgres/src/retrieval.rs`) is the pass a deployment
calls to give one agent's private memories their embeddings. It reads a
page of live memories that have no current vector, in memory-id order after
a cursor, and embeds each.

A memory whose embedding failed ended the call with the embedder's error:

- The caller got no page, so it had no cursor to go on from.
- Memories embedded earlier in the page now had a vector and dropped out of
  the query, so on a retry from the same cursor the refused memory was the
  first row, and the call failed at once.
- Nothing after it, in id order, was ever embedded by the pass.

For a provider that is down, an error is a fair answer: a later retry
works. For a failure that belongs to one input it is a wedge: a content
filter that rejects the text, an input the model finds too long, an answer
of the wrong length for that input. The pass already protected itself
against one unreadable record for exactly this reason, and its
documentation says so.

The issue was filed from reading the code. The first test below reproduced
it: the first call returns the embedder's error.

## 2. Approaches

1. **Count a failed embedding and page past it,** as an unreadable record
   is counted. This is what the communal pass does
   (`rakka_agent_knowledge_graph::embed_claims`, issue #140).
2. **Stop only when a whole page fails.** It keeps an outage cheap: one
   failing page, not one failing call for each memory. But a page of one
   refused memory is then the wedge again, which a last page or a caller's
   page size of one makes ordinary.
3. **Tell an outage from a refusal by the error.** The embedder contract
   has no such distinction to read.

Chosen: 1.

## 3. The change

- `reindex`: a memory whose embedding fails is counted in
  `ReindexPage::failed` and the pass moves on. That is an error from the
  embedder, or an answer of the wrong length or with a component that is
  not finite. The cursor already advances on the row's own id before
  anything fallible runs.
- The failure itself is dropped. A model's error can quote the text it was
  given.
- A SQL failure still ends the call, and so does an embedder identity that
  cannot be stored (no model, no dimensions, more than the pgvector
  ceiling). Those are checked before the page is read, and again at each
  write.
- `index_decoded` is split into the model call and `store_vector`, so the
  pass can tell the two kinds of failure apart. `index_memory`, for one
  memory id, still returns the embedder's error.

## 4. My calls

1. **No new field on `ReindexPage`.** The issue left open whether the page
   should name the first memory that failed. `ReindexPage` is a public
   struct that is not `#[non_exhaustive]`, so a new field breaks any
   caller that builds one, as a test double of the pass would. The count
   says a sweep is incomplete; why one memory fails can be found by calling
   `index_memory` for it, which returns the error.
2. **One count for three causes.** `failed` now holds an undecodable
   record, a record the schema policy refuses, and a failed embedding, and
   does not say which. A caller that labels the count "undecodable" is
   wrong for the third. A second count would be the same breaking change.
3. **No log line.** The crate has no logging dependency, and this change
   does not add one.
4. **An outage is counted, not returned** (section 2).

## 5. What changes for a caller

| | Before | Now |
|---|---|---|
| One memory the embedder refuses | The call returns the embedder's error. Every retry from the same cursor fails on that memory | The page counts it in `failed`, and the cursor moves past it |
| The embedder is unavailable | The call returns the embedder's error on the first memory it tries | Every page counts each memory it tried in `failed`, the sweep runs to its end, and nothing is embedded |
| A SQL failure | The call returns it | Unchanged |

Two consequences:

- **A caller that detects an outage by the error must read `failed`.** A
  page whose `failed` is everything it read, with nothing indexed, is what
  an outage looks like.
- **During an outage a sweep makes one failing model call for each memory
  it reads.** A caller bounds that by how many pages it asks for, and can
  stop at the first page that embedded nothing.

A memory that failed is still a candidate, so the next sweep reads it
again. The pass is idempotent: a rerun reads exactly what is still missing.

## 6. Tests

In the crate's pgvector tests, gated on `RAKKA_POSTGRES_TEST_DSN`
(`RAKKA_POSTGRES_PGVECTOR_REQUIRED=1` makes a skip a failure):

- `pgvector_reindex_pages_past_a_memory_its_embedder_refuses_when_dsn_is_set`:
  three memories, the first by id refused. Once by an error and once by a
  vector one component short. The sweep ends without an error, `failed` is
  1, the other two are retrievable, and a second sweep reads the refused
  memory again and nothing else.
- `pgvector_reindex_through_an_embedder_outage_counts_every_memory_when_dsn_is_set`:
  an embedder that refuses everything. Each page counts what it read, the
  cursor moves, and the sweep ends.

Both failed before the change with the embedder's error. Run against
PostgreSQL 16.14 with pgvector 0.8.5, with no clause skipped.

Not tested: that a SQL failure still ends the call. The write is the same
statement as before, and its error is returned as before.

## 7. Not done

- The in-memory reference retriever has no reindex pass, so nothing
  changes there.
- No way for a caller to learn which memory failed from the page.
