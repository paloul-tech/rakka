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
  not finite.
- The failure itself is dropped. A model's error can quote the text it was
  given.
- A failed write still ends the call. So does an embedder identity that
  cannot be stored, which is checked once, before the page is read.
- `index_decoded` is split into the model call and `store_vector`, so the
  pass can tell the two kinds of failure apart. `index_memory`, for one
  memory id, is as it was: the identity is checked before the model is
  called, and a failed embedding is an error.
- The crate's pgvector unit tests now fail, where they skipped, when
  `RAKKA_POSTGRES_PGVECTOR_REQUIRED=1` is set and the database has no
  `vector` extension. The conformance suite already did.

## 4. My calls

1. **No new field on `ReindexPage`.** The issue left open whether the page
   should name the first memory that failed. `ReindexPage` is a public
   struct that is not `#[non_exhaustive]`, so a new field breaks any caller
   that builds one, as a test double of the pass would.
2. **One count for several causes.** `failed` now holds a row whose id or
   record cannot be read, a record the schema policy refuses, and a failed
   embedding, and does not say which. A caller that labels the count
   "undecodable" is wrong for the last. A second count would be the same
   breaking change.
3. **No log line.** The crate has no logging dependency, and this change
   does not add one.
4. **An outage is counted, not returned** (section 2), and so is an
   embedder that is misconfigured: one whose every answer contradicts the
   identity it declares used to be an error on the first memory, and is
   now a page of failures. That follows from counting a wrong-length
   answer at all, which the issue asked for.

## 5. What changes for a caller

| | Before | Now |
|---|---|---|
| One memory the embedder refuses | The call returns the embedder's error. Every retry from the same cursor fails on that memory | The page counts it in `failed`, and the cursor moves past it |
| The embedder fails for every input: it is unavailable, its credentials or quota are refused, or its answers contradict the identity it declares | The call returns the embedder's error on the first memory it tries | Every page counts each memory it tried in `failed`, the sweep runs to its end, and nothing is embedded |
| A failed write | The call returns it | Unchanged |
| An embedder identity that cannot be stored | The call returns it | Unchanged |

Consequences:

- **A page cannot tell an outage from refusals.** One refused memory in a
  page of one, and an outage, are the same page. So there is no rule a
  caller can apply to a page to detect an outage, and "stop at a page that
  embedded nothing" is approach 2 again: it holds the caller on the
  refused memory. A caller that needs to know whether its embedder works
  has to ask the embedder.
- **During an outage a sweep makes one failing model call for each memory
  it tries to embed,** where it made one in all. A caller bounds that by
  how many pages it asks for.
- **A caller that detected an outage, or a misconfigured embedder, by the
  error no longer gets one.**

A memory that failed is still a candidate, so the next sweep reads it
again. The pass is idempotent: a rerun reads exactly what is still missing.

## 6. Tests

In the crate's pgvector tests, which need `RAKKA_POSTGRES_TEST_DSN`:

- a memory refused first in a sweep of pages of one, by an error, by a
  vector one component short, and by a component that is not a number: the
  sweep ends, the two memories behind it are embedded, and a second sweep
  reads the refused memory again and nothing else;
- a memory refused in the middle of one page: the memories on both sides
  of it are embedded;
- an embedder that refuses everything: each page counts what it tried, at
  one call each, and the sweep ends;
- a failed write ends the call and what was stored before it stays;
- `index_memory` refuses an identity that cannot be stored before the
  model is called, and still returns one memory's failed embedding.

The first three fail on `main` with the embedder's error. Run against
PostgreSQL 16.14 with pgvector 0.8.5.

The change was then broken on purpose, six ways, and each break failed at
least one of these tests: the bug itself; a refusal not counted; a page
that gives up at its first refusal; a write failure counted and not
returned; a non-finite answer still returned as an error; and `index_memory`
calling the model before it checks the identity.

## 7. The review

A fresh reviewer read the first build and probed it against a database. It
found the fix sound for every shape of page it tried, and these faults,
all corrected:

1. **The documents gave callers a rule that brought the wedge back.** The
   first version of section 5, the compatibility entry and the doc comment
   said a page whose `failed` is everything it read is what an outage
   looks like, and that a caller could stop there. A refused memory in a
   page of one is that page. The reviewer showed a caller following the
   rule embeds nothing behind it. The rule is gone and section 5 says why.
2. **The tests passed with the bug present for every page size above
   one.** Nothing had a refused memory followed by an embeddable one in
   the same page, nothing exercised a non-finite answer through the pass,
   and nothing held that a failed write ends the call. Three tests added.
3. **A misconfigured embedder had become silent and no document said so.**
   Section 4, call 4, and the second row of the table.
4. **`docs/rakka-v1-known-limitations-roadmap.md` still said the pass
   stops at a refused memory.** Corrected.
5. **`index_memory` had changed:** the first build checked the identity
   after the model call. Restored, with a test.
6. `RAKKA_POSTGRES_PGVECTOR_REQUIRED=1` did not make these tests fail on a
   skip, though the first version of this record said it did. It does now.
7. Wording: "one failing call for every memory it reads" counted rows the
   pass never sends to the model; the `# Errors` text left out two causes;
   and the changelog entry was filed as a fix though it tells callers what
   to change.

## 8. Not done

- **A report that says why.** The reviewer proposed a second method beside
  `reindex` that returns a `#[non_exhaustive]` report: counts by cause, and
  the id and stable error code of the first failed embedding. It would let
  a caller label its metrics, and tell an outage from refusals by the
  code. It adds public surface, so it is left for a decision.
- **A write that fails because of one record's own data still holds the
  pass on that record.** A digest that holds a NUL is kept by the store
  and refused by the derived table. Not new, and reaching it takes a
  record no public constructor makes.
- **A row whose id this binary refuses ends a sweep of pages of one
  early.** The id is parsed before the cursor is set, so a page of only
  that row answers no cursor. Not new, and reaching it takes a row written
  past the store.
- **The in-memory reference retriever has no reindex pass, but it has the
  same failure on its read path:** it embeds every admitted memory at each
  retrieval and returns the first error, so one refused memory fails every
  retrieval for that agent. Not new.
