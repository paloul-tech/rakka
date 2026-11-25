# A sweep reads past a row it cannot use

Issues #199 and #200, and two siblings of #200 found while reading it.

## 1. What is wrong

Three maintenance passes page through a table in id order, after a cursor
the caller feeds back: the private tier's
`PgvectorPrivateMemoryRetriever::reindex`
(`crates/rakka-agent-postgres/src/retrieval.rs`), and in
`crates/rakka-agent-knowledge-graph-postgres` the store's
`unembedded_claims` page (`semantic.rs`), which `embed_claims` walks, and
the search-index backfill (`search.rs`). Each promises that one bad record
cannot hold it. Two kinds of row still do.

- **A row whose id this build refuses (#200).** `reindex` parses the id
  before it sets the cursor, counts a refused one in `failed`, and does not
  move the cursor. A page made only of such rows answers `next = None`, the
  answer of a finished sweep, though more rows follow. Starting over does
  not help: the refused row is read first again. The two communal passes
  keep the raw text as their cursor, but convert it at the end of a full
  page (`ClaimId::new(last)?`, `semantic.rs:563`, `search.rs:331`). When
  the last row of a full page holds a refused id, the page is an error, and
  so is every retry. Such a row is written past the store: by a later build
  with a wider id alphabet, which is the rolling upgrade the `reindex` doc
  cites, or by a hand repair.
- **A row whose write the derived table refuses (#199).** A private memory
  whose `content_digest.value` holds a NUL is kept by the store and refused
  by the derived table's `TEXT` column (SQLSTATE `22021`). `reindex`
  returns the write error and the cursor stays, so every retry meets the
  same memory. It takes one assignment to a public field to make.
- **Latent, found with #199.** `embedder_identity` lets through an embedder
  version of 0 and a model name holding a NUL. Both fail the derived
  table's write for every row (`CHECK (version > 0)`, `22021`), though the
  `reindex` doc says an identity that cannot be stored is refused before the
  page is read.

## 2. Approaches

For #200, the owner chose 1:

1. **Read on inside the call.** `limit` counts rows whose id this build
   reads. A row whose id it refuses is counted and read past by its raw
   text, and does not count toward `limit`. The cursor always lands on a
   readable id. No public type changes.
2. **A raw-text cursor type** for `after` and `next`. `limit` stays exact,
   but the public API breaks, and rakka.host feeds `next` into `after` and
   builds `ReindexPage` by struct literal.
3. **Document the limit.** Leaves the sweep held in the rolling-upgrade case
   the doc names.

For #199, proposed and not overruled:

1. **Count a write the database refuses for the row's data** (SQLSTATE
   class 22 or 23) in `failed` and page past it, as #151 does for a refused
   embedding. Tighten `embedder_identity` so such an error can only come
   from the row.
2. **Refuse the digest earlier,** in `AgentPrivateMemory::validate`.
   `validate` also runs when a record is decoded (`memory.rs:2211`), so a
   record already stored with such a digest would become unreadable on
   every read path, not just refused on write. It also covers only the
   digest.
3. **Check the known columns in Rust before the write.** Narrow, and blind
   to a cause nobody listed.

## 3. The change

### The private pass

- **The page rule.** A page handles up to `limit` rows whose id this build
  reads. A row whose id it refuses is counted in `failed`, read past by its
  raw text, and not counted toward `limit`. The call reads on until it has
  handled `limit` readable rows or the rows run out. `next` is the last
  readable row handled, and `None` when the rows ran out. Each row is
  counted once per sweep.
- **A write refused for the row's data.** An error from the derived row's
  write whose SQLSTATE is in class 22 (data exception) or 23 (integrity
  constraint violation) is counted in `failed` and paged past. So is a
  record revision outside `BIGINT`, which the write already refuses in Rust
  before any SQL. Every other write error still ends the call: a lost
  connection, a missing table, a refused privilege, a serialization failure
  or a lock timeout. What the call stored before it stays.
- **The identity check.** `embedder_identity` also refuses a version of 0
  and a model holding a NUL, as `memory-embedding-invalid`. With the
  existing checks, every value a write takes from the identity is then one
  the table stores, so a class 22 or 23 error can only come from the row.
- **`index_memory` is unchanged** apart from the identity check: a write
  refused for one memory is still its error. It is how a caller learns why
  one memory fails.

### The communal passes

- `unembedded` (`semantic.rs`) and the search-index backfill (`search.rs`)
  take the same page rule. A row whose claim id this build refuses is
  counted in `undecodable` (respectively `skipped`), read past, and not
  counted toward `limit`, and `next` is always a readable claim id.
- The doc of `KnowledgeGraphStore::unembedded_claims` and of
  `ClaimEmbeddingPage::next` say so. The in-memory store cannot hold such
  a row and is unchanged. `embed_claims` is unchanged.
- Neither communal write takes free text from a record (claim tokens are
  alphanumeric, the other values are typed ids or enums), so #199 has no
  communal counterpart.

## 4. My calls

1. **Classes 22 and 23 whole**, not a list of codes. Both classes describe
   the values written. With the identity check tightened, every such value
   in this statement comes from the row, and neither class is transient.
2. **The tightened identity check also reaches `retrieve`,** which shares
   it. An embedder at version 0 could never store a vector, so its
   retrievals answered nothing. They are now an error, as a dimension count
   above the ceiling already is. rakka.host declares version 1
   (`AgentRevisionNumber::INITIAL`).
3. **A revision outside `BIGINT` is counted, not returned.** It is the
   row's own data, refused one step earlier than the database would.
4. **The two communal siblings are in scope.** Same defect, same rule, and
   the same reachability.
5. **No new field on any page type.** `ReindexPage`, `ClaimEmbeddingPage`
   and `ClaimSearchIndexPage` are public and not `#[non_exhaustive]`, and
   the host builds `ReindexPage` by struct literal (#151's call 1).
6. **No bound on refused rows read in one call.** Each costs a parse, with
   no model call and no write. A bound would end the call on a refused id,
   which no cursor of these types can name.

## 5. What changes for a caller

| | Before | Now |
|---|---|---|
| A page of only refused ids (private) | `next = None`, the sweep ends early | Counted in `failed` and read past; the page ends on a readable id or at the end |
| A refused claim id last in a full page (communal) | An error on this page and every retry | Counted and read past |
| A private memory whose write the table refuses for its data | An error on this page and every retry | Counted in `failed` and paged past |
| Any other write error | Ends the call | Unchanged |
| An embedder at version 0, or with a NUL in its model | The first write fails; `retrieve` answers nothing | `memory-embedding-invalid` before the page is read or the model is called, from `reindex`, `index_memory` and `retrieve` |

- One call may read more than `limit` rows, all of them beyond the first
  `limit` with refused ids. It makes no more model calls or writes than
  `limit`.
- `ReindexPage::failed` now also holds writes refused for the row's data.
  rakka.host counts `failed` as `record_unindexed`, which still fits, and
  needs no change. It continues past such a row where it used to stop on
  the error.

## 6. Tests

All against PostgreSQL 16.14 with pgvector 0.8.5
(`pgvector/pgvector:pg16`), with `RAKKA_POSTGRES_PGVECTOR_REQUIRED=1`.
Refused ids are made as #200 made them: a copy of a healthy row under an
id holding `|`.

- The four sweeps of #200's table, plus a refused id first, last, and as a
  run longer than the page with a limit of 1. Each sweep embeds every
  readable memory, and over a sweep each refused row is counted once.
- #199: a NUL digest in a sweep of pages of one, and in the middle of one
  page. The memories on both sides are embedded, and a second sweep reads
  the refused memory again and nothing else.
- The write-error boundary: a trigger scoped to the test's own tenant
  raises a chosen SQLSTATE on the derived write. Codes in classes 22 and
  23 are counted. Codes outside them (for example `40001`, `42501`,
  `XX000`) end the call, and what was stored before stays. This replaces
  the NUL digest as the #151 test's "a failed write ends the call". The
  test drops its trigger when it ends.
- The identity check: a version of 0 and a model holding a NUL are refused
  by `index_memory`, `reindex` and `retrieve` before the model is called.
- Communal: for both passes, a refused claim id last in a full page, and a
  page of only refused ids. The pass reaches its end and handles every
  readable claim.
- Each fix is broken on purpose and must fail a named test.

## 7. Docs

The `retrieval.rs` module and `reindex` docs, and the comment in the loop;
`ReindexPage::failed`; the SPI and page docs above;
`docs/rakka-v1-known-limitations-roadmap.md`; `docs/rakka-compatibility.md`;
`CHANGELOG.md`.

## 8. Not done

- **A report that says why** a page failed (#151's section 8), still for
  the owner.
- **The in-memory retriever fails every retrieval on one refused memory**
  (#201), next in order, separately.
