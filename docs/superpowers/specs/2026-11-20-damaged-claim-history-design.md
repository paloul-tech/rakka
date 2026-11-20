# A claim whose stored history is damaged, and the sweep's two reads (issues #138 and #137)

2026-11-20. Short design, with my calls listed. The owner reviews at the pull
request.

## 1. What was wrong

**Issue #138.** The PostgreSQL store writes a claim and its transition history
in one statement, so it never leaves the two disagreeing. Rows edited by hand,
or tables restored to different moments, do. For such a claim:

- `tombstone` and `delete` read the mismatch as a transition that had raced
  them, read again eight times, and answered `claim-backend-failed`, "the
  tombstone lost 8 compare-and-set races; retry". Nothing had raced, and a
  retry answered the same. The claim could not be erased through the store.
- The retention sweep returned that error, so every pass ended at that claim.
- A claim missing only its last transition was counted `moved` on every pass
  and never reported.

The issue was written from reading the code. All three are now reproduced:
the five proofs of section 7 that damage a claim were run against the store
as it was on `main`, and failed with those answers.

One more shape turned up while building, which the issue does not name. A
claim that had lost one transition and gained another held as many rows as its
record counts. The store compared only the number, named the operations of
the rows that were there, and **erased the claim with the lost transition's
stored result left in the ledger**. That result holds the whole claim as it
stood after the transition. Run, with the store's check put back to a count
of rows: the tombstone is applied, the lost transition's ledger row is still
a `transition`, and its stored result still holds the statement.

**Issue #137.** The sweep reads a claim that is aged by its last transition
twice: the query returns it with its transition count, and a second read
returns the transition of that ordinal. Where a later transition lies behind
the one it asked for, the sweep counts the claim as moved. No clause reached
that answer. The one clause that moved a claim did it through the
before-delete hook, which runs after both reads.

## 2. The four questions of #138

The issue lists four things to decide. They are calls 1 to 4 of section 5.
The second is the one with a real choice in it.

### Approaches to "should an erasure still go through"

Erasing a claim means clearing the stored result of every operation in its
history. The ledger has no claim column: the operations are named by the claim
(its append, its tombstone) and by its transition rows. A transition with no
row names nothing, and its stored result holds the claim.

1. **Refuse, and nothing else.** Truthful and cheap. The claim still cannot
   be erased through the store, which is the issue's main complaint. An
   operator would do it by hand, and would have to know to clear ledger rows
   and not delete them.
2. **Go through, erasing what the rows name.** This is what the issue
   suggests. It reports success and leaves a whole copy of the claim in the
   ledger for each transition whose row is gone.
3. **Go through, and find the rest in the ledger.** Complete. Finding those
   stored results means reading every transition row of the tenant and space
   in the ledger, since no index leads to them: time in proportion to that
   ledger, once for each damaged claim.
4. **Refuse by default, and do 3 in a store that opted in.**

I chose the fourth. The second is not an erasure. The third as the default
would put that read inside every sweep that meets damage: in a space where a
restore lost the transition table, that is one pass over the ledger for each
claim the sweep deletes, with nothing said to anyone. Refusing by default
tells the operator the store is damaged, costs one read, and keeps the sweep
moving. The opt-in is the path the issue asks for, taken deliberately.

## 3. The rule

A claim's stored history is whole when the store holds the transitions of
ordinals one to the record's count, each once. Anything else is damage.

Damage is told from a race in one read. A transition raises the claim row's
fence and adds its row in one statement, so the history is read together with
the fence. A fence that is no longer the one the erasure read is a claim that
moved: a race, read again, as before. The fence it read beside a history that
is not whole is damage, and reading again would answer the same.

## 4. The change

**`rakka-agent-knowledge-graph-postgres`, `src/erasure.rs`.**

- `CLAIM_HISTORY_SQL` reads the fence and the transition rows together.
  `history_state` judges the read: whole, moved or damaged.
- `tombstone` and `delete` answer `claim-history-damaged` for a damaged claim.
  Nothing is written and no operation id is spent.
- In a store built `with_damaged_history_erasure()`, they go on instead. The
  operations the claim and its stored rows name are kept, and
  `CLAIM_LEDGER_OUTCOMES_SQL` adds the transition results in the scope's
  ledger that belong to the claim. A match on the claim's id is a nomination:
  each nominated result is parsed and its own claim id compared, because the
  id also appears in the result of another claim that points at this one.
  The erasure is then the same fenced statement as any other.

**`rakka-agent-knowledge-graph`.**

- `ClaimError::HistoryDamaged`, code `claim-history-damaged`.
- `KnowledgeSpaceRetentionReport::damaged`.
- The sweep (`src/retention.rs`). When its second read is not the claim's last
  transition with nothing behind it, it reads the claim once more. A claim
  that is gone or at a higher count moved. A claim still at the count the
  query read is damaged. Either way it stops there. A delete the store
  refuses as damaged is counted `damaged` too, and the pass goes on.
- The sweep's test seam gains a step between its two reads, and the
  conformance suite gains the clause that uses it (#137).

No schema change, no migration, no durable field.

## 5. My calls

Yours to reverse.

1. **Its own stable code, `claim-history-damaged`.** Not the drift error the
   issue points at: that one answers `claim-backend-failed`, the code a lost
   race answers too, and a caller cannot tell "retry" from "never" by it.
2. **Refused by default; erased, completely, by a store that opted in**
   (section 2). Reversing to "always erase" is one line. Reversing to "never"
   removes the option and about seventy lines.
3. **The sweep counts a damaged claim and goes on.** It counts it on every
   pass. Nothing remembers that a claim was reported.
4. **Damage is decided by what one statement reads, not by counting
   attempts.** A lost race still answers `claim-backend-failed` after eight
   real ones.
5. **The right number of rows is not enough.** The ordinals are compared.
   This changes an answer that used to be a success (section 1).
6. **The sweep does not act on a claim it cannot read cleanly, whatever the
   store would do.** Through an opted-in store it deletes a damaged claim
   whose last transition it reads cleanly. It still only counts one whose
   last transition is gone, and one with an entry behind its last, though
   that one's last transition is there to age it by. The explicit delete is
   the way out for both. Aging the second kind would be a few lines; I left
   it because the sweep would be acting on a listing it has just found wrong.
7. **An unreadable stored result stops the opted-in erasure.** A result that
   mentions the claim and cannot be parsed fails it with
   `claim-backend-failed`. Clearing it would be safe for every reader, since
   none can read it. I did not want this path to clear a row it cannot
   attribute.
8. **The opt-in is on the store, not on the request.** `ClaimDeleteRequest`
   and `ClaimTombstoneRequest` are plain structs with public fields; a field
   on them breaks every caller that builds one. The store already takes its
   options this way (`with_semantic_search`, `with_migration_lock_wait`).
9. **The sweep's re-read compares only the transition count.** A damaged
   claim that was also tombstoned since the query is counted `damaged`. No
   test could tell that from counting it `moved`, so the comparison is not
   there.
10. **#137 by a hook and not a wrapping store.** The issue allowed either.
    The hook runs the same clause on both backends.

## 6. Mixed builds and rollback

Nothing durable changes. An earlier build answers a damaged claim as before.

## 7. Tests

**#137.** `retention_leaves_a_claim_that_moves_between_its_two_reads`, a
conformance clause run on both backends. It lands a retraction between the
sweep's two reads and holds that the claim is counted `moved` and kept, that
no delete is attempted for it, and that a later pass ages it by its new
state.

**#138, the store** (`postgres_damaged_history_proofs.rs`, ten proofs against
a live database, each damaging rows with a statement of its own):

- Four shapes of damage are refused `claim-history-damaged` by `tombstone`
  and by `delete`, twice each, with nothing written: the last transition
  gone, an earlier one gone, one too many, and the right number of the wrong
  ones.
- The sweep counts three damaged claims, deletes the healthy one behind
  them, and counts the three again on the next pass.
- An opted-in store deletes a damaged claim, clears the stored result of the
  transition with no row, and leaves a neighbour's stored result that
  mentions the claim's id.
- An opted-in store tombstones a damaged claim and deletes the stub; a store
  that did not opt in refuses the stub's delete.
- An unreadable stored result stops the opted-in erasure, and nothing is
  written.
- The sweep through an opted-in store deletes what it reads cleanly and
  counts the rest.
- Three shared erasure clauses pass through an opted-in store.

The first five were watched failing on the store as it was: three with "lost
8 compare-and-set races; retry", one with an erasure that went through, and
the sweep's with the pass ending on that error. The three in which the opt-in
erases were watched failing with the option present and doing nothing. The
last two passed when written and are held by breaks: skipping an unreadable
result fails the one, and a whole history that names no transition's
operation fails the other.

**#138, the sweep** (`retention_damaged_history.rs`, four tests on the
in-memory store behind a store that answers as a damaged backend does): the
last transition gone, an entry behind the last, a delete refused as damaged,
and a claim that is gone by the re-read, which is `moved` and not `damaged`.
The first and third were watched failing. The other two passed when written
and are held by breaks: counting every unclean read as moved fails the
second, and counting every one as damage fails the fourth.

**Unit tests.** `history_state` over whole, moved and damaged reads;
`outcome_claim_id`; the new code's stability.

**Breaks.** Eighteen breaks of the change were run one at a time against
both crates, the PostgreSQL crate against a live database: seven of the sweep
and eleven of the store. Each is caught. One is caught only by a unit test:
with the fence comparison removed, so that a race reads as damage, no
database proof fails (section 8).

## 8. Not done

- Reads do not look for damage. `get`, `query` and `transitions` answer what
  is stored.
- Nothing repairs a damaged history.
- A transition row that cannot be parsed still fails an erasure with
  `claim-backend-failed`, in either store.
- A trust transition of a damaged claim was not examined.
- The moved answer of `history_state` is held by a unit test of the function.
  No test lands a transition between the erasure's two reads at the
  database; issue #136 is the deterministic race proofs.
- The opted-in erasure's ledger read is one statement over the scope's
  transition rows. It is not paged.
