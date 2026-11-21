# A claim whose stored history is damaged, and the sweep's two reads (issues #138 and #137)

2026-11-20. Short design, with my calls listed. The owner reviews at the pull
request. Section 9 is what a review of the first version changed.

## 1. What was wrong

**Issue #138.** The PostgreSQL store writes a claim and its transition history
in one statement, so it never leaves the two disagreeing. Rows edited by hand,
or tables restored to different moments, do. For such a claim:

- `tombstone` and `delete` read the mismatch as a transition that had raced
  them, tried eight times, and answered `claim-backend-failed`, "the
  tombstone lost 8 compare-and-set races; retry". Nothing had raced, and a
  retry answered the same. The claim could not be erased through the store.
- The retention sweep returned that error, so every pass ended at that claim.
- A claim missing only its last transition was counted `moved` on every pass
  and never reported.

The issue was written from reading the code. All three are now reproduced:
the proofs of section 7 that damage a claim were run against the store as it
was on `main`, and failed with those answers.

Two more shapes turned up, which the issue does not name. Both were erased
with a wrong result, because the store compared only the number of rows and
then took an operation's name from every row it found:

- **The right number of wrong rows.** A claim that had lost one transition
  and gained another was erased with the lost transition's stored result
  left in the ledger. That result holds the whole claim as it stood after
  the transition. Run, with the store's check put back to a count of rows:
  the tombstone is applied, the lost transition's ledger row is still a
  `transition`, and its stored result still holds the statement.
- **Another claim's row.** A claim holding, in place of one of its own rows,
  a row that is another claim's transition names that claim's operation, and
  that claim's stored result is cleared with its own. Run, with the store's
  check put back to a count of rows: the tombstone is applied, the other
  claim's stored result is `erased`, and the damaged claim's own result,
  which no row names, is left.

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
3. **Go through, and find the claim's stored results in the ledger.**
   Complete. It means reading every transition row of the tenant and space
   in the ledger, since no index leads to them: time in proportion to that
   ledger, for each damaged claim.
4. **Refuse by default, and do 3 in a store that opted in.**

I chose the fourth. The second is not an erasure. The third as the default
would put that read inside every sweep that meets damage: in a space where a
restore lost the transition table, that is one pass over the ledger for each
claim the sweep deletes, with nothing said to anyone. Refusing by default
tells the operator the store is damaged, costs one read, and keeps the sweep
moving. The opt-in is the path the issue asks for, taken deliberately.

## 3. The rule

A claim's stored history is whole when, for each ordinal from one to its
record's count, the store holds one row whose record is that claim's
transition of that ordinal, and no other row. Anything else is damage: a row
missing, a row too many, a row that is another claim's, a row that cannot be
read.

Damage is told from a race in one read. A transition raises the claim row's
fence and adds its row in one statement, so the history is read together with
the fence. A fence that is no longer the one the erasure read is a claim that
moved: a race, read again, as before. The fence it read beside a history that
is not whole is damage, and reading again would answer the same.

A damaged history is asked for no operation's name. A row that is not the
claim's own names an operation that is not the claim's.

## 4. The change

**`rakka-agent-knowledge-graph-postgres`, `src/erasure.rs`.**

- `CLAIM_HISTORY_SQL` reads the fence and the transition rows together.
  `history_state` judges the read: whole, moved or damaged.
- `tombstone` and `delete` answer `claim-history-damaged` for a damaged claim.
  Nothing is written and no operation id is spent.
- In a store built `with_damaged_history_erasure()`, they go on instead. The
  operations the claim itself names are kept: its append and its tombstone.
  `CLAIM_LEDGER_OUTCOMES_SQL` adds the transition results in the scope's
  ledger that belong to the claim. A match on the claim's id is a
  nomination: each nominated result is parsed and its own claim id compared,
  because the id also appears in the result of another claim that points at
  this one. The erasure is then the same fenced statement as any other.

**`rakka-agent-knowledge-graph`.**

- `ClaimError::HistoryDamaged`, code `claim-history-damaged`.
- `KnowledgeSpaceRetentionReport::damaged`.
- The sweep (`src/retention.rs`) reads the end of a claim's history before
  it acts on what the query returned: the entry of the claim's count, and
  nothing behind it. For a claim past `Proposed` that read dates the state
  it rests in, as before. For a stub or a proposed claim, which its own
  record dates, the read is new, and is made once the claim is due. When the
  read answers anything else, the sweep reads the claim once more. A claim
  that is gone or at a higher count moved. A claim still at the count the
  query read is damaged. Either way it stops there. A delete the store
  refuses as damaged is counted `damaged` too, and the pass goes on.
- The sweep's test seam gains a step before that read, and the conformance
  suite gains the clause that uses it (#137).

No schema change, no migration, no durable field.

## 5. My calls

Yours to reverse.

1. **Its own stable code, `claim-history-damaged`.** Not the drift error the
   issue points at: that one answers `claim-backend-failed`, the code a lost
   race answers too, and a caller cannot tell "retry" from "never" by it.
2. **Refused by default; erased, completely, by a store that opted in**
   (section 2). Reversing to "always erase" is one line. Reversing to "never"
   removes the option and the ledger read.
3. **The sweep counts a damaged claim and goes on.** It counts it wherever it
   meets the damage. It is no scan for damage: a claim in a status with no
   window is retained unread, and a `damaged` of zero does not say a space
   holds none.
4. **Damage is decided by what one statement reads, not by counting
   attempts.** A lost race still answers `claim-backend-failed` after eight
   real ones.
5. **Every row is held to being the claim's own transition of its ordinal.**
   This changes answers that used to be a success (section 1).
6. **The sweep deletes no claim whose history does not end where its record
   says, through either store.** Rows behind the record's count may be the
   claim's real, later history, under a claim row restored to an earlier
   state. A verified claim, which is kept, would be deleted as the proposed
   claim its stale row says it is. So the sweep reads the end of the history
   of every claim it is about to delete, a stub's and a proposed claim's
   too, and only counts one it does not find clean. Through an opted-in
   store it does delete a damaged claim whose history ends where it should:
   one that lost an earlier transition, or holds an unreadable one. The
   explicit erasure is the way out for the rest. This costs one bounded read
   for each stub or proposed claim the sweep deletes.
7. **An unreadable stored result stops the opted-in erasure.** A result that
   mentions the claim and cannot be parsed fails it with
   `claim-backend-failed`, in a message that names the result's operation.
   Clearing it would be safe for every reader, since none can read it. I did
   not want this path to clear a row it cannot attribute.
8. **An unreadable transition row is damage; a row from another schema is
   not.** A row whose record cannot be read answers `claim-history-damaged`,
   where it used to answer `claim-backend-failed` and end the sweep's pass.
   A row from a schema newer or older than the binary reads keeps the
   schema's own refusal, in an opted-in store too: a binary that cannot read
   a record must not erase around it.
9. **The opt-in is on the store, not on the request.** `ClaimDeleteRequest`
   and `ClaimTombstoneRequest` are plain structs with public fields; a field
   on them breaks every caller that builds one. The store already takes its
   options this way (`with_semantic_search`, `with_migration_lock_wait`).
   The cost is that the sweep cannot choose per call, which is why call 6
   is in the sweep.
10. **The sweep's re-read compares only the transition count.** A damaged
    claim that was also tombstoned since the query is counted `damaged`. It
    is damaged. A test could tell that from counting it `moved`; I chose
    the one comparison and the answer that keeps reporting the damage.
11. **#137 by a hook and not a wrapping store.** The issue allowed either.
    The hook runs the same clause on both backends.

## 6. Mixed builds and rollback

Nothing durable changes. An earlier build answers a damaged claim as before.

## 7. Tests

**#137.** `retention_leaves_a_claim_that_moves_between_its_two_reads`, a
conformance clause run on both backends. Three claims move between the
query and the sweep's read of their history: a disputed claim is retracted,
a disputed claim is deleted, and a proposed claim is promoted. Each is
counted `moved`, none `damaged`, and no delete is attempted. A later pass
ages the retracted claim by its new state.

**#138, the store** (`postgres_damaged_history_proofs.rs`, sixteen proofs
against a live database, each damaging rows with a statement of its own):

- Five shapes of damage are refused `claim-history-damaged`, with nothing
  written: the last transition gone, an earlier one gone, one too many, the
  right number of the wrong ones, and a transition stored for a claim that
  counts none.
- The right ordinals are not enough: a row that is another claim's
  transition of the same ordinal, the claim's own transition of another
  ordinal, and an unreadable row are each refused, and the other claim is
  untouched. Nor is the right record under an ordinal that is not its own.
- A row from a newer schema is refused with the schema's code, in an
  opted-in store.
- **A transition between an erasure's two reads is a race**, without a
  pause. The transition table is locked; the erasure's first read answers
  and its history read waits; the lock's holder lands a transition and
  commits. The proof goes on only once the erasure's session is seen
  waiting for a lock. The erasure reads again and erases the claim.
- The sweep counts four damaged claims, deletes the healthy one behind
  them, and counts the four again on the next pass.
- An opted-in store deletes a damaged claim, clears the stored result of
  the transition with no row, and leaves a neighbour's stored result that
  mentions the claim's id.
- An opted-in store clears no stored result that a stray row names and that
  is another claim's; that claim's operation still answers.
- An opted-in store tombstones a damaged claim and deletes the stub; a store
  that did not opt in refuses the stub's delete.
- **A transition after the opted-in erasure read the ledger is not missed.**
  The transition is applied and not committed; the erasure's statement waits
  on the claim row; on the commit the fence stops it, and the next attempt
  clears the new transition's stored result too.
- An unreadable stored result stops the opted-in erasure, by the name of its
  operation, and nothing is written.
- The sweep through an opted-in store deletes two damaged claims whose
  history ends where it should, and only counts four: a missing last
  transition, and a claim, a stub and a proposed claim with entries behind
  their history.
- Three shared erasure clauses pass through an opted-in store.

**#138, the sweep** (`retention_damaged_history.rs`, eight tests on the
in-memory store behind a store that answers as a damaged backend does). Each
gives its roles out in claim-id order, so that the damaged claim is the one
the sweep reaches first:

- the last transition gone; an entry behind the last; a transition of
  another ordinal where the last should be; a delete refused as damaged;
- a due proposed claim with a stored transition, and a due stub with an
  entry behind its history: counted, and no delete asked for;
- damage the sweep does not meet is not reported: a claim with no window,
  and one not yet due;
- a pass bounded to one claim a call resumes past damaged claims.

**Unit tests.** `history_state` over whole, moved and damaged reads, rows
that are not the claim's own among them; `recorded_transition`;
`outcome_claim_id`; the new code's stability.

**Watched failing.** The first version's tests were watched failing before
its code. Five database proofs failed on the store as it was: three with
"lost 8 compare-and-set races; retry", one with an erasure that went
through, and the sweep's with the pass ending on that error. The three in
which the opt-in erases failed with the option present and doing nothing.
Two sweep tests failed on the in-memory store. The tests added after the
review were written with the code they hold and were not watched failing
first. The reviewer had reproduced each defect on the first version, and
each test is held by a break below that puts the earlier behaviour back.

**Breaks.** Thirty breaks were run one at a time against both crates, the
PostgreSQL crate against a live database: twelve of the sweep and eighteen
of the store. Each is caught. One survived its first run, a row's own
ordinal column not being compared: no test stored a right record under a
wrong ordinal. A unit case and a database case were added, and it is caught.

## 8. Not done

- Reads do not look for damage. `get`, `query` and `transitions` answer what
  is stored.
- Nothing repairs a damaged history.
- A stored result an earlier build left behind, when it erased a claim with
  the right number of wrong rows, is still in the ledger. Nothing in the
  store finds it: the claim that would name it is gone.
- A trust transition of a damaged claim was not examined.
- The opted-in erasure's ledger read is one statement over the scope's
  transition rows. It is not paged, and it runs again on each attempt.

## 9. Review

A fresh reviewer read the first version, with probes against a live database
and seventeen breaks of its own. The rule that tells damage from a race held
under everything it ran: 2,300 rounds of concurrent transitions, erasures and
sweeps on one claim gave no `claim-history-damaged`, and the same rounds with
the fence comparison removed gave hundreds. What it found:

- **The opted-in erasure cleared another claim's stored result.** It took an
  operation's name from every stored row, and a stray row copied from
  another claim named that claim's operation. A damaged history is now
  asked for no names (section 3), and a history is whole only when every
  row is the claim's own (call 5), which closes the same hole in a store
  that did not opt in.
- **Through an opted-in store the sweep deleted damaged claims whose history
  it never read**, a proposed claim and a stub, against what my own call
  said. Call 6 is now true: the sweep reads the end of the history of every
  claim it deletes.
- **An unreadable row answered `claim-backend-failed`, and ended the sweep's
  pass**, which is the failure the issue is about. It is damage now
  (call 8).
- **Two of my breaks' accounts were wrong.** I wrote that the fence
  comparison was held by a unit test alone and that no test landed a
  transition between the erasure's reads. An existing race proof does, about
  one run in eleven. The reviewer showed how to hold it every time, and
  that proof is now in the file.
- **The sweep's tests depended on the order of ids that differ on every
  run**, so "the healthy claim behind it" held on about half the runs. The
  roles are now given out in claim-id order.
- Two breaks survived: the damaged path naming stored rows, which was the
  first defect above, and the sweep taking any one entry for the claim's
  last. Both are caught now.
- "Counted on every pass" was false: the sweep is no scan (call 3). Nine
  more sentences of the docs were wrong or loose, and are corrected, among
  them a name for the conformance entry point that does not exist.
- The refusal for an unreadable stored result now names its operation, and
  two redundant rustdoc links are gone.
