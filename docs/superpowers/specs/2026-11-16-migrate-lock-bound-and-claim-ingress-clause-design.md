# A bounded migration-lock wait, and a testkit clause for claims at memory ingress

Issues #143 and #144, one pull request. Both are follow-ups the owner asked for when accepting PR #141 (issue #69). They share nothing in code; they ship together because each is small.

Branch `migrate-lock-bound-claim-clause`, off `main` at `6fca814`.

## 1. Intent

- **#143.** A migrator waiting for the knowledge graph's migration lock waits without bound and reports nothing. A caller that put `migrate` under a deadline and kept its connection leaves the lock held, and every other node then hangs at startup with no error. After this change the waiter stops, and its error tells an operator which connection to close.
- **#144.** A memory-ingress stage written for private memory can allow every communal claim, because it is handed a statement object where it expected a string. Today only a paragraph of documentation guards that. After this change a deployment can run its own chain against a claim, in the exact shape assembly uses, and see what assembly would do with it.

## 2. Goals, non-goals, anti-goals

**Goals**

- `migrate` gives up after a bound, with an error that names the session holding the lock.
- The bound defaults to 5 minutes and a caller can set its own. *(Owner's decision, 2026-11-16.)*
- A testkit clause that evaluates one claim at the memory-ingress boundary and answers what assembly would do with it, built from the same code assembly runs.
- A builder for a claim whose inline value carries text a deployment chooses.

**Non-goals**

- Removing the leak itself. A cancelled `migrate` still leaves its lock on the connection; that needs `migrate` to own a connection, which changes its signature (#143 says so).
- The other PostgreSQL adapters. `rakka-agent-postgres` and `rakka-persistence-postgres` block inside `pg_advisory_lock` without a bound. They build no concurrent index and are not this issue.
- A chain for claims separate from the dispatch authority's, or any change to what a stage is handed.
- #142 and #140.

**Anti-goals**

- A waiter that blocks inside `pg_advisory_lock`, or holds a snapshot while it waits. The lock stays tried.
- An error message that carries query text, an application name, a client address or a connection string.
- Any statement, in code or documents, that the clause proves a deployment's chain is safe. It attests nothing: a deployment that never runs it is where it is today.
- A clause that builds the evaluated content its own way, so that it can drift from assembly.

## 3. Approaches

**#143: how the bound is set**

| Approach | Trade-off |
|---|---|
| **A default of 5 minutes, and a caller can set its own** (chosen by the owner) | The silent hang ends for every caller. A healthy node that starts while a first migration builds indexes for longer than the bound now gets an error and succeeds on a retry. |
| No default; a caller opts in | Nothing changes for existing callers, and the silent hang stays the default. |
| A fixed bound | One less knob. A deployment with a very large claim table cannot lengthen it. |

A waiter cannot tell a leaked lock from a healthy long migration, so no bound separates the two. The error reports whether the holder is idle, which is the best signal available.

**#143: where the caller sets it** *(my call)*

| Approach | Trade-off |
|---|---|
| **A builder on the store** (chosen) | `with_migration_lock_wait(Duration)`. `migrate()` keeps its signature, and the sibling adapters already configure their stores with `with_…` builders. |
| A second method, `migrate_within(Duration)` | Two ways to migrate; a caller holding only the store cannot tell which bound applies. |

**#144: where the shared code lives** *(my call)*

| Approach | Trade-off |
|---|---|
| **One function in `communal`, called by assembly and by the clause** (chosen) | The two cannot drift, which the issue requires. Assembly's walk loses about thirty inline lines to a call. |
| The clause re-implements the step | No change to assembly. It drifts the first time assembly changes. |

## 4. Design

### 4.1 The migration lock (`rakka-agent-knowledge-graph-postgres`)

- `pub const DEFAULT_MIGRATION_LOCK_WAIT: Duration`, 300 seconds.
- `PostgresKnowledgeGraphStore::with_migration_lock_wait(self, wait: Duration) -> Self`. The store carries the value; a clone keeps it.
- The lock loop is as it is: try, and pause 50 ms on the server between tries. After a failed try, if the time since the first try has reached the bound, it stops. The lock is always tried at least once, so a bound of zero means "try once".
- Time is measured with `std::time::Instant`. The crate has no async runtime dependency and gains none.
- Before it returns the error, the waiter reads the holder in one statement that ends at once: the `pg_locks` row for this advisory key in this database that is granted, joined to `pg_stat_activity` by pid.
- The error is `ClaimError::Backend` (`claim-backend-failed`). **No new error code.** *(My call: the issue asks to reuse a code where one fits. The cost is that a caller cannot branch on this failure except by its message.)*
- The message, in this shape:

  ```
  migration lock not acquired within 300s: held by backend pid 4242, connected for 1830s, state idle. If no migration is running on that connection, close it.
  ```

  - A field the server withholds reads `unknown`. A role without `pg_read_all_stats` sees only the pid of another role's session.
  - If the holder released the lock between the last try and the read, the message says so and tells the caller to run `migrate` again.
- A waiter that gives up holds nothing and leaves nothing behind.

### 4.2 The claim clause (`rakka-agent`)

- The per-claim boundary step in `communal.rs` (build the context with the claim's id and space, evaluate the statement with the bounded transform size, read the disposition, apply or reject a transform) becomes one crate-private function. Assembly calls it. Its counters and its snapshot are unchanged: a rejected transform still counts as blocked.
- `rakka_agent::testkit` gains the clause: it takes a guardrail chain, the run scope and a `CommunalClaimRecord`, and answers a public value holding the chain's own decision and one of five outcomes:

  | Outcome | What assembly does |
  |---|---|
  | Embedded | Embeds the statement as it is |
  | Embedded, transformed | Embeds the transformed statement, which the outcome carries |
  | Blocked | Drops the claim |
  | Checkpoint refused | Drops the claim: assembly has no checkpoint to park on |
  | Transform rejected | Drops the claim: the transform is not one assembly applies |

- The clause refuses a record that does not validate, with the record's own error, because assembly never evaluates such a record.
- A builder makes a valid record from a subject, a predicate and either inline text or a node, with the digest derived and fixed, documented defaults for the rest. The claim's id and space can be set, because a stage may branch on them.
- What the clause does not cover, stated on it: the checks before the boundary (resolution, trust, classification, capabilities) and the byte budget after it.

## 5. Compatibility

- **Behaviour change:** a `migrate` that waited more than 5 minutes for another migrator now fails with `claim-backend-failed`. A retry succeeds once the holder is done. A deployment whose first migration builds indexes for longer sets a longer wait on the nodes that start during it.
- **Additive:** the constant, the builder, the clause, its outcome type and the claim builder.
- No durable field, no schema change, no new error code, no new metric.

## 6. Proofs

Against PostgreSQL, gated on `RAKKA_POSTGRES_TEST_DSN` like the crate's other proofs:

1. One session holds the lock. A store with a short wait returns the error within the bound plus a margin, the message names that session's backend pid, and it carries none of the forbidden fields.
2. After the first session closes, the same store's `migrate` succeeds.
3. Several migrators starting at once still all succeed (the existing proof, unchanged).

Without a database:

4. A store's default wait is `DEFAULT_MIGRATION_LOCK_WAIT`, and the builder replaces it.
5. A stage that inspects a top-level string allows a claim whose inline value carries the text it looks for. This is the hazard, shown by the clause.
6. A stage that reads the claim's shape blocks the same claim.
7. The stage is handed the memory-ingress boundary, the claim's id and space, and the statement object.
8. A transform that rewrites the inline value is applied, and the outcome carries the rewritten statement.
9. A transform that changes a node, changes the object's kind, or does not decode to a valid statement is rejected.
10. A checkpoint disposition is refused.
11. The same chain and claim give the same embedded statement through real assembly and through the clause.
12. Every existing communal retrieval test passes unchanged.

## 7. Documents

- `migrate`'s "# Do not cancel" section: the leak stays, the wait is bounded, and what the error says.
- `docs/rakka-agent-security-validation-matrix.md`: the "Owed" item shrinks to the leak itself; the "Delegated to the deployment" item names the clause and still says nothing in Rakka checks that a deployment runs it.
- `docs/rakka-compatibility.md`: roll step 1's hazard and roll step 6 of the issue #69 entry.
- `docs/rakka-agents.md`, where it tells a stage to branch on `context.claim`.
- `CHANGELOG.md`.

## 8. Left to the builder

- The names of the clause, its outcome type and the claim builder, and the defaults the builder gives a record.
- The exact holder query, including how the 64-bit key maps to `pg_locks`' two 32-bit columns.
- The short wait and the margin the PostgreSQL proof uses.
- Whether the outcome is an enum beside the decision or a struct that holds both.
