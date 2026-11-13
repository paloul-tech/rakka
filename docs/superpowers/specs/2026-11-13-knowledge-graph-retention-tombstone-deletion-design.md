# Knowledge-graph retention, tombstone, and deletion (issue #68)

Status: design, awaiting the owner's approval. Branch `knowledge-graph-retention`, off main `d12b0f8`.

## 1. Intent

Specification 13.1 says every memory API must provide retention, tombstone, and deletion semantics. The communal knowledge graph provides none, on any backend. The security matrix records the row as "None — owed".

This was academic until a consumer wrote claims. The host's A9 slice now issues `AppendClaim` from its memory sweep, so a tenant's knowledge space fills and nothing can remove a claim: not for retention, not for an erasure request.

This design adds three things to the graph:

- a **tombstone**, which withdraws one claim's content and keeps an auditable stub;
- a **hard delete**, which removes one claim and cannot be undone by replaying an earlier write;
- a **retention sweep** per knowledge space, with a window per trust status.

All three are proven by conformance clauses that the PostgreSQL backend runs unchanged.

## 2. The code at `d12b0f8`, verified

Paths are under `crates/rakka-agent-knowledge-graph/src/` unless stated. Each fact was read on 2026-11-13.

### 2.1 The store

- `KnowledgeGraphStore` (`store.rs:655`) has six operations: `append`, `get`, `query`, `traverse`, `transition`, `transitions`. None removes or withdraws anything.
- Three types implement the trait: `InMemoryKnowledgeGraphStore` (`store.rs:792`), `PostgresKnowledgeGraphStore` (`rakka-agent-knowledge-graph-postgres/src/lib.rs:424`), and the test double `ShallowKnowledgeGraphStore` (`tests/knowledge_graph_conformance.rs:125`). The host implements it nowhere.
- No read takes a `now` argument.
- Appends and transitions are deduplicated by operation id. The replay ledger stores each operation's original result: the full claim for an append, the full outcome (claim plus transition) for a transition (`ClaimLedgerEntry`, `store.rs`).
- `ClaimFilter` exposes no field accessors. Both backends apply `ClaimFilter::admits` (`store.rs:278`) to each decoded claim, so a rule added to `admits` holds on every backend.

### 2.2 The claim

- A `Claim` (`claim.rs:486`) carries a statement (`subject`, `predicate`, `object`), a `content_digest` of that statement, provenance, evidence artifact references, `created_at`, confidence, classification, access capabilities, trust status, `transition_count`, and optional embedding and policy references.
- `ClaimObject` is `Node(ClaimNodeId)` for an edge or `Value(AgentTaskContent)` for an attribute. An edge claim has no value: its whole content is its subject, predicate and object node.
- `Claim::validate` (`claim.rs:725`) re-derives `content_digest` from the statement and refuses a mismatch with `claim-statement-digest-mismatch`.
- `ClaimId::derive_appended` (`claim.rs:249`) derives the claim id from the scope and the append's operation id. The same operation always produces the same claim id.
- Trust moves only through `Claim::apply_transition`, which increments `transition_count` on every move. The count is capped at 32 (`CLAIM_MAX_TRUST_TRANSITIONS`).
- A claim does not record when it entered its current trust status. Each `ClaimTrustTransition` (`transition.rs:46`) records `occurred_at`, and a transition's `ordinal` equals the claim's `transition_count` after it.
- `CURRENT_CLAIM_SCHEMA_VERSION` is 1. `check_schema_window` (`error.rs:63`) accepts the current version and the one before it.
- A transition record carries a free-text `reason` of up to 512 bytes and its own evidence references.

### 2.3 The PostgreSQL backend

Paths are under `crates/rakka-agent-knowledge-graph-postgres/src/lib.rs`.

- Three tables (`:136`–`:172`): `rakka_agent_knowledge_claim`, `rakka_agent_knowledge_claim_transition`, `rakka_agent_knowledge_claim_op`.
- The claim's `record` column is authoritative. `subject`, `predicate`, `object_node`, `trust` and `transition_count` are copies kept beside it for traversal and for the compare-and-set fence. `subject` and `predicate` are `NOT NULL`.
- The ledger table's `kind` column has a `CHECK` allowing `'append'` and `'transition'`. Its `result` column is `NOT NULL`. It has no `claim_id` column.
- `ledger_hit` (`:345`) treats any `kind` other than `'transition'` as an append, and reads `result` as non-null bytes.
- Every write is one data-modifying CTE statement. The store never issues `BEGIN` or `COMMIT`.
- `query` scans one scope in claim-id order and applies `admits` in Rust. Its cost is linear in the scope's claims whatever the filter.
- `migrate` (`:316`) applies `CREATE ... IF NOT EXISTS` statements under an advisory lock. It has never altered an existing table.

### 2.4 The private tier, which is the model

Paths are under `crates/rakka-agent/src/`.

- `AgentPrivateMemoryStore` (`memory.rs`) has `tombstone`, `delete` and `purge_expired`.
- A tombstoned memory keeps its identity, digest and provenance. Its content becomes `AgentPrivateMemory::tombstone_content()`, a null marker. Validation refuses a tombstone that carries content, with `memory-tombstone-content`.
- A tombstone and a delete both erase the ledger payloads of the memory's earlier operations. A replay of an erased operation fails with `memory-operation-erased`. A delete records its own content-free success marker, so a replayed delete answers success.
- `get` returns a tombstoned stub. `list` excludes stubs unless the cursor opts in.
- `SessionRetentionPolicy` (`memory.rs:623`) is deployment configuration passed to each call, never stored per row. It carries a window and a legal-hold flag.
- `AgentMemoryRetentionSweep` (`memory_retention.rs:233`) is bounded and deployment-invoked. There is no resident sweeper. It reports what it skipped as values, and returns a store failure as an error.
- `memory_retention.rs` states that erasing a private memory does not reach a context snapshot that embedded it, and that Rakka keeps no reverse index from memory to snapshot.

### 2.5 Copies of a claim outside the graph

- The run's `ClaimAppend` effect request (`AgentClaimAppendRequest`, `effect.rs:1355`) carries the subject, predicate, object and evidence. It is held on the run record as `AgentRunEffect::request` (`effect.rs:1785`).
- `KnowledgeGraphClaimAppendExecutor` (`append_executor.rs`) treats every store error except `ClaimError::Backend` as a definitive refusal.
- `MemoryContextSnapshot::communal_claims` is an empty placeholder. No snapshot embeds a claim today. Issue #69 will change that.
- No store method performs semantic search. `KnowledgeGraphCapability::SemanticSearch` is declared and unused, so there is no derived index to scrub.

## 3. Goals, non-goals, anti-goals

### Goals

1. A caller can withdraw one claim's content and keep a stub that shows the withdrawal.
2. A caller can remove one claim entirely.
3. After either, no replayed operation returns or restores the withdrawn content.
4. A deployment can age claims out of a knowledge space, with a separate window for each trust status.
5. Every backend passes the same clauses for all of the above.

### Non-goals

- **No agent-facing path.** No run effect, command or tool tombstones or deletes a claim. These are operations an application calls on the store.
- **No bulk erasure operation.** "Erase everything about node X" is a `query` by subject and by object node, then a `delete` per claim. The application composes it.
- **No whole-space drop.**
- **No reverse index** from a claim to the run records or snapshots that hold a copy of it.
- **No "status since" stamp on the claim** (approach B in section 5).
- **No change to `MemoryContextSnapshot::communal_claims`.** That is issue #69.
- **No space registry.** The application names the spaces it sweeps.

### Anti-goals

Each of these would make the work a failure even if it ran:

- A replayed append restores a deleted or tombstoned claim's content.
- The sweep deletes a claim that is not due, or one whose trust status moved while the sweep was looking at it.
- A default that nobody chose deletes shared knowledge.
- A tombstone or delete under the wrong scope reveals that a claim exists.
- A tombstone leaves the statement's bytes in any column or ledger row of the backend.
- A binary from before this change misreads a stub as a live claim, or writes a statement back over one.

## 4. Constraints

- **Specification 13.4.** Verification, dispute and retraction must preserve original provenance. A tombstone keeps provenance and the transition history. Only a delete removes them.
- **Specification 13.6.** The store trait and its records expose no vendor type, and a backend stays small. Policy belongs in one place, not in each backend.
- **Durable-record rule.** Adding a `#[serde(default)]` field is safe across the N/N+1 window. Changing a field's type is not.
- **PostgreSQL atomicity.** A tombstone or delete is one statement, like every other write in that backend.
- **Scope isolation.** A wrong-scope write answers `claim-not-found`, exactly as an absent claim does.
- **Stable codes.** Every new refusal has a stable kebab-case code, and the codes are a compatibility commitment.
- **No resident task.** The sweep is called by the application on its own schedule.

## 5. Approaches

Three ways to build the retention sweep were considered. The tombstone and delete operations are the same in all three.

**A. Two store operations, and one sweep in the domain crate.** The sweep pages the existing `query`, reads a claim's last transition time through the existing `transitions`, and calls `delete`. It costs one extra bounded read per candidate that has left `Proposed`. The policy logic exists once, and a backend implements two small operations.

**B. A, plus a "status since" stamp on the claim.** Each transition stamps the time on the claim, and a new filter selects by it. This saves the per-candidate read. It adds a second durable field, and claims stored before the change lack the stamp, so the sweep needs A's read as a fallback anyway.

**C. A bulk `purge_due` on the store,** in the shape of the private tier's `purge_expired`. PostgreSQL could do it in one statement. Every backend would re-implement the policy, the conformance suite would have to prove the policy per backend, and it needs B's stamp as a column.

**The owner chose A on 2026-11-13.** It keeps backends small, which specification 13.6 asks for, and the policy has one implementation and one proof. B can be added later without changing A's contract if sweep cost shows up.

The owner also chose, the same day, a **window per trust status** over a single window per space. One window would age verified knowledge out on the same clock as unreviewed claims.

## 6. Design

Names in this section are proposals. The builder may rename a type or method and records the final names in section 11.

### 6.1 The tombstone

#### 6.1.1 What a tombstone clears and keeps

| Cleared | Kept |
| --- | --- |
| `subject`, `predicate`, `object` | `claim_id`, `operation_id` |
| `evidence` | `provenance` |
| `embedding` | `created_at`, `confidence_bps`, `classification`, `access` |
| | `content_digest` (of the original statement) |
| | trust status, `transition_count`, and every transition record |
| | `policy`, `policy_revision` |

The whole statement is cleared, not only an attribute value. For an edge claim the subject, predicate and object node are the content, so a tombstone that kept them would clear nothing.

`content_digest` stays, as it does in the private tier. It lets an operator show that two claims made the same statement. It also lets someone who already knows a statement confirm it against the stub. A caller who cannot accept that uses `delete`. (Author's call.)

Transition records stay whole, including each one's free-text `reason` and evidence. Issue #68 asks for the transition history to be kept. A reason that quotes the statement survives a tombstone and is removed only by a delete. The docs say so.

#### 6.1.2 The stub's durable shape

`Claim` and `ClaimRecord` gain one field:

```rust
#[serde(default)]
pub tombstone: Option<ClaimTombstone>,

pub struct ClaimTombstone {
    pub operation_id: ClaimOperationId,
    pub reason: ClaimTombstoneReason,
    pub actor: PrincipalRef,
    pub tombstoned_at: AgentTimestampMillis,
}

#[non_exhaustive]
pub enum ClaimTombstoneReason {
    /// Its asserter or an operator withdrew the content.
    Withdrawn,
    /// A retention or classification policy removed the content.
    Policy,
}
```

The statement fields keep their types. On a stub they hold fixed markers: a reserved marker value for `subject` and for `predicate`, and `ClaimObject::Value` of the null content marker the private tier uses. `evidence` is empty and `embedding` is `None`. This follows the private tier's `tombstone_content()` and avoids changing a field's type.

`Claim::validate` changes in two ways when `tombstone` is set:

- It requires the statement to be exactly the markers, `evidence` to be empty and `embedding` to be `None`. A stub that carries content is refused with `claim-tombstone-content`.
- It does not re-derive `content_digest`, because the digest describes the withdrawn statement.

The actor is recorded because the graph is communal. In the private tier the owner is implied by the scope; here the withdrawer is not. (Author's call.)

`ClaimTombstoneReason` has no `Retracted` or `Superseded` member. `Retracted` is a trust status in this crate, and superseding is done by a new claim that references the old one.

#### 6.1.3 Schema version

`CURRENT_CLAIM_SCHEMA_VERSION` becomes 2. The window then accepts versions 1 and 2.

A claim is written at version 2 only when it carries a tombstone. A live claim is still written at version 1, because it holds nothing a version 1 reader cannot read. This is the staged pattern of issues #110 and #119.

A binary from before this change that meets a stub refuses it with `schema-version-ahead`. It never reads the markers as a statement.

The transition record's version does not change.

One consequence for a later change: because live claims stay at version 1, a future bump to version 3 would put every live claim outside the default window. Whoever makes that bump must move live claims forward or widen the window. The compatibility doc records this.

#### 6.1.4 The operation

```rust
pub struct ClaimTombstoneRequest {
    pub claim_id: ClaimId,
    pub operation_id: ClaimOperationId,
    pub reason: ClaimTombstoneReason,
    pub actor: PrincipalRef,
    pub tombstoned_at: AgentTimestampMillis,
}

fn tombstone<'a>(
    &'a self,
    scope: &'a KnowledgeSpaceScope,
    request: &'a ClaimTombstoneRequest,
) -> ClaimFuture<'a, Claim>;
```

It returns the stub. Its rules:

- It is idempotent on `operation_id`. A replay returns the stub as first written.
- An absent or out-of-scope claim answers `claim-not-found`.
- A claim already tombstoned under a different operation answers `claim-tombstoned`.
- It erases the ledger payloads of the claim's append and of every earlier transition (section 6.3).
- It does not change the trust status or `transition_count`. A tombstone is a statement about content, not a verdict on truth.
- Any trust status can be tombstoned.

After a tombstone, `transition` on that claim answers `claim-tombstoned`. A stub's trust status is frozen.

#### 6.1.5 Reads

- `get` returns the stub. The withdrawal stays visible to anyone authorized for the space.
- `query` excludes stubs by default. `ClaimFilter` gains an opt-in (`including_tombstoned()`), and the default exclusion lives in `ClaimFilter::admits`, so both backends get it from one place.
- A stub can match only the filter's provenance, trust, classification, confidence and time axes. It never matches a subject, predicate or object filter.
- `traverse` never follows or reports a stub. A stub's object is a value, so it is not an edge.
- `transitions` still lists a stub's history.
- `KnowledgeGraphGoalClaimSource` uses `query` with the default, so a goal view does not list stubs.

### 6.2 The delete

```rust
pub struct ClaimDeleteRequest {
    pub claim_id: ClaimId,
    pub operation_id: ClaimOperationId,
    /// When set, the delete applies only if the claim is still in the state
    /// the caller read.
    pub expected: Option<ClaimDeleteExpectation>,
}

pub struct ClaimDeleteExpectation {
    pub transition_count: u32,
    pub tombstoned: bool,
}

fn delete<'a>(
    &'a self,
    scope: &'a KnowledgeSpaceScope,
    request: &'a ClaimDeleteRequest,
) -> ClaimFuture<'a, ()>;
```

Its rules:

- It removes the claim row and every transition row of the claim.
- It erases the ledger payloads of the claim's append, of every transition, and of a tombstone if there was one (section 6.3).
- It records its own content-free marker. A replayed delete answers success.
- An absent or out-of-scope claim answers `claim-not-found`.
- A stub can be deleted.
- When `expected` is set and either half does not match, the delete answers `claim-delete-stale` and changes nothing. The refusal is not recorded in the ledger, so the same operation id can be tried again later. The sweep relies on this (section 6.4.4).
- The expectation covers the tombstone state as well as the count because a tombstone does not change `transition_count`. Without the second half, a sweep that read a live claim as due could delete the stub an operator made of it a moment later, with the provenance and history the operator meant to keep. (Author's call, from the reviewer's first question.)

After a delete, `get` answers `None`, `transitions` answers the empty page, and the claim id is never produced again, because it derives from an operation id whose ledger row now says "erased".

The request carries no actor. A delete leaves nothing behind to record one on, and who erased what belongs in the application's own audit log.

Both operations are required methods of `KnowledgeGraphStore`, with no default body and no capability flag. Specification 13.1 makes them mandatory for every memory API, so a backend must not be able to omit them silently. This is a source-breaking change for any implementor outside this repository; none is known.

### 6.3 The ledger

The ledger gains three kinds of entry beside "applied append" and "applied transition":

| Entry | Written by | A replay of that operation answers |
| --- | --- | --- |
| applied tombstone | `tombstone` | the stub |
| deleted | `delete`, for its own operation | success |
| erased | `tombstone` and `delete`, over the claim's earlier operations | `claim-operation-erased` |

An erased entry keeps the operation id and holds no payload. It is what stops a replayed append from restoring a claim: the store finds the entry before it would write anything.

The operations to erase are found from the claim itself. The append's operation id is on the claim, each transition's is on its transition record, and a tombstone's is on the stub. There are at most 34.

An operation id reused across kinds still answers `claim-operation-conflict`, and the new kinds join that rule. For example, a delete under an id that an append already spent is a conflict.

Erased and deleted entries are never removed. Each is one small row with no content. The docs record this as an accepted cost.

The append executor needs no change. `claim-operation-erased` is not a backend error, so a replayed append of an erased claim is a definitive refusal, and specification 13.4 already says a definitively failed append does not end the run.

### 6.4 The retention sweep

#### 6.4.1 The policy

```rust
pub struct KnowledgeSpaceRetentionPolicy { /* private */ }

impl KnowledgeSpaceRetentionPolicy {
    /// Nothing ages out.
    pub const fn retain_all() -> Self;
    /// Claims resting in `status` age out `millis` after they entered it.
    pub fn with_window(self, status: ClaimTrustStatus, millis: u64) -> Self;
    /// Stubs age out `millis` after they were tombstoned.
    pub const fn with_tombstone_window(self, millis: u64) -> Self;
    /// A held space survives every sweep.
    pub const fn with_legal_hold(self, hold: bool) -> Self;
}
```

- The policy is deployment configuration, passed to each call and never stored per claim. It can tighten or loosen at any time.
- **There is no built-in window.** `Default` is `retain_all()`. The session tier has a 30-day default because its specification names one; nothing names one for shared knowledge, and deleting it by a default nobody chose is an anti-goal. (Author's call.)
- A trust status with no window is retained. `ClaimTrustStatus` is non-exhaustive, so a status added later is retained until a deployment sets a window for it.
- The tombstone window covers stubs of any trust status. Without it a stub lives until someone deletes it. (Author's call; the owner can strike it.)

#### 6.4.2 The clock

| The claim is | It entered that state at |
| --- | --- |
| a stub | `tombstone.tombstoned_at` |
| `Proposed` | `created_at` |
| in any other status | `occurred_at` of its last transition |

A claim is due when that instant plus the window is at or before `now`. A stub is judged only by the tombstone window, never by its trust status.

The last transition is the one whose `ordinal` equals the claim's `transition_count`. The sweep reads it through `transitions`. If the listing's last ordinal differs from the count the sweep holds, the claim moved under the sweep; it is counted as moved and left alone.

#### 6.4.3 The sweep

```rust
pub struct KnowledgeSpaceRetentionSweep { /* store, max_batch */ }

impl KnowledgeSpaceRetentionSweep {
    pub fn new(store: Arc<dyn KnowledgeGraphStore>) -> Self;
    pub fn with_max_batch(self, max_batch: usize) -> Self;

    pub async fn discharge(
        &self,
        scope: &KnowledgeSpaceScope,
        policy: &KnowledgeSpaceRetentionPolicy,
        now: AgentTimestampMillis,
        cursor: Option<ClaimCursor>,
    ) -> ClaimResult<KnowledgeSpaceRetentionReport>;
}

#[non_exhaustive]
pub struct KnowledgeSpaceRetentionReport {
    pub examined: usize,
    pub deleted: usize,
    pub not_yet_due: usize,
    pub retained: usize,   // no window applies
    pub moved: usize,      // changed under the sweep
    pub held: bool,
    pub next: Option<ClaimCursor>,
}
```

- One call examines at most `max_batch` claims of one space and returns a cursor when more remain. The application calls again until `next` is `None`.
- The cursor is only a position. Starting again from the beginning is always safe.
- Under a legal hold the sweep reads nothing, deletes nothing and reports `held`.
- A store failure is returned as an error. Claims already deleted in that call stay deleted, and the next call carries on.
- The sweep lives in the domain crate and holds only an `Arc<dyn KnowledgeGraphStore>`, so it runs unchanged over any backend.
- The application enumerates its spaces. Rakka keeps no registry of them.

The legal hold governs the sweep only. An explicit `tombstone` or `delete` takes no policy and is not blocked by a hold, as in the private tier. (Author's call.)

#### 6.4.4 How the sweep deletes

For each due claim the sweep calls `delete` with:

- an operation id derived from the scope and the claim id, so a re-drive converges on one delete;
- `expected` set to the transition count and the tombstone state it read.

If the claim's trust moved, or the claim was tombstoned, between the read and the delete, the store answers `claim-delete-stale`. The sweep counts the claim as moved and goes on. The refusal spends nothing, so a later pass can delete the claim under the same derived id once it is due in its new status.

#### 6.4.5 Cost

Each pass walks the whole space on PostgreSQL, because `query` already filters after decoding rows, and makes one bounded `transitions` read per candidate that has left `Proposed` and has a window. This is recorded as an accepted cost. Approach B is the remedy if it matters.

### 6.5 The PostgreSQL backend

- `migrate` gains its first changes to existing tables: a `tombstoned BOOLEAN NOT NULL DEFAULT FALSE` column on the claim table, and a wider `CHECK` on the ledger's `kind`. Both are applied idempotently under the existing advisory lock.
- The new column and the wider check must not disturb a binary from before this change that is still appending. The builder confirms this against the existing insert statements.
- A stub's row holds the markers in `subject` and `predicate`, `NULL` in `object_node`, and `tombstoned = TRUE`. These columns must agree with the record, as the existing drift check requires. A row whose `tombstoned` column disagrees with its record is refused as drift.
- **A stub's `transition_count` column holds a reserved fence value, not the count.** The value is above `CLAIM_MAX_TRUST_TRANSITIONS`, so no reader can hold it as an expectation. The true count stays in the record. This is the one stated exception to "the columns agree with the record", and the drift check knows it. The owner chose this on 2026-11-13.
  - The reason is the transition statement at `d12b0f8` (`TRANSITION_SQL`, `:205`). It fences only on `transition_count`, and a tombstone does not change the count. A binary from before this change that read a live claim just before a tombstone committed would still match the stub row, write the full statement back, and add a transition row and a ledger row that embeds the claim.
  - With the fence value the old update matches nothing. On its retry the old binary reads the stub and refuses it with `schema-version-ahead`.
  - Statements in the new build fence a stub row on `tombstoned` and the fence value. A delete's `expected.transition_count` is still compared with the record's count.
  - The in-memory store needs no equivalent: it has no second binary.
- An erased ledger row keeps `result NOT NULL` and holds empty bytes, with a kind that says erased. A binary from before this change that replays such an operation answers `claim-operation-conflict`: `replayed_append` (`:868`) refuses any kind but `'append'`, and a replayed transition reaches the same refusal through `ledger_hit`. It does not panic on a null, and it does not append again because the row is still there. One path differs for one attempt: the append race fallback (`:483`) decodes the empty bytes and answers `claim-backend-failed`, which the executor retries, and the retry answers the conflict.
- `tombstone` and `delete` are each one statement.

### 6.6 What erasure does not reach

The docs and the sweep's module header state these plainly, as `memory_retention.rs` does for the private tier:

- **The run record.** The `ClaimAppend` effect request on the run that asked for the append keeps the subject, predicate, object and evidence references. Rakka does not purge run records.
- **Session memory.** A tool result the claim was mapped from stays in the run's session memory until that run's retention is discharged.
- **Evidence artifacts.** The store holds references only. The artifacts are the application's.
- **Transition reasons,** after a tombstone (section 6.1.1).
- **Context snapshots,** once issue #69 embeds claims in them. Issue #69 must then follow the rule `memory_retention.rs` already sets for private memory: the snapshot keeps its copy until the run's snapshot purge, and Rakka keeps no reverse index.

Erasing a subject's content therefore means deleting the claims and also discharging the retention of the runs that produced them.

### 6.7 Error codes

| Code | Meaning |
| --- | --- |
| `claim-tombstoned` | The claim is a stub: it cannot transition or be tombstoned again. |
| `claim-operation-erased` | The replayed operation's payload was erased by a later tombstone or delete. |
| `claim-tombstone-content` | A record marked tombstoned still carries a statement, evidence or an embedding. |
| `claim-delete-stale` | The claim is no longer in the state the delete expected: its transition count or its tombstone state differs. |

## 7. Rollout and compatibility

1. **Upgrade every binary that reads the graph.** That is each dispatcher that runs the append executor, anything that serves the goal view, and the application's own sweeps. During the roll nothing on disk changes: appends are still version 1 and no ledger row has a new kind.
2. **Run `migrate` from the new build.**
3. **Only then call `tombstone`, `delete` or the sweep.**

A binary that was missed fails closed on a read and on a replay. A stub answers it `schema-version-ahead`, which fails the `query` page that contains the stub. A replay of an erased operation answers it `claim-operation-conflict`. Neither is misread, and neither restores content.

A missed binary also cannot write over a stub. Its transition statement fences on a count the stub's row no longer holds (section 6.5), so a transition it had in flight when the tombstone committed matches nothing. A transition against a deleted claim matches nothing either, and its retry answers `claim-not-found`.

The host needs one pin bump. It implements `KnowledgeGraphStore` nowhere, so the two new required methods break nothing there. It gains four codes to classify and a sweep to schedule.

## 8. Proofs

Each clause is falsified once: the rule it protects is removed, the clause is seen to fail, and the rule is restored.

### 8.1 Conformance clauses

Added to `conformance.rs` and to `check_knowledge_graph_contract`, so the in-memory store, the shallow test double and PostgreSQL all run them.

1. **Tombstone withdraws content.** The stub keeps id, provenance, classification, access, trust and digest; the statement, evidence and embedding are gone; `get` returns the stub; `transitions` still lists the history.
2. **A stub is hidden from discovery.** `query` excludes it by default and includes it on opt-in; a subject, predicate or object filter never matches it; `traverse` does not follow an edge that was tombstoned.
3. **A stub is frozen.** `transition` and a second tombstone under a new operation both answer `claim-tombstoned`.
4. **Tombstone replay.** The tombstone's own replay returns the stub; a replay of the append and of an earlier transition answers `claim-operation-erased`.
5. **Delete erases.** `get` answers `None`; `transitions` is empty; a stub can be deleted.
6. **Delete cannot be undone.** A replay of the append answers `claim-operation-erased` and the claim stays absent; the delete's own replay answers success.
7. **Stale delete.** A mismatched transition count, and separately a mismatched tombstone state, each answer `claim-delete-stale` and change nothing; the same operation id succeeds once the expectation matches.
8. **Erasure isolation.** A tombstone or delete under a foreign scope answers `claim-not-found`, identical to an absent claim, and leaves the other scope's claim untouched.
9. **Operation kinds.** An operation id reused across append, transition, tombstone and delete answers `claim-operation-conflict`.
10. **Retention sweep.** One clause, or several, covering: a window per status; the clock read from the last transition; a claim not yet due left alone; a status with no window retained; the tombstone window; the legal hold; a claim whose trust moves between read and delete counted as moved and kept; a claim tombstoned between read and delete counted as moved, its stub kept; a re-driven pass converging; the batch bound and the cursor.

### 8.2 Domain tests

- A stub that carries content fails `Claim::restore` with `claim-tombstone-content`.
- A stub round-trips at version 2; a live claim is written at version 1; a version 1 record with no `tombstone` field restores.
- The append executor answers a replayed append of an erased claim as a definitive refusal carrying `claim-operation-erased`.
- `KnowledgeGraphGoalClaimSource` does not list a stub.

### 8.3 PostgreSQL tests

- **Migration from the earlier schema.** Tables created by the schema at `d12b0f8`, holding rows, migrate cleanly; a second run changes nothing; two concurrent migrators both succeed.
- **No bytes left.** After a tombstone, and again after a delete, no row of the three tables in that scope contains the statement's subject, predicate or object bytes, in any column or payload.
- **Atomicity.** The statement count per operation is one.
- **Columns agree with the record** for a stub row, except `transition_count`, which holds the fence value. A row whose `tombstoned` column disagrees with its record is refused as drift.
- **An earlier binary cannot write over a stub.** The transition statement text of `d12b0f8`, bound to the count the claim had before the tombstone, is run against the stub row and applies nothing: the record, the transition table and the ledger are unchanged. Falsified by leaving the true count in the column.
- **An earlier binary's replay of an erased operation** answers `claim-operation-conflict`, proven against the `d12b0f8` ledger read.

### 8.4 Compatibility

- `compatibility_currency` holds the `claim` row of `docs/rakka-compatibility.md` at version 2.

## 9. Docs and changelog

- `docs/plans/rakka-agent/spec.md` section 13.4: a paragraph on tombstone, delete and retention.
- `docs/rakka-agent-security-validation-matrix.md`: the graph's retention cell, the "Owed" entry for #68, and the attack rows for tombstone/delete erasure and retention purge.
- `docs/rakka-agents.md`: the knowledge-graph passage, the roll order, and what erasure does not reach.
- `docs/rakka-compatibility.md`: the `claim` version row, the four codes, the staged-version note and its consequence for a later bump, and the two new required trait methods.
- `docs/plans/rakka-agent/implementation-plan.md`: the "Owed onward" entry for #68.
- `CHANGELOG.md`.

## 10. Left to the builder

- Final names of types, methods, markers and codes, recorded in section 11.
- The marker values for a stub's subject and predicate.
- The reserved fence value for a stub row's `transition_count` column. It must exceed `CLAIM_MAX_TRUST_TRANSITIONS` and satisfy the column's existing `CHECK`.
- How the PostgreSQL store finds a claim's ledger rows in one statement. Two ways are known: read the claim and its transitions, then write one statement fenced on `transition_count` and retry on a lost race; or add a `claim_id` column to the ledger table and backfill it. The first needs no backfill.
- The exact label of the erased ledger kind, and whether the in-memory ledger mirrors it one for one.
- Whether the sweep's proofs are one conformance clause or several.
- The sweep's default batch size, following `AgentMemoryRetentionSweep::DEFAULT_MAX_BATCH`.
- How the sweep walks `transitions` when a backend declares a page smaller than a claim's history.

## 11. After the merge

Post a report on issue #68 for the host, with the merge commit as the pin. It names the roll order of section 7, the four codes, the sweep's call shape, and the list in section 6.6.

## 12. As built

To be written by the builder.
