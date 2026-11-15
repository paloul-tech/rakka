# The communal read path into a model context (issue #69)

Status: design, awaiting the owner's approval. Branch `communal-read-path`, off main `86cf087`.

## 1. Intent

A consumer can write communal claims today and no model context ever sees one. The host's memory sweep appends claims from tool results, so a tenant's knowledge space fills, and `MemoryContextSnapshot::communal_claims` is a placeholder that is always empty.

This design makes a run's model context include the communal claims the run is allowed to read. Each selected claim enters the turn's snapshot with its identity, its trust state, its provenance and its exact statement, after four checks:

- the run is granted the claim's knowledge space;
- the run holds every read capability the claim requires;
- the claim passes the retrieval policy (trust state, classification, confidence);
- the claim passes the memory-ingress guardrails.

Rakka's part ends at the snapshot. Rakka's own model adapter does not read a snapshot. The host's prompt builder renders it, and it must render the new claims as untrusted data.

## 2. The code at `86cf087`, verified

Each fact was read on 2026-11-14.

### 2.1 The private path, which is the model

- `assemble_context` (`crates/rakka-agent/src/retrieval.rs:1074`) builds the session window and then, when a retrieval bundle is wired, the private selections.
- The retriever returns a ranking. Every ranked identity is resolved through the authoritative `AgentPrivateMemoryStore` for the addressed scope, and the record that store answers is what the snapshot embeds. An identity the store does not hold is dropped and counted on `RetrievalReport::unverified`.
- Each resolved record is re-checked (`validate`, `query.admits`), then evaluated at `AgentGuardrailBoundary::MemoryIngress`. A blocked record and a checkpoint-required record are both dropped. A transformed record is embedded with the transform recorded.
- The selection is a prefix of the ranking. The walk stops at the first record that would exceed `max_results` or `max_bytes`. It resolves at most `max_resolutions` identities (`max_results` × 4, capped at 64).
- A retriever failure gives an empty selection. A store failure part-way keeps the prefix already verified. Neither fails the assembly.
- `AgentMemoryRetrieval` (`retrieval.rs:607`) requires its guardrail chain and its authoritative store at construction.
- The run calls `assemble_context` from `persist_context_snapshots` (`crates/rakka-agent/src/run.rs:8672`), in its settle pass, once per model effect that has no snapshot yet. The snapshot store is first-writer-wins, so a retry reuses the first snapshot.

### 2.2 The snapshot

- `MemoryContextSnapshot` (`crates/rakka-agent/src/memory.rs:1100`) carries `communal_claims: Vec<MemoryEntryId>`. Its doc comment says the type is a deliberate placeholder to be replaced by this slice.
- `compute_digest` (`memory.rs:1167`) covers `reference`, `session`, `retrievals`, `private_memory`, `communal_claims`, `policy_revision` and `ingress_revision`. It does not cover `budget` or `assembled_at`.
- The snapshot's schema version is 1 (`crates/rakka-agent/src/schema.rs:154`). The comment there says the first reshape after a release must bump it.
- `SnapshotBudget.communal_claims` exists and is always 0.
- The schema-version check on a loaded snapshot is in the PostgreSQL snapshot store only (`crates/rakka-agent-postgres/src/lib.rs:395`). The snapshot's own `Deserialize` (`memory.rs:1209`) and the in-memory store do not check it.

### 2.3 Authority

- `AgentAuthorityEnvelope` (`crates/rakka-agent/src/definition.rs:645`) has `knowledge_spaces`, "may read or contribute to". A setup may only narrow it (`collect_subset_violations`, `definition.rs:775`).
- No type says which capabilities an agent or a run holds. `AgentCapabilityId` appears on tool declarations, on a delegation target's `capability_scopes`, and on granted dispatches. Nothing enforces `capability_scopes`.
- The dispatcher loads the agent's definition and the run's setup for every effect (`AgentEntityAuthority::authorize`, `crates/rakka-agent/src/dispatch.rs:1905`). The run entity does not hold either. In its settle pass it knows only its own delegation envelope (`run.loop_state.delegation_envelope()`).
- `AgentRunDelegationEnvelope.knowledge_spaces` (`crates/rakka-agent/src/delegation.rs:1083`) is a grant: `Some(set)` is the exact set, `Some(empty)` is nothing, and `None` means no statement was recorded. With `None`, a claim append is refused when the run has lineage, and is left to the definition envelope at the root (`run.rs:6603`).
- Knowledge spaces travel a delegation in five `rakka-agent` records: `AgentDelegationTarget` (the catalog's statement, `delegation.rs:266`), `AgentDelegationRecord` (`delegation.rs:573`), `AgentTaskDelegationProvenance` (`delegation.rs:933`), `AgentTaskHandoffRequest` and `AgentTaskHandoff` (`crates/rakka-agent/src/task.rs:2471`, `:2380`), and the run's envelope. The child's set is the catalog's statement intersected with the parent's own grant (`run.rs:3190`). A handoff intersects the same way (`task.rs:5063`).
- Between the sender's record and the receiver's task they cross the wire in two `rakka-a2a` envelopes: `AgentCollaborationMetadata` and `AgentHandoffCollaborationMetadata` (`crates/rakka-a2a/src/agents/collaboration.rs:139`, `:347`). These are the only non-test constructors of `AgentTaskDelegationProvenance` and `AgentTaskHandoffRequest`. Both envelopes refuse unknown fields, and both omit `knowledge-spaces` when empty, so that "a grant must never be silently dropped": an earlier receiver refuses a send that carries one.
- Autonomy admission re-runs the envelope narrowing check against the current definition (`crates/rakka-agent/src/admission.rs:380`). A definition that gains anything the admitted envelope lacks is refused as `Widened` until it is re-admitted.
- `AgentEnvelopeDimension` labels are reason codes: `widened-knowledge-access`, `undeclared-tool`, and so on (`definition.rs:580`).
- The dispatch authority attests that the memory-ingress chain a run evaluates is its own. `AgentToolAuthority::with_memory_ingress` (`crates/rakka-agent/src/tools.rs:1617`) compares its chain with the private bundle's and refuses `guardrail-chain-mismatch` when they differ or when the memory has no bundle. Only after that does the authority count `MemoryIngress` as an evaluated boundary, which is what lets a definition's mandatory memory-ingress stage pass dispatch.

### 2.4 The graph

- `rakka-agent-knowledge-graph` depends on `rakka-agent`. `rakka-agent` cannot name `Claim`, `ClaimId` or `ClaimTrustStatus`. It has the mirror identity `AgentCommunalClaimId` and the port `AgentGoalClaimSource` (`crates/rakka-agent/src/query.rs:1745`), which the graph crate implements.
- `KnowledgeGraphStore` (`crates/rakka-agent-knowledge-graph/src/store.rs:700`) has `append`, `get`, `query`, `traverse`, `transition`, `transitions`, `tombstone` and `delete`. `query` pages in ascending claim-id order. No operation ranks.
- `KnowledgeGraphCapability` has one variant, `SemanticSearch`, which no backend reports.
- `ClaimAccess.required_read_capabilities` (`crates/rakka-agent-knowledge-graph/src/claim.rs:436`) is stored on every claim, at most 16 entries. Its doc says an empty set means readable by every agent authorized for the space, and that enforcement belongs to the retrieval layer. Nothing enforces it.
- Every claim is born `Proposed`. The host never moves a claim's trust status.
- The PostgreSQL claim table has columns for tenant, space, claim id, subject, predicate, object node, trust, transition count and tombstoned. The whole claim, including an object's text, its classification, confidence and access set, is in one opaque `record` column. `query` already reads a space row by row and applies `ClaimFilter::admits` in Rust, because `ClaimFilter` exposes no accessors.
- A trust transition changes only the trust status and the transition count (`claim.rs:741`). A claim's statement, classification, confidence and access set never change after the append. A tombstone withdraws the statement and keeps the other three (`claim.rs:717`).
- The PostgreSQL tombstone statement (`crates/rakka-agent-knowledge-graph-postgres/src/erasure.rs:57`) sets the row's subject, predicate, object node, record, `tombstoned` and fence. A column it does not name keeps its value. The delete statement removes the row.
- A read that finds a denormalized column disagreeing with the record is refused as drift (`claim-backend-failed`), never skipped (`lib.rs:689`, `:1233`).
- `migrate` (`lib.rs:542`) runs one statement batch, which is one transaction, and then a validating statement of its own, so that no scan runs under the batch's exclusive locks. No Rakka migration creates a trigger. `rakka-persistence-postgres` already keeps a GIN index over a `TEXT[]` column.
- The private tier's indexing pass is `PgvectorPrivateMemoryRetriever::reindex(scope, after, now, limit)` (`crates/rakka-agent-postgres/src/retrieval.rs:539`). It answers a page with `indexed`, `skipped` and `next`, and skips a record it cannot read rather than stopping on it. No loop in Rakka calls it.
- The host runs `PostgresKnowledgeGraphStore`.

### 2.5 The host

- `crates/rakka-host-runtime/src/prompt.rs:359` renders `snapshot.private_memory`. Nothing reads `communal_claims`.

## 3. Goals, non-goals, anti-goals

### Goals

- A wired run's snapshot carries the communal claims selected for that turn, each with claim id, knowledge space, trust state, provenance and exact statement.
- A claim is selected only when the run's grant covers its space and the run holds every capability the claim requires.
- Communal content crosses the memory-ingress guardrail boundary, as private memory does.
- Every ranked identity is resolved through the authoritative graph store before it is embedded. A drop is counted.
- A run's read capabilities are explicit, auditable data: granted by the definition, narrowed by a setup, and passed to a child only by an explicit delegation.
- A deployment that does not wire communal retrieval sees no change: the same snapshot bytes, the same digests.

### Non-goals

- Ranking by meaning. Filed as #140.
- A loop that runs the PostgreSQL indexing pass. A deployment calls it, as it calls the private tier's.
- Rendering a snapshot into a prompt. That is the host's.
- Graph traversal as a retrieval source. Only search feeds the snapshot in this slice.
- A read-capability statement on a goal specification. A goal's root run reads under its definition and setup.
- A communal read tool the model can call. Retrieval is automatic at snapshot assembly, as it is for private memory.
- Re-checking a persisted snapshot when a grant narrows later. See the anti-goals.

### Anti-goals

- **A claim stored in a snapshot before anyone has decided the run may read it.** Every restriction applies before ranking and again at resolution.
- **A reverse index from a claim to snapshots.** Issue #68 settled this: a snapshot keeps its copy until the run's snapshot purge.
- **Communal memory as a liveness gate.** No communal failure may fail or park a model turn.
- **A snapshot an earlier binary misreads silently.** An earlier binary must either read a snapshot correctly or refuse it.
- **A rule that lives only in an adapter.** Scope, access and trust are decided in `rakka-agent` on the authoritative record. A wrong adapter can lose a claim but cannot leak one.
- **Search words that outlive their claim.** The PostgreSQL index holds words taken from a claim's content. Erasing the claim erases its words in the same statement, whichever binary erases it.
- **An index that decides.** The index nominates rows. The record in each row decides whether the claim is a hit.

## 4. Constraints

- `rakka-agent` cannot depend on the graph crate. Every type the snapshot or the assembly names lives in `rakka-agent`.
- Four crates change: `rakka-agent`, `rakka-a2a`, `rakka-agent-knowledge-graph` and `rakka-agent-knowledge-graph-postgres`.
- The settle pass may perform only bounded I/O. Every loop below has a ceiling, and every search statement returns at most its limit.
- There is one database migration, on the graph's claim table. It runs while earlier binaries are serving, so it may not scan or rewrite the table under a lock that blocks writers, and every statement of an earlier binary must keep working after it.
- PostgreSQL needs no extension for this slice.
- Durable records change additively. A field that is new is omitted when empty, so records written by this version without the feature in use are unchanged on the wire.
- Error codes, metric names and label values are a compatibility surface.
- No resolved credential or secret material enters a snapshot. A claim's statement is consumer data and is already bounded and classified by the graph.
- Raw claim text, claim ids and space ids are never metric labels.

## 5. Approaches

### 5.1 Where the run learns what it may read

Three ways were considered.

1. **The settle pass asks, before it reads any claim.** A port answers which spaces and read capabilities the run's definition and setup grant now. The run narrows that by its delegation. Every restriction applies before ranking, and a narrowed definition reaches the next turn. It costs one more durable read per turn.
2. **Assemble under the delegation only, and let the dispatcher check the snapshot when it authorizes the model call.** No new port. But a claim the run may not read is already stored before anyone refuses it, a run with no delegation has no limit at assembly, and a refused model call is stuck because retries reuse the snapshot.
3. **Stamp the grant on the run when it starts.** No extra read. But it adds durable run state, and a later narrowing never reaches a running run.

The owner picked 1 on 2026-11-14.

### 5.2 Owner decisions

- **What it means to hold a read capability:** an explicit read-capability set on the authority envelope. The definition grants it and a setup narrows it. Rejected: deriving it from tool capabilities, and asking a deployment port.
- **Ranking:** word-match search as a new optional store operation, implemented by the in-memory store and the PostgreSQL store. Ranking by meaning is filed as #140.
- **Where the grant is read:** approach 1 above.
- **PostgreSQL search:** indexed, in this slice. The first draft of this design read a space in the application up to a ceiling of 4096 live claims and left the index to a follow-up. The owner chose the index on 2026-11-14.

### 5.3 Calls made in this design, for the owner to check

The first seven are about the PostgreSQL index (section 6.3.1).

- **The index is columns on the claim row, not a separate table.** The claim's search words and three restriction fields are written by the statement that inserts the claim. One row holds the claim and its index, so a delete removes both and nothing can be left pointing at a claim that is gone. A separate word table would need its own erasure statements and its own marker for which claims are indexed.
- **PostgreSQL restricts and ranks in the database, and there is no scan ceiling.** A search ranks every admitted claim of the space that shares a word with the query and returns the best `limit`. The work the database does grows with how many claims share a word with the query.
- **One index serves every tenant.** The scope filter is always applied. But the index entry for a common word lists claims of every tenant before the filter narrows them, so one tenant's volume can slow another's search. The private tier's vector index has the same property. Giving each scope its own index entries is possible later without changing the search operation.
- **A database trigger clears a tombstoned row's search words.** This is the first trigger in a Rakka migration. Without it, a binary from before this slice that tombstones an indexed claim leaves the claim's words in the row, because its statement does not name the new column. The trigger makes "a tombstoned row holds no search words" a rule the database enforces, for every binary.
- **Both new indexes are built without blocking writers**, each in a statement of its own after the migration batch. A build that failed part-way is dropped and rebuilt on the next run.
- **Claims written before the migration, or by an earlier binary, are not found until the indexing pass reaches them.** A search reports when a space holds such claims, and that report reaches the snapshot and a counter. The pass is per knowledge space, and no loop in Rakka calls it.
- **The word-splitting rule is frozen by a test.** The index stores its output. Changing the rule later means re-indexing, so a golden test fails on any change to it.
- **A search carries its restrictions as plain fields**, not as a `ClaimFilter`. The filter type exposes no accessors, so a backend could not apply it inside an index.
- **Read capabilities get their own field through a delegation**, parallel to knowledge spaces. The existing `capability_scopes` is not reused, because narrowing it by the parent's read grant would also strip scopes that have nothing to do with reading.
- **The default policy admits `Verified` claims only.** `Retracted` can never be admitted. The host must admit `Proposed` explicitly to see its own claims.
- **The dispatch authority's memory-ingress attestation covers the communal bundle.** Every retrieval bundle a run's memory carries must hold the authority's own chain, or wiring fails. Without this, a deployment could attest with the private bundle and wire a communal bundle whose chain is empty, and a definition's mandatory memory-ingress stage would never see a communal claim. A deployment that today passes two different chains would fail at startup.
- **Read capabilities cross the A2A wire in a new `read-capabilities` field**, omitted when empty. An earlier receiver refuses a send that carries one, as it does for knowledge spaces.
- **Communal telemetry uses two new counters** and leaves the two private counters unchanged.
- **The snapshot's schema version becomes 2, written only when a communal retrieval ran.**

## 6. Design

### 6.1 Read capabilities on the authority envelope

`AgentAuthorityEnvelope` gains:

```rust
/// Capabilities the agent holds for reading communal claims.
#[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
pub read_capabilities: BTreeSet<AgentCapabilityId>,
```

- A setup may only narrow it. `AgentEnvelopeDimension` gains `ReadCapability` (label `widened-read-capability`, a reason code like its neighbours), and the narrowing check gains one `collect_subset_violations` call beside the knowledge-space one.
- Autonomy admission uses the same check. An autonomously admitted agent whose definition gains a read capability is refused as `Widened` until it is re-admitted. That is specification 7.4 working as intended, and it is in the roll order.
- The field is omitted when empty, so an existing definition serializes to the same bytes and any digest over it is unchanged.
- The set means nothing outside communal reads. It does not authorize a tool, an append or a dispatch.

### 6.2 Read capabilities through delegation and handoff

Each record that carries `knowledge_spaces` gains a `read_capabilities` field beside it, with the same serde attributes as its neighbour:

| Record | Field | Meaning |
|---|---|---|
| `AgentDelegationTarget` | `BTreeSet<AgentCapabilityId>`, builder `with_read_capability` | The catalog's explicit statement for this specialist. |
| `AgentDelegationRecord` | `BTreeSet<AgentCapabilityId>` | The statement intersected with the parent's own grant. |
| `AgentCollaborationMetadata` (`rakka-a2a`) | `Vec<String>`, wire name `read-capabilities` | The record's set on the wire. |
| `AgentTaskDelegationProvenance` | `BTreeSet<AgentCapabilityId>` | What the child's task was delegated. |
| `AgentHandoffCollaborationMetadata` (`rakka-a2a`) | `Vec<String>`, wire name `read-capabilities` | The handoff's set on the wire. |
| `AgentTaskHandoffRequest`, `AgentTaskHandoff` | `BTreeSet<AgentCapabilityId>` | The catalog's statement for the handoff target. |
| `AgentRunDelegationEnvelope` | `Option<BTreeSet<AgentCapabilityId>>` | The run's grant. `None` means no statement. |

The derivations copy the knowledge-space ones exactly, at the same three places:

- **A child's record** (`run.rs:3190`): the target's set intersected with the parent's envelope grant when the parent has one; empty when the parent has lineage and no statement; the target's set at the root.
- **A run's base envelope** (`task.rs`, `delegation_envelope_base`): `None` for a goal root and for a plain task; `Some(provenance.read_capabilities)` for a delegated child. A provenance recorded before the field decodes to the empty set, which is the fail-closed reading.
- **The handoff step** (`task.rs`, `delegation_envelope_for`): the same three-arm intersection with the handoff's set.

The two wire fields are omitted when empty and validated into `AgentCapabilityId` at the receiving surface, exactly as `knowledge-spaces` is. A send that carries read capabilities is refused by a receiver on an earlier binary, because both envelopes refuse unknown fields. A grant is therefore never silently dropped in transit.

`AgentGoalDelegationEdgeView` (`crates/rakka-agent/src/query.rs:394`), which shows a delegation's knowledge spaces, also shows its read capabilities. `AgentGoalContractView` is derived from the goal specification and does not change.

`AgentDelegationToolCall` is untouched. Model output still cannot name a capability.

### 6.3 Keyword search in the graph store

`KnowledgeGraphCapability` gains `KeywordSearch` (label `keyword-search`). `KnowledgeGraphStore` gains:

```rust
fn search<'a>(
    &'a self,
    scope: &'a KnowledgeSpaceScope,
    search: &'a ClaimSearch,
) -> ClaimFuture<'a, ClaimSearchReport>;
```

The method has a default body that answers `claim-capability-unsupported`, so an existing implementation compiles and reports that it cannot search. The in-memory store and the PostgreSQL store implement it and report the capability.

**The request.** `ClaimSearch` holds the following, each behind an accessor so that a backend can apply it inside its index:

- `text`: the query, clamped to `CLAIM_SEARCH_TEXT_MAX_BYTES` (1024) at a character boundary;
- `trust`: the admitted trust states;
- `classifications`: the admitted classifications;
- `min_confidence_bps`: the confidence floor;
- `reader`: the `BTreeSet<AgentCapabilityId>` the reader holds;
- `limit`: clamped to `1..=CLAIM_SEARCH_MAX_HITS` (64).

It does not hold a `ClaimFilter`. That type exposes no accessors, and a search needs none of its other axes.

`ClaimSearch::admits(&Claim)` is the one admission rule, used by every backend. It is true when the claim is live (a tombstoned stub never matches), its trust state and its classification are in the admitted sets, its confidence meets the floor, and `claim.access.required_read_capabilities` is a subset of `reader`. An empty admitted set admits nothing.

**The words.** Two public functions say what a search matches, and every backend uses them:

- `claim_search_tokens(&Claim) -> BTreeSet<String>`: the words of the claim's subject, its predicate, and its object. The object gives its node id for an edge, its text for an inline value, and nothing for a value held by artifact reference.
- `claim_search_query_tokens(&str) -> BTreeSet<String>`: the words of a query.

Both split text the way the private tier does (`retrieval.rs:895`): on every character that is not alphanumeric, lowercased, without duplicates. A golden test pins this rule's output for a fixed set of inputs, because the PostgreSQL index stores it.

**The score.** One public function, `claim_search_relevance_bps(query_tokens, &Claim) -> u16`, is the only scorer: the fraction of the query's words that are among the claim's words, in basis points.

**The answer.** `ClaimSearchReport` holds:

- `hits`: up to `limit` of `ClaimSearchHit { claim, relevance_bps }`, best first, ties by ascending claim id. A claim that scores zero is not a hit.
- `unindexed`: true when the space holds live claims the backend's index does not cover, so this search could not see them.

**The rules.**

- Admission precedes ranking. A claim that is not admitted never takes a hit slot.
- There is no scan ceiling. A search ranks every admitted claim of the addressed space that shares a word with the query.
- A wrong scope answers the same report as an empty space.
- Both shipped backends use the same `admits`, the same words and the same scorer, so they answer the same hits in the same order.

**The in-memory store** reads its space, admits, scores and sorts. Its `unindexed` is always false.

#### 6.3.1 The PostgreSQL index

**Four columns on the claim table**, all nullable:

| Column | Holds |
|---|---|
| `search_tokens TEXT[]` | `claim_search_tokens` of the claim, compared bytewise. `NULL` means the row is not indexed. An indexed claim with no words holds an empty array. |
| `search_classification TEXT` | The claim's classification label. |
| `search_confidence_bps INTEGER` | The claim's confidence. |
| `search_read_capabilities TEXT[]` | The claim's required read capabilities. |

The last three copy fields that never change after the append (section 2.4), so the search statement can restrict before it ranks. Trust and `tombstoned` are already columns. Only `search_tokens` is taken from the claim's content.

**Who writes them.**

- **Append** writes all four in the statement that inserts the claim. A claim appended by this version is searchable as soon as it is stored.
- **A trust transition** writes none of them. Nothing they copy can change.
- **Tombstone** sets `search_tokens` to `NULL` in its one statement.
- **Delete** removes the row.

**The erasure rule: a tombstoned row holds no search words.** The database enforces it. A row trigger on the claim table, before insert and before update, sets `search_tokens` to `NULL` on any row whose `tombstoned` is true. It fires only for a tombstoned row that still holds words, so an ordinary write pays nothing for it. The rule therefore holds for a binary from before this slice too, whose tombstone statement does not name the column, and for any statement written later that forgets to.

**Two indexes.**

- A GIN index on `search_tokens`. It answers "which rows share a word with this query".
- A partial index on `(tenant, space, claim_id)` over rows that are live and have `search_tokens IS NULL`. It answers "does this space hold claims the index does not cover", and it is the indexing pass's cursor. It shrinks to nothing as the pass runs.

**The search** is one statement plus one probe.

- The statement selects rows of the addressed tenant and space that are live, whose `search_tokens` share at least one word with the query's, whose trust and classification are admitted, whose confidence meets the floor, and whose `search_read_capabilities` are all among the reader's. It orders them by how many of the query's words they hold, descending, then by claim id, and returns `limit` rows.
- Each returned row is decoded. The store then checks it with `ClaimSearch::admits` and scores it with the shared scorer. A row the record does not admit, or that scores zero, means a column has drifted from its record. The search is refused as drift (`claim-backend-failed`), as every other read in the store is. The index can therefore never put a claim in front of a reader the record would not.
- The probe asks whether the partial index holds any row for the scope. Its answer is `unindexed`.

**The cost.** The rows returned are bounded by `limit`. The work the database does grows with how many claims share a word with the query. The GIN index covers every tenant, so the entry for a common word lists claims of every tenant before the scope filter narrows them (section 5.3).

**The indexing pass.**

```rust
impl PostgresKnowledgeGraphStore {
    pub async fn index_claims(
        &self,
        scope: &KnowledgeSpaceScope,
        after: Option<&ClaimId>,
        limit: usize,
    ) -> ClaimResult<ClaimSearchIndexPage>;
}
```

`ClaimSearchIndexPage` holds `indexed`, `skipped` and `next`, like the private tier's `ReindexPage`.

- It reads at most `limit` (clamped to `1..=256`) unindexed live rows of the scope in claim-id order after the cursor, derives the four columns from each row's record, and writes them only where the row is still live and still unindexed.
- A tombstone that lands between the read and the write wins: the write's own condition refuses the row, and the trigger would clear the words if it did not.
- A row whose record cannot be decoded is skipped and counted. It does not stop the pass. It stays unindexed, so its space keeps reporting `unindexed` until the record is repaired or erased.
- The pass is idempotent and safe to run at any time, on any node, while the store is serving. No loop in Rakka calls it.

**The migration.** `migrate` gains two steps.

- *In the existing batch:* when `search_tokens` is absent, one `ALTER TABLE` adds the four columns; when the trigger is absent, the trigger and its function are created. A fresh table is created in the final shape.
- *After the batch, each in a statement of its own:* the two indexes, built `CONCURRENTLY`. Before each, an index of that name that an earlier failed build left invalid is dropped.

What the upgrade costs while earlier binaries serve:

- The first run takes an `ACCESS EXCLUSIVE` lock on the claim table for catalog work only. Four nullable columns with no default and a trigger are catalog changes. Nothing is scanned or rewritten under that lock.
- The index builds do not block reads or writes. They take time in proportion to the table, and each waits for transactions that were open when it began.
- A repeat run alters nothing.

**What an earlier binary does to a migrated table.**

| It… | Effect |
|---|---|
| appends a claim | The row has no index columns. Search does not find it and reports `unindexed`. The pass indexes it. |
| moves a claim's trust | The index is unaffected. |
| tombstones a claim | The trigger clears the claim's search words in the same statement. |
| deletes a claim | The row is gone. |
| runs its own `migrate` | Nothing. |

### 6.4 The communal types and ports in `rakka-agent`

A new module, `crates/rakka-agent/src/communal.rs`.

**Mirror types.** `rakka-agent` cannot name the graph's types, so it declares its own:

- `AgentCommunalClaimTrust`: `Proposed`, `Verified`, `Disputed`, `Retracted`. `#[non_exhaustive]`, kebab-case, the same labels as `ClaimTrustStatus`.
- `CommunalClaimStatement { subject: String, predicate: String, object: CommunalClaimObject }`, where `CommunalClaimObject` is `Node(String)` or `Value(AgentTaskContent)`.
- `CommunalClaimProvenance { agent, goal, task, run, delegation }`, the last four optional.
- `CommunalClaimRecord`: what the authority answers for one claim. It holds the claim id (`AgentCommunalClaimId`), the knowledge space, the statement, the statement's digest, trust, provenance, evidence (`Vec<ArtifactRef>`), `created_at`, `confidence_bps`, classification, and `required_read_capabilities`. `validate` bounds every field to the graph's own limits (inline object 4096 bytes, 16 evidence references, 16 capabilities, confidence at most 10000).

**The query.** `CommunalRetrievalQuery` holds the text, a limit, a confidence floor, the admitted classifications, the admitted trust states, and the reader's capabilities. `admits(&CommunalClaimRecord)` is the fail-closed re-check: trust is admitted and is not `Retracted`, the classification is admitted, the confidence meets the floor, and the required capabilities are a subset of the reader's.

**The ports.**

```rust
pub trait AgentCommunalClaimRetriever: Send + Sync + 'static {
    fn backend_name(&self) -> &'static str;
    fn retriever_version(&self) -> AgentRevisionNumber;
    fn retrieve<'a>(
        &'a self,
        tenant: &'a TenantId,
        space: &'a KnowledgeSpaceId,
        query: &'a CommunalRetrievalQuery,
        now: AgentTimestampMillis,
    ) -> MemoryFuture<'a, CommunalRetrievalOutcome>;
}

pub trait AgentCommunalClaimAuthority: Send + Sync + 'static {
    fn backend_name(&self) -> &'static str;
    fn resolve<'a>(
        &'a self,
        tenant: &'a TenantId,
        space: &'a KnowledgeSpaceId,
        claim: &'a AgentCommunalClaimId,
        now: AgentTimestampMillis,
    ) -> MemoryFuture<'a, Option<CommunalClaimRecord>>;
}
```

- `CommunalRetrievalOutcome` holds `ranked: Vec<RankedCommunalClaim { claim, relevance_bps }>`, an optional `index_watermark`, and `index_incomplete` (the backend said the space holds claims its index does not cover). A ranking carries identities and scores only. It carries no content, so there is nothing in it to trust.
- `resolve` answers `None` for a claim the store does not hold in that scope, and for a tombstoned stub. A stub has no statement to give.

**The policy.** `CommunalRetrievalPolicy` clamps rather than errors, like every policy in the crate:

| Knob | Default | Bound |
|---|---|---|
| `max_results` | 8 | `1..=AGENT_SNAPSHOT_COMMUNAL_CLAIM_MAX_ENTRIES` (16) |
| `max_bytes` | 64 KiB | `1..=AGENT_SNAPSHOT_COMMUNAL_CLAIM_MAX_BYTES` (64 KiB) |
| `max_spaces` | 4 | `1..=AGENT_COMMUNAL_RETRIEVAL_MAX_SPACES` (16) |
| `query_entries` | 4 | as the private policy |
| `min_confidence_bps` | 0 | `0..=10000` |
| `classifications` | `{Unclassified}` | any set |
| `trust` | `{Verified}` | any set; `Retracted` is removed |
| `max_resolutions` | derived | `max_results` × 4, capped at 64 |

**The bundle.** `AgentCommunalRetrieval::new(retriever, authority, grants, guardrails, policy)` requires all five. The guardrail chain is required for the reason the private bundle requires its own: an optional chain is a fail-open. `AgentRunMemory::with_communal_retrieval` wires it. A run with no communal bundle behaves exactly as today.

**The attestation.** `AgentToolAuthority::with_memory_ingress` and `attests` cover every retrieval bundle the memory carries:

- A memory with no bundle of either kind is refused, as today.
- Each bundle present must declare the authority's own chain. One that differs is refused under the existing `guardrail-chain-mismatch`.
- A memory with only a communal bundle can attest.

So one chain governs the memory-ingress boundary for both tiers, and it is the chain the dispatch gate enforces.

### 6.5 The read grant

```rust
pub trait AgentCommunalReadGrantSource: Send + Sync + 'static {
    fn backend_name(&self) -> &'static str;
    fn read_grant<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        now: AgentTimestampMillis,
    ) -> MemoryFuture<'a, Option<AgentCommunalReadGrant>>;
}
```

`AgentCommunalReadGrant` holds `spaces`, `read_capabilities`, `definition_revision`, and `setup_revision` (optional).

**The reference implementation**, `AgentEntityCommunalReadGrants`, reads what the dispatcher reads: the agent's durable state through `load_agent_entity_state`, and the run's setup through an optional `AgentRunSetupResolver`.

- No agent state, or an agent whose lifecycle status the dispatcher would refuse: `None`.
- No setup: the definition envelope's `knowledge_spaces` and `read_capabilities`.
- A setup: the intersection of the definition's sets and the setup's sets. Intersecting, rather than taking the setup's, means a definition narrowed after the setup was admitted still binds.

**The run narrows the answer by its delegation envelope**, with the claim-append rule applied to both sets:

- `Some(set)`: intersect.
- `None`, and the envelope has lineage: empty.
- `None` at the root, or no envelope at all: the grant as answered.

The result is the run's **effective read grant**: the spaces it may search and the capabilities it holds.

### 6.6 Assembly

`assemble_context` gains the run's delegation envelope as an input. `persist_context_snapshots` passes `loop_state.delegation_envelope()`. The session and private halves are untouched and run first.

The communal half, when a bundle is wired:

1. **Derive the query** from the session window, as the private half does, using the communal policy's `query_entries`. No text: the communal half is skipped.
2. **Read the grant.** An error: the communal half degrades and selects nothing. `None`, or an effective grant with no spaces: skipped.
3. **Search.** Take the effective spaces in ascending id order, at most `max_spaces`. For each, call `retrieve`. A failure for one space marks the report degraded and that space contributes nothing; the others continue.
4. **Merge** the rankings: relevance descending, then space id ascending, then claim id ascending.
5. **Walk** the merged ranking. For each entry, in order:
   - Stop if `max_results` selections are held.
   - Skip a repeated `(space, claim)` and count it rejected.
   - Stop if `max_resolutions` reads are spent, and set `resolution_capped`.
   - `resolve` it. `None`: count it **unverified**. An error: mark degraded, stop, keep the prefix.
   - Re-check the record: it validates, it names the space and claim that were asked for, and `query.admits` holds. Otherwise count it rejected.
   - Evaluate the statement at the memory-ingress boundary (below).
   - Stop if its bytes would exceed `max_bytes`.
   - Embed it.

The selection is a prefix of the merged ranking that survived the checks, as in the private half.

**The guardrail boundary.** `AgentGuardrailContext` gains `with_claim(&AgentCommunalClaimId, &KnowledgeSpaceId)` beside `with_memory`. The value evaluated is the statement as JSON: `{"subject", "predicate", "object"}`.

- Blocked: dropped, counted blocked.
- Checkpoint required: dropped, counted checkpoint-refused. There is no checkpoint plumbing at assembly.
- Transformed: the output must decode as a `CommunalClaimStatement` that validates. A stage may rewrite the subject, the predicate, or an inline value. It may not change a node object or an artifact reference. Anything else is dropped and counted blocked. An accepted transform is embedded with its record, and the embedded digest is the digest of the transformed statement.

**The report.** `AssembledContext` gains `communal: CommunalRetrievalReport`. It has the private report's fields (`attempted`, `degraded`, `selected`, `rejected`, `unverified`, `blocked`, `transformed`, `reported`, `checkpoint_refused`, `resolution_capped`) and three more: `spaces_searched`, `spaces_capped` (the effective grant named more spaces than `max_spaces`), and `index_incomplete` (a searched space held claims its index does not cover). `RetrievalReport` is unchanged.

### 6.7 The snapshot record

`communal_claims` changes type, and one field is added:

```rust
pub communal_claims: Vec<SnapshotCommunalClaim>,
#[serde(default, skip_serializing_if = "Option::is_none")]
pub communal: Option<SnapshotCommunalRetrieval>,
```

`SnapshotCommunalClaim` holds: `claim_id`, `space`, `trust`, `statement`, `statement_digest`, `provenance`, `evidence`, `created_at`, `classification`, `confidence_bps`, `relevance_bps`, `transforms` and `reports` (both `Vec<SnapshotIngressRecord>`). Every field but `relevance_bps` comes from the authoritative record. `relevance_bps` is clamped to 10000.

`SnapshotCommunalRetrieval` records how the selection was made: `query`, `retriever`, `retriever_version`, `index_watermark`, `spaces` (the spaces searched), `trust`, `classifications` and `min_confidence_bps` (the policy in force), `index_incomplete`, `ingress_revision` (the communal chain's revision), and `definition_revision` and `setup_revision` (what the grant was read under). `retrievals` stays private-only.

`SnapshotBudget` gains `communal_claim_bytes`.

**Version rule.** The current schema version becomes 2.

- A snapshot is written as version 2 when `communal` is present, which is when at least one space was searched. Otherwise it is written as version 1.
- A version 1 snapshot written by this binary has the same digest as today, and an earlier binary reads it.
- The digest of a version 2 snapshot covers today's keys plus `communal`.
- On load, a version 1 snapshot with a non-empty `communal_claims` or with `communal` present is refused. A version 2 snapshot without `communal` is refused. These refusals are in the snapshot's own `Deserialize`, so they hold on every store.
- An earlier binary and a version 2 snapshot:
  - With selected claims, it fails to decode `communal_claims`. It can never read a claim as something else.
  - With none selected, the PostgreSQL store refuses version 2. Another store decodes it and drops `communal`. The model input is the same in that case, because there are no claims; only the record of the search is lost to that reader.
  - Section 7 gives the roll order that avoids both.

### 6.8 The graph crate's adapters

`rakka-agent-knowledge-graph` gains a module beside `goal_claim_source.rs`:

- `KnowledgeGraphCommunalRetriever::new(store)` implements `AgentCommunalClaimRetriever` over `search`. Construction refuses a store that does not report `KeywordSearch`. It maps the query's trust states, classifications, confidence floor, reader capabilities and limit onto a `ClaimSearch`, and the report's `unindexed` onto the outcome's `index_incomplete`.
- `KnowledgeGraphCommunalAuthority::new(store)` implements `AgentCommunalClaimAuthority` over `get`. It maps a live `Claim` to a `CommunalClaimRecord` and a tombstoned stub to `None`.
- The conversions between `ClaimTrustStatus` and `AgentCommunalClaimTrust` are exhaustive in the graph crate, so a trust state added later fails to compile until it is mapped.
- A test holds the mirrored bounds in `rakka-agent` equal to the graph's constants.

The crate's documented non-goals lose "no communal retrieval path" and "no per-claim capability enforcement at read".

### 6.9 Telemetry

Two counters, registered in the catalogue beside the private ones:

- `rakka.agent.communal.retrievals`, labels `backend` and `outcome` (`retrieved`, `degraded`, `skipped`).
- `rakka.agent.communal.ingress.outcomes`, label `outcome`: `blocked`, `transformed`, `reported`, `checkpoint-refused`, `rejected`, `unverified`, `resolution-capped`, `index-incomplete`, `spaces-capped`.

The two private counters keep their names, labels and meaning. Specification 17.10 asks that tiers be distinguishable; the metric name is the tier.

### 6.10 What a snapshot keeps

A snapshot is immutable and is reused on every retry of its turn. A claim that is tombstoned, deleted, retracted, or put out of the run's reach after it was selected stays in that snapshot until the run's snapshot purge. It cannot enter a later snapshot. Rakka keeps no index from a claim to the snapshots that hold it.

### 6.11 New compatibility surface

- Envelope dimension label: `widened-read-capability`.
- A2A wire field on both collaboration envelopes: `read-capabilities`.
- `guardrail-chain-mismatch` now also answers for a communal bundle whose chain differs.
- Graph capability label: `keyword-search`.
- PostgreSQL schema: four columns, one trigger with its function, and two indexes on the claim table (section 6.3.1). Their names are fixed once shipped, because `migrate` finds them by name.
- Metric names and outcome labels: section 6.9.
- No new error code. An unsupported search answers the existing `claim-capability-unsupported`.

## 7. Rollout and compatibility

There is one database migration. Its cost and what an earlier binary does afterwards are in section 6.3.1.

1. **Migrate.** Run this version's `migrate` against the graph's database. Earlier binaries keep serving through it and after it.
2. **Upgrade every Rakka node and every snapshot reader.** For the host, that includes its prompt builder. Nothing else changes yet: no bundle is wired, snapshots stay version 1, and no record carries a read capability.
3. **Index.** Call `index_claims` for each knowledge space until it answers no `next`. Do this after step 2, because a claim appended by an earlier binary is not indexed. The pass can be run again at any time, and should be after any period in which an earlier binary was serving.
4. **Render the claims.** The host's prompt builder renders `communal_claims` as untrusted data, with each claim's trust state and provenance.
5. **Grant.** Definitions that should read restricted claims gain `read_capabilities`. Catalogs that should pass them to a specialist state them on the target. Neither may be done before step 2 completes:
   - An earlier binary ignores the definition's field, which fails closed but is not what the operator asked for.
   - An earlier receiver refuses a delegation or handoff that carries read capabilities.
   - An autonomously admitted agent whose definition gains a read capability must be re-admitted, or its autonomous runs are refused as widened.
6. **Wire the communal bundle**, with the dispatch authority's own guardrail chain and a policy that admits the trust states the deployment wants. Attest the memory with `with_memory_ingress` as today; it now checks both bundles.

Wiring before step 3 is finished is safe. Claims not yet indexed are not found, and each snapshot's communal record says `index_incomplete`.

A rollback to an earlier binary needs no schema change. That binary never names the new columns, and the trigger keeps erasure reaching the index while it serves.

Existing claims carry no read requirement, so once indexed they are readable by any run granted their space, under a policy that admits their trust state.

## 8. Proofs

Every test is written first and watched to fail.

### 8.1 Conformance clauses (graph crate, run unchanged by PostgreSQL)

- A search answers hits in relevance order with ties by claim id, honours the limit, and returns no zero-score claim.
- Admission precedes ranking: a claim excluded by trust, classification, confidence or a missing reader capability takes no slot, with enough admitted claims behind it to prove it.
- A claim that requires nothing is found by a reader holding nothing. A claim that requires two capabilities is not found by a reader holding one.
- A tombstoned stub is never a hit, and a search for the words its statement held before the tombstone finds nothing.
- A wrong scope answers the report an empty space answers.
- A search reaches a matching claim however many claims precede it in claim-id order: the clause appends more non-matching claims than any page size and finds the one that matches.
- The word rule's golden test: fixed inputs, fixed words.
- A store that does not report the capability answers `claim-capability-unsupported`. The test double proves it.
- The in-memory and PostgreSQL stores answer the same hits in the same order for the same corpus.

### 8.2 `rakka-agent`

- **Golden bytes first.** Before any change, a test pins the serialized bytes and digest of a version 1 snapshot and of a definition envelope. It must stay green to the end.
- Envelope narrowing: a setup that adds a read capability is refused under `widened-read-capability`.
- Delegation: one test per derivation arm in section 6.2, mirroring the knowledge-space tests, including the handoff.
- The effective grant: definition only; definition and setup; a definition narrowed below its setup; each of the three delegation arms.
- Assembly, against test doubles of the three ports:
  - a granted, admitted claim is embedded with every field from the authoritative record;
  - a ranked identity the authority does not hold is counted unverified and not embedded;
  - `retrieve` is never called for a space outside the effective grant, and a claim id that belongs to another space resolves to `None` and is counted unverified;
  - a record whose trust moved to `Retracted`, whose classification is not admitted, or whose requirements the run does not hold is counted rejected, even when the ranking named it;
  - a blocked, a checkpoint-required and an illegally transformed claim are dropped and counted; a legal transform is embedded with its record and digest;
  - the selection is a rank prefix under both budgets;
  - a grant-source error, one failing space, and an authority error part-way each degrade as section 6.6 says, and none fails the assembly;
  - one space answering `index_incomplete` sets it on the report and in the snapshot's communal record, and the other spaces' claims are still selected;
  - an unwired run, a run with no query text and a run with no granted space each write a version 1 snapshot with today's bytes;
  - the load refusals in section 6.7.
- The default policy admits only `Verified`, and no policy can admit `Retracted`.
- The attestation: a communal bundle with a different chain is refused beside a matching private bundle; a communal-only memory with the matching chain attests; `attests` answers false for a memory whose communal bundle drifted. These join `tests/memory_guardrail_chain_consistency.rs`.
- Autonomy admission refuses a definition that gained a read capability after it was admitted.

### 8.2a `rakka-a2a`

- Both envelopes carry read capabilities from the sender's record to the receiver's provenance or handoff request.
- An envelope with no read capabilities serializes to the bytes it does today.
- An invalid capability id fails the whole send.

### 8.3 End to end

One test with real stores: a run entity wired with the in-memory graph store, both adapters and the reference grant source. A claim is appended and verified, the run takes a turn, and the persisted snapshot holds the claim. A second run whose definition lacks the space holds nothing. A third whose definition lacks a required capability holds nothing.

### 8.4 PostgreSQL

The conformance suite, gated on `RAKKA_POSTGRES_TEST_DSN` as today, and these, in the same gate. "The `86cf087` statement" means the literal SQL text of that commit, kept in the test.

- **The index is written with the claim.** A claim is found by a search issued straight after its append.
- **An earlier binary's append.** A row inserted by the `86cf087` append statement is not found, and the search reports `unindexed`. After `index_claims` it is found and `unindexed` is false.
- **Erasure reaches the index.**
  - After a tombstone, the row's `search_tokens` is `NULL` and no search finds the claim.
  - After the `86cf087` tombstone statement runs against an indexed row, its `search_tokens` is `NULL`. This test is also run with the trigger dropped, and must fail then.
  - After a delete, no row and no index entry remain.
  - A stub is never counted as unindexed.
- **The index cannot leak.** A test rewrites one row's `search_read_capabilities` to empty for a claim that requires a capability, and another rewrites `search_classification`. A reader the record does not admit gets `claim-backend-failed`, never the claim.
- **The indexing pass.** It honours its limit and its cursor; a second run over an indexed space writes nothing; a row whose record is damaged is skipped and counted and the rows after it are indexed; a tombstone that commits between the pass's read and its write leaves `search_tokens` `NULL`.
- **The migration.**
  - Upgrading the `86cf087` schema does not rewrite the claim table (its file node is unchanged), and a second run alters nothing.
  - An index left invalid is rebuilt by the next run.
  - Every `86cf087` write statement (append, transition, tombstone, delete) and its `migrate` still succeed against the migrated schema.
  - A trust transition leaves the four columns as the append wrote them.

## 9. Docs and changelog

- `docs/rakka-agent-security-validation-matrix.md`: the "communal memory treated as an injection source" row becomes a stated control with its proofs, and #69 leaves the owed list.
- `docs/rakka-agents.md`: the read path, the read-capability grant, the trust default, and what a snapshot keeps.
- The compatibility document: the roll order in section 7 and the snapshot's version rule.
- `docs/rakka-api-boundary-inventory.md`: the new public items.
- Wherever the A2A collaboration envelopes are documented: the new wire field and its cross-version posture.
- The graph crate's `lib.rs`: the non-goals.
- The PostgreSQL graph crate's documentation: the migration and its lock cost, the indexing pass, the trigger, and that erasure reaches the search index.
- Wherever issue #68's erasure is documented: the search index joins the list of what a tombstone and a delete clear.
- `CHANGELOG.md`.

## 10. Left to the builder

- Whether `assemble_context` gains a parameter or a sibling function takes the envelope. Either way the change is in the changelog.
- The names of the new columns, indexes, trigger and function, and whether the indexing pass writes row by row or in one statement.
- How the two crates share the word rule: `rakka-agent` exposes its tokenizer, or the graph crate holds a copy that a test keeps equal to it.
- How the search statement and the probe are issued: two statements or one.
- Whether the envelope's new set needs a size bound. It follows whatever the neighbouring sets do.
- The exact names of the new constants and of the adapters' module.
- Whether the private retrieval emits a telemetry segment today. If it does, the communal half emits its sibling with the tier named; if not, none is added.
- The serde attributes that keep a version 1 snapshot's bytes unchanged, including on `SnapshotBudget`.
- Where the specification's own disposition notes are updated, if this slice closes one.

If the builder finds that an existing test fails for a reason this design did not plan, the builder stops and reports it.

## 11. After the merge

- A report on issue #69 for the host: the pin, the roll order, the fields to render, the trust default and how to admit `Proposed`, the migration and the indexing pass, what `index-incomplete` means, and what a snapshot keeps.
- A note on #140 that the claim row's index columns, the trigger and the indexing pass are the pattern an embedding column should follow.
