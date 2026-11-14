# Knowledge-Graph Retention, Tombstone and Deletion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the communal knowledge graph a tombstone, a hard delete that no replay can undo, and a per-space retention sweep with a window per trust status, proven on both backends (issue #68).

**Architecture:** The claim gains one optional `tombstone` field and a stub shape. `KnowledgeGraphStore` gains two required per-claim operations, `tombstone` and `delete`, each idempotent on an operation id and each erasing the claim's earlier ledger payloads. The retention sweep is written once in the domain crate over the existing `query` and `transitions` reads plus `delete`. PostgreSQL implements the two operations as one fenced statement each.

**Tech Stack:** Rust 1.88, `rakka-agent-knowledge-graph`, `rakka-agent-knowledge-graph-postgres` (`tokio-postgres`), `tokio` tests, the crate's `conformance` module.

**Spec:** `docs/superpowers/specs/2026-11-13-knowledge-graph-retention-tombstone-deletion-design.md`. Read it before any task. Section numbers below refer to it.

## Global Constraints

- Branch `knowledge-graph-retention`, off main `d12b0f8`. Line numbers in this plan are at `d12b0f8`; earlier tasks shift them, so find each site by the name given.
- Paths without a crate prefix are under `crates/rakka-agent-knowledge-graph/`. "The PG crate" is `crates/rakka-agent-knowledge-graph-postgres/`.
- Every public item needs a doc comment. Before each commit: `cargo fmt --all -- --check` and `cargo clippy -p rakka-agent-knowledge-graph -p rakka-agent-knowledge-graph-postgres --all-targets --all-features -- -D warnings`. Put the rustup toolchain's `bin` first on `PATH` so clippy matches CI.
- Stable codes added, exactly: `claim-tombstoned`, `claim-operation-erased`, `claim-tombstone-content`, `claim-delete-stale`. No existing code changes meaning.
- Durable-record rule: add `#[serde(default)]` fields only. Never change a field's type.
- A live claim's encoded bytes must not change. A live claim is written at schema version 1; only a stub is written at version 2.
- A refused operation changes nothing, and each refusal test asserts the stored state after the refusal, not only the error.
- Each guard gets a falsification: remove the guard, see the named test fail, restore it. Say which test failed in the task report.
- A wrong-scope tombstone or delete answers `claim-not-found`, identical to an absent claim.
- Run tests per file: `cargo test -p <crate> --test <file> -- <name>`. Do not run the whole workspace inside a task.
- PG tests are gated on `RAKKA_POSTGRES_TEST_DSN`. Tasks 5 and 6 need a live database. A plain PostgreSQL is enough; this crate does not need pgvector. If no DSN is available, stop and report. A skipped test is not a passed test.
- Commit messages follow the repository's style (a sentence, then `(issue #68)`), and end with `Co-Authored-By: Claude Fable 5.2 <noreply@anthropic.com>`.
- The controller runs `cargo clean` between tasks. An implementer never runs it.
- If an existing test fails for a reason this plan does not name, stop and report it. Do not edit the test.
- Final names may differ from this plan's where the code reads better. Record every difference in spec section 12.

## Review Focus

- **A live claim's encoding.** After this change, a claim that was never tombstoned serializes to the same JSON as before, with no `tombstone` key. The ledger replays stored bytes and the promotion gate digests claim content, so an extra key would change both. Test in Task 1.
- **A claim at the history cap.** A claim with 32 transitions is tombstoned: all 33 earlier operations answer `claim-operation-erased`, and the stub still lists 32 transitions. Test in Task 2.
- **An erased operation id replayed as another kind.** An append's id, erased by a delete, is replayed as a transition, a tombstone and a delete. Each answers `claim-operation-erased`; none writes. Test in Task 3.
- **Sweep arithmetic at the edges.** A window of `u64::MAX` never comes due and does not overflow; `now` earlier than the entry instant is not due; `max_batch` of 0 behaves as 1. Test in Task 4.
- **A cursor that points at a deleted claim.** The sweep deletes the last claim of a batch, returns a cursor at it, and the next call resumes after it without skipping or repeating. Test in Task 4, run on both backends through the conformance clause.

---

### Task 1: The claim's tombstone shape, its validation, and the four codes

**Files:**
- Create: `src/tombstone.rs`
- Modify: `src/claim.rs` (`CURRENT_CLAIM_SCHEMA_VERSION` `:46`, `Claim` `:486`, `Claim::new` `:546`, `apply_transition` `:640`, `to_record` `:663`, `restore` `:693`, `validate` `:725`, `ClaimRecord` `:809`)
- Modify: `src/error.rs` (`ClaimError`, its `code()` near `:271` and its `Display`)
- Modify: `src/lib.rs` (module and re-exports)
- Modify: `docs/rakka-compatibility.md` (`| \`claim\` | \`rakka-agent-knowledge-graph\` | 1 |` at `:202`)
- Test: inline `#[cfg(test)]` module of `src/claim.rs`; `tests/compatibility_currency.rs` (unchanged, must pass)

**Interfaces:**
- Produces, in `tombstone.rs`, all public, documented and re-exported from the crate root:
  - `pub const CLAIM_TOMBSTONE_MARKER: &str = "rakka-claim-tombstoned";` — the value a stub holds in `subject` and in `predicate`.
  - `pub struct ClaimTombstone { pub operation_id: ClaimOperationId, pub reason: ClaimTombstoneReason, pub actor: PrincipalRef, pub tombstoned_at: AgentTimestampMillis }` — `Debug, Clone, PartialEq, Serialize, Deserialize`.
  - `#[non_exhaustive] pub enum ClaimTombstoneReason { Withdrawn, Policy }` — kebab-case serde, `Copy`, with `pub const fn as_label(&self) -> &'static str` returning `withdrawn` and `policy`.
- Produces, in `claim.rs`:
  - `pub const CLAIM_LIVE_SCHEMA_VERSION: StateSchemaVersion = StateSchemaVersion::new(1);`
  - `CURRENT_CLAIM_SCHEMA_VERSION` becomes `StateSchemaVersion::new(2)`.
  - A private `tombstone: Option<ClaimTombstone>` field on `Claim`, and `pub tombstone: Option<ClaimTombstone>` on `ClaimRecord`. Both carry `#[serde(default, skip_serializing_if = "Option::is_none")]`.
  - `impl Claim { pub const fn tombstone(&self) -> Option<&ClaimTombstone>; pub const fn is_tombstoned(&self) -> bool; pub fn apply_tombstone(&self, tombstone: ClaimTombstone) -> ClaimResult<Self> }`
- Produces, in `error.rs`:
  - `ClaimError::Tombstoned { claim_id: ClaimId }` → `claim-tombstoned`
  - `ClaimError::OperationErased { operation_id: ClaimOperationId }` → `claim-operation-erased`
  - `ClaimError::TombstoneCarriesContent { claim_id: ClaimId }` → `claim-tombstone-content`
  - `ClaimError::DeleteStale { claim_id: ClaimId }` → `claim-delete-stale`

- [ ] **Step 1: Write the failing unit tests** in the test module of `claim.rs`.

  - `a_tombstone_clears_the_statement_and_keeps_the_audit_fields`: build a claim with evidence, an embedding, an access capability and a policy; `apply_tombstone`. Assert `subject` and `predicate` equal `CLAIM_TOMBSTONE_MARKER`, `object == ClaimObject::Value(AgentPrivateMemory::tombstone_content())`, `evidence` empty, `embedding` `None`. Assert `claim_id`, `operation_id`, `provenance`, `created_at`, `confidence_bps`, `classification`, `access`, `policy`, `policy_revision`, `trust()`, `transition_count()` and `content_digest` equal the original's. Assert `tombstone()` is the value passed.
  - `a_stub_restores_with_its_original_digest`: `Claim::restore(stub.to_record())` equals the stub. Its `content_digest` is the original statement's, not the markers'.
  - `a_stub_that_carries_content_is_refused`: take `stub.to_record()` and, one field at a time, restore a real subject, a real predicate, a real object, one evidence reference, an embedding. Each `Claim::restore` answers `Err` with `code() == "claim-tombstone-content"`.
  - `a_stub_is_written_at_version_two_and_a_live_claim_at_version_one`: `Claim::new(..).schema_version() == CLAIM_LIVE_SCHEMA_VERSION`; after one `apply_transition` it is still 1; after `apply_tombstone` it is `CURRENT_CLAIM_SCHEMA_VERSION` (2).
  - `a_live_claims_encoding_carries_no_tombstone_key`: `serde_json::to_value(&claim)` and `serde_json::to_value(claim.to_record())` have no `tombstone` key. A JSON record with no `tombstone` key and `schema_version` 1 restores to a claim with `tombstone() == None`.
  - `a_stub_refuses_a_transition_and_a_second_tombstone`: `stub.apply_transition(Retracted)` and `stub.apply_tombstone(other)` both answer `code() == "claim-tombstoned"`.
  - `a_transitioned_claim_keeps_its_trust_through_a_tombstone`: a claim moved `Proposed → Disputed → Retracted` is tombstoned; `trust() == Retracted`, `transition_count() == 2`, and `validate()` passes.
  - `the_four_new_codes_are_stable`: each new variant's `code()` equals its string above.

- [ ] **Step 2: Run them and see them fail**

  Run: `cargo test -p rakka-agent-knowledge-graph --lib claim::tests`
  Expected: compile errors naming `apply_tombstone`, `ClaimTombstone` and the new variants.

- [ ] **Step 3: Implement**

  - `Claim::new` stamps `CLAIM_LIVE_SCHEMA_VERSION`. `restore` keeps the version the record carries and still calls `check_schema_window(.., CURRENT_CLAIM_SCHEMA_VERSION)`.
  - `apply_tombstone` refuses a stub with `Tombstoned`; otherwise returns a clone with the statement set to the markers, `evidence` cleared, `embedding` `None`, `tombstone` set, and `schema_version` set to `CURRENT_CLAIM_SCHEMA_VERSION`. It does not touch `trust`, `transition_count` or `content_digest`.
  - `apply_transition` refuses a stub with `Tombstoned`, before the legality check.
  - `validate`, when `tombstone` is set: require the markers, empty evidence and no embedding, else `TombstoneCarriesContent`; skip the digest re-derivation. When it is not set, behave as today.
  - `to_record` and `restore` carry the new field.

- [ ] **Step 4: Run the unit tests and the crate's existing tests**

  Run: `cargo test -p rakka-agent-knowledge-graph --lib` then `cargo test -p rakka-agent-knowledge-graph --test knowledge_graph_conformance`
  Expected: PASS. The existing conformance suite must pass unchanged; a failure there means a live claim's behaviour moved.

- [ ] **Step 5: Move the compatibility row and see the currency test pass**

  Change the `claim` row of `docs/rakka-compatibility.md` to version 2.
  Run: `cargo test -p rakka-agent-knowledge-graph --test compatibility_currency`
  Expected: PASS. (It fails between Step 3 and this step; that is the test doing its job.)

- [ ] **Step 6: Falsify, then commit**

  Remove the marker check from `validate`; `a_stub_that_carries_content_is_refused` must fail. Restore it.

  ```bash
  git add crates/rakka-agent-knowledge-graph/src docs/rakka-compatibility.md
  git commit -m "Give a claim a tombstone shape and the four erasure codes (issue #68)"
  ```

---

### Task 2: The tombstone operation, in the SPI and the in-memory store

**Files:**
- Modify: `src/tombstone.rs` (the request type)
- Modify: `src/claim.rs` (`impl ClaimOperationId`, near `:206`)
- Modify: `src/store.rs` (`ClaimFilter` `:158` and `admits` `:278`, `KnowledgeGraphStore` `:655`, `ClaimLedgerEntry`, `InMemoryKnowledgeGraphStore`)
- Modify: `src/conformance.rs` (make `transition_request`, `promotion_request` and `drained_query` `pub(crate)`; declare the child module; extend `check_knowledge_graph_contract` `:1313`)
- Create: `src/conformance/erasure.rs`
- Modify: `tests/knowledge_graph_conformance.rs` (`ShallowKnowledgeGraphStore` `:125`, one test per new clause)
- Modify: `tests/goal_claim_source.rs`
- Modify: PG crate `src/lib.rs` (a temporary method body, see Step 3)

**Interfaces:**
- Consumes: Task 1's `ClaimTombstone`, `ClaimTombstoneReason`, `Claim::apply_tombstone`, `Claim::is_tombstoned`, and the codes.
- Produces:
  - `pub struct ClaimTombstoneRequest { pub claim_id: ClaimId, pub operation_id: ClaimOperationId, pub reason: ClaimTombstoneReason, pub actor: PrincipalRef, pub tombstoned_at: AgentTimestampMillis }`
  - `impl ClaimOperationId { pub fn derive_tombstone(scope: &KnowledgeSpaceScope, claim: &ClaimId, discriminator: impl AsRef<str>) -> AgentIdentityResult<Self> }`, salt `claim-tombstone`, same shape as `derive_transition`.
  - On the trait, required: `fn tombstone<'a>(&'a self, scope: &'a KnowledgeSpaceScope, request: &'a ClaimTombstoneRequest) -> ClaimFuture<'a, Claim>;`
  - `impl ClaimFilter { pub fn including_tombstoned(mut self) -> Self }`. `admits` answers `false` for a stub unless this was called.
  - In-memory ledger entries `AppliedTombstone(Box<Claim>)` and `Erased`.
  - Clauses in `conformance::erasure`, each `pub async fn name(store: &dyn KnowledgeGraphStore, scopes: ConformanceScopes)`, re-exported from `conformance`: `tombstone_withdraws_content`, `tombstoned_claims_are_hidden_from_discovery`, `a_stub_is_frozen`, `tombstone_replay_and_erased_operations`, `tombstone_isolation`.

**The operation's rule, in order** (spec 6.1.4 and 6.3):
1. Look the operation id up in the ledger. `AppliedTombstone` → return that stub. `Erased` → `OperationErased`. Any other entry → `OperationConflict`.
2. Look the claim up. Absent or out of scope → `NotFound`.
3. The claim is already a stub → `Tombstoned`.
4. Apply `apply_tombstone`, store the stub, replace the ledger entry of the claim's append and of every transition in its history with `Erased`, and record `AppliedTombstone` under the request's id.

The existing `append` and `transition` gain two ledger answers: `Erased` → `OperationErased`, and `AppliedTombstone` → `OperationConflict`.

- [ ] **Step 1: Write the five clauses** in `src/conformance/erasure.rs`, using `conformance_claim`, `transition_request` and `drained_query`.

  - `tombstone_withdraws_content`: append an edge claim, move it to `Disputed`, tombstone it with reason `Withdrawn`. The returned stub and `get` agree. The stub satisfies Task 1's kept and cleared lists. `transitions` still lists the one transition. A second claim in the scope is untouched.
  - `tombstoned_claims_are_hidden_from_discovery`: with one live and one tombstoned claim sharing a subject: a default `query` returns only the live one; `including_tombstoned()` returns both; `with_subject(original subject)` plus `including_tombstoned()` returns only the live one; `with_agent(..)` plus `including_tombstoned()` returns both. `traverse` from the subject reports the live edge only.
  - `a_stub_is_frozen`: `transition` on the stub and a `tombstone` under a new operation id both answer `claim-tombstoned`, and `get` still returns the same stub.
  - `tombstone_replay_and_erased_operations`: drive a claim to the history cap (`CLAIM_MAX_TRUST_TRANSITIONS` transitions, alternating `Disputed` and `Verified` under a non-consequential policy, as `transition_legality_and_replay` does), keeping every operation id. Tombstone it. Replaying the tombstone returns the stub. Replaying the append and each of the 32 transitions answers `claim-operation-erased`, and `get` still returns the stub. `transitions`, drained, lists 32. A tombstone under the append's operation id of a *different, live* claim answers `claim-operation-conflict`.
  - `tombstone_isolation`: a tombstone addressed under `scopes.foreign` for a claim that exists in `scopes.primary` answers `claim-not-found`, the same code a never-appended id answers, and the primary's claim is still live.

- [ ] **Step 2: Wire the clauses and see them fail**

  Add the five calls to `check_knowledge_graph_contract`, each with its own `ConformanceScopes::unique(..)`. Add one `#[tokio::test]` per clause to `tests/knowledge_graph_conformance.rs`, following the file's existing pattern.
  Run: `cargo test -p rakka-agent-knowledge-graph --test knowledge_graph_conformance`
  Expected: compile error, `tombstone` is not a member of `KnowledgeGraphStore`.

- [ ] **Step 3: Implement**

  - The trait method, the filter opt-in, the in-memory operation and ledger entries, per the rule above.
  - `ShallowKnowledgeGraphStore::tombstone` delegates to `inner`.
  - PG crate: implement `tombstone` as a temporary refusal, `ClaimError::Backend` with the message `"tombstone is not implemented for this backend yet"`, so the workspace builds. Task 5 replaces it. Between this task and Task 6 the PG crate's gated conformance tests fail; that is expected and is the only failure this plan allows.

- [ ] **Step 4: Run the conformance tests**

  Run: `cargo test -p rakka-agent-knowledge-graph --test knowledge_graph_conformance`
  Expected: PASS, including `a_backend_declaring_tighter_limits_passes_the_contract` and `the_whole_contract_passes_as_one_suite`.

- [ ] **Step 5: The goal view does not list a stub**

  Add `a_tombstoned_claim_leaves_the_goal_join` to `tests/goal_claim_source.rs`: append two goal-scoped claims with the file's `append_claim`, tombstone one, and assert `claims_for_goal` returns only the other.
  Run: `cargo test -p rakka-agent-knowledge-graph --test goal_claim_source`
  Expected: PASS with no production change. If it fails, the default exclusion in `admits` is wrong.

- [ ] **Step 6: Falsify, then commit**

  - Skip the ledger erasure in the in-memory `tombstone`: `tombstone_replay_and_erased_operations` must fail.
  - Make `admits` admit stubs by default: `tombstoned_claims_are_hidden_from_discovery` must fail.

  Restore both.

  ```bash
  git add crates/rakka-agent-knowledge-graph crates/rakka-agent-knowledge-graph-postgres
  git commit -m "Tombstone a claim through the store, with its replay ledger erased (issue #68)"
  ```

---

### Task 3: The delete operation, in the SPI and the in-memory store

**Files:**
- Modify: `src/tombstone.rs`, `src/claim.rs` (`impl ClaimOperationId`), `src/store.rs`, `src/conformance.rs`, `src/conformance/erasure.rs`
- Modify: `tests/knowledge_graph_conformance.rs`, `tests/claim_append_executor.rs`
- Modify: PG crate `src/lib.rs` (temporary body)

**Interfaces:**
- Consumes: Task 2's ledger entries and `tombstone`.
- Produces:
  - `pub struct ClaimDeleteExpectation { pub transition_count: u32, pub tombstoned: bool }` — `Debug, Clone, Copy, PartialEq, Eq`.
  - `pub struct ClaimDeleteRequest { pub claim_id: ClaimId, pub operation_id: ClaimOperationId, pub expected: Option<ClaimDeleteExpectation> }`
  - `impl ClaimOperationId { pub fn derive_delete(scope: &KnowledgeSpaceScope, claim: &ClaimId, discriminator: impl AsRef<str>) -> AgentIdentityResult<Self> }`, salt `claim-delete`.
  - On the trait, required: `fn delete<'a>(&'a self, scope: &'a KnowledgeSpaceScope, request: &'a ClaimDeleteRequest) -> ClaimFuture<'a, ()>;`
  - In-memory ledger entry `Deleted`.
  - Clauses in `conformance::erasure`: `delete_erases_a_claim`, `a_deleted_claim_cannot_be_restored_by_replay`, `a_stale_delete_changes_nothing`, `delete_isolation`, `erasure_operation_kinds`.

**The operation's rule, in order** (spec 6.2 and 6.3):
1. Ledger lookup. `Deleted` → `Ok(())`. `Erased` → `OperationErased`. Any other entry → `OperationConflict`.
2. Claim lookup. Absent or out of scope → `NotFound`.
3. If `expected` is set and its `transition_count` or `tombstoned` differs from the claim → `DeleteStale`. Record nothing.
4. Remove the claim and its transition history. Replace with `Erased` the ledger entries of the append, of every transition, and of the tombstone when the claim is a stub. Record `Deleted` under the request's id.

`append`, `transition` and `tombstone` answer `OperationConflict` for a `Deleted` entry.

- [ ] **Step 1: Write the five clauses**

  - `delete_erases_a_claim`: append, transition once, delete with `expected: None`. `get` is `None`; `transitions` is the empty page; a default `query` and one `including_tombstoned()` omit it. Separately: tombstone a second claim, then delete the stub; `get` is `None`.
  - `a_deleted_claim_cannot_be_restored_by_replay`: after a delete, replaying the append (the same `Claim` value) answers `claim-operation-erased` and `get` is still `None`. Replaying the delete answers `Ok(())`. Then the Review Focus case: the append's erased id used for a `transition`, a `tombstone` and a `delete` each answers `claim-operation-erased`.
  - `a_stale_delete_changes_nothing`: a delete expecting `transition_count` one above the claim's answers `claim-delete-stale`; `get` returns the claim unchanged. A delete expecting `tombstoned: true` on a live claim answers the same. The same operation id with the correct expectation then succeeds.
  - `delete_isolation`: as `tombstone_isolation`, for `delete`.
  - `erasure_operation_kinds`: an append's live operation id used for a `delete`, a transition's for a `tombstone`, a tombstone's for a `delete`, and a delete's for an `append`, a `transition` and a `tombstone`: each answers `claim-operation-conflict` and changes nothing.

- [ ] **Step 2: Wire them and see them fail**

  Add the calls to `check_knowledge_graph_contract` and one test each to `tests/knowledge_graph_conformance.rs`.
  Run: `cargo test -p rakka-agent-knowledge-graph --test knowledge_graph_conformance`
  Expected: compile error, `delete` is not a member of `KnowledgeGraphStore`.

- [ ] **Step 3: Implement** the trait method, the in-memory operation, the `Shallow` delegation, and a temporary PG refusal like Task 2's (`"delete is not implemented for this backend yet"`).

- [ ] **Step 4: Run the conformance tests**

  Run: `cargo test -p rakka-agent-knowledge-graph --test knowledge_graph_conformance`
  Expected: PASS.

- [ ] **Step 5: The append executor answers an erased append as a definitive refusal**

  Add `a_replayed_append_of_an_erased_claim_is_a_definitive_refusal` to `tests/claim_append_executor.rs`: execute an append through the executor, delete the claim it reports, execute the same intent again. Assert the second finding is `AgentClaimAppendFinding::Refused` with code `claim-operation-erased`, not an `Err`, and that the store holds no claim.
  Run: `cargo test -p rakka-agent-knowledge-graph --test claim_append_executor`
  Expected: PASS with no production change.

- [ ] **Step 6: Falsify, then commit**

  - Record the stale refusal in the ledger: `a_stale_delete_changes_nothing` must fail at its final successful delete.
  - Leave the append's ledger entry in place on delete: `a_deleted_claim_cannot_be_restored_by_replay` must fail.

  Restore both.

  ```bash
  git add crates/rakka-agent-knowledge-graph crates/rakka-agent-knowledge-graph-postgres
  git commit -m "Delete a claim through the store so no replay restores it (issue #68)"
  ```

---

### Task 4: The retention policy and sweep

**Files:**
- Create: `src/retention.rs`
- Create: `src/conformance/retention.rs`
- Modify: `src/lib.rs`, `src/conformance.rs`, `tests/knowledge_graph_conformance.rs`

**Interfaces:**
- Consumes: `delete`, `ClaimDeleteExpectation`, `ClaimOperationId::derive_delete`, `ClaimFilter::including_tombstoned`, `ClaimTransitionCursor::after_ordinal`.
- Produces, in `retention.rs`, public and re-exported:
  - `pub struct KnowledgeSpaceRetentionPolicy` — `Debug, Clone, PartialEq, Eq, Serialize, Deserialize`; `Default` is `retain_all()`.
    - `pub fn retain_all() -> Self`
    - `pub fn with_window(self, status: ClaimTrustStatus, millis: u64) -> Self`
    - `pub fn with_tombstone_window(self, millis: u64) -> Self`
    - `pub fn with_legal_hold(self, hold: bool) -> Self`
    - `pub fn window(&self, status: ClaimTrustStatus) -> Option<u64>`, `pub fn tombstone_window(&self) -> Option<u64>`, `pub fn legal_hold(&self) -> bool`
  - `pub const KNOWLEDGE_SPACE_RETENTION_DEFAULT_MAX_BATCH: usize = 128;`
  - `#[non_exhaustive] pub struct KnowledgeSpaceRetentionReport { pub examined: usize, pub deleted: usize, pub not_yet_due: usize, pub retained: usize, pub moved: usize, pub held: bool, pub next: Option<ClaimCursor> }` — `Debug, Clone, PartialEq, Default`.
  - `pub async fn discharge_knowledge_space_retention(store: &dyn KnowledgeGraphStore, scope: &KnowledgeSpaceScope, policy: &KnowledgeSpaceRetentionPolicy, now: AgentTimestampMillis, cursor: Option<ClaimCursor>, max_batch: usize) -> ClaimResult<KnowledgeSpaceRetentionReport>`
  - `pub struct KnowledgeSpaceRetentionSweep` with `new(store: Arc<dyn KnowledgeGraphStore>)`, `with_max_batch(self, max_batch: usize)`, and `discharge(&self, scope, policy, now, cursor)` calling the free function.
  - A crate-private variant of the free function that also takes a hook called with each due claim immediately before its `delete`. The conformance clause uses it to move a claim under the sweep. Its exact shape is the implementer's.
  - Clauses in `conformance::retention`: `retention_ages_claims_by_trust_status`, `retention_is_bounded_and_resumable`, `retention_leaves_a_claim_that_moved`.

**The sweep's rule** (spec 6.4):
1. `policy.legal_hold()` → return a report with `held: true` and every count zero. Read nothing.
2. Treat `max_batch` of 0 as 1. Page `query` from `cursor` with `ClaimFilter::matching_all().including_tombstoned()` until `max_batch` claims have been examined or the space is exhausted. `next` is a cursor after the last examined claim when more remain, else `None`.
3. For each claim, find the window and the entry instant:

   | The claim is | Window | Entered at |
   | --- | --- | --- |
   | a stub | `tombstone_window()` | `tombstone().tombstoned_at` |
   | `Proposed` | `window(Proposed)` | `created_at` |
   | otherwise | `window(trust)` | `occurred_at` of the transition whose `ordinal == transition_count` |

   No window → count `retained`. Read the last transition with one call: `transitions(scope, id, ClaimTransitionCursor::after_ordinal(transition_count - 1).with_limit(1))`. If the page is empty, its entry's ordinal is not `transition_count`, or `next` is set, count `moved`.
4. Due means `entered_at.saturating_add(window) <= now`. Not due → count `not_yet_due`.
5. Due → `delete` with `operation_id = derive_delete(scope, claim_id, "retention")` and `expected = Some(ClaimDeleteExpectation { transition_count, tombstoned })` as read. `Ok` → count `deleted`. `DeleteStale` or `NotFound` → count `moved`. Any other error → return it.

- [ ] **Step 1: Write the policy unit tests** in `retention.rs`.

  - `the_default_policy_retains_everything`: `Default::default() == retain_all()`; every `ClaimTrustStatus::ALL` member has `window(..) == None`; `tombstone_window() == None`; `legal_hold() == false`.
  - `a_window_is_set_per_status`: `with_window(Proposed, 5)` leaves `Verified` at `None`.
  - `a_policy_round_trips_through_serde`.

- [ ] **Step 2: Write the three clauses** in `src/conformance/retention.rs`, calling `discharge_knowledge_space_retention`. `conformance_claim` stamps `created_at = 1` and `transition_request` stamps `occurred_at = 10`; choose `now` and windows around those.

  - `retention_ages_claims_by_trust_status`: one live claim each resting `Proposed`, `Verified`, `Disputed`, `Retracted`, plus two stubs tombstoned at `20`, one `Proposed` and one `Verified`. Policy: `Proposed` window 100, `Retracted` window 100, tombstone window 100, nothing for `Verified` or `Disputed`.
    - At `now = 100`: nothing is deleted. `not_yet_due == 4`, `retained == 2`.
    - At `now = 101`: the live `Proposed` claim (1 + 100) is deleted and nothing else. The `Proposed` stub survives, which proves a stub is judged by the tombstone window and not by its trust status.
    - At `now = 110`: the `Retracted` claim (10 + 100) is deleted. This proves the clock is the last transition, not `created_at`.
    - At `now = 120`: both stubs are deleted, including the `Verified` one whose trust status has no window.
    - The `Verified` and `Disputed` claims survive every pass.
    - With `with_legal_hold(true)` and `now = u64::MAX`: `held`, `examined == 0`, nothing deleted.
    - A window of `u64::MAX` at `now = u64::MAX - 1` deletes nothing and does not panic. `now = 0` with a window of 0 deletes nothing whose entry instant is later than 0.
  - `retention_is_bounded_and_resumable`: five due claims, `max_batch = 2`. Three calls, each fed the previous `next`, delete 2, 2 and 1; the last returns `next == None`. Each call's cursor points at a claim that call deleted, and no claim is skipped or examined twice. `max_batch = 0` examines one. Running a full pass again deletes nothing and reports `examined == 0`. A pass over `scopes.foreign` reports all zeros.
  - `retention_leaves_a_claim_that_moved`: with the hook, (a) transition a due `Proposed` claim to `Disputed` just before its delete, and (b) tombstone a due `Proposed` claim just before its delete. Both are counted `moved`; (a) is still live and `Disputed`, (b) is still a stub. A later pass at a `now` where (a) is due under a `Disputed` window deletes it under the same derived operation id.

- [ ] **Step 3: Wire them and see them fail**

  Add the three calls to `check_knowledge_graph_contract` and three tests to `tests/knowledge_graph_conformance.rs`.
  Run: `cargo test -p rakka-agent-knowledge-graph --test knowledge_graph_conformance`
  Expected: compile error naming `discharge_knowledge_space_retention`.

- [ ] **Step 4: Implement** `retention.rs` per the rule. Give the module a header that states what erasure does not reach, as spec 6.6 lists it, in the manner of `crates/rakka-agent/src/memory_retention.rs`.

- [ ] **Step 5: Run**

  Run: `cargo test -p rakka-agent-knowledge-graph --lib retention` then `cargo test -p rakka-agent-knowledge-graph --test knowledge_graph_conformance`
  Expected: PASS, including the shallow backend, whose page bound of 2 exercises paging inside a batch.

- [ ] **Step 6: Falsify, then commit**

  - Read `created_at` as the clock for every claim: the `now = 101` assertion must fail (the `Retracted` claim goes early).
  - Send `expected: None`: `retention_leaves_a_claim_that_moved` must fail.
  - Ignore the legal hold: the hold assertion must fail.

  Restore all three.

  ```bash
  git add crates/rakka-agent-knowledge-graph
  git commit -m "Sweep a knowledge space's claims out by a window per trust status (issue #68)"
  ```

---

### Task 5: PostgreSQL — the migration and the tombstone

**Files:**
- Modify: PG crate `src/lib.rs` (`MIGRATION_SQL` `:135`, the read statements `:230`–`:286`, `LedgerHit` `:287`, `ledger_hit` `:332`, `transition` `:683`, `replayed_append` `:868`, `replayed_transition` `:885`, `decode_claim_row` `:942`, the module header)
- Create: PG crate `src/erasure.rs` (private module)
- Modify: PG crate `tests/postgres_backend_proofs.rs`, `tests/postgres_conformance.rs`

**Interfaces:**
- Consumes: everything Tasks 1–3 produce.
- Produces:
  - `pub const TOMBSTONE_FENCE_TRANSITION_COUNT: i32 = i32::MAX;` — the value a stub row's `transition_count` column holds (spec 6.5).
  - Schema: `rakka_agent_knowledge_claim.tombstoned BOOLEAN NOT NULL DEFAULT FALSE`; the ledger's `kind` check allows exactly `'append'`, `'transition'`, `'tombstone'`, `'delete'`, `'erased'`.
  - `LedgerHit::{Tombstone(Vec<u8>), Deleted, Erased}`. An unrecognized `kind` is a `ClaimError::Backend`, no longer an append.
  - `PostgresKnowledgeGraphStore::tombstone`, real.

**Schema rules:**
- `MIGRATION_SQL` creates fresh tables in the new shape, and upgrades existing ones: `ADD COLUMN IF NOT EXISTS` for `tombstoned`, and the `kind` check replaced only when its current definition does not already allow `erased` (a `DO` block reading `pg_get_constraintdef`). A second `migrate` must issue no table rewrite.
- A stub row: `subject` and `predicate` hold `CLAIM_TOMBSTONE_MARKER`, `object_node` is `NULL`, `tombstoned` is `TRUE`, `transition_count` is `TOMBSTONE_FENCE_TRANSITION_COUNT`, `trust` is unchanged, `record` is the stub.
- An erased ledger row: `kind = 'erased'`, `result = ''` (empty bytes, never `NULL`).
- Every read statement also selects `tombstoned`. `decode_claim_row` refuses with `drift_error` when the column disagrees with `claim.is_tombstoned()`. The traversal statement adds `AND NOT tombstoned`.

**The tombstone, in `erasure.rs`:** a bounded compare-and-set loop like `transition`'s, at most `TRANSITION_CAS_MAX_ATTEMPTS` attempts.
1. `ledger_hit` first, mapping entries as the in-memory store does.
2. Read the claim head. Absent → `NotFound`. A stub → `Tombstoned`. For a live row, the column must equal the record's count, else `drift_error`.
3. Read the claim's transition records and collect their operation ids, plus the claim's own.
4. One statement, fenced on the `transition_count` column as read and on no existing ledger row for the request's id: update the claim row to the stub shape, set the collected ledger rows to erased, insert the `'tombstone'` ledger row holding the stub's bytes. It reports `replayed` and `applied` as the existing statements do.
5. Not applied and not replayed → a concurrent write won; loop.

`transition` changes: after decoding the head, a stub answers `Tombstoned` before the fence comparison. `replayed_append` and `replayed_transition` answer `OperationErased` for `'erased'`.

- [ ] **Step 1: Write the failing proofs** in `tests/postgres_backend_proofs.rs`. Each skips when the DSN is unset, like its neighbours. Put three frozen literals at the top of the file, copied byte for byte from `git show d12b0f8:crates/rakka-agent-knowledge-graph-postgres/src/lib.rs`: `MIGRATION_SQL_D12B0F8`, `APPEND_SQL_D12B0F8`, `TRANSITION_SQL_D12B0F8`.

  - `postgres_migrates_the_earlier_schema_with_rows_when_dsn_is_set`: in a private schema (`CREATE SCHEMA` and `SET search_path`, as `postgres_concurrent_migrators_do_not_race_when_dsn_is_set` does), apply `MIGRATION_SQL_D12B0F8`, append two claims and one transition with the frozen statements, then run the new `migrate` twice. Through the new store: both claims read back equal, the transition lists, a new append works, and inserting a ledger row of kind `'erased'` is accepted.
  - `postgres_an_earlier_binary_still_appends_after_the_migration_when_dsn_is_set`: against the migrated schema, `APPEND_SQL_D12B0F8` reports `applied`, and the new store reads the claim as live.
  - `postgres_a_stub_row_holds_markers_and_the_fence_when_dsn_is_set`: after a tombstone, select the row's columns directly and assert the stub-row rule above. Then set `tombstoned = FALSE` on it by hand: `get` and `query` answer a `claim-backend-failed` drift refusal.
  - `postgres_tombstone_leaves_no_statement_bytes_when_dsn_is_set`: append a claim whose subject, predicate and value object are three distinctive strings, transition it once with a reason that does not contain them, tombstone it. Select every row of the three tables for that scope and assert none of the three strings appears in any text column or any `BYTEA` payload.
  - `postgres_an_earlier_transition_statement_cannot_write_over_a_stub_when_dsn_is_set`: read a live claim's count, tombstone it, then run `TRANSITION_SQL_D12B0F8` bound exactly as an earlier binary would have bound it from the live read. Assert `applied` is false, and that the claim row, the transition rows and the ledger rows are byte-identical before and after.
  - `postgres_concurrent_tombstone_and_transition_stay_coherent_when_dsn_is_set`: race a tombstone and a transition on one claim from two connections, many times. Every run ends in one of two states: the transition applied first and the stub lists it with its ledger row erased, or the tombstone applied first and the transition answered `claim-tombstoned`. In both, the statement strings appear in no row.

- [ ] **Step 2: Add the tombstone clauses to `tests/postgres_conformance.rs`**, one test per clause of Task 2, in the file's existing pattern.

- [ ] **Step 3: Run and see them fail**

  Run: `RAKKA_POSTGRES_TEST_DSN=<dsn> cargo test -p rakka-agent-knowledge-graph-postgres --test postgres_backend_proofs -- --nocapture`
  Expected: the new proofs fail on the temporary refusal or the missing column.

- [ ] **Step 4: Implement** the schema, the read changes, the ledger kinds and the tombstone. Rewrite the `MIGRATION_SQL` doc comment and the module header: both say this crate has no deletion path.

- [ ] **Step 5: Run**

  Run: `RAKKA_POSTGRES_TEST_DSN=<dsn> cargo test -p rakka-agent-knowledge-graph-postgres --test postgres_backend_proofs`, then the same for `--test postgres_conformance`.
  Expected: every proof in `postgres_backend_proofs` passes, including the ones that existed before. In `postgres_conformance`, the five tombstone clause tests and every clause test that existed before pass. The delete and retention clauses, and the whole-contract test, still fail on Task 3's temporary refusal.

- [ ] **Step 6: Falsify, then commit**

  - Leave the true count in a stub row's `transition_count`: `postgres_an_earlier_transition_statement_cannot_write_over_a_stub_when_dsn_is_set` must fail.
  - Skip the ledger update in the tombstone statement: `postgres_tombstone_leaves_no_statement_bytes_when_dsn_is_set` must fail.

  Restore both.

  ```bash
  git add crates/rakka-agent-knowledge-graph-postgres
  git commit -m "Tombstone a claim in PostgreSQL behind a fence no earlier binary can match (issue #68)"
  ```

---

### Task 6: PostgreSQL — the delete, and the earlier-binary replay proofs

**Files:**
- Modify: PG crate `src/erasure.rs`, `src/lib.rs`
- Modify: PG crate `tests/postgres_backend_proofs.rs`, `tests/postgres_conformance.rs`

**Interfaces:**
- Consumes: Task 5's schema, ledger kinds, fence value and loop shape.
- Produces: `PostgresKnowledgeGraphStore::delete`, real. No temporary refusal remains in the crate.

**The delete, in `erasure.rs`:** the same bounded loop.
1. `ledger_hit` first: `'delete'` → `Ok(())`; `'erased'` → `OperationErased`; any other kind → `OperationConflict`.
2. Read the claim head. Absent → `NotFound`.
3. Compare `request.expected`, when set, with the decoded claim's `transition_count()` and `is_tombstoned()`. A difference → `DeleteStale`, nothing written.
4. Collect the operation ids: the claim's, each transition's, and the tombstone's when the claim is a stub.
5. One statement, fenced on the row's `transition_count` column as read and on no existing ledger row for the request's id. The column is the count for a live row and the fence value for a stub, so this one fence also catches a tombstone that lands between the read and the write. The statement: delete the claim row, delete its transition rows, set the collected ledger rows to erased, insert the `'delete'` ledger row with empty bytes.
6. Not applied and not replayed → loop, which re-reads and re-checks the expectation.

- [ ] **Step 1: Write the failing proofs** in `tests/postgres_backend_proofs.rs`.

  - `postgres_delete_leaves_no_statement_bytes_when_dsn_is_set`: as Task 5's proof, with a reason that *does* contain one of the strings, since a delete removes transition records too. After the delete no row of the three tables for the scope contains any of the strings. Repeat for a claim that was tombstoned first.
  - `postgres_an_earlier_binary_replaying_an_erased_append_cannot_restore_it_when_dsn_is_set`: delete a claim, then run `APPEND_SQL_D12B0F8` with the erased operation id and the original record. Assert `replayed` is true, `applied` is false, `replay_kind` is not `'append'`, `replay_result` is empty and non-null, and the claim row is absent. (At `d12b0f8`, `replayed_append` maps that row to `claim-operation-conflict`.)
  - `postgres_an_earlier_transition_statement_against_a_deleted_claim_applies_nothing_when_dsn_is_set`: `TRANSITION_SQL_D12B0F8`, bound from a read taken before the delete, reports `applied` false and inserts no transition row and no ledger row.
  - `postgres_concurrent_delete_and_transition_stay_coherent_when_dsn_is_set`: race a delete with `expected: None` against a transition. Every run ends with the claim absent, no transition rows for it, and every ledger row for its operations erased, or the transition answering `claim-not-found`.
  - `postgres_a_stale_delete_writes_nothing_when_dsn_is_set`: a delete with a wrong expectation leaves the three tables byte-identical and inserts no ledger row.

- [ ] **Step 2: Add the remaining clauses to `tests/postgres_conformance.rs`**: Task 3's five and Task 4's three.

- [ ] **Step 3: Run and see them fail**

  Run: `RAKKA_POSTGRES_TEST_DSN=<dsn> cargo test -p rakka-agent-knowledge-graph-postgres`
  Expected: the new proofs and clauses fail on the temporary refusal.

- [ ] **Step 4: Implement** the delete and remove the temporary refusal.

- [ ] **Step 5: Run both PG test files in full**

  Run: `RAKKA_POSTGRES_TEST_DSN=<dsn> cargo test -p rakka-agent-knowledge-graph-postgres -- --nocapture`
  Expected: PASS, every test, including the whole-contract conformance test. Confirm from the output that no test printed a skip.

- [ ] **Step 6: Falsify, then commit**

  - Delete the claim's ledger rows instead of erasing them: `postgres_an_earlier_binary_replaying_an_erased_append_cannot_restore_it_when_dsn_is_set` must fail, because the append applies.
  - Skip the expectation comparison (step 3 of the rule): the conformance clauses `a_stale_delete_changes_nothing` and `retention_leaves_a_claim_that_moved` must fail on PostgreSQL.
  - Drop the statement's fence on `transition_count`: `postgres_concurrent_delete_and_transition_stay_coherent_when_dsn_is_set` must fail within its repetitions. If it does not, raise the repetition count until it does, and keep that count.

  Restore all three.

  ```bash
  git add crates/rakka-agent-knowledge-graph-postgres
  git commit -m "Delete a claim in PostgreSQL and prove no earlier binary restores it (issue #68)"
  ```

---

### Task 7: Docs, changelog and whole-branch validation

**Files:**
- Modify: `docs/plans/rakka-agent/spec.md` (section 13.4)
- Modify: `docs/rakka-agent-security-validation-matrix.md` (the retention row `:146`, the "Owed" entry `:175`, the attack rows `:241`–`:242`, the test commands near `:264`)
- Modify: `docs/rakka-agents.md` (the knowledge-graph passage `:377`, the owed note `:534`)
- Modify: `docs/rakka-compatibility.md`
- Modify: `docs/plans/rakka-agent/implementation-plan.md` (the "Owed onward" entry near `:5113`)
- Modify: `CHANGELOG.md`
- Modify: `docs/superpowers/specs/2026-11-13-knowledge-graph-retention-tombstone-deletion-design.md` (section 12)

- [ ] **Step 1: Write the docs.** Each must say:

  - **spec 13.4:** a paragraph defining tombstone, delete and the retention sweep, and that a tombstone keeps provenance and the transition history.
  - **Security matrix:** the graph's retention cell names the two operations and the sweep with their proving clauses; the #68 "Owed" entry is removed; the two attack rows name the graph's proofs. Add what erasure does not reach (spec 6.6) where the matrix lists exposures.
  - **`rakka-agents.md`:** how to tombstone, delete and sweep; the roll order of spec section 7, migrate first; the spec 6.6 list; that erased ledger rows are kept for good.
  - **`rakka-compatibility.md`:** the four codes; the two new required methods on `KnowledgeGraphStore` as a source-breaking change for outside implementors; the staged version (live claims at 1, stubs at 2) and its consequence for a bump to 3; what an earlier binary answers for a stub (`schema-version-ahead`) and for a replay of an erased operation (`claim-operation-conflict`); the schema change and the stub row's fence value.
  - **Implementation plan:** the #68 entry marked done with the proving files.
  - **`CHANGELOG.md`:** one entry under the unreleased heading.

- [ ] **Step 2: Fill spec section 12, "As built".** Record every name that differs from the spec or this plan, every ruling made while building, and which test each falsification failed.

- [ ] **Step 3: Run the doc-holding tests**

  Run: `cargo test -p rakka-agent-knowledge-graph --test compatibility_currency` and `cargo test -p rakka-agent --all-features --test compatibility_currency`
  Expected: PASS. If another test in `rakka-agent` parses a doc this task edited and fails, fix the doc, not the test.

- [ ] **Step 4: Whole-branch validation**

  ```bash
  scripts/validate.sh > "$LOG_DIR/validate-68.log" 2>&1; echo "exit=$?"
  scripts/package-check.sh > "$LOG_DIR/package-68.log" 2>&1; echo "exit=$?"
  ```

  `LOG_DIR` is any directory outside the repository. Expected: `exit=0` twice. Redirect to a file and read `$?` as shown; piping `validate.sh` into another command reports that command's status. If the one-shot workspace test is killed under load, run the failing crates one at a time and report which.

- [ ] **Step 5: Both PG suites once more, on the final tree**

  Run: `RAKKA_POSTGRES_TEST_DSN=<dsn> cargo test -p rakka-agent-knowledge-graph-postgres`
  Expected: PASS, no skips.

- [ ] **Step 6: Commit**

  ```bash
  git add docs CHANGELOG.md
  git commit -m "Document knowledge-graph retention, tombstone and deletion (issue #68)"
  ```

---

## After the plan

Not tasks for an implementer:

- A whole-branch review, then a pull request against `main` whose body says `Closes #68`.
- After the merge, the report on issue #68 that spec section 11 describes, with the merge commit as the host's pin.
