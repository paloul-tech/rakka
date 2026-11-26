# Live claim encoding pinned (issue #139) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Hold a live claim's encoding, and the bytes the PostgreSQL backend stores for it, to the bytes the build before tombstones (`d12b0f8`) wrote.

**Architecture:** Three fixture claims built through the public API are encoded once by the earlier build, in a throwaway worktree, and kept as string literals in a new integration test that compares the current build's encoding and decoding against them. A gated PostgreSQL proof holds the backend's stored `record` and append-ledger `result` to `serde_json::to_vec(&claim.to_record())`, which links the backend to the literals without a second copy. Tests and docs only; no product code changes.

**Tech Stack:** Rust, `serde_json`, `tokio-postgres` (existing dev-dependency of the PostgreSQL crate).

**Spec:** `docs/superpowers/specs/2026-11-25-live-claim-encoding-pinned-design.md`

## Global Constraints

- Branch `live-claim-encoding-pinned`, off `main` at `5379856b`.
- The literals come from running the build at `d12b0f8`. Never from the current build, never hand-written.
- If the current build's bytes differ from the `d12b0f8` bytes, stop and report to the owner. Do not adjust a literal or the encoding.
- Every JSON object inside an inline value fixture has its keys in sorted order, so the bytes do not depend on whether `serde_json`'s `preserve_order` feature is on.
- No product code changes. No new dependency.
- `cargo clean` in the repo between tasks (disk pressure; mandatory on this machine). Never while a cargo run is live.
- Run tests per crate, not the one-shot workspace test.
- Validation: `scripts/validate.sh > <scratchpad>/validate.log 2>&1; echo $?`. Do not pipe it into `tail`, which reports tail's exit code.

## Review Focus

- A literal accidentally produced by the current build. The test would prove nothing about compatibility. The reference run must be visibly at `d12b0f8` (`git -C <worktree> rev-parse --short HEAD` printed beside the output).
- A literal mangled on its way into source: a dropped character, an escaped quote, a rustfmt rewrap. Task 1 checks each JSON payload is present in the source byte for byte.
- A test that cannot fail. Task 1 falsifies it: rename a serde key or add a serialized field locally, watch it fail, revert.
- The PostgreSQL proof comparing decoded-and-re-encoded bytes. That passes whatever the backend stores. Task 2 reads the raw columns and compares them to an encoding computed from the claim the test built.
- A default-valued field hiding a change. The furnished claim sets every optional field. The two value claims leave them unset, so both the `Some` and the `None`/empty encodings are pinned.

---

### Task 1: The golden-bytes test

**Files:**
- Create: `crates/rakka-agent-knowledge-graph/tests/live_claim_encoding.rs`
- Scratch (not committed): a worktree at `d12b0f8` under the session scratchpad, with its own `CARGO_TARGET_DIR`

**Interfaces:**
- Produces: nothing other tasks import. Task 3's docs name this file and its three tests.

The file has two parts, in this order:

1. **The fixture section.** The module doc comment, the imports the fixtures need, and the three fixture functions. It ends at a line that reads exactly `// ---- The bytes the build at d12b0f8 wrote ----`. Everything above that line must compile unchanged against `d12b0f8`. So it must not name `CLAIM_LIVE_SCHEMA_VERSION`, `ClaimTombstone`, or anything else #68 added. The public claim API it uses (`Claim::new`, `with_evidence`, `with_access`, `with_embedding`, `with_policy`, `apply_transition`, `to_record`, `ClaimProvenance` builders) is identical at both commits. That was checked.
2. **The literals and the tests**, below the marker. Imports used only here go in a second `use` block below the marker.

- [ ] **Step 1: Write the fixture section**

Functions, all private:

- `fn scope() -> KnowledgeSpaceScope`: tenant `acme`, space `support-kb`.
- `fn furnished_artifact(id: &str) -> ArtifactRef`: every field set. `kind: ArtifactKind::File`, `uri: format!("s3://evidence/{id}")`, `checksum: Some(format!("sha256:{id}"))`, `content_type: Some("application/json")`, `byte_len: Some(64)`, `retention_class: Some("legal-hold-7y")`, `encryption: Some(ArtifactEncryptionRef { algorithm: "aes-256-gcm", key_ref: "kms://keys/evidence", context: {"purpose": "evidence"} })`, `redaction: RedactionStatus::Redacted`, `created_at: AgentTimestampMillis::new(1_700_000_000_000)`, `metadata: {"source": "ticket-system"}`. `ArtifactEncryptionRef` and `AgentAttributes` come from `rakka_agent_workflow`.
- `fn furnished_edge_claim() -> Claim`:
  - operation `ClaimOperationId::derive_append(&scope(), "edge-claim")`; subject `customer-1`; predicate `prefers`; object `ClaimObject::Node("channel-email")`.
  - provenance: `ClaimProvenance::for_agent("scout")` with all five builders: goal `goal-1`, task `task-1`, run `task-1-gen-1`, delegation `delegation-1`, effect `AgentEffectId::new("effect-1")`.
  - `confidence_bps` 7_500; `MemoryClassification::Sensitive`; `created_at` 1_700_000_000_001.
  - then `.with_evidence(vec![furnished_artifact("a"), furnished_artifact("b")])` and `.with_embedding(MemoryEmbeddingRef { model: "embedder-v1", dimensions: 3, version: AgentRevisionNumber::new(2) })`.
  - then `.with_access` with read capabilities `read-pii` and `read-support`, and `.with_policy(AgentPolicyRef "retention-v1", Some(AgentRevisionNumber::new(3)))`.
  - then `.apply_transition(ClaimTrustStatus::Disputed)`.
- `fn inline_value_claim() -> Claim`: operation `derive_append(&scope(), "inline-claim")`, subject `customer-1`, predicate `prefers-hours`. Object `ClaimObject::Value(AgentTaskContent::inline(json!({"days": ["mon", "tue"], "since": 2024, "window": {"end": "17:00", "start": "09:00"}})))`, with keys sorted at every level. Provenance for agent `scout` only, confidence 6_000, `MemoryClassification::Unclassified`, `created_at` 1_700_000_000_002. No builders.
- `fn artifact_value_claim() -> Claim`: operation `derive_append(&scope(), "artifact-claim")`, subject `customer-1`, predicate `has-contract`, object `ClaimObject::Value(AgentTaskContent::artifact(furnished_artifact("contract")))`. Provenance for agent `scout` only, confidence 9_000, `MemoryClassification::Unclassified`, `created_at` 1_700_000_000_003. No builders.

Every fallible constructor is unwrapped with `.expect("...")`, in the style of the crate's existing tests. Add the marker line after the last function.

- [ ] **Step 2: Produce the reference bytes at `d12b0f8`**

```sh
SP=<session scratchpad>
git worktree add --detach "$SP/rakka-d12b0f8" d12b0f8
git -C "$SP/rakka-d12b0f8" rev-parse --short HEAD          # must print d12b0f80
sed -n '1,/^\/\/ ---- The bytes the build at d12b0f8 wrote ----$/p' \
  crates/rakka-agent-knowledge-graph/tests/live_claim_encoding.rs \
  > "$SP/rakka-d12b0f8/crates/rakka-agent-knowledge-graph/tests/reference_encoding.rs"
```

Append one test to that copy, `print_reference_encodings`. For each fixture, as `(NAME, claim)` with names `FURNISHED_EDGE_CLAIM`, `INLINE_VALUE_CLAIM`, `ARTIFACT_VALUE_CLAIM`, it:

- asserts `serde_json::to_string(&claim)` equals `serde_json::to_string(&claim.to_record())` (the two structs agree at `d12b0f8`);
- writes the string, with no trailing newline, to `$SP/reference/<NAME>.json`;
- prints `const <NAME>: &str = r#"<json>"#;`.

Then run it:

```sh
CARGO_TARGET_DIR="$SP/target-d12b0f8" cargo test --manifest-path "$SP/rakka-d12b0f8/Cargo.toml" \
  -p rakka-agent-knowledge-graph --test reference_encoding -- --nocapture
```

Expected: PASS, with three `const` lines printed. If the fixture section does not compile at `d12b0f8`, fix the fixture section in the real file (not just the copy), re-copy, and re-run. The real file's fixture section must stay identical to what ran.

- [ ] **Step 3: Remove the worktree**

```sh
git worktree remove --force "$SP/rakka-d12b0f8" && rm -rf "$SP/target-d12b0f8"
git worktree list    # only the main checkout
```

Keep `$SP/reference/*.json` until Step 5 passes.

- [ ] **Step 4: Write the literals and the tests below the marker**

Paste the three printed `const` lines below the marker, then a short comment above them. It says the strings were printed by running the fixture section above at `d12b0f8` (the merge of #129, the last commit before #68), and that they must never be regenerated from a later build.

Then write one helper and three tests:

```rust
/// What a failure here means, for whoever meets one.
const BROKEN_PROMISE: &str = "a live claim no longer encodes to the bytes the build \
    before tombstones (d12b0f8) wrote. docs/rakka-compatibility.md (the issue #68 \
    entry) promises it does, and a binary from before #68 reads live claims only \
    because it holds. Do not update the literal: restore the encoding, or make the \
    change a schema bump with its own rollout";

fn assert_pinned(name: &str, claim: &Claim, literal: &str)
```

`assert_pinned` asserts, each with `"{name}: {BROKEN_PROMISE}"` as the message:

- `serde_json::to_string(claim)` equals `literal`;
- `serde_json::to_string(&claim.to_record())` equals `literal`;
- `serde_json::from_str::<Claim>(literal)` equals `*claim`;
- `Claim::restore(serde_json::from_str::<ClaimRecord>(literal))` equals `*claim`. This is the PostgreSQL decode path;
- the decoded claim's `schema_version()` equals `CLAIM_LIVE_SCHEMA_VERSION`, and its `.get()` is `1`;
- the decoded claim's `tombstone()` is `None`.

Tests:

- `a_furnished_edge_claim_encodes_to_the_bytes_written_before_tombstones`: `assert_pinned("furnished edge claim", &furnished_edge_claim(), FURNISHED_EDGE_CLAIM)`. Also assert the claim is `Disputed` with `transition_count() == 1`, so the fixture keeps testing what it claims to.
- `an_inline_value_claim_encodes_to_the_bytes_written_before_tombstones`
- `an_artifact_value_claim_encodes_to_the_bytes_written_before_tombstones`

- [ ] **Step 5: Check the literals arrived byte for byte, then run**

```sh
for f in "$SP"/reference/*.json; do grep -c -F -f "$f" crates/rakka-agent-knowledge-graph/tests/live_claim_encoding.rs; done
```

Expected: `1` three times. Then:

```sh
cargo test -p rakka-agent-knowledge-graph --test live_claim_encoding
```

Expected: 3 passed. If a byte comparison fails, the promise is already broken: **stop and report to the owner** with both strings. Do not edit the literal.

- [ ] **Step 6: Falsify**

Temporarily add `#[serde(rename = "subject_node")]` to `subject` on both `Claim` and `ClaimRecord` in `src/claim.rs`. Run the test and confirm all three fail on the byte comparison. Revert. Then temporarily remove `skip_serializing_if = "Option::is_none"` from `tombstone` on `Claim` and `ClaimRecord`, and confirm all three fail. Revert. `git diff --stat src/` must be empty afterwards.

- [ ] **Step 7: fmt, clippy, and the format check again**

```sh
cargo fmt --all
cargo clippy -p rakka-agent-knowledge-graph --all-targets --all-features -- -D warnings
for f in "$SP"/reference/*.json; do grep -c -F -f "$f" crates/rakka-agent-knowledge-graph/tests/live_claim_encoding.rs; done
cargo test -p rakka-agent-knowledge-graph --test live_claim_encoding
```

Expected: clippy clean, `1` three times (rustfmt may move a literal onto its own line but must not change it), 3 passed.

- [ ] **Step 8: Commit**

```bash
git add crates/rakka-agent-knowledge-graph/tests/live_claim_encoding.rs
git commit -m "Pin a live claim's encoding to the bytes written before tombstones (issue #139)"
```

(with the session's `Co-Authored-By` trailer)

---

### Task 2: The PostgreSQL backend stores those bytes

**Files:**
- Modify: `crates/rakka-agent-knowledge-graph-postgres/tests/postgres_backend_proofs.rs`. Add the test, plus one fixture function, and add the new property to the module doc's list.

**Interfaces:**
- Consumes: the file's existing `store()`, `client()`, `transition_request(scope, claim_id, discriminator, to)`; `ConformanceScopes::unique(label)`.
- Produces: the test name `postgres_stores_a_live_claim_as_its_canonical_record_bytes_when_dsn_is_set`, which Task 3's docs cite.

- [ ] **Step 1: Write the test**

Add `fn furnished_claim(scope: &KnowledgeSpaceScope) -> Claim`. It is the same shape as Task 1's furnished edge claim (every optional field set, `furnished`-style artifacts) but built in the given scope, with operation discriminator `live-bytes` and no transition: an append must be `Proposed`. It cannot share Task 1's fixture, because the claim id derives from the scope, and this test's scope is unique per run.

```rust
#[tokio::test]
async fn postgres_stores_a_live_claim_as_its_canonical_record_bytes_when_dsn_is_set()
```

1. `store()` (return early without a DSN), a raw `client()`, `ConformanceScopes::unique("pg-live-bytes")`, `claim = furnished_claim(scope)`.
2. `store.append(scope, &claim)`. Let `expected = serde_json::to_vec(&claim.to_record())`.
3. Read the claim row's raw bytes, `SELECT record FROM rakka_agent_knowledge_claim WHERE tenant = $1 AND space = $2 AND claim_id = $3`, and assert they equal `expected`. Message: "the backend stores a live claim as its record's serde_json bytes, which tests/live_claim_encoding.rs in rakka-agent-knowledge-graph holds to the bytes written before tombstones".
4. Read the append ledger row's raw bytes, `SELECT kind, result FROM rakka_agent_knowledge_claim_op WHERE tenant = $1 AND space = $2 AND operation_id = $3`. Assert `kind == "append"` and `result == expected`.
5. `store.transition(scope, &transition_request(scope, &claim.claim_id, "live-bytes-dispute", ClaimTrustStatus::Disputed), &ClaimPromotionPolicy::default(), AgentTimestampMillis::new(10))`. Let `moved = claim.apply_transition(ClaimTrustStatus::Disputed)`. Re-read the claim row's `record` and assert it equals `serde_json::to_vec(&moved.to_record())`. The transition path writes the record too.
6. Assert `store.get(scope, &claim.claim_id)` returns `Some(moved)`.

Both `expected` values are computed from claims the test built, never from a decoded row.

- [ ] **Step 2: Run it against a live database, and without one**

```sh
RAKKA_POSTGRES_TEST_DSN=postgres://postgres:postgres@localhost:5432/postgres \
  cargo test -p rakka-agent-knowledge-graph-postgres --test postgres_backend_proofs \
  postgres_stores_a_live_claim -- --nocapture
cargo test -p rakka-agent-knowledge-graph-postgres --test postgres_backend_proofs postgres_stores_a_live_claim
```

Expected: PASS against the local `postgres` container on 5432 (no pgvector needed); PASS (silent skip) without the DSN. If 5432 is down, ask before starting a container.

- [ ] **Step 3: Falsify**

Temporarily change the private `encode` in `crates/rakka-agent-knowledge-graph-postgres/src/lib.rs` to `serde_json::to_vec_pretty`, run with the DSN, and confirm the test fails at step 3. Revert, and check that `git diff --stat src/` is empty.

- [ ] **Step 4: Run the whole file with the DSN, then clippy**

```sh
RAKKA_POSTGRES_TEST_DSN=postgres://postgres:postgres@localhost:5432/postgres \
  cargo test -p rakka-agent-knowledge-graph-postgres --test postgres_backend_proofs
cargo fmt --all && cargo clippy -p rakka-agent-knowledge-graph-postgres --all-targets --all-features -- -D warnings
```

Expected: all pass, clippy clean.

- [ ] **Step 5: Commit**

```bash
git add crates/rakka-agent-knowledge-graph-postgres/tests/postgres_backend_proofs.rs
git commit -m "Hold the bytes PostgreSQL stores for a live claim to its record's encoding (issue #139)"
```

---

### Task 3: The two nits, the docs, the changelog

**Files:**
- Modify: `crates/rakka-agent-knowledge-graph/src/claim.rs`, the unit-test helper `fn tombstone(discriminator: &str)` (about line 1451)
- Modify: `crates/rakka-agent-knowledge-graph/src/store.rs:12-15` (the module doc paragraph)
- Modify: `docs/rakka-compatibility.md`, the issue #68 entry (about line 303)
- Modify: `CHANGELOG.md`, Unreleased → `### Validation` (about line 680)

**Interfaces:**
- Consumes: the file and test names from Tasks 1 and 2.

- [ ] **Step 1: The helper derives a tombstone id**

In the `tombstone` helper, replace `ClaimOperationId::derive_transition(&scope(), &claim.claim_id, discriminator)` with `ClaimOperationId::derive_tombstone(&scope(), &claim.claim_id, discriminator)`.

- [ ] **Step 2: Rewrap `store.rs:14`**

Rewrap the module-doc paragraph that holds the 112-character line ("...serve them is not a conformant backend, so they are not "capabilities". Only genuinely optional features are") to the width of its neighbours, about 80 characters. Change no words.

- [ ] **Step 3: Run the crate's unit tests**

```sh
cargo test -p rakka-agent-knowledge-graph --lib
```

Expected: all pass. Every tombstone unit test, `a_tombstone_clears_the_statement_and_keeps_the_audit_fields` included, passes with the new id domain.

- [ ] **Step 4: The compatibility doc names what holds the promise**

In the #68 entry, after the sentence "...a live claim is still written at version 1 and encodes to the same bytes as before (`CLAIM_LIVE_SCHEMA_VERSION`).", add:

> `tests/live_claim_encoding.rs` in `rakka-agent-knowledge-graph` holds that byte for byte: three claims, one with every optional field set, are compared against literals the build at `d12b0f8` wrote, and decoded from them. `postgres_stores_a_live_claim_as_its_canonical_record_bytes_when_dsn_is_set` holds the bytes the PostgreSQL backend stores, in the claim row and the append ledger, to the same encoding.

- [ ] **Step 5: The changelog line**

Under Unreleased → `### Validation`, after the issue #137 bullet:

> - `rakka-agent-knowledge-graph`: `tests/live_claim_encoding.rs` pins a live claim's encoding to the bytes the build before tombstones (`d12b0f8`) wrote, and `rakka-agent-knowledge-graph-postgres` holds the bytes it stores to that encoding (issue #139). The promise in `docs/rakka-compatibility.md` that a live claim "encodes to the same bytes as before" had no test that compared against earlier bytes: the existing ones compared parsed JSON, which ignores key order, and would have passed a renamed key or a new field.

- [ ] **Step 6: Doc tests that parse these files**

```sh
cargo test -p rakka-agent-knowledge-graph --test compatibility_currency
cargo test -p rakka-testkit --test repository_hygiene
cargo test -p rakka-agent --features otel --test compatibility_currency
```

Expected: all pass.

- [ ] **Step 7: Full validation**

```sh
scripts/validate.sh > "$SP/validate.log" 2>&1; echo "exit=$?"
```

Expected: `exit=0`. If the workspace-test phase is killed under load, re-run the affected crates per crate and report it.

- [ ] **Step 8: Commit**

```bash
git add crates/rakka-agent-knowledge-graph/src/claim.rs crates/rakka-agent-knowledge-graph/src/store.rs docs/rakka-compatibility.md CHANGELOG.md
git commit -m "Name what holds a live claim's bytes, and fix the two #68 review nits (issue #139)"
```
