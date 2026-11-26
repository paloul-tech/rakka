# A live claim's encoding is pinned to the bytes written before tombstones (issue #139)

Short design. Branch `live-claim-encoding-pinned`, off `main` at
`5379856b`.

## 1. Why

`docs/rakka-compatibility.md` (the issue #68 entry) promises that a live
claim "is still written at version 1 and encodes to the same bytes as
before" the `tombstone` field existed. A binary from before #68 reads every
live claim only because that holds: it is what makes a rollback to that
build clean while no stub exists.

Two unit tests in `crates/rakka-agent-knowledge-graph/src/claim.rs` stand
near the promise: `a_stub_is_written_at_version_two_and_a_live_claim_at_version_one`
pins the two version numbers, and `a_live_claims_encoding_carries_no_tombstone_key`
pins that the key is absent. Neither compares against bytes the earlier
build wrote, and the second compares `serde_json::Value`s, which ignore key
order. A later field, a renamed key, a changed serde attribute, or a
reordered struct would change a live claim's bytes and both would pass.

## 2. What was verified before designing

- Nothing a live claim nests has changed its serde shape since `d12b0f8`
  (the commit before #68). The commits to `claim.rs` since #68 change doc
  comments only, and `domain.rs` (`ArtifactRef`, its kind, redaction and
  encryption types, `StateSchemaVersion`, `AgentTimestampMillis`) has no
  diff. `definition.rs`, `memory.rs` and `task.rs` changed, but not in a
  type a claim carries. So the test is expected to pin the promise, not to
  find it broken. If the reference bytes and the current bytes differ, the
  build stops and the owner decides: that would be a compatibility defect,
  not a test to adjust.
- The PostgreSQL backend stores `serde_json::to_vec(&claim.to_record())` (its
  private `encode`) in the claim row's `record BYTEA` column, and the same
  bytes in the append ledger row's `result` (`APPEND_SQL` inserts parameter
  `$10`, the record, into both). That is raw bytes of `ClaimRecord`, not the
  `serde_json::Value` of `Claim` the unit tests compare, so the issue's
  condition "if its encoding differs" is met.
- CI's `postgres` job runs the gated PostgreSQL tests against a live
  database, so a proof there does not sit skipped.

## 3. Goals, non-goals, anti-goals

- **Goal:** a live claim's encoding, as `Claim` and as `ClaimRecord`, is
  held byte for byte to the encoding the build at `d12b0f8` produced, and
  those bytes decode to the same claim.
- **Goal:** the bytes the PostgreSQL backend stores for a live claim are
  held to that same encoding.
- **Goal:** the issue's two nits are fixed.
- **Non-goal:** pinning a tombstoned stub's bytes. No earlier build wrote
  one; its compatibility story is `schema-version-ahead`, already tested.
- **Non-goal:** pinning the transition record or the ledger's outcome
  records. The promise is about the claim.
- **Anti-goal:** a reference produced by the current build. The literal
  must come from running the earlier build, or the test proves nothing
  about compatibility.
- **Anti-goal:** a failure message that invites updating the literal. A
  failure means the promise broke; the message says so and names the doc.

## 4. Approaches

- **A. Golden literals in a new integration test, plus a PostgreSQL link
  proof that compares the stored bytes to `serde_json::to_vec` of the
  record.** One copy of each literal. The domain test holds
  literal = encoding; the PostgreSQL test holds stored bytes = encoding.
  **Chosen by the owner.**
- **B. The same literals duplicated into the PostgreSQL test.** Holds the
  backend to the literal directly, but two copies of the same golden bytes
  can drift apart in a later edit, and the crate DAG gives the two test
  crates no shared module.
- **C. Literals added to the existing unit-test module in `claim.rs`.**
  Reuses `furnished_claim`, but that helper leaves several `ArtifactRef`
  options unset, and a unit test can reach private fields an outside reader
  cannot. The promise is about what an outside reader decodes, so the test
  belongs on the public API.

## 5. The design

**The reference.** In a throwaway worktree at `d12b0f8`, a scratch test in
`rakka-agent-knowledge-graph` builds the claims below and prints
`serde_json::to_string` of each claim and of its record. The two strings
must be identical at `d12b0f8` (the two structs declare their fields in the
same order); the output is kept as the literals. The worktree and its
target directory are removed afterwards. The scratch test is not
committed.

**The claims.** Built through the public API only, with fixed inputs
(fixed ids, timestamps, digests are derived):

1. An edge claim (`ClaimObject::Node`) with every optional field set:
   provenance with all six fields (agent, goal, task, run, delegation,
   effect); two evidence refs with every `ArtifactRef` option set
   (checksum, content type, length, retention class, encryption, a
   non-empty metadata map); an embedding note; two read capabilities;
   a policy with its revision; and one trust transition, so `trust` and
   `transition_count` hold values other than their defaults.
2. A value claim whose object is inline JSON content.
3. A value claim whose object is artifact content.

**The test.** A new `crates/rakka-agent-knowledge-graph/tests/live_claim_encoding.rs`.
For each claim:

- `serde_json::to_vec(&claim)` equals the literal;
- `serde_json::to_vec(&claim.to_record())` equals the literal;
- `serde_json::from_slice::<Claim>(literal)` equals the claim;
- `Claim::restore(serde_json::from_slice::<ClaimRecord>(literal))` equals
  the claim (the PostgreSQL decode path);
- the decoded claim's schema version is `CLAIM_LIVE_SCHEMA_VERSION`.

The module doc says where the literals came from (the commit, and that they
were produced by running that build) and what a failure means.

**The PostgreSQL proof.** One gated proof, `..._when_dsn_is_set`, in the
style of the crate's existing proofs: append the furnished edge claim, then
read the claim row's `record` and the append ledger row's `result` raw, and
assert each equals `serde_json::to_vec(&claim.to_record())`. The doc comment
names the domain test that holds those bytes to the literal.

**The nits.** The unit-test helper `tombstone` in `claim.rs` derives its
operation id with `ClaimOperationId::derive_tombstone`. The 112-character
doc line at `store.rs:14` is rewrapped to the width of the lines around it.

**Docs.** In `docs/rakka-compatibility.md`, the sentence carrying the
promise names the test that holds it. One `CHANGELOG.md` line under
Unreleased / Validation. No behaviour changes, so no other product doc
moves.

## 6. Left to the builder

Names, the exact fixture values, which existing PostgreSQL proof file takes
the new proof (or a new one), and the wording of the failure message.
