# Plain Task Handoff Narrowing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A handoff of a plain task gives the target exactly the knowledge spaces and read capabilities the catalog states for it, for reads and appends together (issue #142).

**Architecture:** One change in `delegation_envelope_for` (`crates/rakka-agent/src/task.rs`): when the task holds a handoff that is not refused and has no base envelope, the handoff step starts from `AgentRunDelegationEnvelope::default()`. The existing step then applies the shared rule `delegation::delegated_grant` unchanged, and every door that already reads the envelope enforces the result.

**Tech Stack:** Rust 1.88 MSRV. No new dependency.

**Spec:** `docs/superpowers/specs/2026-11-16-plain-task-handoff-narrowing-design.md`

## Global Constraints

- The synthesized envelope differs from `AgentRunDelegationEnvelope::default()` in `knowledge_spaces` and `read_capabilities` only.
- `delegation::delegated_grant` stays the only grant rule. No second copy.
- No new field on the assignment, the run or the handoff record. No new error code, schema version or metric.
- Reads and appends close together.
- A handoff never widens: the target's grant stays inside its own agent's definition and setup.
- A refused handoff synthesizes nothing.
- The tests of an enveloped task's handoff pass unchanged.
- Every public item documented; `cargo clippy --workspace --all-targets --all-features -- -D warnings` clean.
- `export PATH="$HOME/.rustup/toolchains/stable-aarch64-apple-darwin/bin:$PATH"` before cargo; `cargo clean` at each task boundary; `rakka-agent` test targets need `--all-features`.
- Commit messages name `(issue #142)` and end with `Co-Authored-By: Claude Fable 5.2 <noreply@anthropic.com>`.
- When falsifying, copy the file to the scratchpad, mutate, run each test file on its own, restore from the copy.

## Review Focus

Inputs the design implies and does not name.

1. A catalog target that states spaces and no read capabilities. Expected: the target reads, in those spaces, only claims that require no capability. (Task 1, test 1)
2. A catalog target that states a space the target's own agent is not granted. Expected: the target does not get it; a handoff never widens. (Task 1, test 1)
3. A handoff record written before issue #69, which holds no read capabilities. Expected: it decodes, and its target is passed none. Covered by the existing `pre_slice_records_decode_without_the_handoff_fields` together with test 2, since an absent set decodes empty. No new test.
4. A plain task handed off twice. Expected by the design's non-goal: the second target's grant is the catalog's statement for the second target. No test: the fixture has one handoff target. The reviewer checks it by reading.
5. A wake-epoch task, which also carries no envelope, handed off. Expected: the same as a plain task, since both take the last arm of `delegation_envelope_base`. No test. The reviewer checks it by reading.

---

### Task 1: The handoff step gives a plain task a root envelope

**Files:**
- Modify: `crates/rakka-agent/src/task.rs:5059-5106` (`delegation_envelope_for`)
- Modify: `crates/rakka-agent/tests/handoff_record.rs` (the helpers at `:917-1020`, the test at `:1118`, new tests after it)

**Interfaces:**
- Consumes: `AgentRunDelegationEnvelope: Default`, `delegation::delegated_grant`, `AgentDelegationTarget::with_knowledge_space`, `with_read_capability`, `AgentCommunalReadGrant::narrowed_by`, the fixture helpers `fixture_handing_off_reads`, `target_envelope_after_transfer`, `handoff_task_definition`.
- Produces: no new public item.

- [ ] **Step 1: Generalize the fixture helper**

`handoff_config_delegating_reads(ids)` and `fixture_handing_off_reads(ids)` gain a sibling that also states knowledge spaces on the handoff target: `fixture_handing_off(spaces: &[&str], reads: &[&str]) -> Fixture`. The existing two keep their signatures and call it with no spaces.

- [ ] **Step 2: Replace the pinning test and write the new ones**

The test `a_plain_task_with_no_envelope_is_not_narrowed_by_its_handoff` is deleted. In its place, all on a plain task (`handoff_task_definition()`, which declares no delegation ceilings, created with no goal and no delegation):

| # | Test | Asserts |
|---|---|---|
| 1 | `a_plain_tasks_handoff_grants_only_what_the_catalog_states` | The source's run carries no envelope before the transfer. The catalog states spaces `space-alpha`, `space-omega` and reads `claims.read.a`, `claims.read.b`. The target's envelope equals `AgentRunDelegationEnvelope { knowledge_spaces: Some({space-alpha, space-omega}), read_capabilities: Some({claims.read.a, claims.read.b}), ..Default::default() }`. A read grant of spaces `{space-alpha, space-beta}` and reads `{claims.read.a, claims.read.z}` narrows by it to spaces `{space-alpha}` and reads `{claims.read.a}`: inside the catalog's statement, and never outside the agent's own grant (Review Focus 2). A second fixture stating spaces and no reads narrows the same grant to spaces `{space-alpha}` and no read capability (Review Focus 1) |
| 2 | `a_plain_tasks_handoff_to_a_target_the_catalog_states_nothing_for_grants_nothing` | The catalog states nothing. The envelope's two sets are `Some(empty)`, and any read grant narrows to no space and no read capability |
| 3 | `the_target_of_a_plain_tasks_handoff_appends_only_inside_the_catalogs_spaces` | The catalog states `space-alpha`. An `AppendClaim` to the target's run for `space-alpha` is accepted; one for `space-beta` is refused with code `run-claim-space-not-delegated` and commits nothing |
| 4 | `the_target_of_a_plain_tasks_handoff_delegates_and_passes_on_no_more_than_it_holds` | The catalog passes the handoff target `{space-alpha, space-beta}` and states `{space-alpha, space-gamma}` for the delegation target. The target's turn is a delegation call. It commits (no skill, depth or ceiling refusal), and the delegation record's `knowledge_spaces` is `{space-alpha}` |
| 5 | `a_refused_handoff_of_a_plain_task_leaves_the_source_with_no_envelope` | The target agent is never instantiated, so the transfer is refused and the source resumes. The task's handoff is `Refused` and the source's run still carries no envelope |

Test 3 applies the command to `fixture.run_at(&common::handoff_target_run_scope())`, with an operation id from `claim_append_operation_id` for that scope, as `apply_append` in `communal_claim_append.rs` does for the source. It appends while the target's run is live or inside its post-terminal memory window.

- [ ] **Step 3: Run them to verify they fail**

Run: `cargo test -p rakka-agent --all-features --test handoff_record a_plain_task`, then the same with `the_target_of_a_plain_tasks_handoff` and `a_refused_handoff_of_a_plain_task`
Expected: tests 1 and 2 fail with "the target run carries an envelope" or an envelope of `None`; test 3 fails because the `space-beta` append is accepted; test 4 fails on a `knowledge_spaces` of `{space-alpha, space-gamma}`; test 5 **passes**, because it pins behaviour that must not change.

- [ ] **Step 4: Change the handoff step in `delegation_envelope_for`**

Inside the block that already holds a non-refused handoff, the envelope becomes `envelope.get_or_insert_with(Default::default)` and the two `delegated_grant` calls run on it as they do now. The comment that describes the gap and names the old pinning test is replaced by one that says: a task with no base envelope is a root with no statement, so the catalog's statement becomes the grant, and the default envelope narrows nothing else.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p rakka-agent --all-features --test handoff_record`
Expected: all pass, including `a_root_handoff_grants_the_targets_stated_read_capabilities` and `a_delegated_tasks_handoff_intersects_the_targets_statement_with_its_grant`, unchanged.

- [ ] **Step 6: Falsify**

| Mutation | Test that must fail |
|---|---|
| Restore `if let Some(env) = envelope.as_deref_mut()` (no synthesis) | tests 1, 2, 3, 4 |
| Synthesize outside the handoff block, for every envelope-less task | test 5, and test 1's "no envelope before the transfer" |
| Synthesize with `depth: 1` | test 1's equality with the default envelope |

- [ ] **Step 7: Run the whole crate and the examples that hand off**

Run: `cargo test -p rakka-agent --all-features > <log> 2>&1; echo $?` and `cargo test -p rakka-example-multi-agent-goal-acceptance > <log> 2>&1; echo $?`
Expected: exit 0 for both. A test elsewhere that relied on a plain task's handoff target reading or appending under its whole grant is a finding: report it by name and decide whether the test or its catalog should state the spaces.

- [ ] **Step 8: Commit**

```bash
git add crates/rakka-agent
git commit -m "Narrow a plain task's handoff by the catalog's statement (issue #142)"
```

---

### Task 2: Documents, changelog, validation

**Files:**
- Modify: `docs/rakka-agent-security-validation-matrix.md` (the "Owed" item at `:200-210`; the enforced row for communal reads at `:124`)
- Modify: `docs/rakka-compatibility.md` (the issue #69 entry at `:320`; a new entry before "Run the default compatibility matrix:")
- Modify: `docs/rakka-agents.md:508-514`
- Modify: `CHANGELOG.md` (Changed, Migration notes)

- [ ] **Step 1: Edit the documents**

- Matrix: the "Owed" item "A handoff of a task with no delegation envelope is not narrowed" is removed. The enforced table names `a_plain_tasks_handoff_grants_only_what_the_catalog_states` and `the_target_of_a_plain_tasks_handoff_appends_only_inside_the_catalogs_spaces`.
- `rakka-agents.md`: "A handoff follows the same rule **when the task carries a delegation envelope**" and the exception after it become one statement: a handoff follows the same rule for every task; stating nothing passes on nothing; a plain task's target used to keep its own agent's whole grant.
- Compatibility: any sentence in the issue #69 entry that states the exception is corrected. The new entry has a before/after table, who loses access, when it takes effect (at assignment by a task entity on the new build; a run already accepted is unchanged; a handoff recorded earlier and assigned later is narrowed by what its record holds; during a roll it depends on which build hosts the task entity), the codes a refused append answers (`run-claim-space-not-delegated`, `claim-space-not-delegated`), that a read outside the grant is not an error, that the narrowing is passed on to the target's own delegations, and the roll note.
- Changelog: one **Behaviour-breaking** entry under Changed and one Migration note: before upgrading, state the knowledge spaces and read capabilities on every catalog target a plain task can be handed to; the earlier build ignores them for a plain task.

- [ ] **Step 2: Run the tests that hold documents as data**

Run: `cargo test -p rakka-agent --all-features --test metric_catalogue --test collector_allowlist --test product_doc_currency && cargo test -p rakka-agent-workflow --test operational_runbooks --test api_compatibility && cargo test -p rakka-testkit --test repository_hygiene`
Expected: all pass.

- [ ] **Step 3: Check no document still names the deleted test**

Run: `grep -rn "a_plain_task_with_no_envelope_is_not_narrowed_by_its_handoff" docs crates CHANGELOG.md`
Expected: matches only in `docs/superpowers/` (the designs and plans of issues #69 and #142, which are history) and in earlier changelog entries that describe issue #69 as it shipped.

- [ ] **Step 4: Run the full validation, exit codes read from files**

Run: `scripts/validate.sh > <log> 2>&1; echo $? > <code>` then `scripts/package-check.sh > <log> 2>&1; echo $? > <code>`
Expected: both 0.

- [ ] **Step 5: Commit**

```bash
git add docs CHANGELOG.md
git commit -m "Document the narrowing of a plain task's handoff (issue #142)"
```
