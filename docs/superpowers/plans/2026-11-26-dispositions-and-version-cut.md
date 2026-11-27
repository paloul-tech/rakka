# Issue #72: Dispositions and the First Version Cut — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give specification 21.3's last three decisions their dispositions, and cut version `0.2.0` (manifests, changelog, release docs), each held by a repository-hygiene test.

**Architecture:** Documentation and manifests only, with no code behaviour change. Two new tests in `crates/rakka-testkit/tests/repository_hygiene.rs` read the spec, the changelog and every crate manifest, and fail when a 21.3 decision has no disposition or a crate's version differs from the changelog's newest release.

**Tech Stack:** Rust 1.88 workspace, Cargo manifests, Markdown docs.

**Spec:** `docs/superpowers/specs/2026-11-26-dispositions-and-version-cut-design.md`

## Global Constraints

- Nothing is published to any registry. `scripts/package-check.sh` runs offline and only validates (`CLAUDE.md`, "Publishing policy").
- No code behaviour changes. In particular `DEFAULT_COMPILED_PLAN_RUNTIME_VERSION` (`crates/rakka-agent-workflow/src/compiled_plan.rs:21`) stays `"0.1.0"`: it is the compiled-plan runtime contract, not the crate version.
- The examples (`examples/*/Cargo.toml`) keep their own `version = "0.1.0"`. They are `publish = false`, and nothing depends on them by version.
- No prose outside `CHANGELOG.md` and the manifests states the current version.
- Clippy runs with `-D warnings`. Never add `-D warnings` to the doc build.
- Run `scripts/validate.sh > <file> 2>&1; echo "exit=$?"`. Never pipe it into `tail`.
- `cargo clean` between tasks.
- Commits end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- The `v0.2.0` tag is created and pushed only on the owner's word, after the release PR merges.

## Review Focus

1. **A non-Rakka dependency bumped by mistake.** The bump touches only each crate's `[package]` version and the `path = "../rakka…"` requirements. None of the other `0.1.0` strings in the crate manifests is a third-party requirement today. Task 2 Step 3's grep proves nothing else moved.
2. **A test or fixture that pins `CARGO_PKG_VERSION`'s old value.** The OTel scope version, the MCP client's info, the A2A card's version and the compatibility fixture all read it. The only pins found are the two dependency-line assertions in `crates/rakka-agent-workflow/tests/api_compatibility.rs:569-571`, which Task 2 makes version-agnostic. The full suite in Task 2 Step 7 catches any other.
3. **The examples against `0.2.0` crates.** They depend by `path` with no version requirement, so they build. `validate.sh` builds them.
4. **`scripts/package-check.sh` under the new version.** Its offline list checks resolve internal dependencies by path. It must pass (Task 2 Step 7).
5. **A disposition that names a type or slice the code does not have.** Every name Task 1 cites was found by grep at plan time. Step 3 re-checks each one before writing it.

---

### Task 1: The three dispositions

**Files:**
- Modify: `docs/plans/rakka-agent/spec.md` (21.3, about `:2726-2905`)
- Modify: `docs/superpowers/specs/2026-11-26-dispositions-and-version-cut-design.md` (4.1, decision 6's phase)
- Test: `crates/rakka-testkit/tests/repository_hygiene.rs`

**Interfaces:**
- Consumes: `read`, `section_between` (existing helpers in the test file).
- Produces: nothing later tasks use.

- [ ] **Step 1: Write the failing test** at the end of `repository_hygiene.rs`:

```rust
/// Every decision in specification 21.3 carries a disposition, so none
/// reads as open after the code answered it (issue #72).
#[test]
fn every_decision_in_the_register_carries_a_disposition() {
    let spec = read("docs/plans/rakka-agent/spec.md");
    let section = section_between(
        &spec,
        "### 21.3 Open Decisions",
        "## 22. Initial Acceptance Statement",
    );
    let mut decisions: Vec<(u32, String)> = Vec::new();
    for line in section.lines() {
        let number = line
            .split_once(". **")
            .and_then(|(number, _)| number.parse::<u32>().ok());
        match number {
            Some(number) => decisions.push((number, line.to_string())),
            None => {
                if let Some((_, text)) = decisions.last_mut() {
                    text.push('\n');
                    text.push_str(line);
                }
            }
        }
    }
    let numbers: Vec<u32> = decisions.iter().map(|(number, _)| *number).collect();
    assert!(!numbers.is_empty(), "21.3 lists no decisions");
    assert_eq!(
        numbers,
        (1..=numbers.len() as u32).collect::<Vec<_>>(),
        "21.3 numbers its decisions consecutively from 1"
    );
    let open: Vec<u32> = decisions
        .iter()
        .filter(|(_, text)| !text.contains("*Disposition (M"))
        .map(|(number, _)| *number)
        .collect();
    assert!(open.is_empty(), "decisions without a disposition: {open:?}");
}
```

- [ ] **Step 2: Run it to verify it fails.**
  Run: `cargo test -p rakka-testkit --test repository_hygiene every_decision_in_the_register -- --nocapture`
  Expected: FAIL with `decisions without a disposition: [6, 18, 21]`.

- [ ] **Step 3: Write the three dispositions and the intro.** Grep each name below in `crates/` before using it. Use the existing notes' style: an italic `*Disposition (Mn): accepted — …*` paragraph, indented under its decision.
  - **Decision 6, *Disposition (M5)*.** The implementation plan assigned it to slice 5.1 (`docs/plans/rakka-agent/implementation-plan.md:3272-3274`), and it was never recorded. The facts:
    - application-owned catalogs implement the recommended shape:
      - `A2AAgentCatalog` resolves an A2A send's `io.rakka.agent.id` and task-definition selection to one `(AgentId, AgentTaskDefinition)` target, failing closed on none (slice 1.12);
      - `AgentDelegationCatalog` resolves a model-named skill to the concrete agent (slice 4.3);
      - handoff target resolution reuses the catalog (`resolve_handoff_target`, slice 5.1);
    - every created task has its own `AgentTaskId`, which is A2A `Task.id` (decision 17);
    - each assignment generation's run has its own `AgentRunId`, `run_id_for_assignment`: `{task}-gen-{generation}` (slice 1.4);
    - a handoff keeps the task, and its target runs under a new generation;
    - the A2A agent card is deployment-owned. `A2AAgentCardBuilder` projects one skill per hosted workflow, and a deployment advertises its agents' skills itself. Say that this last point is an implementation call.
  - **Decision 18, *Disposition (M5)*.** Accepted. Each pattern is typed durable state with bounded policy, admitted through the `AgentCoordinationCapabilityKind` envelope dimension:
    - delegation: `AgentDelegationCell` and `AgentDelegationPolicy` (slice 4.3);
    - handoff: `AgentHandoffRecord`/`AgentHandoffCell` and `AgentHandoffPolicy` (slice 5.1);
    - team: the team board entity `AgentTeamEntity` and `AgentTeamPolicy` (slice 5.2);
    - moderation: the conversation entity `AgentConversationEntity` and `AgentModerationPolicy` (slice 5.3).
  - **Decision 21, *Disposition (M5)*.** Accepted. `rakka_agent::events` is a read contract (slice 5.5):
    - one `AgentCoordinationCursor` per scope, `<entity address key>:<sequence>`, monotonic, over the task, team and conversation history logs and the run's decision-event sink;
    - bounded pages (`AGENT_COORDINATION_MAX_PAGE_SIZE`);
    - `AgentCoordinationReplay::WindowExpired` names the retained floor to resync from;
    - there is no second write path, so durable task and run state stay the correctness source;
    - slice 5.5b added the terminal notification exchanges;
    - scenario 45 (`crates/rakka-agent/tests/coordination_replay.rs`) proves it.
  - **The intro** (the paragraph under `### 21.3 Open Decisions`): dispositions are recorded per phase as *Disposition (Mn)*, naming the slice. A decision may carry one per phase that changed its answer. Items without one remain open.
  - **The design.** In 4.1, decision 6 becomes *Disposition (M5)*, with the reason (the implementation plan's assignment).

- [ ] **Step 4: Run the test to verify it passes.**
  Run: the Step 2 command.
  Expected: PASS.

- [ ] **Step 5: Commit.**

```bash
git add docs/plans/rakka-agent/spec.md docs/superpowers/specs/2026-11-26-dispositions-and-version-cut-design.md crates/rakka-testkit/tests/repository_hygiene.rs
git commit -m "Record the dispositions of specification 21.3's decisions 6, 18 and 21 (issue #72)"
```

  Then `cargo clean`.

### Task 2: The `0.2.0` cut

**Files:**
- Modify: `crates/*/Cargo.toml` (23 manifests), `Cargo.lock`
- Modify: `CHANGELOG.md`, `docs/rakka-v1-release-packaging.md` (`:68`, plus a new section)
- Modify: `crates/rakka-agent-workflow/tests/api_compatibility.rs:569-571`
- Test: `crates/rakka-testkit/tests/repository_hygiene.rs`

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces: `fn changelog_version() -> String` in the test file (the newest released version).

- [ ] **Step 1: Write the failing tests.**
  - Add `changelog_version()`. It asserts that the first `## ` heading of `CHANGELOG.md` is `## Unreleased`, and that the next is `## X.Y.Z (date)` with three all-digit parts. It returns `X.Y.Z` and otherwise panics with `the heading under `## Unreleased` is no released version: <heading>`.
  - Add `every_crate_carries_the_changelog_version`. For every `crates/*/Cargo.toml` (via `fs::read_dir`), the first `version = ` line after `[package]` equals `version = "<changelog_version()>"`. Every line containing `path = "../rakka` contains `version = "<changelog_version()>"`. The failure message names the manifest and the line. Assert at least one manifest was checked.
  - In `publishable_crates_have_release_metadata_and_versioned_internal_deps`, replace the hardcoded `"version = \"0.1.0\""` (`:183`) with the changelog version.

- [ ] **Step 2: Run them to verify they fail.**
  Run: `cargo test -p rakka-testkit --test repository_hygiene -- --nocapture`
  Expected: both tests FAIL with `the heading under `## Unreleased` is no released version: ## 0.1.0-v1-rc.0 Draft`.

- [ ] **Step 3: Bump the manifests.**

```bash
perl -pi -e 's/^version = "0\.1\.0"$/version = "0.2.0"/; s/(path = "\.\.\/rakka[^"]*", version = )"0\.1\.0"/$1"0.2.0"/' crates/*/Cargo.toml
grep -c '"0\.1\.0"' crates/*/Cargo.toml | grep -v ':0$'
cargo check --workspace --all-features --offline
```

  Expected: the grep prints nothing; the check succeeds and rewrites `Cargo.lock`'s 23 workspace entries to `0.2.0`. `git diff --stat Cargo.lock` shows only those entries.

- [ ] **Step 4: Cut the changelog.**
  - Replace `## Unreleased` (line 7) with `## Unreleased`, a blank line, and `## 0.2.0 (<today>)`. The history moves under 0.2.0 unchanged.
  - In 0.2.0's `### Changed`, add: every crate is `0.2.0`, the first version cut since `v0.1.0`, and later cuts follow `docs/rakka-v1-release-packaging.md` (issue #72).
  - In 0.2.0's `### Validation`, add: the two repository-hygiene tests, and specification 21.3's last three dispositions (issue #72).

- [ ] **Step 5: The release docs.**
  - **`docs/rakka-v1-release-packaging.md:68`:** internal dependencies use both `path` and `version`. The version is the newest release in `CHANGELOG.md`, which every crate and internal requirement carry, and `repository_hygiene.rs` checks it. Keep the rest of the paragraph, and keep the phrases its existing test reads (`package-list checked offline`, `must always run \`cargo package\` in offline mode`).
  - **A new `## Cutting a Version` section after `## Publishable Crates`:**
    1. on a branch, bump every crate's version and internal requirement to the next minor (`0.Y.0` before 1.0); the examples keep their own;
    2. move `## Unreleased`'s contents under `## 0.Y.0 (<date>)`, with an empty `## Unreleased` above;
    3. `scripts/validate.sh` and `scripts/package-check.sh` pass;
    4. after the PR merges, `git tag -a v0.Y.0 <merge commit> -m "Rakka 0.Y.0"`, pushed only on the owner's word.

    A tag publishes nothing.

- [ ] **Step 6: Make `api_compatibility.rs` version-agnostic.** The two assertions build their expected lines with `env!("CARGO_PKG_VERSION")`, for example `format!("rakka-core = {{ path = \"../rakka-core\", version = \"{}\" }}", env!("CARGO_PKG_VERSION"))`.

- [ ] **Step 7: Run everything.**
  - `cargo test -p rakka-testkit --test repository_hygiene`. Expected: PASS.
  - `cargo test -p rakka-agent-workflow --test api_compatibility`. Expected: PASS.
  - `scripts/package-check.sh > <file> 2>&1; echo "exit=$?"`. Expected: `exit=0`.
  - `scripts/validate.sh > <file> 2>&1; echo "exit=$?"`. Expected: `exit=0`. A workspace test killed under load is re-run per crate before anyone blames the branch.

- [ ] **Step 8: Commit.**

```bash
git add Cargo.lock CHANGELOG.md crates docs/rakka-v1-release-packaging.md
git commit -m "Cut version 0.2.0: every crate, the changelog, and how the next cut is made (issue #72)"
```

  Then `cargo clean`.

## After the merge (not a task)

On the owner's word:
- create `git tag -a v0.2.0 <merge commit> -m "Rakka 0.2.0"` and push it;
- post the #72 report;
- close #72.
