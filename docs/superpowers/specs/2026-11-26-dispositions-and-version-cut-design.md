# Issue #72: the three open dispositions and the first version cut

Short design, approved by the owner on 2026-11-26.

## 1. Why

Issue #72 lists documentation facts that a consumer quotes and that the code
can't hold. One of its three items, the `LICENSE` contradiction, was done in
PR #83. Two are owed.

- **Three decisions in specification 21.3 carry no disposition.** Decisions
  6, 18 and 21 still read as open, although the code answered each of them
  phases ago.
- **No version has ever been cut.**
  - The only tag is `v0.1.0`, from 2026-06-15. `main` is 1,070 commits past
    it.
  - The whole changelog sits under `## Unreleased`.
  - Every crate is `0.1.0`.
  - The main consumer, rakka.host, pins a commit hash and measures every
    bump with a patch worktree. A tag lets it pin a name, and gives the
    compatibility document's schema-window rows a version to bind to.

## 2. Goals, non-goals, anti-goals

- **Goals:**
  - every decision in 21.3 carries a disposition that the code bears out;
  - one cut version, `0.2.0`, in the manifests, the changelog and a tag;
  - a written way to cut the next one;
  - tests that fail when either fact drifts.
- **Non-goals:**
  - publishing anything to a registry;
  - back-filling tags or changelog sections for past slices;
  - versioning the examples;
  - changing any code behaviour.
- **Anti-goals:**
  - **A disposition the code does not bear out.** Each one names the slice
    and the type that answer the decision, checked against `main`.
  - **A version string that drifts after a later cut.** No prose outside the
    changelog and the manifests states the current version.

## 3. Approaches (the version cut)

- **A. Version and tag together, going forward.** Chosen.
  - One release PR bumps all 23 crates to `0.2.0` and moves `## Unreleased`
    under `## 0.2.0`. The tag goes on its merge.
  - Each later cut does the same with the next minor version.
  - The tag name always matches what Cargo reports.
- **B. Tags only, crates stay `0.1.0`.** Cheapest per cut, but the tag and
  the crate version disagree.
- **C. Back-fill tags and changelog sections from gap slice 2 on.** Closest
  to the issue's wording. Splitting about 700 lines of history means tracing
  every entry to its merge, which is slow and error-prone.

The owner picked A.

## 4. The design

### 4.1 Dispositions (`docs/plans/rakka-agent/spec.md`, 21.3)

- **Decision 6, *Disposition (M1)*:** accepted.
  - The deployment supplies the A2A agent card. Rakka builds no
    agent-domain card.
  - An application-owned `A2AAgentCatalog` resolves a request to one
    `(AgentId, AgentTaskDefinition)`, failing closed on no match or several
    (slice 1.12).
  - A2A `Task.id` is the `AgentTaskId`, as decision 17 records.
  - Each assignment generation gets its own derived `AgentRunId`,
    `run_id_for_assignment` (slice 1.4).
  - Leaving card production to the host is the builder's call, marked as
    such.
- **Decision 18, *Disposition (M5)*:** accepted. Each of the four patterns
  is typed durable state with bounded policy:
  - delegation (slice 4.3);
  - handoff (slice 5.1);
  - team (slice 5.2);
  - moderation (slice 5.3).
- **Decision 21, *Disposition (M5)*:** accepted.
  - The replayable read contract: a bounded window, a monotonic scoped
    cursor, and an explicit resync on an expired window (slices 5.5 and
    5.5b).
  - Authoritative task and run state remain the correctness source.
- **The section's intro** says only M1 dispositions exist. It is rewritten
  to cover dispositions from any phase.
- **A hygiene test** fails when a numbered 21.3 decision carries no
  *Disposition* note.

The builder checks each fact against `main` before writing it, and corrects
this section if the code says otherwise.

### 4.2 The `0.2.0` cut

- **Manifests:**
  - every `crates/*/Cargo.toml` package version becomes `0.2.0`;
  - every internal Rakka dependency requirement becomes `version = "0.2.0"`;
  - `Cargo.lock` is updated.

  The examples keep their own `0.1.0`: they are `publish = false` and nothing
  depends on them by version.
- **`CHANGELOG.md`:**
  - `## Unreleased` becomes `## 0.2.0 (<date of the cut>)`, its contents
    unchanged;
  - a new, empty `## Unreleased` goes above it;
  - the `0.1.0-v1-rc.0 Draft` checklist stays where it is.
- **`docs/rakka-v1-release-packaging.md`:**
  - It stops quoting `0.1.0`: "internal dependencies carry the workspace's
    current version".
  - It gains a "Cutting a version" section: bump every crate and requirement
    to the next minor version, move `Unreleased` under it, merge, tag
    `vX.Y.0` on the merge commit, and push the tag only on the owner's word.
    Nothing is published.
- **A hygiene test** holds every crate's package version, and every internal
  requirement, equal to the newest version section in `CHANGELOG.md`.
- **Validation:** `scripts/validate.sh` and `scripts/package-check.sh` must
  pass.

### 4.3 The tag

After the release PR merges, the builder creates an annotated `v0.2.0` on
the merge commit. It is pushed only when the owner says so.

## 5. Left to the builder

- The hygiene tests' file, and how they parse the manifests and the
  changelog.
- The exact wording of each disposition, within 4.1's facts.
- How the bump is applied, by script or by hand, provided every manifest is
  covered and the hygiene test proves it.
