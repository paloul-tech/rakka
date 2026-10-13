# Unknown-Outcome Evidence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A parked MCP effect records why its outcome is unknown, when its call was handed over, and whether a cancellation was issued, and the operational snapshot reports all three (issue #100, ask 5).

**Architecture:** The MCP executor tracks how far each `tools/call` got (`CallProgress`). It names an unknown outcome by that progress and attaches an `AgentUnknownOutcomeReport` to `AgentDispatchError::OutcomeUnknown`. The dispatcher turns the report's monotonic offset into a `sent_at` on its own clock and parks the effect with an `Indeterminate` outcome carrying a reason and an `AgentUnknownOutcomeEvidence`. The run copies the reason into `last_error_reason` and the evidence into the new `AgentRunEffect::unknown_outcome`, which `AgentPendingEffectView` reports. Everything is observability only: no dispatch decision reads it.

**Tech Stack:** Rust 1.88 (MSRV), serde, tokio, rmcp 3.4.0. Crates: `rakka-agent` and `rakka-agent-mcp`.

**Spec:** `docs/superpowers/specs/2026-10-13-unknown-outcome-evidence-design.md`, including its "Amendments from planning" section.

## Global Constraints

- **MSRV is 1.88.** Use no newer language or library features.
- **Lints:** `cargo clippy --workspace --all-targets --all-features -- -D warnings` must pass. Every public item needs a doc comment (`missing_docs`), and `unsafe_code` is forbidden.
- **No schema version moves.** Every new durable field is `#[serde(default)]` and left out while unset, so a record or outcome without it encodes byte-identically to before.
- **Observability only.** No dispatch decision, no settlement, and no safety-class arm reads any new field. Which MCP failures are unknown outcomes does not change, and the call that was never handed over still parks.
- **Reason codes are never metric labels.**
- **The MCP executor reads no wall clock.** It measures with `tokio::time::Instant`, and the dispatcher's `self.now()` is the only wall clock.
- **The four reason codes, verbatim:** `mcp-call-not-handed-over`, `mcp-deadline-awaiting-answer`, `mcp-transport-awaiting-answer`, `mcp-answer-not-recorded`. The pipeline code stays `mcp-outcome-unknown`.
- **Disk:** run `cargo clean` between tasks (an owner rule on this machine), never while a cargo command another agent started may still be running.
- **Running tests:** run them per crate and per test file. Redirect `scripts/validate.sh` to a file and capture `$?`; never pipe it into `tail`.
- **Commits** end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. **An executor that reports only a reason** (no offset, no cancellation). The reason is recorded, and no evidence object is written. Pinned in Task 2, test `a_report_with_only_a_reason_records_no_evidence`.
2. **An absurd offset** (`Duration::MAX`). `sent_at` saturates at `u64::MAX` and never panics. Pinned in Task 2, test `an_absurd_offset_saturates_rather_than_panicking`.
3. **A blank reason code from a third-party executor.** No reason is recorded, the pipeline code still is, and evidence still lands. Pinned in Task 2, test `a_blank_reason_code_records_no_reason_but_keeps_the_evidence`.
4. **An `Indeterminate` encoded before this change** (no `reason`/`evidence` keys) decodes with neither. Pinned in Task 1, test `the_indeterminate_outcomes_evidence_is_additive`.
5. **A server that answers `tools/call` with a result of the wrong kind.** This is an `UnexpectedResponse` after the answer was read, and it is named `mcp-answer-not-recorded`, never a transport or deadline code. Pinned in Task 3, by the classifier unit test's `(WaitEnded { answered: true }, Transport)` row.

---

### Task 1: The durable evidence on the outcome, the record and the view

**Files:**
- Modify: `crates/rakka-agent/src/effect.rs`:
  - the `AgentRunEffectOutcome::Indeterminate` variant, ~line 2370;
  - `failed`/`exhausted`/`with_reason`/`failure_reason`, ~2380-2425;
  - the `AgentRunEffect` struct, ~1775-1875, and `AgentRunEffect::new`, ~1919-1925;
  - `begin_next_generation`, ~2001-2025.
- Modify: `crates/rakka-agent/src/run.rs`: the `Indeterminate` arm of `apply_effect_outcome` (~4746-4758), and the growth-reserve test's two effect loops (`model_effect.last_error_reason = …` ~10450, and `effect.last_error_reason = Some(maximal_reason.clone());` ~10496).
- Modify: `crates/rakka-agent/src/dispatch.rs`: the `Indeterminate` literal in `park_indeterminate` (~3596).
- Modify: `crates/rakka-agent/src/query.rs`: `AgentPendingEffectView` and its projection in `AgentOperationalSnapshot::derive`.
- Modify: `crates/rakka-agent/src/lib.rs`: export `AgentUnknownOutcomeEvidence` from `pub use effect::{…}`.
- Modify (literal migration): `crates/rakka-agent/tests/checkpoint_reconciliation.rs` (3 sites), `checkpoint_resolve_segment.rs` (1), `operational_query.rs` (2), `private_memory_promotion.rs` (1).
- Test: `crates/rakka-agent/tests/failure_reason_records.rs` and `crates/rakka-agent/tests/operational_query.rs`.

**Interfaces:**
- Produces, in `rakka_agent` (lib root):
  - `AgentUnknownOutcomeEvidence { pub sent_at: Option<AgentTimestampMillis>, pub cancellation_issued: bool }`. It is `#[non_exhaustive]`, derives `Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize`, and is built with `AgentUnknownOutcomeEvidence::new(sent_at: Option<AgentTimestampMillis>, cancellation_issued: bool) -> Self` (a `const fn`).
  - `AgentRunEffectOutcome::indeterminate(code: impl Into<String>, message: impl Into<String>) -> Self`.
  - `AgentRunEffectOutcome::with_reason(self, Option<AgentFailureReason>) -> Self`, now also covering `Indeterminate`.
  - `AgentRunEffectOutcome::with_evidence(self, Option<AgentUnknownOutcomeEvidence>) -> Self`, which sets it on `Indeterminate` only.
  - `AgentRunEffectOutcome::unknown_outcome_evidence(&self) -> Option<&AgentUnknownOutcomeEvidence>`.
  - `AgentRunEffectOutcome::failure_reason(&self)`, now also answering for `Indeterminate`.
  - `AgentRunEffectOutcome::Indeterminate { code, message, reason: Option<AgentFailureReason>, evidence: Option<AgentUnknownOutcomeEvidence> }`, marked `#[non_exhaustive]`.
  - `AgentRunEffect::unknown_outcome: Option<AgentUnknownOutcomeEvidence>`.
  - `AgentPendingEffectView::unknown_outcome: Option<AgentUnknownOutcomeEvidence>`.

- [ ] **Step 1: Migrate every external `Indeterminate` literal to the constructor that Step 4 adds**

The variant becomes `#[non_exhaustive]`, so these literals won't compile from an integration test. Run this from the repo root:

```bash
python3 - <<'EOF'
import re
files = [
    "crates/rakka-agent/tests/checkpoint_reconciliation.rs",
    "crates/rakka-agent/tests/checkpoint_resolve_segment.rs",
    "crates/rakka-agent/tests/operational_query.rs",
    "crates/rakka-agent/tests/private_memory_promotion.rs",
]
pattern = re.compile(
    r'AgentRunEffectOutcome::Indeterminate \{\s*code: ("(?:[^"\\]|\\.)*")\.to_string\(\),\s*message: ("(?:[^"\\]|\\.)*")\.to_string\(\),\s*\}',
    re.S,
)
total = 0
for path in files:
    text = open(path).read()
    text, count = pattern.subn(r'AgentRunEffectOutcome::indeterminate(\1, \2)', text)
    total += count
    open(path, "w").write(text)
    print(path, count)
assert total == 7, total
EOF
```

Expected output: counts 3, 1, 2 and 1, with a total of 7. If the assertion fails, a literal spans a concatenated string: migrate the remaining ones by hand to `AgentRunEffectOutcome::indeterminate("<code>", "<message>")`, keeping the exact strings.

- [ ] **Step 2: Write the failing record-shape and projection tests**

Append to `crates/rakka-agent/tests/failure_reason_records.rs`, and add `AgentRunEffectOutcome` and `AgentUnknownOutcomeEvidence` to its `use rakka_agent::{…}` list:

```rust
/// Issue #100 ask 5: the indeterminate outcome's reason and evidence are
/// additive — an outcome encoded before them decodes with neither, an
/// outcome without them encodes without the keys, and one with them
/// round-trips.
#[test]
fn the_indeterminate_outcomes_evidence_is_additive() {
    let before = json!({
        "indeterminate": {"code": "mcp-outcome-unknown", "message": "lost"}
    });
    let decoded: AgentRunEffectOutcome =
        serde_json::from_value(before.clone()).expect("an older encoding decodes");
    assert_eq!(decoded.failure_reason(), None);
    assert_eq!(decoded.unknown_outcome_evidence(), None);
    assert_eq!(
        serde_json::to_value(&decoded).expect("encodes"),
        before,
        "an outcome without the fields encodes exactly as before"
    );

    let with = AgentRunEffectOutcome::indeterminate("mcp-outcome-unknown", "lost")
        .with_reason(AgentFailureReason::new("mcp-deadline-awaiting-answer"))
        .with_evidence(Some(AgentUnknownOutcomeEvidence::new(
            Some(AgentTimestampMillis::new(4_242)),
            true,
        )));
    let encoded = serde_json::to_value(&with).expect("encodes");
    assert_eq!(
        body(&encoded, "indeterminate").get("reason"),
        Some(&json!({"code": "mcp-deadline-awaiting-answer"}))
    );
    assert_eq!(
        body(&encoded, "indeterminate").get("evidence"),
        Some(&json!({"sent_at": 4_242, "cancellation_issued": true}))
    );
    assert_eq!(
        serde_json::from_value::<AgentRunEffectOutcome>(encoded).expect("decodes"),
        with
    );
}

/// Issue #100 ask 5: evidence with nothing set writes neither key, and an
/// unset cancellation is never written as `false`.
#[test]
fn unset_evidence_fields_are_left_out() {
    let empty = serde_json::to_value(AgentUnknownOutcomeEvidence::default()).expect("encodes");
    assert_eq!(empty, json!({}));
    let sent_only = serde_json::to_value(AgentUnknownOutcomeEvidence::new(
        Some(AgentTimestampMillis::new(7)),
        false,
    ))
    .expect("encodes");
    assert_eq!(sent_only, json!({"sent_at": 7}));
}

/// Issue #100 ask 5: the effect record's evidence is additive, and a new
/// generation does not inherit the superseded generation's evidence.
#[test]
fn the_effect_records_evidence_is_additive_and_cleared_by_a_new_generation() {
    let plain = effect();
    let encoded = serde_json::to_value(&plain).expect("encodes");
    assert!(
        !encoded
            .as_object()
            .expect("an object")
            .contains_key("unknown_outcome"),
        "{encoded}"
    );

    let mut parked = effect();
    parked.unknown_outcome = Some(AgentUnknownOutcomeEvidence::new(
        Some(AgentTimestampMillis::new(9)),
        true,
    ));
    let encoded = serde_json::to_value(&parked).expect("encodes");
    assert_eq!(
        encoded.get("unknown_outcome"),
        Some(&json!({"sent_at": 9, "cancellation_issued": true}))
    );
    assert_eq!(
        serde_json::from_value::<AgentRunEffect>(encoded).expect("decodes"),
        parked
    );

    parked
        .begin_next_generation(&run_scope(), AgentTimestampMillis::new(10))
        .expect("the next generation begins");
    assert_eq!(parked.unknown_outcome, None);
}
```

In `crates/rakka-agent/tests/operational_query.rs`, make `park_on_reconciliation` take the outcome it records, so a test can park with evidence. Change its signature and its `outcome:` line:

```rust
async fn park_on_reconciliation(fx: &Fixture, outcome: AgentRunEffectOutcome) {
```

```rust
            outcome: Box::new(outcome),
```

Each of its three existing callers (the batch A tests `a_parked_effect_names_its_target_its_last_error_and_its_hand_off`, `a_pending_effect_reports_the_failure_reason_its_record_carries` and `a_reconciliation_checkpoint_names_its_effect_decisions_and_summary`) becomes:

```rust
    park_on_reconciliation(
        &fx,
        AgentRunEffectOutcome::indeterminate("connection-lost", "the worker died mid-invocation"),
    )
    .await;
```

Add `AgentUnknownOutcomeEvidence` to that file's `use rakka_agent::{…}` list, and append:

```rust
/// Issue #100 ask 5: a park that carries a reason and evidence reports both
/// on the pending effect, and the evidence serializes additively.
#[tokio::test]
async fn a_parked_effect_reports_what_is_known_about_its_unknown_outcome() {
    let fx = reconcilable_fixture();
    park_on_reconciliation(
        &fx,
        AgentRunEffectOutcome::indeterminate("mcp-outcome-unknown", "the answer was lost")
            .with_reason(AgentFailureReason::new("mcp-deadline-awaiting-answer"))
            .with_evidence(Some(AgentUnknownOutcomeEvidence::new(
                Some(AgentTimestampMillis::new(4_242)),
                true,
            ))),
    )
    .await;

    let snapshot = snapshot_of(&fx).await;
    let parked = snapshot
        .pending_effects
        .iter()
        .find(|effect| effect.status == AgentRunEffectStatus::Indeterminate)
        .expect("the ambiguous effect is reported");
    assert_eq!(parked.last_error_code.as_deref(), Some("mcp-outcome-unknown"));
    assert_eq!(
        parked
            .last_error_reason
            .as_ref()
            .map(AgentFailureReason::code),
        Some("mcp-deadline-awaiting-answer")
    );
    assert_eq!(
        parked.unknown_outcome,
        Some(AgentUnknownOutcomeEvidence::new(
            Some(AgentTimestampMillis::new(4_242)),
            true
        ))
    );
    let body = serde_json::to_value(parked).expect("the view serializes");
    assert_eq!(
        body["unknown_outcome"],
        serde_json::json!({"sent_at": 4_242, "cancellation_issued": true})
    );
}
```

Also extend `the_new_view_fields_serialize_additively` in the same file: add `"unknown_outcome"` to the array in its `for absent in [...]` loop.

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cargo test -p rakka-agent --all-features --test failure_reason_records --test operational_query`

Expected: a compile FAIL, naming only the missing items: no `AgentUnknownOutcomeEvidence`, no function `indeterminate`, no method `with_evidence`/`unknown_outcome_evidence`, and no field `unknown_outcome`. Any other error is a typo in Steps 1–2: fix it and rerun.

- [ ] **Step 4: Implement the outcome fields, the constructor and the accessors in `effect.rs`**

Add the durable type and its `skip_serializing_if` predicate just above `pub enum AgentRunEffectOutcome`:

```rust
/// What is known about an attempt whose outcome is unknown, as the effect
/// record keeps it: when its request was handed over, on the dispatcher's
/// clock, and whether a cancellation was issued for it.
///
/// Observability only: no dispatch decision reads it. "Issued" means the
/// client accepted the cancellation within its close bound — never that the
/// target received or honoured it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AgentUnknownOutcomeEvidence {
    /// When the request was handed over, on the dispatcher's clock: the
    /// attempt's start plus the offset the executor measured. `None` when it
    /// never was.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sent_at: Option<AgentTimestampMillis>,
    /// Whether a cancellation was issued for the request.
    #[serde(default, skip_serializing_if = "is_false")]
    pub cancellation_issued: bool,
}

impl AgentUnknownOutcomeEvidence {
    /// Evidence with the given send time and cancellation.
    #[must_use]
    pub const fn new(sent_at: Option<AgentTimestampMillis>, cancellation_issued: bool) -> Self {
        Self {
            sent_at,
            cancellation_issued,
        }
    }
}

/// Keeps an unset flag off the record, so evidence without one encodes
/// exactly as it would have before the flag existed.
#[allow(clippy::trivially_copy_pass_by_ref)]
const fn is_false(value: &bool) -> bool {
    !*value
}
```

Replace the `Indeterminate` variant with:

```rust
    /// An attempt may have invoked the target and its outcome cannot be
    /// established mechanically. The run must park for reconciliation
    /// ([specification 11.5](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// Built with [`Self::indeterminate`]; the variant is `#[non_exhaustive]`
    /// so a later observability field is not a source break.
    #[non_exhaustive]
    Indeterminate {
        /// Stable machine-readable code describing the ambiguity.
        code: String,
        /// Human-readable detail.
        message: String,
        /// Which situation made the outcome unknown, when the executor said:
        /// its own stable code. Observability only.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<crate::failure::AgentFailureReason>,
        /// What is known about the attempt, when the executor reported it.
        /// Observability only.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        evidence: Option<AgentUnknownOutcomeEvidence>,
    },
```

After `pub fn exhausted(...)`, add:

```rust
    /// An unknown outcome with no reason and no evidence beyond its code.
    #[must_use]
    pub fn indeterminate(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Indeterminate {
            code: code.into(),
            message: message.into(),
            reason: None,
            evidence: None,
        }
    }
```

Replace `with_reason` and `failure_reason` with:

```rust
    /// Sets which decision failed a `Failed` or `Exhausted` outcome, or which
    /// situation made an `Indeterminate` one unknown. Every other outcome is
    /// returned unchanged: nothing failed it.
    #[must_use]
    pub fn with_reason(mut self, deciding: Option<crate::failure::AgentFailureReason>) -> Self {
        if let Self::Failed { reason, .. }
        | Self::Exhausted { reason, .. }
        | Self::Indeterminate { reason, .. } = &mut self
        {
            *reason = deciding;
        }
        self
    }

    /// Sets what is known about an `Indeterminate` outcome's attempt. Every
    /// other outcome is returned unchanged.
    #[must_use]
    pub fn with_evidence(mut self, known: Option<AgentUnknownOutcomeEvidence>) -> Self {
        if let Self::Indeterminate { evidence, .. } = &mut self {
            *evidence = known;
        }
        self
    }

    /// Which decision failed the generation, or made its outcome unknown,
    /// when the outcome carries one.
    #[must_use]
    pub fn failure_reason(&self) -> Option<&crate::failure::AgentFailureReason> {
        match self {
            Self::Failed { reason, .. }
            | Self::Exhausted { reason, .. }
            | Self::Indeterminate { reason, .. } => reason.as_ref(),
            _ => None,
        }
    }

    /// What is known about an `Indeterminate` outcome's attempt, when the
    /// executor reported it.
    #[must_use]
    pub fn unknown_outcome_evidence(&self) -> Option<&AgentUnknownOutcomeEvidence> {
        match self {
            Self::Indeterminate { evidence, .. } => evidence.as_ref(),
            _ => None,
        }
    }
```

Add the field to `pub struct AgentRunEffect`, directly after `last_error_reason`:

```rust
    /// What is known about the current generation's unknown outcome, when it
    /// parked with evidence (issue #100): when its request was handed over and
    /// whether a cancellation was issued. Observability only, never
    /// correctness: an effect persisted before this field decodes with none,
    /// and no dispatch decision reads it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unknown_outcome: Option<AgentUnknownOutcomeEvidence>,
```

In `AgentRunEffect::new`, add `unknown_outcome: None,` directly after `last_error_reason: None,`. In `begin_next_generation`, add `self.unknown_outcome = None;` directly after `self.last_error_reason = None;`.

- [ ] **Step 5: Copy the evidence in the run, build through the constructor in the dispatcher, report it in the view**

In `crates/rakka-agent/src/run.rs`, the `AgentRunEffectOutcome::Indeterminate { code, .. }` arm of `apply_effect_outcome`, add after `effect.last_error_reason = outcome.failure_reason().cloned();`:

```rust
            effect.unknown_outcome = outcome.unknown_outcome_evidence().copied();
```

In the same file's growth-reserve test, add after each `….last_error_reason = Some(maximal_reason.clone());` line (there are two: one sets `model_effect.`, one sets `effect.`), using the same variable name as that line:

```rust
        model_effect.unknown_outcome = Some(crate::effect::AgentUnknownOutcomeEvidence::new(
            Some(AgentTimestampMillis::new(u64::MAX)),
            true,
        ));
```

```rust
            effect.unknown_outcome = Some(crate::effect::AgentUnknownOutcomeEvidence::new(
                Some(AgentTimestampMillis::new(u64::MAX)),
                true,
            ));
```

(If `AgentTimestampMillis` is not in scope in that test module, write `rakka_agent_workflow::AgentTimestampMillis`.)

In `crates/rakka-agent/src/dispatch.rs`, `park_indeterminate`, replace the literal:

```rust
            AgentRunEffectOutcome::Indeterminate {
                code: bounded_failure_code(code),
                message: bounded_failure_detail(message),
            },
```

with:

```rust
            AgentRunEffectOutcome::indeterminate(
                bounded_failure_code(code),
                bounded_failure_detail(message),
            ),
```

In `crates/rakka-agent/src/query.rs`:
- Add `AgentUnknownOutcomeEvidence` to the `use crate::effect::{…}` import.
- Append this field to `AgentPendingEffectView`, after `last_error_reason`:

```rust
    /// What is known about its unknown outcome, when it parked with
    /// evidence: when its request was handed over and whether a cancellation
    /// was issued.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unknown_outcome: Option<AgentUnknownOutcomeEvidence>,
```

- Add this to the `AgentPendingEffectView { … }` projection in `AgentOperationalSnapshot::derive`, after `last_error_reason: …,`:

```rust
                        unknown_outcome: effect.unknown_outcome,
```

In `crates/rakka-agent/src/lib.rs`, add `AgentUnknownOutcomeEvidence` to the `pub use effect::{…}` list, in alphabetical position.

Run `cargo fmt -p rakka-agent`.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cargo test -p rakka-agent --all-features --lib --test failure_reason_records --test operational_query --test checkpoint_reconciliation --test checkpoint_resolve_segment --test private_memory_promotion`

Expected: PASS, every test in every listed binary, including the library's growth-reserve test.

- [ ] **Step 7: Falsify**

Temporarily replace `effect.unknown_outcome = outcome.unknown_outcome_evidence().copied();` with `effect.unknown_outcome = None;` and rerun `--test operational_query`. Expected: `a_parked_effect_reports_what_is_known_about_its_unknown_outcome` FAILS. Restore the line, then rerun to confirm it passes.

- [ ] **Step 8: Commit**

```bash
git add crates/rakka-agent/src crates/rakka-agent/tests
git commit -m "Carry an unknown outcome's reason and evidence onto the effect record and the pending-effect view

Issue #100 ask 5, durable half. Indeterminate gains a reason and an
AgentUnknownOutcomeEvidence (sent_at, cancellation_issued), both
serde-defaulted, and becomes #[non_exhaustive] with an indeterminate()
constructor; the run copies them into last_error_reason and the new
AgentRunEffect::unknown_outcome, which a new generation clears, and
AgentPendingEffectView reports it.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

Then run `cargo clean`.

---

### Task 2: The report on `OutcomeUnknown`, stamped by the dispatcher

**Files:**
- Modify: `crates/rakka-agent/src/dispatch.rs`:
  - the `AgentDispatchError::OutcomeUnknown` variant (~4657), `outcome_unknown` (~4703), and the `Display` arm (~4737);
  - the attempt start (~3124, `let attempt_timer = AgentSegmentTimer::start(self.now());`);
  - the `OutcomeUnknown` arm of the invocation match (~3178);
  - `settle_unknown_outcome` (~3315) and `park_indeterminate` (~3577).
- Modify: `crates/rakka-agent/src/lib.rs`: export `AgentUnknownOutcomeReport` from `pub use dispatch::{…}`.
- Test: `crates/rakka-agent/tests/dispatch_failure_outcomes.rs`.

**Interfaces:**
- Consumes (Task 1): `AgentUnknownOutcomeEvidence::new`, `AgentRunEffectOutcome::indeterminate(..).with_reason(..).with_evidence(..)`, and `AgentRunEffect::unknown_outcome`.
- Produces, in `rakka_agent`:
  - `AgentUnknownOutcomeReport { pub reason: Option<AgentFailureReason>, pub handed_over_after: Option<Duration>, pub cancellation_issued: bool }`. It is `#[non_exhaustive]` and derives `Debug, Clone, PartialEq, Eq, Default`.
  - Its builders: `AgentUnknownOutcomeReport::new() -> Self`, `.with_reason(code: impl AsRef<str>) -> Self` (blank gives no reason), `.with_handed_over_after(Duration) -> Self`, and `.with_cancellation_issued(bool) -> Self`.
  - `AgentDispatchError::with_report(self, AgentUnknownOutcomeReport) -> Self` (sets it on `OutcomeUnknown` only) and `AgentDispatchError::unknown_outcome_report(&self) -> Option<&AgentUnknownOutcomeReport>`.
  - The variant `AgentDispatchError::OutcomeUnknown { code, message, report: Option<Box<AgentUnknownOutcomeReport>> }`, marked `#[non_exhaustive]`.

- [ ] **Step 1: Write the failing tests**

In `crates/rakka-agent/tests/dispatch_failure_outcomes.rs`:
- Add `use std::time::Duration;` to the std imports.
- Add `agent_operational_snapshot`, `AgentSchemaPolicy`, `AgentUnknownOutcomeEvidence` and `AgentUnknownOutcomeReport` to the `use rakka_agent::{…}` list.
- Add `AgentTimestampMillis` to the `use rakka_agent_workflow::{…}` import.
- Append:

```rust
/// An executor that answers every call with an unknown outcome carrying
/// `report`, and records the shared clock when it is called. The fixture's
/// dispatcher reads that clock without advancing it, and nothing advances it
/// between the attempt's start and the call, so the recorded value is the
/// attempt's start.
struct ReportingExecutor {
    clock: Arc<std::sync::atomic::AtomicU64>,
    called_at: Arc<std::sync::atomic::AtomicU64>,
    report: fn() -> AgentUnknownOutcomeReport,
}

impl AgentDispatchToolExecutor for ReportingExecutor {
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _intent: &'a AgentRunEffect,
        _call: &'a AgentToolCallRequest,
        _credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentDispatchFuture<'a, AgentTaskContent> {
        self.called_at
            .store(self.clock.load(Ordering::SeqCst), Ordering::SeqCst);
        let error = AgentDispatchError::outcome_unknown(
            "scripted-outcome-unknown",
            "the call was handed over and no answer was read",
        )
        .with_report((self.report)());
        Box::pin(async move { Err(error) })
    }
}

/// A run whose tool ticket, under `spec`, is ready to claim, behind an
/// executor that reports `report`; and the cell its call time lands in.
fn reporting_fixture(
    spec: &AgentEffectSpec,
    report: fn() -> AgentUnknownOutcomeReport,
) -> (AuthorityFixture, Arc<std::sync::atomic::AtomicU64>) {
    let fx = AuthorityFixture::new(
        tool_then_proposal(),
        AgentToolAuthority::new(tool_registry_for_spec(TOOL, spec)),
        None,
    );
    let called_at = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let executor = ReportingExecutor {
        clock: fx.fx.clock.clone(),
        called_at: called_at.clone(),
        report,
    };
    (fx.with_tool_executor(Arc::new(executor)), called_at)
}

fn deadline_report() -> AgentUnknownOutcomeReport {
    AgentUnknownOutcomeReport::new()
        .with_reason("scripted-deadline-awaiting-answer")
        .with_handed_over_after(Duration::from_millis(250))
        .with_cancellation_issued(true)
}

/// Issue #100 ask 5: a non-idempotent park records the executor's reason, a
/// send time on the dispatcher's clock (the attempt's start plus the
/// reported offset), and the cancellation; the snapshot reports them.
#[tokio::test]
async fn a_parked_effect_records_what_its_executor_knew() {
    let (fx, called_at) = reporting_fixture(&AgentEffectSpec::non_idempotent(), deadline_report);
    ready(&fx).await;

    let pass = fx.one_pass().await;

    assert_eq!(pass.parked_indeterminate, 1, "{pass:?}");
    let started = called_at.load(Ordering::SeqCst);
    assert!(started > 0, "the executor ran and read the clock");
    let parked = fx.effect_at(1).await.expect("the parked effect");
    assert_eq!(parked.status, AgentRunEffectStatus::Indeterminate);
    assert_eq!(
        parked.last_error_code.as_deref(),
        Some("scripted-outcome-unknown"),
        "the pipeline code is unchanged"
    );
    assert_eq!(
        parked
            .last_error_reason
            .as_ref()
            .map(AgentFailureReason::code),
        Some("scripted-deadline-awaiting-answer")
    );
    let expected = AgentUnknownOutcomeEvidence::new(
        Some(AgentTimestampMillis::new(started + 250)),
        true,
    );
    assert_eq!(parked.unknown_outcome, Some(expected));

    let snapshot = agent_operational_snapshot(
        &fx.fx.runs,
        &run_scope(),
        &AgentSchemaPolicy::default(),
        AgentTimestampMillis::new(started + 1_000),
    )
    .await
    .expect("the point query answers")
    .expect("the run exists");
    let view = snapshot
        .pending_effects
        .iter()
        .find(|effect| effect.effect_id == parked.effect_id)
        .expect("the parked effect is reported");
    assert_eq!(view.unknown_outcome, Some(expected));
}

fn reason_only_report() -> AgentUnknownOutcomeReport {
    AgentUnknownOutcomeReport::new().with_reason("scripted-call-not-handed-over")
}

/// Review focus 1: a report that knows only why records the reason and
/// writes no evidence object.
#[tokio::test]
async fn a_report_with_only_a_reason_records_no_evidence() {
    let (fx, _) = reporting_fixture(&AgentEffectSpec::non_idempotent(), reason_only_report);
    ready(&fx).await;
    fx.one_pass().await;

    let parked = fx.effect_at(1).await.expect("the parked effect");
    assert_eq!(
        parked
            .last_error_reason
            .as_ref()
            .map(AgentFailureReason::code),
        Some("scripted-call-not-handed-over")
    );
    assert_eq!(parked.unknown_outcome, None);
}

fn absurd_offset_report() -> AgentUnknownOutcomeReport {
    AgentUnknownOutcomeReport::new().with_handed_over_after(Duration::MAX)
}

/// Review focus 2: an offset past any representable time saturates.
#[tokio::test]
async fn an_absurd_offset_saturates_rather_than_panicking() {
    let (fx, _) = reporting_fixture(&AgentEffectSpec::non_idempotent(), absurd_offset_report);
    ready(&fx).await;
    fx.one_pass().await;

    let parked = fx.effect_at(1).await.expect("the parked effect");
    assert_eq!(
        parked.unknown_outcome,
        Some(AgentUnknownOutcomeEvidence::new(
            Some(AgentTimestampMillis::new(u64::MAX)),
            false
        ))
    );
}

fn blank_reason_report() -> AgentUnknownOutcomeReport {
    AgentUnknownOutcomeReport::new()
        .with_reason("   ")
        .with_cancellation_issued(true)
}

/// Review focus 3: a blank reason identifies nothing, so none is recorded;
/// the pipeline code and the evidence still are.
#[tokio::test]
async fn a_blank_reason_code_records_no_reason_but_keeps_the_evidence() {
    let (fx, _) = reporting_fixture(&AgentEffectSpec::non_idempotent(), blank_reason_report);
    ready(&fx).await;
    fx.one_pass().await;

    let parked = fx.effect_at(1).await.expect("the parked effect");
    assert_eq!(
        parked.last_error_code.as_deref(),
        Some("scripted-outcome-unknown")
    );
    assert_eq!(parked.last_error_reason, None);
    assert_eq!(
        parked.unknown_outcome,
        Some(AgentUnknownOutcomeEvidence::new(None, true))
    );
}

/// Issue #100 ask 5: only the park records evidence. An idempotent effect
/// whose last attempt's outcome is unknown exhausts as before, carrying
/// neither the reason nor the evidence.
#[tokio::test]
async fn an_exhausted_idempotent_effect_records_no_evidence() {
    let spec = AgentEffectSpec::idempotent(1).expect("the spec is valid");
    let (fx, _) = reporting_fixture(&spec, deadline_report);
    ready(&fx).await;
    fx.one_pass().await;

    let resolved = fx.effect_at(1).await.expect("the failed effect");
    assert_eq!(resolved.status, AgentRunEffectStatus::Exhausted);
    assert_eq!(resolved.last_error_reason, None);
    assert_eq!(resolved.unknown_outcome, None);
}

/// The report rides only an unknown outcome, and reads back as set.
#[test]
fn a_report_attaches_only_to_an_unknown_outcome() {
    let unknown = AgentDispatchError::outcome_unknown("c", "m").with_report(deadline_report());
    assert_eq!(unknown.unknown_outcome_report(), Some(&deadline_report()));
    let other = AgentDispatchError::collaborator("c", "m").with_report(deadline_report());
    assert_eq!(other.unknown_outcome_report(), None);
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p rakka-agent --all-features --test dispatch_failure_outcomes`

Expected: a compile FAIL, naming only `AgentUnknownOutcomeReport`, `with_report` and `unknown_outcome_report`.

- [ ] **Step 3: Implement the report and the error's methods**

In `crates/rakka-agent/src/dispatch.rs`, add `use std::time::Duration;` beside the other `std` imports. Add `AgentUnknownOutcomeEvidence` to the `use crate::effect::{…}` import, or write `crate::effect::AgentUnknownOutcomeEvidence` at its uses. Then add, just above `pub enum AgentDispatchError`:

```rust
/// What an executor knows about an attempt whose outcome is unknown,
/// attached to [`AgentDispatchError::OutcomeUnknown`] with
/// [`AgentDispatchError::with_report`].
///
/// Observability only: the dispatcher settles the attempt by the intent's
/// safety class exactly as it would without a report, and a non-idempotent
/// park records the report — the reason as the effect's failure reason,
/// the rest as [`crate::effect::AgentUnknownOutcomeEvidence`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct AgentUnknownOutcomeReport {
    /// Which situation made the outcome unknown: the executor's own stable
    /// code.
    pub reason: Option<crate::failure::AgentFailureReason>,
    /// How long after the attempt began the request was handed over,
    /// measured on a monotonic clock. `None` when it never was. The
    /// dispatcher adds it to the attempt's start on its own clock.
    pub handed_over_after: Option<Duration>,
    /// Whether a cancellation was issued for the request: accepted by the
    /// client, never known to be received or honoured.
    pub cancellation_issued: bool,
}

impl AgentUnknownOutcomeReport {
    /// A report that knows nothing yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Names the situation. A blank code identifies nothing and sets no
    /// reason.
    #[must_use]
    pub fn with_reason(mut self, code: impl AsRef<str>) -> Self {
        self.reason = crate::failure::AgentFailureReason::new(code);
        self
    }

    /// Records how long after the attempt began the request was handed over.
    #[must_use]
    pub fn with_handed_over_after(mut self, after: Duration) -> Self {
        self.handed_over_after = Some(after);
        self
    }

    /// Records whether a cancellation was issued for the request.
    #[must_use]
    pub fn with_cancellation_issued(mut self, issued: bool) -> Self {
        self.cancellation_issued = issued;
        self
    }
}
```

Replace the `OutcomeUnknown` variant (keep the doc comment above it, changing its first sentence to "The attempt may have invoked the target and its outcome is unknown: no answer was read."):

```rust
    #[non_exhaustive]
    OutcomeUnknown {
        /// Stable machine-readable code: the executor's own.
        code: &'static str,
        /// The failure detail. It becomes durable state: no credential,
        /// argument, or content material.
        message: String,
        /// What the executor knew about the attempt, when it said
        /// ([`Self::with_report`]). Observability only.
        report: Option<Box<AgentUnknownOutcomeReport>>,
    },
```

In `outcome_unknown`, add `report: None,` to the literal. In `impl Display`, change the arm to `Self::OutcomeUnknown { code, message, .. } => {`. After `outcome_unknown`, add:

```rust
    /// Attaches what the executor knew to an unknown outcome. Every other
    /// error is returned unchanged.
    #[must_use]
    pub fn with_report(mut self, found: AgentUnknownOutcomeReport) -> Self {
        if let Self::OutcomeUnknown { report, .. } = &mut self {
            *report = Some(Box::new(found));
        }
        self
    }

    /// What the executor knew about an unknown outcome, when it said.
    #[must_use]
    pub fn unknown_outcome_report(&self) -> Option<&AgentUnknownOutcomeReport> {
        match self {
            Self::OutcomeUnknown { report, .. } => report.as_deref(),
            _ => None,
        }
    }
```

Add `AgentUnknownOutcomeReport` to `pub use dispatch::{…}` in `crates/rakka-agent/src/lib.rs`.

- [ ] **Step 4: Implement the stamping and the evidence-carrying park**

Replace the attempt timer line with one clock read that both uses share:

```rust
        let attempt_started_at = self.now();
        let attempt_timer = AgentSegmentTimer::start(attempt_started_at);
```

Pass it through the invocation match's unknown arm:

```rust
            Err(error @ AgentDispatchError::OutcomeUnknown { .. }) => {
                return self
                    .settle_unknown_outcome(
                        scope,
                        claim,
                        state,
                        intent,
                        attempt,
                        attempt_started_at,
                        &error,
                        pass,
                    )
                    .await;
            }
```

In `settle_unknown_outcome`:
- add the parameter `attempt_started_at: AgentTimestampMillis,` after `attempt: u32,`;
- change the destructure to `let AgentDispatchError::OutcomeUnknown { code, message, report } = error else {`;
- replace the `NonIdempotent` arm with:

```rust
            AgentEffectSafetyClass::NonIdempotent => {
                // Only the park records what the executor knew: a retried
                // attempt is replaced by the next one, and a reconciler's park
                // records the reconciler's own code.
                let (reason, evidence) = match report.as_deref() {
                    Some(report) => (
                        report.reason.clone(),
                        unknown_outcome_evidence(report, attempt_started_at),
                    ),
                    None => (None, None),
                };
                self.park_indeterminate_with(
                    scope, claim, intent, attempt, code, message, reason, evidence, pass,
                )
                .await
            }
```

Split `park_indeterminate`:
- The existing body becomes `park_indeterminate_with`, which adds two parameters after `message: &str,`: `reason: Option<crate::failure::AgentFailureReason>,` and `evidence: Option<AgentUnknownOutcomeEvidence>,`.
- Its outcome becomes:

```rust
            AgentRunEffectOutcome::indeterminate(
                bounded_failure_code(code),
                bounded_failure_detail(message),
            )
            .with_reason(reason)
            .with_evidence(evidence),
```

- `park_indeterminate` keeps its signature and delegates:

```rust
    /// Parks one generation as indeterminate, with nothing known beyond its
    /// code, and revokes its dispatch eligibility.
    #[allow(clippy::too_many_arguments)]
    async fn park_indeterminate(
        &mut self,
        scope: &AgentRunScope,
        claim: AgentDispatchClaim,
        intent: &AgentRunEffect,
        attempt: u32,
        code: &str,
        message: &str,
        pass: &mut AgentDispatchPass,
    ) -> AgentDispatchResult<ClaimConclusion> {
        self.park_indeterminate_with(
            scope, claim, intent, attempt, code, message, None, None, pass,
        )
        .await
    }
```

Give `park_indeterminate_with` the doc comment "Parks one generation as indeterminate with what is known about it, and revokes its dispatch eligibility." and keep `#[allow(clippy::too_many_arguments)]`.

Add this free function near `collaborator_reason`:

```rust
/// The durable evidence of a report, on the dispatcher's clock: the send
/// time is the attempt's start plus the executor's offset, in whole
/// milliseconds and saturating. `None` when the report knows neither a send
/// time nor a cancellation, so a record gains no empty object.
fn unknown_outcome_evidence(
    report: &AgentUnknownOutcomeReport,
    attempt_started_at: AgentTimestampMillis,
) -> Option<AgentUnknownOutcomeEvidence> {
    let sent_at = report.handed_over_after.map(|after| {
        let after_ms = u64::try_from(after.as_millis()).unwrap_or(u64::MAX);
        AgentTimestampMillis::new(attempt_started_at.as_millis().saturating_add(after_ms))
    });
    (sent_at.is_some() || report.cancellation_issued)
        .then(|| AgentUnknownOutcomeEvidence::new(sent_at, report.cancellation_issued))
}
```

Run `cargo fmt -p rakka-agent`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p rakka-agent --all-features --test dispatch_failure_outcomes --test operational_query`

Expected: PASS for all tests. If `a_parked_effect_records_what_its_executor_knew` fails because `sent_at` is off by a few milliseconds, something advanced the shared clock between the attempt's start and the call. Find it and report it; do not loosen the assertion.

- [ ] **Step 6: Falsify**

Temporarily change `attempt_started_at.as_millis().saturating_add(after_ms)` to `after_ms`, and run `--test dispatch_failure_outcomes`. Expected: `a_parked_effect_records_what_its_executor_knew` FAILS. Restore the line. Then temporarily make the `NonIdempotent` arm pass `None, None`. Expected: the four park tests FAIL. Restore, and rerun to green.

- [ ] **Step 7: Commit**

```bash
git add crates/rakka-agent/src crates/rakka-agent/tests
git commit -m "Report what an executor knew about an unknown outcome, and stamp its send time on the dispatcher's clock

Issue #100 ask 5, dispatcher half. AgentDispatchError::OutcomeUnknown
gains an AgentUnknownOutcomeReport (reason, handed_over_after,
cancellation_issued) and becomes #[non_exhaustive]; the non-idempotent
park records the reason and evidence whose sent_at is the attempt's start
plus the reported offset. The other safety-class arms are unchanged.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

Then run `cargo clean`.

---

### Task 3: The MCP executor names the outcome by how far the call got

**Files:**
- Modify: `crates/rakka-agent-mcp/src/client.rs`: `call_tool` (~370-395) and `cancel_and_close` (~397-415). Add `CallProgress`.
- Modify: `crates/rakka-agent-mcp/src/executor.rs`:
  - `attempt` (~375-465) and `call` (~530-560);
  - remove `within_after_send` (~918-936), and change `sent_call_error` (~938-951);
  - add the reason constants, `UnknownCause`, `unknown_reason`, `unknown_outcome` and `cancellation_issued_on`;
  - the unit-test module (~1163).
- Test: `crates/rakka-agent-mcp/tests/client_dispatch.rs`:
  - `a_timeout_after_the_call_was_sent_leaves_the_outcome_unknown` (~883);
  - `a_transport_failure_after_the_call_was_sent_leaves_the_outcome_unknown` (~1191);
  - the `abandon_slow_call` helper (~1323).
- Test: `crates/rakka-agent-mcp/tests/artifact_sink.rs`: `a_deadline_that_elapses_while_the_answer_is_stored_leaves_the_outcome_unknown` (~469).

**Interfaces:**
- Consumes (Task 2): `AgentUnknownOutcomeReport::new().with_reason(..).with_handed_over_after(..).with_cancellation_issued(..)`, `AgentDispatchError::outcome_unknown(..).with_report(..)`, `AgentDispatchError::unknown_outcome_report()`, and the report's public fields.
- Produces (crate-private):
  - `client::CallProgress` (`NotHandedOver`, `AwaitingAnswer { request: RequestId, handed_over: tokio::time::Instant }`, `WaitEnded { handed_over: tokio::time::Instant, answered: bool }`) and `CallProgress::handed_over() -> Option<tokio::time::Instant>`;
  - `McpClientSession::call_tool(&self, params, progress: &mut CallProgress)`;
  - `McpClientSession::cancel_and_close(self, request, reason) -> bool`.

- [ ] **Step 1: Write the failing tests**

In `crates/rakka-agent-mcp/tests/client_dispatch.rs`, add `AgentFailureReason` to the `use rakka_agent::{…}` list.

In `a_timeout_after_the_call_was_sent_leaves_the_outcome_unknown`, append after the existing `call_count` assertion:

```rust
    let report = error
        .unknown_outcome_report()
        .expect("the executor said what it knew");
    assert_eq!(
        report.reason.as_ref().map(AgentFailureReason::code),
        Some("mcp-deadline-awaiting-answer")
    );
    assert!(
        report
            .handed_over_after
            .is_some_and(|after| after < Duration::from_millis(200)),
        "handed over inside the timeout: {report:?}"
    );
    assert!(
        report.cancellation_issued,
        "a call the deadline abandoned is cancelled: {report:?}"
    );
```

In `abandon_slow_call`, append after its final assertion:

```rust
    let report = error
        .unknown_outcome_report()
        .expect("the executor said what it knew");
    assert_eq!(
        report.reason.as_ref().map(AgentFailureReason::code),
        Some("mcp-deadline-awaiting-answer")
    );
    assert!(
        report.cancellation_issued,
        "the cancellation was issued, whatever the protocol revision carries it as: {report:?}"
    );
```

In `a_transport_failure_after_the_call_was_sent_leaves_the_outcome_unknown`, append:

```rust
    let report = error
        .unknown_outcome_report()
        .expect("the executor said what it knew");
    assert_eq!(
        report.reason.as_ref().map(AgentFailureReason::code),
        Some("mcp-transport-awaiting-answer")
    );
    assert!(report.handed_over_after.is_some(), "{report:?}");
    assert!(
        !report.cancellation_issued,
        "a broken transport has no session to cancel on"
    );
```

In `crates/rakka-agent-mcp/tests/artifact_sink.rs`, add `AgentFailureReason` to its `use rakka_agent::{…}` list. In `a_deadline_that_elapses_while_the_answer_is_stored_leaves_the_outcome_unknown`, append:

```rust
    let report = error
        .unknown_outcome_report()
        .expect("the executor said what it knew");
    assert_eq!(
        report.reason.as_ref().map(AgentFailureReason::code),
        Some("mcp-answer-not-recorded"),
        "the server answered; storing its answer overran the deadline"
    );
    assert!(report.handed_over_after.is_some(), "{report:?}");
    assert!(!report.cancellation_issued, "nothing was in flight to cancel");
```

In `crates/rakka-agent-mcp/src/executor.rs`'s `#[cfg(test)] mod tests`:
- add `use std::time::Duration;` and `use rmcp::model::RequestId;`;
- add `use rakka_agent::AgentFailureReason;`;
- add `use crate::client::CallProgress;`;
- add `cancellation_issued_on, unknown_outcome, UnknownCause` to the `use super::{…}` list;
- append:

```rust
    /// Review focus 5 and the spec's table: every progress state against both
    /// causes names the situation by how far the call got, reports the
    /// hand-off offset when there was one, and never claims a cancellation.
    #[test]
    fn an_unknown_outcome_is_named_by_how_far_the_call_got() {
        let started = tokio::time::Instant::now();
        let handed_over = started + Duration::from_millis(40);
        let awaiting = CallProgress::AwaitingAnswer {
            request: RequestId::Number(7),
            handed_over,
        };
        let cases = [
            (CallProgress::NotHandedOver, UnknownCause::Deadline, "mcp-call-not-handed-over", None),
            (CallProgress::NotHandedOver, UnknownCause::Transport, "mcp-call-not-handed-over", None),
            (awaiting.clone(), UnknownCause::Deadline, "mcp-deadline-awaiting-answer", Some(40)),
            (awaiting, UnknownCause::Transport, "mcp-transport-awaiting-answer", Some(40)),
            (
                CallProgress::WaitEnded { handed_over, answered: false },
                UnknownCause::Transport,
                "mcp-transport-awaiting-answer",
                Some(40),
            ),
            (
                CallProgress::WaitEnded { handed_over, answered: false },
                UnknownCause::Deadline,
                "mcp-transport-awaiting-answer",
                Some(40),
            ),
            (
                CallProgress::WaitEnded { handed_over, answered: true },
                UnknownCause::Deadline,
                "mcp-answer-not-recorded",
                Some(40),
            ),
            (
                CallProgress::WaitEnded { handed_over, answered: true },
                UnknownCause::Transport,
                "mcp-answer-not-recorded",
                Some(40),
            ),
        ];
        for (progress, cause, code, after_ms) in cases {
            let error = unknown_outcome(cause, &progress, started, "detail");
            assert_eq!(error.code(), "mcp-outcome-unknown", "{progress:?} {cause:?}");
            let report = error.unknown_outcome_report().expect("a report");
            assert_eq!(
                report.reason.as_ref().map(AgentFailureReason::code),
                Some(code),
                "{progress:?} {cause:?}"
            );
            assert_eq!(
                report.handed_over_after,
                after_ms.map(Duration::from_millis),
                "{progress:?} {cause:?}"
            );
            assert!(!report.cancellation_issued, "{progress:?} {cause:?}");
        }
    }

    /// The close's cancellation lands on the unknown outcome's report, and
    /// nowhere else.
    #[test]
    fn an_issued_cancellation_marks_only_an_unknown_outcome() {
        let started = tokio::time::Instant::now();
        let marked = cancellation_issued_on(unknown_outcome(
            UnknownCause::Deadline,
            &CallProgress::NotHandedOver,
            started,
            "detail",
        ));
        assert!(marked
            .unknown_outcome_report()
            .is_some_and(|report| report.cancellation_issued));
        let other = cancellation_issued_on(AgentDispatchError::collaborator("c", "m"));
        assert_eq!(other.unknown_outcome_report(), None);
    }
```

(If `AgentDispatchError` is not already imported in the test module, add it to its `use rakka_agent::{…}` list.)

- [ ] **Step 2: Run the tests to verify they fail**

Run these two commands separately, because a compile error in one target stops `cargo test` before any target runs:

`cargo test -p rakka-agent-mcp --all-features --test client_dispatch --test artifact_sink`

Expected: the four extended tests FAIL with "the executor said what it knew" (no report yet), and every other test passes.

`cargo test -p rakka-agent-mcp --all-features --lib`

Expected: a compile FAIL, naming `CallProgress`, `UnknownCause`, `unknown_outcome` and `cancellation_issued_on`.

- [ ] **Step 3: Implement `CallProgress` and the client changes**

In `crates/rakka-agent-mcp/src/client.rs`, add above `impl McpClientSession` (or beside `CallToolResponse`):

```rust
/// How far one `tools/call` got, as the attempt that sent it last saw it.
///
/// The executor reads it after the call settles, or after the attempt's
/// deadline dropped the call, to name an unknown outcome and to cancel a
/// call still in flight.
#[derive(Debug, Clone)]
pub(crate) enum CallProgress {
    /// rmcp has not accepted the request: nothing left.
    NotHandedOver,
    /// rmcp accepted the request at `handed_over` and no answer has been
    /// read. A deadline that drops the call here leaves `request` behind for
    /// the cancellation.
    AwaitingAnswer {
        request: RequestId,
        handed_over: tokio::time::Instant,
    },
    /// The wait ended. `answered` is whether the server answered — a result
    /// or a JSON-RPC error — rather than the transport failing.
    WaitEnded {
        handed_over: tokio::time::Instant,
        answered: bool,
    },
}

impl CallProgress {
    /// When rmcp accepted the request, once it did.
    pub(crate) const fn handed_over(&self) -> Option<tokio::time::Instant> {
        match self {
            Self::NotHandedOver => None,
            Self::AwaitingAnswer { handed_over, .. } | Self::WaitEnded { handed_over, .. } => {
                Some(*handed_over)
            }
        }
    }
}
```

Replace `call_tool`. Its doc comment becomes:

```rust
    /// Sends one `tools/call` and waits for its answer, recording in
    /// `progress` how far it got.
    ///
    /// The request is handed over when rmcp accepts it onto its own queue —
    /// the closest point this client can observe; the wire write follows on
    /// rmcp's worker. A deadline that drops this future while it waits leaves
    /// `progress` at [`CallProgress::AwaitingAnswer`], with the id a
    /// cancellation needs; any end of the wait moves it to
    /// [`CallProgress::WaitEnded`]. The answer is mapped as rmcp's own
    /// `call_tool_once` maps it.
```

and its signature and body:

```rust
    pub(crate) async fn call_tool(
        &self,
        params: CallToolRequestParams,
        progress: &mut CallProgress,
    ) -> Result<CallToolResponse, ServiceError> {
        let handle = self
            .peer()
            .send_cancellable_request(
                ClientRequest::CallToolRequest(CallToolRequest::new(params)),
                PeerRequestOptions::no_options(),
            )
            .await?;
        let handed_over = tokio::time::Instant::now();
        *progress = CallProgress::AwaitingAnswer {
            request: handle.id.clone(),
            handed_over,
        };
        let answer = handle.await_response().await;
        *progress = CallProgress::WaitEnded {
            handed_over,
            answered: matches!(answer, Ok(_) | Err(ServiceError::McpError(_))),
        };
        match answer? {
            ServerResult::CallToolResult(result) => Ok(CallToolResponse::Complete(result)),
            ServerResult::InputRequiredResult(result) => {
                Ok(CallToolResponse::InputRequired(result))
            }
            ServerResult::CreateTaskResult(result) => Ok(CallToolResponse::Task(result)),
            _ => Err(ServiceError::UnexpectedResponse),
        }
    }
```

Change `cancel_and_close` to return whether the cancellation was issued. Append this paragraph to its doc comment:

```rust
    ///
    /// Answers whether the cancellation was issued: rmcp accepted it within
    /// the bound. Never that the server received or honoured it.
```

and its signature and body:

```rust
    pub(crate) async fn cancel_and_close(mut self, request: RequestId, reason: &str) -> bool {
        let bound = tokio::time::Instant::now() + SESSION_CLOSE_BOUND;
        let notice = CancelledNotificationParam::new(Some(request), Some(reason.to_string()));
        let notified =
            tokio::time::timeout_at(bound, self.peer().notify_cancelled(notice)).await;
        let left = bound.saturating_duration_since(tokio::time::Instant::now());
        let _ = self.running.close_with_timeout(left).await;
        matches!(notified, Ok(Ok(())))
    }
```

- [ ] **Step 4: Implement the classification in the executor**

In `crates/rakka-agent-mcp/src/executor.rs`, add `AgentUnknownOutcomeReport` to the `use rakka_agent::{…}` list. Below `const MCP_OUTCOME_UNKNOWN`, add:

```rust
/// The reason an unknown outcome records when rmcp never accepted the
/// request: it provably never left, yet the attempt still parks (issue #100
/// ask 5 records it; changing that is a separate follow-up).
const MCP_CALL_NOT_HANDED_OVER: &str = "mcp-call-not-handed-over";

/// The reason when the deadline fired while the attempt waited for the
/// answer to a request it handed over.
const MCP_DEADLINE_AWAITING_ANSWER: &str = "mcp-deadline-awaiting-answer";

/// The reason when the transport failed after the hand-off, with no answer
/// read.
const MCP_TRANSPORT_AWAITING_ANSWER: &str = "mcp-transport-awaiting-answer";

/// The reason when the server answered and its answer was not recorded: the
/// deadline fired while the answer was mapped or stored, or the answer was
/// of a kind `tools/call` never returns.
const MCP_ANSWER_NOT_RECORDED: &str = "mcp-answer-not-recorded";

/// What ended an attempt whose outcome is unknown.
#[derive(Debug, Clone, Copy)]
enum UnknownCause {
    /// The attempt's deadline.
    Deadline,
    /// A failure of the transport or the session, not the server's answer.
    Transport,
}

/// The reason an unknown outcome records: decided by how far the call got,
/// never by which wrapper noticed.
fn unknown_reason(cause: UnknownCause, progress: &CallProgress) -> &'static str {
    match progress {
        CallProgress::NotHandedOver => MCP_CALL_NOT_HANDED_OVER,
        CallProgress::AwaitingAnswer { .. } => match cause {
            UnknownCause::Deadline => MCP_DEADLINE_AWAITING_ANSWER,
            UnknownCause::Transport => MCP_TRANSPORT_AWAITING_ANSWER,
        },
        CallProgress::WaitEnded { answered: true, .. } => MCP_ANSWER_NOT_RECORDED,
        CallProgress::WaitEnded { answered: false, .. } => MCP_TRANSPORT_AWAITING_ANSWER,
    }
}

/// The unknown outcome of an attempt that got as far as `progress`, with
/// the report a park records: the reason, and the hand-off's offset from
/// `started` when there was one. The cancellation is the close's to add
/// ([`cancellation_issued_on`]).
fn unknown_outcome(
    cause: UnknownCause,
    progress: &CallProgress,
    started: tokio::time::Instant,
    message: impl Into<String>,
) -> AgentDispatchError {
    let mut report = AgentUnknownOutcomeReport::new().with_reason(unknown_reason(cause, progress));
    if let Some(at) = progress.handed_over() {
        report = report.with_handed_over_after(at.saturating_duration_since(started));
    }
    AgentDispatchError::outcome_unknown(MCP_OUTCOME_UNKNOWN, message).with_report(report)
}

/// Marks an unknown outcome's report with the cancellation the close issued.
/// Every other error is returned unchanged.
fn cancellation_issued_on(error: AgentDispatchError) -> AgentDispatchError {
    match error.unknown_outcome_report().cloned() {
        Some(report) => error.with_report(report.with_cancellation_issued(true)),
        None => error,
    }
}
```

Delete `within_after_send` and its doc comment. Replace `sent_call_error` with:

```rust
/// Maps a failed `tools/call` onto the dispatch pipeline's error.
///
/// A JSON-RPC error is the server's own answer — it told us what it did —
/// and keeps the mapping a failure before the send gets. Every other failure
/// left no answer to read, so the outcome is unknown and named by how far the
/// call got.
fn sent_call_error(
    server: &str,
    error: &ServiceError,
    progress: &CallProgress,
    started: tokio::time::Instant,
) -> AgentDispatchError {
    let mapped = client::service_error(server, error);
    match error {
        ServiceError::McpError(_) => dispatch_error(mapped),
        _ => unknown_outcome(
            UnknownCause::Transport,
            progress,
            started,
            format!("no answer to tools/call was read: {mapped}"),
        ),
    }
}
```

In `call`:
- replace the parameter `in_flight: &mut Option<RequestId>,` with `progress: &mut CallProgress,` and `started: tokio::time::Instant,`;
- replace the `let answer = …` statement with:

```rust
        // No timeout of its own: the attempt's deadline wraps this whole
        // function, recheck included.
        let answer = session.call_tool(params, progress).await;
        let answer = answer.map_err(|error| {
            sent_call_error(server.binding.server_id.as_str(), &error, progress, started)
        })?;
```

In `attempt`:
- take the start before the deadline: `let started = tokio::time::Instant::now();` on the line above `let deadline = attempt_deadline(...)`;
- replace the `in_flight` declaration and its comment with:

```rust
        // How far the call got. A deadline that drops the call while it waits
        // leaves it at `AwaitingAnswer`, so the server can be told the call
        // was abandoned; every unknown outcome is named by it.
        let mut progress = CallProgress::NotHandedOver;
```

- replace the `within_after_send(...)` expression inside the `outcome` block with:

```rust
            let called = tokio::time::timeout_at(
                deadline,
                self.call(
                    &session,
                    scope,
                    server,
                    descriptor,
                    policy,
                    intent,
                    call,
                    &secrets,
                    &mut progress,
                    started,
                ),
            )
            .await;
            match called {
                Ok(result) => result,
                // Past the recheck the request may have been handed over, so a
                // deadline here leaves the outcome unknown rather than failing
                // the attempt.
                Err(_) => Err(unknown_outcome(
                    UnknownCause::Deadline,
                    &progress,
                    started,
                    "the attempt exceeded the effect's timeout during tools/call",
                )),
            }
```

- replace the close `match in_flight { … }` and the final `outcome` with:

```rust
        let cancellation_issued = match progress {
            CallProgress::AwaitingAnswer { request, .. } => {
                session
                    .cancel_and_close(request, DEADLINE_CANCEL_REASON)
                    .await
            }
            _ => {
                session.close().await;
                false
            }
        };
        if cancellation_issued {
            outcome.map_err(cancellation_issued_on)
        } else {
            outcome
        }
```

Import `CallProgress` (`use crate::client::CallProgress;`, or name it as `client::CallProgress`, matching how the file already refers to `client::`). Remove `RequestId` from the `rmcp::model` import if the compiler reports it unused. Update any remaining doc comment that mentions `within_after_send` or `in_flight` so it describes `CallProgress` instead (`rg -n "within_after_send|in_flight" crates/rakka-agent-mcp` must print nothing).

Run `cargo fmt -p rakka-agent-mcp`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p rakka-agent-mcp --all-features`

Expected: PASS, every test. If `on_a_modern_server_the_aborted_request_is_the_cancellation` fails on `report.cancellation_issued`, rmcp answered the modern-session cancellation with an error. Stop and report it: the spec's claim about modern sessions would then be wrong. Do not weaken the assertion.

- [ ] **Step 6: Falsify**

Temporarily make `unknown_reason` return `MCP_CALL_NOT_HANDED_OVER` for every input. Expected: the classifier unit test and the four extended integration tests FAIL. Restore it. Temporarily make `cancel_and_close` return `false`. Expected: the legacy and modern deadline tests and `a_timeout_after_the_call_was_sent_leaves_the_outcome_unknown` FAIL. Restore, and rerun to green.

- [ ] **Step 7: Commit**

```bash
git add crates/rakka-agent-mcp
git commit -m "Name an unknown MCP outcome by how far the call got, with its hand-off offset and cancellation

Issue #100 ask 5, executor half. CallProgress records the hand-off to
rmcp and the end of the wait; an unknown outcome's report carries one of
mcp-call-not-handed-over, mcp-deadline-awaiting-answer,
mcp-transport-awaiting-answer or mcp-answer-not-recorded, the offset from
the attempt's start, and whether cancel_and_close issued the
cancellation. Which failures are unknown, and how they settle, is
unchanged.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

Then run `cargo clean`.

---

### Task 4: Docs, the follow-up issue, validation and the PR

**Files:**
- Modify: `docs/rakka-compatibility.md`. Add a new bullet directly above `- Issue #100 asks 3 and 4 (`.
- Modify: `CHANGELOG.md`. Add a new `### Fixed` bullet directly after the line starting `- A run's operational snapshot could not say which tool a pending or parked effect calls`.
- Modify: `docs/rakka-agents.md`, the sentence ending "a non-idempotent tool parks for reconciliation" (~line 157).

- [ ] **Step 1: File the follow-up issue for the call that never left**

```bash
gh issue create --title "An MCP call that never left still parks for reconciliation" --body "$(cat <<'EOF'
Found while landing #100 ask 5. When the attempt's deadline fires, or the transport closes, before rmcp accepts a `tools/call` onto its service channel (`rmcp-3.4.0/src/service.rs:919-937`; the send is cancel-safe and `TransportClosed` there means the request was never queued), the request provably never left. The MCP executor still reports it as an unknown outcome, so a non-idempotent effect parks `WaitingForReconciliation` for a person to decide.

Since #100 ask 5 the parked effect's reason says so: `mcp-call-not-handed-over`, with no `sent_at`. The owner ruled (2026-10-13) to record it there and fix the behaviour separately.

Proposed fix: map that case to the retryable `mcp-transport-failed`, as a failure before the send already is, so it retries under the budget and never parks. That changes a dispatch decision, so it needs its own compatibility entry.
EOF
)"
```

Record the issue number it prints, since Steps 2 and 5 cite it.

- [ ] **Step 2: Write the docs**

Compatibility bullet, with `<N>` replaced by the issue number from Step 1:

```markdown
- Issue #100 ask 5 ("what an unknown MCP outcome knows") adds two public types, one serde-defaulted field on `AgentRunEffect`, one on `AgentPendingEffectView`, two on the `Indeterminate` outcome, and four stable MCP reason codes; no code, settlement, or schema version changes, and no dispatch decision reads any of them. **The MCP executor** now names an unknown outcome by how far its `tools/call` got, as the reason under the unchanged pipeline code `mcp-outcome-unknown`: `mcp-call-not-handed-over` (rmcp never accepted the request, so it provably never left — it still parks; #<N> tracks changing that), `mcp-deadline-awaiting-answer`, `mcp-transport-awaiting-answer`, and `mcp-answer-not-recorded` (the server answered; the deadline fired while its answer was mapped or stored, or the answer was of a kind `tools/call` never returns). "Handed over" is rmcp accepting the request onto its own queue; the wire write follows. **`AgentUnknownOutcomeReport`** (`reason`, `handed_over_after`, `cancellation_issued`) is what an executor attaches with `AgentDispatchError::with_report` and reads back with `unknown_outcome_report`; `AgentDispatchError::OutcomeUnknown` gains its `report` field and becomes `#[non_exhaustive]`. Only a `NonIdempotent` park records it: the reason as `last_error_reason`, and the new **`AgentUnknownOutcomeEvidence`** (`sent_at`, `cancellation_issued`) as `AgentRunEffect::unknown_outcome`, which a new generation clears and `AgentPendingEffectView::unknown_outcome` reports. `sent_at` is the dispatcher's own clock: the attempt's start plus the executor's monotonic offset, in whole milliseconds, saturating; the executor still reads no wall clock. `cancellation_issued` means rmcp accepted the cancellation within the session close bound — on a legacy (2025-11-25 and earlier) or stdio session a `notifications/cancelled` message, on a modern HTTP session the abort of the request's HTTP exchange — never that the server received or honoured it; only `mcp-deadline-awaiting-answer` can carry it. A report that knows neither a send time nor a cancellation writes no evidence object. The retry arms (`ReadOnly`, `Idempotent`) and the `Reconcileable` arm settle exactly as before and record none of it. **`AgentRunEffectOutcome::Indeterminate`** gains `reason` and `evidence` and becomes `#[non_exhaustive]`, built with the new `AgentRunEffectOutcome::indeterminate(code, message)`; `with_reason` and `failure_reason` now cover it, and `with_evidence`/`unknown_outcome_evidence` are new. Every new field is `#[serde(default)]` and omitted while unset, so records and outcomes without them encode byte-identically, and an older peer ignores or drops them, degrading to today's answer. Source changes: an external pattern of either widened variant needs `..`, and an external literal of `Indeterminate` must use the constructor (rakka.host has one, a unit test). Reason codes are never metric labels.
```

Changelog bullet, with `<N>` replaced likewise:

```markdown
- A non-idempotent MCP call whose answer was lost parked for a person to reconcile with only `mcp-outcome-unknown` to go on: nothing said whether the deadline fired or the connection dropped, whether the server had even received the call or had already answered it, when the call was sent, or whether the server was told to stop. The MCP executor now names the situation by how far the call got (`mcp-call-not-handed-over`, `mcp-deadline-awaiting-answer`, `mcp-transport-awaiting-answer`, `mcp-answer-not-recorded`), and the parked effect records it as its failure reason beside `unknown_outcome` — when the request was handed over, on the dispatcher's clock, and whether a cancellation was issued — which the operational snapshot's pending effect reports. A call that never left still parks; #<N> tracks that (issue #100, ask 5).
```

`docs/rakka-agents.md`: replace

```markdown
   A call whose request was sent and whose answer was lost has an unknown
   outcome and is settled by its safety class — a non-idempotent tool parks
   for reconciliation — and a refusal the deployment declares definitive (a
```

with:

```markdown
   A call whose request was sent and whose answer was lost has an unknown
   outcome and is settled by its safety class — a non-idempotent tool parks
   for reconciliation, recording how far the call got, when it was handed
   over, and whether a cancellation was issued — and a refusal the
   deployment declares definitive (a
```

- [ ] **Step 3: Validate**

```bash
scripts/validate.sh > "$SCRATCH/validate-c.log" 2>&1; echo "exit=$?" >> "$SCRATCH/validate-c.log"
```

Here `$SCRATCH` is the session scratchpad. Run it in the background and wait for the notification; don't poll in the foreground.

Expected: the last line is `exit=0`, and `grep -E "^test result:" "$SCRATCH/validate-c.log"` shows `0 failed` on every line. If the one-shot workspace test run gets killed under load, rerun the failing crates per crate (`cargo test -p <crate> --all-features`) and report both runs.

- [ ] **Step 4: Commit the docs**

```bash
git add CHANGELOG.md docs/rakka-compatibility.md docs/rakka-agents.md
git commit -m "Record issue #100 ask 5 in the compatibility document and the changelog

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 5: Push and open the PR**

```bash
git push -u origin run-evidence-unknown-outcome
gh pr create --base main --head run-evidence-unknown-outcome \
  --title "Run evidence: what an unknown MCP outcome knows (#100 ask 5)" \
  --body-file "$SCRATCH/pr-c.md"
```

`pr-c.md` must cover, in the style of #101 and #102:
- the four reason codes, and what "handed over" and "issued" mean;
- the public names: `AgentUnknownOutcomeReport`, `AgentUnknownOutcomeEvidence`, `AgentRunEffectOutcome::indeterminate`, `with_evidence`, `unknown_outcome_evidence`, `AgentDispatchError::with_report` and `unknown_outcome_report`, and the `unknown_outcome` fields;
- the wire impact (additive);
- the one host change (`crates/rakka-host-runtime/src/fleet.rs:2368` moves to `AgentRunEffectOutcome::indeterminate`);
- the follow-up issue #<N>;
- the spec amendment (`mcp-answer-not-recorded`);
- the tests, the falsifications and the validation result;
- `Refs #100`, and the line `🤖 Generated with [Claude Code](https://claude.com/claude-code)`.
