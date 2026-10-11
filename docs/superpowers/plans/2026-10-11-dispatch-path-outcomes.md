# Dispatch-Path Outcomes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a dispatch collaborator declare a failure definitive (exhausting its ticket on that attempt), let an executor report an unknown outcome (handled by specification 11.5's safety-class table), and make the MCP executor use both — issue #85 asks 7, 14, and 15.

**Architecture:** Two additive `AgentDispatchError` variants carry the new meanings from any executor to the dispatcher. `Definitive` reaches `record_attempt_failure`, which exhausts the outbox entry through a new substrate write; `OutcomeUnknown` reaches a new `settle_unknown_outcome` that reuses the worker-loss recovery paths. The MCP executor maps a non-retryable egress refusal and a refused artifact write to `Definitive`, and splits its one deadline into a before-send and an after-send window so every failure after `tools/call` is sent (except a JSON-RPC error answer) becomes `OutcomeUnknown`.

**Tech Stack:** Rust 1.88 (MSRV), tokio, rmcp 3.4.0, the workspace's `rakka-workflow` / `rakka-agent-workflow` / `rakka-agent` / `rakka-agent-mcp` crates.

**Spec:** `docs/superpowers/specs/2026-10-11-dispatch-path-outcomes-design.md`

## Global Constraints

- MSRV is Rust 1.88: `cargo +1.88.0 check --workspace --all-targets` must pass; no API newer than 1.88.
- Workspace lints: `missing_docs = "warn"` and validation runs `clippy -D warnings`, so every new public item has a doc comment and no warning survives.
- Error text that reaches a durable record (every `AgentDispatchError` `Display`, every outbox line) carries no credential, argument, or content material.
- Stable codes are kebab-case and are registered in `docs/rakka-compatibility.md`: `Definitive::code()` is `dispatch-collaborator-failed`; `AgentArtifactError::Refused::code()` is `artifact-store-refused`; the MCP unknown-outcome code is `mcp-outcome-unknown`.
- `AgentDispatchError` stays `#[non_exhaustive]`; `AgentArtifactError` becomes `#[non_exhaustive]` in Task 4 (a deliberate, documented source break).
- No schema version moves and no durable field is added.
- This machine's rule: run `cargo clean` between tasks (never while a cargo command is still running); every task's first build is a full rebuild, so run tests per crate.
- Commit subjects are imperative sentences with no prefix; bodies explain the mechanism; every commit ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Branch: `dispatch-path-outcomes` (already created off `e2e7a29`, with the spec committed as `72632b2`).

## Review Focus

1. An idempotent effect whose **last** attempt ends in an unknown outcome exhausts like any spent budget, with the executor's code — tested in Task 3.
2. The deadline elapsing **after** the server answered (while the result is being stored) is an unknown outcome, not a retryable failure — tested in Task 5 with a slow artifact sink.
3. A JSON-RPC error answer to `tools/call` is a known outcome and keeps today's retryable mapping — tested in Task 5.
4. An egress refusal the host marks `retryable: true` is still retried — tested in Task 4.
5. A failure before the send (a stalled listing, an unanswered handshake) still reads `mcp-transport-failed` and is retried — held by the existing tests `the_intents_timeout_bounds_a_listing_that_stalls_before_the_call` and `the_intents_timeout_bounds_a_handshake_that_is_never_answered`, which Task 5 must leave passing unchanged.

---

## File Structure

| File | Change | Responsibility |
| --- | --- | --- |
| `crates/rakka-workflow/src/model.rs` | Modify | `OutboxEntry::record_definitive_failure` — the exhausting transition |
| `crates/rakka-workflow/src/inbox.rs` | Modify | `DurableInbox::record_outbox_exhausted` — the durable write |
| `crates/rakka-workflow/tests/durable_inbox.rs` | Modify | substrate proofs |
| `crates/rakka-agent/src/dispatch.rs` | Modify | the two error variants; `InboxWrite::Exhausted`; `record_attempt_failure`'s `definitive` flag; `settle_unknown_outcome` |
| `crates/rakka-agent/tests/common/mod.rs` | Modify | `AuthorityFixture::with_reconciler` |
| `crates/rakka-agent/tests/dispatch_failure_outcomes.rs` | Create | dispatcher proofs with a scripted failing executor |
| `crates/rakka-agent-workflow/src/artifacts.rs` | Modify | `AgentArtifactError::Refused`, `#[non_exhaustive]` |
| `crates/rakka-agent-mcp/src/executor.rs` | Modify | the egress and artifact mappings; the two windows; `sent_call_error` |
| `crates/rakka-agent-mcp/src/testkit.rs` | Modify | `FakeMcpServer::with_call_transport_failure`; `FakeToolBehaviour::RpcError` |
| `crates/rakka-agent-mcp/tests/client_dispatch.rs` | Modify | executor-level mapping proofs; the call-timeout test's new expectation |
| `crates/rakka-agent-mcp/tests/artifact_sink.rs` | Modify | refused and slow sinks |
| `crates/rakka-agent/tests/mcp_client_dispatch.rs` | Modify | end-to-end proofs through the real dispatcher |
| `docs/plans/rakka-agent/spec.md`, `docs/rakka-compatibility.md`, `docs/rakka-agents.md`, `CHANGELOG.md`, the spec's status line | Modify | Task 6 |

---

### Task 1: The substrate's exhausting write

**Files:**
- Modify: `crates/rakka-workflow/src/model.rs` (after `OutboxEntry::record_failure`, which ends near line 668)
- Modify: `crates/rakka-workflow/src/inbox.rs` (after `DurableInbox::record_outbox_failure`, which ends near line 635)
- Test: `crates/rakka-workflow/tests/durable_inbox.rs`

**Interfaces:**
- Consumes: nothing new.
- Produces: `pub async fn record_outbox_exhausted(&mut self, message_id: &OutboxMessageId, message: impl Into<String>) -> WorkflowResult<WorkflowTelemetryEvent>` on `DurableInbox<Store, Clock>`, answering `WorkflowTelemetryEvent::OutboxDispatchExhausted { message_id, attempts, message }`. Task 2 calls it.

- [ ] **Step 1: Write the failing tests**

Append to `crates/rakka-workflow/tests/durable_inbox.rs`:

```rust
#[tokio::test]
async fn a_definitive_failure_exhausts_an_entry_with_budget_left() {
    let store = InMemoryDurableStateStore::<WorkflowState>::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(900));
    let workflow_id = WorkflowId::new("workflow-outbox-definitive");
    let mut workflow = DurableInbox::with_clock(workflow_id.clone(), store.clone(), clock.clone());
    workflow.recover().await.unwrap();
    workflow
        .schedule_outbox(
            OutboxCommand::new(
                "out-definitive",
                OutboxTarget::application("payments"),
                "Charge",
                b"payload".to_vec(),
            )
            .retry_policy(RetryPolicy::new(3, 10, 10)),
        )
        .await
        .unwrap();

    let event = workflow
        .record_outbox_exhausted(&OutboxMessageId::new("out-definitive"), "egress denied")
        .await
        .expect("the definitive failure is recorded");

    assert_eq!(
        event,
        WorkflowTelemetryEvent::OutboxDispatchExhausted {
            message_id: OutboxMessageId::new("out-definitive"),
            attempts: 1,
            message: "egress denied".to_string(),
        }
    );
    let persisted = store
        .load(&workflow_id.persistence_id())
        .await
        .unwrap()
        .expect("workflow state should be persisted");
    let entry = persisted
        .state
        .outbox_entry(&OutboxMessageId::new("out-definitive"))
        .unwrap();
    assert_eq!(entry.status(), OutboxStatus::Exhausted);
    assert_eq!(entry.attempts().attempts(), 1);
    assert_eq!(
        entry.attempts().max_attempts_value(),
        Some(3),
        "two attempts of budget were left, and spent"
    );
    assert_eq!(entry.attempts().next_retry_at(), None);
    assert_eq!(entry.attempts().last_error(), Some("egress denied"));
    clock.advance_millis(1_000);
    assert!(
        workflow.due_outbox().unwrap().is_empty(),
        "an exhausted entry is never due again"
    );
}

#[tokio::test]
async fn a_definitive_failure_for_an_unknown_message_is_refused() {
    let store = InMemoryDurableStateStore::<WorkflowState>::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(900));
    let mut workflow =
        DurableInbox::with_clock(WorkflowId::new("workflow-outbox-missing"), store, clock);
    workflow.recover().await.unwrap();

    let error = workflow
        .record_outbox_exhausted(&OutboxMessageId::new("missing"), "egress denied")
        .await
        .expect_err("no such entry");

    assert!(
        matches!(error, WorkflowError::OutboxEntryNotFound { .. }),
        "{error:?}"
    );
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p rakka-workflow --test durable_inbox definitive`
Expected: FAIL to compile with `no method named record_outbox_exhausted found`.

- [ ] **Step 3: Implement the transition and the write**

In `crates/rakka-workflow/src/model.rs`, inside `impl OutboxEntry`, directly after `record_failure`:

```rust
    /// Records a failed dispatch that retrying cannot change, exhausting the
    /// entry whatever budget its retry policy has left.
    pub(crate) fn record_definitive_failure(
        &mut self,
        now: WorkflowTimestamp,
        message: impl Into<String>,
    ) {
        self.attempts = self.attempts.clone().record_exhaustion(message);
        self.set_status(OutboxStatus::Exhausted, now);
    }
```

In `crates/rakka-workflow/src/inbox.rs`, inside `impl DurableInbox`, directly after `record_outbox_failure`:

```rust
    /// Records a failed outbox dispatch that retrying cannot change: the entry
    /// is exhausted on this attempt, whatever budget its retry policy has
    /// left.
    ///
    /// For a failure its reporter declared definitive. In every other respect
    /// it is [`Self::record_outbox_failure`]: the same recovered-record
    /// compare-and-set, the same refusal of an unknown message id.
    pub async fn record_outbox_exhausted(
        &mut self,
        message_id: &OutboxMessageId,
        message: impl Into<String>,
    ) -> WorkflowResult<WorkflowTelemetryEvent> {
        let now = self.clock.now();
        let message = message.into();
        let record = self.recovered_record()?;
        let mut next_state = record.state.clone();
        let entry = next_state
            .update_outbox(message_id, |entry| {
                entry.record_definitive_failure(now, message.clone());
            })
            .ok_or_else(|| WorkflowError::OutboxEntryNotFound {
                workflow_id: self.workflow_id.clone(),
                message_id: message_id.clone(),
            })?;
        self.persist_state(record.revision, next_state).await?;
        Ok(WorkflowTelemetryEvent::OutboxDispatchExhausted {
            message_id: entry.message_id().clone(),
            attempts: entry.attempts().attempts(),
            message,
        })
    }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p rakka-workflow`
Expected: PASS, including both new tests.

- [ ] **Step 5: Commit**

```bash
git add crates/rakka-workflow/src/model.rs crates/rakka-workflow/src/inbox.rs crates/rakka-workflow/tests/durable_inbox.rs
git commit -m "Add an exhausting outbox write for a definitive failure

DurableInbox::record_outbox_exhausted records a failed dispatch its
reporter declared definitive and exhausts the entry on that attempt,
whatever budget its retry policy has left, through a new
OutboxEntry::record_definitive_failure transition. It is
record_outbox_failure in every other respect — the same compare-and-set,
the same OutboxEntryNotFound refusal — and answers
OutboxDispatchExhausted.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: `AgentDispatchError::Definitive` exhausts its ticket on the attempt

**Files:**
- Modify: `crates/rakka-agent/src/dispatch.rs` — `collaborator_code` (~line 234), `collaborator_reason` (~244), the `InboxWrite` enum and `operation()` (search `enum InboxWrite`), `write_outbox`'s match (search `InboxWrite::Cancelled { reason } =>`), `record_attempt_failure` (search `async fn record_attempt_failure`), its two call sites (search `.record_attempt_failure(`), `AgentDispatchError` and its `impl` and `Display` (search `pub enum AgentDispatchError`)
- Create: `crates/rakka-agent/tests/dispatch_failure_outcomes.rs`

**Interfaces:**
- Consumes: `DurableInbox::record_outbox_exhausted` (Task 1), reached through `self.write_outbox(..)`.
- Produces: `AgentDispatchError::Definitive { code: String, message: String }` and `pub fn definitive(code: impl Into<String>, message: impl Into<String>) -> Self` — Task 4 calls `AgentDispatchError::definitive`. `record_attempt_failure` gains a trailing `definitive: bool` parameter before `pass`. The counter `rakka.agent.dispatch.write_retries` gains the `operation` value `exhausted`.

- [ ] **Step 1: Write the failing tests**

Create `crates/rakka-agent/tests/dispatch_failure_outcomes.rs`:

```rust
//! How the dispatcher settles an attempt whose executor answered a
//! definitive refusal or an unknown outcome (issue #85, asks 7 and 14).
//!
//! A scripted executor answers every call with one error and counts the
//! calls; the run, the outbox row, and the call count are the evidence.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use rakka_agent::testkit::DeterministicModelAdapter;
use rakka_agent::{
    AgentDispatchError, AgentDispatchFuture, AgentDispatchToolExecutor, AgentEffectSpec,
    AgentFailureReason, AgentModelTurn, AgentRunEffect, AgentRunEffectStatus, AgentRunScope,
    AgentTaskContent, AgentToolAuthority, AgentToolCallId, AgentToolCallRequest, AgentToolId,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::substrate::OutboxStatus;
use rakka_agent_workflow::AgentEphemeralCredential;

mod common;

use common::*;

const TOOL: &str = "charge-card";

fn tool_then_proposal() -> DeterministicModelAdapter {
    DeterministicModelAdapter::new()
        .with_turn_for(
            1,
            AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION).with_tool_call(
                AgentToolCallRequest::new(
                    AgentToolCallId::new("call-1").expect("the call id is valid"),
                    AgentToolId::new(TOOL).expect("the tool id is valid"),
                    serde_json::json!({ "amount": 42 }),
                )
                .expect("the tool call is bounded"),
            ),
        )
        .with_turn_for(
            2,
            AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION).with_proposal(
                AgentTaskContent::inline(serde_json::json!({ "answer": "charged" }))
                    .expect("the proposal is inline-bounded"),
            ),
        )
}

/// A tool executor that answers every call with one scripted error and
/// counts the calls.
struct FailingExecutor {
    error: fn() -> AgentDispatchError,
    calls: Arc<AtomicUsize>,
}

impl AgentDispatchToolExecutor for FailingExecutor {
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _intent: &'a AgentRunEffect,
        _call: &'a AgentToolCallRequest,
        _credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentDispatchFuture<'a, AgentTaskContent> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let error = (self.error)();
        Box::pin(async move { Err(error) })
    }
}

/// A run whose tool ticket, under `spec`, is ready to claim, behind an
/// executor that answers `error`.
fn fixture(spec: &AgentEffectSpec, error: fn() -> AgentDispatchError) -> (AuthorityFixture, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let fx = AuthorityFixture::new(
        tool_then_proposal(),
        AgentToolAuthority::new(tool_registry_for_spec(TOOL, spec)),
        None,
    )
    .with_tool_executor(Arc::new(FailingExecutor {
        error,
        calls: calls.clone(),
    }));
    (fx, calls)
}

async fn ready(fx: &AuthorityFixture) -> AgentRunEffect {
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    fx.effect_at(1).await.expect("the tool effect is committed")
}

fn definitive() -> AgentDispatchError {
    AgentDispatchError::definitive("egress-denied", "the host refused the destination")
}

fn retryable() -> AgentDispatchError {
    AgentDispatchError::collaborator("egress-denied", "the host refused the destination")
}

#[tokio::test]
async fn a_definitive_refusal_exhausts_the_ticket_on_its_first_attempt() {
    let spec = AgentEffectSpec::idempotent(3).expect("the spec is valid");
    let (fx, calls) = fixture(&spec, definitive);
    let effect = ready(&fx).await;

    let pass = fx.one_pass().await;

    assert_eq!((pass.invoked, pass.failed_attempts), (1, 1), "{pass:?}");
    let row = fx.outbox_row(&effect).await.expect("the ticket's row");
    assert_eq!(
        row.status(),
        OutboxStatus::Exhausted,
        "a definitive refusal spends the budget at once"
    );
    assert_eq!(row.attempts().attempts(), 1);
    let resolved = fx
        .effect_at(1)
        .await
        .expect("a failed run keeps its failed effect");
    assert_eq!(resolved.status, AgentRunEffectStatus::Exhausted);
    assert_eq!(
        resolved.last_error_code.as_deref(),
        Some("dispatch-collaborator-failed"),
        "the pipeline code a spent retryable refusal records today"
    );
    assert_eq!(
        resolved
            .last_error_reason
            .as_ref()
            .map(AgentFailureReason::code),
        Some("egress-denied")
    );

    fx.expire_lease();
    fx.pump().await;
    assert_eq!(calls.load(Ordering::SeqCst), 1, "nothing invokes it again");
}

#[tokio::test]
async fn a_retryable_refusal_still_spends_its_budget_one_attempt_at_a_time() {
    let spec = AgentEffectSpec::idempotent(3).expect("the spec is valid");
    let (fx, calls) = fixture(&spec, retryable);
    let effect = ready(&fx).await;

    fx.one_pass().await;

    let row = fx.outbox_row(&effect).await.expect("the ticket's row");
    assert_eq!(row.status(), OutboxStatus::Failed, "a retry is scheduled");
    assert_eq!(row.attempts().attempts(), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p rakka-agent --test dispatch_failure_outcomes`
Expected: FAIL to compile with `no function or associated item named definitive found for enum AgentDispatchError`.

- [ ] **Step 3: Add the variant**

In `crates/rakka-agent/src/dispatch.rs`, in `pub enum AgentDispatchError`, after the `Collaborator { .. }` variant:

```rust
    /// A collaborator refused, and retrying cannot change its answer: the
    /// attempt's ticket is exhausted on this attempt, whatever budget the
    /// effect has left.
    ///
    /// The collaborator's own code rides the failure detail and the
    /// failure's reason exactly as a [`Self::Collaborator`] code does, and
    /// [`Self::code`] is the same pipeline code, so a definitive refusal ends
    /// in the state a retryable one reaches once its budget is spent — only
    /// sooner.
    Definitive {
        /// Stable machine-readable code: the collaborator's own.
        code: String,
        /// The failure detail. It becomes durable state: no credential,
        /// argument, or content material.
        message: String,
    },
```

In `impl AgentDispatchError`, in `code()`, add the arm beside `Collaborator`:

```rust
            Self::Collaborator { .. } | Self::Definitive { .. } => "dispatch-collaborator-failed",
```

(replacing the existing `Self::Collaborator { .. } => "dispatch-collaborator-failed",` arm), and after `pub fn collaborator(..)`:

```rust
    /// A collaborator's definitive refusal: one retrying cannot change.
    #[must_use]
    pub fn definitive(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Definitive {
            code: code.into(),
            message: message.into(),
        }
    }
```

In `impl Display for AgentDispatchError`, after the `Collaborator` arm:

```rust
            Self::Definitive { code, message } => {
                write!(f, "a dispatch collaborator refused definitively ({code}): {message}")
            }
```

(`impl Error`'s `source` ends in `_ => None` and needs no change.)

Change `collaborator_code` and `collaborator_reason` (near the top of the file) to treat both variants alike:

```rust
fn collaborator_code(error: &AgentDispatchError) -> String {
    match error {
        AgentDispatchError::Collaborator { code, .. }
        | AgentDispatchError::Definitive { code, .. } => bounded_failure_code(code),
        other => other.code().to_string(),
    }
}
```

```rust
fn collaborator_reason(error: &AgentDispatchError) -> Option<crate::failure::AgentFailureReason> {
    match error {
        AgentDispatchError::Collaborator { .. } | AgentDispatchError::Definitive { .. } => {
            crate::failure::AgentFailureReason::new(collaborator_code(error))
        }
        _ => None,
    }
}
```

- [ ] **Step 4: Add the exhausting inbox write**

In `enum InboxWrite<'a>`, after the `Failed { .. }` variant:

```rust
    /// A definitive failure's record, exhausting the entry on this attempt.
    Exhausted {
        /// The bounded failure line the row keeps.
        line: &'a str,
    },
```

In `InboxWrite::operation`, add `Self::Exhausted { .. } => "exhausted",`.

In `write_outbox`'s `match &write`, after the `InboxWrite::Failed { line, timed_out } =>` arm:

```rust
                InboxWrite::Exhausted { line } => inbox
                    .inner_mut()
                    .record_outbox_exhausted(message_id, *line)
                    .await
                    .map(Some),
```

- [ ] **Step 5: Honor `definitive` in `record_attempt_failure`**

Add a parameter `definitive: bool,` immediately before `pass: &mut AgentDispatchPass,` in `record_attempt_failure`'s signature, extend its doc comment with:

```rust
    ///
    /// A `definitive` failure exhausts the entry on this attempt, whatever
    /// budget remains; it then follows the exhausted branch below exactly as
    /// a spent budget does.
```

and replace its write:

```rust
        let event = self
            .write_outbox(
                scope,
                &message_id,
                InboxWrite::Failed {
                    line: &line,
                    timed_out: false,
                },
            )
            .await?
            .expect("a failure record answers its event");
```

with:

```rust
        let write = if definitive {
            InboxWrite::Exhausted { line: &line }
        } else {
            InboxWrite::Failed {
                line: &line,
                timed_out: false,
            }
        };
        let event = self
            .write_outbox(scope, &message_id, write)
            .await?
            .expect("a failure record answers its event");
```

At the credential-resolution call site (the one passing `"credential-resolution-failed"`), pass `false,` before `pass`. At the invocation call site in `attempt_invocation` (the one passing `collaborator_reason(&error)`), pass:

```rust
                        collaborator_reason(&error),
                        matches!(error, AgentDispatchError::Definitive { .. }),
                        pass,
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cargo test -p rakka-agent --test dispatch_failure_outcomes`
Expected: PASS (2 tests).
Run: `cargo test -p rakka-agent --test dispatch_inbox_write_retry --test mcp_client_dispatch --test effect_dispatch`
Expected: PASS — in particular `an_egress_refusal_fails_the_attempt_under_the_deployments_code_after_the_credential_was_resolved_and_dropped` is unchanged by this task.

- [ ] **Step 7: Commit**

```bash
git add crates/rakka-agent/src/dispatch.rs crates/rakka-agent/tests/dispatch_failure_outcomes.rs
git commit -m "Exhaust a ticket on the attempt a collaborator refuses definitively

AgentDispatchError::Definitive (built with AgentDispatchError::definitive)
is a collaborator's refusal that retrying cannot change. Its code() is
the pipeline's dispatch-collaborator-failed and its own code rides the
detail and the failure reason exactly as a Collaborator code does, so it
ends in the state a spent retryable refusal reaches — only on the first
attempt. record_attempt_failure takes a definitive flag, and a definitive
failure is recorded through InboxWrite::Exhausted, the substrate's new
record_outbox_exhausted, then follows the existing exhausted branch:
the fleet entry is marked Exhausted and the run receives Exhausted with
the collaborator's code as its reason. The write_retries counter gains
the operation value exhausted.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: `AgentDispatchError::OutcomeUnknown` follows specification 11.5's table

**Files:**
- Modify: `crates/rakka-agent/src/dispatch.rs` — `AgentDispatchError` (variant, constructor, `code()`, `Display`), `attempt_invocation`'s error arm (search `let outcome = match invoked {`), a new `settle_unknown_outcome` placed directly after `recover_ambiguous`
- Modify: `crates/rakka-agent/tests/common/mod.rs` — `AuthorityFixture` (a `reconciler` field, `with_reconciler`, and the pipeline wiring)
- Test: `crates/rakka-agent/tests/dispatch_failure_outcomes.rs`

**Interfaces:**
- Consumes: `record_attempt_failure(.., definitive: bool, pass)` (Task 2), `reconcile_ambiguous`, `park_indeterminate` (existing).
- Produces: `AgentDispatchError::OutcomeUnknown { code: &'static str, message: String }` and `pub fn outcome_unknown(code: &'static str, message: impl Into<String>) -> Self` — Task 5 calls it. `AuthorityFixture::with_reconciler(self, Arc<dyn AgentEffectReconciler>) -> Self`.

- [ ] **Step 1: Add the fixture's reconciler hook**

In `crates/rakka-agent/tests/common/mod.rs`, add a field to `pub struct AuthorityFixture` after `dispatch_segments`:

```rust
    /// The reconciler the pipeline asks about an ambiguous attempt, when a
    /// test wires one — see [`AuthorityFixture::with_reconciler`].
    pub reconciler: Option<Arc<dyn rakka_agent::AgentEffectReconciler>>,
```

initialize it in the struct literal (search `dispatch_segments: None,`) with `reconciler: None,`, add the builder beside `with_dispatch_segments`:

```rust
    /// Wires the reconciler the dispatch pipeline queries for a
    /// `Reconcileable` effect whose outcome is unknown.
    pub fn with_reconciler(mut self, reconciler: Arc<dyn rakka_agent::AgentEffectReconciler>) -> Self {
        self.reconciler = Some(reconciler);
        self
    }
```

and in the pipeline builder, after the `dispatch_segments` wiring:

```rust
        if let Some(reconciler) = &self.reconciler {
            pipeline = pipeline.with_reconciler(reconciler.clone());
        }
```

- [ ] **Step 2: Write the failing tests**

Append to `crates/rakka-agent/tests/dispatch_failure_outcomes.rs` (and add `AgentReconciliationProtocolRef, AgentRunStatus` to the `rakka_agent::{..}` import and `use rakka_agent::testkit::ScriptedReconciler;`):

```rust
fn unknown() -> AgentDispatchError {
    AgentDispatchError::outcome_unknown(
        "scripted-outcome-unknown",
        "the call was sent and no answer was read",
    )
}

#[tokio::test]
async fn an_unknown_outcome_parks_a_non_idempotent_effect_for_reconciliation() {
    let (fx, calls) = fixture(&AgentEffectSpec::non_idempotent(), unknown);
    ready(&fx).await;

    let pass = fx.one_pass().await;

    assert_eq!(pass.parked_indeterminate, 1, "{pass:?}");
    let parked = fx.effect_at(1).await.expect("the parked effect");
    assert_eq!(parked.status, AgentRunEffectStatus::Indeterminate);
    assert_eq!(
        parked.last_error_code.as_deref(),
        Some("scripted-outcome-unknown"),
        "the executor's code, not the worker-loss code"
    );
    assert_eq!(
        fx.fx.run_snapshot().await.expect("the run").status,
        AgentRunStatus::WaitingForReconciliation
    );

    fx.expire_lease();
    fx.pump().await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "a non-idempotent target is never invoked again"
    );
}

#[tokio::test]
async fn an_unknown_outcome_retries_a_read_only_or_idempotent_effect_under_its_budget() {
    for spec in [
        AgentEffectSpec::read_only()
            .with_max_attempts(3)
            .expect("the spec is valid"),
        AgentEffectSpec::idempotent(3).expect("the spec is valid"),
    ] {
        let class = spec.safety_class;
        let (fx, calls) = fixture(&spec, unknown);
        let effect = ready(&fx).await;

        let pass = fx.one_pass().await;

        assert_eq!(pass.failed_attempts, 1, "{class:?}: {pass:?}");
        let row = fx.outbox_row(&effect).await.expect("the ticket's row");
        assert_eq!(row.status(), OutboxStatus::Failed, "{class:?}: retry scheduled");
        assert!(
            row.attempts()
                .last_error()
                .is_some_and(|line| line.starts_with("scripted-outcome-unknown")),
            "{class:?}: the executor's code leads the line: {:?}",
            row.attempts().last_error()
        );
        fx.expire_lease();
        fx.one_pass().await;
        assert_eq!(calls.load(Ordering::SeqCst), 2, "{class:?}: retried");
    }
}

#[tokio::test]
async fn an_unknown_outcome_on_an_idempotent_effects_last_attempt_exhausts_it() {
    let spec = AgentEffectSpec::idempotent(1).expect("the spec is valid");
    let (fx, _calls) = fixture(&spec, unknown);
    ready(&fx).await;

    fx.one_pass().await;

    let resolved = fx.effect_at(1).await.expect("the failed effect");
    assert_eq!(resolved.status, AgentRunEffectStatus::Exhausted);
    assert_eq!(
        resolved.last_error_code.as_deref(),
        Some("scripted-outcome-unknown")
    );
}

#[tokio::test]
async fn an_unknown_outcome_asks_the_reconciler_of_a_reconcileable_effect_in_the_same_pass() {
    let protocol = AgentReconciliationProtocolRef::new("payment-ledger").expect("the ref is valid");
    let spec = AgentEffectSpec::reconcileable(protocol, 3).expect("the spec is valid");
    let reconciler = ScriptedReconciler::new();
    let (fx, calls) = fixture(&spec, unknown);
    let fx = fx.with_reconciler(Arc::new(reconciler.clone()));
    ready(&fx).await;

    fx.one_pass().await;

    assert_eq!(reconciler.queries(), 1, "queried in the pass that saw the unknown outcome");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cargo test -p rakka-agent --test dispatch_failure_outcomes`
Expected: FAIL to compile with `no function or associated item named outcome_unknown found`.

- [ ] **Step 4: Add the variant**

In `pub enum AgentDispatchError`, after `Definitive { .. }`:

```rust
    /// The attempt may have invoked the target and its outcome is unknown:
    /// the request left and no answer was read. The dispatcher settles it by
    /// the intent's safety class, as it settles an attempt whose worker died
    /// after `Started` ([specification 11.5]).
    ///
    /// [specification 11.5]: ../../../docs/plans/rakka-agent/spec.md
    OutcomeUnknown {
        /// Stable machine-readable code: the executor's own.
        code: &'static str,
        /// The failure detail. It becomes durable state: no credential,
        /// argument, or content material.
        message: String,
    },
```

In `code()`: `Self::OutcomeUnknown { code, .. } => code,`. After `definitive(..)`:

```rust
    /// An attempt that may have invoked the target, whose outcome is unknown.
    #[must_use]
    pub fn outcome_unknown(code: &'static str, message: impl Into<String>) -> Self {
        Self::OutcomeUnknown {
            code,
            message: message.into(),
        }
    }
```

In `Display`:

```rust
            Self::OutcomeUnknown { code, message } => {
                write!(f, "the invocation's outcome is unknown ({code}): {message}")
            }
```

- [ ] **Step 5: Route it through the table**

Directly after `recover_ambiguous`, add:

```rust
    /// Settles an attempt whose executor reported that the target may have
    /// acted and the outcome is unknown, by the intent's safety class
    /// ([specification 11.5](../../../docs/plans/rakka-agent/spec.md)): the
    /// table [`Self::recover_ambiguous`] applies to an attempt whose worker
    /// died after `Started`, with the executor's code in place of the
    /// worker-loss code.
    ///
    /// A read-only or idempotent effect records a failed attempt and is
    /// retried under its budget with the same idempotency key, exactly as an
    /// invocation failure is; a reconcileable one asks the reconciler now; a
    /// non-idempotent one parks for reconciliation and is never re-invoked.
    /// This runs only from a fresh attempt, which the wind-down fence has
    /// already admitted, so the worker-loss path's wind-down arm does not
    /// apply.
    #[allow(clippy::too_many_arguments)]
    async fn settle_unknown_outcome(
        &mut self,
        scope: &AgentRunScope,
        claim: AgentDispatchClaim,
        state: &AgentRunState,
        intent: &AgentRunEffect,
        attempt: u32,
        error: &AgentDispatchError,
        pass: &mut AgentDispatchPass,
    ) -> AgentDispatchResult<ClaimConclusion> {
        let AgentDispatchError::OutcomeUnknown { code, message } = error else {
            unreachable!("only an unknown outcome is settled here");
        };
        match intent.safety.class() {
            AgentEffectSafetyClass::ReadOnly | AgentEffectSafetyClass::Idempotent => {
                self.record_attempt_failure(
                    scope,
                    &claim,
                    intent,
                    attempt,
                    code,
                    &error.to_string(),
                    None,
                    false,
                    pass,
                )
                .await
            }
            AgentEffectSafetyClass::Reconcileable => {
                self.reconcile_ambiguous(scope, claim, state, intent, attempt, false, pass)
                    .await
            }
            AgentEffectSafetyClass::NonIdempotent => {
                self.park_indeterminate(scope, claim, intent, attempt, code, message, pass)
                    .await
            }
        }
    }
```

In `attempt_invocation`, replace:

```rust
        let outcome = match invoked {
            Ok(outcome) => outcome,
            Err(error) => {
```

with:

```rust
        let outcome = match invoked {
            Ok(outcome) => outcome,
            Err(error @ AgentDispatchError::OutcomeUnknown { .. }) => {
                return self
                    .settle_unknown_outcome(scope, claim, state, intent, attempt, &error, pass)
                    .await;
            }
            Err(error) => {
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cargo test -p rakka-agent --test dispatch_failure_outcomes`
Expected: PASS (6 tests).
Run: `cargo test -p rakka-agent --test effect_dispatch --test mcp_client_dispatch`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/rakka-agent/src/dispatch.rs crates/rakka-agent/tests/common/mod.rs crates/rakka-agent/tests/dispatch_failure_outcomes.rs
git commit -m "Settle an executor-reported unknown outcome by its safety class

AgentDispatchError::OutcomeUnknown (built with outcome_unknown) is an
attempt that may have invoked the target and whose outcome is unknown.
attempt_invocation hands it to settle_unknown_outcome, which applies
specification 11.5's table exactly as recover_ambiguous does for a
worker lost after Started, with the executor's code: a read-only or
idempotent effect records a failed attempt and is retried under its
budget, a reconcileable one asks the reconciler in the same pass, and a
non-idempotent one parks for reconciliation and is never re-invoked.
The authority fixture gains with_reconciler.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: Definitive egress and artifact refusals in the MCP executor

**Files:**
- Modify: `crates/rakka-agent-workflow/src/artifacts.rs` — `AgentArtifactError` (variant, `#[non_exhaustive]`, `code()`, `Display`)
- Modify: `crates/rakka-agent-mcp/src/executor.rs` — `dispatch_error` (~line 1057), the `refused` closure in result mapping (search `let refused = |error: rakka_agent_workflow::AgentArtifactError|`)
- Test: `crates/rakka-agent-mcp/tests/client_dispatch.rs`, `crates/rakka-agent-mcp/tests/artifact_sink.rs`, `crates/rakka-agent/tests/mcp_client_dispatch.rs`

**Interfaces:**
- Consumes: `AgentDispatchError::definitive` (Task 2).
- Produces: `AgentArtifactError::Refused { message: String }` with `code()` `artifact-store-refused`.

- [ ] **Step 1: Write the failing executor-level tests**

In `crates/rakka-agent-mcp/tests/client_dispatch.rs`, add `AgentDispatchError` to the `rakka_agent::{..}` import, and append:

```rust
struct RefuseTransiently;

impl McpEgressCheck for RefuseTransiently {
    fn check(&self, server: &McpServerId, _url: &str) -> Result<(), AgentAuthorityRefusal> {
        Err(AgentAuthorityRefusal::transient(
            "egress-temporarily-denied",
            format!("{server} is not reachable right now"),
        ))
    }
}

#[tokio::test]
async fn an_egress_refusal_the_host_marks_definitive_is_definitive() {
    let endpoint = serve_fake(fake()).await;
    let (executor, _, _) = executor_over(binding(&endpoint.url), Arc::new(RefuseAll)).await;
    let error = executor
        .execute(
            &run_scope(),
            &tool_intent_with_timeout("mcp.crm.echo", Some(5_000)),
            &call("echo", json!({})),
            None,
        )
        .await
        .expect_err("refused");
    assert!(
        matches!(&error, AgentDispatchError::Definitive { code, .. } if code == "egress-denied-by-policy"),
        "{error:?}"
    );
}

#[tokio::test]
async fn an_egress_refusal_the_host_marks_retryable_stays_retryable() {
    let endpoint = serve_fake(fake()).await;
    let (executor, _, _) =
        executor_over(binding(&endpoint.url), Arc::new(RefuseTransiently)).await;
    let error = executor
        .execute(
            &run_scope(),
            &tool_intent_with_timeout("mcp.crm.echo", Some(5_000)),
            &call("echo", json!({})),
            None,
        )
        .await
        .expect_err("refused");
    assert!(
        matches!(&error, AgentDispatchError::Collaborator { code, .. } if code == "egress-temporarily-denied"),
        "{error:?}"
    );
}
```

(`executor_over`, `binding`, `call`, `fake`, and `RefuseAll` already exist in that file; `AgentAuthorityRefusal::of` — what `RefuseAll` uses — is `retryable: false`, `AgentAuthorityRefusal::transient` is `retryable: true`.)

In `crates/rakka-agent-mcp/tests/artifact_sink.rs`, add a `SinkMode::Refusing` variant:

```rust
    /// The store refuses the write and always will.
    Refusing,
```

handle it in `RecordingSink::put_result` beside the `Failing` branch:

```rust
            if self.mode == SinkMode::Refusing {
                return Err(AgentArtifactError::Refused {
                    message: "the result is larger than this store accepts".to_string(),
                });
            }
```

and append:

```rust
#[tokio::test]
async fn a_sink_that_refuses_the_write_fails_the_attempt_definitively() {
    let endpoint = serve_fake(fake()).await;
    let executor = executor_over(
        &endpoint.url,
        McpArtifacts::sink(RecordingSink::new(SinkMode::Refusing)),
    )
    .await;
    let error = executor
        .execute(
            &run_scope(),
            &tool_intent_with_timeout("mcp.crm.big", Some(5_000)),
            &call("big"),
            None,
        )
        .await
        .expect_err("the store refuses");
    assert!(
        matches!(&error, AgentDispatchError::Definitive { code, .. } if code == "artifact-store-refused"),
        "{error:?}"
    );
}

#[tokio::test]
async fn a_sink_that_is_down_fails_the_attempt_retryably() {
    let endpoint = serve_fake(fake()).await;
    let executor = executor_over(
        &endpoint.url,
        McpArtifacts::sink(RecordingSink::new(SinkMode::Failing)),
    )
    .await;
    let error = executor
        .execute(
            &run_scope(),
            &tool_intent_with_timeout("mcp.crm.big", Some(5_000)),
            &call("big"),
            None,
        )
        .await
        .expect_err("the store is down");
    assert!(
        matches!(&error, AgentDispatchError::Collaborator { code, .. } if code == "artifact-store"),
        "{error:?}"
    );
}
```

(add `AgentDispatchError` to that file's `rakka_agent` import).

- [ ] **Step 2: Write the failing end-to-end tests**

In `crates/rakka-agent/tests/mcp_client_dispatch.rs`:

1. Split `McpWorld::assemble` so the artifact path can be chosen: rename the existing body to `assemble_with_artifacts(fake, binding_of, egress, sync_credential, artifacts: impl Into<McpArtifacts>)`, replacing its `mcp_artifact_store(artifacts.clone())` argument with `artifacts` and keeping `artifacts: SharedArtifactStore::default()` in the returned struct, and make `assemble` call it with `mcp_artifact_store(SharedArtifactStore::default())`. Import `McpArtifacts, McpArtifactSink, McpArtifactFuture` from `rakka_agent_mcp` and `AgentArtifactError, AgentArtifactWriteRequest` from `rakka_agent_workflow`.

2. Append:

```rust
/// A deployment's artifact store that refuses every write and always will.
struct RefusingSink;

impl McpArtifactSink for RefusingSink {
    fn put_result<'a>(
        &'a self,
        _scope: &'a rakka_agent::AgentRunScope,
        _request: AgentArtifactWriteRequest,
    ) -> McpArtifactFuture<'a> {
        Box::pin(async move {
            Err(AgentArtifactError::Refused {
                message: "the result is larger than this store accepts".to_string(),
            })
        })
    }
}

#[tokio::test]
async fn a_definitive_egress_refusal_exhausts_a_multi_attempt_budget_at_once() {
    // An idempotent declaration carries two attempts in this suite, so a
    // retryable refusal would be tried twice.
    let world = McpWorld::build(AgentEffectSafetyClass::Idempotent, Arc::new(RefuseAll)).await;
    let fx = world.fixture(ECHO, json!({ "q": "refunds" }));
    let committed = start_and_commit_the_tool_call(&fx).await;
    assert_eq!(committed.max_attempts, 2);

    fx.one_pass().await;

    let row = fx.outbox_row(&committed).await.expect("the ticket's row");
    assert_eq!(row.status(), OutboxStatus::Exhausted);
    assert_eq!(row.attempts().attempts(), 1, "one attempt, not two");
    assert_eq!(world.endpoint.server.call_count(), 0, "no connection");
}

#[tokio::test]
async fn a_refused_artifact_write_invokes_the_remote_tool_exactly_once() {
    let world = McpWorld::assemble_with_artifacts(
        fake_server(),
        |url| server_binding(url, AgentEffectSafetyClass::Idempotent),
        Arc::new(McpAllowAllEgress),
        None,
        McpArtifacts::sink(RefusingSink),
    )
    .await;
    let fx = world.fixture(BIG, json!({}));
    let committed = start_and_commit_the_tool_call(&fx).await;

    fx.one_pass().await;
    fx.expire_lease();
    fx.pump().await;

    assert_eq!(
        world.endpoint.server.call_count(),
        1,
        "the store's refusal is definitive: the tool is not invoked again"
    );
    let row = fx.outbox_row(&committed).await.expect("the ticket's row");
    assert_eq!(row.status(), OutboxStatus::Exhausted);
    assert_eq!(row.attempts().attempts(), 1);
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cargo test -p rakka-agent-mcp --features testkit --test client_dispatch --test artifact_sink`
Expected: FAIL to compile — `no variant named Refused found for enum AgentArtifactError`.

- [ ] **Step 4: Add `AgentArtifactError::Refused`**

In `crates/rakka-agent-workflow/src/artifacts.rs`, add `#[non_exhaustive]` above `pub enum AgentArtifactError`, then after `Store { .. }`:

```rust
    /// The application-owned store refused the write and will refuse it
    /// again: retrying cannot help. A store that may accept the same write
    /// later answers [`Self::Store`].
    Refused {
        /// Stable bounded refusal message.
        message: String,
    },
```

In `code()`: `Self::Refused { .. } => "artifact-store-refused",`. In `Display`, match the existing `Store` arm's wording:

```rust
            Self::Refused { message } => {
                write!(f, "the artifact store refused the write: {message}")
            }
```

- [ ] **Step 5: Map both refusals in the MCP executor**

In `crates/rakka-agent-mcp/src/executor.rs`, replace `dispatch_error`'s body with:

```rust
fn dispatch_error(error: McpClientError) -> AgentDispatchError {
    match &error {
        McpClientError::Transport { .. } | McpClientError::Protocol { .. } => {
            AgentDispatchError::Invocation {
                code: "mcp-transport-failed",
                message: error.to_string(),
            }
        }
        // The host declared the refusing condition permanent: retrying the
        // attempt would only ask it again.
        McpClientError::Egress {
            retryable: false, ..
        } => AgentDispatchError::definitive(error.code().to_string(), error.to_string()),
        other => AgentDispatchError::collaborator(other.code().to_string(), error.to_string()),
    }
}
```

and extend its doc comment with: "An egress refusal the host declared not retryable is a definitive refusal, which exhausts the attempt's ticket at once."

Replace the `refused` closure:

```rust
                let refused = |error: rakka_agent_workflow::AgentArtifactError| {
                    AgentDispatchError::collaborator(error.code(), error.to_string())
                };
```

with:

```rust
                // A store that refused the write will refuse it again, and a
                // retry would invoke the remote tool again only to lose its
                // result the same way: that refusal is definitive.
                let refused = |error: rakka_agent_workflow::AgentArtifactError| match error {
                    rakka_agent_workflow::AgentArtifactError::Refused { .. } => {
                        AgentDispatchError::definitive(error.code(), error.to_string())
                    }
                    other => AgentDispatchError::collaborator(other.code(), other.to_string()),
                };
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cargo test -p rakka-agent-workflow`
Run: `cargo test -p rakka-agent-mcp --features testkit`
Run: `cargo test -p rakka-agent --test mcp_client_dispatch`
Expected: PASS, including `an_egress_refusal_fails_before_any_client_exists_and_never_touches_the_credential` (its refusal is now `Definitive`, and its `Display` still contains `egress-denied-by-policy`).

- [ ] **Step 7: Commit**

```bash
git add crates/rakka-agent-workflow/src/artifacts.rs crates/rakka-agent-mcp/src/executor.rs crates/rakka-agent-mcp/tests/client_dispatch.rs crates/rakka-agent-mcp/tests/artifact_sink.rs crates/rakka-agent/tests/mcp_client_dispatch.rs
git commit -m "Make a non-retryable egress refusal and a refused artifact write definitive

dispatch_error maps an McpClientError::Egress the host declared
retryable: false to AgentDispatchError::Definitive under the host's own
code, so its ticket exhausts on the first attempt instead of after
maxAttempts; a retryable one stays a Collaborator. AgentArtifactError
gains Refused (artifact-store-refused) for a store that will never
accept a write, and the executor maps it to Definitive, so a too-large
result no longer has the remote tool invoked again on every attempt.
AgentArtifactError becomes non_exhaustive in the same change: the new
variant already breaks an exhaustive match outside the crate, and this
makes it the last such break.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: The MCP call's outcome is unknown once `tools/call` was sent

**Files:**
- Modify: `crates/rakka-agent-mcp/src/testkit.rs` — `FakeState` (a `call_transport_failure: bool`), `FakeMcpServer::with_call_transport_failure`, the `capture_headers` middleware, `FakeToolBehaviour::RpcError`, `call_tool`
- Modify: `crates/rakka-agent-mcp/src/executor.rs` — the attempt in `execute` (search `let session = within(deadline, self.open(`), `call` (remove its recheck), `within_after_send`, `sent_call_error`, the `call_tool_once` error mapping
- Test: `crates/rakka-agent-mcp/tests/client_dispatch.rs`, `crates/rakka-agent-mcp/tests/artifact_sink.rs`, `crates/rakka-agent/tests/mcp_client_dispatch.rs`

**Interfaces:**
- Consumes: `AgentDispatchError::outcome_unknown` (Task 3).
- Produces: the stable code `mcp-outcome-unknown`; `FakeMcpServer::with_call_transport_failure(self) -> Self`; `FakeToolBehaviour::RpcError(String)`.

- [ ] **Step 1: Add the testkit's failure modes**

In `crates/rakka-agent-mcp/src/testkit.rs`:

Add to `FakeToolBehaviour` (it is `#[non_exhaustive]`):

```rust
    /// Answers `tools/call` with a JSON-RPC error (internal error) carrying
    /// this text, rather than with a result: the server answered, and what it
    /// answered was an error.
    RpcError(String),
```

Add to `struct FakeState` the field `call_transport_failure: bool,` (initialized `false` in `FakeMcpServer::new`), and the builder after `with_list_delay`:

```rust
    /// Makes the transport fail on every `tools/call` after the tool has run:
    /// the handler runs the call (it is counted in [`Self::seen_calls`] and
    /// [`Self::call_count`]), and the HTTP response is replaced with
    /// `502 Bad Gateway` — the shape of a connection or proxy that died after
    /// the request arrived, leaving the client no answer to read.
    #[must_use]
    pub fn with_call_transport_failure(self) -> Self {
        self.lock().call_transport_failure = true;
        self
    }
```

In `capture_headers`, replace `next.run(request).await` with:

```rust
    let calls_before = server.call_count();
    let response = next.run(request).await;
    if server.lock().call_transport_failure && server.call_count() > calls_before {
        return axum::http::StatusCode::BAD_GATEWAY.into_response();
    }
    response
```

(add `use axum::response::IntoResponse;`; `lock()` is a private method of `FakeMcpServer` in the same module, so the middleware can call it).

In `call_tool`, after `let Some(behaviour) = behaviour else { .. };`, before `Ok(answer(behaviour, arguments).await)`:

```rust
        if let FakeToolBehaviour::RpcError(text) = &behaviour {
            return Err(McpError::internal_error(text.clone(), None));
        }
```

and give `answer`'s match an arm for the variant so it stays exhaustive: `FakeToolBehaviour::RpcError(_) => unreachable!("answered as a JSON-RPC error by call_tool"),`.

- [ ] **Step 2: Write the failing executor-level tests**

In `crates/rakka-agent-mcp/tests/client_dispatch.rs`, change the end of `a_timeout_from_the_intent_bounds_the_call` and rename it:

```rust
#[tokio::test]
async fn a_timeout_after_the_call_was_sent_leaves_the_outcome_unknown() {
```

replacing its final assertion with:

```rust
    assert!(
        matches!(&error, AgentDispatchError::OutcomeUnknown { code: "mcp-outcome-unknown", .. }),
        "the call was sent and no answer was read: {error:?}"
    );
    assert_eq!(endpoint.server.call_count(), 1, "the server received the call");
```

Append:

```rust
#[tokio::test]
async fn a_transport_failure_after_the_call_was_sent_leaves_the_outcome_unknown() {
    let endpoint = serve_fake(fake().with_call_transport_failure()).await;
    let (executor, _, _) = executor_over(binding(&endpoint.url), Arc::new(McpAllowAllEgress)).await;
    let error = executor
        .execute(
            &run_scope(),
            &tool_intent_with_timeout("mcp.crm.echo", Some(5_000)),
            &call("echo", json!({})),
            None,
        )
        .await
        .expect_err("the answer was lost");
    assert!(
        matches!(&error, AgentDispatchError::OutcomeUnknown { code: "mcp-outcome-unknown", .. }),
        "{error:?}"
    );
    assert_eq!(endpoint.server.call_count(), 1, "the tool ran");
}

#[tokio::test]
async fn a_json_rpc_error_answer_is_a_known_outcome() {
    let endpoint = serve_fake(fake().with_tool(FakeTool::new(
        "broken",
        "Answers with a JSON-RPC error.",
        json!({"type":"object"}),
        FakeToolBehaviour::RpcError("the tool backend is down".to_string()),
    )))
    .await;
    let binding = McpServerBinding::streamable_http(server_id(), &endpoint.url)
        .with_tool(
            "broken",
            policy(
                AgentEffectSafetyClass::NonIdempotent,
                AgentToolResultBehavior::InlineBounded,
            ),
        )
        .expect("t");
    let (executor, _, _) = executor_over(binding, Arc::new(McpAllowAllEgress)).await;
    let error = executor
        .execute(
            &run_scope(),
            &tool_intent_with_timeout("mcp.crm.broken", Some(5_000)),
            &call("broken", json!({})),
            None,
        )
        .await
        .expect_err("the server answered with an error");
    assert!(
        matches!(&error, AgentDispatchError::Invocation { code: "mcp-transport-failed", .. }),
        "the server answered, so the outcome is known: {error:?}"
    );
}
```

(`executor_over(binding, egress)` in this file returns `(executor, http, _)`.)

In `crates/rakka-agent-mcp/tests/artifact_sink.rs`, add `SinkMode::Slow` (`/// The store takes longer than any attempt's deadline.`), handled first in `put_result`:

```rust
            if self.mode == SinkMode::Slow {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
```

and append:

```rust
#[tokio::test]
async fn a_deadline_that_elapses_while_the_answer_is_stored_leaves_the_outcome_unknown() {
    let endpoint = serve_fake(fake()).await;
    let executor = executor_over(
        &endpoint.url,
        McpArtifacts::sink(RecordingSink::new(SinkMode::Slow)),
    )
    .await;
    let error = executor
        .execute(
            &run_scope(),
            &tool_intent_with_timeout("mcp.crm.big", Some(300)),
            &call("big"),
            None,
        )
        .await
        .expect_err("the deadline elapsed while the result was being stored");
    assert!(
        matches!(&error, AgentDispatchError::OutcomeUnknown { code: "mcp-outcome-unknown", .. }),
        "the server had answered: {error:?}"
    );
}
```

- [ ] **Step 3: Write the failing end-to-end test**

In `crates/rakka-agent/tests/mcp_client_dispatch.rs`, append:

```rust
#[tokio::test]
async fn a_non_idempotent_call_whose_answer_never_arrives_parks_for_reconciliation() {
    let world = McpWorld::assemble(
        fake_server().with_tool(FakeTool::new(
            "slow",
            "Answers after the attempt's deadline.",
            json!({"type": "object"}),
            FakeToolBehaviour::Sleep { millis: 10_000 },
        )),
        |url| {
            server_binding(url, AgentEffectSafetyClass::NonIdempotent)
                .with_tool(
                    "slow",
                    policy(
                        AgentEffectSafetyClass::NonIdempotent,
                        AgentToolResultBehavior::InlineBounded,
                    )
                    .with_timeout_ms(300),
                )
                .expect("the slow tool binds")
        },
        Arc::new(McpAllowAllEgress),
        None,
    )
    .await;
    let fx = world.fixture("mcp.crm.slow", json!({}));
    start_and_commit_the_tool_call(&fx).await;

    let pass = fx.one_pass().await;

    assert_eq!(pass.parked_indeterminate, 1, "{pass:?}");
    let parked = fx.effect_at(TOOL_SLOT).await.expect("the parked effect");
    assert_eq!(parked.status, AgentRunEffectStatus::Indeterminate);
    assert_eq!(parked.last_error_code.as_deref(), Some("mcp-outcome-unknown"));
    assert_eq!(
        fx.fx.run_snapshot().await.expect("the run").status,
        AgentRunStatus::WaitingForReconciliation
    );
    assert_eq!(world.endpoint.server.call_count(), 1);
}
```

- [ ] **Step 4: Run the tests to verify they fail**

Run: `cargo test -p rakka-agent-mcp --features testkit --test client_dispatch --test artifact_sink`
Expected: the three `OutcomeUnknown` assertions FAIL (today each reads `Invocation { code: "mcp-transport-failed", .. }`); `a_json_rpc_error_answer_is_a_known_outcome` PASSES already (it pins today's behaviour so Step 5 cannot widen it).
Run: `cargo test -p rakka-agent --test mcp_client_dispatch a_non_idempotent_call_whose_answer_never_arrives_parks_for_reconciliation`
Expected: FAIL — `parked_indeterminate` is 0 (the attempt exhausts as `mcp-transport-failed`).

- [ ] **Step 5: Split the deadline at the send**

In `crates/rakka-agent-mcp/src/executor.rs`:

Add `use rmcp::service::ServiceError;` beside the other `rmcp` imports, and beside `within`:

```rust
/// The stable code of an attempt whose `tools/call` was sent and whose
/// answer was never read: the server may have acted.
const MCP_OUTCOME_UNKNOWN: &str = "mcp-outcome-unknown";

/// Runs the part of the attempt from the send of `tools/call` on, under the
/// same deadline as [`within`].
///
/// Past the send the server may have acted, so a deadline that elapses here
/// leaves the outcome unknown rather than failing the attempt.
async fn within_after_send<T>(
    deadline: tokio::time::Instant,
    work: impl Future<Output = T>,
) -> Result<T, AgentDispatchError> {
    tokio::time::timeout_at(deadline, work).await.map_err(|_| {
        AgentDispatchError::outcome_unknown(
            MCP_OUTCOME_UNKNOWN,
            "the attempt exceeded the effect's timeout after tools/call was sent",
        )
    })
}

/// Maps a failed `tools/call` onto the dispatch pipeline's error.
///
/// The request was sent, so a failure that left no answer to read leaves the
/// outcome unknown. A JSON-RPC error is the server's own answer — it told us
/// what it did — and keeps the mapping a failure before the send gets.
fn sent_call_error(server: &str, error: &ServiceError) -> AgentDispatchError {
    let mapped = client::service_error(server, error);
    match error {
        ServiceError::McpError(_) => dispatch_error(mapped),
        _ => AgentDispatchError::outcome_unknown(
            MCP_OUTCOME_UNKNOWN,
            format!("tools/call was sent and no answer was read: {mapped}"),
        ),
    }
}
```

Update `within`'s doc comment: "Runs one stage of the attempt before `tools/call` is sent under its deadline. Elapsed reads the same wherever it fired — the handshake or the recheck's listing — because the call was never sent: the attempt did not land, and nothing acted."

In `execute`, replace:

```rust
        let outcome = within(
            deadline,
            self.call(
                &session, scope, server, descriptor, policy, intent, call, &secrets,
            ),
        )
        .await
        .and_then(std::convert::identity);
```

with:

```rust
        // The recheck's listing runs before the send and the call from it on,
        // both under the attempt's one deadline: what an elapsed deadline
        // means depends on which side of the send it fired.
        let outcome = async {
            within(
                deadline,
                self.recheck(&session, server, descriptor, &call.tool),
            )
            .await??;
            within_after_send(
                deadline,
                self.call(
                    &session, scope, server, descriptor, policy, intent, call, &secrets,
                ),
            )
            .await?
        }
        .await;
```

In `call`, delete the line `self.recheck(session, server, descriptor, &call.tool).await?;`, update its doc comment to "The request and the mapping of its answer — everything from the send on.", and replace the `call_tool_once` error mapping:

```rust
            .map_err(|error| {
                dispatch_error(client::service_error(
                    server.binding.server_id.as_str(),
                    &error,
                ))
            })?;
```

with:

```rust
            .map_err(|error| sent_call_error(server.binding.server_id.as_str(), &error))?;
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cargo test -p rakka-agent-mcp --features testkit`
Expected: PASS — including the unchanged `the_intents_timeout_bounds_a_listing_that_stalls_before_the_call`, `an_intent_with_no_timeout_is_still_bounded_by_the_executors_default`, `the_intents_timeout_bounds_a_handshake_that_is_never_answered`, and the two `child_process.rs` deadline tests, all of which still read `mcp-transport-failed`.
Run: `cargo test -p rakka-agent --test mcp_client_dispatch`
Expected: PASS.

If `a_transport_failure_after_the_call_was_sent_leaves_the_outcome_unknown` fails because the 502 reaches the client as `ServiceError::McpError`, the fake is not producing a transport failure: make the middleware answer a body that is not valid JSON-RPC (`(StatusCode::OK, "not json-rpc")`), which rmcp reports as a transport or protocol failure, and keep the test's assertion unchanged.

- [ ] **Step 7: Commit**

```bash
git add crates/rakka-agent-mcp/src/testkit.rs crates/rakka-agent-mcp/src/executor.rs crates/rakka-agent-mcp/tests/client_dispatch.rs crates/rakka-agent-mcp/tests/artifact_sink.rs crates/rakka-agent/tests/mcp_client_dispatch.rs
git commit -m "Leave an MCP call's outcome unknown once tools/call was sent

The executor mapped every elapsed deadline and every transport or
protocol failure to a retryable mcp-transport-failed, whether or not
tools/call had gone out, so a non-idempotent tool exhausted as failed
when the server may have acted. The attempt keeps its one deadline but
spends it in two windows: the handshake and the recheck's listing run
under within, where a failure still reads mcp-transport-failed, and the
call and the mapping of its answer run under within_after_send, where an
elapsed deadline and every rmcp ServiceError except McpError (a JSON-RPC
error the server answered with) become AgentDispatchError::OutcomeUnknown
under the new mcp-outcome-unknown code, which the dispatcher settles by
safety class. The testkit's fake server gains a transport failure after
the call runs and a JSON-RPC error behaviour.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: Documentation

**Files:**
- Modify: `docs/plans/rakka-agent/spec.md` (section 11.5, after the paragraph beginning "For a non-idempotent ambiguous attempt")
- Modify: `docs/rakka-compatibility.md` (a new bullet directly before the one beginning `- Issue #81 (`)
- Modify: `docs/rakka-agents.md` (the MCP sentences in step 4, "Effects")
- Modify: `CHANGELOG.md` (`### Fixed`, before `### Security`)
- Modify: `docs/superpowers/specs/2026-10-11-dispatch-path-outcomes-design.md` (status line)

- [ ] **Step 1: Specification 11.5**

Add after the paragraph that begins "For a non-idempotent ambiguous attempt":

```markdown
An executor that knows an attempt may have invoked the target and could not
read its outcome — a request sent and no answer read — reports the attempt's
outcome as unknown, and the dispatcher MUST settle it by this table exactly as
it settles an attempt whose worker was lost after `Started`. A collaborator
MAY instead declare a failure definitive: the dispatcher then MUST NOT retry
it, and the generation's budget is spent on that attempt. (Issue #85, asks 7
and 14, 2026-10-11.)
```

- [ ] **Step 2: The compatibility bullet**

Insert before the `- Issue #81 (` bullet:

```markdown
- Issue #85 asks 7, 14, and 15 ("dispatch-path outcomes") add two `AgentDispatchError` variants, one substrate method, one `AgentArtifactError` variant, and one stable MCP code, and change how three MCP failures are settled; no schema version moves and no durable field is added. **`AgentDispatchError::Definitive { code, message }`** (`AgentDispatchError::definitive`) is a collaborator's refusal that retrying cannot change: `record_attempt_failure` records it through the new `DurableInbox::record_outbox_exhausted`, which exhausts the outbox entry on that attempt whatever budget remains, and the run receives `Exhausted` — the state a `Collaborator` refusal reaches once its budget is spent, reached on the first attempt. Its `code()` is `dispatch-collaborator-failed`, as `Collaborator`'s is, and its own code rides the detail and the failure reason the same way, so no recorded code changes meaning. **`AgentDispatchError::OutcomeUnknown { code, message }`** (`AgentDispatchError::outcome_unknown`) is an attempt that may have invoked the target and whose outcome is unknown; its `code()` is its own. The dispatcher settles it by specification 11.5's table, as it settles a worker lost after `Started`: `ReadOnly` and `Idempotent` record a failed attempt under the executor's code and retry under the budget with the same idempotency key (and exhaust on the last attempt); `Reconcileable` asks the reconciler in the same pass; `NonIdempotent` parks `WaitingForReconciliation` with an `Indeterminate` outcome under the executor's code and is never re-invoked. Both are honored on the invocation result path, for every effect kind's executor; the credential resolver's failure path is unchanged. `AgentDispatchError` was already `#[non_exhaustive]`. **`AgentArtifactError::Refused { message }`** (`artifact-store-refused`) is a store's refusal it will repeat; `AgentArtifactError` is now **`#[non_exhaustive]`, a source-breaking change** for an exhaustive `match` on it outside `rakka-agent-workflow` (the new variant alone would already have been one). **The MCP executor:** an egress refusal the host declared `retryable: false` (what `AgentAuthorityRefusal::of` builds) is now `Definitive` and exhausts on the first attempt — before, it was retried up to the tool's `max_attempts`; one declared `retryable: true` (`AgentAuthorityRefusal::transient`) is still a retried `Collaborator`. An artifact sink answering `Refused` is `Definitive`, so the remote tool is not invoked again; every other sink error is still a retried `Collaborator`. The attempt's one deadline is spent in two windows split at the send of `tools/call`: before it (the handshake and the recheck's listing), an elapsed deadline or a transport or protocol failure is still `mcp-transport-failed`; from it on (the call and the mapping of its answer), an elapsed deadline and every rmcp `ServiceError` except `McpError` — a JSON-RPC error the server answered with, which keeps `mcp-transport-failed` — is `OutcomeUnknown` under **`mcp-outcome-unknown`**, the seventeenth MCP code. So the slice 7.7 bullet's `mcp-transport-failed` case "the call, and result mapping … running past its one shared deadline" is now `mcp-outcome-unknown`, and a **non-idempotent MCP tool whose call was sent and whose answer was lost now parks for reconciliation instead of exhausting as failed**. The `rakka.agent.dispatch.write_retries` counter's `operation` label gains the value `exhausted`. The testkit's `FakeMcpServer` gains `with_call_transport_failure` and `FakeToolBehaviour` gains `RpcError`, both additive (`FakeToolBehaviour` is `#[non_exhaustive]`).
```

- [ ] **Step 3: The product doc**

In `docs/rakka-agents.md` step 4, after the sentence ending "and runs under a time bound (`MCP_SYNC_TIMEOUT_DEFAULT_MS`, or the caller's own).", add:

```markdown
   A call whose request was sent and whose answer was lost has an unknown
   outcome and is settled by its safety class — a non-idempotent tool parks
   for reconciliation — and a refusal the deployment declares definitive (a
   non-retryable egress refusal, an artifact store's refusal) is not retried.
```

- [ ] **Step 4: The changelog**

Add to `### Fixed`, before `### Security`:

```markdown
- A dispatch collaborator could not declare a failure definitive, an artifact store could not refuse a write definitively, and the MCP executor read every failure after `tools/call` was sent as a retryable `mcp-transport-failed` — so an egress refusal the host marked non-retryable was retried up to `max_attempts` times, a store's refusal of a too-large result had the remote tool invoked again on every attempt, and a non-idempotent MCP tool whose answer was lost exhausted as failed when the server may have acted. `AgentDispatchError::Definitive` now exhausts its ticket on the attempt (through the new `DurableInbox::record_outbox_exhausted`), `AgentDispatchError::OutcomeUnknown` is settled by specification 11.5's safety-class table, `AgentArtifactError` gains `Refused` and becomes `#[non_exhaustive]`, and the MCP executor maps a non-retryable egress refusal and a refused write to `Definitive` and every call-phase failure but a JSON-RPC error answer to `OutcomeUnknown` under the new `mcp-outcome-unknown` (issue #85, asks 7, 14, and 15).
```

- [ ] **Step 5: The spec's status**

In `docs/superpowers/specs/2026-10-11-dispatch-path-outcomes-design.md`, replace `Status: design, 2026-10-11.` with `Status: implemented, 2026-10-11.`

- [ ] **Step 6: Check and commit**

Run: `cargo test -p rakka-agent --test product_doc_currency --test compatibility_currency`
Expected: PASS.

```bash
git add docs/plans/rakka-agent/spec.md docs/rakka-compatibility.md docs/rakka-agents.md CHANGELOG.md docs/superpowers/specs/2026-10-11-dispatch-path-outcomes-design.md
git commit -m "Record the dispatch-path outcomes in the specification, the compatibility document, the product doc, and the changelog

Specification 11.5 now requires an executor-reported unknown outcome to
be settled by its table and a definitive refusal not to be retried. The
compatibility document registers the two AgentDispatchError variants,
record_outbox_exhausted, AgentArtifactError::Refused and its
source-breaking non_exhaustive, mcp-outcome-unknown, the narrowed
mcp-transport-failed, and the behaviour change for non-idempotent MCP
tools; the product doc and the changelog follow, and the design spec is
marked implemented.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: Validation and the pull request

- [ ] **Step 1: Full validation**

Run in the background with `SCRATCH` set to the session's scratchpad directory, wait for the notification, and read the log (never pipe it into `tail` for the exit code):

```bash
scripts/validate.sh > "$SCRATCH/validate-dispatch-path-outcomes.log" 2>&1; echo "VALIDATE_EXIT=$?" >> "$SCRATCH/validate-dispatch-path-outcomes.log"
```

Expected: `VALIDATE_EXIT=0` and no `warning:` lines. If the workspace test step fails only in `examples/generated-contracts` with `ask timed out`, check for another project's `cargo test` loading the machine, wait for it, and re-run before concluding anything.

- [ ] **Step 2: Cold MSRV check**

```bash
rm -rf target/msrv88-outcomes && CARGO_TARGET_DIR=target/msrv88-outcomes cargo +1.88.0 check --workspace --all-targets; echo "MSRV_EXIT=$?"; rm -rf target/msrv88-outcomes
```

Expected: `MSRV_EXIT=0`.

- [ ] **Step 3: Push and open the PR against `main`**

Title: `Settle definitive refusals and unknown MCP outcomes on the dispatch path`. The body summarizes the six spec decisions, names the source break (`AgentArtifactError` `#[non_exhaustive]`), the behaviour change for non-idempotent MCP tools, the tests with their pre-fix failures, and the validation results, and ends with `🤖 Generated with [Claude Code](https://claude.com/claude-code)`.
