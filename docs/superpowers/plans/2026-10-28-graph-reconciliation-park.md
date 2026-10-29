# Graph Reconciliation Park Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A graph node whose effect may have run can be parked in the graph state as a waiting node that waits on a person, a decision through the durable inbox either re-offers it under a new effect generation with a fresh budget or abandons it, recovery can reach such a node even when its effect exhausted while the run actor was down, a node's declared attempt bound governs its retries, and a requested cancel survives a non-terminal graph update (issue #110 and its addendum).

**Architecture:** `AgentGraphWaitReason::Reconciliation` plus a cause code on the node is the park; the existing `EffectFailed` command enters it through a fourth disposition, `Reconcile`, which the bridge refuses unless the effect's outbox entry is already terminal. One new inbox command kind carries a graph-level decision type with `NotExecuted` and `Abandon`. A per-node `effect_generation` counter, bumped by `NotExecuted`, suffixes the effect id and dedupe key for generations one and up, selects the idempotency key by the target's declared safety class, and fences every effect-keyed command so a superseded generation's command is a no-op. The graph state schema version moves to 2 with a reader gate; recovery re-links every outbox entry that names a graph node, not only the due ones.

**Tech Stack:** Rust 1.88, Tokio, serde (JSON through `serde_json`), the in-memory durable stores of `rakka-persistence`; `cargo test -p rakka-agent-workflow --test <file>`; `scripts/validate.sh`.

**Spec:** `docs/superpowers/specs/2026-10-28-graph-reconciliation-park-design.md`

## Global Constraints

- MSRV 1.88; workspace lints apply: every public item documented, `cargo clippy --workspace --all-targets --all-features -- -D warnings` clean.
- `rakka-agent-workflow` must not depend on `rakka-agent` (the dependency runs the other way); attribute names the bridge reads are defined in `rakka-agent-workflow` and `rakka-agent` keeps equal strings.
- `AgentGraphNodeStatus`, `AgentGraphWaitReason`, `AgentGraphEffectFailureDisposition`, `AgentCommandKind`, and the new `AgentGraphReconciliationDecision` stay exhaustive, not `#[non_exhaustive]`; every addition is named as source-breaking in `CHANGELOG.md`.
- No existing stable code changes. New codes: `graph-state-schema-unsupported` (scheduler error), `graph-effect-not-terminal` (bridge error), `empty-reconciliation-cause` (scheduler error), and the `InvalidCompiledPlan` validation codes `invalid-effect-safety-class` and `invalid-effect-max-attempts`.
- Generation-0 strings never change: the effect id `graph-effect:{identity}`, dedupe key `graph-effect-dedupe:{identity}`, idempotency key `graph-effect-idempotency:{identity}`, with `identity` exactly as `effect_identity` builds it today.
- The wait reason `Reconciliation` is written only by `EffectFailed` with disposition `Reconcile`; no timer, recovery, retry, or scheduler pass writes it. `effect_generation` moves only in `resume_parked_node`.
- Every new persisted field carries `#[serde(default)]`; new enum values use kebab-case serde names equal to their labels: `reconciliation`, `reconcile`, `graph-reconciliation-decided`, `not-executed`, `abandon`.
- Attribute names: `agent_effect_safety_class` with values `read-only`, `idempotent`, `non-idempotent`, `reconcileable`; `agent_effect_max_attempts` as a positive decimal integer. Both as public `&str` constants in `rakka-agent-workflow`.
- Commit after every task with the attribution trailer `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`. Run `cargo clean` only at a task boundary, and only when disk space needs it.

## Review Focus

Inputs the spec implies but no proof exercises directly. Each line's test is added to the owning task.

1. A generation-0 command arrives in the window after `NotExecuted` and before the host re-schedules the node (the node is `Running`, generation 1, listing only the generation-0 id): it must be a no-op, not an `InvalidNodeStatus`. Task 4, test `a_generation_zero_command_in_the_resume_window_is_a_no_op`.
2. A child-workflow node (wait reason `ChildWorkflow`, not `Effect`) is parked and resumed by the same path. Task 2, test `reconcile_parks_a_child_workflow_node_too`.
3. The dispatcher breaks its contract and sends `Exhausted` before `Reconcile` for the same effect: the run has failed, and the later `Reconcile` must be a no-op on the failed node, never a panic or a resurrection. Task 2, test `reconcile_after_an_exhausted_report_is_a_no_op_on_the_failed_node`.
4. A graph state persisted at version 1 without `effect_generation` is parked by a version-2 binary: it decodes with generation 0, parks, is written at version 2, and decodes again. Task 2, test `a_version_one_state_parks_and_is_written_at_version_two`.
5. A run cancel lands while a node is parked: the node is cancelled with the rest. Task 2, test `cancel_graph_run_cancels_a_parked_node`.

---

### Task 1: Schema version 2 and the reader gate

**Files:**
- Modify: `crates/rakka-agent-workflow/src/graph_state.rs:20` (`CURRENT_AGENT_GRAPH_STATE_SCHEMA_VERSION`)
- Modify: `crates/rakka-agent-workflow/src/graph_scheduler.rs` (`AgentGraphSchedulerError` `:22-56`, `code()` `:84-91`, `Display`, `validate_plan_state` `:732`)
- Modify: `crates/rakka-agent-workflow/src/effect_bridge.rs` (`validate_plan_state` `:1102`)
- Modify: `crates/rakka-agent-workflow/src/runner.rs` (`update_graph_state` `:891`)
- Modify: `docs/rakka-compatibility.md:198` (the version row)
- Test: `crates/rakka-agent-workflow/tests/graph_scheduler.rs`, `tests/effect_bridge.rs`, `tests/step_runner.rs`, `tests/api_compatibility.rs`

**Interfaces:**
- Produces: `CURRENT_AGENT_GRAPH_STATE_SCHEMA_VERSION == AgentGraphStateSchemaVersion::new(2)`; `AgentGraphSchedulerError::UnsupportedSchemaVersion { found: AgentGraphStateSchemaVersion, supported: AgentGraphStateSchemaVersion }` with code `graph-state-schema-unsupported` and display `graph state schema version {found} is newer than the supported {supported}`; the scheduler's and the bridge's `validate_plan_state` return it (the bridge wraps it as `AgentGraphEffectBridgeError::Scheduler { error }`) when `state.graph_schema_version > CURRENT_AGENT_GRAPH_STATE_SCHEMA_VERSION`; `AgentStepRunner::update_graph_state` refuses such a state with `AgentRunEngineError::WorkflowMismatch { run_id, field: "graph_schema_version", expected: "<= 2", actual: "<found>" }` right after `current_record`, before `reject_terminal` and before persisting.

- [ ] **Step 1: Write the failing tests**

In `graph_scheduler.rs` tests, using `linear_plan()` and `AgentGraphScheduler::initialize_state`:

```rust
#[test]
fn scheduler_refuses_a_graph_state_from_a_newer_schema() {
    let plan = linear_plan();
    let scheduler = AgentGraphScheduler::new();
    let mut state = scheduler.initialize_state(&plan, ts(100)).unwrap();
    state.graph_schema_version = AgentGraphStateSchemaVersion::new(3);
    let error = scheduler.mark_ready_nodes_runnable(&plan, state, ts(110)).unwrap_err();
    assert!(matches!(error, AgentGraphSchedulerError::UnsupportedSchemaVersion { found, supported }
        if found == AgentGraphStateSchemaVersion::new(3) && supported == CURRENT_AGENT_GRAPH_STATE_SCHEMA_VERSION));
    assert_eq!(error.code(), "graph-state-schema-unsupported");
}
```

In `effect_bridge.rs` tests, with `effect_plan(...)` and `running_effect_state`, set the version to 3 on the state and assert `schedule_node_effect` returns `AgentGraphEffectBridgeError::Scheduler { error: AgentGraphSchedulerError::UnsupportedSchemaVersion { .. } }` (`bridge_refuses_a_graph_state_from_a_newer_schema`).

In `step_runner.rs`, `update_graph_state_refuses_a_newer_graph_schema`: `start_only(..)`, then `update_graph_state(state_at_version_3, ts(300))` returns `AgentRunEngineError::WorkflowMismatch { field: "graph_schema_version", .. }` and `runner.state()` still has `graph_state == None`. Build the state with `AgentGraphRunState::new(AgentCompiledPlanId::new("plan-x"), AgentCompiledPlanFingerprint::new("sha256:x"))` and set the version.

In `api_compatibility.rs`, `graph_state_json_accepts_current_previous_and_missing_additive_defaults` (`:423`) writes `0` into its "previous" fixture and asserts `AgentGraphStateSchemaVersion::new(0)` (`:445-448`): make both `CURRENT_AGENT_GRAPH_STATE_SCHEMA_VERSION.get() - 1`, so the fixture is the version-1 record a version-2 reader must still decode.

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p rakka-agent-workflow --test graph_scheduler newer_schema` and the same filter for `effect_bridge` and `step_runner`.
Expected: compile error, `UnsupportedSchemaVersion` does not exist.

- [ ] **Step 3: Implement the version, the variant and the three gates**

Set the constant to 2. Add the variant, its code and display. In both `validate_plan_state` functions, after the fingerprint check: `if state.graph_schema_version > CURRENT_AGENT_GRAPH_STATE_SCHEMA_VERSION { return Err(...) }`. In `update_graph_state`, the same comparison right after `let record = self.current_record()?;`, mapped to `WorkflowMismatch` with `expected: format!("<= {}", CURRENT_AGENT_GRAPH_STATE_SCHEMA_VERSION.get())` and `actual: graph_state.graph_schema_version.get().to_string()`. Change the compatibility table row to `2`.

- [ ] **Step 4: Run the crate's tests and the doc-holding test**

Run: `cargo test -p rakka-agent-workflow && cargo test -p rakka-agent --features otel --test compatibility_currency`
Expected: PASS, including `compatibility_currency` with the row at 2.

- [ ] **Step 5: Commit**

```bash
git add crates/rakka-agent-workflow/src/graph_state.rs crates/rakka-agent-workflow/src/graph_scheduler.rs crates/rakka-agent-workflow/src/effect_bridge.rs crates/rakka-agent-workflow/src/runner.rs docs/rakka-compatibility.md crates/rakka-agent-workflow/tests/graph_scheduler.rs crates/rakka-agent-workflow/tests/effect_bridge.rs crates/rakka-agent-workflow/tests/step_runner.rs crates/rakka-agent-workflow/tests/api_compatibility.rs
git commit -m "Move the graph state schema to version 2 and refuse a newer one with a typed error"
```

---

### Task 2: The park: wait reason, scheduler transition, and the `Reconcile` disposition

**Files:**
- Modify: `crates/rakka-agent-workflow/src/graph_state.rs:463` (`AgentGraphWaitReason`, `as_label`)
- Modify: `crates/rakka-agent-workflow/src/graph_scheduler.rs` (new `park_waiting_node` next to `wait_node` `:590`; new error variant `EmptyCauseCode`)
- Modify: `crates/rakka-agent-workflow/src/effect_bridge.rs` (`AgentGraphEffectFailureDisposition` `:644-690`, `AgentGraphEffectBridgeError` `:45-192`, `accept_and_apply_effect_failed` `:1021`, `apply_effect_failed_command` `:1392`, `apply_effect_completed_command` `:1347`)
- Modify: `crates/rakka-agent-workflow/src/snapshots.rs:329-385` (a `graph_reconciliation_waiting_node_count`)
- Test: `crates/rakka-agent-workflow/tests/effect_bridge.rs`, `tests/graph_scheduler.rs`, `tests/graph_state_contract.rs`, `tests/runtime_snapshots.rs`, `tests/workflow_query_model.rs`

**Interfaces:**
- Consumes: Task 1's version constant (the park raises a version-1 state to it).
- Produces: `AgentGraphWaitReason::Reconciliation` (serde `reconciliation`, label `reconciliation`). `AgentGraphScheduler::park_waiting_node(&self, plan: &AgentCompiledExecutionPlan, state: AgentGraphRunState, node_id: impl Into<AgentCompiledNodeId>, from_reason: AgentGraphWaitReason, cause_code: impl Into<String>, now: AgentTimestampMillis) -> AgentGraphSchedulerResult<AgentGraphSchedulerTransition>`: requires `Waiting(from_reason)`; sets `wait_reason = Some(Reconciliation)`, `error_code = Some(cause_code)`, bumps `scheduler_revision`, clears `blocked_reason`, sets `state.graph_schema_version = CURRENT_AGENT_GRAPH_STATE_SCHEMA_VERSION`; an empty (after trim) cause returns `AgentGraphSchedulerError::EmptyCauseCode { node_id }`, code `empty-reconciliation-cause` (`effect_failed_command` already refuses a blank `error_code` through `require_command`, `facade.rs:610`, and that is the cause's only bound; the scheduler check guards the public transition). `AgentGraphEffectFailureDisposition::Reconcile` (label and `FromStr` value `reconcile`). `AgentGraphEffectBridgeError::EffectNotTerminal { effect_id: AgentEffectId, status: OutboxStatus }`, code `graph-effect-not-terminal`. `AgentGraphEffectBridge::accept_and_apply_effect_failed` with disposition `Reconcile` reads `inbox.inner().state()` (a not-recovered inbox maps to the bridge's `Inbox` error the way `AgentRunInbox::due_effects` maps the same call) and looks up `outbox_entry(&OutboxMessageId::new(effect_id.as_str()))` before accepting the command: no entry is `UnknownEffect`, a status other than `Exhausted` or `Cancelled` is `EffectNotTerminal`. The private helper `fn node_accepts_effect_command(node_state: &AgentGraphNodeState, kind_reason: AgentGraphWaitReason) -> bool` is `status == Waiting && matches!(wait_reason, Some(r) if r == kind_reason || r == Reconciliation)`, used by both apply functions in place of the exact-reason check. `AgentWorkflowRuntimeSnapshot` (serialize-only, private fields) gains a `graph_reconciliation_waiting_node_count` field with a `pub const fn graph_reconciliation_waiting_node_count(&self) -> usize` accessor, summed through `graph_wait_reason_count(graph, Reconciliation)` beside the four existing reasons; no other export site lists the per-reason counts.

- [ ] **Step 1: Write the failing tests**

In `effect_bridge.rs` tests, with a helper `async fn exhaust(inbox: &mut TestInbox, effect_id: &AgentEffectId)` that calls `inbox.inner_mut().record_outbox_exhausted(&OutboxMessageId::new(effect_id.as_str()), "effect-outcome-unknown").await.unwrap()`, and `fn reconcile_command(bridge, plan, run_id, command_id, effect_id, cause) -> AgentCommand` wrapping `effect_failed_command(.., cause, AgentGraphEffectFailureDisposition::Reconcile)`:

```rust
#[tokio::test]
async fn reconcile_disposition_parks_the_node_and_leaves_the_run_running() {
    // schedule a ToolCall effect; exhaust its entry; apply Reconcile with cause "effect-outcome-unknown"
    let node = node_state(&outcome.transition.state, "effect");
    assert_eq!(node.status, AgentGraphNodeStatus::Waiting);
    assert_eq!(node.wait_reason, Some(AgentGraphWaitReason::Reconciliation));
    assert_eq!(node.error_code.as_deref(), Some("effect-outcome-unknown"));
    assert!(node.scheduled_effect_ids.contains(&effect_id));
    assert_eq!(outcome.transition.state.terminal_status, None);
    assert_eq!(node_state(&outcome.transition.state, "terminal").status, AgentGraphNodeStatus::Pending);
    assert!(outcome.transition.runnable_node_ids.is_empty());
    // the entry is still Exhausted and not due:
    assert_eq!(inbox.inner().state().unwrap().outbox_entry(&message_id).unwrap().status(), OutboxStatus::Exhausted);
    assert!(inbox.due_effects().unwrap().is_empty());
}

#[tokio::test]
async fn reconcile_is_refused_while_the_entry_is_live() {
    // Pending entry => Err(EffectNotTerminal { status: OutboxStatus::Pending }); after inbox.inner_mut().mark_outbox_dispatching(..) => Dispatching;
    // after record_outbox_failure(.., "rate-limited", false) with attempts left => Failed; on a fresh run after record_outbox_success => Dispatched;
    // the node is still Waiting(Effect) each time; after exhausting, the same command (same command id) is accepted, not a duplicate: the refusals recorded nothing.
    // Sibling test reconcile_parks_over_a_cancelled_entry: record_outbox_cancelled (rakka-workflow/src/inbox.rs:576) then Reconcile parks.
}

#[tokio::test]
async fn reconcile_twice_is_a_no_op_and_a_new_cause_replaces() {
    // same cause again: changed_node_ids empty, scheduler_revision unchanged; cause "effect-reply-lost": error_code replaced, revision +1
}

#[tokio::test]
async fn parked_node_completes_on_a_late_effect_completed() {
    // park, then effect_completed_command(.., effect_id, None) => node Completed, wait None, error None, terminal node Runnable
}

#[tokio::test]
async fn parked_node_fails_on_a_definitive_effect_failed() {
    // park, then EffectFailed{Exhausted,"retry-budget-exhausted"} => node Failed, run terminal Failed
}

#[tokio::test]
async fn parked_node_ignores_a_retry_scheduled_report() {
    // park, then EffectFailed{RetryScheduled,"rate-limited"} => no-op, node still parked with the original cause
}

#[tokio::test]
async fn reconcile_after_an_exhausted_report_is_a_no_op_on_the_failed_node() {
    // EffectFailed{Exhausted} first => run Failed; then Reconcile => Ok, changed empty, node still Failed
}

#[tokio::test]
async fn reconcile_parks_a_child_workflow_node_too() {
    // effect_plan(AgentCompiledNodeKind::ChildWorkflowCommand, "workflow", "child", "child-workflow"): wait reason ChildWorkflow parks to Reconciliation
}

#[tokio::test]
async fn a_version_one_state_parks_and_is_written_at_version_two() {
    // serialize running_effect_state(..) to JSON, set "graph_schema_version" to 1, remove any "effect_generation", decode; schedule + exhaust + Reconcile;
    // outcome.transition.state.graph_schema_version == CURRENT_AGENT_GRAPH_STATE_SCHEMA_VERSION; the state round-trips through JSON
}
```

In `graph_scheduler.rs` tests: `cancel_graph_run_cancels_a_parked_node` (`wait_node(Effect)` then `park_waiting_node(.., Effect, "effect-outcome-unknown", ..)` then `cancel_graph_run` => node `Cancelled`, `wait_reason None`, terminal `Cancelled`) and `park_waiting_node_requires_a_cause_code` (`"  "` => `EmptyCauseCode`, code `empty-reconciliation-cause`; a node waiting on `Timer` with `from_reason: Effect` => the existing wait-reason error).

In `graph_state_contract.rs`: extend `graph_node_statuses_have_stable_wire_names_and_labels` with `serde_json::to_value(AgentGraphWaitReason::Reconciliation) == json!("reconciliation")` and the label.

In `runtime_snapshots.rs`: `runtime_snapshot_counts_parked_nodes`, modelled on `runtime_snapshot_reports_graph_summaries` (`:144`) with its own graph state (built like `graph_state()` at `:365`) holding one `Waiting` node with `wait_reason: Some(Reconciliation)`: `runtime.graph_reconciliation_waiting_node_count() == 1` and `runtime.graph_waiting_node_count() == 1`.

In `workflow_query_model.rs`: `query_index_filters_parked_nodes_by_wait_reason`, modelled on `query_index_projects_and_filters_graph_state` (`:251`): a run whose graph (built with `graph_state(..)` at `:411`) holds one parked node is returned by the query's wait-reason filter (`query.rs:550`) for `Reconciliation` and not for `Effect`.

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p rakka-agent-workflow --test effect_bridge reconcile parked_node`
Expected: compile error on `Reconciliation` and `Reconcile`.

- [ ] **Step 3: Implement**

The enum values and labels; `park_waiting_node` via `transition_node(expected Waiting, next Waiting, ..)` after `ensure_node_wait_reason(from_reason)`; the disposition and its parse; the error variant, code and display; the entry precondition in `accept_and_apply_effect_failed` (only when the parsed disposition is `Reconcile`); in `apply_effect_failed_command`, the widened guard and the dispatch: `Reconcile` => `park_waiting_node(.., wait_reason_of_the_kind, error_code, received_at)` unless already parked (same cause: no-op; other cause: replace through `park_waiting_node` with `from_reason: Reconciliation`); `Exhausted`/`Terminal` => `fail_waiting_node` with the node's current wait reason; `RetryScheduled` on a parked node => no-op. In `apply_effect_completed_command`, the widened guard and `complete_waiting_node` with the node's current wait reason. The snapshot counter.

- [ ] **Step 4: Run the crate's tests and clippy**

Run: `cargo test -p rakka-agent-workflow && cargo clippy -p rakka-agent-workflow --all-targets --all-features -- -D warnings`
Expected: PASS and clean. `exhausted_failure_marks_node_and_graph_failed` and `retryable_failure_keeps_effect_node_waiting` still pass.

- [ ] **Step 5: Commit**

```bash
git add crates/rakka-agent-workflow/src/graph_state.rs crates/rakka-agent-workflow/src/graph_scheduler.rs crates/rakka-agent-workflow/src/effect_bridge.rs crates/rakka-agent-workflow/src/snapshots.rs crates/rakka-agent-workflow/tests/
git commit -m "Park a graph node whose effect may have run as a waiting node that waits on a person (issue #110 ask 1)"
```

---

### Task 3: The decision command, `NotExecuted` and `Abandon`

**Files:**
- Modify: `crates/rakka-agent-workflow/src/graph_state.rs` (new `AgentGraphReconciliationDecision`; `AgentGraphNodeState.effect_generation` `:256-300`)
- Modify: `crates/rakka-agent-workflow/src/facade.rs:194-262` (`AgentCommandKind`, `type_name`, `message_type`)
- Modify: `crates/rakka-agent-workflow/src/graph_scheduler.rs` (new `resume_parked_node`)
- Modify: `crates/rakka-agent-workflow/src/effect_bridge.rs` (new `reconciliation_decision_command`, `accept_and_apply_reconciliation_decision`, `apply_reconciliation_decision_command`)
- Modify: `crates/rakka-agent-workflow/src/lib.rs` (export the decision type)
- Test: `crates/rakka-agent-workflow/tests/effect_bridge.rs`, `tests/graph_state_contract.rs`

**Interfaces:**
- Consumes: Task 2's park and `fail_waiting_node(.., Reconciliation, ..)`.
- Produces: `pub enum AgentGraphReconciliationDecision { NotExecuted, Abandon { error_code: String } }` with `#[serde(tag = "kind", rename_all = "kebab-case")]` and `pub const fn as_label(&self) -> &'static str` (`not-executed`, `abandon`). `AgentCommandKind::GraphReconciliationDecided { effect_id: AgentEffectId, decision: AgentGraphReconciliationDecision }`, `type_name` `GraphReconciliationDecided`, `message_type` `agent.graph-reconciliation-decided`. `AgentGraphNodeState.effect_generation: u32` (`#[serde(default)]`). `AgentGraphScheduler::resume_parked_node(&self, plan, state, node_id, now) -> AgentGraphSchedulerResult<AgentGraphSchedulerTransition>`: `Waiting(Reconciliation)` to `Running`, `wait_reason = None`, `error_code = None`, `effect_generation += 1`, `attempt` and `started_at` unchanged, version raised to current, revision bumped. `AgentGraphEffectBridge::reconciliation_decision_command(&self, metadata: AgentCommandMetadata, effect_id: AgentEffectId, decision: AgentGraphReconciliationDecision) -> AgentGraphEffectBridgeResult<AgentCommand>` and `async fn accept_and_apply_reconciliation_decision<Store, Clock>(&self, plan, state, command, inbox) -> AgentGraphEffectBridgeResult<AgentGraphEffectCommandOutcome>` with the apply rules of spec section 5.2 (generation fencing arrives in Task 4). An `Abandon` whose code is empty after trim is refused with `Scheduler { error: AgentGraphSchedulerError::EmptyCauseCode { node_id } }` before any transition.

- [ ] **Step 1: Write the failing tests**

In `effect_bridge.rs` tests, with `async fn park(bridge, plan, run_id, inbox) -> (AgentEffectId, AgentGraphRunState)` doing schedule + exhaust + `Reconcile`:

```rust
#[tokio::test]
async fn not_executed_decision_resumes_the_node_running_with_the_next_generation() {
    let (effect_id, state) = park(..).await;
    let command = bridge.reconciliation_decision_command(command_metadata(&plan, run_id, "cmd-decide-1", 400), effect_id.clone(), AgentGraphReconciliationDecision::NotExecuted).unwrap();
    let outcome = bridge.accept_and_apply_reconciliation_decision(&plan, state, command, &mut inbox).await.unwrap();
    let node = node_state(&outcome.transition.state, "effect");
    assert_eq!(node.status, AgentGraphNodeStatus::Running);
    assert_eq!(node.wait_reason, None);
    assert_eq!(node.error_code, None);
    assert_eq!(node.effect_generation, 1);
    assert_eq!((node.attempt, node.started_at), (parked.attempt, parked.started_at)); // parked = node_state(&state, "effect").clone() before the decision
    assert_eq!(node.scheduled_effect_ids, vec![effect_id]);
    assert_eq!(outcome.transition.state.terminal_status, None);
}

#[tokio::test]
async fn abandon_decision_fails_the_node_and_the_run() {
    // Abandon { error_code: "reconciliation-abandoned" } => node Failed with that code, terminal Failed, terminal node Cancelled
}

#[tokio::test]
async fn a_decision_on_an_unparked_node_is_refused() {
    // schedule only (Waiting(Effect)) => Err(InvalidNodeStatus { status: Waiting, .. })
}

#[tokio::test]
async fn a_replayed_decision_after_the_node_resolved_is_a_no_op() {
    // Abandon applied; then NotExecuted under a new command id => Ok, changed empty, node still Failed
}

#[tokio::test]
async fn a_decision_for_an_unknown_effect_is_refused() {
    // effect id "graph-effect:nope" => Err(UnknownEffect)
}
```

In `graph_state_contract.rs`: `reconciliation_decisions_have_stable_wire_names` asserting `json!({"kind":"not-executed"})` and `json!({"kind":"abandon","error_code":"x"})`, and `AgentCommandKind::GraphReconciliationDecided { .. }` serializes with `"type": "graph-reconciliation-decided"`, `type_name() == "GraphReconciliationDecided"`, `message_type() == "agent.graph-reconciliation-decided"`; and `AgentGraphNodeState` JSON without `effect_generation` decodes with 0.

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p rakka-agent-workflow --test effect_bridge decision`
Expected: compile error on `AgentGraphReconciliationDecision`.

- [ ] **Step 3: Implement**

The type, the field, the command kind and its two names, `resume_parked_node` via `transition_node(expected Waiting, next Running, ..)` after `ensure_node_wait_reason(Reconciliation)`, the builder (an `AgentCommand::new(kind, metadata)` mapped to `InvalidCommand`), the accept-and-apply pair shaped like `accept_and_apply_effect_failed`, and the apply function with the status rules.

- [ ] **Step 4: Run the crate's tests and clippy**

Run: `cargo test -p rakka-agent-workflow && cargo clippy -p rakka-agent-workflow --all-targets --all-features -- -D warnings`
Expected: PASS and clean.

- [ ] **Step 5: Commit**

```bash
git add crates/rakka-agent-workflow/src/ crates/rakka-agent-workflow/tests/
git commit -m "Resolve a parked graph node with a durable not-executed or abandon decision (issue #110 ask 2)"
```

---

### Task 4: Effect generations and generation fencing

**Files:**
- Modify: `crates/rakka-agent-workflow/src/effect_bridge.rs` (`effect_from_node` `:1680-1718`, `effect_identity` `:1989`, `schedule_node_effect` `:716`, the three apply functions, a new `effect_from_node_in_state`)
- Modify: `crates/rakka-agent-workflow/src/lib.rs` (export `ATTR_AGENT_EFFECT_SAFETY_CLASS`)
- Modify: `crates/rakka-agent/src/effect.rs` (a test that its `ATTR_AGENT_EFFECT_SAFETY_CLASS` equals the kernel's)
- Test: `crates/rakka-agent-workflow/tests/effect_bridge.rs`

**Interfaces:**
- Consumes: Task 3's `effect_generation` and `resume_parked_node`.
- Produces: `pub const ATTR_AGENT_EFFECT_SAFETY_CLASS: &str = "agent_effect_safety_class"` in `rakka-agent-workflow`. `AgentGraphEffectBridge::effect_from_node_in_state(&self, plan, state: &AgentGraphRunState, request) -> AgentGraphEffectBridgeResult<AgentEffect>`; `effect_from_node(plan, request)` is unchanged and equals the state form at generation 0; `schedule_node_effect` uses the state form. Private: `fn effect_identity_for_generation(plan, node, request, kind, target_class, generation: u32) -> String` returning `effect_identity(..)` for 0 and `format!("{identity};gen={generation}")` otherwise; `fn effect_generation_of(effect_id: &AgentEffectId) -> u32` reading a trailing `;gen=<u32>` (0 when absent or unparsable); `fn idempotency_key_for(identity_gen0: &str, identity_gen: &str, safety_class: Option<&str>) -> Result<String>`: `Some("idempotent")` => `graph-effect-idempotency:{identity_gen}`; `None`, `Some("non-idempotent")`, `Some("reconcileable")`, `Some("read-only")` => `graph-effect-idempotency:{identity_gen0}`; anything else => `InvalidCompiledPlan { validation_code: "invalid-effect-safety-class", reason }`. In the three apply functions, after `node_id_for_effect`: `if effect_generation_of(effect_id) != node_state.effect_generation { return Ok(noop_transition(state)); }`.

- [ ] **Step 1: Write the failing tests**

In `effect_bridge.rs` tests, with `fn effect_plan_with(kind, target_type, target_name, target_class, attributes: &[(&str, &str)])` (same as `effect_plan` plus extra target attributes):

```rust
#[tokio::test]
async fn a_re_offered_node_schedules_a_new_generation_with_a_fresh_budget() {
    // park; NotExecuted; schedule_node_effect(.., effect_request(run_id, "effect", 500), ..)
    assert!(second.acceptance.is_scheduled());
    assert!(second.effect.effect_id.as_str().ends_with(";gen=1"));
    assert!(second.effect.deduplication_key.as_str().ends_with(";gen=1"));
    let state = inbox.inner().state().unwrap();
    assert_eq!(state.outbox_entry(&OutboxMessageId::new(second.effect.effect_id.as_str())).unwrap().status(), OutboxStatus::Pending);
    assert_eq!(state.outbox_entry(&OutboxMessageId::new(second.effect.effect_id.as_str())).unwrap().retry_policy().max_attempts(), 3);
    assert_eq!(state.outbox_entry(&OutboxMessageId::new(first_effect_id.as_str())).unwrap().status(), OutboxStatus::Exhausted);
    let node = node_state(&second.transition.state, "effect");
    assert_eq!(node.status, AgentGraphNodeStatus::Waiting);
    assert_eq!(node.wait_reason, Some(AgentGraphWaitReason::Effect));
    assert_eq!(node.scheduled_effect_ids, vec![first_effect_id, second.effect.effect_id.clone()]);
}

#[tokio::test]
async fn idempotency_key_follows_the_target_safety_class() {
    // for class in ["idempotent"]: generation-1 key ends with ";gen=1"
    // for class in ["non-idempotent", "reconcileable", "read-only"] and absent: generation-1 key == generation-0 key
    // class "bogus": schedule refused InvalidCompiledPlan { validation_code: "invalid-effect-safety-class", .. }
}

#[tokio::test]
async fn generation_zero_identity_is_unchanged() {
    let effect = bridge.effect_from_node(&plan, &effect_request(run_id.clone(), "effect", 200)).unwrap();
    let identity = format!("run={};plan={};node=effect;loop=root;kind=tool-call;target_class=issue-tracker", run_id.as_str(), plan.plan_fingerprint.as_str());
    assert_eq!(effect.effect_id.as_str(), format!("graph-effect:{identity}"));
    assert_eq!(effect.deduplication_key.as_str(), format!("graph-effect-dedupe:{identity}"));
    assert_eq!(effect.idempotency_key.as_str(), format!("graph-effect-idempotency:{identity}"));
    assert_eq!(bridge.effect_from_node_in_state(&plan, &running_effect_state(&plan), &effect_request(run_id, "effect", 200)).unwrap(), effect);
}

#[tokio::test]
async fn superseded_generation_commands_are_no_ops() {
    // after the generation-1 schedule: generation-0 Reconcile, Exhausted, EffectCompleted and NotExecuted each => Ok, changed empty, node unchanged
}

#[tokio::test]
async fn a_generation_zero_command_in_the_resume_window_is_a_no_op() {
    // after NotExecuted, before any schedule: generation-0 EffectCompleted => Ok, changed empty, node still Running at generation 1
}

#[tokio::test]
async fn generation_one_outcomes_apply() {
    // generation-1 EffectCompleted completes the node; a fresh run: generation-1 Exhausted fails it
}
```

In `crates/rakka-agent/src/effect.rs`'s test module: `attribute_vocabulary_matches_the_workflow_kernel` asserting `ATTR_AGENT_EFFECT_SAFETY_CLASS == rakka_agent_workflow::ATTR_AGENT_EFFECT_SAFETY_CLASS`.

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p rakka-agent-workflow --test effect_bridge generation`
Expected: compile error on `effect_from_node_in_state`; after a stub, `a_re_offered_node_schedules_a_new_generation_with_a_fresh_budget` fails with a `Duplicate` acceptance.

- [ ] **Step 3: Implement**

As the Interfaces block says. `effect_from_node_in_state` reads `state.node_states[&request.node_id].effect_generation` (missing node state: `MissingNodeState`) and threads the generation into the identity, the dedupe key, and the idempotency key; the safety class is read from the node target's attributes (the plan's, before the bridge adds its own).

- [ ] **Step 4: Run the crate's tests and clippy, and the agent crate's unit test**

Run: `cargo test -p rakka-agent-workflow && cargo test -p rakka-agent --lib attribute_vocabulary && cargo clippy -p rakka-agent-workflow -p rakka-agent --all-targets --all-features -- -D warnings`
Expected: PASS and clean. `idempotency_key_is_stable_across_recovery_duplicate` and `duplicate_effect_node_scheduling_returns_existing_outbox_entry` still pass.

- [ ] **Step 5: Commit**

```bash
git add crates/rakka-agent-workflow/src/ crates/rakka-agent-workflow/tests/effect_bridge.rs crates/rakka-agent/src/effect.rs
git commit -m "Re-offer a parked node under a new effect generation and fence commands by generation (issue #110 ask 3)"
```

---

### Task 5: Recovery re-links every graph effect

**Files:**
- Modify: `crates/rakka-agent-workflow/src/outbox.rs:277-283` (new `outbox_effects`)
- Modify: `crates/rakka-agent-workflow/src/runtime.rs:795-799` (`reconcile_recovered_graph_effects`)
- Test: `crates/rakka-agent-workflow/tests/failure_injection.rs`, `tests/runtime_actor.rs`

**Interfaces:**
- Consumes: Task 2's park; Task 4's generation-suffixed ids.
- Produces: `AgentRunInbox::outbox_effects(&self) -> AgentOutboxResult<Vec<AgentDueEffect>>`, every outbox entry decoded as an effect whatever its status (`inner().state()?.outbox().values()` through `agent_due_effect_from_entry`). `AgentRunActor::reconcile_recovered_graph_effects` feeds `reconcile_recovered_effects` the result of `outbox_effects()` instead of `due_effects()`; `reconcile_recovered_effects` is unchanged: a resolved node is left alone, an unresolved node gains the id and waits on the effect's reason.

Ruling carried from the spec: section 5.7 says a terminal entry's re-link "does not change the node's status"; the re-link must set `Waiting` on the effect's reason as it does today, because `Reconcile` requires that status to park the node. Record it in the ledger.

- [ ] **Step 1: Write the failing tests**

In `failure_injection.rs`, next to `crash_between_effect_outbox_write_and_graph_persist_relinks_on_recovery` (`:527`):

```rust
#[tokio::test]
async fn crash_window_effect_exhausted_before_recovery_still_links_and_parks() {
    // as the neighbour up to the drop; then on a fresh inbox: record_outbox_exhausted(effect_id, "effect-outcome-unknown")
    assert!(recovered.due_effects().unwrap().is_empty());
    let effects: Vec<AgentEffect> = recovered.outbox_effects().unwrap().into_iter().map(|due| due.effect).collect();
    assert_eq!(effects.len(), 1);
    let reconciled = bridge.reconcile_recovered_effects(persisted_state, &effects);
    // node Waiting(Effect) listing the id; then EffectFailed{Reconcile} parks it instead of UnknownEffect
}

#[tokio::test]
async fn crash_between_generation_one_outbox_write_and_graph_persist_relinks_on_recovery() {
    // park (schedule + exhaust + Reconcile) and apply NotExecuted; persisted_state = that transition's state (Running, generation 1, listing only the generation-0 id);
    // schedule_node_effect for generation 1 and drop the transition; recover on a fresh inbox: outbox_effects() holds both entries;
    // reconcile_recovered_effects(persisted_state, &effects): node Waiting(Effect), scheduled_effect_ids == [gen0_id, gen1_id]; a second pass changes nothing;
    // then generation 1's EffectCompleted completes the node instead of UnknownEffect
}
```

In `runtime_actor.rs`: `actor_recovery_relinks_an_effect_that_exhausted_while_it_was_down`: drive actor A to a `Running` effect node (`start_graph`, `mark_graph_ready`, `start_graph_node("input")`, `complete_graph_node("input")`, `mark_graph_ready`, `start_graph_node("effect")`), stop it; with a standalone `AgentRunInbox::with_clock(run_id, fixture.workflow_store.clone(), fixture.clock.clone())` schedule the effect through the bridge against that running state (do not persist the transition) and exhaust it; spawn actor B; `snapshot(&B)` shows the effect node `Waiting` with the id in `scheduled_effect_ids` and `due_effect_count == 0`.

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p rakka-agent-workflow --test failure_injection -- exhausted_before_recovery generation_one_outbox_write && cargo test -p rakka-agent-workflow --test runtime_actor relinks_an_effect`
Expected: compile error on `outbox_effects`; after a stub returning `due_effects()`, the exhausted test sees zero effects and the actor test shows the node still `Running` with no id.

- [ ] **Step 3: Implement**

`outbox_effects` and the one-line change in `reconcile_recovered_graph_effects` (keep `now` as the max `created_at`).

- [ ] **Step 4: Run the crate's tests and clippy**

Run: `cargo test -p rakka-agent-workflow && cargo clippy -p rakka-agent-workflow --all-targets --all-features -- -D warnings`
Expected: PASS and clean, including `actor_restart_after_graph_effect_scheduled_recovers_graph_and_due_effect`.

- [ ] **Step 5: Commit**

```bash
git add crates/rakka-agent-workflow/src/outbox.rs crates/rakka-agent-workflow/src/runtime.rs crates/rakka-agent-workflow/tests/failure_injection.rs crates/rakka-agent-workflow/tests/runtime_actor.rs
git commit -m "Re-link every graph effect on recovery so a call exhausted during the crash window can still be parked"
```

---

### Task 6: The declared attempt bound

**Files:**
- Modify: `crates/rakka-agent-workflow/src/effect_bridge.rs` (`AgentGraphEffectScheduleRequest` `:347-375` and its builders, `effect_target_for_node` `:1925-1970`, a validation step in `schedule_node_effect`)
- Modify: `crates/rakka-agent-workflow/src/outbox.rs:309-329` (`agent_effect_to_outbox_command`)
- Modify: `crates/rakka-agent-workflow/src/lib.rs` (export `ATTR_AGENT_EFFECT_MAX_ATTEMPTS`)
- Modify: `crates/rakka-agent/src/effect.rs` (extend the vocabulary test)
- Test: `crates/rakka-agent-workflow/tests/effect_bridge.rs`, `tests/outbox_facade.rs`

**Interfaces:**
- Produces: `pub const ATTR_AGENT_EFFECT_MAX_ATTEMPTS: &str = "agent_effect_max_attempts"` in `rakka-agent-workflow`. `AgentGraphEffectScheduleRequest::max_attempts(mut self, max_attempts: u32) -> Self` setting `pub max_attempts: Option<u32>`; `effect_target_for_node` inserts the request's value (as decimal) into the target attributes, overriding the plan's; the bridge refuses, before scheduling, an attribute that is not a positive decimal integer with `InvalidCompiledPlan { validation_code: "invalid-effect-max-attempts", reason }`. `agent_effect_to_outbox_command` parses the attribute leniently and, when it is a positive integer, calls the existing `OutboxCommand::retry_policy` builder (`rakka-workflow/src/inbox.rs:157`) with `RetryPolicy::new(n, default.initial_backoff_millis(), default.max_backoff_millis())` where `default = RetryPolicy::default()`; otherwise the command keeps the default policy. Tests read the bound through `OutboxEntry::retry_policy().max_attempts()`.

- [ ] **Step 1: Write the failing tests**

In `effect_bridge.rs` tests:

```rust
#[tokio::test]
async fn declared_max_attempts_reach_the_outbox_entry() {
    // effect_plan_with(.., &[("agent_effect_max_attempts", "5")]) => entry.retry_policy().max_attempts() == 5
}

#[tokio::test]
async fn request_max_attempts_override_the_declared_bound() {
    // plan "5", request.max_attempts(2) => 2; the effect's target attribute reads "2"
}

#[tokio::test]
async fn absent_bound_keeps_the_default_of_three() { /* == 3 */ }

#[tokio::test]
async fn an_invalid_bound_is_refused_at_schedule() {
    // "0" and "many" => InvalidCompiledPlan { validation_code: "invalid-effect-max-attempts", .. }; nothing scheduled (due_effects empty)
}
```

In `outbox_facade.rs`: `outbox_command_reads_the_attempt_bound_leniently` (an `AgentEffect` whose target carries `"many"` converts with the default three; `"4"` converts with four).

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p rakka-agent-workflow --test effect_bridge attempts`
Expected: compile error on `max_attempts`; after a stub, the entry reports three.

- [ ] **Step 3: Implement**

As the Interfaces block says; extend the `rakka-agent` vocabulary test with the second constant.

- [ ] **Step 4: Run the tests and clippy**

Run: `cargo test -p rakka-agent-workflow && cargo test -p rakka-workflow && cargo test -p rakka-agent --lib attribute_vocabulary && cargo clippy -p rakka-agent-workflow -p rakka-workflow -p rakka-agent --all-targets --all-features -- -D warnings`
Expected: PASS and clean.

- [ ] **Step 5: Commit**

```bash
git add crates/rakka-agent-workflow/src/ crates/rakka-agent/src/effect.rs crates/rakka-agent-workflow/tests/
git commit -m "Carry a node's declared attempt bound into its outbox entry's retry policy (issue #110 optional ask)"
```

---

### Task 7: Keep `Cancelling` across a non-terminal graph update

**Files:**
- Modify: `crates/rakka-agent-workflow/src/runner.rs:914-917` (the `None` arm of `update_graph_state`)
- Test: `crates/rakka-agent-workflow/tests/step_runner.rs`

**Interfaces:**
- Produces: in the `None` arm, `next.status` is `Cancelling` when `previous_status == Cancelling`, else `Running`; terminal arms unchanged.

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn a_non_terminal_graph_update_keeps_a_requested_cancellation() {
    let mut runner = start_only(single_step_workflow(), AgentRunId::new("run-cancel-graph"), AgentStepId::new("plan"), TestStore::new()).await;
    runner.request_cancellation("parent-winding-down", None, ts(250)).await.unwrap();
    let graph = AgentGraphRunState::new(AgentCompiledPlanId::new("plan-x"), AgentCompiledPlanFingerprint::new("sha256:x"));
    let updated = runner.update_graph_state(graph.clone(), ts(260)).await.unwrap();
    assert_eq!(updated.next_status, AgentRunStatus::Cancelling);
    assert!(updated.state.cancellation.is_some());
    let mut terminal = graph; terminal.terminal_status = Some(AgentGraphTerminalStatus::Cancelled);
    assert_eq!(runner.update_graph_state(terminal, ts(270)).await.unwrap().next_status, AgentRunStatus::Cancelled);
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p rakka-agent-workflow --test step_runner keeps_a_requested_cancellation`
Expected: FAIL, `left: Running, right: Cancelling`.

- [ ] **Step 3: Implement the one-arm change**

- [ ] **Step 4: Run the crate's tests**

Run: `cargo test -p rakka-agent-workflow`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/rakka-agent-workflow/src/runner.rs crates/rakka-agent-workflow/tests/step_runner.rs
git commit -m "Keep a run Cancelling across a non-terminal graph update (issue #110 addendum)"
```

---

### Task 8: Docs, changelog, module docs

**Files:**
- Modify: `docs/rakka-compatibility.md` (the bullets after the schema table, `:200-209`)
- Modify: `CHANGELOG.md` (`### Added`, `### Changed`, `### Fixed`, `### Migration notes`)
- Modify: module docs at the top of `crates/rakka-agent-workflow/src/graph_state.rs`, `effect_bridge.rs`, `graph_scheduler.rs`

**Interfaces:**
- Consumes: every name and code from Tasks 1 to 7.

- [ ] **Step 1: Write the compatibility doc changes**

Under "The records above version 1": a bullet for the graph state at version 2 stating what the version carries (`Reconciliation` wait reason, `effect_generation`, generation-suffixed effect ids), that `initialize_state` stamps 2 and a version-1 state is raised to 2 at its first park or resume, that readers refuse a newer version with `graph-state-schema-unsupported`, the table of new values and who writes each (spec section 6), that only the wait reason is undecodable by an older binary and the command kind is an opaque payload, and the two-step rollout (read first, write later). The bridge's error codes are not tabled in the docs; the source-breaking additions (`AgentGraphWaitReason::Reconciliation`, `AgentGraphEffectFailureDisposition::Reconcile`, `AgentCommandKind::GraphReconciliationDecided`, `AgentGraphReconciliationDecision`, the two error variants), the new codes, the dispatcher contract (one `Reconcile`, never `Exhausted` first; the entry recorded exhausted before the command), and the recommended abandon code `reconciliation-abandoned` go in the changelog entries and the `effect_bridge` module doc.

- [ ] **Step 2: Write the changelog and module docs**

Added: the park, the decision command, generations and the safety-class rule, the attempt bound, the version gate, `outbox_effects`, `effect_from_node_in_state`, the two attribute constants, the snapshot counter. Changed: schema version 2, recovery re-links all graph effects, `accept_and_apply_effect_failed`'s precondition on `Reconcile`, the widened guards. Fixed: the `Cancelling` overwrite (addendum), recovery missing an effect exhausted during the crash window. Migration notes: the two-step rollout. Each entry names issue #110. One paragraph in each module doc.

- [ ] **Step 3: Run the doc-holding tests and the doc build**

Run: `cargo test -p rakka-testkit --test repository_hygiene && cargo test -p rakka-agent --features otel --test compatibility_currency && cargo doc --workspace --all-features --no-deps 2>&1 | tail -3`
Expected: PASS; the doc build is warning-free.

- [ ] **Step 4: Commit**

```bash
git add docs/rakka-compatibility.md CHANGELOG.md crates/rakka-agent-workflow/src/graph_state.rs crates/rakka-agent-workflow/src/effect_bridge.rs crates/rakka-agent-workflow/src/graph_scheduler.rs
git commit -m "Document the graph reconciliation park, its rollout, and the dispatcher contract (issue #110)"
```

---

### Task 9: Whole-workspace validation

**Files:** none new.

- [ ] **Step 1: Format and clippy**

Run: `cargo fmt --all -- --check && cargo clippy --workspace --all-targets --all-features -- -D warnings`
Expected: clean.

- [ ] **Step 2: Run the full validation entry point**

Run: `scripts/validate.sh > <workspace>/validate.log 2>&1; echo exit=$?` (capture the exit code directly). If the workspace test run is killed under load, run `cargo test -p <crate>` for `rakka-agent-workflow`, `rakka-workflow`, `rakka-agent`, `rakka-a2a`, and `rakka-testkit`, then re-run `validate.sh` once.
Expected: `exit=0`.

- [ ] **Step 3: Package check**

Run: `scripts/package-check.sh`
Expected: exit 0, nothing published.

- [ ] **Step 4: Fix anything that failed, re-run, commit the fixes**

```bash
git commit -am "Address validation findings"
```
