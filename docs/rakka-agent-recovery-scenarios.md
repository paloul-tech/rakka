# Agent Domain Recovery Scenario Roster

Status: implemented (slice 6.4).

[Specification 18](plans/rakka-agent/spec.md) names sixty-one recovery
scenarios the agent domain is not production-ready without. This document
binds each one to the test that proves it, at the fidelity the proof runs at.
It is the artifact behind slice 6.4's done-when — "every shipped-phase
scenario passes under in-process fault injection; every scenario the
fault-injection matrix names as requiring multi-pod fidelity passes there
too" — stated as a table so that it can be checked rather than believed.

The table is held to the tree by
`cargo test -p rakka-agent --test recovery_scenario_roster`: the rows number
exactly the scenarios the specification lists, each row's milestone is the one
the specification binds, every cited file exists and cites the scenario it is
rostered for, the multi-pod rows and only they cite the multi-pod harness, and
the multi-pod set agrees with the
[fault-injection matrix](rakka-agent-fault-injection-matrix.md), which remains
the authority for *why* a scenario needs that fidelity. In the other direction,
a test file whose module doc cites a scenario must appear in that scenario's
row, so a proof cannot exist unrostered.

## Reading the table

- **Fidelity** is `in-process` (an owner loss is a `CrashingStateStore` error
  and a rebuilt facade over the same in-memory store, which is every call for a
  sharded entity), `multi-pod` (a real `abort()` in a real OS process whose
  durable store is a directory outside it, gated by
  `RAKKA_RUN_MULTI_PROCESS_COMPATIBILITY=1`), or both. The two fidelities and
  what each can reach are explained in the fault-injection matrix.
- **Proof** cites repo-relative test files. A file is a proof of a row when its
  body exercises the claim; the module doc names the scenario, and the
  roster test checks the citation both ways. A cited file may hold more than
  one row's proof, and a row may need more than one file.
- **Claim** paraphrases the specification's sentence; the specification's
  wording governs.

## Roster

| # | Milestone | Fidelity | Proof | Claim |
| --- | --- | --- | --- | --- |
| 1 | M1 | in-process + multi-pod | `crates/rakka-a2a/tests/agents_surface.rs`, `examples/multi-pod-agent-fault-soak/src/lib.rs` | Duplicate A2A task message acceptance creates no second task, initial run, or turn. The multi-pod harness re-proves the creation-deduplication half: both pods seed every run and exactly one agent and one task exist. |
| 2 | M1 | in-process + multi-pod | `crates/rakka-agent/tests/run_entity.rs`, `examples/multi-pod-agent-fault-soak/src/lib.rs` | Agent/task/run restart after each loop transition resumes correctly; at multi-pod fidelity the restart is a pod loss and the resume happens on the surviving pod. |
| 3 | M1 | in-process | `crates/rakka-agent/tests/checkpoints.rs`, `crates/rakka-agent/tests/checkpoint_run.rs`, `crates/rakka-agent/tests/checkpoint_reconciliation.rs` | Passivation during approval, authorization, timer, and reconciliation waits consumes no live execution task and resumes on the next command. |
| 4 | M1 | in-process | `crates/rakka-agent/tests/stale_owner_fencing.rs` | Shard movement rejects stale owner state writes. |
| 5 | M1 | in-process | `crates/rakka-agent/tests/effect_dispatch.rs` | Dispatcher loss before durable `Started` safely redispatches. |
| 6 | M1 | in-process | `crates/rakka-agent/tests/effect_dispatch.rs` | Dispatcher loss after `Started` retries a read-only effect under policy. |
| 7 | M1 | in-process | `crates/rakka-agent/tests/effect_dispatch.rs`, `crates/rakka-agent/tests/tool_authority.rs` | Dispatcher loss after `Started` reuses the same idempotency key for an idempotent effect. |
| 8 | M1 | in-process | `crates/rakka-agent/tests/effect_dispatch.rs` | Dispatcher loss after `Started` reconciles a reconcileable effect before any retry. |
| 9 | M1 | in-process | `crates/rakka-agent/tests/effect_dispatch.rs`, `crates/rakka-agent/tests/tool_authority.rs` | Dispatcher loss in the ambiguous non-idempotent window produces exactly one durable `Indeterminate` outcome and no automatic re-invocation. |
| 10 | M1 | in-process | `crates/rakka-agent/tests/effect_dispatch.rs`, `crates/rakka-agent/tests/run_entity.rs` | Duplicate or stale tool/model completions do not advance twice. |
| 11 | M1 | in-process | `crates/rakka-agent/tests/checkpoints.rs`, `crates/rakka-agent/tests/checkpoint_run.rs`, `crates/rakka-agent/tests/checkpoint_reconciliation.rs` | Duplicate human/authorization decisions do not resume twice. |
| 12 | M1 | in-process | `crates/rakka-agent/tests/checkpoints.rs` | A changed effect digest invalidates an old approval: a grant is bound to the exact intent's digest. |
| 13 | M1 | in-process | `crates/rakka-agent/tests/tool_authority.rs`, `crates/rakka-agent/tests/wait_invalidation.rs` | Immediate capability or credential revocation prevents later dispatch, including a revocation injected while the run waits and swept under owner loss. |
| 14 | M1 | in-process | `crates/rakka-agent/tests/memory_store_contract.rs`, `crates/rakka-agent/tests/session_memory.rs`, `crates/rakka-agent-postgres/tests/memory_conformance.rs` | Short-term memory is isolated by both `AgentId` and `AgentRunId`, on every backend through one conformance suite. |
| 15 | M2 | in-process | `crates/rakka-agent/tests/private_memory_promotion.rs`, `crates/rakka-agent/tests/memory_store_contract.rs`, `crates/rakka-agent-postgres/tests/memory_conformance.rs` | Concurrent runs append private memory without stale overwrite. |
| 16 | M2 | in-process | `crates/rakka-agent/tests/private_memory_promotion.rs`, `crates/rakka-agent/tests/session_memory.rs`, `crates/rakka-agent/tests/memory_store_contract.rs`, `crates/rakka-agent-postgres/tests/memory_conformance.rs`, `crates/rakka-agent-knowledge-graph/tests/knowledge_graph_conformance.rs` | Replayed memory and graph writes are idempotent. |
| 17 | M1 | in-process | `crates/rakka-agent/tests/session_memory.rs`, `crates/rakka-agent/tests/private_memory_retrieval.rs`, `crates/rakka-agent/tests/memory_retention.rs` | A model-effect retry uses the original memory context snapshot. |
| 18 | M2 | in-process | `crates/rakka-agent/tests/private_memory_promotion.rs`, `crates/rakka-agent/tests/memory_store_contract.rs`, `crates/rakka-agent/tests/memory_scope_fence.rs`, `crates/rakka-agent/tests/tenant_isolation.rs`, `crates/rakka-agent/tests/private_memory_retrieval.rs`, `crates/rakka-agent-knowledge-graph/tests/knowledge_graph_conformance.rs` | Unauthorized graph/private-memory reads do not reveal existence. |
| 19 | M1 | in-process | `crates/rakka-agent/tests/terminal_run_recovery.rs` | Terminal run recovery does not reschedule completed effects. |
| 20 | M2 | in-process | `crates/rakka-agent-knowledge-graph/tests/knowledge_graph_conformance.rs`, `crates/rakka-agent-knowledge-graph-postgres/tests/postgres_conformance.rs` | Every communal graph backend passes the same conformance suite without changing agent-domain code. The PostgreSQL arm is gated on `RAKKA_POSTGRES_TEST_DSN`. |
| 21 | M1 | in-process | `crates/rakka-agent/tests/operational_query.rs`, `crates/rakka-agent/tests/decision_events.rs` | Ingress, decisions, model calls, effect scheduling, dispatcher attempts, tool calls, waits, recovery, and terminal outcomes are reconstructable as one authorized session view by `AgentRunId`. |
| 22 | M1 | in-process | `crates/rakka-agent/tests/trace_scenarios.rs`, `crates/rakka-agent/tests/goal_passivation.rs` | Passivation and long waits leave no open in-memory span, and resume spans link to both the parked and the triggering operations. |
| 23 | M1 | in-process | `crates/rakka-agent/tests/telemetry_context.rs`, `crates/rakka-agent/tests/trace_scenarios.rs`, `crates/rakka-agent/tests/effect_dispatch.rs` | Trace context and causal links survive dispatcher restart, owner loss, and shard movement without changing effect behavior. |
| 24 | M1 | in-process | `crates/rakka-agent/tests/trace_scenarios.rs` | Trace sampling does not change metrics, audit records, runtime-event acceptance, or durable execution. |
| 25 | M1 | in-process | `crates/rakka-agent/tests/secret_exclusion.rs`, `crates/rakka-agent/tests/trace_scenarios.rs`, `crates/rakka-agent/tests/otel_span_mapping.rs`, `crates/rakka-agent/tests/agent_metrics.rs`, `examples/durable-agent-acceptance/tests/acceptance.rs`, `examples/agent-otlp-export-acceptance/src/flow.rs` | Default telemetry carries no prompt, completion, hidden reasoning, tool payload, memory content, or credential material — re-proven on the decoded OTLP wire. |
| 26 | M1 | in-process | `crates/rakka-agent/tests/trace_scenarios.rs`, `examples/agent-otlp-export-acceptance/tests/exporter_failure.rs` | An unavailable Collector/exporter path blocks no correctness and produces bounded queue/drop/failure visibility — proven against a real exporter and a socket nothing answers. |
| 27 | M4 | in-process | `crates/rakka-agent/tests/fan_out_fan_in.rs`, `crates/rakka-agent/tests/fan_in_recovery.rs` | A root run durably fans out to specialists, passivates, and deterministically resumes and fans in after restart or shard movement. |
| 28 | M4 | in-process | `crates/rakka-a2a/tests/collaboration_surface.rs`, `crates/rakka-agent/tests/delegation_record.rs`, `crates/rakka-agent/tests/fan_in_recovery.rs` | Replaying a delegation command or A2A send creates exactly one logical child task/run or an explicit conflict. |
| 29 | M4 | in-process | `crates/rakka-agent/tests/cancellation_propagation.rs`, `crates/rakka-agent/tests/cancellation_recovery.rs`, `examples/multi-agent-goal-acceptance/src/lib.rs` | Root, parent, or dispatcher crashes do not replay a child's opaque non-idempotent effect; ambiguity stays indeterminate in the child. |
| 30 | M4 | in-process | `crates/rakka-agent/tests/goal_evaluation.rs` | A root goal becomes `Satisfied` only after the current success-criteria revision is evaluated against durable evidence. |
| 31 | M4 | in-process | `crates/rakka-agent/tests/cancellation_propagation.rs`, `crates/rakka-agent/tests/cancellation_recovery.rs` | Cancellation, deadline, and immediate revocation propagate durably to children without falsely claiming their started effects stopped. |
| 32 | M4 | in-process | `crates/rakka-agent/tests/workflow_tool.rs` | Replaying a workflow-tool invocation creates or adopts one durable child workflow run and duplicates none of its internal effects. |
| 33 | M4 | in-process | `crates/rakka-agent/tests/communal_claim_append.rs`, `crates/rakka-agent-knowledge-graph/tests/claim_append_executor.rs`, `crates/rakka-agent-knowledge-graph-postgres/tests/postgres_backend_proofs.rs` | Concurrent specialist appends to communal memory retain goal/task/run/delegation provenance and stable append idempotency. |
| 34 | M4 | in-process | `crates/rakka-agent/tests/delegation_limits.rs`, `crates/rakka-agent/tests/fan_in_recovery.rs`, `crates/rakka-agent/tests/cancellation_recovery.rs` | Depth, fan-out, descendant, concurrency, budget, and cycle limits fail closed and recover after coordinator loss, including a loss inside the compare-and-set that spends the ceiling. |
| 35 | M1 | in-process | `crates/rakka-agent/tests/goal_passivation.rs` | An `Active` goal and its waiting runs all passivate with nothing resident, and one durable trigger reactivates the correct owner exactly once. |
| 36 | M3 | in-process | `crates/rakka-agent/tests/epoch_lifecycle.rs` | A continuous goal completes one bounded epoch, persists its next durable wake condition, passivates, and resumes without an immortal poller. |
| 37 | M1 | in-process | `crates/rakka-agent/tests/task_entity.rs` | Replaying typed task creation/dependency/assignment commands yields one `AgentTaskId`, one dependency edge, and one current assignment. |
| 38 | M5 | in-process | `crates/rakka-a2a/tests/handoff_surface.rs`, `crates/rakka-agent/tests/handoff_record.rs`, `crates/rakka-agent/tests/handoff_recovery.rs`, `crates/rakka-agent/tests/handoff_cancellation.rs` | Handoff preserves `AgentTaskId`, terminates and fences the source run, creates one target `AgentRunId`, and exposes no source session/private memory. |
| 39 | M4 | in-process | `crates/rakka-a2a/tests/collaboration_surface.rs`, `crates/rakka-agent/tests/delegation_record.rs` | Delegation creates exactly one child `AgentTaskId`/`AgentRunId` while the parent task's identity and ownership stay unchanged. |
| 40 | M1 | in-process | `crates/rakka-agent/tests/task_results.rs` | A malformed or rule-rejected task result never completes the task, persists one rejection decision, and consumes only bounded additional iterations. |
| 41 | M5 | in-process | `crates/rakka-a2a/tests/human_task_surface.rs`, `crates/rakka-agent/tests/human_owned_tasks.rs` | A human-owned typed task unblocks dependents after authenticated, deduplicated completion; a failed one propagates its declared dependency policy. |
| 42 | M5 | in-process | `crates/rakka-a2a/tests/team_surface.rs`, `crates/rakka-agent/tests/team_board.rs`, `crates/rakka-agent/tests/team_claim_assignment.rs`, `crates/rakka-agent/tests/team_claim_recovery.rs`, `crates/rakka-agent/tests/task_unclaimed_expiry.rs`, `crates/rakka-agent/tests/team_passivation.rs` | Concurrent team members atomically claim a task so only one owner may schedule effects; stale claim/release/transfer commands fail closed. |
| 43 | M5 | in-process | `crates/rakka-a2a/tests/conversation_surface.rs`, `crates/rakka-agent/tests/conversation_protocol.rs`, `crates/rakka-agent/tests/conversation_turns.rs`, `crates/rakka-agent/tests/conversation_recovery.rs`, `crates/rakka-agent/tests/conversation_passivation.rs` | Moderation recovers participants, round, turn owner, transcript reference, and budgets after passivation or shard movement without duplicating a turn. |
| 44 | M1 | in-process | `crates/rakka-agent/tests/tool_authority.rs`, `crates/rakka-agent/tests/memory_guardrail_chain_consistency.rs` | Per-run setup/settings cannot add an undeclared tool/peer/model, widen authorization, or weaken a mandatory guardrail. |
| 45 | M5 | in-process | `crates/rakka-a2a/tests/coordination_surface.rs`, `crates/rakka-agent/tests/coordination_replay.rs` | Task/run/coordination event replay resumes from a cursor or answers an explicit retention-gap/resync response. |
| 46 | M1 | in-process | `crates/rakka-agent/tests/idle_agent_reactivation.rs` | An idle agent with assigned/blocked future tasks auto-passivates without `terminate` or `suspend` and reactivates when work becomes eligible. |
| 47 | M3 | in-process | `crates/rakka-agent/tests/wake_scanner.rs`, `crates/rakka-agent/tests/wake_sharding.rs` | Start, restart, rollout, and shard movement create no continuous epoch unless a durable wake is independently due and accepted. |
| 48 | M3 | in-process | `crates/rakka-agent/tests/wake_scanner.rs`, `crates/rakka-agent/tests/wake_coalescing.rs`, `crates/rakka-agent/tests/epoch_admission.rs` | Duplicate timer scans, events, callbacks, or A2A trigger delivery resolve to one `AgentWakeId` and at most one child epoch task/run. |
| 49 | M3 | in-process | `crates/rakka-agent/tests/wake_fencing.rs` | An obsolete schedule revision cannot admit an epoch after an update, and a restart resets neither the revision nor the missed-occurrence policy. |
| 50 | M3 | in-process | `crates/rakka-agent/tests/wake_coalescing.rs` | The default overlap policy coalesces concurrent triggers while exactly one epoch owns execution; the default downtime policy admits at most one coalesced epoch. |
| 51 | M3 | in-process | `crates/rakka-agent/tests/epoch_memory.rs` | Continuous epochs use distinct finite task/run short-term-memory scopes and recover cross-epoch continuity only from authorized shared state. |
| 52 | M1 | in-process | `crates/rakka-agent/tests/escrow_ledger.rs`, `crates/rakka-agent/tests/goal_budget.rs` | Budget allocation/reservation/settlement survives restart and concurrency without oversubscription; a `Started` attempt that becomes `Indeterminate` still consumes its attempt budget. |
| 53 | M1 | in-process | `crates/rakka-agent/tests/autonomy_admission.rs`, `crates/rakka-agent/tests/wait_invalidation.rs`, `crates/rakka-agent/tests/tool_authority.rs` | Unattended execution fails closed when admission is missing or expired, or a settings update widens an unadmitted scope. |
| 54 | M1 | in-process | `crates/rakka-agent/tests/tool_authority.rs`, `crates/rakka-agent/tests/executor_isolation.rs` | A model-visible tool call stays undispatchable when its binding, grant, credential, checkpoint, execution-policy, or immediate safety check fails. |
| 55 | M1 | in-process | `crates/rakka-agent/tests/task_bounded_state.rs` | Bounded task materialized state stays within configured limits; older history and content are reachable only through authorized cursors or artifact references. |
| 56 | M1 | in-process | `crates/rakka-agent/tests/operational_query.rs` | Authoritative lifecycle/wait/wake/budget/effect/cancellation queries stay correct when telemetry is sampled, delayed, dropped, or unavailable. |
| 57 | M1 | in-process | `crates/rakka-agent/tests/effect_dispatch.rs`, `crates/rakka-agent/tests/checkpoints.rs`, `crates/rakka-agent/tests/checkpoint_reconciliation.rs`, `crates/rakka-agent/tests/operational_query.rs` | Cancellation with an ambiguous consequential effect fences all new work, stays nonterminal in reconciliation, and projects terminal cancellation only after the outcome is explicitly resolved. |
| 58 | M1 | in-process | `crates/rakka-agent/tests/choreography.rs` | Replaying any section 9.8 inter-entity exchange produces one logical transition per operation id on both entities. |
| 59 | M1 | in-process | `crates/rakka-agent/tests/run_result_exchange.rs`, `crates/rakka-agent/tests/task_results.rs`, `crates/rakka-agent/tests/choreography.rs` | Loss of the run or task at any point in the result exchange converges without a second validation, a duplicate completion, or a lost rejection. |
| 60 | M1 | in-process + multi-pod | `crates/rakka-agent/tests/choreography_cluster.rs`, `crates/rakka-agent/tests/stale_owner_fencing.rs`, `examples/multi-pod-agent-fault-soak/src/lib.rs` | Cross-entity commands between colocated entities traverse durable outbox/inbox acceptance and stay correct after the entities move to different nodes. The in-process arm needs a loopback bind and skips itself where the sandbox refuses one; the multi-pod arm is where the movement is a real pod loss. |
| 61 | M1 | in-process | `crates/rakka-agent/tests/escrow_ledger.rs`, `crates/rakka-agent/tests/choreography.rs` | Dispatch-time budget reservation touches only the run's own ledger; replaying an allocation, settlement, or return never double-debits or double-credits a parent scope. |

## What the roster does not claim

A row says a test exists whose body exercises the claim and whose module doc
names the scenario. It does not say the test is exhaustive over every durable
write the claim could be killed at — that is what the fault-injection matrix's
sweep rows say, per boundary, and a scenario can be rostered here while its
sweep is recorded there as inferred. The
[security](rakka-agent-security-validation-matrix.md) and
[telemetry](rakka-agent-telemetry-validation-matrix.md) matrices record, for
scenarios 18, 25, 26, 44, 53, and 54, which of their clauses are enforced,
delegated to the deployment, or still inferred.

## Repeatable commands

```sh
cargo test -p rakka-agent --test recovery_scenario_roster
cargo test -p rakka-agent --all-features
cargo test -p rakka-a2a --all-features
cargo test -p rakka-agent-knowledge-graph
RAKKA_POSTGRES_TEST_DSN=postgres://postgres:postgres@localhost:5432/postgres cargo test -p rakka-agent-postgres
RAKKA_POSTGRES_TEST_DSN=postgres://postgres:postgres@localhost:5432/postgres cargo test -p rakka-agent-knowledge-graph-postgres
```

The multi-pod rows:

```sh
RAKKA_RUN_MULTI_PROCESS_COMPATIBILITY=1 \
  cargo test -p rakka-testkit --test compatibility_matrix -- --nocapture
```
