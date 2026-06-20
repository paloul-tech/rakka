# Phase 7.1 Failure-Injection Suite

Status: implemented.

This document maps Rakka's documented reliability guarantees and
non-guarantees to repeatable failure-injection tests. The goal is not to claim
exactly-once behavior. The goal is to prove where durable recovery works, where
fencing prevents stale writers, and where applications must provide
idempotency, reconciliation, or operator policy.

## Reliability Boundaries

The source reliability contract is `docs/rakka-v1-reliability-boundaries.md`.
For agent workflows, the important production boundary is:

- commands are safe only after durable inbox acceptance;
- side-effect intent is safe only after durable outbox scheduling;
- external side effects are not exactly once;
- timers and human waits resume through durable state and durable commands;
- dispatch leases and revision checks fence stale local writers;
- core actor, remote, and shard delivery remain at-most-once unless wrapped in
  durable workflow semantics.

## Failure Matrix

| Failure | Expected Result | Test Or Example |
| --- | --- | --- |
| Crash after inbox acceptance | Replayed start command deduplicates and accepted work remains recoverable. | `cargo test -p rakka-agent-workflow --test failure_injection crash_after_inbox_acceptance_and_effect_scheduling_recovers_durable_work` |
| Crash after effect scheduling | Scheduled effect is recovered from the durable outbox. | `cargo test -p rakka-agent-workflow --test failure_injection crash_after_inbox_acceptance_and_effect_scheduling_recovers_durable_work` |
| Crash after marking dispatching but before side effect returns | Expired dispatcher lease can be claimed by another worker and the dispatching outbox entry is recoverable. | `cargo test -p rakka-agent-workflow --test dispatcher_fleet expired_claim_after_dispatching_is_recoverable_by_another_worker` |
| Crash after side effect returns but before success persistence | Effect may be redelivered; downstream APIs need idempotency keys or reconciliation. | `cargo test -p rakka-agent-workflow --test failure_injection crash_after_external_result_before_success_persistence_redelivers_effect` |
| Human approval during crash | Open checkpoint survives runtime restart; later decision resumes once and duplicate decision deduplicates. | `cargo test -p rakka-agent-workflow --test failure_injection human_decision_after_checkpoint_runtime_restart_resumes_once` |
| Timer firing during crash | Fired timer state and timer command deduplication prevent double resume after scanner restart. | `cargo test -p rakka-agent-workflow --test failure_injection timer_firing_after_scanner_restart_does_not_resume_twice` |
| Pod drain timeout | Timed-out drain preserves a partial report and stops later phases under fail-fast policy. | `cargo test -p rakka-testkit --test phase7_operational_validation phase7_operational_validation_preserves_failure_policy_and_timeout_reports` |
| Lease loss | Dispatcher claim fencing prevents stale result persistence. | `cargo test -p rakka-agent-workflow --test failure_injection crash_after_external_result_before_success_persistence_redelivers_effect` |
| Stale coordinator writer | Coordinator compare-and-set rejects stale revisions. | `cargo test -p rakka-sharding --test sharding_foundation in_memory_coordinator_store_rejects_stale_revision` |
| PostgreSQL revision conflict | PostgreSQL durable state compare-and-set reports `revision-conflict`. | `RAKKA_POSTGRES_TEST_DSN=postgres://postgres:postgres@localhost:5432/postgres cargo test -p rakka-persistence-postgres -- --test-threads=1` |
| Remote delivery failure | Remote delivery fails closed for unknown peers, stale actor UIDs, wrong message types, missing codecs, and late replies. | `cargo test -p rakka-remote --test remote_boundary tcp_remote_clustered_receptionist_missing_peer_fails_closed` |
| Model provider timeout | Adapter timeout maps to durable outbox timeout result. | `cargo test -p rakka-agent-workflow --test failure_injection model_provider_timeout_maps_to_retryable_durable_outbox_timeout` |
| Tool process restart-budget exhaustion | Process actor enters failed state and records restart budget exhaustion. | `cargo test -p rakka-process --test process_lifecycle process_actor_restarts_unexpected_exit_until_budget_is_exhausted` |
| Shard handoff/passivation during active run | Sharded run recovers durable state after passivation and resumes lazily. | `cargo test -p rakka-agent-workflow --features sharding --test sharded_run sharded_run_routes_by_stable_run_id_and_recovers_after_passivation` |

## Repeatable Commands

Local deterministic suite:

```sh
cargo test -p rakka-agent-workflow --test failure_injection
cargo test -p rakka-agent-workflow --test dispatcher_fleet
cargo test -p rakka-testkit --test phase7_operational_validation
cargo test -p rakka-remote --test remote_boundary tcp_remote_clustered_receptionist_missing_peer_fails_closed
cargo test -p rakka-process --test process_lifecycle process_actor_restarts_unexpected_exit_until_budget_is_exhausted
cargo test -p rakka-sharding --test sharding_foundation in_memory_coordinator_store_rejects_stale_revision
```

Feature-gated sharding path:

```sh
cargo test -p rakka-agent-workflow --features sharding --test sharded_run sharded_run_routes_by_stable_run_id_and_recovers_after_passivation
```

Local PostgreSQL path, using the Docker container credentials already used in
Phase 5:

```sh
RAKKA_POSTGRES_TEST_DSN=postgres://postgres:postgres@localhost:5432/postgres \
  cargo test -p rakka-persistence-postgres -- --test-threads=1
```

Optional multi-process compatibility path:

```sh
RAKKA_RUN_MULTI_PROCESS_COMPATIBILITY=1 \
  cargo test -p rakka-testkit --test compatibility_matrix optional_multi_process_compatibility_example_is_gated -- --nocapture
```

## Production Interpretation

Passing these tests means the durable boundaries are test-backed. It does not
remove the need for:

- downstream idempotency keys for external effects;
- reconciliation for provider callbacks or ambiguous external outcomes;
- retry and compensation policy per workflow;
- CNI-backed NetworkPolicy enforcement;
- production PostgreSQL backup/restore testing;
- load and cardinality testing in Slice 7.2.
