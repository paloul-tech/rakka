# Phase 7.3 API Review and Compatibility

Status: implemented.

This document records the Slice 7.3 API review for `rakka-agent-workflow` and
the compatibility checks that back rolling-update expectations. The goal is to
make additive changes easy to identify, and breaking changes explicit enough
to require schema/version policy before production rollout.

## Reviewed API Surface

The public root re-exports in `rakka-agent-workflow` cover the stable surfaces
applications need to build durable agent workflows:

- workflow definitions, registry keys, payload descriptors, run state, steps,
  commands, effects, timers, checkpoints, artifacts, audit records, and
  telemetry context;
- durable inbox/outbox facades, step runner, actor-backed runtime, dispatcher
  fleet, timer scanner, human checkpoint runtime, retention, migration, query
  index, snapshots, metrics, trace context, adapters, and OTLP helpers;
- feature-gated HTTP, gRPC, Kubernetes, PostgreSQL, process-tool, sharding,
  and testkit integrations.

The default feature set remains empty. Optional integrations stay additive and
feature-gated so core workflow contracts can compile without Kubernetes,
PostgreSQL, sharding, process tools, or HTTP/gRPC adapters.

## Compatibility Rules

Additive changes:

- adding optional fields to JSON-serialized command, effect, trace, audit,
  state, or projection records;
- adding new optional feature flags;
- adding new bounded metric labels only when they pass the agent metric
  cardinality policy;
- adding new query filters that preserve existing result ordering and limit
  behavior;
- adding new Kubernetes environment variables or annotations while preserving
  existing compatibility keys.

Breaking changes:

- renaming or removing required command/effect/state fields such as
  `command_id`, `idempotency_key`, `state_schema_version`, or
  `definition_version`;
- changing first-class command/effect enum wire names;
- changing durable inbox/outbox message ids, deduplication keys, idempotency
  keys, or trace context field names;
- removing public root re-exports that examples or applications use;
- changing query projection semantics without a migration/backfill policy;
- changing Kubernetes compatibility annotations, ConfigMap keys, required
  startup services, or rolling-update behavior without a manifest version bump.

Durable run-state and query-index changes should use the existing N/N+1
`AgentWorkflowMigrationPolicy`. Current binaries accept current and previous
state/index schema versions, reject too-old or ahead versions, and can restrict
workflow definition versions during a deployment.

## Implemented Compatibility Matrix

| Area | Compatibility Expectation | Test |
| --- | --- | --- |
| Root public API | Default public agent workflow types and validation helpers are re-exported from the crate root. | `cargo test -p rakka-agent-workflow --test api_compatibility public_root_exports_cover_stable_default_api_surface` |
| Kubernetes public API | Kubernetes startup/readiness API is root-exported when the `k8s` feature is enabled. | `cargo test -p rakka-agent-workflow --all-features --test api_compatibility public_root_exports_cover_kubernetes_api_surface_when_enabled` |
| Commands | Unknown additive fields deserialize; removing required command metadata is breaking. | `cargo test -p rakka-agent-workflow --test api_compatibility command_and_effect_wire_contract_accepts_additive_fields_and_rejects_breaking_removals` |
| Effects | Unknown additive fields deserialize; removing downstream idempotency metadata is breaking. | `cargo test -p rakka-agent-workflow --test api_compatibility command_and_effect_wire_contract_accepts_additive_fields_and_rejects_breaking_removals` |
| Durable state | Current and previous schema versions are compatible through N/N+1 policy; missing schema metadata is breaking. | `cargo test -p rakka-agent-workflow --test api_compatibility durable_state_trace_and_query_contracts_are_versioned_for_rolling_updates` |
| Trace context | Persisted telemetry context accepts additive fields and still validates W3C trace metadata and span links. | `cargo test -p rakka-agent-workflow --test api_compatibility durable_state_trace_and_query_contracts_are_versioned_for_rolling_updates` |
| Query indexes | Run, timer, and dispatch projection records remain queryable with bounded query APIs. | `cargo test -p rakka-agent-workflow --test api_compatibility query_index_compatibility_accepts_additive_projection_shapes` |
| Feature flags | Optional integrations remain additive and default features remain empty. | `cargo test -p rakka-agent-workflow --test api_compatibility feature_flags_are_additive_and_match_api_review_boundaries` |
| Kubernetes manifests | Reference topology carries state/index schema versions, definition versions, N/N+1 policy, manifest/API/protocol versions, required services, and rolling-update settings. | `cargo test -p rakka-agent-workflow --test api_compatibility kubernetes_reference_manifest_carries_agent_workflow_compatibility_contract` |

Related existing coverage:

```sh
cargo test -p rakka-agent-workflow --test migration_backfill
cargo test -p rakka-testkit --test compatibility_matrix
```

## Versioning Notes

- `StateSchemaVersion` gates durable run-state readers.
- `WorkflowDefinitionVersion` gates workflow definition selection.
- `AgentWorkflowIndexSchemaVersion` gates query projection schema and repair.
- Kubernetes manifests carry `RAKKA_AGENT_WORKFLOW_CURRENT_STATE_SCHEMA_VERSION`,
  `RAKKA_AGENT_WORKFLOW_CURRENT_INDEX_SCHEMA_VERSION`,
  `RAKKA_AGENT_WORKFLOW_COMPAT_POLICY`, and
  `RAKKA_AGENT_WORKFLOW_EXPECTED_DEFINITION_VERSIONS`.
- Cluster protocol and generated API compatibility remain covered by the
  repository-wide V1 compatibility matrix in `rakka-testkit`.

## Production Interpretation

Passing Slice 7.3 means local API and wire-contract compatibility expectations
are test-backed. Before a production release, each breaking candidate should be
classified in release notes, tied to a schema or manifest version bump, and
validated in a rolling-update rehearsal against Kubernetes, PostgreSQL, and the
OpenTelemetry Collector topology.
