# Agentic Workflow Phase 0.1 API Boundary

Status: implemented
Date: 2026-06-18
Plan slice: `docs/plans/agentic-workflow/agentic-workflow-implementation-plan.md`

## Decision

Agentic workflow implementation starts in a new additive crate:
`rakka-agent-workflow`.

The crate is a facade over the existing `rakka-workflow` durable inbox/outbox
substrate. It does not move, fork, or redefine the lower-level workflow
reliability model.

The top-level `rakka` crate exposes the new facade behind the optional
`agent-workflow` feature as `rakka::agent_workflow`.

## Rationale

The agentic workflow spec combines orchestration, human checkpoints, timers,
dispatcher fleets, model/tool adapters, audit, OpenTelemetry, query indexes,
retention, and Kubernetes deployment. Keeping that surface in its own crate
prevents the lower-level `rakka-workflow` crate from becoming both a reliability
substrate and a full agent orchestration runtime.

This split preserves the existing boundary:

- `rakka-workflow` owns durable inbox/outbox primitives, retry policy,
  deduplication, workflow state, clocks, and recovery helpers.
- `rakka-agent-workflow` will own first-class agent concepts such as runs,
  steps, effects, human checkpoints, telemetry context, audit events,
  dispatcher orchestration, model/tool adapter traits, and Kubernetes-scale
  helpers.
- `rakka` owns the curated application-facing facade and re-export.

## Initial Crate Map

`crates/rakka-agent-workflow`

- Purpose: first-class agent workflow orchestration facade.
- Current state: thin boundary crate for Phase 0.1.
- Depends on: `rakka-workflow`.
- Optional integration features: `http`, `grpc`, `k8s`, `process-tools`,
  `postgres`, `otel`, and `testkit`.

`crates/rakka-workflow`

- Purpose: durable workflow reliability substrate.
- Owns durable inbox/outbox behavior, command/effect persistence, retry state,
  deduplication, clocks, and telemetry events.
- Should remain usable without agent concepts.

`crates/rakka`

- Purpose: top-level facade.
- Adds optional `agent-workflow` feature.
- Re-exports the new crate as `rakka::agent_workflow` when that feature is
  enabled.

## Planned Module Map

The new crate starts with only a documented substrate re-export module. Future
phases should add modules in this general shape:

- `domain`: `AgentWorkflow`, `AgentRun`, `AgentStep`, `AgentEffect`,
  `HumanCheckpoint`, `ArtifactRef`, status enums, and id types.
- `commands`: public command metadata and validation.
- `runtime`: local runner, actor-backed runner, sharded run integration, and
  recovery.
- `dispatch`: outbox dispatcher fleet, effect claiming, leases, target
  concurrency, and retry classification.
- `timers`: durable timer entries, scanner, and `TimerFired` injection.
- `human`: checkpoint state, approval submission, timeout, and escalation.
- `adapters`: model, tool, process, HTTP, gRPC, notification, and artifact
  adapter traits.
- `telemetry`: OpenTelemetry context, metric instruments, span links,
  structured logs, and audit correlation.
- `query`: operational query indexes and retention/compaction interfaces.
- `kubernetes`: startup, readiness, drain, autoscaling, and deployment helpers.
- `testkit`: deterministic fixtures and assertions.

The module map is intentionally not materialized yet. Slice 0.2 should define
the durable domain data contracts first.

## Substrate Types That Stay in `rakka-workflow`

These types remain lower-level substrate API, not agent-domain API:

- `DurableInbox`
- `InboxCommand`
- `InboxAcceptance`
- `InboxEntry`
- `InboxStatus`
- `OutboxCommand`
- `OutboxAcceptance`
- `OutboxEntry`
- `OutboxStatus`
- `OutboxDispatcher`
- `OutboxDispatchFuture`
- `OutboxDispatchResult`
- `OutboxFailureTransition`
- `OutboxTarget`
- `WorkflowState`
- `WorkflowId`
- `WorkflowMessageId`
- `OutboxMessageId`
- `DeduplicationKey`
- `RetryPolicy`
- `RetryAttempt`
- `RetryJitter`
- `WorkflowClock`
- `SystemWorkflowClock`
- `ManualWorkflowClock`
- `WorkflowTimestamp`
- `WorkflowTelemetryEvent`
- `WorkflowError`
- `WorkflowResult`

The agent facade may wrap or compose these types, but it should not require
`rakka-workflow` to know about agents, models, tools, humans, Kubernetes, or
OpenTelemetry exporters.

## Acceptance Notes

- The workspace now has a dedicated `rakka-agent-workflow` crate.
- The top-level facade exposes the crate behind an optional feature.
- The new crate is additive and does not change `rakka-workflow` runtime
  behavior or reliability semantics.
