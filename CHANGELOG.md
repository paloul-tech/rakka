# Changelog

All notable Rakka changes should be recorded here before a release candidate is cut.

The format follows Keep a Changelog style sections, and versioning is expected to follow SemVer once v1 release candidates begin.

## Unreleased

### Added

- V1 hardening foundations for TCP remoting, cluster runtime integration, compatibility matrix tests, generated HTTP/gRPC contract examples, observability exporters, Kubernetes local-cluster scenarios, security defaults, and repository validation scripts.
- `scripts/validate.sh` as the required local validation entry point.
- `scripts/package-check.sh` as the offline package metadata and publishability check entry point.
- V1 release-candidate review docs for reliability boundaries, N/N+1 rolling updates, known limitations, post-v1 roadmap, and final review checklist.
- Phase 5 agent workflow autonomy APIs: effect target catalog and policy validation, A2A peer adapter contracts, dispatcher registry routing, A2A peer/webhook/push dispatch classes, and deterministic dispatcher cancellation marking.
- `rakka-agent`: the crate that will own the durable agent domain — goal, typed task, run, evaluation, handoff, delegation, team, moderation, and workflow-tool contracts, the durable loop runtime, the provider-neutral model adapter trait, the continuous wake controller, the escrow budget ledger, autonomy admission, guardrails, checkpoints, tool authority, memory traits, structured telemetry, bounded operational queries, and deterministic test support. The first release landed the crate scaffolding: the documented module map, the `rig` (default) and `otel` feature gates, and the `agent`, `agent-rig`, and `agent-otel` facade features. The crate builds and tests with `--no-default-features`, which `scripts/validate.sh` now enforces.
- `rakka-agent` identity, definition, and agent-entity contracts:
  - Tenant-scoped identities (`AgentId`, `AgentGoalId`, `AgentTaskId`, `AgentRunId`, `AgentDelegationId`, `AgentWakeId`, `AgentEnvironmentRef`, `KnowledgeSpaceId`) that stay distinct types even where their values coincide, plus the composite scope keys (`AgentScope`, `AgentTaskScope`, `AgentRunScope`) that address the sharded entities. Identifier values are validated and fail closed on deserialization so that flattening a composite scope into an entity or persistence id stays injective and cannot alias two tenants onto one durable record. `AgentRunBinding` fixes a run's task at construction; there is no setter to re-target it.
  - `AgentOperationId` and `AgentOperationKind`: derived — not generated — stable operation and deduplication identifiers, convertible to the substrate's `AgentDeduplicationKey` and `AgentCommandId` so the durable inbox and outbox deduplicate on the same value the agent domain reasons about.
  - `AgentDefinitionRevision`, `SettingsRevision` with the turn-bound / immediate-safety / run-pinned timing classes, and `AgentSetupRevision`. `AgentAuthorityEnvelope` carries the definition's authority in one place, and a setup or settings revision may only narrow it: introducing an undeclared tool, weakening a mandatory guardrail, choosing an unapproved model, widening credential/knowledge/environment access, adding an unauthorized peer, downgrading a tool's effect safety, rerouting a tool through a different execution policy (opaque policies cannot be ranked, so the declared routing is pinned), or raising a budget ceiling is rejected with a stable reason code. Definition content is validated at construction, on deserialization, and again at the entity's accept path, so an out-of-bounds description can neither cross the wire nor be persisted, and a settings revision's change list is bounded on the load path as well. `effective_settings_for_turn` resolves a run's pinned revision against the agent's current one.
  - Sharded `AgentEntity`, keyed `(TenantId, AgentId)`, owning the durable definition, lifecycle status (`Active`/`Suspended`/`Terminated`), current settings revision, policy and logical credential-binding references, and the agent-private memory namespace. Its `AgentEntityCommand`/`AgentEntityReply` protocol is serializable from the first commit and routable over `rakka-remote` via `init_agent_entity_remote_sharding`. Administrative commands deduplicate on their operation id and return the original outcome on replay; settings updates are fenced on the settings revision they expect to succeed, and suspend/resume/terminate on the monotonic lifecycle revision they expect to advance, so a replay that has aged out of the deduplication window cannot reorder over a decision its initiator never saw. Instantiation takes the definition content and publishes the initial revision itself, so a foreign revision number or schema version cannot enter the first durable record. `load_agent_entity_state` is the durable read path that keeps run creation out of the entity's mailbox.
  - Schema versions on every persisted record, with a default N/N+1 compatibility policy (`AgentSchemaPolicy`). A record written by a newer binary, or one older than the supported window, fails closed on load rather than being interpreted with guessed semantics.
  - No resolved credential material appears in durable state, snapshots, or replies; credential bindings are logical references only.
- `rakka-agent` inter-entity choreography substrate:
  - `AgentExchangeJournal`, the durable saga record every choreography participant embeds in its own state. It is the entity's outbox and inbox at once: the exchanges it owes, the operations it has settled, and the operations it has applied together with the logical result each returned. Because it lives inside the participant's state record, an exchange is persisted in the *same* compare-and-set as the domain transition that owed it, and a receiver's decision is persisted in the same compare-and-set as the transition that produced it — so there is no window in which an entity has transitioned but forgotten what it owes, and none in which it has decided but not recorded the decision.
  - `AgentExchangeHost`, the durable host that owns recovery, the fail-closed schema check, deduplication, and the compare-and-set write of every transition. Participants supply only bounded in-memory transitions through `AgentExchangeParticipant`, and an accepted exchange may owe the next one atomically, which is what makes the canonical creation → assignment → run-acceptance chain of specification 9.8 unbreakable. `drive_pending_exchanges` re-drives everything an entity owes from durable state under the operation ids its transitions first minted, so calling it after a transition, after recovery, or on a timer are the same operation.
  - `AgentEntityAddress` (agent, task, and run classes) as the routing key and durable record locator of an exchange, `AgentExchangeKind` for the six exchanges of M1 (creation, assignment, result proposal, budget allocation, settlement, and return), and the bounded, typed `AgentExchangePayload`. An exchange may not cross a tenant boundary, an entity may not apply an exchange addressed to another, and one operation id may not name two exchanges.
  - `AgentExchangeTransport`, `ShardedExchangeRoute`, `AgentExchangeRouter`, and `register_agent_exchange_codecs`: an exchange resolves its target's shard owner and is then either asked of the local entity or asked of the owning node over `rakka-remote`. Both paths reach the same durable accept path, so colocation changes the transport and never the durable record — and an exchange stays correct after its entities move to different nodes. Delivery stays at-most-once; a delivery failure is never read as evidence that the receiver did not apply the exchange.
  - The per-exchange failure-window table (initiator loss before send, receiver loss after acceptance, reply loss, duplicate delivery) is a doc section of `rakka_agent::choreography`, with the test that proves each row.
  - `rakka_agent::testkit` gained `ChoreographyProbe`, a fenced reference participant, and `InProcessExchangeTransport`, which injects those failure windows.
- `rakka-a2a`: a reusable A2A protocol adapter crate for durable Rakka agent-workflow runs. It provides A2A-to-Rakka command mapping, an async task projection store with in-memory and PostgreSQL implementations, durable public task-event streaming replay with a bounded-interval PostgreSQL event watcher, a builder-based durable A2A SDK `RequestHandler` with tenant-resolver/authorizer/workflow-catalog hooks, a sharded run owner host and cluster owner router, a push notification config store with credential redaction plus a push dispatch boundary, a dynamic agent-card builder, axum route composition, and bounded observability snapshots. Exposed through the gated `rakka` facade features `a2a`, `a2a-server`, `a2a-sharding`, `a2a-postgres`, `a2a-http`, `a2a-k8s`, `a2a-otel`, and `a2a-testkit`. The `clustered-sharded-entity-a2a-agents` example is now a thin product composition over this crate.

### Changed

- Workspace MSRV is raised from Rust 1.80 to Rust 1.85, required by the published A2A SDK crates used by the Phase 0 clustered sharded A2A agents example.
- The workspace `axum` dependency is upgraded from 0.7 to 0.8. `axum` types appear in the `rakka-http` public API, and axum 0.8 replaces the `/:param` route syntax with `/{param}` and changes websocket text/close payloads to `Utf8Bytes`.
- Workspace crates now share release metadata and internal path dependencies include explicit versions for packaging.
- CI separates required validation from optional PostgreSQL and local Kubernetes integration jobs.
- Historical implementation plans now live under `docs/plans/` instead of the product-doc root.
- `rakka-workflow` outbox entries support a terminal `Cancelled` status: `DurableInbox::record_outbox_cancelled` settles an undelivered entry before dispatch, cancelled entries are never due again, and compaction reclaims them like other terminal entries.
- Agent dispatcher cancellation is durable end to end: `AgentDispatcherWorker::cancel_run_dispatches` settles cancelled effects at the outbox layer, in-flight entries carry a typed `cancellation_requested` flag that blocks re-claiming after lease expiry, and worker refresh finalizes expired cancellation-requested entries as cancelled instead of redelivering them.
- Agent dispatch target classification only honors `target_class` attribute and target-type refinements that are compatible with the effect kind, so a mislabeled effect can no longer be routed to a dispatcher that deterministically rejects it.
- Agent autonomy policy classification (`AgentAutonomyTargetClass::for_effect`) is derived from the dispatcher's `AgentDispatchTargetClass::classify`, removing the name-substring webhook/push heuristics so policy admission and dispatch routing always agree on an effect's class.

### Security

- Security and operational defaults are documented in `docs/rakka-v1-security-operational-defaults.md`.
- Internal remoting is documented as trusted-cluster traffic and production exposure remains out of scope for v1 without an operator-provided security layer.

### Validation

- Required validation: `scripts/validate.sh`.
- Packaging validation: `scripts/package-check.sh` in Cargo offline mode.
- Release-candidate review: `docs/rakka-v1-release-candidate-review.md`.
- Optional PostgreSQL validation: `RAKKA_POSTGRES_TEST_DSN=postgres://postgres:postgres@localhost:5432/postgres cargo test -p rakka-persistence-postgres`.
- Optional Kubernetes validation: `RAKKA_K8S_RUN_LOCAL_CLUSTER=1 RAKKA_K8S_IMAGE=<image> examples/kubernetes/local-cluster-scenario.sh`.

## 0.1.0-v1-rc.0 Draft

This section is reserved for the first reviewable v1 release-candidate notes.

### Release Checklist

- Run `scripts/validate.sh` from a clean checkout.
- Run `scripts/package-check.sh` from a clean checkout.
- Review `docs/rakka-v1-release-packaging.md`.
- Confirm optional PostgreSQL and Kubernetes checks were either run or explicitly deferred.
- Update this changelog with user-facing changes, known limitations, and migration notes.
