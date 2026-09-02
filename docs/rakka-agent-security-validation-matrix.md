# Agent Domain Security Validation Matrix

Status: implemented (slice 6.2).

This document maps [specification 16](plans/rakka-agent/spec.md) and the
memory clauses of [13.1](plans/rakka-agent/spec.md) to the code that enforces
them and the tests that prove it. The goal is not to claim the agent domain is
secure. It is to say precisely which security claims are *enforced*, which are
*delegated* to the deployment, and which are currently *inferred* rather than
demonstrated.

The companion documents are
[`rakka-agent-fault-injection-matrix.md`](rakka-agent-fault-injection-matrix.md),
whose closing section named this validation as still owed,
[`rakka-agent-telemetry-validation-matrix.md`](rakka-agent-telemetry-validation-matrix.md),
which does the same for the telemetry claims, and
[`rakka-v1-security-operational-defaults.md`](rakka-v1-security-operational-defaults.md),
which states the framework-versus-operator split for the substrate.

## What this slice found

Four of the five things it fixed were **fail-opens**, not missing tests: paths
where the code was wrong and no test would have noticed. Each is recorded here
with the falsification that proves the test is load-bearing — reverting the fix
and confirming the suite fails.

| Fail-open | What it was | What closed it | Falsified by |
| --- | --- | --- | --- |
| The retriever decided what a model saw | `assemble_context` re-checked every property of a retrieved record except scope, and the trait doc called that unavoidable. A backend with a wrong predicate had its bytes embedded in the immutable snapshot. | Every ranked identity is resolved through the authoritative scope-addressed store; the record *it* holds is what is checked and embedded. The retriever supplies a ranking, nothing else. | Reverting to the retriever's payload fails all 7 `memory_scope_fence` tests |
| The two guardrail chains were never compared | `AGENT_EVALUATED_GUARDRAIL_BOUNDARIES` unconditionally claimed memory-ingress was evaluated, so a mandatory ingress-only stage satisfied coverage at an authority that had never seen a retrieval bundle. | The deployment attests with `AgentToolAuthority::with_memory_ingress`, and the attestation is checked against a chain *declaration digest*. Unattested, the authority does not count the boundary and fails closed. | `an_empty_bundle_chain_at_the_same_revision_is_refused_at_wiring` — the case a revision comparison misses |
| The attested chain was not the installed one | The attestation took a bare `AgentMemoryRetrieval`, compared digests and dropped the reference. Nothing bound it to the bundle the run assembles through, so a bundle attested and then not installed satisfied the check and ran no ingress stage — which is the shape the positive control itself had. | `with_memory_ingress` takes the `AgentRunMemory`, so the object attested is the object the run assembles through; a memory carrying no bundle is refused, and `AgentToolAuthority::attests` re-checks a memory assembled elsewhere. | `the_attested_memory_is_the_one_the_run_assembles_through` — dropping the run's memory leaves its ingress stage uncounted |
| A resolver's words became durable state | `AgentCredentialError`'s `Display` reached `record_attempt_failure`, which writes into the workflow outbox row and the fleet index — both unbounded, both durable, both fleet-readable. | The attempt persists Rakka-authored text naming the logical binding; the resolver's own detail goes to a bounded `tracing::warn!` and nowhere else. Every persisted attempt detail is truncated at `AGENT_DISPATCH_FAILURE_DETAIL_MAX_LENGTH`. | Reverting shows the sentinel in the fleet entry's `last_error_code` |
| A general worker killed sandboxed work | One fleet, any worker claims any ticket, and a worker whose router rejects the class refuses *definitively*. In a heterogeneous fleet the race decided, and the wrong winner failed the effect permanently. | `AgentDispatchClaimFilter` skips a non-served entry **before taking the lease**, so the ticket stays claimable. Counted as `class_filtered`, which needs no durable write. | Reverting fails the 3 routing tests in `executor_isolation` |

The fifth was softer: retention had no production caller anywhere, so
`purge_run` was a capability nobody composed. It now has
`discharge_run_memory_retention` and `AgentMemoryRetentionSweep`, and a
`terminal_at` stamp to measure from — `updated_at` could not serve, because a
terminal run keeps accepting settlement commands and the deadline would recede.

### The pre-upgrade backlog, and the one-time repair

The stamp is written at the single terminal transition, under the
already-terminal guard that makes it once-only. That guard also puts an entire
population out of reach: **a run that was already terminal when the field
shipped never re-enters the one place that could stamp it.** Nothing in normal
operation can, so `discharge_run_memory_retention` answers
`TerminalTimeUnknown` for that run *for the life of the deployment*, and its
session rows and context snapshots — the tier that embeds model-visible
content — are never purged. The only signal was `terminal_time_unknown`, a
counter climbing beside a healthy `discharged`, with no documented remediation.
Refusing silently forever is the one option that leaves content past its
window, so it is not the option taken.

`backfill_run_terminal_stamp` repairs one scope and
`AgentRunTerminalStampBackfill` is the bounded, deployment-invoked pass over
many — the same shape as the retention sweep, and for the same reason: Rakka
keeps no index of runs by terminal state, so enumeration belongs to the
application. Four properties are what make it safe to run against a live
fleet, and each is asserted:

- **The clock is `updated_at`, and it is sound in one direction only.** It is
  the time of the last accepted transition, so for a terminal run it is never
  *earlier* than the true terminal time: the terminal transition sets both to
  the same instant, and only transitions landing afterwards move it on. A
  backfilled deadline therefore falls at or after the real one — the run is
  retained at least as long as policy requires and never purged early. That
  asymmetry is the argument for repairing at all: erring late is recoverable by
  running the sweep again, erring early destroys a record no replay can
  rebuild. It is an approximation, and it is written down rather than implied.
- **It is opt-in, not something the discharge does on its own.** Re-dating a
  retention clock is a decision a deployment makes; a sweep that did it
  silently would turn "this run's window has elapsed" into "this run's window
  elapsed relative to whenever the migration happened to run", with nothing in
  the record saying so.
- **It never moves a stamp that exists.** The guard is re-checked inside
  `AgentRunState::backfill_terminal_at`, not trusted from the caller, so a
  completed migration is safe to re-drive and a normally-stamped run is
  untouched. `updated_at` itself is deliberately not moved either — a repair is
  not an accepted transition, and moving it would push the clock the next pass
  would read.
- **It loses every race.** The write is a compare-and-set against the revision
  it read, so a resident entity that wrote in between wins and the pass reports
  `Conflicted` — the only retryable outcome — rather than clobbering it. In the
  other direction the entity's own persist drops its cached record on a
  revision conflict and recovers the authoritative one, so a backfill racing a
  live terminal run costs that run one re-driven command, not a wedge.

**Operational ordering: run it once the fleet is fully upgraded, never during
the rolling update.** A repaired record carries run-state schema version 2,
which a binary from before the bump fails closed on — correctly, since that
binary would otherwise load it and drop the stamp again on the next settlement
it applied. Repairing early therefore makes those records unreadable to peers
still running, and nothing is gained by hurrying: an unstamped record is
refused by the discharge, not deleted. A migration is complete when a pass over
the same scopes reports `conflicted: 0` and `stamped: 0`.

Proven in `tests/memory_retention.rs` (8 clauses), each falsified: removing
both once-only guards, dropping the schema upgrade, taking the stamp from
`accepted_at` instead of `updated_at`, propagating a revision conflict as an
error, and reporting a stamp without writing one each fail their own test.
Removing only the *caller's* guard does not — which is the point of re-checking
inside the mutator, and is what that arrangement was verified for.

## Specification 16, clause by clause

| Clause | Enforced where | Proof | Status |
| --- | --- | --- | --- |
| Every request authenticated and tenant-authorized before data access | `A2AAuthorizer` hook per operation class; tenant is a *key* on every scoped read | `rakka-a2a` surface tests | **Authn delegated** — see "Delegated to the deployment" |
| Policy checks before existence-revealing queries | `query.rs` goal-view owner wrapper; `runsync.rs`/`error.rs` map denial to not-found | `goal_view.rs`, `coordination_surface.rs` | Met |
| Tool capabilities declared outside model output, enforced before scheduling and dispatch | `AgentToolRegistry` + `AgentToolAuthority` | `tool_authority.rs` | Met |
| Descriptor ≠ dispatch authority; five layers each validated at its boundary | `tools.rs` authorize ladder | `tool_authority.rs` | 4 of 5 — the model-visible descriptor rung is unwired (below) |
| A dispatcher lacks ambient authority beyond its declared trust/tool/tenant class | Claim-time `AgentDispatchClaimFilter` + the authority's `execution-policy-unroutable` gate, as two independent layers | `executor_isolation.rs` | Met for routing; the worker's *actual* isolation is the platform's |
| Versioned ordered guardrail stages at all seven boundaries | `AgentGuardrailChain`, evaluated at model-request, tool-request, tool-response, memory-ingress. The tool-response point (`AgentToolAuthority::review_tool_response`) runs in the dispatcher after execution and before delivery — the last point at which the result is in memory and nothing durable has recorded it — so a blocked result reaches neither the run, its session memory, nor a later context snapshot; it fails the effect as a determinate `guardrail-blocked` outcome of a tool that did run, delivered once and never retried, and a transformed result is what is delivered, so a redelivery carries the same content | `tool_authority.rs` (`a_blocked_tool_response_never_reaches_the_run`, `a_transformed_tool_response_is_what_the_run_records`, `a_checkpoint_requiring_tool_response_stage_fails_closed`, `a_tool_response_only_mandatory_stage_satisfies_coverage`), `memory_ingress_guardrails.rs` | **4 of 7** — see "Owed" |
| Bounded outcome set, stable reason code, protected evidence | `AgentGuardrailOutcome` | `guardrails.rs` unit tests | Met |
| Deployment/tenant policy adds mandatory guardrails a definition cannot weaken | Deployment chain `mandatory()`; envelope `mandatory_guardrails` | `tool_authority.rs`, `definition_setup_envelope.rs` | Deployment-level met; **tenant-level does not exist** |
| A transform is deterministic under a recorded revision; a retry reuses the accepted input | Synchronous rule trait + per-stage revision + the intent's chain-revision pin | `tool_authority.rs` | Met |
| `report-only` grants nothing | Structural: the fold cannot set a disposition from it | `guardrails.rs`, `memory_ingress_guardrails.rs` | Met |
| Immediate revocation re-checked before external invocation | `AgentEntityAuthority::authorize` re-loads durable state per attempt | `wait_invalidation.rs` (12 tests), `tool_authority.rs` | Met, **not atomic** with the invocation (below) |
| Credentials resolved only for the bounded attempt, never logged or persisted | Resolution between durable `Started` and `invoke`, dropped after; `AgentEphemeralCredential` has no `Serialize` and a redacting `Debug` | `secret_exclusion.rs` (9 tests), incl. a kill *while the credential is live* | Met |
| Memory retrieval enforces tenant, agent, classification before ranking | `MemoryRetrievalQuery::admits` as pre-ranking predicates, and — new — the authoritative store resolves every ranked identity | `memory_scope_fence.rs`, `private_memory_retrieval.rs` | Met; *purpose* restrictions remain unmodelled |
| Communal memory treated as an injection source; provenance and trust available to the context builder | — | — | **Not applicable yet**: communal retrieval is unwired, so no claim reaches a model context |
| Tool arguments, prompts, raw memory, credentials, high-cardinality ids never metric labels | `validate_agent_domain_metric_attributes` per observation | `agent_metrics.rs`, and `secret_exclusion.rs` over the whole emitted *series set* | Met |

## Memory: specification 13.1

| Clause | Session | Snapshots | Private | Communal graph |
| --- | --- | --- | --- | --- |
| Explicit tenant and ownership scope | Key | Key (record's own `scope` field) | Key | Key |
| Authorize before revealing existence | Wrong scope ≡ absent | Wrong scope ≡ absent | Wrong scope ≡ absent, byte-identical | `authorization_isolation` conformance clause |
| Stable idempotent operation ids | Yes | First-writer-wins | Operation ledger | Yes |
| Provenance and classification preserved | Yes | Yes | Yes | Yes |
| Retention, tombstone, deletion | `purge_run`, now with a caller and a conformance clause | `purge_run`, purged first | Retention, tombstone, delete, `purge_expired` | **None — owed** |
| Resolved credentials excluded | `secret_exclusion.rs` | same | same | Not swept |
| Authoritative records distinguished from derived indexes | n/a | n/a | The store answers; the index only ranks | n/a |

## Delegated to the deployment, and named here so it is not assumed

- **Authentication.** `rakka-a2a` performs none. `AllowAllAuthorizer` is the
  default wired at `handler.rs`, and the crate's routers add no auth layer. A
  deployment that mounts the surface without an authorizer has an open surface.
- **The request-supplied tenant.** `A2AHeaderTenantResolver` accepts
  `request.tenant` when no `x-rakka-tenant`/`x-tenant-id` header is present,
  and `default_tenant` assigns one when nothing resolves. Both are documented
  as appropriate only behind an ingress that authenticates and sets the header.
  Behind a misconfigured ingress they are a tenant-spoofing door.
- **What an execution class actually isolates.** Rakka routes by the class and
  refuses to run an effect on a worker that does not serve it. The worker pool,
  Kubernetes RBAC, NetworkPolicy, credential issuer, and sandbox behind the
  reference are the platform's.
- **What an executor puts in its error text.** Rakka bounds it and persists it;
  it cannot know what is secret inside it. The contract is stated on all nine
  executor traits and on the credential resolver.

## Owed, and why

- **Guardrail evaluation points for `ModelResponse`, `A2aIngress`, and
  `A2aEgress`** — 3 of 7 declared boundaries have no evaluation point.
  `ToolResponse`, the poisoning-relevant one, was closed after Phase 6 (a tool
  result now crosses the boundary before it is delivered; the `RequireCheckpoint`
  outcome fails closed there, since no checkpoint can gate a response that
  already exists). The model-response point is the same shape and the same
  durable-semantics answer; the A2A points belong to the protocol adapter's
  ingress and egress. The coverage gate fails closed meanwhile
  (`guardrail-stage-unevaluated`).
- **Communal retrieval into a model context**, `SnapshotCommunalClaim`, and
  per-claim read-capability enforcement. Deferred by slice 4.6;
  `MemoryContextSnapshot::communal_claims` is a permanently empty placeholder.
  Until it exists there is no communal poisoning surface to defend.
- **Knowledge-graph retention, tombstone, and deletion** — an absolute
  specification 13.1 requirement with no implementation on any backend.
  `Retracted` is a trust transition that preserves content.
- **The model-visible descriptor rung.** `AgentToolRegistry::model_visible` has
  no production call site and `AgentModelRequest` carries no tool list, so "the
  descriptor grants nothing" is vacuous rather than enforced.
- **Descriptor/endpoint revision pinning across recovery.** Generic tool
  intents record no descriptor digest; the workflow tool records one that
  nothing compares.
- **Tenant-scoped mandatory guardrails.** Deployment-level exists; nothing keys
  a mandatory set to a `TenantId`.
- **The revocation re-check is not atomic with the invocation.** Two durable
  writes and a credential resolution sit between `authorize` and `invoke`. A
  revocation landing in that window is honoured on the next attempt, not this
  one.
- **A cardinality oracle in `SessionPurgeOutcome::Purged { entries }`.** A
  nonexistent scope answers zero and a populated one does not. Deliberately not
  collapsed: the count is what makes a fleet sweep observable and a purge
  auditable, and no memory surface is reachable from A2A or the operational
  query. Any surface that ever exposes purge to a caller MUST authorize the
  scope first and SHOULD collapse the count.
- **Cross-tier erasure.** A private `delete` does not discharge an erasure
  request on its own: the snapshots of every run that embedded the memory must
  also be purged. Required by scenario 17's retry determinism — a store that
  scrubbed embedded content would make the immutable tier mutable — and bounded
  by the retention window, 30 days by default. Both the exposure and its bound
  are proven in `memory_retention.rs` rather than left as a footnote.
- **The generic workflow outbox still persists an unbounded failure
  message.** The 512-byte bound this slice put on every persisted attempt
  detail lives in the agent substrate
  (`rakka_agent_workflow::AGENT_DISPATCH_LAST_ERROR_MAX_LENGTH`, which
  `AGENT_DISPATCH_FAILURE_DETAIL_MAX_LENGTH` aliases) and covers the fleet
  index and every agent-domain outbox row. `rakka-workflow`'s own
  `record_outbox_failure`, reached by `dispatch_due_outbox` on the non-agent
  path, still stores the application's message as `last_error` unchanged.
  Left deliberately: that path is a v1 compatibility surface shared by every
  workflow consumer, and bounding it is a substrate decision with its own
  compatibility note, not an agent-domain fix. Until it is taken, a non-agent
  workflow's outbox row carries whatever its dispatcher chose to say.

## One contract, every backend

The three memory stores and the vector retriever now share one suite —
`rakka_agent::memory_conformance` — instead of two hand-written copies that
drift. The isolation clauses compare a foreign scope's answer to a *third
genuinely-empty* scope's by **whole value**: `is_empty()` and `is_none()`,
which the previous copies used, are satisfied by a backend that answers
"empty" differently from how it answers an unknown scope, and that difference
is the disclosure. Two clauses did not honour that rule and now do: the
withdrawal arm aimed at an id that exists in no scope, which proved a
not-found path exists and nothing about isolation, and the retriever clause
compared answers by length. The withdrawal arm now targets a primary-owned
record no outsider twins — comparing the outsider's refusal to an uninvolved
scope's refusal for the *same* id, since `MemoryError::NotFound` echoes the id
asked for and two different ids could never compare equal — and re-reads the
record to prove it survives un-withdrawn. The retriever clause compares whole
outcomes, and asserts the empty scope's own answer is empty, without which a
retriever ignoring scope altogether would answer the primary's corpus to every
scope, empty included, and match itself. Exhaustiveness is a compiler matter —
one operation enum per trait, matched with no wildcard — so a method added to
a trait fails to build until its isolation arm exists.

| Clause | What a non-conformant backend does | Runners |
| --- | --- | --- |
| Session, snapshot, and private scope isolation | Answers an outsider differently from an empty scope | in-memory (ungated), PostgreSQL (DSN-gated) |
| Snapshot isolation specifically | Keys on the reference rather than the record's own `scope` field — `persist` takes no scope argument, so that field is the whole fence | same |
| Private write preconditions | Lets a stale expectation overwrite a concurrent write | same |
| Tombstone and delete erasure | Lets a replayed pre-withdrawal write resurrect withdrawn content | same |
| Retention purge | Ignores a legal hold, or purges before the window elapses | same |
| Filters before ranking | Answers a *short* page, because it filtered after its `LIMIT` | in-memory, pgvector |
| Ranked record matches the authoritative one | Returns a stale or synthesized copy | same |

The pgvector arm needs the `vector` extension, which a stock `postgres` image
does not carry. Without it the arm **announces** the three clauses it did not
run rather than reporting a silent `ok`, and
`RAKKA_POSTGRES_PGVECTOR_REQUIRED=1` turns that announcement into a failure —
what a CI or release run should set, since a suite that quietly stops covering
what its name claims is the failure mode this whole document exists to refuse.

Verified against PostgreSQL 16 with pgvector 0.8.5: all twelve clauses pass
under `RAKKA_POSTGRES_PGVECTOR_REQUIRED=1` with no skips, so the shared suite
holds on both backends unchanged — the acceptance shape slice 2.4 established
for the knowledge graph.

## Repeatable commands

```sh
cargo test -p rakka-agent --test memory_store_contract
cargo test -p rakka-agent --test memory_scope_fence
cargo test -p rakka-agent --test memory_guardrail_chain_consistency
cargo test -p rakka-agent --test memory_retention
cargo test -p rakka-agent --test secret_exclusion
cargo test -p rakka-agent --test executor_isolation
cargo test -p rakka-agent --test tenant_isolation

# The store tiers, against any PostgreSQL:
RAKKA_POSTGRES_TEST_DSN=postgres://postgres:postgres@localhost:5432/postgres \
  cargo test -p rakka-agent-postgres --test memory_conformance

# The whole contract, against a pgvector-enabled image, with no silent skips:
RAKKA_POSTGRES_TEST_DSN=postgres://postgres:postgres@localhost:5433/postgres \
RAKKA_POSTGRES_PGVECTOR_REQUIRED=1 \
  cargo test -p rakka-agent-postgres --test memory_conformance
```

## Production interpretation

Passing these means each claim in the tables above is test-backed at the
fidelity its row names. It does not remove the need for an authenticating
ingress, a real worker-pool and sandbox implementation behind the execution
classes, a credential issuer that does not echo secrets in its errors, or the
telemetry and Collector validation slices 6.3a and 6.3b own.
