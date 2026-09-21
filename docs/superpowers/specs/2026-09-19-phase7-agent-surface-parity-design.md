# Phase 7 — Agent surface parity: mounted endpoint, providers, MCP client, response guardrails, streaming, memory modes, registry, builder DSL

Status: design spec, awaiting review. Drafted 2026-09-19; scope reduced
2026-09-20 at the author's direction (see "Cut from the first draft" in the
summary); revised 2026-09-20 after the host's product-authority assessment
(section 0); the host accepted the revision later the same day, and its one
optional ask back is adopted as R17 (section 0, "Host response"). Branch
`rakka-agents`.
Brief: close the developer-facing gaps section 10 of
[`docs/comparisons/akka/rakka-akka-comparison-audit-2026-09-19.md`](../../comparisons/akka/rakka-akka-comparison-audit-2026-09-19.md)
names, without weakening any durability, effect-safety, secret, replay, or
passivation boundary the agent domain already holds. Normative source stays
[`docs/plans/rakka-agent/spec.md`](../../plans/rakka-agent/spec.md); where this
document tightens it, the tightening is listed in section 11 as a spec
amendment to land with the phase.

Every API shape below was checked against the tree at `a757ad2`. Where a
current signature is quoted, it is quoted from the file named.

## 0. Revisions (2026-09-20)

Source: the host's assessment at
`../paloul.rakka.host/docs/specs/2026-09-20-upstream-phase7-parity-assessment.md`
(host main `8247472`, pinned to `a757ad2`), written as the product authority
for the Rakka Agent ecosystem. Each entry names the assessment section it
answers; every revised passage is marked "(revised 2026-09-20)" in place, and
every line, signature, and constant a revised passage quotes was re-verified
at `a757ad2` on 2026-09-20. No change below weakens a boundary in section 2;
each tightens one or gates a capability off by default.

| # | Change | Where | Answers |
| --- | --- | --- | --- |
| R1 | Response guardrails (slice 7.2) is the first slice and depends on nothing; it is proven with `DeterministicModelAdapter` and the `reviewed_tool_outcome` precedent, not with `RigProviderAdapter`. The consumer's priority order is recorded. | Summary "Ordering", 12 | §2.4, §5 item 1 |
| R2 | `A2aIngress` is evaluated inside `RakkaAgentA2AService`, in `normalized_send`, the one step every public content entry runs (`send`, `send_message`, `team_command`, `conversation_command`); the handler calls `send` and adds only the SDK error mapping. | 6.3 | §2.4, §5 item 1 |
| R3 | Decision 5 reversed: `rig-core` gains `rustls` only, never `reqwest`, whose feature enables `reqwest/system-proxy`. | 4.5, 11.1, Summary | §2.2, §3, §5 item 2 |
| R4 | `RigProviderAdapter` takes the HTTP backend it sends through as a constructor argument and never builds rig's default client; the base URL is validated before the client is built; the host's `providers.rs` is the precedent shape. | 4.4 | §2.2, §5 item 2 |
| R5 | The credential path is spelled out: `AgentEffectCredentialResolver::resolve(scope, binding, effect)`, the effect's `timeout_ms` as the only deadline input, a per-attempt `deadline_at` stamped from it, and a Model effect that will resolve a credential refused without a `timeout_ms`. | 1, 4.2, 4.3 | §2.2 seam 1, §3, §6 |
| R6 | `AgentModelProfileCatalog` is implementable over a deployment-owned record with no second store; `StaticAgentModelProfileCatalog` is one implementation, never the required one. | 4.1 | §2.2 |
| R7 | `AgentModelRequest.tools` is confirmed as the filtered model-visible set with revoked tools withheld, as `model_visible(envelope, settings)` computes. | 4.4 | §2.2 seam 2 |
| R8 | MCP client under three conditions: an injected transport client with a per-attempt egress hook; `ChildProcess` refused at construction without a launcher seam, rmcp's child-process transport behind an off-by-default feature; descriptor sync a standalone publish-time function with `Manual` refresh the default; `credential_binding` the only credential path. | 5.1–5.4, 11.1 | §2.3, §5 item 3 |
| R9 | Compatibility guarantee for `RakkaAgentA2AService::new`, `send`, `send_message`, the two executors' `new`, and every existing `with_*` builder; the header resolvers are defaults a deployment replaces; an unmounted handler costs nothing. | 3.2, 3.5, 11.1 | §2.1, §5 item 4 |
| R10 | `AgentTaskHistoryWatcher` and the three Postgres history stores are optional: not requirements of the service, of the history-store traits, or of the `assert_*_history_store_contract` harnesses; the watcher leg is a separate `check_*`. | 7.1, 7.3, 12 | §2.5, §5 item 4 |
| R11 | Directory: the static catalogs stay the service's default, delegation never consults the directory unless it is what was installed, `gen_ai.agent.name` and `gen_ai.agent.version` ride the same identity policy as `gen_ai.agent.id` and are off by default, the sweep is deployment-invoked. | 9.2, 9.4, 9.5 | §2.7, §5 item 4 |
| R12 | The memory mode is a bundle field, narrowable by settings, outside the digest `with_memory_ingress` attests; a mode change is not a rolling restart. | 8.1, 8.2 | §2.6, §5 item 5 |
| R13 | `CompiledPlanBuilder::build()` accepts a caller-supplied plan id and fingerprint and derives them only when absent. | 10.3 | §2.8 |
| R14 | Compatibility section gains the feature-unification note, the unchanged-constructor guarantee, and the new stable codes R5 and R8 introduce. | 11.1, 11.2 | §3 |
| R15 | New section 14 records the host's stated priorities beyond this phase. | 14 | §4, §5 item 6 |
| R16 | The decisions list is updated (decision 5 reversed; six decisions added) and the Status line records the revision. | Summary | all |
| R17 | (from the host's response, later the same day) The MCP executor's egress check is a required argument of `new`, and the only opt-out is the explicit `McpAllowAllEgress` value, so no deployment can forget a builder. Stricter than the fail-closed refusal the host suggested, with no runtime check. | 5.3, 5.4, 11.2, 12, Summary | §8, "One optional ask back" |

Two premises in the assessment differ from the tree at `a757ad2` and are
corrected where they matter rather than adopted: the effect's `deadline_at` is
never written today (R5, section 1), and Rakka's own delegation and handoff
executors deliver through `send_message`, not `send` (R2, sections 1 and
6.3). Both corrections make the host's ask hold more widely, not less.

**Host response (2026-09-20, later the same day).** Section 8 of the
assessment accepts all sixteen revisions and the three corrections as stated.
It records that correction R5 also exposed a defect on the host's own
tool-credential path: its resolvers pass the unwritten `deadline_at` through,
so every tool credential the dispatcher resolves there today is resolved
against the minimum lease alone. The host fixes that on its side now and takes
the per-attempt stamp at the next pin; the finding is credited to this
exchange. Its one optional ask back is adopted as R17.

**Plan refinements (2026-09-20, slice 7.2 plan).** Three details the tree
forced while planning slice 7.2, each marked inline as a plan refinement:
the Block and RequireCheckpoint codes follow the `ToolResponse` precedent
(`guardrail-blocked`, `checkpoint-required`; no new code); ingress evaluates
at the three authorized leaves after `normalized_send`, where authorization
actually lives; and the guardrail context names an `AgentGuardrailSubject`
because ingress has no run. Plan:
`docs/superpowers/plans/2026-09-20-phase7-slice-7-2-response-guardrails.md`.

## Summary

**What this phase delivers.** Eight capabilities, each a decision section below:

| # | Capability | Result | Section |
| --- | --- | --- | --- |
| 1 | Mounted agent endpoint | The agent entities are served over A2A 1.0 JSON-RPC and REST on axum, card at the well-known path, with tenant, authorization, drain, and push-config parity with the workflow surface | 3 |
| 2 | Wired model providers | (revised 2026-09-20) A durable, secret-free model profile catalog; `call_with`, which hands the adapter the credential the dispatcher resolved inside the attempt under the effect's own deadline; the model-visible tool list on the request; provider telemetry slots; and, as the optional tail, a per-profile Rig adapter over an injected HTTP backend and a router that selects by profile, so every Rig 0.37 provider is reachable | 4 |
| 3 | MCP client | A new `rakka-agent-mcp` crate on the Rust SDK `rmcp` 3.4 (MCP 2026-07-28): remote MCP tools become registered tool bindings dispatched through the durable effect path, under the three conditions in section 5 | 5 |
| 4 | Response guardrails | The `ModelResponse`, `A2aIngress`, and `A2aEgress` boundaries gain evaluation points, closing issue #70 and making all seven declared boundaries real; a refused model response lands as a determinate failed effect exactly as a refused tool response does today | 6 |
| 5 | Streaming | Task-state SSE on the agents surface with cursor replay and explicit window expiry | 7 |
| 6 | Memory modes | Session memory gains Akka's read-only, write-only, filtered, and disabled modes as a declarative policy on the run memory bundle | 8 |
| 7 | Registry | A tenant-scoped agent directory: a durable read model derived from agent entity snapshots that answers resolve, enumerate, and skill queries, feeds the A2A card, and can stand in as the delegation catalog when a deployment installs it | 9 |
| 8 | Builder DSL | Two code-first Rust builders that emit the existing data: `AgentDefinition::builder` and a compiled-plan builder that emits a validated, fingerprinted `AgentCompiledExecutionPlan`. Neither is a text DSL or a compiler | 10 |

**Ordering (revised 2026-09-20).** Response guardrails first (slice 7.2): it
depends on nothing, and it is proven with `DeterministicModelAdapter`
(`crates/rakka-agent/src/testkit.rs:1856`) and the `reviewed_tool_outcome`
precedent (`crates/rakka-agent/src/dispatch.rs:3312`), not with a provider
adapter. Then the three provider seams (7.1), then the MCP client (7.7). The
mounted endpoint with the history stores it needs, streaming, and the
directory form a second, independent track; memory modes and builders run
alone. Consumer priority, from the host's assessment: 7.2 is needed; 7.1's
three seams (`call_with`, `AgentModelRequest.tools`, the telemetry slots) are
wanted; 7.7 is wanted under the three conditions in section 5; 7.3, 7.4,
7.5, 7.6, 7.8, and 7.9 are not requested by the host and stay feature-gated
and inert for a deployment that does not mount or install them. Slice list in
section 12.

**Cut from the first draft (2026-09-20).** ACP, the MCP server, model-token
deltas, push delivery for agent tasks, and the compaction executor. Section
11.4 records each with the seam it would attach to, so a later phase can pick
one up without re-deriving the design.

**Decisions I made that you should confirm or overturn (revised 2026-09-20).**

1. Streaming on the agents surface requires shared (PostgreSQL) task, team,
   and conversation history stores. Today history sinks are per pod (fault
   matrix, "Production interpretation"), so a stream served from one pod would
   miss what another pod appended. This phase therefore adds
   `PostgresAgentTaskHistoryStore` and its two siblings with LISTEN/NOTIFY
   watchers, as optional types a deployment that owns its own stores never
   constructs (section 7.1). This is the largest piece of prerequisite work
   in the phase.
2. Two trait additions are breaking for out-of-tree implementors:
   `AgentDispatchAuthority::review_model_response` (required, on purpose, for
   the same reason `review_tool_response` is required) and
   `AgentModelAdapter::call_with` (defaulted, so in-tree and out-of-tree
   adapters keep compiling; only the dispatcher calls it).
3. The registry is a projection, not a sixth sharded entity. The agent entity
   stays the authority; the directory is rebuilt by a sweep from entity state
   whenever it drifts, exactly as `recover_task_projections` rebuilds the A2A
   task projection.
4. MCP multi-round-trip requests (the 2026-07-28 replacement for sampling and
   elicitation) are refused with a stable code in this phase. Mapping them to
   human checkpoints is a follow-up.
5. **Reversed 2026-09-20.** `rig-core` gains the `rustls` feature only, never
   `reqwest`: in `rig-core 0.37.0` the `reqwest` feature enables
   `reqwest/system-proxy`, an environment-proxy egress bypass, and Cargo
   feature unification would switch it on for every crate in a consumer's
   graph. The first draft's "no provider can make an HTTP call at all" is
   answered by `rustls` alone, which selects a TLS backend and nothing else
   (section 4.5).
6. `AGENT_GUARDRAIL_CONTENT_MAX_BYTES` rises from 8 KiB to 16 KiB so a full
   model turn (`AGENT_MODEL_TURN_MAX_BYTES`) can be evaluated without
   truncation.
7. The builders live in the crates that own the data they build
   (`rakka-agent`, `rakka-agent-workflow`); there is no new DSL crate. The
   composition root (`AgentSystem::builder`) is out of scope, as you asked.
8. (added 2026-09-20) `A2aIngress` is evaluated inside
   `RakkaAgentA2AService`, at `normalized_send`, not in the HTTP handler, so
   every in-process caller is covered and the handler adds only error mapping
   (section 6.3).
9. (added 2026-09-20) `RigProviderAdapter` and `McpDispatchToolExecutor` take
   the HTTP backend they send through as a constructor argument and never
   build one; the MCP executor also hands each server URL to a deployment
   egress hook before a client exists (sections 4.4, 5.3).
10. (added 2026-09-20) MCP descriptor sync is a standalone function a
    deployment calls at publish time and stores as release data; `Manual`
    refresh is the default, so a registry built from stored descriptors makes
    no network call (section 5.2).
11. (added 2026-09-20) `gen_ai.agent.name` and `gen_ai.agent.version` are
    supplied only through the caller's identity policy, exactly as
    `gen_ai.agent.id` is, and stay `None` unless a deployment fills them
    (section 9.4).
12. (added 2026-09-20) A Model effect that will resolve a credential must
    carry a `timeout_ms` on its spec: the dispatcher stamps the attempt's
    `deadline_at` from it, and `authorize_model` refuses a credential-bearing
    model call without one (`model-timeout-unset`), because the effect is the
    resolver's only deadline input (section 4.2).
13. (added 2026-09-20, from the host's response) `McpDispatchToolExecutor::new`
    requires an `McpEgressCheck`; `McpAllowAllEgress` is the explicit,
    by-name opt-out for in-cluster and test use. A required argument is
    stricter than the fail-closed refusal the host suggested and needs no
    runtime check (section 5.3).

**What does not change.** Every model call, tool call, and outbound message
stays a durable outbox effect; no secret ever lands in a profile, binding,
directory entry, plan, or stream frame; runtime events and stream frames stay
observability, never correctness; a passivated entity stays addressable; the
A2A task projection stays a public view of authoritative task state. (added
2026-09-20) A deployment that installs none of sections 3, 7, 9, or 10 sees no
behavioural change from them and runs no new migration.

## 1. Where the seams are today

The facts the decisions rest on, with file references at `a757ad2`.

- The workflow A2A surface is mounted through `RakkaA2ARequestHandler`, the one
  `impl RequestHandler` (`crates/rakka-a2a/src/handler.rs:1922`), and
  `a2a_routes_at` (`routes.rs:66`) composing the SDK's `agent_card_router`,
  `rest_router`, and `jsonrpc_router`. The SDK trait
  (`a2a-server-lf 0.4.0`, `handler.rs:347`) has eleven required async methods,
  no associated types, and `ServiceParams` is the lower-cased request header
  map. SSE framing is the SDK's (`sse_from_stream`, `sse_jsonrpc_stream`),
  which sets no event ids and no keep-alive; the handler's own stream emits
  heartbeats.
- The agents surface is `RakkaAgentA2AService<Tasks, Agents, History, Runs,
  Teams, TeamHistory, Conversations, ConversationHistory>`
  (`crates/rakka-a2a/src/agents/service.rs:111`) with direct methods `send`,
  `send_message`, `get_task`, `cancel_task`, `manage_agent`, `team_command`,
  `conversation_command`, `replay_task_events`, `replay_coordination_events`,
  `agent_goal_view`, each taking `&ServiceParams` and SDK request types. It
  has no `RequestHandler` impl, no route, no streaming, no push, and no
  `list_tasks` (the plan deferred the binding at slice 1.12,
  `implementation-plan.md:1170`).
- (added 2026-09-20) Every public content entry of the service runs
  `normalized_send` (`service.rs:490`) first: `send` (`:447`), `team_command`
  (`:529`), `conversation_command` (`:638`), and `send_message` (`:961`).
  `send` then routes to `team_command_normalized`,
  `conversation_command_normalized`, or `send_message_normalized`.
  `A2AAgentDelegationSendExecutor` and `A2AAgentHandoffSendExecutor` deliver
  in-process through `send_message` (`delegation.rs:318`, `handoff.rs:425`),
  not through `send`. `manage_agent` (`:731`) has its own gate and carries a
  command, not message content.
- A model call is `AgentRunEffectRequest::Model { context, profile }` created
  with `profile: None` (`run.rs:2566`); `AgentToolAuthority::authorize_model`
  (`tools.rs:1921`) selects the profile from the agent's current settings and
  validates it against the envelope; the dispatcher's `invoke` Model arm
  (`dispatch.rs:3370`) calls `self.model.call(&request)` on the single
  `Arc<dyn AgentModelAdapter>` a dispatcher holds, passes no credential, and
  runs no review before delivering `AgentRunEffectOutcome::Model`.
  `RigModelAdapter<M>` (`rig.rs:87`) wraps an application-constructed
  `CompletionModel`; `AgentModelProfileId` is an opaque validated id with no
  record behind it; `rig-core = "=0.37.0", default-features = false`
  (`crates/rakka-agent/Cargo.toml:44`, no feature list).
- (added 2026-09-20) The dispatcher resolves a credential inside the attempt
  through `AgentEffectCredentialResolver::resolve(scope, binding, effect)`
  (`dispatch.rs:1346–1354`; the call at `:2612–2634`) and hands the value to
  the executor; it never outlives the attempt. The effect it hands the
  resolver carries two time fields: `timeout_ms`, copied from the spec
  `AgentEffectPolicies::spec_for` selects (`effect.rs:936`; copied by
  `AgentRunEffect::new` at `:1895`), and `deadline_at`, which nothing writes
  (`:1896` sets `None`; no other assignment exists in the workspace). The
  dispatcher applies no wall-clock bound of its own; the dispatch gate reads
  `intent.timeout_ms` only to check it against a binding's bound
  (`tools.rs:1759–1760`), and an executor is expected to honour it.
- `AgentGuardrailBoundary` already declares all seven boundaries
  (`guardrails.rs:110`); `AGENT_EVALUATED_GUARDRAIL_BOUNDARIES` names four
  (`tools.rs:128`). `reviewed_tool_outcome` (`dispatch.rs:3312`) is the
  ToolResponse evaluation point to mirror: after execution, before the outcome
  exists, refusal becomes a determinate `Failed` outcome delivered once. A
  failed model effect winds the run down as `EffectFailed { effect_id, code }`
  (`run.rs:4443`).
- Session memory is `SessionMemoryStore { append, read, purge_run }`
  (`memory.rs:734`), windowed by `SessionWindowPolicy { max_entries,
  include_summaries }` (`memory.rs:1400`), assembled by `assemble_context`
  (`retrieval.rs:1074`), appended by the run at `run.rs:7285`.
- The registry precedent is `A2AAgentCatalog { resolve, targets }` with a
  static impl (`agents/catalog.rs`); the card is built from the workflow
  catalog (`agent_card.rs:203`); `AgentEntityState` holds scope, lifecycle
  status, definition and settings revisions, and admission (`agent.rs:234`);
  `Describe` answers `AgentEntityReply::Snapshot`.
- Plans are `AgentCompiledExecutionPlan` (`compiled_plan.rs:126`) with eighteen
  node kinds, ports, edges, artifact-referenced configs, validated by
  `validate_compiled_execution_plan_with_catalog` (`:1403`) and registered as
  `AgentCompiledWorkflowRegistration::new(workflow, plan)`
  (`definition.rs:244`). Definitions are `AgentDefinition::new(id,
  description, envelope)` over a public-field `AgentAuthorityEnvelope`
  (`definition.rs:643`). Both are assembled by struct literal today
  (`examples/agent-otlp-export-acceptance/src/flow.rs:552`).
- Upstream: MCP's current revision is 2026-07-28 (stateless, no session
  header, `server/discover`, `subscriptions/listen`, multi-round-trip
  requests replacing sampling and elicitation, tasks as an extension, roots
  and sampling deprecated); `rmcp` 3.4.0 (2026-09-15) implements it and is
  compatible with 2025-11-25.

## 2. Principles this phase holds

- P1. A public surface is a projection of entity state and a door into the
  durable command path; it never holds correctness state of its own.
- P2. Anything that reaches a model or a tool is an outbox effect with a safety
  class, a generation, and an idempotency key. Streaming and MCP do not add a
  second execution path.
- P3. Secrets exist only as `AgentEphemeralCredential` inside one dispatch
  attempt. Profiles, bindings, directory entries, plans, and frames carry
  logical references at most.
- P4. Every new evaluation point is declared by extending the evaluated-boundary
  constant, so the fail-closed coverage check (`guardrail-stage-unevaluated`)
  starts admitting stages bound there.
- P5. Nothing resident: a stream is a bounded reader over a durable log with a
  lease; a directory is rebuildable.
- P6. Rakka does not own the visual editor, the text DSL, or the compiler
  (CLAUDE.md). A Rust builder that emits the IR is a convenience for Rust
  callers, not a product DSL.

## 3. Mounted agent endpoint

### 3.1 The handler

New module `crates/rakka-a2a/src/agents/handler.rs` under the `agents` feature
(which already implies `server`):

```rust
pub struct RakkaAgentA2ARequestHandler<Tasks, Agents, History, Runs, Teams, TeamHistory, Conversations, ConversationHistory> {
    service: Arc<RakkaAgentA2AService<Tasks, Agents, History, Runs, Teams, TeamHistory, Conversations, ConversationHistory>>,
    agent_card: Arc<dyn AgentCardProducer>,          // section 9.5
    principals: Arc<dyn A2APrincipalResolver>,       // new; header default
    push_configs: Option<A2APushConfigStore>,        // reused type; section 3.6
    streams: AgentStreamSettings,                    // section 7
    drain_gate: A2ADrainGate,
    observer: Option<RequestObserver>,
}

#[async_trait]
impl<...> RequestHandler for RakkaAgentA2ARequestHandler<...> where ...: 'static { /* all eleven methods */ }
```

Method mapping (SDK method → service call):

| SDK method | Service call | Notes |
| --- | --- | --- |
| `send_message` | `service.send(params, &req)` | MUST route through `send`, never `send_message`, because `send` is what dispatches team, conversation, handoff, and human-result envelopes; (revised 2026-09-20) the ingress guardrail runs inside the service (section 6.3), so the handler adds only the SDK error mapping |
| `send_streaming_message` | `service.send` then `stream_task(snapshot_first = true)` | section 7.2; the stream lease is acquired before durable accept, as the workflow surface does; when `send` answers a `Message` rather than a `Task`, the stream emits that message and completes |
| `get_task` | `service.get_task(params, req.tenant.as_deref(), &req.id, principal, req.history_length)` | |
| `list_tasks` | new `service.list_tasks(params, &req, principal)` | authorizes `A2AOperation::ListTasks`, forces the tenant scope onto the request, delegates to `A2ATaskProjectionStore::list` |
| `cancel_task` | `service.cancel_task(params, &req)` | |
| `subscribe_to_task` | `stream_task(snapshot_first = false, after_cursor from `rakka-a2a-replay-cursor` or `last-event-id`)` | |
| `create/get/list/delete_push_config` | `A2APushConfigStore` CRUD under `A2AOperation::PushConfigWrite/Read` | section 3.6 |
| `get_extended_agent_card` | directory-backed per-agent card when a directory is installed, else the static card | section 9.5; selector from `io.rakka.agent.id` metadata or the `x-rakka-agent` header |

Error mapping: `RakkaAgentA2AError` gains `into_a2a_error(self) -> A2AError`
with the same policy as `RakkaA2AHandlerError::into_a2a_error` (`error.rs:104`):
refusals keep their stable Rakka code in the error `data` under
`io.rakka.code`; authorization denials on reads answer not-found (deny is
absent); drain answers the SDK's unavailable error. The mapping table is held
by a test that walks every `RakkaAgentA2AError` variant.

### 3.2 Principal and tenant

Tenant resolution stays `A2ATenantResolver` (`A2AHeaderTenantResolver`,
`x-rakka-tenant`; `mapping.rs:224`, `:257`), authorization stays
`A2AAuthorizer` (`auth.rs:378`). The service methods that take
`principal: Option<&PrincipalRef>` need a principal source the handler does
not have today, so:

```rust
pub trait A2APrincipalResolver: Send + Sync + 'static {
    fn principal(&self, params: &ServiceParams) -> Option<PrincipalRef>;
}
pub struct A2AHeaderPrincipalResolver; // reads `x-rakka-principal`; deployments replace it with their auth middleware's claim
```

Authentication itself remains application-owned (CLAUDE.md: auth is not
Rakka's). A deployment authenticates in an axum layer or an SDK
`CallInterceptor` and stamps the principal header for the resolver, or
supplies its own resolver.

(revised 2026-09-20) Both header resolvers are defaults for examples and
tests, not the shape a production deployment keeps. A deployment whose scope
is derived from the authenticated connection installs its own
`A2ATenantResolver` and `A2APrincipalResolver` and never reads either header;
the handler takes both as `Arc<dyn ..>` and neither default is constructed
unless the deployment asks for it.

### 3.3 Routes

```rust
pub fn agent_a2a_routes(handler: Arc<RakkaAgentA2ARequestHandler<...>>) -> Router;              // card + REST + JSON-RPC at default paths
pub fn agent_a2a_routes_at(handler: Arc<...>, paths: &A2ARoutePaths) -> Router;
pub fn agent_directory_routes(handler: Arc<...>) -> Router;                                        // GET /agents, GET /agents/{agent_id}/card  (section 9.5)
```

Default paths are the same as the workflow surface (`/a2a`, `/a2a/jsonrpc`,
card at `/.well-known/agent-card.json`). One host serves one well-known card.
A deployment that mounts both surfaces mounts the workflow surface under a
sub-path with `a2a_routes_at` and its own card there, which the existing code
already supports; the agents surface is the primary surface. The README of
`examples/clustered-sharded-entity-a2a-agents` documents this layout.

### 3.4 Drain and readiness

`accepts_public_commands` and `begin_drain` are added to the agents handler
with the same semantics as the workflow handler, backed by the shared
`A2ADrainGate`, and `register_agent_workflow_ingress_stop_task` covers it.
Streams end with a terminal heartbeat frame carrying
`io.rakka.stream.event = "draining"` when the gate closes; the task is
unaffected (spec 14.2).

### 3.5 Facade, features, and the compatibility guarantee

No new feature on `rakka-a2a`: `agents` already implies `server`. The facade
re-exports `RakkaAgentA2ARequestHandler`, `agent_a2a_routes`,
`agent_a2a_routes_at`, and `A2APrincipalResolver` from `rakka::a2a` under
`a2a-agents`. `SharedRakkaAgentA2AService` gains a sibling alias
`SharedRakkaAgentA2AHandler`.

(revised 2026-09-20) **Compatibility guarantee.** These keep their signatures
and semantics across the phase: `RakkaAgentA2AService::new`
(`service.rs:177`, thirteen arguments ending in the catalog, the projection
store, the tenant resolver, and the authorizer), `send` (`:416`),
`send_message` (`:952`), `A2AAgentDelegationSendExecutor::new`
(`delegation.rs:98`), `A2AAgentHandoffSendExecutor::new` (`handoff.rs:108`),
and every existing `with_*` builder on the three (`with_metrics` `:226`,
`with_clock` `:273`, `with_default_tenant` `:280`, `with_decision_events`
`:294`, `with_segments` `:312`, `with_goal_claim_source` `:402` on the
service; `with_principal` on each executor, `delegation.rs:118`,
`handoff.rs:128`). Everything sections 3, 6.3, 7, and 9 add to these types is
a new type or a new `with_*` builder whose absence leaves behaviour
byte-identical to today. A deployment that never mounts the handler pays
nothing: no route, no resolver, no push-config store, and no stream settings
are constructed, and an in-process consumer's wiring compiles across the pin
bump without touching a surface it does not mount.

### 3.6 Push notification configs

The four push-config methods store and read under the existing
`A2APushConfigStore`, keyed by tenant and task id, under
`A2AOperation::PushConfigWrite` and `PushConfigRead`, so a client written
against the SDK gets the same answers on both surfaces. Delivery of push
notifications for agent tasks is deferred (section 11.4); until it lands the
agents card advertises `push_notifications: false`, and a stored config is
inert. Storing it now means adding delivery later is a new consumer of the
task event feed (section 7.1), not a schema change.

### 3.7 Tests and example

- `crates/rakka-a2a/tests/agents_http_surface.rs`: tower `oneshot` over the
  router, mirroring `routes.rs:103`, covering card, JSON-RPC and REST send,
  get, list, cancel, tenant header, principal header, an authorizer refusal
  mapped to the SDK error, drain, and every push-config method.
- `crates/rakka-a2a/tests/agents_error_mapping.rs`: every error variant.
- `examples/clustered-sharded-entity-a2a-agents` mounts the agents surface
  beside the workflow surface and its README's expected output gains the agent
  lines; `docs/rakka-agents.md:260` and `:348` are corrected in the same
  change.

## 4. Wired model providers

### 4.1 The profile record

`AgentModelProfileId` stays the opaque id the envelope approves. Behind it, a
deployment-owned record:

```rust
pub struct AgentModelProfile {
    pub profile_id: AgentModelProfileId,
    pub revision: AgentRevisionNumber,
    pub provider: AgentModelProviderKind,
    pub model: String,                               // provider model name, ≤ 128 bytes
    pub base_url: Option<String>,                    // non-default endpoint; no query string, no userinfo
    pub credential_binding: Option<AgentCredentialBindingRef>,
    pub default_sampling: AgentSamplingSettings,
    pub capabilities: AgentModelCapabilities,        // { tool_calls: bool }
    pub attributes: BTreeMap<String, String>,        // bounded, non-secret (e.g. anthropic_version)
}
impl AgentModelProfile {
    pub fn validate(&self) -> Result<(), AgentModelProfileError>;
    pub fn digest(&self) -> AgentContentDigest;      // over the canonical serialization; what the grant records (4.2)
}

#[non_exhaustive]
pub enum AgentModelProviderKind { Anthropic, OpenAiResponses, OpenAiCompletions, AzureOpenAi, Gemini, Ollama, OpenRouter, Custom(String) }

pub trait AgentModelProfileCatalog: Send + Sync + 'static {
    fn profile(&self, id: &AgentModelProfileId) -> Option<AgentModelProfile>;
}
pub struct StaticAgentModelProfileCatalog { .. }     // builder, validated on insert; one implementation among others
```

Validation refuses a `base_url` with userinfo or a query string, any attribute
key on a small deny-list (`api_key`, `token`, `authorization`, `secret`), and
attribute values over 256 bytes. A profile MUST NOT carry credential material;
`credential_binding` is the only way to reach one.

(revised 2026-09-20) The trait returns a projection built on demand, not a
handle into a store. A deployment whose profiles are release data implements
`profile` in one function over its own record and stores nothing new; there is
no second store, no registration step, and no requirement that
`AgentModelProfile` be the record of truth. `StaticAgentModelProfileCatalog`
is one implementation, for tests, examples, and deployments with no release
data, never the required one. `digest()` is over the record's canonical
serialization, so a content-addressed source yields the same digest on every
node and the grant's revision check in 4.2 holds across a release
reassignment. `Custom(String)` stays as the escape for OpenAI-compatible and
other endpoints a deployment names itself.

### 4.2 Selection, authorization, and the credential

The profile the run uses is still chosen where it is today:
`authorize_model` takes `settings.model_profile`, validates it against the
definition and setup envelopes, and puts it on `AgentGrantedDispatch`. Three
additions:

1. `AgentToolAuthority::with_model_profiles(catalog: Arc<dyn AgentModelProfileCatalog>)`.
   With a catalog installed, `authorize_model` resolves the profile record,
   refuses an unknown id (`model-profile-unknown`), runs the existing
   `check_credential` against the profile's binding (so a revoked binding
   refuses the model call exactly as it refuses a tool), and records the
   profile's `revision` and `digest()` on the grant descriptor
   (`AgentGrantDescriptor`), so a recovery that finds a materially different
   profile refuses (`model-profile-revision-mismatch`, spec 11.8's last
   paragraph applied to models).
2. `AgentGrantedDispatch.model_credential_binding: Option<AgentCredentialBindingRef>`
   and the grant's `credential_binding` set from it. The dispatcher's credential
   resolution (`dispatch.rs:2612`) resolves
   `intent.credential_binding.or(granted.grant.credential_binding)` inside the
   attempt, so the `CredentialResolved` kill window and the never-persisted
   contract cover model credentials with no new path.
3. (revised 2026-09-20) **The credential path and its deadline, spelled out.**
   The value `call_with` receives is resolved through
   `AgentEffectCredentialResolver::resolve(scope, binding, effect)`
   (`dispatch.rs:1346–1354`), called at `:2634` with the intent as `effect`,
   and is dropped with the attempt. The resolver's only time input is that
   effect: `AgentRunEffect.timeout_ms` is copied from the spec
   `AgentEffectPolicies::spec_for` selects for the request (`effect.rs:936`;
   `AgentRunEffect::new` copies it at `:1895`), and `AgentRunEffect.deadline_at`
   has no writer at `a757ad2` (`:1896` sets `None`; nothing else in the
   workspace assigns it). So the only deadline a resolver can honour is the
   attempt's, derived from the spec's `timeout_ms`, and this phase makes that
   derivation explicit:
   - The Model effect's `timeout_ms` comes from `AgentEffectPolicies.model`,
     the spec `spec_for` returns for `AgentRunEffectRequest::Model`. A
     deployment sets it through `with_model_spec(spec)` (`effect.rs:776`) on
     an `AgentEffectSpec` built with `with_timeout_ms(..)` (`:431`); the
     policies the dispatcher runs under come from
     `AgentToolAuthority::effect_policies` (`tools.rs:1372`), which is where a
     deployment's request timeout lands.
   - `authorize_model` refuses, with `model-timeout-unset`, a Model intent
     whose grant will carry a credential binding (a resolved profile with
     `credential_binding: Some`) and whose `intent.timeout_ms` is `None`. The
     refusal is at the authority, before any resolver call, so no lease is
     ever asked for. An unprofiled model call, or a profile with no binding,
     is unaffected.
   - The dispatcher stamps `deadline_at = attempt start + timeout_ms` on the
     intent it hands to `resolve`, to `execute`, and to `call_with`, per
     attempt: recomputed on retry and not persisted, because a durable
     `deadline_at` would outlive the generation it bounds. When `timeout_ms`
     is `None` the field stays `None`, which the previous point makes
     impossible for a credential-bearing model call.
   Why this is a rule and not a note: a resolver handed a `None` deadline can
   only ask for its minimum lease, and a lease shorter than the request budget
   resolves and then lapses in the middle of the call. The host closed exactly
   that defect in its own adapter by deriving the deadline from its request
   timeout; a `None` here would re-open it silently for every model call that
   moves onto `call_with`.

### 4.3 The adapter trait

```rust
pub trait AgentModelAdapter: Send + Sync {
    fn adapter_version(&self) -> AgentRevisionNumber;
    fn retry_policy(&self) -> AgentModelRetryPolicy { AgentModelRetryPolicy::DEFAULT }
    fn call<'a>(&'a self, request: &'a AgentModelRequest) -> AgentModelFuture<'a>;
    /// Called by the dispatcher. Default: `self.call(request)`.
    fn call_with<'a>(
        &'a self,
        request: &'a AgentModelRequest,
        credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentModelFuture<'a> { self.call(request) }
}
```

`call_with` is defaulted so `RigModelAdapter`, `DeterministicModelAdapter`, and
every out-of-tree adapter keep compiling. The dispatcher calls `call_with`
only. (revised 2026-09-20) The credential it passes is the one 4.2 item 3
resolved, inside the attempt, under the effect's deadline; an adapter never
resolves a credential itself and never holds a resolver, which is what lets a
deployment that resolves model credentials in its adapter today retire that
code and the authority special-casing that went with it.

### 4.4 Rig provider adapter and router

(revised 2026-09-20)

```rust
pub struct RigProviderAdapter<H> { profile: AgentModelProfile, http: H, adapter_version, retry_policy, result_tool }
impl<H> RigProviderAdapter<H>
where H: rig::http_client::HttpClientExt + Clone + Send + Sync + 'static
{
    /// The only constructor. The adapter never builds an HTTP backend of its own.
    pub fn new(profile: AgentModelProfile, http: H) -> Result<Self, AgentModelProfileError>;   // runs profile.validate()
}
impl<H: ..> AgentModelAdapter for RigProviderAdapter<H> { /* call → call_with(request, None) */ }

pub struct AgentModelRouter { routes: BTreeMap<AgentModelProfileId, Arc<dyn AgentModelAdapter>>, default: Option<Arc<dyn AgentModelAdapter>> }
impl AgentModelAdapter for AgentModelRouter { /* routes on request.profile; unknown → model-profile-unknown; None → default or refuse */ }
```

The adapter takes the HTTP backend it sends through as a constructor argument
and never constructs rig's default client. `reqwest::Client` is the plain
case: `rig-core 0.37.0` links `reqwest` 0.13 as a non-optional dependency
with `default-features = false` and implements `HttpClientExt` for
`reqwest::Client` unconditionally (`rig-core-0.37.0/src/http_client/mod.rs:137`),
so no rig feature is needed to inject one. A deployment with an egress policy
injects its own `HttpClientExt` type instead. The precedent shape is the
host's `crates/rakka-host-runtime/src/providers.rs`: an egress-checked
`reqwest` client adapted to rig's HTTP backend, with the base URL
egress-checked before the provider client is built; this adapter is that
shape with the backend left to the caller.

Per attempt, `call_with` first validates the base URL the call will use
(4.1's rules: it parses as a URL and carries no userinfo and no query string;
`model-profile-invalid-base-url` otherwise) before anything else happens, then
builds the provider client over the injected backend with the ephemeral
credential (`ClientBuilder::new(..).http_client(self.http.clone()).base_url(..)`
— `rig-core-0.37.0/src/client/mod.rs:658` is the backend setter — then
`completion_model(&profile.model)`), issues `completion`, maps the response
through the existing `turn_from_response` and `model_usage`, and drops the
provider client with the credential. The injected backend holds no
credential; nothing long-lived holds the key. A profile whose provider needs
no credential (a local Ollama) takes `None`. Provider-specific attributes
(`anthropic_version`, Azure deployment) come from `profile.attributes` through
the provider's builder extension.

The prompt assembly in `build_request` stays as it is in this phase except for
one change that is a prerequisite for real providers: the request carries the
model-visible tool list. `AgentModelRequest.tools: Vec<AgentToolDescriptor>` is
filled by the dispatcher from `AgentToolRegistry::model_visible(envelope,
settings)` (the security matrix's "descriptor rung" owed item), and the
adapter declares each as a Rig `ToolDefinition` beside the result tool. This
turns "the model is never told which tools exist" into a real surface.
(revised 2026-09-20) The list is the *filtered* model-visible set:
`model_visible` (`tools.rs:723–735`) keeps a descriptor only when the envelope
declares the tool and the current settings have not revoked it
(`envelope.tools.contains_key(tool) && !settings.revoked_tools.contains(tool)`),
so a revoked tool is withheld from the model rather than offered and refused
after the model calls it. A deployment that assembles this list itself today
deletes that code and reads the request. The context snapshot content into
the prompt is unchanged and remains its own slice.

### 4.5 Features and pins

(revised 2026-09-20; reverses decision 5)

- `rakka-agent` `rig` feature:
  `rig-core = { version = "=0.37.0", default-features = false, features = ["rustls"], optional = true }`
  (today `crates/rakka-agent/Cargo.toml:44` has no feature list). Verified in
  `rig-core 0.37.0`'s manifest: `rustls = ["reqwest/rustls",
  "tokio-tungstenite?/rustls-tls-webpki-roots"]` selects a TLS backend and
  nothing else (the tungstenite half is inert without a websocket feature);
  `reqwest = ["reqwest/charset", "reqwest/http2", "reqwest/system-proxy"]`,
  and it is part of rig's `default`. `system-proxy` makes every provider call
  honour the environment's proxy variables, which is an egress bypass, and
  under Cargo feature unification any crate in a consumer's dependency graph
  that enabled it would switch it on for the consumer's own `rig-core`,
  whether or not that consumer asked. The `reqwest` feature is therefore
  never enabled by any Rakka crate, example, or test. Rig does not gate
  providers individually, so `rustls` alone reaches all of them; the
  workspace has no other TLS backend.
- The host builds `rig-core` with `default-features = false, features =
  ["rustls"]` for exactly this reason and keeps the facade's `agent-rig`
  feature (`crates/rakka/Cargo.toml:34`) off, with `rakka-agent` at
  `default-features = false` (`:60`). This pin makes Rakka's `rig` feature
  safe to enable beside such a build: it adds a TLS backend the host already
  selected and no proxy behaviour.
- Rig's `rmcp` feature stays off: tool dispatch is Rakka's, never Rig's.
- The pin review note in `rig.rs` gains the client-builder surface and the
  feature list to its compatibility checklist.

### 4.6 Telemetry

`gen_ai.provider.name` and `gen_ai.request.model` come from the profile;
`gen_ai.response.model`, finish reason, cached and reasoning tokens (17.8's
"provider fields have no slot") get slots on `AgentModelUsage` /
`AgentModelTurn` as optional fields, filled by the Rig adapter where the
provider reports them. Additive, schema note in section 11. A deployment's
own adapter fills the same slots and its spans gain them from the segment it
already writes.

### 4.7 Tests and example

- `crates/rakka-agent/tests/model_profile_catalog.rs`: validation, refusal
  codes, narrowing (a settings revision may only select an approved id); a
  catalog implemented over a fixture record type that is not
  `AgentModelProfile` (the deployment-owned case), with digest stability
  across two nodes' projections.
- `crates/rakka-agent/tests/model_provider_dispatch.rs`: the authority puts the
  profile's binding on the grant; the dispatcher resolves it; the credential is
  not on any persisted record (extends `secret_exclusion.rs`'s scan to the new
  types); a revoked binding refuses the model call; a revision mismatch refuses;
  (added 2026-09-20) a credential-bearing model call with no `timeout_ms` is
  refused `model-timeout-unset` before the resolver is called, and the intent
  a `ScriptedCredentialResolver` receives carries `deadline_at` equal to the
  attempt start plus `timeout_ms`, recomputed on the retry.
- `crates/rakka-agent/tests/rig_provider_fake_endpoint.rs`: an in-process axum
  fake of the OpenAI-compatible completions API and of the Anthropic messages
  API; the real `RigProviderAdapter` runs against both over an injected
  `reqwest::Client`, including tool calls and usage mapping; a counting
  `HttpClientExt` fake proves every send goes through the injected backend
  and an invalid base URL is refused before it is touched. No network, no key.
- `examples/durable-agent-acceptance --provider`: gated on
  `RAKKA_MODEL_PROFILE` plus the provider's credential env var, runs the same
  walk against a live provider through an env-backed credential resolver that
  lives in the example (never in a crate). Documented like the PostgreSQL
  gates in CLAUDE.md.

## 5. MCP client

### 5.1 Crate

(revised 2026-09-20) New adapter-tier crate `crates/rakka-agent-mcp`,
`publish = false` like its siblings. Pins `rmcp = "=3.4.0"` with `client`,
`transport-streamable-http-client-reqwest`, and `reqwest`. Verified in
`rmcp 3.4.0`'s manifest: `transport-streamable-http-client-reqwest =
["transport-streamable-http-client", "__reqwest"]`, `reqwest = ["__reqwest",
"reqwest?/rustls"]`, and its `reqwest` dependency is `0.13.2` with
`default-features = false, features = ["json", "stream"]`; none of these
enables `system-proxy`, so the crate carries the same TLS-only stance as 4.5.
`transport-child-process` (`["transport-async-rw", "tokio/process",
"dep:process-wrap"]`) is enabled only under the crate's own `child-process`
feature, off by default (5.3, condition b). The pin, its feature list, and
the MCP revision (`2026-07-28`, compatible `2025-11-25`) are recorded in the
compatibility document's pin table. Facade feature `agent-mcp`.

### 5.2 Server binding and descriptor sync

```rust
pub struct McpServerBinding {
    pub server_id: McpServerId,                       // validated id
    pub transport: McpTransport,                      // StreamableHttp { url } | ChildProcess { spec_ref: ArtifactRef }
    pub credential_binding: Option<AgentCredentialBindingRef>,
    pub protocol_versions: Vec<String>,               // preferred first; default ["2026-07-28", "2025-11-25"]
    pub tools: BTreeMap<String, McpToolPolicy>,       // allow-list; a tool not listed is not registered
    pub refresh: McpDescriptorRefresh,                // Manual (default) | Interval { millis } | Listen
}
pub struct McpToolPolicy {
    pub declaration: AgentToolDeclaration,            // safety, capabilities, environments — operator-declared
    pub max_attempts: u32,
    pub timeout_ms: Option<u64>,
    pub result_behavior: AgentToolResultBehavior,
    pub honor_hints: bool,                            // default false
}
```

(revised 2026-09-20) Descriptor sync is a standalone function, not a step of
registry construction:

```rust
pub async fn sync_mcp_descriptors<C>(
    http: &C,                                         // the injected transport client (5.3, condition a)
    binding: &McpServerBinding,
    credential: Option<&AgentEphemeralCredential>,
) -> Result<McpDescriptorSet, McpSyncError>
where C: <rmcp's Streamable HTTP client trait> + Clone + Send + Sync + 'static;

pub struct McpDescriptorSet { pub server_id: McpServerId, pub protocol_version: String, pub synced_at: AgentTimestampMillis, pub descriptors: Vec<McpSyncedDescriptor> }
pub struct McpSyncedDescriptor {
    pub tool: String,
    pub binding: AgentToolBinding,                    // descriptor + the operator's declaration, named `mcp.<server_id>.<tool>`
    pub input_schema: serde_json::Value,              // raw, always returned
    pub schema_digest: AgentContentDigest,            // what the grant records and 5.3 rechecks
    pub output_schema_digest: Option<AgentContentDigest>,
}
```

It performs no I/O beyond `server/discover` and `tools/list`, touches no
store, and returns each allowed tool's `AgentToolBinding` together with the
raw input schema and its digest, so a deployment runs it at publish time — an
administrative operation, outside any run — and stores the result as release
data. It converts each allowed tool to an `AgentToolDescriptor { kind:
RemoteMcp, description, parameters: inputSchema when ≤ 4 KiB, output_schema
from outputSchema digest, version: from the schema digest }`; a schema over
4 KiB is returned raw with the descriptor's `input_schema` left unset, for the
caller to store as an artifact and reference (`McpSyncedDescriptor::with_input_schema_ref`);
a schema over `MCP_DESCRIPTOR_SCHEMA_MAX_BYTES` (64 KiB) is refused
`mcp-descriptor-schema-too-large`; a transport or protocol failure during
sync is `mcp-descriptor-sync-failed` (the version fallback of 5.3 applies
first). For a child-process binding the same function runs over the
transport the launcher (5.3) produced. MCP tool annotations (`readOnlyHint`,
`idempotentHint`, `destructiveHint`) never widen a declaration; with
`honor_hints` they may only narrow (a `destructiveHint: true` on a tool
declared `Idempotent` refuses registration,
`mcp-hint-contradicts-declaration`). Names are prefixed
`mcp.<server_id>.<tool>` to keep the registry namespace flat and stable.

`McpDescriptorRefresh::Manual` is the default: a registry built from a stored
`McpDescriptorSet` makes no network call at construction, which is what
"agents are instantiated only from releases" requires. `Interval` and `Listen`
are optional, for a deployment that owns its own registry-rebuild schedule:
`Listen` subscribes to `subscriptions/listen` for `toolsListChanged` and marks
the binding stale; `Interval` re-runs the sync on a timer and marks it stale
on a digest change. Neither rebuilds a registry by itself.

Because `AgentToolRegistry` is immutable, a descriptor change is a new
registry build. The grant already records the descriptor's schema digest; the
executor compares it against the live `tools/list` entry (cached under the
result's `ttlMs`) and refuses on mismatch (`tool-descriptor-revision-mismatch`),
which is spec 11.8's "recovery MUST NOT silently execute against a materially
different schema" made concrete. This dispatch-time recheck stays whatever the
refresh mode: stored descriptors say what was published; the recheck says
whether the server still agrees.

Rule preserved: MCP is never an agent-to-agent channel (spec 14.4). A
`McpServerBinding` whose `server/discover` identity names a Rakka agent server
(`serverInfo.name` prefixed `rakka-agent`) is refused at registration
(`mcp-peer-agent-channel-refused`), so a later Rakka MCP server cannot become
a side channel between agents.

### 5.3 Client executor

(revised 2026-09-20)

```rust
pub struct McpDispatchToolExecutor<C> {
    bindings: BTreeMap<McpServerId, McpServerBinding>,
    descriptors: BTreeMap<McpServerId, McpDescriptorSet>,   // the stored sync output the registry was built from
    artifacts: Arc<dyn AgentArtifactStore>,
    clock: Arc<dyn AgentClock>,
    http: C,                                                  // injected; never built here
    egress: Arc<dyn McpEgressCheck>,                          // required (revised 2026-09-20, R17)
    launcher: Option<Arc<dyn McpChildProcessLauncher>>,
}
impl<C> McpDispatchToolExecutor<C> where C: <rmcp's Streamable HTTP client trait> + Clone + Send + Sync + 'static {
    pub fn new(descriptors: Vec<McpDescriptorSet>, bindings: Vec<McpServerBinding>, artifacts: Arc<dyn AgentArtifactStore>, clock: Arc<dyn AgentClock>, http: C, egress: Arc<dyn McpEgressCheck>)
        -> Result<Self, McpRegistrationError>;               // refuses ChildProcess bindings without a launcher: mcp-transport-unsupported
    pub fn with_child_process_launcher(self, launcher: Arc<dyn McpChildProcessLauncher>) -> Result<Self, McpRegistrationError>;
}
pub struct McpAllowAllEgress;                                 // the explicit opt-out: implements McpEgressCheck as always-Ok, for in-cluster and test use
impl<C: ..> AgentDispatchToolExecutor for McpDispatchToolExecutor<C> { fn execute(scope, intent, call, credential) -> AgentDispatchFuture<AgentTaskContent> }   // dispatch.rs:409–418

pub trait McpEgressCheck: Send + Sync + 'static {
    /// Runs per attempt, before any client for the attempt exists.
    fn check(&self, server: &McpServerId, url: &str) -> Result<(), AgentAuthorityRefusal>;
}
pub trait McpChildProcessLauncher: Send + Sync + 'static {
    /// Produces the stdio transport for one attempt from the binding's spec artifact, inside the deployment's own sandbox.
    fn launch<'a>(&'a self, scope: &'a AgentRunScope, spec: &'a ArtifactRef) -> McpFuture<'a, McpChildTransport>;
}
```

The three conditions, each a stated invariant of the host:

- **(a) Transport client injection.** The executor takes the HTTP client it
  sends through as a constructor argument and never builds one;
  `reqwest::Client` under `transport-streamable-http-client-reqwest` is the
  plain case, and a deployment with an egress policy injects its own
  implementation of rmcp's client trait, the same shape 4.4 gives the Rig
  adapter. Before the client for an attempt is built, the binding's Streamable
  HTTP URL is handed to the executor's `McpEgressCheck`; a refusal is a
  determinate dispatch failure under the refusal's own code (the deployment's
  vocabulary; Rakka adds no code for it), and the credential the dispatcher
  resolved for the attempt is dropped unused. (revised 2026-09-20, R17) The
  check is a required argument of `new`, not a builder: an executor cannot
  exist without one, so no deployment can forget it, and the only way to run
  without a policy is to pass `McpAllowAllEgress` by name, which is the
  in-cluster and test case and is visible in review. This is stricter than
  the fail-closed refusal the host suggested and needs no runtime check.
- **(b) No child process without a launcher.** `McpTransport::ChildProcess`
  stays a variant so a binding deserializes and is refused deterministically:
  `new` refuses it with `mcp-transport-unsupported` unless a launcher is
  installed first through `with_child_process_launcher` (which is why that
  builder is fallible: it re-validates the bindings). The crate ships no
  launcher by default. rmcp's `transport-child-process` is compiled only under
  the crate's `child-process` feature, which also ships
  `TokioChildProcessLauncher`, an unsandboxed reference launcher for tests
  and examples, documented as such. A deployment that runs tools inside its
  own sandbox implements `McpChildProcessLauncher` over that sandbox and never
  enables the feature.
- **(c) Descriptors are release data; the credential path is the existing
  one.** The executor is built from stored `McpDescriptorSet`s (5.2), never
  from a live listing, and the only credential path is
  `binding.credential_binding` through `AgentEffectCredentialResolver`: the
  dispatcher resolves it inside the attempt and passes it to `execute`; the
  executor sets the binding's declared header (`Authorization: Bearer ..` by
  default) from it for that attempt and never reads an environment variable,
  a file, or a profile attribute for one.

Per attempt: build an `rmcp` client for the binding's transport over the
injected client or the launcher's transport (stateless in 2026-07-28, so per
attempt is the natural unit and holds the credential only that long), set
`_meta` with the protocol version, `clientInfo` (`rakka-agent-mcp/<version>`),
`traceparent`/`tracestate` from the intent's telemetry context, and
`io.rakka.idempotency-key` from the effect's external idempotency key so an
idempotent server can deduplicate; send `tools/call` with `call.arguments`;
map:

| MCP result | Outcome |
| --- | --- |
| `resultType: complete`, `structuredContent` or text ≤ 2 KiB | `AgentTaskContent::Inline` |
| larger content, or binary parts | written to the artifact store, `AgentTaskContent::Artifact` (the binding's `result_behavior` must be `ArtifactReference`, else `mcp-result-too-large`) |
| `isError: true` | dispatch error `mcp-tool-error` with bounded detail (512 bytes) |
| `resultType: input_required` (MRTR) | `mcp-input-required`, determinate failure in this phase |
| `UnsupportedProtocolVersionError` | retried once with the next listed version, then `mcp-protocol-unsupported` |
| transport error, timeout from `intent.timeout_ms` | the existing failure classes; safety class decides retry |

Guardrails at `ToolRequest` and `ToolResponse` apply unchanged; an MCP response
is untrusted content and enters memory classified `Unclassified` like any tool
result.

Composition: the dispatcher holds one `tools: Arc<dyn AgentDispatchToolExecutor>`,
so `rakka-agent` gains `AgentToolExecutorRouter`, routing by tool id prefix or
binding kind to an executor, with a fallback. MCP, function, and process
executors compose through it; a deployment with its own tiers wires the MCP
executor as one more route and keeps its tiers as they are.

### 5.4 Tests

- `crates/rakka-agent-mcp/tests/descriptor_sync.rs`: an in-process `rmcp`
  server with three tools, one with a contradicting hint; sync output and
  refusals; (added 2026-09-20) the sync runs with no registry and no store
  and its output round-trips through serialization; a registry built from
  the stored set makes no network call (the fake server counts `tools/list`);
  `Manual` is the default; the 4 KiB and 64 KiB schema bounds.
- `tests/client_dispatch.rs`: `tools/call` through the real dispatcher with
  `ScriptedCredentialResolver`; idempotency key in `_meta`; artifact overflow;
  `isError`; MRTR refusal; version fallback; descriptor mismatch refusal; the
  `secret_exclusion` scan extended to MCP types; (added 2026-09-20) a counting
  client fake proves every send goes through the injected client; an egress
  check refusal fails the attempt before any client is built and after the
  credential was dropped, and `McpAllowAllEgress` is the one opt-out (R17);
  a `ChildProcess` binding is refused
  `mcp-transport-unsupported` without a launcher and runs through a fake
  launcher with one; the credential arrives only through the resolver.

## 6. Response guardrails

### 6.1 The model-response evaluation point

`AgentDispatchAuthority` gains a third required method, for the reason the
trait doc gives for the second one (a wrapper that forgets to forward it must
not build):

```rust
fn review_model_response<'a>(&'a self, scope: &'a AgentRunScope, intent: &'a AgentRunEffect, turn: AgentModelTurn)
    -> AgentDispatchFuture<'a, AgentModelResponseDecision>;

pub enum AgentModelResponseDecision { Accepted(Box<AgentModelResponseReview>), Refused(AgentAuthorityRefusal) }
pub struct AgentModelResponseReview { pub turn: AgentModelTurn, pub transforms: Vec<AgentGuardrailTransform>, pub reports: Vec<AgentGuardrailReport> }
```

`AgentToolAuthority::review_model_response` evaluates the chain at
`ModelResponse` over a bounded JSON view of the turn:
`{ "kind": "model-response", "text", "tool_calls": [{ "tool", "arguments" }], "proposal", "profile", "turn" }`.
The dispatcher's `invoke` Model arm calls it after `turn.validate()` and before
constructing `AgentRunEffectOutcome::Model`, mirroring `reviewed_tool_outcome`:

| Outcome | Effect |
| --- | --- |
| Allow | unchanged turn |
| Transform | the reviewed turn replaces the original; it is re-validated (`AgentModelTurn::validate`) and refused if a transform pushed it over a bound (`guardrail-transform-invalid`); transforms and reports are logged and attached to the `model-inference` segment; the durable turn is the transformed one, which satisfies spec 16's "a retry MUST reuse the accepted transformed input" because the turn is committed exactly once by `RecordEffectResult` |
| ReportOnly | reports logged and attached; turn unchanged |
| Block | `AgentRunEffectOutcome::Failed { code: "guardrail-blocked", message }`, the stage id and its reason code in the message, through the `ToolResponse` mapping (`refuse_guardrail_disposition`) unchanged (plan refinement 2026-09-20: the first draft said the stage's reason code; the precedent and the host's terminal-code classification say `guardrail-blocked`); the run winds down as `EffectFailed`, delivered once, never retried, exactly as a refused tool response |
| RequireCheckpoint | fails closed under `checkpoint-required`, exactly the `ToolResponse` precedent (plan refinement 2026-09-20: no new code); gating an already-produced answer behind a human is a follow-up |

`AGENT_EVALUATED_GUARDRAIL_BOUNDARIES` and
`AGENT_AUTHORITY_EVALUATED_GUARDRAIL_BOUNDARIES` both gain `ModelResponse`, so
a mandatory stage bound there starts satisfying coverage. Because the reviewed
turn is what `RecordEffectResult` commits, the assistant text appended to
session memory at `run.rs:7285` is the sanitized text, closing the memory
note under #70.

(revised 2026-09-20) This evaluation point needs no provider to be proven.
The tests in 6.5 drive `DeterministicModelAdapter` (`testkit.rs:1856`)
through the real dispatcher, and the refusal shape is `reviewed_tool_outcome`'s
(`dispatch.rs:3312`), so this section depends on nothing in section 4 and is
the first slice of the phase. The required method is deliberate: a wrapper
authority that forgets to forward it must fail to build rather than silently
drop the boundary, which is the lesson `review_tool_response` already taught
(`dispatch.rs:1471`, `:1714`).

### 6.2 Content bound

`AGENT_GUARDRAIL_CONTENT_MAX_BYTES` (`guardrails.rs:98`, `8 * 1024`) rises to
16 KiB, equal to `AGENT_MODEL_TURN_MAX_BYTES`, so no valid turn is truncated
at this boundary. `evaluate_bounded`'s clamp (`:678`) follows the constant.
Compatibility note in section 11; a changelog line records it, since a
consumer references neither constant by value.

### 6.3 A2A ingress and egress

Closing #70 fully:

- `A2aIngress` (revised 2026-09-20; refined by the slice 7.2 plan the same
  day): evaluated inside `RakkaAgentA2AService`, at the authorized leaf every
  public content entry reaches after `normalized_send` (`service.rs:490`).
  `send_message_normalized`, `team_command_normalized`, and
  `conversation_command_normalized` each call one shared helper exactly once,
  directly after their own authorization (authorization lives in those
  leaves, not in `normalized_send`); the public entries — `send` (`:447`),
  `team_command` (`:529`), `conversation_command` (`:638`), and
  `send_message` (`:961`) — all end in one of those leaves, so an
  in-process caller is covered whether it enters through `send` or, as
  Rakka's own delegation and handoff executors do at `a757ad2`
  (`delegation.rs:318`, `handoff.rs:425`), through `send_message`. The HTTP
  handler (section 3) calls `send` and adds only the SDK error mapping; it
  evaluates nothing itself. The chain is installed through a new builder,
  `RakkaAgentA2AService::with_ingress_guardrails(chain: Arc<AgentGuardrailChain>)`;
  with none installed `normalized_send` is unchanged and evaluates nothing.
  The evaluation runs after tenant resolution and authorization (a caller the
  authorizer denies never reaches a stage) and before any entity command,
  over the message's parts (`{ "kind": "a2a-ingress", "parts": [...] }`,
  bounded to the content limit; oversized parts are digested and marked
  truncated), once per request (`send` evaluates nothing itself; the leaf it
  routes to does). The view also carries the free-text fields of a
  collaboration cluster (`body`, `reason`, `context`), because a conversation
  turn's text rides the cluster, not the parts. The context names an
  `AgentGuardrailSubject` (`Task`, `Team`, or `Conversation`), since no run
  exists at ingress. Block answers `RakkaAgentA2AError::Refused { code:
  "guardrail-blocked", message }` (`error.rs:52`; the stage and its reason
  code in the message), which the handler maps to
  the SDK invalid-request error with the code under `io.rakka.code`, and
  which an in-process send executor records as a determinate send failure
  under that code, the disposition it gives any other send failure; Transform
  replaces the parts the request is normalized from; RequireCheckpoint fails
  closed. `manage_agent` (`:731`) carries a command, not content a model will
  see, and stays outside the boundary. Attestation mirrors
  `with_memory_ingress` (`tools.rs:1233–1247`):
  `AgentToolAuthority::with_a2a_guardrails(declaration: AgentContentDigest) -> Result<Self, AgentGuardrailError>`
  refuses `guardrail-chain-mismatch` unless the digest equals
  `declared_chain()` (`:1270`), and the authority's coverage counts
  `A2aIngress` and `A2aEgress` only once attested — so a deployment that
  installs no chain on the service keeps refusing a mandatory stage bound
  there as unevaluated, and one that installs a different chain cannot start.
- `A2aEgress`: `A2AAgentDelegationSendExecutor` and
  `A2AAgentHandoffSendExecutor` evaluate the chain over the outbound
  collaboration message before sending, installed through a new
  `with_egress_guardrails(chain)` on each executor; Block is a determinate
  send failure (a failed delegation disposition, as today for other send
  failures). A deployment that wires the two executors gets the point with
  one builder call each; without it the executors are unchanged.

Both boundaries join `AGENT_EVALUATED_GUARDRAIL_BOUNDARIES`. With that, all
seven declared boundaries have evaluation points.

### 6.4 Built-in stages

A `guardrails::builtin` module with deterministic, dependency-free rules:
`MaxTextLength`, `DenySubstrings` (case-folded literal list, bounded),
`RequireResultTool` (a model response at `DecidingContinuation` must call the
result tool or a declared tool), and `ReportOnly<R>` wrapper. No regex (not a
workspace dependency) and no model-backed classifier: a stage that needs a
model is itself an external effect and belongs in a follow-up that runs it
through the outbox. Welcome but not required by any consumer: a deployment's
own stages stay where they are.

### 6.5 Tests

`crates/rakka-agent/tests/model_response_guardrails.rs`, on
`DeterministicModelAdapter` and no provider: each outcome through the real
dispatcher; the transformed turn is what the run records and what session
memory holds; a blocked turn ends the run `EffectFailed` with the stage's code
after exactly one invocation and is never retried; coverage now admits a
`ModelResponse`-only mandatory stage; a chain upgraded while parked is
honoured on the next attempt (the `wait_invalidation.rs` pattern).
`crates/rakka-a2a/tests/ingress_egress_guardrails.rs` for 6.3: (revised
2026-09-20) the ingress stage fires for a message entering through each of
the four public entries, exactly once for a routed `send`, and for a
delegation delivered in-process by the real executor with no handler
mounted; a service with no chain installed is byte-identical in behaviour;
a mismatched attestation refuses at startup. The security matrix's #70 row
moves to "Met".

## 7. Streaming

### 7.1 The task event feed

The agents surface streams what the task history already records.
`AgentCoordinationEvent::from_task_history` and `replay_task_events` are the
existing mapping from `AgentTaskHistoryEntry` to `A2ATaskEvent`. What is missing
is a wake-up and a shared store:

```rust
pub trait AgentTaskHistoryWatcher: Send + Sync + 'static {
    /// Resolves when an entry past `after` may exist, or after the bounded wait.
    fn watch<'a>(&'a self, scope: &'a AgentTaskScope, after: AgentTaskHistorySequence, max_wait: Duration) -> AgentTaskHistoryFuture<'a, AgentTaskHistorySignal>;
}
```

Implementations: the in-memory history store (a broadcast per scope, single
node) and `PostgresAgentTaskHistoryStore` (LISTEN/NOTIFY on
`rakka_agent_task_history_<tenant-hash>` channels, precedent
`postgres_watcher.rs`). The Postgres store is new in this phase, in
`rakka-agent-postgres`, with `PostgresAgentTeamHistoryStore` and
`PostgresAgentConversationHistoryStore` beside it and their idempotent
migrations under the existing advisory lock; the memory conformance harness
pattern is reused as a history conformance harness run in-memory and on
PostgreSQL. This is the prerequisite the summary flags: without a shared
history store, a stream on pod A cannot see an entry pod B appended.

(revised 2026-09-20) **Optional, all of it.** `AgentTaskHistoryWatcher` is a
separate trait: never a supertrait of `AgentTaskHistoryStore`
(`task.rs:3158`), `AgentTeamHistoryStore` (`team.rs:689`), or
`AgentConversationHistoryStore` (`conversation.rs:963`), never an argument of
`RakkaAgentA2AService::new` or of any existing method, and never a bound on
the service's generics. The handler takes it through
`with_task_history_watcher(..)`; without one it streams by bounded polling of
the store it already has, with `watch_max_wait` as the poll interval, which is
correct and merely slower. The three Postgres stores are new types in
`rakka-agent-postgres` behind its existing feature; their migrations run only
when a deployment constructs them, and nothing in the service, the runtime, or
the facade constructs one. The `assert_task_history_store_contract`,
`assert_team_history_store_contract`, and
`assert_conversation_history_store_contract` harnesses (`testkit.rs:4371`,
`:4564`, `:4752`) keep their signatures and their contract; the watcher leg is
a separate `check_task_history_watcher_contract` (and two siblings) that a
store without a watcher never runs. An application that already owns
Postgres-backed history stores is unaffected: it implements nothing new,
migrates nothing, keeps passing the harnesses it runs today, and may adopt a
LISTEN/NOTIFY wake on its own log whenever it measures the polling cost to
matter.

### 7.2 Task-state SSE

`stream_task(tenant, task_id, after_cursor, snapshot_first)` on the agents
handler produces `BoxStream<'static, Result<StreamResponse, A2AError>>` exactly
in the workflow surface's shape (`handler.rs:1333`), with these rules:

- a stream lease from `A2AStreamLimits` per task, acquired before the durable
  accept on `send_streaming_message`;
- `snapshot_first` emits `StreamResponse::Task(get_task(..))`, then replays
  `replay_task_events(after_cursor)` pages, then waits on the watcher (or the
  polling fallback), then reads the next page; every frame carries
  `io.rakka.replay.cursor` (`AgentCoordinationCursor::encode`) and
  `io.rakka.task_event.kind`;
- a heartbeat `StatusUpdate` every `stream_heartbeat_interval` (15 s) with
  `io.rakka.stream.event = "heartbeat"`;
- a cursor older than the retained window answers a resync frame (the current
  snapshot) with `io.rakka.stream.event = "resync"` and continues from the
  head, the `WindowExpired` arm carried to the wire;
- the stream ends after the terminal entry, on lease loss, or on drain; the
  task is never affected;
- coordination scopes (team, conversation, run) are reachable through a
  Rakka-extension subscribe (`io.rakka.scope` metadata) over
  `replay_coordination_events`, so a client can follow a team board or a
  moderated conversation live.

Settings: `AgentStreamSettings { heartbeat_interval, watch_max_wait, page_limit }`.

Frames are projections of durable history entries and nothing else: no frame
is emitted that does not correspond to a history entry with a cursor, apart
from heartbeat, resync, and draining markers, which carry no content.

### 7.3 Tests

`crates/rakka-a2a/tests/agents_streaming.rs` (snapshot-first, cursor replay,
resync on expired window, heartbeat, drain, lease limit, terminal end,
coordination-scope subscribe, and (added 2026-09-20) the polling fallback with
no watcher installed);
`crates/rakka-agent-postgres/tests/history_conformance.rs` (gated): the
existing `assert_*` harness unchanged, then the separate `check_*` watcher
leg.

## 8. Memory modes

### 8.1 Policy

`SessionWindowPolicy` stays; the mode is a sibling on `AgentRunMemory`:

```rust
pub enum SessionMemoryMode {
    ReadWrite,                                   // today's behaviour, default
    ReadOnly,                                    // assemble reads; the run appends nothing
    WriteOnly,                                   // the run appends; assemble includes no session entries
    Filtered { roles: BTreeSet<MemoryEntryRole>, exclude_classifications: BTreeSet<MemoryClassification> },
    Disabled,                                    // neither; the snapshot's session is empty and nothing is appended
}
impl AgentRunMemory { pub fn with_mode(self, mode: SessionMemoryMode) -> Self }
```

Applied at the two existing sites: `assemble_session_context`
(`retrieval.rs`) honours `ReadOnly | ReadWrite | Filtered` and yields an empty
session for `WriteOnly | Disabled`; the run's append at `run.rs:7285` is
skipped under `ReadOnly | Disabled` with a debug event, never an error. The
mode is recorded on `MemoryContextSnapshot` (`session_mode` label) so retry
determinism and audit can see it. A per-agent override rides
`AgentSettingsChange::MemoryMode(SessionMemoryMode)` (turn-bound), validated so
an agent may only narrow what the deployment's bundle allows
(`ReadWrite` bundle admits any mode; a `ReadOnly` bundle admits `ReadOnly` or
`Disabled`).

(revised 2026-09-20) The mode is a plain field of the bundle a deployment
builds once — `AgentRunMemory::new(session, snapshots)` (`memory.rs:2677`),
then `with_mode`, beside `with_window` (`:2692`) — and it can be set from
release or configuration data at that point. A deployment that binds one
bundle per node and needs a per-agent mode sets it through the settings
change above; it never needs a second bundle.

### 8.2 Why not a per-request policy

Akka sets memory per agent call. Rakka's unit is the run, and the run's memory
bundle is what the authority attests (`with_memory_ingress`). Keeping the mode
on the bundle, narrowable by settings, keeps the attestation meaningful.

(revised 2026-09-20) The mode is not an input to that attestation.
`with_memory_ingress` (`tools.rs:1233–1247`) compares the retrieval bundle's
chain `declaration_digest()` (`declared_by`, `:1278`) with the authority's own
(`declared_chain`, `:1270`); the mode is part of neither digest and this phase
does not add it. Changing a mode — at boot from release data, or through a
settings revision at runtime — therefore neither fails the attestation nor
requires a rolling restart, while a chain change still does, as it should.
The mode is audited where it acts instead: on the snapshot's `session_mode`
label and in the settings revision the turn resolved against.

`include_summaries` on the window policy keeps its meaning; producing a
`Summary` entry is deferred (section 11.4), so in this phase the flag selects
summaries an application wrote through its own path.

### 8.3 Tests

`crates/rakka-agent/tests/session_memory_modes.rs`: each mode at both sites,
narrowing refusals, snapshot label, scenario 17 determinism unchanged, and
(added 2026-09-20) a mode change on an attested bundle leaves
`with_memory_ingress` green.

## 9. Registry: the agent directory

### 9.1 What it is

A tenant-scoped, durable read model of agent entities, derived purely from
`AgentEntitySnapshot`, that answers the questions Akka's registry answers and
`A2AAgentCatalog` cannot: enumerate agents, find by skill or task definition,
read lifecycle and admission, and build cards. It is never the authority (P1)
and never a sixth entity class.

```rust
pub struct AgentDirectoryEntry {
    pub scope: AgentScope,                                    // (TenantId, AgentId)
    pub definition_id: AgentDefinitionId,
    pub definition_revision: AgentRevisionNumber,
    pub settings_revision: AgentRevisionNumber,
    pub lifecycle: AgentLifecycleStatus,
    pub lifecycle_revision: AgentRevisionNumber,
    pub admitted: bool,
    pub description: String,
    pub task_definitions: BTreeSet<AgentTaskDefinitionId>,
    pub skills: BTreeSet<AgentCapabilityId>,                  // union of tool-declared capabilities
    pub coordination: BTreeSet<AgentCoordinationCapabilityKind>,
    pub operation_classes: BTreeSet<AgentOperationClass>,
    pub model_profiles: BTreeSet<AgentModelProfileId>,
    pub updated_at: AgentTimestampMillis,
    pub source_operation: AgentOperationId,                   // the entity operation this entry reflects
}
pub trait AgentDirectoryStore: Send + Sync + 'static {
    fn upsert<'a>(&'a self, entry: &'a AgentDirectoryEntry) -> AgentDirectoryFuture<'a, AgentDirectoryWrite>;   // idempotent on (scope, lifecycle_revision, definition_revision, settings_revision); stale writes are no-ops
    fn get<'a>(&'a self, scope: &'a AgentScope) -> AgentDirectoryFuture<'a, Option<AgentDirectoryEntry>>;
    fn query<'a>(&'a self, tenant: &'a TenantId, filter: &'a AgentDirectoryFilter, cursor: AgentDirectoryCursor) -> AgentDirectoryFuture<'a, AgentDirectoryPage>;
    fn remove<'a>(&'a self, scope: &'a AgentScope) -> AgentDirectoryFuture<'a, ()>;
}
```

Stores: `InMemoryAgentDirectoryStore`; `PostgresAgentDirectoryStore`
(`rakka_agent_directory`, GIN on skills and task definitions). No secret can
be present: the entry is derived from the definition and settings, neither of
which holds one, and the `secret_exclusion` scan covers the new record.

### 9.2 How entries get written

Three writers, all idempotent on the entity's revisions, none of them a second
writer of entity state, and (revised 2026-09-20) none of them active unless a
deployment installs a directory:

1. The agents A2A service after every management command it applies
   (`manage_agent`) and after `Instantiate` through `send`'s agent-creating
   path, from the `AgentEntityReply::Applied` outcome's snapshot: the same
   place it upserts the task projection today. Installed through a new
   `RakkaAgentA2AService::with_directory(store)`; without it the service
   writes no directory entry.
2. `AgentDirectoryPublisher::publish(reply)` for applications that apply
   `AgentEntityCommand` directly (the acceptance examples do), one line after
   the `ask`.
3. `AgentDirectorySweep`: a bounded repair that lists agent entity scopes for
   a tenant from the entity store's persistence-id listing
   (`DurableStateStore::persistence_ids`) and re-derives entries, the
   `recover_task_projections` precedent. It is also how a directory is first
   built for an existing deployment. (revised 2026-09-20) It is
   deployment-invoked, never automatic: no timer, no startup hook, no
   background task, and no path in the service or the runtime calls it. A
   deployment that never calls it never lists its store.

### 9.3 Deny is absent

Every query is tenant-scoped; a caller may never enumerate across tenants;
`get` for a scope the caller's tenant does not own answers `None` byte-identical
to a missing entry.

### 9.4 What it can stand in for

(revised 2026-09-20)

- `AgentDirectory` implements `A2AAgentCatalog` (`resolve` by agent id or task
  definition; `targets` for the tenant), so a deployment may pass it where
  `RakkaAgentA2AService::new` takes `Arc<dyn A2AAgentCatalog>` today. The
  static catalog stays the service's default and the constructor is
  unchanged: nothing constructs a directory implicitly, and the service never
  consults one it was not given.
- It implements `AgentDelegationCatalog`: specialist resolution by skill,
  filtered by the delegating agent's `collaborators` envelope and by
  `admitted && lifecycle == Active`. This is open decision 15's
  "application-owned authorized catalog" with a default implementation; the
  model still names only a skill. Delegation resolution keeps consulting
  whichever `AgentDelegationCatalog` the deployment installed and never
  consults the directory unless the directory is what was installed, so a
  deployment whose catalog is release data keeps the property that a model
  can reach only an agent a release declared.
- Telemetry. The fleet gauge `rakka.agent.directory.entries{lifecycle,
  admitted}` is recorded by the sweep only. `gen_ai.agent.name` and
  `gen_ai.agent.version` go through the same identity policy as
  `gen_ai.agent.id` and are off by default: today `AgentGenAiIdentity`
  (`otel.rs:809`) is filled by the caller under its tenant's telemetry access
  policy, and `identity_of` (`:788`) hard-codes both to `None`. This phase
  adds `agent_name: Option<String>` and `agent_version: Option<String>` to
  `AgentSegmentIdentity` (`observability.rs`), filled only by the same caller
  that fills `agent`, under the same policy and its pseudonymization; `identity_of`
  maps them, and both stay `None` unless that caller sets them. A name is an
  identifier however bounded, so a consumer whose observability rule forbids
  any agent identifier on the wire sets neither and exports nothing new. The
  directory offers `AgentDirectoryEntry::telemetry_identity()` as one source
  a deployment may draw from; nothing fills the fields automatically.

### 9.5 Cards and the directory route

`A2AAgentCardBuilder::build_from_directory(&self, directory, tenant) -> AgentCard`:
one `AgentSkill` per (agent, task definition) with `tags` from skills and the
agent id, `description` from the definition, extensions as today. When a
directory is installed, the `AgentCardProducer` for the well-known path is a
`DirectoryAgentCard` that rebuilds on a bounded interval; otherwise the card
is built from the static catalog as it is today. `GetExtendedAgentCard`
answers the per-agent card (skills of one agent, its coordination
capabilities, its supported protocol versions). Rakka-extension routes:
`GET /agents?skill=&task_definition=&lifecycle=&cursor=` and
`GET /agents/{agent_id}/card`, under a new `A2AOperation::DirectoryRead`,
mounted only by `agent_directory_routes` (section 3.3).

### 9.6 Tests

`crates/rakka-agent/tests/agent_directory.rs` (derivation, idempotent stale
upsert, tenant fence, deny is absent, sweep rebuild after a dropped write);
`crates/rakka-agent-postgres/tests/directory_conformance.rs` (gated);
`crates/rakka-a2a/tests/directory_surface.rs` (card, routes, catalog and
delegation resolution through the directory; (added 2026-09-20) a service
built without a directory resolves through the static catalogs only and
writes no entry, and a span from a run with no identity caller set carries
neither agent name nor version).

## 10. Builder DSL

### 10.1 Boundary

Rakka does not own a DSL or a compiler. The two builders below are Rust
conveniences that produce the same validated data an application backend's
compiler produces; they add no new IR, node kind, or field. They live in the
crates that own the data, with no new dependency. A deployment that compiles
plans and deserializes definitions from its own release data never calls
either; nothing depends on them.

### 10.2 `AgentDefinition::builder`

```rust
let definition = AgentDefinition::builder("support-v1")
    .description("Resolves customer support tickets end to end.")
    .instructions(instructions_ref)
    .task_definition(&ticket_task)                       // inserts the id
    .tools_from(&registry)                               // AgentToolRegistry::tool_declarations
    .tool("search_kb", AgentToolDeclaration::new(AgentEffectSafetyClass::ReadOnly).with_capability(kb_read))
    .model_profile("anthropic-sonnet")
    .workflow_tool("refund-v3")
    .collaborator("billing-agent")
    .knowledge_space("support")
    .environment("crm")
    .credential_binding("crm-token")
    .coordination(AgentCoordinationCapabilityKind::Delegation)
    .operation_class(AgentOperationClass::BoundedUnattended)
    .mandatory_guardrail("pii-block")
    .budgets(|b| b.max_loop_iterations(50).max_tokens(200_000).max_wall_clock_millis(600_000))
    .policies(|p| p.approval("support-approvals").retention("support-30d"))
    .build()?;                                            // AgentDefinition::new + validate; errors carry the definition codes
```

Also `AgentTaskDefinition::builder(id, version)` with `input_schema`,
`result_schema`, `result_rule`, `limits`, `budgets`, `run_allocation`, and
`AgentGoalSpec::builder(owner, objective)` for the fields the acceptance
examples set by literal. Every builder is consuming, `#[must_use]`, and ends
in the existing constructor so validation is not duplicated. String arguments
that are ids go through the `validated_id!` constructors and surface their
codes.

### 10.3 Compiled plan builder

In `rakka-agent-workflow::compiled_plan::builder`:

```rust
let registration = CompiledPlanBuilder::new("triage", WorkflowDefinitionVersion::new(3))
    .input("in", |n| n.output_port("ticket", "application/json", ticket_schema_ref))
    .model_call("classify", |n| n.target(model_target("anthropic-sonnet")).config(classify_cfg).timeout(timeouts).input_port("prompt", "text/plain").output_port("label", "application/json"))
    .branch("route", |n| n.condition(route_cond).output_ports(["urgent", "normal"]))
    .tool_call("page", |n| n.target(tool_target("mcp.pagerduty.page")).credential("pagerduty").input_port("in", "application/json"))
    .human_checkpoint("approve", |n| n.config(approval_cfg))
    .join("done", |n| n.merge(AgentCompiledEdgeMergeBehavior::All))
    .terminal("end")
    .edge("in.ticket", "classify.prompt")
    .edge("classify.label", "route.in")
    .edge("route.urgent", "page.in")
    .edge("route.normal", "approve.in")
    .edges(["page.out", "approve.out"], "done.in")
    .edge("done.out", "end.in")
    .observability_label("team", "support")
    .build()?;                                           // -> AgentCompiledWorkflowRegistration
```

- Node closures take a typed per-kind node builder that exposes only the fields
  that kind uses (`AgentCompiledNodeKindCatalog` says which ports and targets a
  kind allows; the builder checks at build time and returns
  `plan-builder-port-not-allowed` before the plan validator would).
- Edges are `"node.port"` strings resolved against the nodes declared so far;
  an unknown node or port is `plan-builder-unknown-endpoint`. `then(a, b)` is
  sugar for a single-output-to-single-input edge.
- (revised 2026-09-20) `build()` accepts a caller-supplied plan id and
  fingerprint — `.plan_id(AgentCompiledPlanId)` and
  `.plan_fingerprint(AgentCompiledPlanFingerprint)` — and derives them only
  when absent (the id from workflow id and version; the fingerprint the way
  the substrate computes it). `AgentCompiledExecutionPlan::new`
  (`compiled_plan.rs:183`) already takes both as arguments, so a compiler that
  content-addresses its plans keeps its own identity through the builder and
  the builder adds no derivation the caller cannot bypass. It then runs
  `validate_compiled_execution_plan_with_catalog` and pairs the plan with a
  generated `AgentWorkflow` descriptor (id, type, version, payload types from
  the ports) so the result registers with
  `AgentWorkflowRegistry::register_compiled` directly.
- Configs, conditions, schemas, and policies stay `ArtifactRef`s; the builder
  accepts no inline secret-shaped value (the plan validator's secret rules run
  unchanged; the builder adds nothing that could carry one).
- Error type `CompiledPlanBuilderError` with stable codes, registered in the
  compatibility document.

`examples/minimal-local-agent-workflow` is rewritten on the builder as its
proof, and `examples/durable-agent-acceptance` on the definition builder.

### 10.4 Tests

`crates/rakka-agent/tests/definition_builder.rs` (every setter; validation
parity: the builder and the literal produce byte-equal serialized
definitions); `crates/rakka-agent-workflow/tests/compiled_plan_builder.rs`
(a built plan equals a hand-assembled one, validates, fingerprints stably;
each builder-time error; (added 2026-09-20) a caller-supplied id and
fingerprint survive `build()` unchanged).

## 11. Compatibility, security, and documentation

### 11.1 Compatibility (all additive unless noted)

- New durable fields: `AgentModelTurn`/`AgentModelUsage` provider fields
  (optional), `MemoryContextSnapshot.session_mode`, `AgentGrantDescriptor`
  profile revision and digest, `AgentSettingsChange::MemoryMode`,
  `AgentSegmentIdentity.{agent_name, agent_version}` (optional, default
  `None`). Each lands under the N/N+1 schema policy with a version bump where
  the record is versioned. `AgentRunEffect.deadline_at` gains a per-attempt
  writer but no persisted value (4.2 item 3); its schema is unchanged.
- Changed constant: `AGENT_GUARDRAIL_CONTENT_MAX_BYTES` 8 KiB → 16 KiB.
  Chains that relied on truncation at 8 KiB see more content; noted in the
  changelog.
- Trait changes: `AgentDispatchAuthority::review_model_response` (required,
  breaking for out-of-tree authorities, deliberate; a production authority
  and its wrappers forward it in one method each);
  `AgentModelAdapter::call_with` (defaulted). New traits:
  `AgentModelProfileCatalog`, `A2APrincipalResolver`,
  `AgentTaskHistoryWatcher`, `AgentDirectoryStore`, `McpEgressCheck`,
  `McpChildProcessLauncher`.
- (plan refinement 2026-09-20) `AgentGuardrailContext.scope` becomes
  `subject: AgentGuardrailSubject<'a>` (`Run`, `Task`, `Team`,
  `Conversation`), because an A2A ingress evaluation has no run to name;
  `AgentGuardrailContext::new(boundary, &run_scope)` keeps its signature and
  `scope()` answers the run when there is one. Breaking only for a stage that
  read the `scope` field directly; none exists in the tree. Two evaluated-set
  constants join the existing pair: `AGENT_MEMORY_ATTESTED_GUARDRAIL_BOUNDARIES`
  (five) and `AGENT_A2A_ATTESTED_GUARDRAIL_BOUNDARIES` (six);
  `AGENT_AUTHORITY_EVALUATED_GUARDRAIL_BOUNDARIES` grows to four and
  `AGENT_EVALUATED_GUARDRAIL_BOUNDARIES` to seven, both changing their array
  length, and `evaluated_boundaries()` answers whichever set the authority's
  two attestations select.
- (added 2026-09-20) **Unchanged constructors, guaranteed** (section 3.5):
  `RakkaAgentA2AService::new`, `send`, `send_message`,
  `A2AAgentDelegationSendExecutor::new`, `A2AAgentHandoffSendExecutor::new`,
  and every existing `with_*` builder on the three keep their signatures and
  semantics; the phase adds only new builders (`with_ingress_guardrails`,
  `with_directory` on the service; `with_egress_guardrails` on each
  executor) whose absence leaves behaviour byte-identical.
- (added 2026-09-20) **Feature unification.** `rig-core 0.37.0`'s `reqwest`
  feature enables `reqwest/system-proxy`; under Cargo feature unification any
  crate in a consumer's dependency graph that enabled it would switch it on
  for the consumer's own `rig-core`. Rakka's `rig` feature enables `rustls`
  alone (4.5), and no Rakka crate, example, or test may enable `reqwest`;
  `rakka-agent-mcp`'s `rmcp` features enable `reqwest?/rustls` alone (5.1).
  A consumer that keeps `agent-rig` off is unaffected either way; one that
  enables it gains a TLS backend and no proxy behaviour. A test in
  `crates/rakka-agent/tests/` parses the manifest and fails if the `rig`
  feature ever lists `reqwest`.
- New stable codes, registered in `docs/rakka-compatibility.md`:
  `model-profile-unknown`, `model-profile-revision-mismatch`,
  `model-profile-invalid-base-url`, `model-timeout-unset`,
  `guardrail-rule-invalid` (a built-in stage constructed with an empty or
  oversized rule; `guardrail-transform-invalid` and `checkpoint-required` are
  already registered and reused, so the first draft's
  `guardrail-checkpoint-unsupported-at-response` is withdrawn — plan
  refinement 2026-09-20), `mcp-tool-error`, `mcp-input-required`, `mcp-protocol-unsupported`,
  `mcp-result-too-large`, `mcp-hint-contradicts-declaration`,
  `mcp-peer-agent-channel-refused`, `mcp-transport-unsupported`,
  `mcp-descriptor-schema-too-large`, `mcp-descriptor-sync-failed`,
  `tool-descriptor-revision-mismatch`, `plan-builder-unknown-endpoint`,
  `plan-builder-port-not-allowed`, `directory-tenant-mismatch`. The
  registry-backed check the plan owes (slice 6.4) is a good companion.
- Pins added to the compatibility table: `rmcp =3.4.0` with features
  `client`, `transport-streamable-http-client-reqwest`, `reqwest`
  (`transport-child-process` under the crate's `child-process` feature
  only), MCP `2026-07-28` (`2025-11-25` compatible), and `rig-core =0.37.0`
  with `rustls` only. `a2a-server-lf` may move to 0.4.3 (public surface
  byte-identical, `a2a-pb` 0.2.0); optional.
- `A2AOperation` gains `DirectoryRead`.

### 11.2 Security

- No secret on any new record; the `secret_exclusion` scan is extended to
  every new type by its compile-time-complete match.
- Model and MCP credentials resolve only inside the attempt through the
  existing resolver, under the effect's deadline (4.2 item 3), and are dropped
  with the client.
- (revised 2026-09-20) The Rig adapter and the MCP executor send only through
  the client injected at construction; neither builds one. A model base URL
  is validated before a provider client exists; an MCP server URL is handed to
  the egress check the executor was constructed with before a client exists,
  a required argument (R17). No Rakka crate enables a proxy-honouring HTTP
  feature.
- Ingress guardrails run inside the service, so in-process delivery — not
  only HTTP — is covered; ingress and egress chains are attested against the
  authority's chain.
- A child-process MCP transport never runs without a deployment-supplied
  launcher.
- Directory reads are deny-is-absent across tenants; no agent name or version
  reaches a span unless the identity-policy caller sets it.
- A2A authentication stays application-owned; the spec's "every request MUST
  be authenticated" holds at the layer the deployment installs.
- The security matrix gains rows for profiles, MCP bindings, directory, and
  the three new evaluation points.

### 11.3 Documentation

- `docs/rakka-agents.md`: the A2A section rewritten for the mounted surface,
  streaming, and the directory; the corrections at lines 260 and 348; new
  sections for providers, the MCP client, memory modes, builders.
- `docs/plans/rakka-agent/spec.md` amendments: 10.1 (profile catalog,
  `call_with`, the model effect's `timeout_ms` rule), 11.7 (MCP binding
  rules, hint policy, the three conditions), 13.2 (modes), 14.1 (mounting,
  principal, ingress at `normalized_send`), 14.5 (streaming subscription
  shape), 16 (seven boundaries evaluated), 19 (crate shape:
  `rakka-agent-mcp`, features), 21.3 decision 6 disposition (cards from the
  directory when installed).
- `docs/rakka-compatibility.md`, `CHANGELOG.md`, the telemetry and security
  matrices, and `docs/rakka-api-boundary-inventory.md` (new crate row, new
  facade features).

### 11.4 Deferred from this phase

Cut on 2026-09-20. Each is recorded with the seam it attaches to so a later
phase starts from a known place rather than from this document's history.

- **ACP façade.** ACP merged into A2A under the Linux Foundation in August
  2025; A2A 1.0 is the live standard. If a named consumer ever needs it, it is
  a stateless REST façade over the agents handler (ACP run id = A2A task id,
  ACP agent name = `AgentId`) behind a `rakka-a2a` feature, with the ACP
  OpenAPI revision pinned in the compatibility table.
- **MCP server.** An `rmcp::ServerHandler` over `RakkaAgentClient` and the
  directory, exposing task definitions as tools (`tools/call` creates a task
  through the durable path, the MCP tasks extension polls it) and task and
  goal views as resources, on a stateless Streamable HTTP tower service
  beside the A2A routes. The peer-channel refusal in section 5.2 is already in
  place for it.
- **Model-token deltas.** A defaulted `deltas: Option<&dyn AgentModelDeltaSink>`
  argument on `call_with`, a per-attempt sink from an in-process broadcast
  publisher, `CompletionModel::stream` in `RigProviderAdapter` with the final
  turn assembled from the completed stream, and `StreamResponse::Message`
  frames tagged `io.rakka.stream.event = "model-delta"` with no cursor and
  no persistence. Precondition when it returns: deltas stream only when the
  effective `ModelResponse` chain has no stage that can block or transform, so
  a blocked answer is never partially shown.
- **Push delivery for agent tasks.** A bounded relay consuming the task event
  feed (section 7.1), persisting a delivered cursor per stored push config,
  and handing `TaskStatusUpdateEvent`s to the existing push dispatch boundary
  in `crate::dispatch`. The configs it needs are already stored (section 3.6).
- **Compaction executor.** An `AgentSessionCompactionExecutor` beside the
  other executor traits, driven by a `CompactSession` run command that
  persists a `SessionCompactionCall` effect whose result appends one
  `Summary` entry with `revision` and `compaction_of: (first, last)`
  provenance (spec 13.2), plus a deterministic truncating executor in the
  testkit. `include_summaries` already selects summaries into the window.

## 12. Slices and order

(revised 2026-09-20)

Each slice ends green under `scripts/validate.sh`, with its tests named above,
and updates the docs it touches. Estimated in slices, not hours.

| Slice | Content | Depends on |
| --- | --- | --- |
| 7.2 | `review_model_response`, coverage constants, content bound, built-in stages, `A2aIngress` at `normalized_send` with attestation, `A2aEgress` in the executors, #70 closed | — (first; proven on `DeterministicModelAdapter` and the `reviewed_tool_outcome` precedent, no provider) |
| 7.1 | Model profile catalog (trait, static implementation), `call_with`, the model effect's `timeout_ms` rule and per-attempt `deadline_at`, `AgentModelRequest.tools`, telemetry slots, `rig` feature `rustls`; then the optional tail: `RigProviderAdapter<H>` over an injected backend, `AgentModelRouter`, fake-endpoint tests, gated live walk | — |
| 7.7 | `rakka-agent-mcp` client under the three conditions: bindings, publish-time sync, executor with injected client and required egress check, launcher seam, executor router | 7.1 by consumer priority only; no API dependency (it uses the existing tool-executor seam and resolver) |
| 7.3 | PostgreSQL task/team/conversation history stores, watchers, separate `check_*` watcher leg | — |
| 7.4 | Agents `RequestHandler`, routes, principal resolver, error mapping, `list_tasks`, push-config CRUD, example mount, doc corrections | 7.3 only for multi-pod truth; runs on in-memory or an application's own stores with the polling fallback |
| 7.5 | Task-state SSE, cursor and resync, heartbeat, drain frame, coordination-scope subscribe, polling fallback | 7.4 |
| 7.6 | Agent directory, stores, writers, sweep, catalogs, cards, routes, fleet gauge, identity fields for the agent name | 7.4 |
| 7.8 | Memory modes | — |
| 7.9 | Definition, task, goal builders; compiled plan builder with caller-supplied identity; examples rewritten | — |
| 7.10 | Phase close: spec amendments, matrices, changelog, product doc, boundary inventory, acceptance example `mounted-agent-endpoint-acceptance` that drives a real HTTP client through card, send, stream, MCP tool call, and directory lookup | all |

Independent tracks that can run in parallel: {7.2 → 7.1 → 7.7}, in
consumer-priority order; {7.3 → 7.4 → 7.5, 7.6}; {7.8}; {7.9}. Consumer
priority, from the host's assessment: 7.2 needed; 7.1's three seams wanted;
7.7 wanted under the conditions in section 5; 7.3, 7.4, 7.5, 7.6, 7.8, and
7.9 not requested by the host, and each is inert for a deployment that does
not mount or install it. If the phase is cut short, it is cut after 7.7.

## 13. Acceptance statement

The phase is complete when one acceptance walk, run from the repository with
no external service, does the following through the network surface and
proves each step from durable state, not from the frames it received:

1. reads the well-known card and finds the agent's task definition as a skill
   whose description came from the directory;
2. sends a task over JSON-RPC and, separately, over REST, with a tenant and a
   principal header, and receives the same task id for a duplicate send;
3. subscribes over SSE, sees the snapshot, the assignment, the model turn, the
   result acceptance, and the terminal frame, then reconnects with the last
   cursor and receives nothing it already saw, then reconnects with an expired
   cursor and receives a resync frame;
4. the model turn came from a `RigProviderAdapter` over an injected client
   against the in-process fake provider, selected by profile, with a
   credential that appears on no persisted record and was resolved under the
   effect's deadline;
5. one tool call went through `McpDispatchToolExecutor` over an injected
   client to an in-process MCP server whose descriptors were synced ahead of
   the walk and stored, and its result is in session memory as a tool result
   naming the effect;
6. a `ModelResponse` stage transformed one turn and blocked another, and the
   blocked run ended `EffectFailed` with the stage's code; an `A2aIngress`
   stage fired for a delegation delivered in-process with no handler on the
   path;
7. the definition and the plan the walk used were produced by the builders and
   serialize byte-equal to their literal twins;
8. `docs/rakka-agents.md`'s A2A section is held by a test the way its exchange
   list is today.

The walk's transcript is asserted verbatim by `tests/acceptance.rs`, as the
five existing acceptance examples do.

## 14. Consumer priorities beyond this phase

(added 2026-09-20) Recorded from the host's assessment
(`../paloul.rakka.host/docs/specs/2026-09-20-upstream-phase7-parity-assessment.md`,
sections 4 and 5), in the host's order, for what should follow or replace the
back half of this phase (slices 7.3 through 7.6, 7.8, 7.9). Nothing in this
list is in scope for Phase 7; it is here so the next brief starts from it
rather than from this document's history.

1. **#70, `ModelResponse`.** This document's section 6; the one item the host
   calls necessary.
2. **#69, the communal read path.** `MemoryContextSnapshot::communal_claims`
   is a placeholder, so every claim written is unreadable by any run.
3. **#68, knowledge-graph retention and deletion.** Operational the moment a
   release declares a mapping; nothing can delete a claim.
4. **#71, the workflow-substrate arms.** Branch-complete, child-settlement,
   and the iterator unroll: consumer-side workarounds exist only because the
   actor has no arm.
5. **The compaction executor** cut from this draft (section 11.4). A
   `Summary` entry with provenance is the memory feature the product spec
   names; session windows only grow without it.
6. **Writers for `max_wall_clock_millis` and for
   `AgentRunStatus::{Suspended, WaitingForTimer}`** (audit section 8): bounds
   and states the product declares and the runtime does not enforce.

None of these is Akka parity, which is why the audit's section 10 does not
rank them; all of them are product-vision gaps a consumer cannot close on its
own side.
