# Phase 7 Slice 7.7: MCP Client Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship `rakka-agent-mcp`, an adapter-tier crate that binds remote MCP servers as Rakka tools under the host's three conditions: an injected transport client with a required per-attempt egress check, no child process without a deployment-supplied launcher, and descriptors as release data synced at publish time with the credential arriving only through the dispatcher's resolver.

**Architecture:** A `McpServerBinding` names a server, its transport, its logical credential binding, and an allow-list of tools with operator-declared safety. `sync_mcp_descriptors` turns a live `tools/list` into stored `AgentToolBinding`s (`mcp.<server>.<tool>`) plus schema digests, with no store and no registry. `McpDispatchToolExecutor<C>` implements `rakka_agent::AgentDispatchToolExecutor`: per attempt it runs the egress check, builds an `rmcp` client over the injected `C: StreamableHttpClient` (or the launcher's stdio pair), rechecks the live schema digest against the stored one, sends `tools/call` with `_meta` carrying trace context and the idempotency key, and maps the result to bounded inline content, an artifact, or a stable refusal. `rakka-agent` gains `AgentToolExecutorRouter` so MCP, function, and process executors compose behind the dispatcher's one `Arc<dyn AgentDispatchToolExecutor>`.

**Tech Stack:** Rust 1.88 workspace; `rmcp = "=3.4.0"` (`client`, `transport-streamable-http-client-reqwest`, `reqwest`, `transport-io`; `server` + `transport-streamable-http-server` dev/testkit only; `transport-child-process` behind the crate's own `child-process` feature); axum 0.8 (workspace) for the in-process fake; tokio; serde/serde_json; `rakka-agent` (default features off) and `rakka-agent-workflow`.

**Spec:** `docs/superpowers/specs/2026-09-19-phase7-agent-surface-parity-design.md`, section 5 (5.1–5.4), with 11.1 (codes, pins), 11.2 (security), 11.3 (docs), 12 (slice row 7.7). R8 and R17 in section 0 are binding. Rig/provider seams from slice 7.1 (merged as PR #78, `rakka-agents` at ab9c602) are the base.

## Global Constraints

- Every public item needs a doc comment (`missing_docs = "warn"`; validation runs clippy with `-D warnings`). `unsafe_code = "forbid"`. MSRV 1.88. The new crate is `publish = false` with the knowledge-graph crate's guard comment, `[lints] workspace = true`, and joins the workspace `members` list.
- `rmcp = { version = "=3.4.0", default-features = false, features = ["client", "transport-streamable-http-client-reqwest", "reqwest", "transport-io"], optional = false }` in `[dependencies]`; `transport-child-process` only under the crate's `child-process` feature (off by default); `server` and `transport-streamable-http-server` only under the crate's `testkit` feature and in `[dev-dependencies]`. rmcp's `reqwest` feature is `["__reqwest", "reqwest?/rustls"]` and its reqwest dependency is `0.13` with `default-features = false, features = ["json", "stream"]`; nothing enables `reqwest/system-proxy`, and `cargo tree -p rakka-agent-mcp -e features -i reqwest --all-features | grep -c system-proxy` prints `0`. A test parses the manifest and holds these.
- P2: every MCP call stays a durable outbox tool effect dispatched by `AgentRunEffectDispatcher`; this slice adds no second execution path. P3: a secret exists only as the `AgentEphemeralCredential` the dispatcher resolved for one attempt; `McpServerBinding` carries a `credential_binding` reference only; the executor sets the transport's auth header from the credential for that attempt and never reads an environment variable, a file, or a profile attribute; no key value reaches a `Debug` output, an error message, a telemetry attribute, or a durable record.
- Condition (a): `McpDispatchToolExecutor::new(descriptors, bindings, artifacts, http, egress)` takes the transport client and the `McpEgressCheck` as required arguments; the only opt-out is passing `McpAllowAllEgress` by name (R17). The URL is handed to the check before any client for the attempt exists; a refusal returns the refusal's own code and the credential is never read.
- Condition (b): `McpTransport::ChildProcess` is refused by `new` with `mcp-transport-unsupported` unless `with_child_process_launcher` installed a launcher first (fallible: it re-validates every binding). The crate ships no launcher by default; `TokioChildProcessLauncher` exists only under `child-process` and is documented as unsandboxed.
- Condition (c): the executor is built from stored `McpDescriptorSet`s and makes no network call at construction (a test counts the fake's `tools/list`); `McpDescriptorRefresh::Manual` is the default.
- MCP is never an agent-to-agent channel: a server whose initialize `serverInfo.name` starts with `rakka-agent` is refused `mcp-peer-agent-channel-refused` at sync and at every attempt.
- The durable error-text rule of `AgentDispatchToolExecutor` (dispatch.rs:395–412): a failing attempt's text is persisted bounded to `AGENT_DISPATCH_FAILURE_DETAIL_MAX_LENGTH` (512); the executor's messages carry a stable code and a short reason, never the arguments, never a credential, and never a response body except the `isError` text the spec allows, sanitized to one line and cut at `MCP_TOOL_ERROR_DETAIL_MAX_BYTES` (512).
- Stable codes this slice registers (Task 7): `mcp-tool-error`, `mcp-input-required`, `mcp-protocol-unsupported`, `mcp-result-too-large`, `mcp-hint-contradicts-declaration`, `mcp-peer-agent-channel-refused`, `mcp-transport-unsupported`, `mcp-descriptor-schema-too-large`, `mcp-descriptor-sync-failed` (the nine in spec 11.1), plus `mcp-binding-invalid`, `mcp-tool-unbound`, `mcp-credential-material-unsupported`, `mcp-transport-failed` (the `AgentDispatchError::Invocation` code for a transport or protocol failure and for the intent's timeout), and `tool-descriptor-revision-mismatch` (spec 5.2 names it; it is first registered here) — fourteen in all. Bounds: `MCP_DESCRIPTOR_SCHEMA_MAX_BYTES = 64 * 1024`, descriptor `parameters` carried only when ≤ `AGENT_TOOL_PARAMETERS_MAX_BYTES` (4 KiB), `MCP_INLINE_RESULT_MAX_BYTES = 2 * 1024`, `MCP_TOOL_ERROR_DETAIL_MAX_BYTES = 512`, `MCP_DESCRIPTOR_RECHECK_TTL_DEFAULT_MS = 60_000`.
- Every existing constructor and `with_*` builder in `rakka-agent` keeps its signature; `AgentDispatchToolExecutor` is unchanged; `AgentToolBinding` gains serde derives only.
- Test files: one concern per file under `crates/<crate>/tests/`; unit tests inline. Commit messages end with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`. Never push or open a PR without the owner's go-ahead (Task 8).
- On this machine run tests per crate, give every cargo command an explicit 600000 ms timeout in the foreground, redirect `scripts/validate.sh` to a file and read the real exit code, and run `cargo clean` at every task boundary (the owner's rule; the executor records each clean in the ledger).

## Refinements the tree forced (to be marked in the spec as "plan refinement 2026-09-22")

1. **No `AgentClock` exists.** `sync_mcp_descriptors` takes `synced_at: AgentTimestampMillis` from the caller (publish time is the caller's clock), and the executor's descriptor-recheck cache is keyed on a monotonic `std::time::Instant`; `McpDispatchToolExecutor::new` takes no clock.
2. **`McpDescriptorRefresh` is `Manual | Interval { millis }` in this slice.** rmcp 3.4.0 has no `subscriptions/listen`; tool-list changes arrive as a notification on a long-lived session, which contradicts the stateless per-attempt client. `Interval` is served by the pure `mcp_descriptor_staleness(stored, fresh)` helper the deployment calls from its own timer after re-running the sync; no background task lives in the crate. `Listen` is deferred to a later slice with the seam named in the spec mark.
3. **Server identity comes from the initialize handshake.** rmcp 3.4.0's `DiscoverResult` carries `supported_versions`, `capabilities`, `ttl_ms` but no server info; the peer-agent rule reads `RunningService::peer_info().server_info.name`. Version fallback is rmcp's `ClientLifecycleMode::Discover { preferred_versions }` negotiation (one round; `ClientInitializeError::NoCompatibleProtocolVersion` maps to `mcp-protocol-unsupported`), not a Rakka-side retry.
4. **The recheck TTL is executor-configured.** rmcp 3.4.0's `tools/list` result carries no `ttlMs`, so `with_descriptor_recheck_ttl_ms(ms)` sets the cache window (default `MCP_DESCRIPTOR_RECHECK_TTL_DEFAULT_MS`; `0` rechecks on every attempt).
5. **Determinacy is the safety class's.** `AgentDispatchToolExecutor` returns `Err(AgentDispatchError)` and the dispatcher retries per the declaration's class and attempt bound; an executor cannot mark an error non-retryable. The egress refusal, the MRTR refusal, and `isError` are `AgentDispatchError::Collaborator { code, message }`; the egress check re-fires on every attempt before any client exists, which is the spec's "determinate failure" in effect.
6. **The artifact store is `Arc<tokio::sync::Mutex<dyn AgentArtifactStore + Send>>`** (`McpArtifactStore`), because `AgentArtifactStore::put_artifact` takes `&mut self`.
7. **`with_input_schema_ref` becomes `with_input_schema_artifact(ArtifactRef)`** on `McpSyncedDescriptor` (a schema over 4 KiB is returned raw and the caller records where it stored it); the descriptor's `AgentSchemaRef`s name `mcp.<server>.<tool>.input` / `.output` at `AgentRevisionNumber::INITIAL`, and `version` stays `INITIAL` — the schema digest is the change detector the grant and the recheck use.
8. **The hint rule, made concrete.** With `honor_hints`, a hint that contradicts the declared class refuses registration: `destructiveHint: true` or `idempotentHint: false` against a `ReadOnly` or `Idempotent` declaration, and `readOnlyHint: false` against `ReadOnly`. Hints never change a declaration; without `honor_hints` they are ignored.
9. **Five codes beyond spec 11.1's nine**: `mcp-binding-invalid` (a structural binding refusal: URL, tool name, duplicate server, a binding with no stored descriptor set or a set with no binding), `mcp-tool-unbound` (the executor is handed a tool id it holds no descriptor for), `mcp-credential-material-unsupported` (`Basic`/`Custom` material; rmcp's transport takes an auth header value or a named header), `mcp-transport-failed` (the `Invocation`-class code for transport, protocol, and timeout failures on the call path, retried per the safety class), and `tool-descriptor-revision-mismatch` (spec 5.2's recheck refusal, registered now).
10. **The dispatcher-level proof lives in `rakka-agent`'s tests** through a `[dev-dependencies] rakka-agent-mcp = { path, features = ["testkit"] }` edge (Cargo permits the dev-dependency cycle), because the real pipeline fixture (`tests/common/mod.rs::AuthorityFixture`) is not exported; the fixture gains `with_tool_executor(Arc<dyn AgentDispatchToolExecutor>)` mirroring slice 7.1's `with_model_adapter`.
11. **The child transport is an `AsyncRead`/`AsyncWrite` pair** (`McpChildTransport { reader, writer }`), which rmcp's `transport-async-rw` turns into a transport; a launcher needs no rmcp trait objects. The reference `TokioChildProcessLauncher` (feature `child-process`) reads the spec artifact `{ "command": "...", "args": [...] }` from the artifact store it is given and spawns it with `rmcp::transport::TokioChildProcess`.
12. **The `_meta` idempotency key** is `intent.external_key()` when the effect carries one, else the durable `intent.idempotency_key` (the same value `RecordingToolExecutor` records).

## File structure

| File | Responsibility |
| --- | --- |
| `crates/rakka-agent/src/tool_router.rs` (new) | `AgentToolExecutorRouter`: routes `AgentDispatchToolExecutor::execute` by tool-id prefix, exact tool id, or registry-declared kind, with a fallback |
| `crates/rakka-agent/src/tools.rs` | `AgentToolBinding` gains `Serialize`/`Deserialize` |
| `crates/rakka-agent/src/lib.rs` | module map + re-exports |
| `crates/rakka-agent/tests/common/mod.rs` | `AuthorityFixture::with_tool_executor` |
| `crates/rakka-agent/tests/tool_executor_router.rs` (new) | router proofs |
| `crates/rakka-agent-mcp/Cargo.toml` (new) | the pin, features `child-process`, `testkit` |
| `crates/rakka-agent-mcp/src/lib.rs` (new) | module map, re-exports, crate doc with the three conditions |
| `crates/rakka-agent-mcp/src/binding.rs` (new) | `McpServerId`, `McpTransport`, `McpToolPolicy`, `McpDescriptorRefresh`, `McpServerBinding`, `McpRegistrationError`, constants |
| `crates/rakka-agent-mcp/src/client.rs` (new) | per-attempt client construction shared by sync and executor: `McpClientConfig`, credential → header, `connect`, peer-agent check, error mapping |
| `crates/rakka-agent-mcp/src/sync.rs` (new) | `sync_mcp_descriptors`, `McpDescriptorSet`, `McpSyncedDescriptor`, `McpSyncError`, hint narrowing, `mcp_descriptor_staleness` |
| `crates/rakka-agent-mcp/src/executor.rs` (new) | `McpEgressCheck`, `McpAllowAllEgress`, `McpArtifactStore`, `McpDispatchToolExecutor`, result mapping, recheck cache |
| `crates/rakka-agent-mcp/src/launcher.rs` (new) | `McpChildTransport`, `McpChildProcessLauncher`; `TokioChildProcessLauncher` under `child-process` |
| `crates/rakka-agent-mcp/src/testkit.rs` (new, feature `testkit`) | `FakeMcpServer` (rmcp `ServerHandler`), `FakeTool`, `serve_fake`, `CountingClient` |
| `crates/rakka-agent-mcp/tests/{crate_shape,binding,descriptor_sync,client_dispatch,child_process}.rs` (new) | the crate's proofs |
| `crates/rakka-agent/tests/mcp_client_dispatch.rs` (new) | the real-dispatcher proofs |
| `Cargo.toml`, `crates/rakka/Cargo.toml` | member; facade feature `agent-mcp` |
| docs | `docs/rakka-agents.md`, `docs/rakka-compatibility.md`, `docs/rakka-api-boundary-inventory.md`, `docs/rakka-agent-security-validation-matrix.md`, `CHANGELOG.md`, `CLAUDE.md`, the Phase 7 spec marks |

Verified at ab9c602 (the merge of PR #78) and in `rmcp-3.4.0` (`~/.cargo/registry/src/*/rmcp-3.4.0`): `AgentDispatchToolExecutor::execute(&self, scope: &AgentRunScope, intent: &AgentRunEffect, call: &AgentToolCallRequest, credential: Option<&AgentEphemeralCredential>) -> AgentDispatchFuture<'a, AgentTaskContent>` (`dispatch.rs:413`); `AgentDispatchError::{Invocation { code: &'static str, message }, Collaborator { code: String, message }}` with `AgentDispatchError::collaborator(code, message)` (`:4219`); `AgentToolBinding { descriptor, declaration, max_attempts, timeout_ms, .. }` derives `Debug, Clone, PartialEq` only (`tools.rs:488`), `AgentToolBinding::new(descriptor, declaration, max_attempts)` (`:527`), `unclassified` (`:505`), `effect_spec` (`:652`); `AgentToolRegistry::binding(&AgentToolId) -> Option<&AgentToolBinding>` (`:745`), `register` (`:691`); `AgentToolDescriptor::new(tool, kind, description, input_schema: AgentSchemaRef, output_schema: AgentSchemaRef)` (`:339`), `with_parameters(Value)` (`:368`), fields `tool, version, kind, description, input_schema, output_schema, parameters, result_behavior` (`:317`); `AgentToolKind::RemoteMcp` (`:237`); `AgentToolResultBehavior::{InlineBounded, ArtifactReference}` (`:275`); `AgentToolDeclaration { safety, capabilities, credential_binding, execution_policy, environments }` with `new(safety)` (`definition.rs:339–370`); `AgentEffectSafetyClass::{ReadOnly, Idempotent, Reconcileable, NonIdempotent}` (`:205–212`); `AgentAuthorityRefusal { code, message, retryable }` with `of(code, message)` (`tools.rs:1059–1072`); `AgentSchemaRef { schema_id: AgentSchemaId, version }` (`task.rs:465`); `AgentContentDigest::of_json` (`task.rs:638`); `AgentTaskContent::inline(Value) -> Result` bounded at 8 KiB and `::artifact(ArtifactRef)` (`task.rs:938–948`); `AgentRunEffect { idempotency_key, timeout_ms, telemetry, .. }` with `external_key() -> Option<&AgentExternalIdempotencyKey>` (`effect.rs:271`); `AgentTelemetryContext { trace_parent: Option<String>, trace_state: Option<String>, .. }` (`rakka-agent-workflow/src/domain.rs:815`); `AgentArtifactStore::put_artifact(&mut self, AgentArtifactWriteRequest { artifact_id: Option<String>, kind: ArtifactKind, bytes, content_type, checksum, retention_class }) -> ArtifactRef` and `get_artifact` (`artifacts.rs:400`, request at `:297`); `ArtifactKind::{Input, Prompt, Completion, File, Embedding, ..}` (`domain.rs:742`); `FakeArtifactStore` (`rakka-agent-workflow/src/testkit.rs:206`, `Default`, `insert`, `bytes`, `len`); `validate_identity_segment(field, value)` is `pub` (`identity.rs:66`) and rejects only `/`, `|`, control characters, empty, and > 256 bytes, so `mcp.<server>.<tool>` is a valid `AgentToolId`; `validated_id!` is not exported; `AuthorityFixture` has `tools: RecordingToolExecutor`, `with_credential_resolver(token)`, `with_model_adapter(Arc<dyn AgentModelAdapter>)`, and `build_pipeline(&self, worker_id, tools: RecordingToolExecutor)` (`tests/common/mod.rs:2621–2817`); `rakka-agent` dev-dependencies are `proptest`, `rakka-cluster`, `axum`, `bytes`. rmcp 3.4.0: `StreamableHttpClient` trait (`transport/streamable_http_client.rs:411`: `post_message(uri, message, session_id, auth_header: Option<String>, custom_headers: HashMap<HeaderName, HeaderValue>)`, `delete_session`, `get_stream`, plus two `_with_max_sse_event_size` defaults), `impl StreamableHttpClient for reqwest::Client` (`transport/common/reqwest/streamable_http_client.rs:49`), `StreamableHttpClientTransportConfig { uri: Arc<str>, auth_header, custom_headers, retry_config, channel_buffer_capacity, allow_stateless (default true), max_sse_event_size }` (`:2021–2056`), `StreamableHttpClientTransport::with_client(client, config)` (`:2021`); `serve_client_with_lifecycle(service, transport, ClientLifecycleMode::Discover { preferred_versions }) -> Result<RunningService<RoleClient, S>, ClientInitializeError>` (`service/client.rs:725`), `ClientInitializeError::NoCompatibleProtocolVersion { client_supported, server_supported }` (`:34`), `RunningService::{peer(), peer_info(), cancel(self, reason)}` (`service.rs:672–1082`), `Peer<RoleClient>::{list_tools, list_all_tools, call_tool_once(CallToolRequestParams) -> CallToolResponse}` (`service/client.rs:1367–1748`); `ClientInfo = InitializeRequestParams { meta, protocol_version, capabilities, client_info: Implementation }` with `new(capabilities, client_info)` and `impl ClientHandler for ClientConfig` (`model.rs:1031–1174`, `handler/client.rs:299`); `Implementation { name, title, version, description, icons }` (`model.rs:1484`); `ProtocolVersion::{V_2026_07_28, V_2025_11_25, KNOWN_VERSIONS}` (`model.rs:170`); `Tool { name, description, input_schema: Arc<JsonObject>, output_schema, annotations: Option<ToolAnnotations { read_only_hint, destructive_hint, idempotent_hint, open_world_hint }>, meta }` (`model/tool.rs:17–91`); `CallToolRequestParams { meta: Option<RequestMetaObject>, name, arguments: Option<JsonObject>, input_responses }` (`model.rs:4142`); `CallToolResult { result_type, content: Vec<ContentBlock>, structured_content: Option<Value>, is_error: Option<bool>, meta }` (`:3873`); `CallToolResponse::{Complete, InputRequired, Task}` (`model/mrtr.rs:105`); `ContentBlock::{Text, Image, Audio, Resource, ResourceLink}` with `as_text()` (`model/content.rs:259–311`); `MetaObject` derefs to `JsonObject` with `set_traceparent`/`set_tracestate` and `RequestMetaObject(pub MetaObject)` (`model/meta.rs:244–406`); `ServerHandler::{get_info, supported_protocol_versions, list_tools(Option<PaginatedRequestParams>, ctx) -> ListToolsResult, call_tool(CallToolRequestParams, ctx) -> CallToolResponse, discover}` (`handler/server.rs:300–643`); `StreamableHttpService::new(factory, Arc<LocalSessionManager>, StreamableHttpServerConfig { json_response, sse_keep_alive, legacy_session_mode, .. })` is a `tower_service::Service<Request<B>>` mountable with `axum::Router::nest_service` (`transport/streamable_http_server/tower.rs:78–1134`); `impl<Role, R, W> IntoTransport for (R, W)` under `transport-async-rw` (`transport/async_rw.rs:24`); `TokioChildProcess::new(command)` under `transport-child-process` (`transport/child_process.rs:61`).

---

### Task 1: `rakka-agent` prerequisites — serde on the binding, the executor router, the fixture hook

**Files:**
- Modify: `crates/rakka-agent/src/tools.rs` (the `AgentToolBinding` derive at `:488`)
- Create: `crates/rakka-agent/src/tool_router.rs`
- Modify: `crates/rakka-agent/src/lib.rs` (`pub mod tool_router;` after `pub mod tools;` and the module-map doc line; re-export `AgentToolExecutorRouter`)
- Modify: `crates/rakka-agent/tests/common/mod.rs` (`AuthorityFixture::with_tool_executor`; `build_pipeline` takes `Arc<dyn AgentDispatchToolExecutor>`)
- Create: `crates/rakka-agent/tests/tool_executor_router.rs`

**Interfaces:**
- Consumes: `AgentDispatchToolExecutor` (dispatch.rs:413), `AgentToolRegistry::binding`, `AgentToolKind`, `AgentToolId`.
- Produces:
  - `AgentToolBinding: Serialize + Deserialize` (a decoded binding is re-validated: descriptor `validate()` and `max_attempts >= 1`, refusing with the existing `AgentToolError` the way `AgentModelTurn`'s shadow record does).
  - `pub struct AgentToolExecutorRouter` with `new(fallback: Arc<dyn AgentDispatchToolExecutor>) -> Self`, `with_prefix_route(self, prefix: impl Into<String>, executor: Arc<dyn AgentDispatchToolExecutor>) -> Self`, `with_tool_route(self, tool: AgentToolId, executor) -> Self`, `with_kind_route(self, kind: AgentToolKind, registry: AgentToolRegistry, executor) -> Self`; `impl AgentDispatchToolExecutor for AgentToolExecutorRouter` resolving exact tool → longest prefix → kind (through the registry the kind route was given) → fallback. `Debug` prints route counts only.
  - `AuthorityFixture::with_tool_executor(self, executor: Arc<dyn AgentDispatchToolExecutor>) -> Self`; `build_pipeline` wires it in place of `Arc::new(tools.clone())` when set. `fx.tools` (the `RecordingToolExecutor`) stays the default.

- [ ] **Step 1: Write the failing tests**

Create `crates/rakka-agent/tests/tool_executor_router.rs`:

```rust
//! The executor router composes tool executors behind the dispatcher's one
//! `Arc<dyn AgentDispatchToolExecutor>`: an exact tool id wins over a
//! prefix, a prefix over a registry-declared kind, and everything else
//! reaches the fallback. Routing is by identity, never by arguments.

use std::sync::Arc;

use rakka_agent::testkit::RecordingToolExecutor;
use rakka_agent::{
    AgentDispatchToolExecutor, AgentSchemaId, AgentSchemaRef, AgentToolBinding,
    AgentToolCallId, AgentToolCallRequest, AgentToolDescriptor, AgentToolExecutorRouter,
    AgentToolId, AgentToolKind, AgentToolRegistry, AgentRevisionNumber,
};

mod common;
use common::{run_scope, tool_intent};

fn descriptor(id: &str, kind: AgentToolKind) -> AgentToolDescriptor {
    AgentToolDescriptor::new(
        AgentToolId::new(id).expect("tool id"),
        kind,
        format!("{id} descriptor"),
        AgentSchemaRef { schema_id: AgentSchemaId::new("in").expect("schema"), version: AgentRevisionNumber::INITIAL },
        AgentSchemaRef { schema_id: AgentSchemaId::new("out").expect("schema"), version: AgentRevisionNumber::INITIAL },
    )
    .expect("descriptor")
}

fn call(id: &str) -> AgentToolCallRequest {
    AgentToolCallRequest::new(
        AgentToolCallId::new("call-1").expect("call id"),
        AgentToolId::new(id).expect("tool id"),
        serde_json::json!({}),
    )
    .expect("call")
}

async fn execute_via(router: &AgentToolExecutorRouter, id: &str) {
    router
        .execute(&run_scope(), &tool_intent(id), &call(id), None)
        .await
        .expect("the routed executor answers");
}

#[tokio::test]
async fn exact_beats_prefix_beats_kind_beats_fallback() {
    let exact = RecordingToolExecutor::new();
    let prefixed = RecordingToolExecutor::new();
    let by_kind = RecordingToolExecutor::new();
    let fallback = RecordingToolExecutor::new();
    let registry = AgentToolRegistry::new()
        .register(AgentToolBinding::unclassified(descriptor("proc.tool", AgentToolKind::Process)))
        .expect("registered");

    let router = AgentToolExecutorRouter::new(Arc::new(fallback.clone()))
        .with_prefix_route("mcp.", Arc::new(prefixed.clone()))
        .with_tool_route(AgentToolId::new("mcp.crm.exact").expect("id"), Arc::new(exact.clone()))
        .with_kind_route(AgentToolKind::Process, registry, Arc::new(by_kind.clone()));

    execute_via(&router, "mcp.crm.exact").await;
    execute_via(&router, "mcp.crm.other").await;
    execute_via(&router, "proc.tool").await;
    execute_via(&router, "charge-card").await;

    let names = |executor: &RecordingToolExecutor| -> Vec<String> {
        executor.invocations().iter().map(|i| i.tool.clone()).collect()
    };
    assert_eq!(names(&exact), vec!["mcp.crm.exact"]);
    assert_eq!(names(&prefixed), vec!["mcp.crm.other"]);
    assert_eq!(names(&by_kind), vec!["proc.tool"]);
    assert_eq!(names(&fallback), vec!["charge-card"]);
}

#[tokio::test]
async fn the_longest_prefix_wins_and_the_credential_passes_through() {
    let short = RecordingToolExecutor::new();
    let long = RecordingToolExecutor::new();
    let router = AgentToolExecutorRouter::new(Arc::new(RecordingToolExecutor::new()))
        .with_prefix_route("mcp.", Arc::new(short.clone()))
        .with_prefix_route("mcp.crm.", Arc::new(long.clone()));
    let credential = rakka_agent_workflow::AgentEphemeralCredential::bearer_token("t");
    router
        .execute(&run_scope(), &tool_intent("mcp.crm.search"), &call("mcp.crm.search"), Some(&credential))
        .await
        .expect("answers");
    assert!(short.invocations().is_empty());
    let seen = long.invocations();
    assert_eq!(seen.len(), 1);
    assert!(seen[0].with_credential, "the credential reaches the routed executor for the attempt");
}

#[test]
fn a_binding_round_trips_through_serde_and_is_revalidated_on_decode() {
    let binding = AgentToolBinding::unclassified(descriptor("mcp.crm.search", AgentToolKind::RemoteMcp));
    let encoded = serde_json::to_string(&binding).expect("encodes");
    let decoded: AgentToolBinding = serde_json::from_str(&encoded).expect("decodes");
    assert_eq!(decoded, binding);
    let tampered = encoded.replace("\"max_attempts\":1", "\"max_attempts\":0");
    assert!(tampered != encoded, "the fixture must hit the field");
    assert!(serde_json::from_str::<AgentToolBinding>(&tampered).is_err(), "a decoded binding is validated");
}
```

If `tests/common/mod.rs` does not already expose `run_scope()` and `tool_intent(tool: &str) -> AgentRunEffect`, add them beside its existing scope/intent helpers (the fixture builds tool intents for its own tests; reuse that code path). If `RecordingToolExecutor::invocations()` or `RecordedToolInvocation.with_credential` are named differently, use the names the testkit has (`testkit.rs:3235–3345`).

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p rakka-agent --test tool_executor_router`
Expected: compile errors: `AgentToolExecutorRouter` missing; `AgentToolBinding` has no `Serialize`.

- [ ] **Step 3: Serde on the binding**

In `tools.rs`, change the derive on `AgentToolBinding` to `#[derive(Debug, Clone, PartialEq, Serialize)]` and add a hand-written `Deserialize` through a shadow record (follow `AgentModelTurnRecord`, `model.rs:407–462`): `#[derive(Deserialize)] struct AgentToolBindingRecord { descriptor: AgentToolDescriptor, declaration: AgentToolDeclaration, max_attempts: u32, #[serde(default)] timeout_ms: Option<u64>, … every other field … }` whose `TryFrom` runs `descriptor.validate()` and refuses `max_attempts == 0` with the existing bound error `AgentToolBinding::new` would raise (find the check in `new`/`validate`; if `new` does not validate attempts, add a `validate(&self)` that does and call it from both). Every field of the struct must appear in the record; list them from the struct definition, not from memory.

- [ ] **Step 4: The router**

Create `crates/rakka-agent/src/tool_router.rs`:

```rust
//! Composes tool executors behind the dispatcher's single executor slot.
//!
//! The dispatcher holds one `Arc<dyn AgentDispatchToolExecutor>`; a
//! deployment with function tools, process tools, and remote MCP tools
//! routes each call to the executor that owns it by identity — an exact
//! tool id, a tool-id prefix (`mcp.` for every MCP binding), or the kind the
//! registry declares for the tool — and everything else reaches the
//! fallback. Arguments never influence routing, so a call cannot steer
//! itself to another executor.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use rakka_agent_workflow::AgentEphemeralCredential;

use crate::definition::{AgentToolId, AgentToolKind};
use crate::dispatch::{AgentDispatchFuture, AgentDispatchToolExecutor};
use crate::effect::AgentRunEffect;
use crate::identity::AgentRunScope;
use crate::model::AgentToolCallRequest;
use crate::task::AgentTaskContent;
use crate::tools::AgentToolRegistry;

type Executor = Arc<dyn AgentDispatchToolExecutor>;

/// Routes tool calls to executors by tool identity, with a fallback.
pub struct AgentToolExecutorRouter {
    fallback: Executor,
    exact: BTreeMap<AgentToolId, Executor>,
    prefixes: Vec<(String, Executor)>,
    kinds: Vec<(AgentToolKind, AgentToolRegistry, Executor)>,
}

impl fmt::Debug for AgentToolExecutorRouter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentToolExecutorRouter")
            .field("exact_routes", &self.exact.len())
            .field("prefix_routes", &self.prefixes.len())
            .field("kind_routes", &self.kinds.len())
            .finish_non_exhaustive()
    }
}

impl AgentToolExecutorRouter {
    /// A router whose every unmatched call reaches `fallback`.
    #[must_use]
    pub fn new(fallback: Executor) -> Self {
        Self { fallback, exact: BTreeMap::new(), prefixes: Vec::new(), kinds: Vec::new() }
    }

    /// Routes every tool whose id starts with `prefix`; the longest matching
    /// prefix wins.
    #[must_use]
    pub fn with_prefix_route(mut self, prefix: impl Into<String>, executor: Executor) -> Self {
        self.prefixes.push((prefix.into(), executor));
        self.prefixes.sort_by(|a, b| b.0.len().cmp(&a.0.len()).then_with(|| a.0.cmp(&b.0)));
        self
    }

    /// Routes one tool id exactly; an exact route beats every prefix.
    #[must_use]
    pub fn with_tool_route(mut self, tool: AgentToolId, executor: Executor) -> Self {
        self.exact.insert(tool, executor);
        self
    }

    /// Routes every tool the given registry declares with `kind`.
    #[must_use]
    pub fn with_kind_route(mut self, kind: AgentToolKind, registry: AgentToolRegistry, executor: Executor) -> Self {
        self.kinds.push((kind, registry, executor));
        self
    }

    fn resolve(&self, tool: &AgentToolId) -> &Executor {
        if let Some(executor) = self.exact.get(tool) {
            return executor;
        }
        let id = tool.as_str();
        if let Some((_, executor)) = self.prefixes.iter().find(|(prefix, _)| id.starts_with(prefix.as_str())) {
            return executor;
        }
        for (kind, registry, executor) in &self.kinds {
            if registry.binding(tool).is_some_and(|binding| binding.descriptor.kind == *kind) {
                return executor;
            }
        }
        &self.fallback
    }
}

impl AgentDispatchToolExecutor for AgentToolExecutorRouter {
    fn execute<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        intent: &'a AgentRunEffect,
        call: &'a AgentToolCallRequest,
        credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentDispatchFuture<'a, AgentTaskContent> {
        self.resolve(&call.tool).execute(scope, intent, call, credential)
    }
}
```

Adjust the import paths to where each type actually lives (`AgentToolId`/`AgentToolKind` may be in `definition` or `tools`; `AgentRunScope` in `identity`; the binding's `descriptor` field may be private — if so add a `pub fn descriptor(&self) -> &AgentToolDescriptor` accessor on `AgentToolBinding` if none exists). Add `pub mod tool_router;` to `lib.rs` (alphabetical, after `pub mod tools;`? — `tool_router` sorts before `tools`; follow the file's order), a line in the module-map doc comment, and `pub use tool_router::AgentToolExecutorRouter;`.

- [ ] **Step 5: The fixture hook**

In `tests/common/mod.rs`, add a field `tool_executor: Option<Arc<dyn AgentDispatchToolExecutor>>` to `AuthorityFixture` (initialised `None` in `new`), a builder:

```rust
    /// Puts an executor other than the recording one in the pipeline's tool slot.
    pub fn with_tool_executor(mut self, executor: Arc<dyn AgentDispatchToolExecutor>) -> Self {
        self.tool_executor = Some(executor);
        self
    }
```

and in `build_pipeline`, use `self.tool_executor.clone().unwrap_or_else(|| Arc::new(tools.clone()) as Arc<dyn AgentDispatchToolExecutor>)` for the dispatcher's tools argument. Nothing else in the fixture changes.

- [ ] **Step 6: Run the tests**

Run: `cargo test -p rakka-agent --test tool_executor_router` then `cargo test -p rakka-agent --test tool_authority --test crate_shape --test schema_compatibility --test model_provider_dispatch` (the crate-shape test holds the module map; schema tests hold record shapes), `cargo test -p rakka-agent --lib`, `cargo check -p rakka-agent --no-default-features`, `cargo clippy -p rakka-agent --all-targets --all-features -- -D warnings`, `cargo fmt --all -- --check`, `RUSTDOCFLAGS="-D warnings" cargo doc -p rakka-agent --no-deps --all-features`.
Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git checkout -b rakka-agents-phase7-slice-7-7
git add crates/rakka-agent/src/tools.rs crates/rakka-agent/src/tool_router.rs crates/rakka-agent/src/lib.rs crates/rakka-agent/tests/common/mod.rs crates/rakka-agent/tests/tool_executor_router.rs
git commit -m "Route tool calls to executors by identity behind the dispatcher's one executor slot, and let a binding round-trip through serde

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 2: The `rakka-agent-mcp` crate: manifest, ids, bindings, policies, refusals

**Files:**
- Create: `crates/rakka-agent-mcp/Cargo.toml`, `crates/rakka-agent-mcp/src/lib.rs`, `crates/rakka-agent-mcp/src/binding.rs`
- Create: `crates/rakka-agent-mcp/tests/crate_shape.rs`, `crates/rakka-agent-mcp/tests/binding.rs`
- Modify: `Cargo.toml` (workspace `members`: `"crates/rakka-agent-mcp",` after `"crates/rakka-agent-knowledge-graph-postgres",`), `crates/rakka/Cargo.toml` (feature `agent-mcp = ["agent", "dep:rakka-agent-mcp"]` and the optional dependency `rakka-agent-mcp = { path = "../rakka-agent-mcp", version = "0.1.0", optional = true }`; if the facade re-exports component crates under `pub use`, add the gated `pub use rakka_agent_mcp;` in `crates/rakka/src/lib.rs` following the `rakka-agent-knowledge-graph`/`rakka-a2a` pattern there)

**Interfaces:**
- Consumes: `rakka_agent::{AgentToolDeclaration, AgentToolResultBehavior, AgentToolId, AgentCredentialBindingRef, AgentEffectSafetyClass}`, `rakka_agent::identity::validate_identity_segment`, `rakka_agent_workflow::ArtifactRef`.
- Produces (all in `binding.rs`, re-exported at the crate root):
  - Constants: `MCP_DEFAULT_PROTOCOL_VERSIONS: [&str; 2] = ["2026-07-28", "2025-11-25"]`, `MCP_DESCRIPTOR_SCHEMA_MAX_BYTES: usize = 64 * 1024`, `MCP_INLINE_RESULT_MAX_BYTES: usize = 2 * 1024`, `MCP_TOOL_ERROR_DETAIL_MAX_BYTES: usize = 512`, `MCP_DESCRIPTOR_RECHECK_TTL_DEFAULT_MS: u64 = 60_000`, `MCP_CLIENT_NAME: &str = "rakka-agent-mcp"`, `MCP_PEER_AGENT_SERVER_PREFIX: &str = "rakka-agent"`, `MCP_META_IDEMPOTENCY_KEY: &str = "io.rakka.idempotency-key"`, `MCP_TOOL_ID_PREFIX: &str = "mcp."`.
  - `pub struct McpServerId(String)`: `new(impl Into<String>) -> Result<Self, McpRegistrationError>` (runs `validate_identity_segment("mcp_server_id", ..)` and refuses a `.` in the value so `mcp.<server>.<tool>` splits unambiguously), `as_str`, `Display`, `Ord`, `Serialize`, validating `Deserialize`.
  - `pub enum McpTransport { StreamableHttp { url: String }, ChildProcess { spec_ref: ArtifactRef } }` (serde, tagged `kind`).
  - `pub struct McpToolPolicy { pub declaration: AgentToolDeclaration, pub max_attempts: u32, pub timeout_ms: Option<u64>, pub result_behavior: AgentToolResultBehavior, pub honor_hints: bool }` with `McpToolPolicy::new(declaration) -> Self` (`max_attempts: 1`, `timeout_ms: None`, `InlineBounded`, `honor_hints: false`) and `with_max_attempts`, `with_timeout_ms`, `with_result_behavior`, `with_honor_hints` builders.
  - `pub enum McpDescriptorRefresh { Manual, Interval { millis: u64 } }`, `Default = Manual`.
  - `pub struct McpServerBinding { pub server_id, pub transport, pub credential_binding: Option<AgentCredentialBindingRef>, pub protocol_versions: Vec<String>, pub tools: BTreeMap<String, McpToolPolicy>, pub refresh: McpDescriptorRefresh }` with `streamable_http(server_id, url: impl Into<String>) -> Self`, `child_process(server_id, spec_ref) -> Self`, `with_credential_binding`, `with_protocol_versions(Vec<String>)`, `with_refresh`, `with_tool(name, policy) -> Result<Self, McpRegistrationError>` (validates the derived tool id), `validate(&self) -> Result<(), McpRegistrationError>`, `tool_id(&self, tool: &str) -> Result<AgentToolId, McpRegistrationError>` (`format!("{MCP_TOOL_ID_PREFIX}{server}.{tool}")`), `url(&self) -> Option<&str>`.
  - `pub enum McpRegistrationError { InvalidUrl { server, reason: &'static str }, ToolNameInvalid { server, tool, reason: String }, DuplicateServer { server }, DescriptorSetMissing { server }, BindingMissing { server }, TransportUnsupported { server }, HintContradictsDeclaration { server, tool, hint: &'static str }, PeerAgentChannel { server, name }, ProtocolVersionsEmpty { server }, NoTools { server }, InvalidServerId { reason: String } }` with `code(&self) -> &'static str`: `TransportUnsupported → "mcp-transport-unsupported"`, `HintContradictsDeclaration → "mcp-hint-contradicts-declaration"`, `PeerAgentChannel → "mcp-peer-agent-channel-refused"`, every other variant → `"mcp-binding-invalid"`; `Display`; `std::error::Error`.
  - URL rule (`validate`): `StreamableHttp.url` parses with scheme `http` or `https`, a non-empty host, no userinfo, no fragment (a query is allowed: MCP endpoints may carry one); reuse the same hand-rolled check shape as `rakka_agent::model_profile`'s `check_base_url` (do not add a `url` crate dependency).

- [ ] **Step 1: Write the failing tests**

Create `crates/rakka-agent-mcp/tests/crate_shape.rs`:

```rust
//! The crate's manifest holds the pin and the three conditions' feature
//! shape: rmcp at exactly 3.4.0 with the client transport features and
//! nothing that enables an environment-proxy egress bypass; the child
//! process transport only behind `child-process`, off by default; the
//! server side only for the testkit.

fn manifest() -> String {
    std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml")).expect("manifest")
}

fn dependency_line(manifest: &str, name: &str) -> String {
    manifest
        .lines()
        .find(|line| line.trim_start().starts_with(&format!("{name} ")))
        .unwrap_or_else(|| panic!("{name} is declared"))
        .to_string()
}

#[test]
fn rmcp_is_pinned_exactly_with_the_client_features_and_nothing_proxying() {
    let manifest = manifest();
    let line = dependency_line(&manifest, "rmcp");
    assert!(line.contains("\"=3.4.0\""), "{line}");
    assert!(line.contains("default-features = false"), "{line}");
    for feature in ["\"client\"", "\"transport-streamable-http-client-reqwest\"", "\"reqwest\"", "\"transport-io\""] {
        assert!(line.contains(feature), "{feature} missing: {line}");
    }
    for forbidden in ["\"transport-child-process\"", "\"server\"", "\"reqwest-native-tls\"", "system-proxy"] {
        assert!(!line.contains(forbidden), "{forbidden} must not ride the default dependency: {line}");
    }
}

#[test]
fn the_child_process_transport_is_a_non_default_feature() {
    let manifest = manifest();
    let features = manifest.split("[features]").nth(1).and_then(|s| s.split("\n[").next()).expect("features table");
    assert!(features.contains("default = []"), "{features}");
    assert!(features.contains("child-process = [\"rmcp/transport-child-process\"]"), "{features}");
    assert!(features.contains("testkit = [\"rmcp/server\", \"rmcp/transport-streamable-http-server\""), "{features}");
}

#[test]
fn the_crate_is_workspace_only_and_lints_from_the_workspace() {
    let manifest = manifest();
    assert!(manifest.contains("publish = false"));
    assert!(manifest.contains("[lints]\nworkspace = true"));
}
```

Create `crates/rakka-agent-mcp/tests/binding.rs`:

```rust
//! A binding names a server, its transport, a logical credential, and an
//! allow-list of tools with operator-declared safety; every refusal has a
//! stable code and nothing in a binding is a secret.

use rakka_agent::{AgentEffectSafetyClass, AgentToolDeclaration};
use rakka_agent_mcp::{
    McpDescriptorRefresh, McpRegistrationError, McpServerBinding, McpServerId, McpToolPolicy,
    MCP_DEFAULT_PROTOCOL_VERSIONS,
};

fn server(id: &str) -> McpServerId {
    McpServerId::new(id).expect("server id")
}

fn policy() -> McpToolPolicy {
    McpToolPolicy::new(AgentToolDeclaration::new(AgentEffectSafetyClass::Idempotent))
}

#[test]
fn a_server_id_is_a_validated_identity_segment_without_dots() {
    assert_eq!(server("crm").as_str(), "crm");
    assert_eq!(McpServerId::new("").expect_err("empty").code(), "mcp-binding-invalid");
    assert_eq!(McpServerId::new("a/b").expect_err("separator").code(), "mcp-binding-invalid");
    assert_eq!(McpServerId::new("a.b").expect_err("dot").code(), "mcp-binding-invalid");
    let decoded: Result<McpServerId, _> = serde_json::from_str("\"bad/id\"");
    assert!(decoded.is_err(), "decoding validates too");
}

#[test]
fn tool_ids_are_prefixed_and_validated() {
    let binding = McpServerBinding::streamable_http(server("crm"), "https://mcp.example.test/mcp")
        .with_tool("search_contacts", policy())
        .expect("valid tool");
    assert_eq!(binding.tool_id("search_contacts").expect("id").as_str(), "mcp.crm.search_contacts");
    let error = McpServerBinding::streamable_http(server("crm"), "https://mcp.example.test/mcp")
        .with_tool("bad|name", policy())
        .expect_err("a persistence separator is refused");
    assert_eq!(error.code(), "mcp-binding-invalid");
}

#[test]
fn the_url_rule_accepts_http_and_https_and_refuses_userinfo_and_fragments() {
    for ok in ["http://127.0.0.1:1/mcp", "https://h/mcp?x=1"] {
        McpServerBinding::streamable_http(server("s"), ok).with_tool("t", policy()).expect("tool").validate().expect(ok);
    }
    for bad in ["ftp://h/mcp", "https://u:p@h/mcp", "https://h/mcp#f", "not a url", "https:///mcp"] {
        let error = McpServerBinding::streamable_http(server("s"), bad).with_tool("t", policy()).expect("tool").validate().expect_err(bad);
        assert_eq!(error.code(), "mcp-binding-invalid", "{bad}");
        assert!(matches!(error, McpRegistrationError::InvalidUrl { .. }), "{bad}");
    }
}

#[test]
fn defaults_are_manual_refresh_the_two_versions_and_one_inline_attempt() {
    let binding = McpServerBinding::streamable_http(server("s"), "https://h/mcp").with_tool("t", policy()).expect("tool");
    assert_eq!(binding.refresh, McpDescriptorRefresh::Manual);
    assert_eq!(binding.protocol_versions, MCP_DEFAULT_PROTOCOL_VERSIONS.map(str::to_string).to_vec());
    let policy = &binding.tools["t"];
    assert_eq!((policy.max_attempts, policy.timeout_ms, policy.honor_hints), (1, None, false));
    assert!(binding.credential_binding.is_none());
    let empty = McpServerBinding::streamable_http(server("s"), "https://h/mcp");
    assert!(matches!(empty.validate().expect_err("a binding lists at least one tool"), McpRegistrationError::NoTools { .. }));
    let no_versions = binding.clone().with_protocol_versions(vec![]);
    assert!(matches!(no_versions.validate().expect_err("versions"), McpRegistrationError::ProtocolVersionsEmpty { .. }));
}

#[test]
fn a_binding_round_trips_and_carries_no_secret_shaped_field() {
    let binding = McpServerBinding::streamable_http(server("s"), "https://h/mcp").with_tool("t", policy()).expect("tool");
    let encoded = serde_json::to_string(&binding).expect("encodes");
    let decoded: McpServerBinding = serde_json::from_str(&encoded).expect("decodes");
    assert_eq!(decoded, binding);
    for forbidden in ["token", "secret", "api_key", "password"] {
        assert!(!encoded.contains(forbidden), "{forbidden} appears in a binding's encoding: {encoded}");
    }
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p rakka-agent-mcp --test crate_shape --test binding`
Expected: the package does not exist yet (`error: package ID specification`); after the manifest lands, compile errors on the missing types.

- [ ] **Step 3: The manifest, the workspace member, the facade feature**

Create `crates/rakka-agent-mcp/Cargo.toml`:

```toml
[package]
name = "rakka-agent-mcp"
version = "0.1.0"
description.workspace = true
edition.workspace = true
rust-version.workspace = true
repository.workspace = true
homepage.workspace = true
documentation.workspace = true
readme.workspace = true
license-file.workspace = true
# Not yet in the release-candidate publishable set (scripts/package-check.sh,
# docs/rakka-v1-release-packaging.md); remove this guard when it enters it.
publish = false

[lib]
name = "rakka_agent_mcp"

[features]
default = []
# rmcp's stdio child-process transport and the unsandboxed reference launcher
# `TokioChildProcessLauncher`. Off by default: a deployment that runs tools
# inside its own sandbox implements `McpChildProcessLauncher` over that
# sandbox and never enables this.
child-process = ["rmcp/transport-child-process"]
# The in-process fake MCP server and counting client the crate's own tests and
# `rakka-agent`'s dispatcher proofs drive. Pulls rmcp's server side and axum.
testkit = ["rmcp/server", "rmcp/transport-streamable-http-server", "dep:axum"]

[dependencies]
# Exact pin (docs/rakka-compatibility.md). `client` + the Streamable HTTP
# client over rmcp's own reqwest (`reqwest` = `reqwest?/rustls`, TLS only;
# nothing here enables `reqwest/system-proxy`), and `transport-io` so a
# launcher's AsyncRead/AsyncWrite pair is a transport. The server side and
# the child-process transport are feature-gated above.
rmcp = { version = "=3.4.0", default-features = false, features = ["client", "transport-streamable-http-client-reqwest", "reqwest", "transport-io"] }
# The tool contracts this adapter fills: descriptors, bindings, declarations,
# the dispatch executor trait, refusals, identity validation. Default features
# off: an MCP executor never touches the model adapter.
rakka-agent = { path = "../rakka-agent", version = "0.1.0", default-features = false }
# `AgentEphemeralCredential`, `ArtifactRef`, the artifact store, timestamps.
rakka-agent-workflow = { path = "../rakka-agent-workflow", version = "0.1.0" }
serde.workspace = true
serde_json.workspace = true
tokio.workspace = true
futures-util.workspace = true
axum = { workspace = true, optional = true }

[dev-dependencies]
axum.workspace = true
tokio.workspace = true
rmcp = { version = "=3.4.0", default-features = false, features = ["server", "transport-streamable-http-server"] }

[lints]
workspace = true
```

`[dev-dependencies] rmcp` with extra features is how the crate's own tests get the server side without the `testkit` feature; Cargo unifies the two declarations. Add `"crates/rakka-agent-mcp",` to the workspace `members`. In `crates/rakka/Cargo.toml` add `agent-mcp = ["agent", "dep:rakka-agent-mcp"]` beside `agent-rig`/`agent-otel` and the optional path dependency; check `crates/rakka/src/lib.rs` for how `rakka_agent_knowledge_graph` or `rakka_a2a` are re-exported under their features and mirror it with `#[cfg(feature = "agent-mcp")] pub use rakka_agent_mcp;`.

- [ ] **Step 4: `lib.rs` and `binding.rs`**

`src/lib.rs`:

```rust
//! Remote MCP servers as Rakka tools, under three conditions the host holds
//! as invariants: the transport client is injected and every attempt's
//! server URL passes a required egress check before a client exists; no
//! child process runs without a deployment-supplied launcher; descriptors are
//! release data synced at publish time, and the only credential path is the
//! binding's logical reference resolved by the dispatcher inside the attempt.
//!
//! Module map:
//! - `binding`: server ids, transports, tool policies, the binding record, and
//!   its refusals.
//! - `client`: per-attempt rmcp client construction shared by the sync and the
//!   executor — credential to header, protocol negotiation, the peer-agent
//!   rule, error mapping.
//! - `sync`: `sync_mcp_descriptors`, the stored descriptor set, hint
//!   narrowing, staleness.
//! - `executor`: `McpDispatchToolExecutor`, the egress check, result mapping.
//! - `launcher`: the child-process seam and (feature `child-process`) the
//!   unsandboxed reference launcher.
//! - `testkit` (feature `testkit`): the in-process fake server and counting
//!   client.
//!
//! MCP is never an agent-to-agent channel ([specification 14.4]); a server
//! that identifies as a Rakka agent is refused.
//!
//! [specification 14.4]: ../../../docs/plans/rakka-agent/spec.md

#![forbid(unsafe_code)]

pub mod binding;
pub mod client;
pub mod executor;
pub mod launcher;
pub mod sync;
#[cfg(feature = "testkit")]
pub mod testkit;

pub use binding::{ … every public item … };
pub use client::{McpClientError};
pub use executor::{McpAllowAllEgress, McpArtifactStore, McpDispatchToolExecutor, McpEgressCheck};
pub use launcher::{McpChildProcessLauncher, McpChildTransport, McpLaunchError, McpLaunchFuture};
#[cfg(feature = "child-process")]
pub use launcher::TokioChildProcessLauncher;
pub use sync::{mcp_descriptor_staleness, sync_mcp_descriptors, McpDescriptorSet, McpDescriptorStaleness, McpSyncError, McpSyncedDescriptor};
```

For Task 2 create `client.rs`, `executor.rs`, `launcher.rs`, `sync.rs` as empty modules with a one-line `//!` doc each (Tasks 3–5 fill them) so the crate compiles; keep only the `binding` re-exports until then.

`src/binding.rs` — write it in full: the constants above with docs; `McpServerId` (`#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]`, a `Deserialize` impl that calls `new`), `McpTransport` (`#[serde(tag = "kind", rename_all = "kebab-case")]`), `McpToolPolicy`, `McpDescriptorRefresh` (`#[serde(tag = "mode", rename_all = "kebab-case")]`, `#[derive(Default)]` with `#[default] Manual`), `McpServerBinding` (`#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]`, `#[serde(default)]` on `protocol_versions` (falling back to the two defaults when empty on decode is NOT done — an empty list is a validation refusal), `refresh`, `credential_binding`), the builders, `validate` (server id already validated by type; URL rule; `tools` non-empty; every tool name derives a valid id; `protocol_versions` non-empty and each entry a `YYYY-MM-DD` string of exactly 10 ASCII bytes; every policy `max_attempts >= 1`), `tool_id`, and `McpRegistrationError` with `code`, `Display` (messages name the server and the tool, never a URL's query string), `std::error::Error`. Unit tests for the URL checker's branches live inline.

- [ ] **Step 5: Run the tests**

Run: `cargo test -p rakka-agent-mcp` then `cargo test -p rakka-agent-mcp --all-features` (compiles the empty testkit module), `cargo check -p rakka --features agent-mcp`, `cargo check -p rakka --no-default-features --features agent`, `cargo clippy -p rakka-agent-mcp --all-targets --all-features -- -D warnings`, `cargo fmt --all -- --check`, `RUSTDOCFLAGS="-D warnings" cargo doc -p rakka-agent-mcp --no-deps --all-features`, `cargo tree -p rakka-agent-mcp -e features -i reqwest --all-features | grep -c system-proxy` (expected `0`), and `cargo test -p rakka-testkit --test repository_hygiene` (it checks members, publish flags, and the boundary inventory — if it fails on the new crate's absence from `docs/rakka-api-boundary-inventory.md`, add the Adapter row now rather than waiting for Task 7, and say so).
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml Cargo.lock crates/rakka/Cargo.toml crates/rakka/src/lib.rs crates/rakka-agent-mcp docs/rakka-api-boundary-inventory.md
git commit -m "Add the rakka-agent-mcp crate: pinned rmcp client, server bindings with operator-declared tool policies, and stable refusals

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 3: The fake server testkit, the shared client seam, and publish-time descriptor sync

**Files:**
- Create: `crates/rakka-agent-mcp/src/testkit.rs` (feature `testkit`)
- Create: `crates/rakka-agent-mcp/src/client.rs` (replacing the empty module)
- Create: `crates/rakka-agent-mcp/src/sync.rs` (replacing the empty module)
- Create: `crates/rakka-agent-mcp/tests/descriptor_sync.rs`
- Modify: `crates/rakka-agent-mcp/src/lib.rs` (re-exports)

**Interfaces:**
- Consumes: Task 2's binding types; rmcp's client API (verified list above); `rakka_agent::{AgentToolBinding, AgentToolDescriptor, AgentToolKind, AgentSchemaRef, AgentSchemaId, AgentRevisionNumber, AgentContentDigest, AGENT_TOOL_PARAMETERS_MAX_BYTES, AGENT_TOOL_DESCRIPTION_MAX_LENGTH}`, `rakka_agent_workflow::{AgentEphemeralCredential, AgentEphemeralCredentialMaterial, AgentTimestampMillis, ArtifactRef}`.
- Produces:
  - `testkit::FakeTool { name, description, input_schema: Value, output_schema: Option<Value>, annotations: Option<rmcp::model::ToolAnnotations>, behaviour: FakeToolBehaviour }` with `FakeToolBehaviour::{Text(String), Structured(Value), Image { bytes: usize }, Error(String), InputRequired, Task, Echo}` (`Echo` answers the arguments as structured content, and `Error` answers `isError: true` with the text).
  - `testkit::FakeMcpServer::new() -> Self`, `with_tool(FakeTool)`, `with_server_name(impl Into<String>)`, `with_supported_versions(Vec<ProtocolVersion>)`; counters `list_calls() -> usize`, `call_count() -> usize`, `seen_calls() -> Vec<SeenCall { name, arguments: Value, meta: Value, authorization: Option<String>, headers: Vec<(String, String)> }>` (the HTTP headers are captured by an axum middleware layer in `serve_fake` and attached to the next seen call); `swap_tool_schema(name, Value)` to mutate a tool's schema after sync (the mismatch test).
  - `testkit::serve_fake(server: FakeMcpServer) -> FakeMcpEndpoint { pub url: String /* http://127.0.0.1:<port>/mcp */, pub server: FakeMcpServer, handle: JoinHandle<()> }` (`Drop` aborts the task). Mounts `rmcp::transport::streamable_http_server::tower::StreamableHttpService::new(move || Ok(handler.clone()), Arc::new(LocalSessionManager::default()), StreamableHttpServerConfig { json_response: true, ..Default::default() })` at `/mcp` on an axum `Router` over a `TcpListener` bound to `127.0.0.1:0`.
  - `testkit::CountingClient { inner: rmcp's reqwest client (rmcp re-exports `reqwest`? if not, take `rmcp::transport::streamable_http_client::…`'s reqwest through the `__reqwest`-gated path — the crate depends on rmcp's reqwest only transitively; declare `reqwest = { version = "0.13", default-features = false }` in `[dev-dependencies]`/testkit only if rmcp does not re-export it, and record which), sends: Arc<AtomicUsize> }` implementing `StreamableHttpClient` by delegation (`post_message`, `delete_session`, `get_stream`), counting every method.
  - `client::McpClientSession<C>` (crate-private except its type): `client::connect<C>(http: &C, binding: &McpServerBinding, credential: Option<&AgentEphemeralCredential>) -> Result<McpClientSession<C>, McpClientError>` building `StreamableHttpClientTransport::with_client(http.clone(), config)` with `config.uri = url`, `allow_stateless = true`, the auth header / custom header from the credential, then `serve_client_with_lifecycle(client_info(), transport, ClientLifecycleMode::Discover { preferred_versions: from binding.protocol_versions })`, then the peer-agent check on `peer_info().server_info.name`; `McpClientSession::{peer(), negotiated_version(), server_name(), close()}`; `client::client_info() -> ClientInfo` (`Implementation { name: MCP_CLIENT_NAME, version: env!("CARGO_PKG_VERSION"), .. }`); `client::connect_over(transport: impl IntoTransport, ..)` for the launcher path (Task 5).
  - `client::McpClientError { ProtocolUnsupported { server, client: Vec<String>, server_versions: Vec<String> }, PeerAgentChannel { server, name }, CredentialMaterialUnsupported { server, material: &'static str }, Transport { server, reason: String }, Protocol { server, reason: String } }` with `code()`: `mcp-protocol-unsupported`, `mcp-peer-agent-channel-refused`, `mcp-credential-material-unsupported`, and `mcp-descriptor-sync-failed` for `Transport`/`Protocol` when raised by the sync; the executor maps `Transport`/`Protocol` to `AgentDispatchError::Invocation` (Task 4). `reason` strings are rmcp error *variant names* plus a status code when one exists — never a response body (read `ClientInitializeError`, `ServiceError`, and `StreamableHttpError` variants and map each by name).
  - Credential → header: `BearerToken { token }` → `auth_header = Some(<value>)` where `<value>` is what rmcp's reqwest impl expects — read `transport/common/reqwest/streamable_http_client.rs:49–…` and confirm whether it prefixes `Bearer ` or sends the string verbatim as the `Authorization` value; set accordingly and record it; `ApiKey { name, value }` → `custom_headers.insert(HeaderName::from_bytes(name)?, HeaderValue::from_str(value)?)` (an invalid header name/value → `CredentialMaterialUnsupported`); `Basic`/`Custom` → `CredentialMaterialUnsupported`. The value never appears in any error.
  - `sync::sync_mcp_descriptors<C>(http: &C, binding: &McpServerBinding, credential: Option<&AgentEphemeralCredential>, synced_at: AgentTimestampMillis) -> Result<McpDescriptorSet, McpSyncError>` where `C: StreamableHttpClient + Clone + Send + Sync + 'static`.
  - `sync::McpDescriptorSet { pub server_id: McpServerId, pub server_name: String, pub protocol_version: String, pub synced_at: AgentTimestampMillis, pub descriptors: Vec<McpSyncedDescriptor> }` (serde; `digest(&self) -> AgentContentDigest` over the canonical JSON of `descriptors`' `(tool, schema_digest, output_schema_digest)` triples; `bindings(&self) -> impl Iterator<Item = &AgentToolBinding>`; `descriptor(&self, tool: &str) -> Option<&McpSyncedDescriptor>`).
  - `sync::McpSyncedDescriptor { pub tool: String, pub binding: AgentToolBinding, pub input_schema: Value, pub schema_digest: AgentContentDigest, pub output_schema_digest: Option<AgentContentDigest>, pub input_schema_artifact: Option<ArtifactRef> }` with `with_input_schema_artifact(self, ArtifactRef) -> Self` and `carries_inline_schema(&self) -> bool` (`binding.descriptor.parameters.is_some()`).
  - `sync::McpSyncError { Client(McpClientError), SchemaTooLarge { server, tool, bytes: usize }, HintContradictsDeclaration { server, tool, hint: &'static str }, Registration(McpRegistrationError), Descriptor { server, tool, reason: String } }` with `code()`: `SchemaTooLarge → mcp-descriptor-schema-too-large`, `HintContradictsDeclaration → mcp-hint-contradicts-declaration`, `Client(e) → e.code()` except `Transport`/`Protocol` → `mcp-descriptor-sync-failed`, `Registration(e) → e.code()`, `Descriptor → mcp-binding-invalid`.
  - `sync::mcp_descriptor_staleness(stored: &McpDescriptorSet, fresh: &McpDescriptorSet) -> McpDescriptorStaleness` with `McpDescriptorStaleness::{Fresh, Stale { changed: Vec<String> }}` (a tool added, removed, or with a different `schema_digest`/`output_schema_digest`).

- [ ] **Step 1: Write the failing tests**

Create `crates/rakka-agent-mcp/tests/descriptor_sync.rs`:

```rust
//! Publish-time descriptor sync: a standalone function over the injected
//! client, no store, no registry; the allow-list, the bounds, the hint rule,
//! the peer-agent rule, round-tripping, and the fact that a registry built
//! from the stored set makes no network call.

use rakka_agent::{
    AgentEffectSafetyClass, AgentToolDeclaration, AgentToolKind, AgentToolRegistry,
    AGENT_TOOL_PARAMETERS_MAX_BYTES,
};
use rakka_agent_mcp::testkit::{serve_fake, FakeMcpServer, FakeTool, FakeToolBehaviour};
use rakka_agent_mcp::{
    mcp_descriptor_staleness, sync_mcp_descriptors, McpDescriptorSet, McpDescriptorStaleness,
    McpServerBinding, McpServerId, McpToolPolicy, MCP_DESCRIPTOR_SCHEMA_MAX_BYTES,
};
use rakka_agent_workflow::{AgentEphemeralCredential, AgentTimestampMillis};
use rmcp::model::ToolAnnotations;
use serde_json::json;

fn schema(props: usize) -> serde_json::Value {
    let mut properties = serde_json::Map::new();
    for i in 0..props {
        properties.insert(format!("p{i}"), json!({ "type": "string", "description": "x".repeat(40) }));
    }
    json!({ "type": "object", "properties": properties })
}

fn tool(name: &str) -> FakeTool {
    FakeTool::text(name, "Does a thing.", schema(1), "ok")
}

fn declared(class: AgentEffectSafetyClass) -> McpToolPolicy {
    McpToolPolicy::new(AgentToolDeclaration::new(class))
}

fn client() -> rakka_agent_mcp::testkit::ReqwestClient {
    rakka_agent_mcp::testkit::ReqwestClient::new()
}

fn binding(url: &str) -> McpServerBinding {
    McpServerBinding::streamable_http(McpServerId::new("crm").expect("id"), url)
        .with_tool("search", declared(AgentEffectSafetyClass::ReadOnly))
        .expect("tool")
        .with_tool("update", declared(AgentEffectSafetyClass::Idempotent))
        .expect("tool")
}

#[tokio::test]
async fn sync_returns_only_the_allow_listed_tools_as_prefixed_bindings_with_digests() {
    let endpoint = serve_fake(
        FakeMcpServer::new().with_tool(tool("search")).with_tool(tool("update")).with_tool(tool("delete_everything")),
    )
    .await;
    let set = sync_mcp_descriptors(&client(), &binding(&endpoint.url), None, AgentTimestampMillis::new(7))
        .await
        .expect("syncs");
    let names: Vec<&str> = set.descriptors.iter().map(|d| d.tool.as_str()).collect();
    assert_eq!(names, vec!["search", "update"], "the unlisted tool is not registered");
    let search = set.descriptor("search").expect("present");
    assert_eq!(search.binding.descriptor.tool.as_str(), "mcp.crm.search");
    assert_eq!(search.binding.descriptor.kind, AgentToolKind::RemoteMcp);
    assert_eq!(search.binding.declaration.safety, AgentEffectSafetyClass::ReadOnly);
    assert!(search.carries_inline_schema(), "a 1-property schema rides the descriptor");
    assert_eq!(search.binding.descriptor.parameters.as_ref(), Some(&search.input_schema));
    assert_eq!(set.synced_at, AgentTimestampMillis::new(7));
    assert_eq!(set.protocol_version, "2026-07-28");
    assert_eq!(endpoint.server.list_calls(), 1);
    let encoded = serde_json::to_string(&set).expect("encodes");
    let decoded: McpDescriptorSet = serde_json::from_str(&encoded).expect("decodes");
    assert_eq!(decoded, set);
    assert_eq!(decoded.digest(), set.digest());
}

#[tokio::test]
async fn a_registry_built_from_the_stored_set_makes_no_network_call() {
    let endpoint = serve_fake(FakeMcpServer::new().with_tool(tool("search")).with_tool(tool("update"))).await;
    let set = sync_mcp_descriptors(&client(), &binding(&endpoint.url), None, AgentTimestampMillis::new(1)).await.expect("syncs");
    let before = endpoint.server.list_calls();
    let mut registry = AgentToolRegistry::new();
    for binding in set.bindings() {
        registry = registry.register(binding.clone()).expect("registers");
    }
    assert!(registry.binding(&rakka_agent::AgentToolId::new("mcp.crm.search").expect("id")).is_some());
    assert_eq!(endpoint.server.list_calls(), before, "Manual refresh: no call at registry construction");
}

#[tokio::test]
async fn the_credential_reaches_the_wire_only_as_the_declared_header() {
    let endpoint = serve_fake(FakeMcpServer::new().with_tool(tool("search"))).await;
    let credential = AgentEphemeralCredential::bearer_token("sync-token-sentinel");
    sync_mcp_descriptors(&client(), &binding(&endpoint.url), Some(&credential), AgentTimestampMillis::new(1)).await.expect("syncs");
    let seen = endpoint.server.seen_headers();
    assert!(seen.iter().any(|(name, value)| name.eq_ignore_ascii_case("authorization") && value.ends_with("sync-token-sentinel")), "{seen:?}");
    let basic = AgentEphemeralCredential::basic("u", "p");
    let error = sync_mcp_descriptors(&client(), &binding(&endpoint.url), Some(&basic), AgentTimestampMillis::new(1)).await.expect_err("basic is not a header");
    assert_eq!(error.code(), "mcp-credential-material-unsupported");
    assert!(!error.to_string().contains('p'), "no material in the message: {error}");
}

#[tokio::test]
async fn the_schema_bounds_are_the_4kib_inline_and_64kib_refusal_lines() {
    let big_inline = schema(200); // > 4 KiB, < 64 KiB
    assert!(serde_json::to_vec(&big_inline).expect("bytes").len() > AGENT_TOOL_PARAMETERS_MAX_BYTES);
    let huge = schema(3000);
    assert!(serde_json::to_vec(&huge).expect("bytes").len() > MCP_DESCRIPTOR_SCHEMA_MAX_BYTES);
    let endpoint = serve_fake(
        FakeMcpServer::new()
            .with_tool(FakeTool::text("search", "d", big_inline.clone(), "ok"))
            .with_tool(FakeTool::text("update", "d", huge, "ok")),
    )
    .await;
    let only_search = McpServerBinding::streamable_http(McpServerId::new("crm").expect("id"), &endpoint.url)
        .with_tool("search", declared(AgentEffectSafetyClass::ReadOnly)).expect("tool");
    let set = sync_mcp_descriptors(&client(), &only_search, None, AgentTimestampMillis::new(1)).await.expect("syncs");
    let search = set.descriptor("search").expect("present");
    assert!(!search.carries_inline_schema(), "over 4 KiB the descriptor carries no parameters");
    assert_eq!(search.input_schema, big_inline, "but the raw schema is returned for the caller to store");
    let error = sync_mcp_descriptors(&client(), &binding(&endpoint.url), None, AgentTimestampMillis::new(1)).await.expect_err("64 KiB");
    assert_eq!(error.code(), "mcp-descriptor-schema-too-large");
}

#[tokio::test]
async fn a_contradicting_hint_refuses_only_when_hints_are_honored() {
    let destructive = FakeTool::text("update", "d", schema(1), "ok").with_annotations(ToolAnnotations {
        destructive_hint: Some(true),
        ..Default::default()
    });
    let endpoint = serve_fake(FakeMcpServer::new().with_tool(tool("search")).with_tool(destructive)).await;
    let ignoring = binding(&endpoint.url);
    sync_mcp_descriptors(&client(), &ignoring, None, AgentTimestampMillis::new(1)).await.expect("hints ignored by default");
    let honoring = McpServerBinding::streamable_http(McpServerId::new("crm").expect("id"), &endpoint.url)
        .with_tool("update", declared(AgentEffectSafetyClass::Idempotent).with_honor_hints(true)).expect("tool");
    let error = sync_mcp_descriptors(&client(), &honoring, None, AgentTimestampMillis::new(1)).await.expect_err("contradiction");
    assert_eq!(error.code(), "mcp-hint-contradicts-declaration");
    let honoring_non_idempotent = McpServerBinding::streamable_http(McpServerId::new("crm").expect("id"), &endpoint.url)
        .with_tool("update", declared(AgentEffectSafetyClass::NonIdempotent).with_honor_hints(true)).expect("tool");
    let set = sync_mcp_descriptors(&client(), &honoring_non_idempotent, None, AgentTimestampMillis::new(1)).await.expect("no contradiction");
    assert_eq!(set.descriptor("update").expect("present").binding.declaration.safety, AgentEffectSafetyClass::NonIdempotent, "a hint never changes a declaration");
}

#[tokio::test]
async fn a_server_that_identifies_as_a_rakka_agent_is_refused() {
    let endpoint = serve_fake(FakeMcpServer::new().with_tool(tool("search")).with_server_name("rakka-agent-support")).await;
    let error = sync_mcp_descriptors(&client(), &binding(&endpoint.url), None, AgentTimestampMillis::new(1)).await.expect_err("peer channel");
    assert_eq!(error.code(), "mcp-peer-agent-channel-refused");
}

#[tokio::test]
async fn version_negotiation_falls_back_to_the_next_listed_version_or_refuses() {
    let older = serve_fake(FakeMcpServer::new().with_tool(tool("search")).with_supported_versions(vec![rmcp::model::ProtocolVersion::V_2025_11_25])).await;
    let set = sync_mcp_descriptors(&client(), &binding(&older.url), None, AgentTimestampMillis::new(1)).await.expect("falls back");
    assert_eq!(set.protocol_version, "2025-11-25");
    let ancient = serve_fake(FakeMcpServer::new().with_tool(tool("search")).with_supported_versions(vec![rmcp::model::ProtocolVersion::V_2024_11_05])).await;
    let error = sync_mcp_descriptors(&client(), &binding(&ancient.url), None, AgentTimestampMillis::new(1)).await.expect_err("no common version");
    assert_eq!(error.code(), "mcp-protocol-unsupported");
}

#[tokio::test]
async fn a_dead_endpoint_is_a_sync_failure_with_no_body_in_the_message() {
    let closed = McpServerBinding::streamable_http(McpServerId::new("crm").expect("id"), "http://127.0.0.1:9/mcp")
        .with_tool("search", declared(AgentEffectSafetyClass::ReadOnly)).expect("tool");
    let error = sync_mcp_descriptors(&client(), &closed, None, AgentTimestampMillis::new(1)).await.expect_err("dead");
    assert_eq!(error.code(), "mcp-descriptor-sync-failed");
    assert!(error.to_string().len() < 512);
}

#[tokio::test]
async fn staleness_reports_added_removed_and_changed_tools() {
    let endpoint = serve_fake(FakeMcpServer::new().with_tool(tool("search")).with_tool(tool("update"))).await;
    let stored = sync_mcp_descriptors(&client(), &binding(&endpoint.url), None, AgentTimestampMillis::new(1)).await.expect("syncs");
    assert_eq!(mcp_descriptor_staleness(&stored, &stored), McpDescriptorStaleness::Fresh);
    endpoint.server.swap_tool_schema("update", schema(2));
    let fresh = sync_mcp_descriptors(&client(), &binding(&endpoint.url), None, AgentTimestampMillis::new(2)).await.expect("syncs");
    assert_eq!(mcp_descriptor_staleness(&stored, &fresh), McpDescriptorStaleness::Stale { changed: vec!["update".to_string()] });
}
```

`FakeTool::text(name, description, input_schema, answer)` and `.with_annotations(..)` are the testkit's constructors; `FakeMcpServer::seen_headers()` returns the last request's headers as `(name, value)` pairs; `testkit::ReqwestClient` re-exports the reqwest client type rmcp's transport implements `StreamableHttpClient` for (rmcp depends on `reqwest` 0.13 under `__reqwest`; the testkit re-exports it as `pub use reqwest::Client as ReqwestClient;` — this needs the crate to name `reqwest` as a dependency; add `reqwest = { version = "0.13", default-features = false, optional = true }` gated by `testkit` and ALSO by nothing else: the executor's own client type is generic. If rmcp re-exports its reqwest (`grep -rn "pub use reqwest" ~/.cargo/registry/src/*/rmcp-3.4.0/src`), prefer that re-export and add no dependency. Record which.)

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p rakka-agent-mcp --features testkit --test descriptor_sync`
Expected: compile errors on the missing testkit and sync items.

- [ ] **Step 3: The testkit**

`src/testkit.rs`: `FakeMcpServer` is `Clone` over `Arc<Mutex<FakeState { tools: Vec<FakeTool>, server_name: String, versions: Vec<ProtocolVersion>, list_calls: usize, calls: Vec<SeenCall>, last_headers: Vec<(String,String)> }>>`; implement `rmcp::ServerHandler`:

- `get_info()`: `ServerInfo { protocol_version: <first supported>, capabilities: ServerCapabilities::builder().enable_tools().build(), server_info: Implementation { name: server_name, version: "fake".into(), .. }, instructions: None, .. }` (fill every required field; use `..Default::default()` where the type has `Default`).
- `supported_protocol_versions()`: `Cow::Owned(versions.clone())`.
- `list_tools`: increments `list_calls`; returns `ListToolsResult` with each `FakeTool` as `rmcp::model::Tool { name: Cow::Owned(name), description: Some(..), input_schema: Arc::new(input_schema.as_object().cloned().unwrap_or_default()), output_schema, annotations, .. }` and `next_cursor: None`.
- `call_tool(params, _ctx)`: records `SeenCall { name, arguments: Value::Object(params.arguments.unwrap_or_default()), meta: params.meta.map(|m| Value::Object(m.0.0)).unwrap_or(Value::Null), authorization: last_headers' `authorization` value, headers: last_headers.clone() }`; answers by behaviour: `Text(s)` → `CallToolResponse::Complete(CallToolResult { content: vec![ContentBlock::text(s)], structured_content: None, is_error: Some(false), .. })`; `Structured(v)` → `structured_content: Some(v)`; `Image { bytes }` → `ContentBlock::image(base64 of `bytes` zeroes, "image/png")` (use rmcp's `Content::image` constructor if present); `Error(s)` → `is_error: Some(true)` with text `s`; `InputRequired` → `CallToolResponse::InputRequired(InputRequiredResult { .. minimal valid value per `model/mrtr.rs` .. })`; `Task` → `CallToolResponse::Task(CreateTaskResult { .. minimal .. })`; `Echo` → `structured_content: Some(arguments)`.

`serve_fake`: an axum `Router` with a middleware (`axum::middleware::from_fn_with_state`) that copies the request headers into `last_headers`, then `.nest_service("/mcp", StreamableHttpService::new(..))`; `axum::serve(listener, router)` spawned; returns `FakeMcpEndpoint { url: format!("http://{addr}/mcp"), server, handle }`. If `nest_service` and rmcp's tower service disagree on the body type, use `axum::routing::any_service` on `/mcp` (rmcp's own examples do `Router::new().nest_service("/mcp", service)` — mirror them, and note rmcp's comment at `tower.rs:907` about `Host`).

`CountingClient`: fields `inner: ReqwestClient, sends: Arc<AtomicUsize>`; `impl StreamableHttpClient for CountingClient` delegating the three required methods and incrementing `sends` in each.

- [ ] **Step 4: The client seam**

`src/client.rs`: `client_info()`, `credential_headers(binding, credential) -> Result<(Option<String>, HashMap<HeaderName, HeaderValue>), McpClientError>`, `preferred_versions(binding) -> Vec<ProtocolVersion>` (`ProtocolVersion::from(String)`; if `ProtocolVersion` has no public constructor from a string, match the two known constants and refuse others at `McpServerBinding::validate` as `mcp-binding-invalid` — record which), `connect(http, binding, credential) -> Result<McpClientSession<C>, McpClientError>`, `McpClientSession { running: RunningService<RoleClient, ClientInfo>, server_name: String, negotiated: ProtocolVersion }` with `peer(&self) -> &Peer<RoleClient>`, `close(self)` (`running.cancel(None).await`, ignoring the result). Error mapping: `ClientInitializeError::NoCompatibleProtocolVersion { client_supported, server_supported }` → `ProtocolUnsupported`; `TransportError`/`ConnectionClosed` → `Transport { reason: <variant name> }`; the rest → `Protocol { reason: <variant name> }`. The peer-agent check: `running.peer_info().map(|info| info.server_info.name.clone())`; a name starting with `MCP_PEER_AGENT_SERVER_PREFIX` → `PeerAgentChannel` (after `close()`).

- [ ] **Step 5: The sync**

`src/sync.rs`: `sync_mcp_descriptors` = `binding.validate()` → `connect` → `peer().list_all_tools()` (error → `Client(Transport/Protocol)`) → for each `(name, policy)` in `binding.tools` in order: find the tool (absent → `Descriptor { reason: "the server does not list it" }`), `serde_json::to_vec(&input_schema)` length: `> MCP_DESCRIPTOR_SCHEMA_MAX_BYTES` → `SchemaTooLarge`; hints (`honor_hints` per refinement 8) → `HintContradictsDeclaration { hint: "destructiveHint" | "idempotentHint" | "readOnlyHint" }`; description = tool description or `format!("MCP tool {name} on {server}")`, truncated at a char boundary to `AGENT_TOOL_DESCRIPTION_MAX_LENGTH`; `AgentToolDescriptor::new(binding.tool_id(name)?, RemoteMcp, description, AgentSchemaRef { schema_id: AgentSchemaId::new(format!("mcp.{server}.{name}.input"))?, version: INITIAL }, AgentSchemaRef { .. ".output" .. })` → `.with_parameters(schema)` only when `len <= AGENT_TOOL_PARAMETERS_MAX_BYTES` → set `result_behavior` from the policy (use the descriptor's builder if one exists; else set the public field) → `AgentToolBinding::new(descriptor, policy.declaration.clone(), policy.max_attempts)` + `timeout_ms` (builder or field) → `McpSyncedDescriptor { tool, binding, input_schema, schema_digest: AgentContentDigest::of_json(&input_schema), output_schema_digest: output_schema.map(|s| of_json(&s)), input_schema_artifact: None }`; then `close()`; return the set with `server_name`, `protocol_version: negotiated.to_string()`. `mcp_descriptor_staleness` is a pure comparison over the two sets' `(tool → digests)` maps.

- [ ] **Step 6: Run the tests**

Run: `cargo test -p rakka-agent-mcp --features testkit --test descriptor_sync` then `cargo test -p rakka-agent-mcp --all-features`, `cargo test -p rakka-agent-mcp` (no features: the crate-shape and binding tests still compile), `cargo clippy -p rakka-agent-mcp --all-targets --all-features -- -D warnings`, `cargo fmt --all -- --check`, `RUSTDOCFLAGS="-D warnings" cargo doc -p rakka-agent-mcp --no-deps --all-features`.
Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add crates/rakka-agent-mcp Cargo.lock
git commit -m "Sync MCP descriptors at publish time over the injected client, with the schema bounds, the hint rule, and the peer-agent refusal

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 4: `McpDispatchToolExecutor` — the required egress check, the per-attempt client, the recheck, and result mapping

**Files:**
- Create: `crates/rakka-agent-mcp/src/executor.rs` (replacing the empty module)
- Modify: `crates/rakka-agent-mcp/src/lib.rs` (re-exports)
- Create: `crates/rakka-agent-mcp/tests/client_dispatch.rs`

**Interfaces:**
- Consumes: Tasks 2–3; `rakka_agent::{AgentDispatchToolExecutor, AgentDispatchError, AgentDispatchFuture, AgentAuthorityRefusal, AgentRunScope, AgentRunEffect, AgentToolCallRequest, AgentTaskContent, AgentContentDigest, AgentToolResultBehavior}`, `rakka_agent_workflow::{AgentArtifactStore, AgentArtifactWriteRequest, ArtifactKind, ArtifactRef, AgentEphemeralCredential}`; rmcp `Peer::{list_all_tools, call_tool_once}`, `CallToolRequestParams`, `RequestMetaObject`, `MetaObject`, `CallToolResponse`, `ContentBlock`.
- Produces:
  - `pub trait McpEgressCheck: Send + Sync + 'static { fn check(&self, server: &McpServerId, url: &str) -> Result<(), AgentAuthorityRefusal>; }`
  - `pub struct McpAllowAllEgress;` (`impl McpEgressCheck` always `Ok(())`; doc: the explicit opt-out for in-cluster and test use).
  - `pub type McpArtifactStore = Arc<tokio::sync::Mutex<dyn AgentArtifactStore + Send>>;` with `pub fn mcp_artifact_store<S: AgentArtifactStore + Send + 'static>(store: S) -> McpArtifactStore`.
  - `pub struct McpDispatchToolExecutor<C>` with `new(descriptors: Vec<McpDescriptorSet>, bindings: Vec<McpServerBinding>, artifacts: McpArtifactStore, http: C, egress: Arc<dyn McpEgressCheck>) -> Result<Self, McpRegistrationError>`, `with_child_process_launcher(self, launcher: Arc<dyn McpChildProcessLauncher>) -> Result<Self, McpRegistrationError>` (Task 5 fills the launcher path; in this task it stores the launcher and re-validates), `with_descriptor_recheck_ttl_ms(self, ttl_ms: u64) -> Self`, `bound_tools(&self) -> impl Iterator<Item = &AgentToolId>`; `impl<C: StreamableHttpClient + Clone + Send + Sync + 'static> AgentDispatchToolExecutor for McpDispatchToolExecutor<C>`; `Debug` prints server and tool counts only.
  - `new` validation: every binding `validate()`s; server ids unique (`DuplicateServer`); every binding has a set (`DescriptorSetMissing`) and every set a binding (`BindingMissing`); every tool in a binding's allow-list has a synced descriptor in its set (`Descriptor`-class → `mcp-binding-invalid`); a `ChildProcess` binding with no launcher → `TransportUnsupported`. Builds `tools: BTreeMap<AgentToolId, (McpServerId, index into set)>`.

Per attempt (`execute`), in this order:
1. `call.tool` → entry, else `Err(AgentDispatchError::collaborator("mcp-tool-unbound", format!("{tool} is bound to no MCP server this executor holds")))`.
2. For `StreamableHttp { url }`: `self.egress.check(server, url)` → `Err(refusal)` ⇒ `Err(AgentDispatchError::collaborator(refusal.code, refusal.message))` — before the credential is touched (the function must not read `credential` above this line; structure the code so the borrow is first used after the check).
3. `client::connect(&self.http, binding, credential)` (or the launcher path) → `McpClientError` ⇒ `ProtocolUnsupported`/`PeerAgentChannel`/`CredentialMaterialUnsupported` → `collaborator(code, message)`; `Transport`/`Protocol` → `AgentDispatchError::Invocation { code: "mcp-transport-failed", message }` if the enum admits a new `&'static str` code there — the variant's `code` is `&'static str` (dispatch.rs:4185), so `"mcp-transport-failed"` is fine; register it? It is a *pipeline* code carried inside `Invocation`; the durable detail composes `code: message`. Add it to the registered list in Task 7 (fourteen codes).
4. Recheck: if the server's cache entry is older than the TTL (or TTL is 0): `peer().list_all_tools()`, rebuild `tool → AgentContentDigest::of_json(&input_schema)` for the server, stamp `Instant::now()`; then compare the stored `schema_digest` with the live one: absent or different → `collaborator("tool-descriptor-revision-mismatch", format!("{tool}: the server's schema no longer matches the published descriptor"))` (after `close()`).
5. Arguments: `call.arguments.as_object().cloned()` — a non-object → `collaborator("mcp-tool-error", "the call's arguments are not a JSON object")`.
6. `_meta`: `MetaObject::new()`; `set_traceparent`/`set_tracestate` from `intent.telemetry` when present; `insert(MCP_META_IDEMPOTENCY_KEY, json!(key))` where `key = intent.external_key().map(ToString::to_string).unwrap_or_else(|| intent.idempotency_key.as_str().to_string())`; `CallToolRequestParams { meta: Some(RequestMetaObject(meta)), name: Cow::Owned(tool_name), arguments: Some(args), input_responses: None }`.
7. `tokio::time::timeout(Duration::from_millis(intent.timeout_ms.unwrap_or(u64::MAX / 2)), peer().call_tool_once(params))` → elapsed ⇒ `Invocation { code: "mcp-transport-failed", message: "the call exceeded the effect's timeout" }`; `ServiceError` ⇒ `Invocation` with the variant name.
8. Map: `CallToolResponse::InputRequired(_)` ⇒ `collaborator("mcp-input-required", ..)`; `Task(_)` ⇒ `collaborator("mcp-protocol-unsupported", "task-augmented results are not requested by this client")`; `Complete(result)`: `is_error == Some(true)` ⇒ `collaborator("mcp-tool-error", detail)` where `detail` = the first `Text` block's text with control characters replaced by spaces, cut at a char boundary to `MCP_TOOL_ERROR_DETAIL_MAX_BYTES`, or `"the tool reported an error"` when there is no text; otherwise `content_of(result)`:
   - `structured_content: Some(value)` → candidate `value`; else the text blocks joined by `\n` → candidate `json!({ "text": joined })`; any `Image`/`Audio`/`Resource`/`ResourceLink` block ⇒ overflow (binary or reference parts never inline).
   - candidate serialized length ≤ `MCP_INLINE_RESULT_MAX_BYTES` and no overflow ⇒ `AgentTaskContent::inline(candidate)` (its own 8 KiB validation cannot fail below 2 KiB).
   - otherwise: `policy.result_behavior == ArtifactReference` ⇒ `put_artifact(AgentArtifactWriteRequest { artifact_id: Some(format!("mcp-{}-g{}-{}", intent.effect_id, intent.generation.get(), call.call_id)), kind: ArtifactKind::File, bytes: serde_json::to_vec(&json!({ "content": <rmcp content serialized>, "structuredContent": result.structured_content })), content_type: Some("application/json"), checksum: None, retention_class: None })` under `self.artifacts.lock().await` ⇒ `AgentTaskContent::artifact(reference)`; else `collaborator("mcp-result-too-large", format!("{tool}: the result exceeds {MCP_INLINE_RESULT_MAX_BYTES} bytes and the binding keeps results inline"))`.
9. `session.close().await` on every path that opened one (use a small guard or explicit `close` calls; never leak a running service).

- [ ] **Step 1: Write the failing tests**

Create `crates/rakka-agent-mcp/tests/client_dispatch.rs` (direct executor proofs; the real-dispatcher proofs are Task 6):

```rust
//! The MCP executor per attempt: the egress check before any client, the
//! credential on the wire and nowhere else, `_meta` with the idempotency
//! key and trace context, the schema recheck, and every result mapping —
//! against the in-process fake, over a counting client.

use std::sync::Arc;

use rakka_agent::testkit::tool_call_intent; // see note below
use rakka_agent::{
    AgentAuthorityRefusal, AgentDispatchToolExecutor, AgentEffectSafetyClass, AgentTaskContent,
    AgentToolCallId, AgentToolCallRequest, AgentToolDeclaration, AgentToolId,
    AgentToolResultBehavior,
};
use rakka_agent_mcp::testkit::{serve_fake, CountingClient, FakeMcpServer, FakeTool, FakeToolBehaviour, ReqwestClient};
use rakka_agent_mcp::{
    mcp_artifact_store, sync_mcp_descriptors, McpAllowAllEgress, McpDispatchToolExecutor,
    McpEgressCheck, McpServerBinding, McpServerId, McpToolPolicy, MCP_META_IDEMPOTENCY_KEY,
};
use rakka_agent_workflow::testkit::FakeArtifactStore;
use rakka_agent_workflow::{AgentEphemeralCredential, AgentTimestampMillis};
use serde_json::json;

mod support;
use support::{run_scope, tool_intent_with_timeout};

fn server_id() -> McpServerId { McpServerId::new("crm").expect("id") }

fn policy(class: AgentEffectSafetyClass, behavior: AgentToolResultBehavior) -> McpToolPolicy {
    McpToolPolicy::new(AgentToolDeclaration::new(class)).with_result_behavior(behavior)
}

fn fake() -> FakeMcpServer {
    FakeMcpServer::new()
        .with_tool(FakeTool::new("echo", "Echoes.", json!({"type":"object"}), FakeToolBehaviour::Echo))
        .with_tool(FakeTool::new("big", "Big.", json!({"type":"object"}), FakeToolBehaviour::Text("x".repeat(5000))))
        .with_tool(FakeTool::new("img", "Image.", json!({"type":"object"}), FakeToolBehaviour::Image { bytes: 16 }))
        .with_tool(FakeTool::new("fail", "Fails.", json!({"type":"object"}), FakeToolBehaviour::Error("boom\nline2 with-arg-echo".into())))
        .with_tool(FakeTool::new("ask", "Asks.", json!({"type":"object"}), FakeToolBehaviour::InputRequired))
}

fn binding(url: &str) -> McpServerBinding {
    McpServerBinding::streamable_http(server_id(), url)
        .with_tool("echo", policy(AgentEffectSafetyClass::ReadOnly, AgentToolResultBehavior::InlineBounded)).expect("t")
        .with_tool("big", policy(AgentEffectSafetyClass::ReadOnly, AgentToolResultBehavior::ArtifactReference)).expect("t")
        .with_tool("img", policy(AgentEffectSafetyClass::ReadOnly, AgentToolResultBehavior::InlineBounded)).expect("t")
        .with_tool("fail", policy(AgentEffectSafetyClass::ReadOnly, AgentToolResultBehavior::InlineBounded)).expect("t")
        .with_tool("ask", policy(AgentEffectSafetyClass::ReadOnly, AgentToolResultBehavior::InlineBounded)).expect("t")
        .with_credential_binding(rakka_agent::AgentCredentialBindingRef::new("crm-key").expect("binding"))
}

fn call(tool: &str, arguments: serde_json::Value) -> AgentToolCallRequest {
    AgentToolCallRequest::new(AgentToolCallId::new("call-1").expect("id"), AgentToolId::new(format!("mcp.crm.{tool}")).expect("id"), arguments).expect("call")
}

async fn executor(url: &str, egress: Arc<dyn McpEgressCheck>) -> (McpDispatchToolExecutor<CountingClient>, CountingClient, FakeArtifactStore) {
    let http = CountingClient::new(ReqwestClient::new());
    let binding = binding(url);
    let set = sync_mcp_descriptors(&http, &binding, None, AgentTimestampMillis::new(1)).await.expect("syncs");
    let store = FakeArtifactStore::default();
    let executor = McpDispatchToolExecutor::new(vec![set], vec![binding], mcp_artifact_store(store.clone()), http.clone(), egress).expect("builds");
    (executor, http, store)
}

#[tokio::test]
async fn a_call_carries_the_credential_the_meta_and_maps_structured_content_inline() {
    let endpoint = serve_fake(fake()).await;
    let (executor, http, _) = executor(&endpoint.url, Arc::new(McpAllowAllEgress)).await;
    let sends_after_sync = http.sends();
    let credential = AgentEphemeralCredential::bearer_token("attempt-token-sentinel");
    let intent = tool_intent_with_timeout("mcp.crm.echo", Some(5_000));
    let content = executor.execute(&run_scope(), &intent, &call("echo", json!({"q": "refunds"})), Some(&credential)).await.expect("answers");
    assert!(matches!(content, AgentTaskContent::Inline(ref v) if v["q"] == "refunds"), "{content:?}");
    let seen = endpoint.server.seen_calls();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].name, "echo");
    assert!(seen[0].authorization.as_deref().is_some_and(|v| v.ends_with("attempt-token-sentinel")));
    assert_eq!(seen[0].meta[MCP_META_IDEMPOTENCY_KEY], json!(intent.idempotency_key.as_str()));
    assert!(seen[0].meta.get("traceparent").is_some() == intent.telemetry.trace_parent.is_some());
    assert!(http.sends() > sends_after_sync, "every send went through the injected client");
    assert_eq!(endpoint.server.list_calls(), 2, "one at sync, one recheck on the first attempt");
}

#[tokio::test]
async fn the_recheck_is_cached_under_the_ttl_and_refuses_a_changed_schema() {
    let endpoint = serve_fake(fake()).await;
    let (executor, _, _) = executor(&endpoint.url, Arc::new(McpAllowAllEgress)).await;
    let intent = tool_intent_with_timeout("mcp.crm.echo", Some(5_000));
    executor.execute(&run_scope(), &intent, &call("echo", json!({})), None).await.expect("first");
    executor.execute(&run_scope(), &intent, &call("echo", json!({})), None).await.expect("second");
    assert_eq!(endpoint.server.list_calls(), 2, "the second attempt reused the cached listing");
    let every_attempt = {
        let http = ReqwestClient::new();
        let binding = binding(&endpoint.url);
        let set = sync_mcp_descriptors(&http, &binding, None, AgentTimestampMillis::new(1)).await.expect("syncs");
        McpDispatchToolExecutor::new(vec![set], vec![binding], mcp_artifact_store(FakeArtifactStore::default()), http, Arc::new(McpAllowAllEgress)).expect("builds").with_descriptor_recheck_ttl_ms(0)
    };
    endpoint.server.swap_tool_schema("echo", json!({"type":"object","properties":{"changed":{"type":"string"}}}));
    let error = every_attempt.execute(&run_scope(), &intent, &call("echo", json!({})), None).await.expect_err("mismatch");
    assert_eq!(error.to_string().contains("tool-descriptor-revision-mismatch"), true, "{error}");
    assert_eq!(endpoint.server.call_count(), 2, "the mismatched attempt never called the tool");
}

#[tokio::test]
async fn large_results_go_to_the_artifact_store_or_refuse_by_the_bindings_behavior() {
    let endpoint = serve_fake(fake()).await;
    let (executor, _, store) = executor(&endpoint.url, Arc::new(McpAllowAllEgress)).await;
    let intent = tool_intent_with_timeout("mcp.crm.big", Some(5_000));
    let content = executor.execute(&run_scope(), &intent, &call("big", json!({})), None).await.expect("artifact");
    let AgentTaskContent::Artifact(reference) = content else { panic!("expected an artifact, got {content:?}") };
    assert_eq!(store.len(), 1);
    assert!(store.bytes(&reference.artifact_id).is_some_and(|b| b.len() > 5000));
    let error = executor.execute(&run_scope(), &tool_intent_with_timeout("mcp.crm.img", Some(5_000)), &call("img", json!({})), None).await.expect_err("inline binding, binary part");
    assert!(error.to_string().contains("mcp-result-too-large"), "{error}");
}

#[tokio::test]
async fn is_error_and_input_required_are_stable_refusals_with_bounded_body_free_detail() {
    let endpoint = serve_fake(fake()).await;
    let (executor, _, _) = executor(&endpoint.url, Arc::new(McpAllowAllEgress)).await;
    let error = executor.execute(&run_scope(), &tool_intent_with_timeout("mcp.crm.fail", Some(5_000)), &call("fail", json!({"secret_arg": "ARG-SENTINEL"})), None).await.expect_err("isError");
    let text = error.to_string();
    assert!(text.contains("mcp-tool-error") && text.contains("boom line2"), "{text}");
    assert!(!text.contains("ARG-SENTINEL") && !text.contains('\n'), "{text}");
    let error = executor.execute(&run_scope(), &tool_intent_with_timeout("mcp.crm.ask", Some(5_000)), &call("ask", json!({})), None).await.expect_err("MRTR");
    assert!(error.to_string().contains("mcp-input-required"), "{error}");
}

struct RefuseAll;
impl McpEgressCheck for RefuseAll {
    fn check(&self, server: &McpServerId, _url: &str) -> Result<(), AgentAuthorityRefusal> {
        Err(AgentAuthorityRefusal::of("egress-denied-by-policy", format!("{server} is not an allowed egress")))
    }
}

#[tokio::test]
async fn an_egress_refusal_fails_before_any_client_exists_and_never_touches_the_credential() {
    let endpoint = serve_fake(fake()).await;
    let (executor, http, _) = executor(&endpoint.url, Arc::new(RefuseAll)).await;
    let sends_after_sync = http.sends();
    let credential = AgentEphemeralCredential::bearer_token("never-sent");
    let error = executor.execute(&run_scope(), &tool_intent_with_timeout("mcp.crm.echo", Some(5_000)), &call("echo", json!({})), Some(&credential)).await.expect_err("refused");
    assert!(error.to_string().contains("egress-denied-by-policy"), "the deployment's own code: {error}");
    assert_eq!(http.sends(), sends_after_sync, "no client was built");
    assert!(endpoint.server.seen_calls().is_empty());
}

#[tokio::test]
async fn construction_is_offline_and_a_tool_without_a_descriptor_is_refused() {
    let endpoint = serve_fake(fake()).await;
    let http = ReqwestClient::new();
    let binding = binding(&endpoint.url);
    let set = sync_mcp_descriptors(&http, &binding, None, AgentTimestampMillis::new(1)).await.expect("syncs");
    let lists = endpoint.server.list_calls();
    let executor = McpDispatchToolExecutor::new(vec![set.clone()], vec![binding.clone()], mcp_artifact_store(FakeArtifactStore::default()), http.clone(), Arc::new(McpAllowAllEgress)).expect("builds");
    assert_eq!(endpoint.server.list_calls(), lists, "construction makes no network call");
    let error = executor.execute(&run_scope(), &tool_intent_with_timeout("mcp.crm.nope", Some(1_000)), &call("nope", json!({})), None).await.expect_err("unbound");
    assert!(error.to_string().contains("mcp-tool-unbound"), "{error}");
    let widened = binding.with_tool("extra", policy(AgentEffectSafetyClass::ReadOnly, AgentToolResultBehavior::InlineBounded)).expect("t");
    let error = McpDispatchToolExecutor::new(vec![set], vec![widened], mcp_artifact_store(FakeArtifactStore::default()), http, Arc::new(McpAllowAllEgress)).expect_err("a listed tool with no synced descriptor");
    assert_eq!(error.code(), "mcp-binding-invalid");
}

#[tokio::test]
async fn a_timeout_from_the_intent_bounds_the_call() {
    let endpoint = serve_fake(FakeMcpServer::new().with_tool(FakeTool::new("slow", "Slow.", json!({"type":"object"}), FakeToolBehaviour::Sleep { millis: 2_000 }))).await;
    let http = ReqwestClient::new();
    let binding = McpServerBinding::streamable_http(server_id(), &endpoint.url).with_tool("slow", policy(AgentEffectSafetyClass::ReadOnly, AgentToolResultBehavior::InlineBounded)).expect("t");
    let set = sync_mcp_descriptors(&http, &binding, None, AgentTimestampMillis::new(1)).await.expect("syncs");
    let executor = McpDispatchToolExecutor::new(vec![set], vec![binding], mcp_artifact_store(FakeArtifactStore::default()), http, Arc::new(McpAllowAllEgress)).expect("builds");
    let started = std::time::Instant::now();
    let error = executor.execute(&run_scope(), &tool_intent_with_timeout("mcp.crm.slow", Some(200)), &call("slow", json!({})), None).await.expect_err("timed out");
    assert!(started.elapsed() < std::time::Duration::from_secs(2), "the timeout bounded the call");
    assert!(error.to_string().contains("mcp-transport-failed"), "{error}");
}
```

Add `FakeToolBehaviour::Sleep { millis }` to the testkit (answers text `"late"` after sleeping). `tests/support/mod.rs` provides `run_scope()` and `tool_intent_with_timeout(tool: &str, timeout_ms: Option<u64>) -> AgentRunEffect` built the way `rakka_agent::testkit` or the crate's public constructors allow (`AgentRunEffect::new`/`for_tool_call` — read `effect.rs:1866` and the `AgentRunEffectRequest::ToolCall` shape; if the public constructor needs an `AgentEffectSpec`, use `AgentEffectSpec::read_only().with_timeout_ms(..)` and `AgentEffectPolicies`). Remove the unused `tool_call_intent` import once the support module exists. `CountingClient::new(inner)` and `sends()` are the testkit's.

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p rakka-agent-mcp --features testkit --test client_dispatch`
Expected: compile errors on the missing executor items.

- [ ] **Step 3: The executor**

Write `src/executor.rs` per the interface and the nine-step attempt above. Keep the per-server recheck cache as `Mutex<BTreeMap<McpServerId, RecheckEntry { at: Instant, digests: BTreeMap<String, AgentContentDigest> }>>` (a `std::sync::Mutex` held only across the map update, never across an await). The launcher path in `connect` for `ChildProcess` is `Err(collaborator("mcp-transport-unsupported", ..))` in this task if no launcher is installed (it cannot be, since `new` refuses), and a `todo!()`-free stub that calls `self.launcher.launch(scope, spec_ref)` and `client::connect_over(transport, binding, credential)` — Task 5 implements `connect_over`; in this task declare it in `client.rs` returning `Err(McpClientError::Protocol { reason: "child transport lands in Task 5" })` so the executor compiles, and note it in the report.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p rakka-agent-mcp --features testkit --test client_dispatch` then `cargo test -p rakka-agent-mcp --all-features`, `cargo test -p rakka-agent-mcp`, `cargo clippy -p rakka-agent-mcp --all-targets --all-features -- -D warnings`, `cargo fmt --all -- --check`, `RUSTDOCFLAGS="-D warnings" cargo doc -p rakka-agent-mcp --no-deps --all-features`.
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add crates/rakka-agent-mcp
git commit -m "Dispatch MCP tool calls through a per-attempt client behind a required egress check, rechecking the published schema and mapping results to bounded content

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 5: The child-process seam: `McpChildTransport`, the launcher trait, the gated reference launcher

**Files:**
- Create: `crates/rakka-agent-mcp/src/launcher.rs` (replacing the empty module)
- Modify: `crates/rakka-agent-mcp/src/client.rs` (`connect_over`), `src/executor.rs` (the `ChildProcess` arm), `src/lib.rs` (re-exports), `Cargo.toml` (the `child-process` feature already exists; add a `[[test]]` entry `child_process_launcher` with `required-features = ["child-process", "testkit"]`)
- Create: `crates/rakka-agent-mcp/tests/child_process.rs` (ungated: refusal without a launcher; an in-memory launcher over a duplex pair) and `crates/rakka-agent-mcp/tests/child_process_launcher.rs` (gated: the reference launcher spawns a process)

**Interfaces:**
- Produces:
  - `pub struct McpChildTransport { pub reader: Box<dyn tokio::io::AsyncRead + Send + Unpin>, pub writer: Box<dyn tokio::io::AsyncWrite + Send + Unpin> }` with `new(reader, writer)`.
  - `pub type McpLaunchFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, McpLaunchError>> + Send + 'a>>;`
  - `pub enum McpLaunchError { SpecUnreadable { reason: String }, SpawnFailed { reason: String }, Unsupported }` with `code()` (`mcp-transport-unsupported` for every variant — a launch failure is a transport failure of the unsupported class; the reason names no path or argument).
  - `pub trait McpChildProcessLauncher: Send + Sync + 'static { fn launch<'a>(&'a self, scope: &'a AgentRunScope, spec: &'a ArtifactRef) -> McpLaunchFuture<'a, McpChildTransport>; }`
  - `client::connect_over<T: IntoTransport<RoleClient, E, A>, ..>(transport: T, binding, credential) -> Result<McpClientSession<()>, McpClientError>` — the session type gains a generic over the transport kind or erases it; simplest: `McpClientSession` holds `RunningService<RoleClient, ClientInfo>`, which is transport-agnostic, so `connect` and `connect_over` share a `finish(running, binding)` step; a `ChildProcess` binding's credential is refused `CredentialMaterialUnsupported { material: "child-process" }` when present (a stdio child has no header to carry it; the spec routes secrets to HTTP bindings only).
  - Under `#[cfg(feature = "child-process")]`: `pub struct TokioChildProcessLauncher { artifacts: McpArtifactStore }` with `new(artifacts)`; `launch` reads the spec artifact (`get_artifact`), decodes `{ "command": String, "args": [String], "env": { String: String } }`, spawns `rmcp::transport::TokioChildProcess::new(tokio::process::Command::new(command).args(args).envs(env))` and returns its stdin/stdout as the pair (`TokioChildProcess` exposes them through `split()` or implements `Transport` directly — read `transport/child_process.rs:36–170`; if it only implements `Transport`, make `McpChildTransport` an enum `{ Pair { reader, writer }, #[cfg(feature = "child-process")] Process(TokioChildProcess) }` and have `connect_over` accept either). Documented as unsandboxed and for tests/examples only.

- [ ] **Step 1: Write the failing tests**

`tests/child_process.rs`:

```rust
//! Condition (b): a child-process binding is refused at construction without
//! a launcher, and runs through a launcher-produced transport with one — here
//! an in-memory duplex pair to an rmcp server running in the same process, so
//! no process is spawned and no sandbox is assumed.

use std::sync::Arc;

use rakka_agent::{AgentDispatchToolExecutor, AgentEffectSafetyClass, AgentRunScope, AgentToolCallId, AgentToolCallRequest, AgentToolDeclaration, AgentToolId};
use rakka_agent_mcp::testkit::{FakeMcpServer, FakeTool, FakeToolBehaviour, ReqwestClient};
use rakka_agent_mcp::{mcp_artifact_store, McpAllowAllEgress, McpChildProcessLauncher, McpChildTransport, McpDescriptorSet, McpDispatchToolExecutor, McpLaunchFuture, McpServerBinding, McpServerId, McpToolPolicy};
use rakka_agent_workflow::testkit::FakeArtifactStore;
use rakka_agent_workflow::ArtifactRef;
use serde_json::json;

mod support;
use support::{run_scope, tool_intent_with_timeout, spec_artifact_ref, stored_set_for};

struct DuplexLauncher { server: FakeMcpServer }

impl McpChildProcessLauncher for DuplexLauncher {
    fn launch<'a>(&'a self, _scope: &'a AgentRunScope, _spec: &'a ArtifactRef) -> McpLaunchFuture<'a, McpChildTransport> {
        Box::pin(async move {
            let (client_side, server_side) = tokio::io::duplex(64 * 1024);
            let (server_read, server_write) = tokio::io::split(server_side);
            let handler = self.server.clone();
            tokio::spawn(async move {
                let running = rmcp::serve_server(handler, (server_read, server_write)).await.expect("server serves");
                let _ = running.waiting().await;
            });
            let (client_read, client_write) = tokio::io::split(client_side);
            Ok(McpChildTransport::new(Box::new(client_read), Box::new(client_write)))
        })
    }
}

fn binding() -> McpServerBinding {
    McpServerBinding::child_process(McpServerId::new("local").expect("id"), spec_artifact_ref())
        .with_tool("echo", McpToolPolicy::new(AgentToolDeclaration::new(AgentEffectSafetyClass::ReadOnly))).expect("t")
}

#[tokio::test]
async fn a_child_process_binding_is_refused_without_a_launcher() {
    let server = FakeMcpServer::new().with_tool(FakeTool::new("echo", "Echoes.", json!({"type":"object"}), FakeToolBehaviour::Echo));
    let set: McpDescriptorSet = stored_set_for(&binding(), &server);
    let error = McpDispatchToolExecutor::new(vec![set], vec![binding()], mcp_artifact_store(FakeArtifactStore::default()), ReqwestClient::new(), Arc::new(McpAllowAllEgress)).expect_err("no launcher");
    assert_eq!(error.code(), "mcp-transport-unsupported");
}

#[tokio::test]
async fn a_child_process_binding_runs_through_the_launchers_transport() {
    let server = FakeMcpServer::new().with_tool(FakeTool::new("echo", "Echoes.", json!({"type":"object"}), FakeToolBehaviour::Echo));
    let set = stored_set_for(&binding(), &server);
    let executor = McpDispatchToolExecutor::with_launcher(
        vec![set],
        vec![binding()],
        mcp_artifact_store(FakeArtifactStore::default()),
        ReqwestClient::new(),
        Arc::new(McpAllowAllEgress),
        Arc::new(DuplexLauncher { server: server.clone() }),
    )
    .expect("builds with a launcher");
    let request = AgentToolCallRequest::new(
        AgentToolCallId::new("c").expect("id"),
        AgentToolId::new("mcp.local.echo").expect("id"),
        json!({"k": 1}),
    )
    .expect("call");
    let content = executor
        .execute(&run_scope(), &tool_intent_with_timeout("mcp.local.echo", Some(5_000)), &request, None)
        .await
        .expect("answers over stdio");
    assert!(matches!(content, rakka_agent::AgentTaskContent::Inline(ref v) if v["k"] == 1), "{content:?}");
    assert_eq!(server.call_count(), 1);
}
```

Constructor shape (binding on both tasks): `McpDispatchToolExecutor::new` refuses any `ChildProcess` binding with `mcp-transport-unsupported` (the spec's wording); `McpDispatchToolExecutor::with_launcher(descriptors, bindings, artifacts, http, egress, launcher) -> Result<Self, McpRegistrationError>` is the constructor that admits them; `with_child_process_launcher(self, launcher) -> Result<Self, _>` installs the capability on an executor built without child bindings (its re-validation then trivially passes) and stays for API symmetry with the spec.

`support::stored_set_for(binding, server)` produces a `McpDescriptorSet` without a network call by calling `sync_mcp_descriptors` over a duplex transport — expose `rakka_agent_mcp::sync::sync_mcp_descriptors_over(transport, binding, synced_at)` for that (the launcher path's sync, spec 5.2 "for a child-process binding the same function runs over the transport the launcher produced"); `support::spec_artifact_ref()` builds an `ArtifactRef` with `artifact_id: "spec-1"`, `kind: ArtifactKind::File`, `uri: "mem://spec-1"`.

`tests/child_process_launcher.rs` (gated):

```rust
//! The unsandboxed reference launcher, feature `child-process`: it reads the
//! spec artifact and spawns the command. Proven with `/bin/cat`, which echoes
//! the initialize request back, so the client refuses the protocol — what
//! matters is that a process was launched from the artifact and the failure
//! is bounded and body-free.
#![cfg(all(feature = "child-process", feature = "testkit"))]
// build a FakeArtifactStore holding {"command":"/bin/cat","args":[]} at "spec-1";
// TokioChildProcessLauncher::new(mcp_artifact_store(store)).launch(&run_scope(), &spec_artifact_ref()).await → Ok(transport);
// connect_over(transport, &binding, None) within tokio::time::timeout(2s) → Err whose code is mcp-descriptor-sync-failed or mcp-protocol-unsupported, message < 512 bytes, containing neither "/bin/cat" nor a JSON body.
```

Write the test body out in full in the file (no comments-as-code): the store insert, the launch, the bounded connect, the assertions.

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p rakka-agent-mcp --features testkit --test child_process` and `cargo test -p rakka-agent-mcp --features child-process,testkit --test child_process_launcher`
Expected: compile errors on the missing launcher items.

- [ ] **Step 3: Implement**

`launcher.rs` per the interface; `client::connect_over` building the transport from the pair with `rmcp`'s `(R, W)` `IntoTransport` (`transport-async-rw`); `sync::sync_mcp_descriptors_over`; the executor's `ChildProcess` arm (`self.launcher.as_ref().ok_or(collaborator("mcp-transport-unsupported", ..))?.launch(scope, spec_ref).await.map_err(|e| collaborator(e.code(), e.to_string()))?` → `connect_over`); `McpDispatchToolExecutor::with_launcher`. The `Cargo.toml` `[[test]]` entry with `required-features`.

- [ ] **Step 4: Run the tests**

Run: the two test commands from Step 2, then `cargo test -p rakka-agent-mcp --all-features`, `cargo test -p rakka-agent-mcp` (no features), `cargo clippy -p rakka-agent-mcp --all-targets --all-features -- -D warnings`, `cargo clippy -p rakka-agent-mcp --all-targets -- -D warnings` (no features), `cargo fmt --all -- --check`, `RUSTDOCFLAGS="-D warnings" cargo doc -p rakka-agent-mcp --no-deps --all-features`.
Expected: all pass; without `child-process`, `TokioChildProcessLauncher` does not exist.

- [ ] **Step 5: Commit**

```bash
git add crates/rakka-agent-mcp
git commit -m "Refuse a child-process MCP binding without a launcher, and run one through the launcher's stdio pair when a deployment supplies it

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 6: The real-dispatcher proofs in `rakka-agent`

**Files:**
- Modify: `crates/rakka-agent/Cargo.toml` (`[dev-dependencies] rakka-agent-mcp = { path = "../rakka-agent-mcp", features = ["testkit"] }` with a comment naming the dev-dependency cycle and why it is allowed)
- Create: `crates/rakka-agent/tests/mcp_client_dispatch.rs`
- Modify: `crates/rakka-agent/tests/secret_exclusion.rs` (one scenario reusing `credentialed_fixture`'s shape with the MCP executor; or keep the scan in the new file — one concern per file says the new file owns MCP, so put the scan there and reference `secret_exclusion.rs`'s helper by copying its `SECRETS`/scan loop into `tests/common/mod.rs` if it is not already shared)

**Interfaces:**
- Consumes: Task 1's `AuthorityFixture::with_tool_executor` and `AgentToolExecutorRouter`; Tasks 3–4's testkit and executor; the fixture's `with_credential_resolver(token)`, `apply_settings`, `start`, `pump`, `durable_surfaces()`, `terminal_failure_code()`, `credentials` (the `ScriptedCredentialResolver` with `resolutions()`), and the registry the fixture builds (find how it registers tools: the fixture's registry must include the synced MCP bindings, so the fixture needs a way to add bindings — `AuthorityFixture::with_registered_bindings(Vec<AgentToolBinding>)` or build the fixture from a custom `AgentToolRegistry`; read `tests/common/mod.rs:2633–2700` and add the smallest hook, in Task 6, documented in the report).

- [ ] **Step 1: Write the tests**

`crates/rakka-agent/tests/mcp_client_dispatch.rs` — five scenarios through the real pipeline:

1. `an_mcp_tool_call_dispatches_through_the_router_with_the_resolved_credential`: fixture with a definition whose envelope declares the MCP tool binding (from a stored set synced against `serve_fake`), the MCP executor behind `AgentToolExecutorRouter::new(Arc::new(fx.tools.clone())).with_prefix_route("mcp.", mcp_executor)`, `with_credential_resolver("run-token-sentinel")`, the effect spec for the tool carrying the binding's credential binding and a timeout; the scripted model requests the tool call; after `pump`, the fake saw one call with `Authorization` ending in the sentinel, `_meta` carrying the effect's idempotency key, `fx.credentials.resolutions() == 1`, the run recorded the tool result (assert the recorded content), and `fx.tools` (the fallback) saw nothing.
2. `the_credential_never_reaches_a_durable_record_or_the_fleet_index`: same walk; scan every `durable_surfaces()` and the segments/metrics the fixture exposes for `run-token-sentinel`; assert absent everywhere; positive control: the fake saw it.
3. `an_egress_refusal_fails_the_attempt_under_the_deployments_code_after_the_credential_was_resolved_and_dropped`: `RefuseAll` egress; after the pump the effect's last error carries `egress-denied-by-policy`, `resolutions() == 1` (the dispatcher resolved it), the fake saw zero calls, and the run ends per the safety class (a `NonIdempotent` declaration → one attempt, `Indeterminate`? no: the executor returned before invoking — read how the dispatcher classifies an `Err` from `execute` under `NonIdempotent` (module doc dispatch.rs:43–86) and assert that classification, naming it in the test).
4. `a_large_result_reaches_the_artifact_store_and_the_run_records_the_reference`: `big` tool with `ArtifactReference`; the run's recorded content is an artifact whose id the `FakeArtifactStore` holds.
5. `the_secret_exclusion_scan_covers_the_mcp_types`: serialize `McpServerBinding`, `McpDescriptorSet`, and the executor's `Debug` output for the sentinel and for `token`/`secret`-shaped keys; assert absent (the crate carries only references).

Each scenario names the exact fixture calls; where the fixture lacks a hook (registering external bindings, exposing the outbox row's last error), add the smallest public helper to `tests/common/mod.rs` and document it in the report.

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p rakka-agent --test mcp_client_dispatch`
Expected: compile errors until the dev-dependency and the hooks exist, then assertion failures until wiring is right.

- [ ] **Step 3: Implement the hooks and make the tests pass**

Only test-support code changes in `rakka-agent` (`tests/common/mod.rs`); if a production seam is missing (for example the effect spec for a registered tool not carrying its binding's `credential_binding`), stop and report DONE_WITH_CONCERNS naming the seam rather than patching production code in this task.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p rakka-agent --test mcp_client_dispatch --test tool_executor_router --test secret_exclusion --test tool_authority`, `cargo clippy -p rakka-agent --all-targets --all-features -- -D warnings`, `cargo fmt --all -- --check`; then, once, `cargo test -p rakka-agent --all-features > /tmp/t6-tests.log 2>&1; echo "exit=$?"` (the dev-dependency changes the crate's test build; the whole crate must stay green).
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add crates/rakka-agent/Cargo.toml Cargo.lock crates/rakka-agent/tests/mcp_client_dispatch.rs crates/rakka-agent/tests/common/mod.rs
git commit -m "Prove an MCP tool call through the real dispatcher: the resolved credential on the wire and nowhere durable, the egress refusal, the artifact overflow

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 7: Documentation, the compatibility record, matrices, and the spec marks

**Files:**
- Modify: `docs/rakka-agents.md` (crate roster row for `rakka-agent-mcp` after the knowledge-graph rows: "The MCP client adapter: server bindings with operator-declared tool policies, publish-time descriptor sync, the dispatch executor over an injected transport client with a required egress check, and the child-process launcher seam." with facade feature `agent-mcp`; and a paragraph in the tools/effects section: remote MCP tools are registered from stored descriptor sets, called per attempt through the executor, never an agent-to-agent channel)
- Modify: `docs/rakka-compatibility.md` (a Phase 7 slice 7.7 bullet after the slice 7.1 bullet: the fourteen codes with one clause each — `mcp-tool-error`, `mcp-input-required`, `mcp-protocol-unsupported`, `mcp-result-too-large`, `mcp-hint-contradicts-declaration`, `mcp-peer-agent-channel-refused`, `mcp-transport-unsupported`, `mcp-descriptor-schema-too-large`, `mcp-descriptor-sync-failed`, `mcp-binding-invalid`, `mcp-tool-unbound`, `mcp-credential-material-unsupported`, `tool-descriptor-revision-mismatch`, `mcp-transport-failed`; `AgentToolBinding` now serde with validating decode; `AgentToolExecutorRouter` additive; the `agent-mcp` facade feature; the dev-dependency cycle note; `McpDescriptorRefresh::Listen` deferred; pin table rows `rmcp` `=3.4.0` "Declared in `crates/rakka-agent-mcp/Cargo.toml` with `default-features = false, features = ["client", "transport-streamable-http-client-reqwest", "reqwest", "transport-io"]`; `transport-child-process` only under the crate's `child-process` feature; the server side only under `testkit`" with bump policy "Exact pin. A bump is a review of the client lifecycle, the tool model, and the MRTR/task result shapes; the fake server in `testkit` is re-run against it" and `MCP protocol` `2026-07-28 (2025-11-25 compatible)` "Declared in `rakka_agent_mcp::MCP_DEFAULT_PROTOCOL_VERSIONS`; negotiated per attempt by rmcp's Discover lifecycle" with policy "A revision move is a `McpServerBinding.protocol_versions` default change and a compatibility review of spec 14.4")
- Modify: `CHANGELOG.md` (Unreleased → Added: the crate, the router, the binding serde; Unreleased → Changed: none breaking — say so if true)
- Modify: `docs/rakka-api-boundary-inventory.md` (bullet: `rakka-agent-mcp` owns the MCP client adapter; table row `| `rakka-agent-mcp` | Adapter | … |`) — if Task 2 already added the row for `repository_hygiene`, verify wording only
- Modify: `docs/rakka-agent-security-validation-matrix.md` (rows: "MCP egress is a required policy hook: no client exists before `McpEgressCheck` passes; `McpAllowAllEgress` is the only opt-out" → `client_dispatch.rs::an_egress_refusal_fails_before_any_client_exists_and_never_touches_the_credential`, `mcp_client_dispatch.rs` scenario 3, Met; "No child process without a deployment launcher" → `child_process.rs::a_child_process_binding_is_refused_without_a_launcher`, Met; "MCP credentials only through the resolver, only for the attempt, never durable" → scenarios 1–2, Met; "MCP is never an agent-to-agent channel" → `descriptor_sync.rs::a_server_that_identifies_as_a_rakka_agent_is_refused`, Met; "Recovery never executes against a materially different schema (11.8)" → `client_dispatch.rs::the_recheck_is_cached_under_the_ttl_and_refuses_a_changed_schema`, Met for MCP tools)
- Modify: `CLAUDE.md` (the crate-layering diagram: add `rakka-agent-mcp` under `rakka-agent` with "the MCP client adapter (optional; `agent-mcp`)")
- Modify: `docs/superpowers/specs/2026-09-19-phase7-agent-surface-parity-design.md` (section 0: a "Plan refinements (2026-09-22, slice 7.7 plan)" paragraph naming the twelve refinements and the plan path; inline marks "plan refinement 2026-09-22" at 5.1 (no clock; `transport-io`), 5.2 (`synced_at` parameter; refresh `Manual | Interval` with `Listen` deferred and the seam named; identity from initialize; `with_input_schema_artifact`; the hint rule; `tool-descriptor-revision-mismatch` registered; recheck TTL), 5.3 (no clock; `McpArtifactStore`; `with_launcher` constructor beside `with_child_process_launcher`; `McpChildTransport` pair; `mcp-transport-failed`; determinacy by safety class), 5.4 (the dispatcher proofs live in `rakka-agent` via the dev-dependency), 11.1 (the five extra codes))
- Verify with the doc-holding tests: `cargo test -p rakka-agent --features otel --test compatibility_currency --test crate_shape --test product_doc_currency`, `cargo test -p rakka-testkit --test repository_hygiene`, and any test that parses the security matrix or the inventory (`grep -rln "security-validation-matrix\|api-boundary-inventory" crates/*/tests`).

- [ ] **Step 1: Baseline** — run the doc-holding tests before editing.
- [ ] **Step 2: Edit** every file above; every test name cited must exist (`grep -rn "fn <name>"`); every code string must exist in the code (`grep -rn '"mcp-'`, `'"tool-descriptor-revision-mismatch"'`).
- [ ] **Step 3: Run the doc-holding tests again.**
- [ ] **Step 4: Commit**

```bash
git add docs/rakka-agents.md docs/rakka-compatibility.md docs/rakka-api-boundary-inventory.md docs/rakka-agent-security-validation-matrix.md CHANGELOG.md CLAUDE.md docs/superpowers/specs/2026-09-19-phase7-agent-surface-parity-design.md
git commit -m "Record the MCP client adapter: its codes, pins, conditions, and the slice 7.7 refinements in the product docs

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 8: Validation and the plan file

- [ ] **Step 1:** `cargo fmt --all -- --check`; `cargo clippy --workspace --all-targets --all-features -- -D warnings`.
- [ ] **Step 2 (per crate):** `cargo test -p rakka-agent-mcp`, `cargo test -p rakka-agent-mcp --all-features`, `cargo test -p rakka-agent --all-features`, `cargo test -p rakka-agent --no-default-features`, `cargo test -p rakka --features agent-mcp`, `cargo test -p rakka-a2a --all-features`, `cargo test -p rakka-testkit`, `cargo test -p rakka-example-durable-agent-acceptance`.
- [ ] **Step 3:** `cargo check -p rakka-agent-mcp --no-default-features`, `cargo check -p rakka --no-default-features --features agent`, `cargo check -p rakka --features agent-mcp`, `cargo check -p rakka-stream --no-default-features`, `cargo check -p rakka-process --no-default-features`, `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features`, `cargo tree -p rakka-agent-mcp -e features -i reqwest --all-features | grep -c system-proxy` (expected `0`), `cargo tree -p rakka -e features --features agent-mcp -i rmcp | head` (shows only the four client features plus `testkit`-less), `scripts/package-check.sh > /tmp/pkg.log 2>&1; echo "exit=$?"` (the new crate is `publish = false`, so it is outside the publishable list; the script must still exit 0).
- [ ] **Step 4:** `scripts/validate.sh > /tmp/validate-7-7.log 2>&1; echo "exit=$?"`; if the workspace test phase is killed, say so and rely on Step 2.
- [ ] **Step 5:** Commit the plan file:

```bash
git add docs/superpowers/plans/2026-09-22-phase7-slice-7-7-mcp-client.md
git commit -m "Add the slice 7.7 implementation plan

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

Pushing and opening a pull request wait for the owner's go-ahead.

---

## Self-review record

**Spec coverage (section 5 as refined):**

| Spec item | Task |
| --- | --- |
| 5.1 crate, pin, features (`child-process` off), facade `agent-mcp`, MCP revision recorded | 2, 7 |
| 5.2 `McpServerBinding`, `McpToolPolicy`, refresh modes, `sync_mcp_descriptors`, `McpDescriptorSet`/`McpSyncedDescriptor`, prefixing, schema bounds, hint rule, `Manual` default, staleness, dispatch-time recheck, peer-agent rule | 2, 3, 4 |
| 5.3 executor struct and `new` signature (minus the clock), `McpEgressCheck` required + `McpAllowAllEgress` (R17), launcher seam and `child-process` feature, credential path, per-attempt client with `_meta`, the result table, guardrails unchanged, `AgentToolExecutorRouter` | 1, 4, 5 |
| 5.4 `descriptor_sync.rs` items; `client_dispatch.rs` items (real dispatcher → Task 6 in `rakka-agent`; counting client; egress; child process; credential only through the resolver) | 3, 4, 5, 6 |
| 11.1 codes and pins; 11.2 security rows; 11.3 docs | 7 |
| 12 slice row 7.7 | all |

Gaps: `McpDescriptorRefresh::Listen` (deferred by refinement 2, marked in the spec); no example (7.10 owns the acceptance walk).

**Placeholder scan:** no "TBD"/"TODO"/"similar to Task N". Four "read the source at the named line and use what compiles" instructions remain, each with the file and line: rmcp's `auth_header` semantics, `ProtocolVersion` construction from a string, `TokioChildProcess`'s transport shape, and the fixture's registry hook — all resolved in the implementer's report.

**Type consistency:** `McpServerBinding::with_tool -> Result` is used with `.expect` in every test; `McpToolPolicy::new(declaration)` + builders everywhere; `sync_mcp_descriptors(http, binding, credential, synced_at)` has the same four arguments in Tasks 3, 4, 6; `McpDispatchToolExecutor::new(descriptors, bindings, artifacts, http, egress)` and `with_launcher(.., launcher)` in Tasks 4, 5, 6; `McpChildTransport::new(reader, writer)` in Task 5; `AgentToolExecutorRouter::new(fallback).with_prefix_route("mcp.", ..)` in Tasks 1 and 6; codes match the Global Constraints list plus `mcp-transport-failed` (added in Task 4, registered in Task 7).
