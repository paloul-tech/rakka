//! [`McpDispatchToolExecutor`]: one MCP tool call per dispatch attempt, and
//! the mapping from what a server answers onto bounded task content.
//!
//! Four properties this module holds:
//!
//! - **One attempt is one session.** The client is built inside the attempt
//!   and closed on every path out of it, including every refusal. Nothing
//!   about a server survives an attempt except the schema recheck cache, which
//!   is not authority — it can only refuse.
//! - **The destination is admitted before the credential is read.** The egress
//!   rule fires inside the `client` module's own `connect`, so a refusal
//!   happens before a transport, a header, or a request exists.
//! - **The published descriptor is the contract.** Before a call goes out, the
//!   server's live input schema is compared with the digest the publish-time
//!   sync pinned; a server that reshaped a tool is refused rather than called
//!   with arguments the model chose against the old shape.
//! - **A result is bounded before it becomes state.** Inline content stays
//!   under [`MCP_INLINE_RESULT_MAX_BYTES`]; anything larger, and anything
//!   carrying a binary or reference part, either becomes an artifact — when
//!   the operator's policy says so — or is refused. A tool's own error text is
//!   flattened and truncated, and no argument the model sent is ever echoed
//!   back into a refusal.

use std::collections::BTreeMap;
use std::fmt::{self, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rakka_agent::{
    AgentContentDigest, AgentDispatchError, AgentDispatchFuture, AgentDispatchToolExecutor,
    AgentRunEffect, AgentRunScope, AgentTaskContent, AgentToolCallRequest, AgentToolId,
    AgentToolResultBehavior,
};
use rakka_agent_workflow::{
    AgentArtifactStore, AgentArtifactWriteRequest, AgentEphemeralCredential, ArtifactKind,
};
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, MetaObject,
    RequestMetaObject,
};
use rmcp::transport::streamable_http_client::StreamableHttpClient;
use serde::Serialize;
use serde_json::{json, Map, Value};

use crate::binding::{
    McpRegistrationError, McpServerBinding, McpServerId, McpToolPolicy, McpTransport,
    MCP_DESCRIPTOR_RECHECK_TTL_DEFAULT_MS, MCP_INLINE_RESULT_MAX_BYTES, MCP_META_IDEMPOTENCY_KEY,
    MCP_TOOL_ERROR_DETAIL_MAX_BYTES,
};
use crate::client::{self, McpClientError, McpClientSession, McpEgressCheck};
use crate::launcher::McpChildProcessLauncher;
use crate::sync::{McpDescriptorSet, McpSyncedDescriptor};

/// The artifact store an executor writes an over-large tool result to.
///
/// A [`tokio::sync::Mutex`] rather than a plain reference because
/// [`AgentArtifactStore::put_artifact`] takes `&mut self` while a dispatch
/// executor is shared across concurrent attempts, and an `async` mutex because
/// the write is awaited.
pub type McpArtifactStore = Arc<tokio::sync::Mutex<dyn AgentArtifactStore + Send>>;

/// Wraps one application-owned artifact store as an [`McpArtifactStore`].
#[must_use]
pub fn mcp_artifact_store<S>(store: S) -> McpArtifactStore
where
    S: AgentArtifactStore + Send + 'static,
{
    Arc::new(tokio::sync::Mutex::new(store))
}

/// The attempt's session, behind a boxed future.
///
/// Boxed rather than an `async fn`'s opaque type on purpose. The Streamable
/// HTTP path is generic over `C: StreamableHttpClient`, whose own methods
/// return `-> impl Future + Send` (`rmcp-3.4.0/src/transport/streamable_http_client.rs:411`),
/// and letting that `C`-dependent opaque type leak into the executor's outer
/// future makes rustc ask for a *higher-ranked* `Send` bound — "implementation
/// of `Send` is not general enough" — that no caller can give. Erasing it
/// behind `dyn Future + Send + 'a` pins the obligation to this call's own
/// lifetime, which is the one that does hold.
type McpSessionFuture<'a> =
    Pin<Box<dyn Future<Output = Result<McpClientSession, AgentDispatchError>> + Send + 'a>>;

/// One bound server: the operator's binding and the descriptor set a
/// publish-time sync pinned for it.
struct McpBoundServer {
    binding: McpServerBinding,
    descriptors: McpDescriptorSet,
}

/// Where one Rakka tool id dispatches to: a server, and the position of its
/// synced descriptor in that server's set.
struct McpToolRoute {
    server: McpServerId,
    index: usize,
}

/// One server's live tool shapes, as the last recheck read them.
struct RecheckEntry {
    at: Instant,
    digests: BTreeMap<String, AgentContentDigest>,
}

/// The stored shape of an over-large result, as the artifact holds it.
#[derive(Serialize)]
struct StoredResult<'a> {
    content: &'a [ContentBlock],
    #[serde(rename = "structuredContent")]
    structured_content: &'a Option<Value>,
}

/// Dispatches Rakka tool calls onto the MCP servers an operator bound, one
/// per-attempt client at a time.
///
/// Construction is offline: it pairs bindings with the descriptor sets a
/// publish-time sync produced and makes no network call, so a registry built
/// from this executor's tools is release data.
pub struct McpDispatchToolExecutor<C> {
    servers: BTreeMap<McpServerId, McpBoundServer>,
    tools: BTreeMap<AgentToolId, McpToolRoute>,
    artifacts: McpArtifactStore,
    http: C,
    egress: Arc<dyn McpEgressCheck>,
    launcher: Option<Arc<dyn McpChildProcessLauncher>>,
    recheck_ttl_ms: u64,
    rechecked: Mutex<BTreeMap<McpServerId, RecheckEntry>>,
}

impl<C> fmt::Debug for McpDispatchToolExecutor<C> {
    /// Counts only: a binding carries an operator's endpoint URL and a
    /// credential *reference*, and neither belongs in a log line that a
    /// `{:?}` anywhere could produce.
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("McpDispatchToolExecutor")
            .field("servers", &self.servers.len())
            .field("tools", &self.tools.len())
            .finish()
    }
}

impl<C> McpDispatchToolExecutor<C> {
    /// Builds the executor from the operator's bindings and the descriptor
    /// sets a publish-time sync produced for them.
    ///
    /// Offline by construction: nothing here contacts a server. The pairing is
    /// total in both directions — every binding needs a set and every set
    /// needs a binding — and every allow-listed tool must have a synced
    /// descriptor, because a tool with no pinned schema is one no attempt
    /// could ever recheck.
    ///
    /// `egress` has no default: see [`McpEgressCheck`].
    ///
    /// # Errors
    ///
    /// [`McpRegistrationError`] with its stable code.
    pub fn new(
        descriptors: Vec<McpDescriptorSet>,
        bindings: Vec<McpServerBinding>,
        artifacts: McpArtifactStore,
        http: C,
        egress: Arc<dyn McpEgressCheck>,
    ) -> Result<Self, McpRegistrationError> {
        let mut bound: BTreeMap<McpServerId, McpServerBinding> = BTreeMap::new();
        for binding in bindings {
            if bound.contains_key(&binding.server_id) {
                return Err(McpRegistrationError::DuplicateServer {
                    server: binding.server_id.to_string(),
                });
            }
            bound.insert(binding.server_id.clone(), binding);
        }
        let mut sets: BTreeMap<McpServerId, McpDescriptorSet> = BTreeMap::new();
        for set in descriptors {
            if sets.contains_key(&set.server_id) {
                return Err(McpRegistrationError::DuplicateServer {
                    server: set.server_id.to_string(),
                });
            }
            sets.insert(set.server_id.clone(), set);
        }
        let mut servers: BTreeMap<McpServerId, McpBoundServer> = BTreeMap::new();
        for (server, binding) in bound {
            let Some(descriptors) = sets.remove(&server) else {
                return Err(McpRegistrationError::DescriptorSetMissing {
                    server: server.to_string(),
                });
            };
            servers.insert(
                server,
                McpBoundServer {
                    binding,
                    descriptors,
                },
            );
        }
        if let Some(server) = sets.keys().next() {
            return Err(McpRegistrationError::BindingMissing {
                server: server.to_string(),
            });
        }
        let tools = routes(&servers, None)?;
        Ok(Self {
            servers,
            tools,
            artifacts,
            http,
            egress,
            launcher: None,
            recheck_ttl_ms: MCP_DESCRIPTOR_RECHECK_TTL_DEFAULT_MS,
            rechecked: Mutex::new(BTreeMap::new()),
        })
    }

    /// Installs the deployment's child-process launcher and re-validates.
    ///
    /// Re-validation is the point: [`Self::new`] refuses a `ChildProcess`
    /// binding outright, so this is where such a binding becomes legal — and
    /// it must become legal by the same rule, not by skipping it.
    ///
    /// # Errors
    ///
    /// [`McpRegistrationError`] with its stable code.
    pub fn with_child_process_launcher(
        mut self,
        launcher: Arc<dyn McpChildProcessLauncher>,
    ) -> Result<Self, McpRegistrationError> {
        self.tools = routes(&self.servers, Some(&launcher))?;
        self.launcher = Some(launcher);
        Ok(self)
    }

    /// Sets how long a server's rechecked tool shapes stay usable before the
    /// next attempt re-reads them. `0` rechecks on every attempt.
    #[must_use]
    pub const fn with_descriptor_recheck_ttl_ms(mut self, ttl_ms: u64) -> Self {
        self.recheck_ttl_ms = ttl_ms;
        self
    }

    /// Every Rakka tool id this executor dispatches, in id order.
    pub fn bound_tools(&self) -> impl Iterator<Item = &AgentToolId> {
        self.tools.keys()
    }

    /// Whether the server's rechecked shapes are older than the TTL — or were
    /// never read at all.
    ///
    /// A poisoned cache reads as due: the cache can only ever *refuse* a call,
    /// so re-reading is the safe answer, never the permissive one.
    fn recheck_due(&self, server: &McpServerId) -> bool {
        let Ok(held) = self.rechecked.lock() else {
            return true;
        };
        let Some(entry) = held.get(server) else {
            return true;
        };
        entry.at.elapsed() >= Duration::from_millis(self.recheck_ttl_ms)
    }

    /// Records what one recheck read.
    fn store_recheck(&self, server: McpServerId, digests: BTreeMap<String, AgentContentDigest>) {
        if let Ok(mut held) = self.rechecked.lock() {
            held.insert(
                server,
                RecheckEntry {
                    at: Instant::now(),
                    digests,
                },
            );
        }
    }

    /// The live digest of one tool's input schema, as the last recheck read
    /// it. `None` when the server did not list the tool, or when the cache is
    /// unreadable — both of which refuse the attempt.
    fn live_digest(&self, server: &McpServerId, tool: &str) -> Option<AgentContentDigest> {
        let held = self.rechecked.lock().ok()?;
        held.get(server)?.digests.get(tool).cloned()
    }
}

impl<C> McpDispatchToolExecutor<C>
where
    C: StreamableHttpClient + Clone + Send + Sync + 'static,
{
    /// One dispatch attempt, end to end.
    ///
    /// The session is opened once and closed on every path out, including
    /// every refusal raised after it opened.
    async fn attempt(
        &self,
        scope: &AgentRunScope,
        intent: &AgentRunEffect,
        call: &AgentToolCallRequest,
        credential: Option<&AgentEphemeralCredential>,
    ) -> Result<AgentTaskContent, AgentDispatchError> {
        let unbound = || {
            AgentDispatchError::collaborator(
                "mcp-tool-unbound",
                format!(
                    "{} is bound to no MCP server this executor holds",
                    call.tool
                ),
            )
        };
        let route = self.tools.get(&call.tool).ok_or_else(unbound)?;
        // The three lookups below cannot miss for a routing table `routes`
        // built — the route names a server that was in the map, at an index
        // taken from that server's own set, for a tool the binding lists. They
        // resolve to the same refusal rather than a panic anyway: an executor
        // whose own table disagreed with itself is one that dispatches
        // nothing, not one that brings the worker down.
        let server = self.servers.get(&route.server).ok_or_else(unbound)?;
        let descriptor = server
            .descriptors
            .descriptors
            .get(route.index)
            .ok_or_else(unbound)?;
        let Some(policy) = server.binding.tools.get(&descriptor.tool) else {
            return Err(unbound());
        };
        let session = self.open(scope, &server.binding, credential).await?;
        let outcome = self
            .call(&session, server, descriptor, policy, intent, call)
            .await;
        session.close().await;
        outcome
    }

    /// Opens the attempt's client.
    ///
    /// The egress rule is not applied here: it fires inside the `client`
    /// module's own `connect`, which is the only way this crate opens an
    /// outbound HTTP session, so *being connected* and *having passed the
    /// rule* are one event. The credential is handed straight through and read
    /// only on the far side of that check.
    fn open<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        binding: &'a McpServerBinding,
        credential: Option<&'a AgentEphemeralCredential>,
    ) -> McpSessionFuture<'a> {
        Box::pin(async move {
            match &binding.transport {
                McpTransport::StreamableHttp { .. } => {
                    client::connect(&self.http, binding, credential, self.egress.as_ref())
                        .await
                        .map_err(dispatch_error)
                }
                McpTransport::ChildProcess { spec_ref } => {
                    let Some(launcher) = self.launcher.as_ref() else {
                        return Err(AgentDispatchError::collaborator(
                            "mcp-transport-unsupported",
                            format!(
                                "the MCP server {} is a child process and this executor holds \
                                 no launcher",
                                binding.server_id
                            ),
                        ));
                    };
                    let transport = launcher.launch(scope, spec_ref).await.map_err(|error| {
                        AgentDispatchError::collaborator(error.code(), error.to_string())
                    })?;
                    // The launched pair *is* rmcp's transport: `(R, W)` of an
                    // `AsyncRead` and an `AsyncWrite` implements
                    // `IntoTransport` (`rmcp-3.4.0/src/transport/async_rw.rs:24`).
                    // `into_pair` is what makes each half concrete; see its
                    // own documentation for why that is load-bearing. No
                    // credential rides this path — a child process has no
                    // request to put a header on.
                    client::connect_over(transport.into_pair(), binding)
                        .await
                        .map_err(dispatch_error)
                }
            }
        })
    }

    /// The recheck, the request, the timeout, and the mapping.
    async fn call(
        &self,
        session: &McpClientSession,
        server: &McpBoundServer,
        descriptor: &McpSyncedDescriptor,
        policy: &McpToolPolicy,
        intent: &AgentRunEffect,
        call: &AgentToolCallRequest,
    ) -> Result<AgentTaskContent, AgentDispatchError> {
        self.recheck(session, server, descriptor, &call.tool)
            .await?;
        let Some(arguments) = call.arguments.as_object().cloned() else {
            return Err(AgentDispatchError::collaborator(
                "mcp-tool-error",
                "the call's arguments are not a JSON object",
            ));
        };
        let mut params =
            CallToolRequestParams::new(descriptor.tool.clone()).with_arguments(arguments);
        params.meta = Some(call_meta(intent));
        // `unwrap_or` rather than a branch: an effect with no timeout is
        // bounded by the far future, which `tokio::time::timeout` clamps
        // rather than overflowing (`tokio-1.52.3/src/time/timeout.rs:92`).
        let bound = Duration::from_millis(intent.timeout_ms.unwrap_or(u64::MAX / 2));
        let answered = tokio::time::timeout(bound, session.peer().call_tool_once(params)).await;
        let answer = match answered {
            Err(_) => {
                return Err(AgentDispatchError::Invocation {
                    code: "mcp-transport-failed",
                    message: "the call exceeded the effect's timeout".to_string(),
                })
            }
            Ok(Err(error)) => {
                return Err(dispatch_error(client::service_error(
                    server.binding.server_id.as_str(),
                    &error,
                )))
            }
            Ok(Ok(answer)) => answer,
        };
        self.content(policy, &call.tool, intent, call, answer).await
    }

    /// Refuses the attempt unless the server's live input schema is still the
    /// one the publish-time sync pinned.
    ///
    /// The listing is read at most once per TTL per server, and the comparison
    /// runs on every attempt — against a fresh listing when one was due, and
    /// against the last one otherwise. Both digests are SHA-256 over the same
    /// canonical JSON the sync used, so the comparison is like for like.
    async fn recheck(
        &self,
        session: &McpClientSession,
        server: &McpBoundServer,
        descriptor: &McpSyncedDescriptor,
        tool: &AgentToolId,
    ) -> Result<(), AgentDispatchError> {
        let id = &server.binding.server_id;
        if self.recheck_due(id) {
            let listed = session
                .peer()
                .list_all_tools()
                .await
                .map_err(|error| dispatch_error(client::service_error(id.as_str(), &error)))?;
            let digests = listed
                .iter()
                .map(|listed| {
                    (
                        listed.name.to_string(),
                        AgentContentDigest::sha256_of_json(&Value::Object(
                            (*listed.input_schema).clone(),
                        )),
                    )
                })
                .collect();
            self.store_recheck(id.clone(), digests);
        }
        if self.live_digest(id, &descriptor.tool).as_ref() == Some(&descriptor.schema_digest) {
            return Ok(());
        }
        Err(AgentDispatchError::collaborator(
            "tool-descriptor-revision-mismatch",
            format!("{tool}: the server's schema no longer matches the published descriptor"),
        ))
    }

    /// Maps one answered call onto bounded task content.
    async fn content(
        &self,
        policy: &McpToolPolicy,
        tool: &AgentToolId,
        intent: &AgentRunEffect,
        call: &AgentToolCallRequest,
        answer: CallToolResponse,
    ) -> Result<AgentTaskContent, AgentDispatchError> {
        match answer {
            CallToolResponse::InputRequired(_) => Err(AgentDispatchError::collaborator(
                "mcp-input-required",
                format!(
                    "{tool}: the server asked for another round of input, which this client \
                     does not answer"
                ),
            )),
            CallToolResponse::Task(_) => Err(AgentDispatchError::collaborator(
                "mcp-protocol-unsupported",
                "task-augmented results are not requested by this client",
            )),
            CallToolResponse::Complete(result) => {
                if result.is_error == Some(true) {
                    return Err(AgentDispatchError::collaborator(
                        "mcp-tool-error",
                        error_detail(&result),
                    ));
                }
                self.bounded(policy, tool, intent, call, &result).await
            }
            // `CallToolResponse` is `#[non_exhaustive]`: a response kind rmcp
            // adds later is one this client did not ask for, and it is refused
            // rather than silently read as a success.
            _ => Err(AgentDispatchError::collaborator(
                "mcp-protocol-unsupported",
                format!(
                    "{tool}: the server answered with a response kind this client does not request"
                ),
            )),
        }
    }

    /// Inline when the result is small and wholly textual or structured; an
    /// artifact when the binding says so; a refusal otherwise.
    async fn bounded(
        &self,
        policy: &McpToolPolicy,
        tool: &AgentToolId,
        intent: &AgentRunEffect,
        call: &AgentToolCallRequest,
        result: &CallToolResult,
    ) -> Result<AgentTaskContent, AgentDispatchError> {
        let (candidate, overflow) = candidate_of(result);
        let encoded = serde_json::to_vec(&candidate).map_err(encoding_refused)?;
        if !overflow && encoded.len() <= MCP_INLINE_RESULT_MAX_BYTES {
            return AgentTaskContent::inline(candidate).map_err(|error| {
                AgentDispatchError::collaborator("mcp-tool-error", error.to_string())
            });
        }
        match policy.result_behavior {
            AgentToolResultBehavior::ArtifactReference => {
                let bytes = serde_json::to_vec(&StoredResult {
                    content: &result.content,
                    structured_content: &result.structured_content,
                })
                .map_err(encoding_refused)?;
                let request = AgentArtifactWriteRequest {
                    // Derived, so a re-driven attempt of the same generation
                    // writes the same artifact rather than a second one.
                    artifact_id: Some(format!(
                        "mcp-{}-g{}-{}",
                        intent.effect_id,
                        intent.generation.get(),
                        call.call_id
                    )),
                    // The retention class is the deployment's decision about
                    // its own data, not this adapter's.
                    retention_class: None,
                    // The executor holds no clock; the effect's own commit
                    // time is durable and identical across a re-drive, which
                    // is what the derived artifact id needs it to be.
                    ..AgentArtifactWriteRequest::new(
                        ArtifactKind::File,
                        "application/json",
                        bytes,
                        intent.created_at,
                    )
                };
                let mut store = self.artifacts.lock().await;
                let reference = store.put_artifact(request).await.map_err(|error| {
                    AgentDispatchError::collaborator(error.code(), error.to_string())
                })?;
                Ok(AgentTaskContent::artifact(reference))
            }
            // `InlineBounded`, and — since `AgentToolResultBehavior` is
            // `#[non_exhaustive]` — any behavior a later version adds that
            // this adapter does not know how to satisfy. Both keep the inline
            // bound, which refuses rather than widening what becomes state.
            _ => Err(AgentDispatchError::collaborator(
                "mcp-result-too-large",
                format!(
                    "{tool}: the result exceeds {MCP_INLINE_RESULT_MAX_BYTES} bytes and the \
                     binding keeps results inline"
                ),
            )),
        }
    }
}

impl<C> AgentDispatchToolExecutor for McpDispatchToolExecutor<C>
where
    C: StreamableHttpClient + Clone + Send + Sync + 'static,
{
    fn execute<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        intent: &'a AgentRunEffect,
        call: &'a AgentToolCallRequest,
        credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentDispatchFuture<'a, AgentTaskContent> {
        Box::pin(self.attempt(scope, intent, call, credential))
    }
}

/// Validates the bound servers and derives the tool routing table.
///
/// Runs both at construction and whenever the launcher changes, because the
/// launcher is what decides whether a `ChildProcess` binding is dispatchable
/// at all.
fn routes(
    servers: &BTreeMap<McpServerId, McpBoundServer>,
    launcher: Option<&Arc<dyn McpChildProcessLauncher>>,
) -> Result<BTreeMap<AgentToolId, McpToolRoute>, McpRegistrationError> {
    let mut tools = BTreeMap::new();
    for (server, bound) in servers {
        bound.binding.validate()?;
        if matches!(bound.binding.transport, McpTransport::ChildProcess { .. })
            && launcher.is_none()
        {
            return Err(McpRegistrationError::TransportUnsupported {
                server: server.to_string(),
            });
        }
        for tool in bound.binding.tools.keys() {
            let Some(index) = bound
                .descriptors
                .descriptors
                .iter()
                .position(|descriptor| &descriptor.tool == tool)
            else {
                return Err(McpRegistrationError::ToolNameInvalid {
                    server: server.to_string(),
                    tool: tool.clone(),
                    reason: "the server's synced descriptor set holds no descriptor for it; \
                             re-sync the server before binding it"
                        .to_string(),
                });
            };
            tools.insert(
                bound.binding.tool_id(tool)?,
                McpToolRoute {
                    server: server.clone(),
                    index,
                },
            );
        }
    }
    Ok(tools)
}

/// The `_meta` one call carries: the effect's idempotency key, and the trace
/// context when the effect committed one.
///
/// The key is the external idempotency key when the effect's safety class
/// carries one — that is the value a retry must reuse — and the derived
/// internal key otherwise.
fn call_meta(intent: &AgentRunEffect) -> RequestMetaObject {
    let mut meta = MetaObject::new();
    if let Some(trace_parent) = &intent.telemetry.trace_parent {
        meta.set_traceparent(trace_parent.clone());
    }
    if let Some(trace_state) = &intent.telemetry.trace_state {
        meta.set_tracestate(trace_state.clone());
    }
    let key = intent
        .safety
        .external_key()
        .map(ToString::to_string)
        .unwrap_or_else(|| intent.idempotency_key.as_str().to_string());
    meta.0
        .insert(MCP_META_IDEMPOTENCY_KEY.to_string(), json!(key));
    RequestMetaObject(meta)
}

/// The value a successful result would become inline, and whether any part of
/// it cannot be inlined at all.
///
/// A binary or reference part is an overflow whatever its size: an image, an
/// audio blob, an embedded resource, or a link to one is content this adapter
/// does not inline into a run's durable state.
fn candidate_of(result: &CallToolResult) -> (Value, bool) {
    let overflow = result
        .content
        .iter()
        .any(|block| !matches!(block, ContentBlock::Text(_)));
    let candidate = result.structured_content.clone().unwrap_or_else(|| {
        let joined = result
            .content
            .iter()
            .filter_map(ContentBlock::as_text)
            .map(|text| text.text.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        Value::Object(Map::from_iter([(
            "text".to_string(),
            Value::String(joined),
        )]))
    });
    (candidate, overflow)
}

/// The detail a tool-reported error becomes: the first text block, flattened
/// and truncated.
///
/// Bounded and control-character-free because this text is written to the
/// run's durable outbox row and echoed across the dispatcher fleet, and the
/// server chose it. Nothing the *model* sent is read here at all, so no
/// argument can ride a refusal back out.
fn error_detail(result: &CallToolResult) -> String {
    result
        .content
        .iter()
        .find_map(ContentBlock::as_text)
        .map_or_else(
            || "the tool reported an error".to_string(),
            |text| bounded_detail(&text.text),
        )
}

/// Flattens control characters to spaces and truncates at a character
/// boundary.
fn bounded_detail(text: &str) -> String {
    let flattened: String = text
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect();
    if flattened.len() <= MCP_TOOL_ERROR_DETAIL_MAX_BYTES {
        return flattened;
    }
    let mut end = MCP_TOOL_ERROR_DETAIL_MAX_BYTES;
    while end > 0 && !flattened.is_char_boundary(end) {
        end -= 1;
    }
    flattened[..end].to_string()
}

/// A result this adapter could not even re-encode is a tool error, not a
/// panic.
fn encoding_refused(error: serde_json::Error) -> AgentDispatchError {
    AgentDispatchError::collaborator(
        "mcp-tool-error",
        format!("the result could not be encoded: {error}"),
    )
}

/// Maps a client failure onto the dispatch pipeline's own error.
///
/// A transport or protocol failure is an *invocation* failure — the attempt
/// ran and did not land — while every other variant is a collaborator refusal
/// under its own stable code, including an egress refusal, which carries the
/// host's code rather than one this crate invented.
fn dispatch_error(error: McpClientError) -> AgentDispatchError {
    match &error {
        McpClientError::Transport { .. } | McpClientError::Protocol { .. } => {
            AgentDispatchError::Invocation {
                code: "mcp-transport-failed",
                message: error.to_string(),
            }
        }
        other => AgentDispatchError::collaborator(other.code().to_string(), error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rakka_agent::{
        AgentContentDigest, AgentEffectSafetyClass, AgentRevisionNumber, AgentSchemaId,
        AgentSchemaRef, AgentToolBinding, AgentToolDeclaration, AgentToolDescriptor, AgentToolId,
        AgentToolKind,
    };
    use rakka_agent_workflow::{
        AgentArtifactError, AgentArtifactRead, AgentArtifactStore, AgentArtifactStoreFuture,
        AgentArtifactWriteRequest, AgentTimestampMillis, ArtifactRef,
    };
    use rmcp::model::{CallToolResult, ContentBlock};
    use serde_json::json;

    use super::{
        bounded_detail, candidate_of, error_detail, mcp_artifact_store, McpArtifactStore,
        McpDispatchToolExecutor,
    };
    use crate::binding::{
        McpServerBinding, McpServerId, McpToolPolicy, MCP_TOOL_ERROR_DETAIL_MAX_BYTES,
    };
    use crate::client::McpAllowAllEgress;
    use crate::sync::{McpDescriptorSet, McpSyncedDescriptor};

    /// A store no construction proof ever writes to: every one of them refuses
    /// before an attempt exists.
    struct UnusedStore;

    impl AgentArtifactStore for UnusedStore {
        fn put_artifact<'a>(
            &'a mut self,
            _request: AgentArtifactWriteRequest,
        ) -> AgentArtifactStoreFuture<'a, ArtifactRef> {
            Box::pin(async {
                Err(AgentArtifactError::ArtifactNotFound {
                    artifact_id: "unused".to_string(),
                })
            })
        }

        fn get_artifact<'a>(
            &'a self,
            reference: &'a ArtifactRef,
        ) -> AgentArtifactStoreFuture<'a, AgentArtifactRead> {
            let artifact_id = reference.artifact_id.clone();
            Box::pin(async move { Err(AgentArtifactError::ArtifactNotFound { artifact_id }) })
        }
    }

    fn store() -> McpArtifactStore {
        mcp_artifact_store(UnusedStore)
    }

    fn server(name: &str) -> McpServerId {
        McpServerId::new(name).expect("the server id is valid")
    }

    fn http_binding(name: &str) -> McpServerBinding {
        McpServerBinding::streamable_http(server(name), "https://example.test/mcp")
            .with_tool(
                "search",
                McpToolPolicy::new(AgentToolDeclaration::new(AgentEffectSafetyClass::ReadOnly)),
            )
            .expect("the tool name is valid")
    }

    fn child_binding(name: &str) -> McpServerBinding {
        McpServerBinding::child_process(
            server(name),
            ArtifactRef {
                artifact_id: "spec".to_string(),
                kind: rakka_agent_workflow::ArtifactKind::File,
                uri: "memory://spec".to_string(),
                checksum: None,
                content_type: None,
                byte_len: None,
                retention_class: None,
                encryption: None,
                redaction: rakka_agent_workflow::RedactionStatus::ReferenceOnly,
                created_at: AgentTimestampMillis::new(1),
                metadata: rakka_agent_workflow::AgentAttributes::new(),
            },
        )
        .with_tool(
            "search",
            McpToolPolicy::new(AgentToolDeclaration::new(AgentEffectSafetyClass::ReadOnly)),
        )
        .expect("the tool name is valid")
    }

    /// A descriptor set as a publish-time sync would have left it, built by
    /// hand so that construction stays offline even in a unit test.
    fn set(name: &str, tools: &[&str]) -> McpDescriptorSet {
        let binding = http_binding(name);
        McpDescriptorSet {
            server_id: server(name),
            server_name: "fake".to_string(),
            protocol_version: "2026-07-28".to_string(),
            synced_at: AgentTimestampMillis::new(1),
            descriptors: tools
                .iter()
                .map(|tool| {
                    let schema = json!({ "type": "object" });
                    let descriptor = AgentToolDescriptor::new(
                        binding.tool_id(tool).expect("the tool id derives"),
                        AgentToolKind::RemoteMcp,
                        "A synced tool.",
                        AgentSchemaRef::new(
                            AgentSchemaId::new(format!("mcp.{name}.{tool}.input"))
                                .expect("the schema id is valid"),
                            AgentRevisionNumber::INITIAL,
                        ),
                        AgentSchemaRef::new(
                            AgentSchemaId::new(format!("mcp.{name}.{tool}.output"))
                                .expect("the schema id is valid"),
                            AgentRevisionNumber::INITIAL,
                        ),
                    )
                    .expect("the descriptor is valid");
                    McpSyncedDescriptor {
                        tool: (*tool).to_string(),
                        binding: AgentToolBinding::new(
                            descriptor,
                            AgentToolDeclaration::new(AgentEffectSafetyClass::ReadOnly),
                            1,
                        ),
                        schema_digest: AgentContentDigest::sha256_of_json(&schema),
                        input_schema: schema,
                        output_schema_digest: None,
                        input_schema_artifact: None,
                    }
                })
                .collect(),
        }
    }

    fn build(
        sets: Vec<McpDescriptorSet>,
        bindings: Vec<McpServerBinding>,
    ) -> Result<McpDispatchToolExecutor<()>, crate::binding::McpRegistrationError> {
        // `()` as the HTTP client: construction is offline, and no proof here
        // reaches a dispatch, so the client type is irrelevant to what is
        // being asserted.
        McpDispatchToolExecutor::new(sets, bindings, store(), (), Arc::new(McpAllowAllEgress))
    }

    #[test]
    fn the_routing_table_and_the_debug_line_hold_only_counts() {
        let executor = build(vec![set("crm", &["search"])], vec![http_binding("crm")])
            .expect("the binding and its set pair");
        let tools: Vec<&str> = executor.bound_tools().map(AgentToolId::as_str).collect();
        assert_eq!(tools, vec!["mcp.crm.search"]);
        let debug = format!("{executor:?}");
        assert!(
            debug.contains("servers: 1") && debug.contains("tools: 1"),
            "{debug}"
        );
        assert!(
            !debug.contains("example.test"),
            "no endpoint URL reaches a debug line: {debug}"
        );
    }

    #[test]
    fn the_pairing_is_total_in_both_directions_and_admits_no_duplicate() {
        let missing_set =
            build(Vec::new(), vec![http_binding("crm")]).expect_err("a binding with no synced set");
        assert_eq!(missing_set.code(), "mcp-binding-invalid");
        assert!(
            missing_set.to_string().contains("no synced descriptor set"),
            "{missing_set}"
        );

        let missing_binding =
            build(vec![set("crm", &["search"])], Vec::new()).expect_err("a set with no binding");
        assert!(
            missing_binding
                .to_string()
                .contains("no MCP binding is registered"),
            "{missing_binding}"
        );

        for (sets, bindings) in [
            (
                vec![set("crm", &["search"]), set("crm", &["search"])],
                vec![http_binding("crm")],
            ),
            (
                vec![set("crm", &["search"])],
                vec![http_binding("crm"), http_binding("crm")],
            ),
        ] {
            let error = build(sets, bindings).expect_err("one server, registered twice");
            assert!(error.to_string().contains("already registered"), "{error}");
        }
    }

    #[test]
    fn a_child_process_binding_needs_a_launcher_before_it_is_dispatchable() {
        let error = build(vec![set("crm", &["search"])], vec![child_binding("crm")])
            .expect_err("no launcher is installed");
        assert_eq!(error.code(), "mcp-transport-unsupported");
    }

    #[test]
    fn structured_content_is_the_candidate_when_the_server_sent_it() {
        let result = CallToolResult::structured(json!({ "total": 3 }));
        let (candidate, overflow) = candidate_of(&result);
        assert_eq!(candidate, json!({ "total": 3 }));
        assert!(!overflow);
    }

    #[test]
    fn text_blocks_join_under_one_text_key() {
        let result = CallToolResult::success(vec![
            ContentBlock::text("first"),
            ContentBlock::text("second"),
        ]);
        let (candidate, overflow) = candidate_of(&result);
        assert_eq!(candidate, json!({ "text": "first\nsecond" }));
        assert!(!overflow);
    }

    #[test]
    fn a_binary_part_overflows_however_small_it_is() {
        let result = CallToolResult::success(vec![ContentBlock::image("AA==", "image/png")]);
        let (_, overflow) = candidate_of(&result);
        assert!(
            overflow,
            "a binary part never inlines, whatever the encoded size says"
        );
    }

    #[test]
    fn an_error_with_no_text_reads_back_as_a_fixed_line() {
        let result = CallToolResult::error(Vec::new());
        assert_eq!(error_detail(&result), "the tool reported an error");
    }

    #[test]
    fn an_error_detail_is_flattened_and_bounded_at_a_character_boundary() {
        let result = CallToolResult::error(vec![ContentBlock::text("boom\nline2\ttabbed")]);
        assert_eq!(error_detail(&result), "boom line2 tabbed");
        let wide = "é".repeat(MCP_TOOL_ERROR_DETAIL_MAX_BYTES);
        let bounded = bounded_detail(&wide);
        assert!(bounded.len() <= MCP_TOOL_ERROR_DETAIL_MAX_BYTES);
        assert!(wide.starts_with(&bounded));
    }
}
