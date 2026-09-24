//! An MCP tool call through the real dispatch pipeline: the run commits the
//! intent from bindings a publish-time sync produced, the outbox ticket is
//! leased, the gate authorizes the registered binding, the dispatcher resolves
//! the declared credential inside the attempt, and the executor router hands
//! the call to `rakka-agent-mcp`'s executor — which reaches an in-process MCP
//! server over real Streamable HTTP.
//!
//! What `rakka-agent-mcp`'s own `client_dispatch.rs` proves about the executor
//! in isolation — the header, `_meta`, the recheck, result mapping — it proves
//! against an intent it built by hand. This file proves the same executor
//! behind the *pipeline*: the credential is the dispatcher's own resolution,
//! the idempotency key is the durable effect record's, a refusal is classified
//! by the dispatcher's own attempt rules, and the recorded result is what the
//! run durably records — its session memory, since the loop clears a turn's
//! tool results from its own state when the turn is recorded.
//!
//! The secret sweep reuses `secret_exclusion.rs`'s approach — one sentinel,
//! every durable surface the fixture yields, plus its telemetry — for the MCP
//! walk, and extends it to the MCP crate's own types. That file owns the
//! exhaustiveness spine over the durable record catalogue; this one owns MCP.

use std::sync::Arc;

use rakka_agent::testkit::{CapturingSubscriber, DeterministicModelAdapter};
use rakka_agent::{
    AgentAuthorityRefusal, AgentContentDigest, AgentCredentialBindingRef,
    AgentDispatchToolExecutor, AgentEffectSafetyClass, AgentModelTurn, AgentRunEffect,
    AgentRunEffectRequest, AgentRunEffectStatus, AgentRunMemory, AgentRunStatus, AgentTaskContent,
    AgentToolCallId, AgentToolCallRequest, AgentToolDeclaration, AgentToolExecutorRouter,
    AgentToolId, AgentToolRegistry, AgentToolResultBehavior, InMemoryAgentSegmentSink,
    InMemoryContextSnapshotStore, InMemorySessionMemoryStore, MemoryEntryRole, SessionMemoryCursor,
    SessionMemoryEntry, SessionMemoryStore, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_mcp::testkit::{
    hardened_reqwest_client, serve_fake, FakeMcpEndpoint, FakeMcpServer, FakeTool,
    FakeToolBehaviour, ReqwestClient,
};
use rakka_agent_mcp::{
    mcp_artifact_store, sync_mcp_descriptors, McpAllowAllEgress, McpDescriptorSet,
    McpDispatchToolExecutor, McpEgressCheck, McpServerBinding, McpServerId, McpToolPolicy,
    MCP_INLINE_RESULT_MAX_BYTES, MCP_META_IDEMPOTENCY_KEY,
};
use rakka_agent_workflow::substrate::OutboxStatus;
use rakka_agent_workflow::{validate_artifact_ref, AgentEphemeralCredential, AgentTimestampMillis};
use serde_json::{json, Value};

mod common;
use common::{run_scope, AuthorityFixture, SharedArtifactStore};

/// The token the fixture's credential resolver mints: on the wire, and
/// nowhere else.
const SENTINEL: &str = "run-token-sentinel";

/// The token scenario 7's resolver mints instead of [`SENTINEL`], and its
/// hostile server echoes back. A token of its own because the echo does reach
/// one surface Rakka does not own: rmcp's own `DEBUG`/`TRACE` logging of the
/// response it received. Scenario 2 sweeps a *process-global* log capture, so
/// a shared token echoed here would read as a leak there.
const ECHOED: &str = "echoed-token-sentinel";

/// The MCP server every proof binds.
const SERVER: &str = "crm";

/// The logical credential binding the server and its tools declare.
const CREDENTIAL: &str = "crm-key";

/// The echo tool's Rakka id: `mcp.<server>.<tool>`.
const ECHO: &str = "mcp.crm.echo";

/// The oversized tool's Rakka id.
const BIG: &str = "mcp.crm.big";

/// The Rakka id of the tool that fails quoting the credential it was sent.
const LEAK: &str = "mcp.crm.leak";

/// How many bytes of text the oversized tool answers with — past
/// [`MCP_INLINE_RESULT_MAX_BYTES`], so the result cannot stay inline.
const BIG_RESULT_BYTES: usize = 5_000;

/// The per-attempt timeout every MCP tool policy declares, projected onto the
/// effect spec through `AgentToolBinding::effect_spec`.
const TOOL_TIMEOUT_MS: u64 = 5_000;

/// The loop slot of the tool effect while turn 1 is open: the model call is
/// slot 0 and the tool call it requested is slot 1. (Turn 2 starts its slots
/// again, once turn 1 is recorded and its resolved effects leave the loop.)
const TOOL_SLOT: usize = 1;

/// The stable code the host's egress rule refuses with.
const EGRESS_DENIED: &str = "egress-denied-by-policy";

/// The pipeline code the dispatcher persists for a failure that came from an
/// executor as an `AgentDispatchError::Collaborator` — every `rakka-agent-mcp`
/// refusal except `mcp-transport-failed`, which is an `Invocation`.
const COLLABORATOR_FAILED: &str = "dispatch-collaborator-failed";

/// Names that would mean a field is carrying secret material rather than a
/// reference to it. A key is secret-shaped when it *is* one of these or ends
/// in one (`access_token`, `client-secret`); a policy flag that merely starts
/// with one — `authorization_required` — names a requirement, not material.
const SECRET_SHAPED_KEYS: &[&str] = &[
    "token",
    "secret",
    "password",
    "authorization",
    "bearer",
    "api_key",
    "api-key",
    "apikey",
];

fn credential_binding() -> AgentCredentialBindingRef {
    AgentCredentialBindingRef::new(CREDENTIAL).expect("the binding ref is valid")
}

/// One tool's policy: the given safety class, the credential binding the
/// dispatcher resolves from, a timeout, and a result behavior.
///
/// A `NonIdempotent` declaration keeps the policy's single attempt — its
/// validated budget — and every other class is allowed two, so a retry would
/// be visible.
fn policy(class: AgentEffectSafetyClass, behavior: AgentToolResultBehavior) -> McpToolPolicy {
    let mut policy = unbound_policy(class, behavior);
    policy.declaration = policy
        .declaration
        .with_credential_binding(credential_binding());
    policy
}

/// [`policy`] without the tool-level credential binding: the declaration
/// names none, so only a server-level binding could supply one.
fn unbound_policy(
    class: AgentEffectSafetyClass,
    behavior: AgentToolResultBehavior,
) -> McpToolPolicy {
    let attempts = if class == AgentEffectSafetyClass::NonIdempotent {
        1
    } else {
        2
    };
    McpToolPolicy::new(AgentToolDeclaration::new(class))
        .with_max_attempts(attempts)
        .with_timeout_ms(TOOL_TIMEOUT_MS)
        .with_result_behavior(behavior)
}

/// The fake server: an echo tool and a tool whose text answer is too large to
/// keep inline.
fn fake_server() -> FakeMcpServer {
    FakeMcpServer::new()
        .with_tool(FakeTool::new(
            "echo",
            "Echoes its arguments.",
            json!({"type": "object"}),
            FakeToolBehaviour::Echo,
        ))
        .with_tool(FakeTool::new(
            "big",
            "Answers with more text than fits inline.",
            json!({"type": "object"}),
            FakeToolBehaviour::Text("x".repeat(BIG_RESULT_BYTES)),
        ))
}

/// [`fake_server`] plus a tool whose error text quotes the bearer token its
/// request carried — a hostile server folding the credential into the text
/// the dispatcher persists.
fn leaking_fake_server() -> FakeMcpServer {
    fake_server().with_tool(FakeTool::new(
        "leak",
        "Fails, quoting the credential it was sent.",
        json!({"type": "object"}),
        FakeToolBehaviour::Error(format!("denied: bearer {ECHOED} is revoked")),
    ))
}

/// [`server_binding`] with the leaking tool allow-listed too.
fn leaking_binding(url: &str, class: AgentEffectSafetyClass) -> McpServerBinding {
    server_binding(url, class)
        .with_tool(
            "leak",
            policy(class, AgentToolResultBehavior::InlineBounded),
        )
        .expect("the leak tool binds")
}

/// The operator's binding for the fake: both tools under one safety class,
/// each tool's declaration and the server naming the same credential binding
/// — a tool may repeat the server-level binding, never name another.
fn server_binding(url: &str, class: AgentEffectSafetyClass) -> McpServerBinding {
    McpServerBinding::streamable_http(McpServerId::new(SERVER).expect("server id"), url)
        .with_tool(
            "echo",
            policy(class, AgentToolResultBehavior::InlineBounded),
        )
        .expect("the echo tool binds")
        .with_tool(
            "big",
            policy(class, AgentToolResultBehavior::ArtifactReference),
        )
        .expect("the big tool binds")
        .with_credential_binding(credential_binding())
}

/// The same binding with the credential named on the server alone: neither
/// tool's declaration names one, so the dispatcher resolves it only because
/// the publish-time sync copied it into each synced declaration.
fn server_level_binding(url: &str, class: AgentEffectSafetyClass) -> McpServerBinding {
    McpServerBinding::streamable_http(McpServerId::new(SERVER).expect("server id"), url)
        .with_tool(
            "echo",
            unbound_policy(class, AgentToolResultBehavior::InlineBounded),
        )
        .expect("the echo tool binds")
        .with_tool(
            "big",
            unbound_policy(class, AgentToolResultBehavior::ArtifactReference),
        )
        .expect("the big tool binds")
        .with_credential_binding(credential_binding())
}

/// A served fake, the stored descriptor set a publish-time sync produced for
/// it, and the executor built from both — everything a deployment has before
/// the first run starts — plus the stores a proof reads back.
struct McpWorld {
    endpoint: FakeMcpEndpoint,
    binding: McpServerBinding,
    set: McpDescriptorSet,
    executor: Arc<McpDispatchToolExecutor<ReqwestClient>>,
    artifacts: SharedArtifactStore,
    session: Arc<InMemorySessionMemoryStore>,
}

impl McpWorld {
    /// Syncs the fake with no credential and builds the executor behind the
    /// given egress rule.
    async fn build(class: AgentEffectSafetyClass, egress: Arc<dyn McpEgressCheck>) -> Self {
        Self::build_with_sync_credential(class, egress, None).await
    }

    /// As [`Self::build`], handing the publish-time sync `sync_credential`.
    ///
    /// The sync itself always passes the explicit allow-all egress rule: it
    /// is the operator's publish step, and what a proof refuses is the
    /// *attempt*.
    async fn build_with_sync_credential(
        class: AgentEffectSafetyClass,
        egress: Arc<dyn McpEgressCheck>,
        sync_credential: Option<&AgentEphemeralCredential>,
    ) -> Self {
        Self::assemble(
            fake_server(),
            |url| server_binding(url, class),
            egress,
            sync_credential,
        )
        .await
    }

    /// Serves `fake`, syncs the binding `binding_of` builds for its URL, and
    /// builds the executor behind `egress` — the one construction path every
    /// world takes.
    async fn assemble(
        fake: FakeMcpServer,
        binding_of: impl FnOnce(&str) -> McpServerBinding,
        egress: Arc<dyn McpEgressCheck>,
        sync_credential: Option<&AgentEphemeralCredential>,
    ) -> Self {
        let endpoint = serve_fake(fake).await;
        let binding = binding_of(&endpoint.url);
        let http = hardened_reqwest_client();
        let set = sync_mcp_descriptors(
            &http,
            &binding,
            sync_credential,
            AgentTimestampMillis::new(1),
            &McpAllowAllEgress,
        )
        .await
        .expect("the publish-time sync reads the fake");
        let artifacts = SharedArtifactStore::default();
        let executor = McpDispatchToolExecutor::new(
            vec![set.clone()],
            vec![binding.clone()],
            mcp_artifact_store(artifacts.clone()),
            http,
            egress,
        )
        .expect("the executor builds offline from the stored set");
        Self {
            endpoint,
            binding,
            set,
            executor: Arc::new(executor),
            artifacts,
            session: Arc::new(InMemorySessionMemoryStore::new()),
        }
    }

    /// The authority fixture a deployment would run: the synced bindings
    /// registered everywhere a tool must appear, the fixture's credential
    /// resolver minting [`SENTINEL`], the MCP executor behind the router's
    /// `mcp.` prefix with the fixture's recording executor as the fallback,
    /// and a session-memory bundle the loop records each turn into.
    ///
    /// The model asks for `tool` with `arguments` on turn 1 and proposes a
    /// result on turn 2.
    fn fixture(&self, tool: &str, arguments: Value) -> AuthorityFixture {
        self.fixture_minting(tool, arguments, SENTINEL)
    }

    /// As [`Self::fixture`], with the credential resolver minting `token`.
    fn fixture_minting(&self, tool: &str, arguments: Value, token: &str) -> AuthorityFixture {
        let adapter = DeterministicModelAdapter::new()
            .with_turn_for(1, tool_calling_turn(tool, arguments))
            .with_turn_for(2, proposing_turn());
        let fixture = AuthorityFixture::over(adapter, AgentToolRegistry::new(), None)
            .with_registered_bindings(self.set.bindings().cloned().collect())
            .with_credential_resolver(token)
            .with_memory(AgentRunMemory::new(
                self.session.clone(),
                Arc::new(InMemoryContextSnapshotStore::new()),
            ));
        let mcp: Arc<dyn AgentDispatchToolExecutor> = self.executor.clone();
        let router = AgentToolExecutorRouter::new(Arc::new(fixture.tools.clone()))
            .with_prefix_route("mcp.", mcp);
        fixture.with_tool_executor(Arc::new(router))
    }

    /// Every entry the run recorded to its session memory.
    async fn session_entries(&self) -> Vec<SessionMemoryEntry> {
        self.session
            .read(&run_scope(), SessionMemoryCursor::start())
            .await
            .expect("the session reads")
            .entries
    }

    /// The tool results the run recorded: its `ToolResult` session entries.
    async fn recorded_tool_results(&self) -> Vec<SessionMemoryEntry> {
        self.session_entries()
            .await
            .into_iter()
            .filter(|entry| entry.role == MemoryEntryRole::ToolResult)
            .collect()
    }
}

fn tool_calling_turn(tool: &str, arguments: Value) -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Looking it up.")
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("call-1").expect("the call id is valid"),
                AgentToolId::new(tool).expect("the tool id is valid"),
                arguments,
            )
            .expect("the tool call is bounded"),
        )
}

fn proposing_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Done.")
        .with_proposal(
            AgentTaskContent::inline(json!({ "answer": "found" }))
                .expect("the proposal is inline-bounded"),
        )
}

/// Starts the run and drives it until the tool's ticket is ready, returning
/// the committed tool effect — read now, because once turn 1 is recorded its
/// resolved effects leave the loop and the record is gone.
async fn start_and_commit_the_tool_call(fx: &AuthorityFixture) -> AgentRunEffect {
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    let effect = fx.effect_at(TOOL_SLOT).await.expect("the tool effect");
    assert!(
        matches!(effect.request, AgentRunEffectRequest::Tool { .. }),
        "slot {TOOL_SLOT} is the tool effect: {:?}",
        effect.request
    );
    assert_eq!(effect.status, AgentRunEffectStatus::Ready);
    effect
}

/// How many times the fixture's credential resolver was asked.
fn resolutions(fx: &AuthorityFixture) -> usize {
    fx.credentials
        .as_ref()
        .expect("the fixture wires a credential resolver")
        .resolutions()
}

/// Asserts `needle` appears on none of `surfaces`, naming the surface when it
/// does — "a secret leaked" without saying where is a finding nobody can act
/// on.
fn assert_absent_from(surfaces: &[(&str, String)], needle: &str) {
    for (label, rendered) in surfaces {
        assert!(
            !rendered.contains(needle),
            "surface {label:?} carries {needle:?}: {rendered}"
        );
    }
}

/// Asserts the surface `label` holds `needle` — the positive control that
/// says a sweep of that surface reads a real record, not an empty one.
fn assert_surface_holds(surfaces: &[(&str, String)], label: &str, needle: &str) {
    let rendered = surfaces
        .iter()
        .find(|(swept, _)| *swept == label)
        .map(|(_, rendered)| rendered.as_str())
        .unwrap_or_else(|| panic!("no surface is labelled {label:?}"));
    assert!(
        rendered.contains(needle),
        "surface {label:?} does not hold {needle:?}, so a sweep of it reads nothing: {rendered}"
    );
}

/// Whether an object key is secret-shaped under [`SECRET_SHAPED_KEYS`].
fn is_secret_shaped(key: &str) -> bool {
    let lowered = key.to_ascii_lowercase();
    SECRET_SHAPED_KEYS.iter().any(|shaped| {
        lowered == *shaped
            || lowered.ends_with(&format!("_{shaped}"))
            || lowered.ends_with(&format!("-{shaped}"))
            || lowered.ends_with(&format!(".{shaped}"))
    })
}

/// Every object key in `value`, at any depth.
fn object_keys(value: &Value, keys: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            for (key, nested) in map {
                keys.push(key.clone());
                object_keys(nested, keys);
            }
        }
        Value::Array(items) => {
            for item in items {
                object_keys(item, keys);
            }
        }
        _ => {}
    }
}

/// An egress rule that refuses every destination under the deployment's own
/// code.
struct RefuseAll;

impl McpEgressCheck for RefuseAll {
    fn check(&self, server: &McpServerId, _url: &str) -> Result<(), AgentAuthorityRefusal> {
        Err(AgentAuthorityRefusal::of(
            EGRESS_DENIED,
            format!("{server} is not an allowed egress"),
        ))
    }
}

// ---------------------------------------------------------------------------
// 1. The happy path, end to end.
// ---------------------------------------------------------------------------

/// The router hands the call to the MCP executor, which puts the dispatcher's
/// resolved credential on the wire and the durable effect's idempotency key in
/// `_meta`; the run records the server's answer as the tool result.
#[tokio::test]
async fn an_mcp_tool_call_dispatches_through_the_router_with_the_resolved_credential() {
    let world = McpWorld::build(
        AgentEffectSafetyClass::Idempotent,
        Arc::new(McpAllowAllEgress),
    )
    .await;
    let fx = world.fixture(ECHO, json!({ "q": "refunds" }));
    assert!(
        fx.envelope
            .credential_bindings
            .contains(&credential_binding()),
        "the registration hook authorizes the tool's credential binding in the envelope"
    );

    // The committed intent carries what the binding's effect spec projected.
    let effect = start_and_commit_the_tool_call(&fx).await;
    assert_eq!(
        effect.credential_binding,
        Some(credential_binding()),
        "the effect names the binding the dispatcher resolves"
    );
    assert_eq!(effect.timeout_ms, Some(TOOL_TIMEOUT_MS));
    assert_eq!(effect.safety.class(), AgentEffectSafetyClass::Idempotent);

    let pass = fx.one_pass().await;
    assert_eq!(
        pass.invoked, 1,
        "the pass dispatched the tool call: {pass:?}"
    );
    fx.pump().await;

    // The wire: one call, the resolved credential, the durable key.
    let seen = world.endpoint.server.seen_calls();
    assert_eq!(seen.len(), 1, "one attempt, one call: {seen:?}");
    assert_eq!(seen[0].name, "echo");
    assert_eq!(seen[0].arguments, json!({ "q": "refunds" }));
    assert!(
        seen[0]
            .authorization
            .as_deref()
            .is_some_and(|value| value.ends_with(SENTINEL)),
        "the dispatcher's resolved credential reached the wire as the header: {:?}",
        seen[0].authorization
    );
    assert_eq!(
        seen[0].meta[MCP_META_IDEMPOTENCY_KEY],
        json!(effect.idempotency_key.as_str()),
        "`_meta` carries the durable effect's idempotency key: {:?}",
        seen[0].meta
    );
    assert_eq!(
        resolutions(&fx),
        1,
        "the dispatcher resolved the binding once, inside the one attempt"
    );

    // The run: the answer recorded against the effect, the run finished.
    let recorded = world.recorded_tool_results().await;
    assert_eq!(recorded.len(), 1, "{recorded:?}");
    assert_eq!(
        recorded[0].content,
        AgentTaskContent::inline(json!({ "q": "refunds" })).expect("inline"),
        "the run recorded the server's structured answer"
    );
    assert_eq!(
        recorded[0].tool.as_ref().map(AgentToolId::as_str),
        Some(ECHO)
    );
    assert_eq!(recorded[0].effect_id.as_ref(), Some(&effect.effect_id));
    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);

    // The fallback never saw the call.
    assert!(
        fx.tools.invocations().is_empty(),
        "the router's fallback — the fixture's recording executor — was not reached"
    );
}

// ---------------------------------------------------------------------------
// 2. The credential is on the wire and on no record.
// ---------------------------------------------------------------------------

/// The same walk, swept: every durable surface — the run, the task, the
/// agent, the task history, the workflow outbox and the dispatcher fleet index,
/// and the session memory the result was recorded to — and every telemetry
/// surface the fixture wires (metrics, the entity's and the dispatcher's
/// segments, structured logs) carry no trace of the resolved credential.
#[tokio::test]
async fn the_credential_never_reaches_a_durable_record_or_the_fleet_index() {
    let logs = CapturingSubscriber::install_global();
    let world = McpWorld::build(
        AgentEffectSafetyClass::Idempotent,
        Arc::new(McpAllowAllEgress),
    )
    .await;
    let metrics = Arc::new(rakka_core::InMemoryMetricsRecorder::new());
    let segments = Arc::new(InMemoryAgentSegmentSink::new());
    let mut fx = world
        .fixture(ECHO, json!({ "q": "refunds" }))
        .with_segments(segments.clone())
        .with_dispatch_segments(segments.clone());
    fx.fx = fx.fx.with_metrics(metrics.clone());

    let _effect = start_and_commit_the_tool_call(&fx).await;
    let pass = fx.one_pass().await;
    assert_eq!(
        pass.invoked, 1,
        "the pass dispatched the tool call: {pass:?}"
    );

    // Positive control: the credential really was resolved and sent, so
    // "absent everywhere" is not true merely because nothing had it.
    assert_eq!(resolutions(&fx), 1);
    let seen = world.endpoint.server.seen_calls();
    assert!(
        seen.iter().any(|call| call
            .authorization
            .as_deref()
            .is_some_and(|value| value.ends_with(SENTINEL))),
        "the fake never saw the credential, so this sweep proves nothing: {seen:?}"
    );

    // Swept right after the credentialed attempt, when its outbox row and
    // fleet entry were last written — and both substrate records exist, so
    // sweeping them is not sweeping nothing.
    let mid_run = fx.durable_surfaces().await;
    assert_surface_holds(&mid_run, "workflow", "effect-dispatch/");
    assert_surface_holds(&mid_run, "fleet", "effect-dispatch/");
    assert_absent_from(&mid_run, SENTINEL);

    // And once the run has finished: every surface again, plus the session
    // memory and the telemetry.
    fx.pump().await;
    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
    let mut surfaces: Vec<(&str, String)> = fx.durable_surfaces().await;
    surfaces.push((
        "session-memory",
        serde_json::to_string(&world.session_entries().await).expect("the entries serialize"),
    ));
    // The scanner can see what a record really holds: the tool's answer is
    // durable content, recorded to the session.
    assert_surface_holds(&surfaces, "session-memory", "refunds");
    assert_surface_holds(&surfaces, "runs", "found");
    assert!(
        !metrics.snapshot().observations().is_empty(),
        "no metric was recorded, so the metrics sweep is empty"
    );
    surfaces.extend(fx.telemetry_surfaces(&metrics));
    let recorded_segments = segments.segments();
    assert!(
        !recorded_segments.is_empty(),
        "no segment was recorded, so the segment sweep is empty"
    );
    surfaces.push(("segments", format!("{recorded_segments:?}")));
    let events = logs.events();
    assert!(
        !events.is_empty(),
        "no structured log was captured, so the log sweep cannot tell a clean surface from \
         an absent one"
    );
    surfaces.push(("logs", events.join("\n")));

    assert_absent_from(&surfaces, SENTINEL);
}

// ---------------------------------------------------------------------------
// 3. The egress refusal, as the dispatcher classifies it.
// ---------------------------------------------------------------------------

/// The deployment's egress rule refuses the server inside the attempt: the
/// dispatcher had already resolved the credential, the executor refused before
/// reading it, and nothing reached the server.
///
/// The classification is the dispatcher's rule for an executor `Err`, not its
/// crash-recovery table: `dispatch.rs` records the attempt as failed against
/// the outbox's aligned budget (`record_attempt_failure`) — an `Err` that came
/// *back* is not an ambiguous loss, so no class parks it `Indeterminate`. A
/// `NonIdempotent` declaration's validated single-attempt budget then exhausts
/// on that first failure (the module doc's "a `NonIdempotent` row is never
/// retry-scheduled"): the generation resolves `Exhausted`, and the run fails
/// on it.
///
/// Where each code lands: the dispatcher persists the pipeline code
/// `dispatch-collaborator-failed` as the stable code — the effect's
/// `last_error_code`, the run's terminal reason — and the deployment's own
/// `egress-denied-by-policy` rides the bounded detail on the outbox row and
/// the fleet index.
#[tokio::test]
async fn an_egress_refusal_fails_the_attempt_under_the_deployments_code_after_the_credential_was_resolved_and_dropped(
) {
    let world = McpWorld::build(AgentEffectSafetyClass::NonIdempotent, Arc::new(RefuseAll)).await;
    let fx = world.fixture(ECHO, json!({ "q": "refunds" }));

    let committed = start_and_commit_the_tool_call(&fx).await;
    assert_eq!(
        committed.safety.class(),
        AgentEffectSafetyClass::NonIdempotent
    );
    assert_eq!(committed.max_attempts, 1);
    let pass = fx.one_pass().await;
    assert_eq!(
        (pass.invoked, pass.failed_attempts),
        (1, 1),
        "the dispatcher invoked the executor once and recorded the attempt failed: {pass:?}"
    );
    fx.pump().await;

    // The credential was resolved — by the dispatcher, before the executor
    // ran — and the refused attempt reached no server.
    assert_eq!(
        resolutions(&fx),
        1,
        "the dispatcher resolved the binding inside the attempt"
    );
    assert!(
        world.endpoint.server.seen_calls().is_empty(),
        "the refused attempt made no call"
    );
    assert_eq!(world.endpoint.server.call_count(), 0);
    assert_eq!(
        world.endpoint.server.list_calls(),
        1,
        "the publish-time sync listed once; the refused attempt never reached the recheck"
    );

    // One attempt, exhausted: the `NonIdempotent` budget. A failed run keeps
    // its failed effect on the loop, so the record is still there to read.
    let effect = fx.effect_at(TOOL_SLOT).await.expect("the tool effect");
    assert_eq!(effect.effect_id, committed.effect_id);
    assert_eq!(
        effect.status,
        AgentRunEffectStatus::Exhausted,
        "an executor error spends the attempt; the single-attempt budget exhausts"
    );
    assert_eq!(effect.last_error_code.as_deref(), Some(COLLABORATOR_FAILED));

    let row = fx
        .outbox_row(&effect)
        .await
        .expect("the ticket's outbox row");
    assert_eq!(row.status(), OutboxStatus::Exhausted);
    assert_eq!(
        row.attempts().attempts(),
        1,
        "exactly one attempt was spent"
    );
    let last_error = row
        .attempts()
        .last_error()
        .expect("the failed attempt recorded its detail");
    assert!(
        last_error.starts_with(&format!("{COLLABORATOR_FAILED}: ")),
        "the row's line leads with the pipeline code: {last_error}"
    );
    assert!(
        last_error.contains(EGRESS_DENIED),
        "the row's detail carries the deployment's own code: {last_error}"
    );
    let surfaces = fx.durable_surfaces().await;
    assert_surface_holds(&surfaces, "fleet", EGRESS_DENIED);

    // The run failed on the exhausted effect.
    assert_eq!(fx.terminal_failure_code().await, COLLABORATOR_FAILED);
    assert!(
        world.recorded_tool_results().await.is_empty(),
        "no result was recorded"
    );
    assert!(
        fx.tools.invocations().is_empty(),
        "the fallback was not reached"
    );

    // And the failure path, where application strings reach durable records,
    // carries the credential nowhere.
    assert_absent_from(&surfaces, SENTINEL);
}

// ---------------------------------------------------------------------------
// 4. The artifact overflow.
// ---------------------------------------------------------------------------

/// A result too large to keep inline, under an `ArtifactReference` policy,
/// reaches the deployment's artifact store; the run records the reference, and
/// the bytes stay out of every durable record.
#[tokio::test]
async fn a_large_result_reaches_the_artifact_store_and_the_run_records_the_reference() {
    let world = McpWorld::build(
        AgentEffectSafetyClass::Idempotent,
        Arc::new(McpAllowAllEgress),
    )
    .await;
    let fx = world.fixture(BIG, json!({}));

    let effect = start_and_commit_the_tool_call(&fx).await;
    let pass = fx.one_pass().await;
    assert_eq!(
        pass.invoked, 1,
        "the pass dispatched the tool call: {pass:?}"
    );
    fx.pump().await;
    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);

    let recorded = world.recorded_tool_results().await;
    assert_eq!(recorded.len(), 1, "{recorded:?}");
    assert_eq!(recorded[0].effect_id.as_ref(), Some(&effect.effect_id));
    let AgentTaskContent::Artifact(reference) = &recorded[0].content else {
        panic!(
            "the run recorded inline content for an oversized result: {:?}",
            recorded[0].content
        )
    };
    assert_eq!(
        reference.artifact_id,
        format!(
            "mcp-{}-g{}-call-1",
            effect.effect_id,
            effect.generation.get()
        ),
        "the artifact id derives from the durable effect, its generation, and the call"
    );
    validate_artifact_ref(reference).unwrap_or_else(|error| {
        panic!("the recorded reference passes the workflow's own validation: {error}")
    });

    assert_eq!(world.artifacts.len().await, 1, "one artifact, written once");
    let stored = world
        .artifacts
        .bytes(&reference.artifact_id)
        .await
        .expect("the store holds the artifact the run recorded");
    assert!(stored.len() > BIG_RESULT_BYTES, "{} bytes", stored.len());
    assert_eq!(
        reference.checksum,
        Some(format!(
            "sha256:{}",
            AgentContentDigest::sha256_of_bytes(&stored).value
        )),
        "the recorded reference's checksum is the stored bytes'"
    );

    // The bytes live in the store, not in state: no durable record holds even
    // the inline bound's worth of the answer, while the session does hold
    // the reference.
    let mut surfaces = fx.durable_surfaces().await;
    surfaces.push((
        "session-memory",
        serde_json::to_string(&world.session_entries().await).expect("the entries serialize"),
    ));
    assert_surface_holds(&surfaces, "session-memory", &reference.artifact_id);
    assert_absent_from(&surfaces, &"x".repeat(MCP_INLINE_RESULT_MAX_BYTES));
}

// ---------------------------------------------------------------------------
// 5. The MCP crate's own types carry references, never material.
// ---------------------------------------------------------------------------

/// The records a deployment persists or logs from `rakka-agent-mcp` — the
/// operator's binding, the stored descriptor set, the bindings registered
/// from it, and the executor's `Debug` — carry the credential *reference* and
/// never the credential, even after the sync was handed one and the executor
/// sent one.
#[tokio::test]
async fn the_secret_exclusion_scan_covers_the_mcp_types() {
    let sync_credential = AgentEphemeralCredential::bearer_token(SENTINEL);
    let world = McpWorld::build_with_sync_credential(
        AgentEffectSafetyClass::Idempotent,
        Arc::new(McpAllowAllEgress),
        Some(&sync_credential),
    )
    .await;
    // Positive control for the sync: the publish step really sent the
    // credential, so a stored set without it is one that dropped it rather
    // than one that never had it. Read before the run, whose own requests
    // replace the fake's last-request headers.
    let sync_headers = world.endpoint.server.seen_headers();
    assert!(
        sync_headers.iter().any(|(name, value)| {
            name.eq_ignore_ascii_case("authorization") && value.ends_with(SENTINEL)
        }),
        "the publish-time sync never sent the credential, so the stored set's \
         absence of it proves nothing: {sync_headers:?}"
    );
    let fx = world.fixture(ECHO, json!({ "q": "refunds" }));
    fx.start().await;
    fx.pump().await;

    // Positive control: the executor these types describe really did carry
    // the credential to the server.
    let seen = world.endpoint.server.seen_calls();
    assert_eq!(seen.len(), 1, "{seen:?}");
    assert!(
        seen[0]
            .authorization
            .as_deref()
            .is_some_and(|value| value.ends_with(SENTINEL)),
        "the executor never sent the credential, so this scan proves nothing"
    );

    let registered: Vec<_> = world.set.bindings().collect();
    let records: Vec<(&str, Value)> = vec![
        (
            "server-binding",
            serde_json::to_value(&world.binding).expect("the binding serializes"),
        ),
        (
            "descriptor-set",
            serde_json::to_value(&world.set).expect("the set serializes"),
        ),
        (
            "registered-bindings",
            serde_json::to_value(&registered).expect("the bindings serialize"),
        ),
    ];
    let mut surfaces: Vec<(&str, String)> = records
        .iter()
        .map(|(label, value)| (*label, value.to_string()))
        .collect();
    let executor_debug = format!("{:?}", world.executor);
    surfaces.push(("executor-debug", executor_debug.clone()));

    assert_absent_from(&surfaces, SENTINEL);

    // No record grew a secret-shaped field …
    for (label, value) in &records {
        let mut keys = Vec::new();
        object_keys(value, &mut keys);
        assert!(!keys.is_empty(), "{label} has no fields to check");
        for key in keys {
            assert!(
                !is_secret_shaped(&key),
                "{label} carries a secret-shaped key {key:?}: {value}"
            );
        }
    }
    // … and the executor's `Debug` names none, nor the endpoint it dials.
    let lowered = executor_debug.to_ascii_lowercase();
    for shaped in SECRET_SHAPED_KEYS {
        assert!(
            !lowered.contains(shaped),
            "the executor's Debug mentions {shaped:?}: {executor_debug}"
        );
    }
    assert!(
        !executor_debug.contains(&world.endpoint.url),
        "the executor's Debug leaks the endpoint: {executor_debug}"
    );

    // What the crate does carry is the reference, by name.
    assert_eq!(
        records[0].1["credential_binding"],
        json!(CREDENTIAL),
        "the server binding carries the logical reference"
    );
    assert_eq!(registered.len(), 2);
    for binding in &registered {
        assert_eq!(
            binding.declaration().credential_binding,
            Some(credential_binding()),
            "every registered tool carries the reference it resolves from"
        );
    }
}

// ---------------------------------------------------------------------------
// 6. A credential binding named on the server alone.
// ---------------------------------------------------------------------------

/// The operator names the credential on the server binding and on no tool:
/// the publish-time sync copies it into each synced declaration, so the
/// committed effect names it, the dispatcher resolves it, and it reaches the
/// wire exactly as a tool-level binding would.
#[tokio::test]
async fn a_server_level_credential_binding_alone_reaches_the_wire_through_the_dispatcher() {
    let world = McpWorld::assemble(
        fake_server(),
        |url| server_level_binding(url, AgentEffectSafetyClass::Idempotent),
        Arc::new(McpAllowAllEgress),
        None,
    )
    .await;
    assert!(
        world
            .binding
            .tools
            .values()
            .all(|policy| policy.declaration.credential_binding.is_none()),
        "no tool declaration names the credential; only the server does"
    );
    assert_eq!(world.binding.credential_binding, Some(credential_binding()));
    let fx = world.fixture(ECHO, json!({ "q": "refunds" }));

    let effect = start_and_commit_the_tool_call(&fx).await;
    assert_eq!(
        effect.credential_binding,
        Some(credential_binding()),
        "the effect names the server-level binding the sync carried into the declaration"
    );
    let pass = fx.one_pass().await;
    assert_eq!(
        pass.invoked, 1,
        "the pass dispatched the tool call: {pass:?}"
    );
    fx.pump().await;

    assert_eq!(
        resolutions(&fx),
        1,
        "the dispatcher resolved the server-level binding inside the attempt"
    );
    let seen = world.endpoint.server.seen_calls();
    assert_eq!(seen.len(), 1, "{seen:?}");
    assert!(
        seen[0]
            .authorization
            .as_deref()
            .is_some_and(|value| value.ends_with(SENTINEL)),
        "the resolved credential reached the wire: {:?}",
        seen[0].authorization
    );
    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
}

// ---------------------------------------------------------------------------
// 7. A server that echoes the credential into its error text.
// ---------------------------------------------------------------------------

/// The server answers `isError` with text that quotes the bearer token the
/// dispatcher resolved and the executor sent. That text is the one piece of
/// server-chosen content the dispatcher persists on a failed attempt — onto
/// the outbox row and across the fleet index — so the executor scrubs the
/// credential from it before the bound cuts it, and the persisted line
/// carries `<redacted>` where the token was.
#[tokio::test]
async fn a_credential_the_server_echoes_into_its_error_text_is_redacted_before_it_is_persisted() {
    let world = McpWorld::assemble(
        leaking_fake_server(),
        |url| leaking_binding(url, AgentEffectSafetyClass::NonIdempotent),
        Arc::new(McpAllowAllEgress),
        None,
    )
    .await;
    let fx = world.fixture_minting(LEAK, json!({}), ECHOED);

    start_and_commit_the_tool_call(&fx).await;
    let pass = fx.one_pass().await;
    assert_eq!(
        (pass.invoked, pass.failed_attempts),
        (1, 1),
        "the dispatcher invoked the executor once and recorded the attempt failed: {pass:?}"
    );
    fx.pump().await;

    // Positive control: the server really was sent the credential, and really
    // did answer with it.
    let seen = world.endpoint.server.seen_calls();
    assert_eq!(seen.len(), 1, "{seen:?}");
    assert!(
        seen[0]
            .authorization
            .as_deref()
            .is_some_and(|value| value.ends_with(ECHOED)),
        "the server never received the credential, so there was nothing to echo"
    );

    let effect = fx.effect_at(TOOL_SLOT).await.expect("the tool effect");
    assert_eq!(effect.status, AgentRunEffectStatus::Exhausted);
    assert_eq!(effect.last_error_code.as_deref(), Some(COLLABORATOR_FAILED));
    let row = fx
        .outbox_row(&effect)
        .await
        .expect("the ticket's outbox row");
    let last_error = row
        .attempts()
        .last_error()
        .expect("the failed attempt recorded its detail");
    assert!(
        last_error.contains("mcp-tool-error")
            && last_error.contains("denied: bearer <redacted> is revoked"),
        "the persisted line keeps the server's text with the token scrubbed: {last_error}"
    );
    assert!(!last_error.contains(ECHOED), "{last_error}");

    let surfaces = fx.durable_surfaces().await;
    assert_surface_holds(&surfaces, "fleet", "<redacted>");
    assert_absent_from(&surfaces, ECHOED);
}
