//! Condition (b): a child-process binding is refused at construction without
//! a launcher, and runs through a launcher-produced transport with one — here
//! an in-memory duplex pair to an rmcp server running in the same process, so
//! no process is spawned and no sandbox is assumed.
//!
//! Around that: a resolved credential is refused before the launcher is ever
//! asked (a stdio child has no header to carry it), a launcher's own refusal
//! reads back under the transport-unsupported code, an HTTP binding is never
//! synced over a launched transport, and an effect deadline that fires
//! mid-launch or mid-handshake drops the launch future or the launched
//! transport — which is what obliges a launcher's process to die with either.
//!
//! The whole file is gated: it drives the in-process fake server, which only
//! exists under `testkit`.
#![cfg(feature = "testkit")]

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rakka_agent::{
    AgentDispatchError, AgentDispatchToolExecutor, AgentEffectSafetyClass, AgentRunScope,
    AgentTaskContent, AgentToolCallId, AgentToolCallRequest, AgentToolDeclaration, AgentToolId,
};
use rakka_agent_mcp::testkit::{
    hardened_reqwest_client, FakeMcpServer, FakeTool, FakeToolBehaviour, ReqwestClient,
};
use rakka_agent_mcp::{
    mcp_artifact_store, sync_mcp_descriptors_over, McpAllowAllEgress, McpChildProcessLauncher,
    McpChildTransport, McpDescriptorSet, McpDispatchToolExecutor, McpLaunchError, McpLaunchFuture,
    McpServerBinding, McpServerId, McpToolPolicy,
};
use rakka_agent_workflow::{AgentEphemeralCredential, AgentTimestampMillis, ArtifactRef};
use serde_json::json;
use tokio::io::AsyncReadExt;

mod support;
use support::{
    duplex_transport, run_scope, spec_artifact_ref, stored_set_for, tool_intent_with_timeout,
    SharedArtifactStore,
};

/// What every elapsed deadline reads as, wherever in the attempt it fired.
const ATTEMPT_TIMED_OUT: &str = "the attempt exceeded the effect's timeout";

/// Serves the fake over an in-memory pair, and counts how often it was asked.
struct DuplexLauncher {
    server: FakeMcpServer,
    launches: AtomicUsize,
}

impl DuplexLauncher {
    fn new(server: FakeMcpServer) -> Arc<Self> {
        Arc::new(Self {
            server,
            launches: AtomicUsize::new(0),
        })
    }

    fn launches(&self) -> usize {
        self.launches.load(Ordering::SeqCst)
    }
}

impl McpChildProcessLauncher for DuplexLauncher {
    fn launch<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _spec: &'a ArtifactRef,
    ) -> McpLaunchFuture<'a, McpChildTransport> {
        self.launches.fetch_add(1, Ordering::SeqCst);
        let server = self.server.clone();
        Box::pin(async move { Ok(duplex_transport(server)) })
    }
}

/// A launcher whose spawn always fails, as a sandbox that refused the command
/// would.
struct RefusingLauncher;

impl McpChildProcessLauncher for RefusingLauncher {
    fn launch<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _spec: &'a ArtifactRef,
    ) -> McpLaunchFuture<'a, McpChildTransport> {
        Box::pin(async {
            Err(McpLaunchError::SpawnFailed {
                reason: "entity not found".to_string(),
            })
        })
    }
}

/// A launcher whose child never says a word, and which records when the
/// client side of the pair it handed out is dropped: the far end reads EOF
/// once, and only once, the client's writer is gone.
struct MuteLauncher {
    dropped: Arc<AtomicBool>,
}

impl McpChildProcessLauncher for MuteLauncher {
    fn launch<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _spec: &'a ArtifactRef,
    ) -> McpLaunchFuture<'a, McpChildTransport> {
        let dropped = Arc::clone(&self.dropped);
        Box::pin(async move {
            let (client_side, mut child_side) = tokio::io::duplex(64 * 1024);
            tokio::spawn(async move {
                let mut sink = [0_u8; 1024];
                // Read and discard until EOF; never answer.
                while let Ok(read) = child_side.read(&mut sink).await {
                    if read == 0 {
                        break;
                    }
                }
                dropped.store(true, Ordering::SeqCst);
            });
            let (read, write) = tokio::io::split(client_side);
            Ok(McpChildTransport::new(Box::new(read), Box::new(write)))
        })
    }
}

/// Sets its flag when dropped.
struct DropFlag(Arc<AtomicBool>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

/// A launcher that never finishes launching — a sandbox still waiting on a
/// readiness signal — and records when its launch future is dropped.
struct StalledLauncher {
    dropped: Arc<AtomicBool>,
}

impl McpChildProcessLauncher for StalledLauncher {
    fn launch<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _spec: &'a ArtifactRef,
    ) -> McpLaunchFuture<'a, McpChildTransport> {
        // Armed before the first await, as the trait requires of a real
        // process: whatever drops the future drops this with it.
        let armed = DropFlag(Arc::clone(&self.dropped));
        Box::pin(async move {
            let _armed = armed;
            std::future::pending().await
        })
    }
}

fn fake() -> FakeMcpServer {
    FakeMcpServer::new().with_tool(FakeTool::new(
        "echo",
        "Echoes.",
        json!({"type":"object"}),
        FakeToolBehaviour::Echo,
    ))
}

fn binding() -> McpServerBinding {
    McpServerBinding::child_process(McpServerId::new("local").expect("id"), spec_artifact_ref())
        .with_tool(
            "echo",
            McpToolPolicy::new(AgentToolDeclaration::new(AgentEffectSafetyClass::ReadOnly)),
        )
        .expect("t")
}

fn echo_call() -> AgentToolCallRequest {
    AgentToolCallRequest::new(
        AgentToolCallId::new("c").expect("id"),
        AgentToolId::new("mcp.local.echo").expect("id"),
        json!({"k": 1}),
    )
    .expect("call")
}

fn executor_with(
    set: McpDescriptorSet,
    launcher: Arc<dyn McpChildProcessLauncher>,
) -> McpDispatchToolExecutor<ReqwestClient> {
    McpDispatchToolExecutor::with_launcher(
        vec![set],
        vec![binding()],
        mcp_artifact_store(SharedArtifactStore::default()),
        hardened_reqwest_client(),
        Arc::new(McpAllowAllEgress),
        launcher,
    )
    .expect("builds with a launcher")
}

fn collaborator_code(error: &AgentDispatchError) -> &str {
    match error {
        AgentDispatchError::Collaborator { code, .. } => code,
        other => other.code(),
    }
}

#[tokio::test]
async fn a_child_process_binding_is_refused_without_a_launcher() {
    let server = fake();
    let set: McpDescriptorSet = stored_set_for(&binding(), &server).await;
    let error = McpDispatchToolExecutor::new(
        vec![set],
        vec![binding()],
        mcp_artifact_store(SharedArtifactStore::default()),
        hardened_reqwest_client(),
        Arc::new(McpAllowAllEgress),
    )
    .expect_err("no launcher");
    assert_eq!(error.code(), "mcp-transport-unsupported");
}

#[tokio::test]
async fn a_child_process_binding_runs_through_the_launchers_transport() {
    let server = fake();
    let set = stored_set_for(&binding(), &server).await;
    let launcher = DuplexLauncher::new(server.clone());
    let executor = executor_with(set, launcher.clone());
    let content = executor
        .execute(
            &run_scope(),
            &tool_intent_with_timeout("mcp.local.echo", Some(5_000)),
            &echo_call(),
            None,
        )
        .await
        .expect("answers over stdio");
    assert!(
        matches!(content, AgentTaskContent::Inline(ref v) if v["k"] == 1),
        "{content:?}"
    );
    assert_eq!(server.call_count(), 1);
    assert_eq!(launcher.launches(), 1, "one attempt, one launch");
}

#[tokio::test]
async fn a_credential_is_refused_before_the_launcher_is_asked() {
    // The binding names no credential — `validate` would refuse one — so this
    // is the credential a run's grant supplied, which only the dispatch-time
    // check can catch.
    let server = fake();
    let set = stored_set_for(&binding(), &server).await;
    let launcher = DuplexLauncher::new(server.clone());
    let executor = executor_with(set, launcher.clone());
    let credential = AgentEphemeralCredential::bearer_token("child-token-sentinel");
    let error = executor
        .execute(
            &run_scope(),
            &tool_intent_with_timeout("mcp.local.echo", Some(5_000)),
            &echo_call(),
            Some(&credential),
        )
        .await
        .expect_err("a stdio child has no header to carry a credential");
    assert_eq!(
        collaborator_code(&error),
        "mcp-credential-material-unsupported"
    );
    let message = error.to_string();
    assert!(message.contains("child-process"), "{message}");
    assert!(!message.contains("child-token-sentinel"), "{message}");
    assert_eq!(launcher.launches(), 0, "no process was asked for");
    assert_eq!(server.call_count(), 0);
}

#[tokio::test]
async fn a_launch_refusal_reads_back_as_transport_unsupported() {
    let server = fake();
    let set = stored_set_for(&binding(), &server).await;
    let executor = executor_with(set, Arc::new(RefusingLauncher));
    let error = executor
        .execute(
            &run_scope(),
            &tool_intent_with_timeout("mcp.local.echo", Some(5_000)),
            &echo_call(),
            None,
        )
        .await
        .expect_err("the launcher refused");
    assert_eq!(collaborator_code(&error), "mcp-transport-unsupported");
    assert!(
        error
            .to_string()
            .contains("the MCP child process could not be started"),
        "{error}"
    );
}

#[tokio::test]
async fn a_streamable_http_binding_is_never_synced_over_a_launched_transport() {
    // Its descriptors come from its URL, through the egress rule, or not at
    // all — never from whatever a launcher happened to connect.
    let server = fake();
    let http = McpServerBinding::streamable_http(
        McpServerId::new("local").expect("id"),
        "https://example.test/mcp",
    )
    .with_tool(
        "echo",
        McpToolPolicy::new(AgentToolDeclaration::new(AgentEffectSafetyClass::ReadOnly)),
    )
    .expect("t");
    let error = sync_mcp_descriptors_over(
        duplex_transport(server.clone()),
        &http,
        AgentTimestampMillis::new(1),
    )
    .await
    .expect_err("not a child-process binding");
    assert_eq!(error.code(), "mcp-descriptor-sync-failed");
    assert!(
        error.to_string().contains("names no child process"),
        "{error}"
    );
    assert_eq!(server.list_calls(), 0, "nothing was listed");
}

#[tokio::test]
async fn an_elapsed_deadline_mid_launch_cancels_the_launch_future() {
    let server = fake();
    let set = stored_set_for(&binding(), &server).await;
    let dropped = Arc::new(AtomicBool::new(false));
    let executor = executor_with(
        set,
        Arc::new(StalledLauncher {
            dropped: Arc::clone(&dropped),
        }),
    );
    let error = executor
        .execute(
            &run_scope(),
            &tool_intent_with_timeout("mcp.local.echo", Some(200)),
            &echo_call(),
            None,
        )
        .await
        .expect_err("a launch that never returns never reaches a handshake");
    assert_eq!(error.code(), "mcp-transport-failed");
    assert!(error.to_string().contains(ATTEMPT_TIMED_OUT), "{error}");
    // Dropped with the attempt, before any transport existed: this is the
    // case the trait's rule covers — a process spawned inside `launch` must
    // already be armed to die with this future.
    assert!(
        dropped.load(Ordering::SeqCst),
        "the deadline cancelled the launch itself"
    );
}

#[tokio::test]
async fn an_elapsed_deadline_mid_handshake_drops_the_launched_transport() {
    let server = fake();
    let set = stored_set_for(&binding(), &server).await;
    let dropped = Arc::new(AtomicBool::new(false));
    let executor = executor_with(
        set,
        Arc::new(MuteLauncher {
            dropped: Arc::clone(&dropped),
        }),
    );
    let error = executor
        .execute(
            &run_scope(),
            &tool_intent_with_timeout("mcp.local.echo", Some(200)),
            &echo_call(),
            None,
        )
        .await
        .expect_err("a mute child never finishes the handshake");
    assert_eq!(error.code(), "mcp-transport-failed");
    assert!(error.to_string().contains(ATTEMPT_TIMED_OUT), "{error}");
    // The pair was dropped with the abandoned handshake, not closed: the far
    // end sees EOF. A launcher's process must therefore not outlive it.
    tokio::time::timeout(Duration::from_secs(2), async {
        while !dropped.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the launched transport was dropped with the attempt");
}
