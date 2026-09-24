//! The unsandboxed reference launcher, feature `child-process`: it reads the
//! spec artifact and spawns the command. Proven with `/bin/cat`, which echoes
//! the client's first request back. rmcp reads the echoed `server/discover`
//! as a request *from* the server — `ServerRequest` ends in a catch-all
//! `CustomRequest` (`rmcp-3.4.0/src/model.rs:4604`–`4609`) — and ignores it
//! as an unexpected pre-handshake message
//! (`rmcp-3.4.0/src/service/client.rs:239`–`240`), so the client never hears
//! an answer and no refusal code exists to assert. What matters is that a
//! process was launched from the artifact, that the caller's deadline bounds
//! the handshake, and that the process dies with the transport the deadline
//! drops — proven strictly with `/bin/sleep`, which never reads its input and
//! so cannot be ending on its own at end of input, as `cat` does.
//!
//! Around that, with `/bin/sh` scripts standing in for servers: a peer that
//! refuses the handshake reads back bounded and body-free, the child sees only
//! the environment its specification names, and every refusal of the
//! specification itself names no command, argument, or path.
#![cfg(all(unix, feature = "child-process", feature = "testkit"))]

use std::time::Duration;

use rakka_agent::{AgentEffectSafetyClass, AgentToolDeclaration};
use rakka_agent_mcp::client::connect_over;
use rakka_agent_mcp::{
    mcp_artifact_store, McpChildProcessLauncher, McpChildTransport, McpServerBinding, McpServerId,
    McpToolPolicy, TokioChildProcessLauncher,
};
use serde_json::{json, Value};

mod support;
use support::{run_scope, spec_artifact_ref, SharedArtifactStore};

/// The bound every handshake here runs under.
const HANDSHAKE_BOUND: Duration = Duration::from_secs(2);

/// Bytes a refusal may never exceed.
const MESSAGE_BOUND: usize = 512;

fn binding() -> McpServerBinding {
    McpServerBinding::child_process(McpServerId::new("local").expect("id"), spec_artifact_ref())
        .with_tool(
            "echo",
            McpToolPolicy::new(AgentToolDeclaration::new(AgentEffectSafetyClass::ReadOnly)),
        )
        .expect("t")
}

/// A launcher over a store that holds `spec` at `spec-1`.
async fn launcher_with(spec: &Value) -> TokioChildProcessLauncher {
    let store = SharedArtifactStore::default();
    store
        .insert(
            spec_artifact_ref(),
            serde_json::to_vec(spec).expect("the spec encodes"),
        )
        .await;
    TokioChildProcessLauncher::new(mcp_artifact_store(store))
}

/// A server played by `/bin/sh`: reads the client's first request, then runs
/// `answer`.
fn scripted(answer: &str, env: &Value) -> Value {
    json!({
        "command": "/bin/sh",
        "args": ["-c", format!("read -r request; {answer}")],
        "env": env,
    })
}

/// Whether `pid` still names a process, asked of `/bin/sh`'s own `kill -0`.
///
/// An exited child nobody reaped still answers — it is a zombie — so a `false`
/// here means killed *and* waited for, which is what rmcp's drop does.
async fn alive(pid: u32) -> bool {
    tokio::process::Command::new("/bin/sh")
        .args(["-c", &format!("kill -0 {pid} 2>/dev/null")])
        .status()
        .await
        .expect("the shell runs")
        .success()
}

/// The launched process's id, read off rmcp's process transport.
fn pid_of(transport: &McpChildTransport) -> u32 {
    match transport {
        McpChildTransport::Process(process) => process.id().expect("the process is running"),
        other => panic!("the reference launcher hands back rmcp's process transport: {other:?}"),
    }
}

/// Waits, boundedly, for `pid` to be gone.
async fn gone_within(pid: u32, bound: Duration) {
    tokio::time::timeout(bound, async {
        while alive(pid).await {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("the launched process dies with its dropped transport");
}

#[tokio::test]
async fn the_reference_launcher_spawns_the_artifacts_command_and_it_dies_with_the_transport() {
    let launcher = launcher_with(&json!({"command": "/bin/cat", "args": []})).await;
    let transport = launcher
        .launch(&run_scope(), &spec_artifact_ref())
        .await
        .expect("the specification launches");
    let pid = pid_of(&transport);
    assert!(alive(pid).await, "a process was launched from the artifact");

    let outcome = tokio::time::timeout(HANDSHAKE_BOUND, connect_over(transport, &binding())).await;
    match outcome {
        Err(_elapsed) => {}
        Ok(Ok(_)) => panic!("an echo is not an MCP server"),
        Ok(Err(error)) => panic!("expected the echo to go unanswered, got {error}"),
    }

    // The deadline dropped the handshake — and the transport inside it —
    // with no close. The process must not outlive it.
    gone_within(pid, Duration::from_secs(5)).await;
}

#[tokio::test]
async fn a_child_that_never_reads_its_input_is_killed_when_the_transport_drops() {
    let launcher = launcher_with(&json!({"command": "/bin/sleep", "args": ["30"]})).await;
    let transport = launcher
        .launch(&run_scope(), &spec_artifact_ref())
        .await
        .expect("the specification launches");
    let pid = pid_of(&transport);
    assert!(alive(pid).await, "a process was launched from the artifact");
    let outcome = tokio::time::timeout(
        Duration::from_millis(300),
        connect_over(transport, &binding()),
    )
    .await;
    assert!(outcome.is_err(), "a sleeping child never answers");
    // `sleep` never sees end of input, so nothing but a kill ends it before
    // its thirty seconds are up.
    gone_within(pid, Duration::from_secs(5)).await;
}

#[tokio::test]
async fn a_peer_that_refuses_the_handshake_reads_back_bounded_and_body_free() {
    let answer = r#"printf '%s\n' '{"jsonrpc":"2.0","error":{"code":4001,"message":"/bin/sh {\"body\":\"body-sentinel\"}","data":{"body":"body-sentinel"}}}'"#;
    let launcher = launcher_with(&scripted(answer, &json!({}))).await;
    let transport = launcher
        .launch(&run_scope(), &spec_artifact_ref())
        .await
        .expect("the specification launches");
    let error =
        match tokio::time::timeout(HANDSHAKE_BOUND, connect_over(transport, &binding())).await {
            Ok(Err(error)) => error,
            Ok(Ok(_)) => panic!("a refused handshake opened no session"),
            Err(_) => panic!("a refused handshake answers inside the bound"),
        };
    assert_eq!(error.code(), "mcp-transport-failed");
    let message = error.to_string();
    assert!(message.len() < MESSAGE_BOUND, "{message}");
    assert!(message.contains("JsonRpcError(4001)"), "{message}");
    for leaked in ["/bin/sh", "body-sentinel", "{", "jsonrpc"] {
        assert!(!message.contains(leaked), "{leaked} leaked: {message}");
    }
}

#[tokio::test]
async fn the_child_sees_only_the_environment_its_specification_names() {
    // The proof is only as good as the variable it looks for: cargo sets this
    // one in every test process, so a child that inherited this process's
    // environment would see it.
    assert!(
        std::env::var_os("CARGO_MANIFEST_DIR").is_some(),
        "the test process carries the variable the child must not see"
    );
    let answer = r#"if [ -z "$CARGO_MANIFEST_DIR" ] && [ "$ONLY" = "given" ]; then code=4001; else code=4002; fi; printf '{"jsonrpc":"2.0","error":{"code":%s,"message":"env"}}\n' "$code""#;
    let launcher = launcher_with(&scripted(answer, &json!({"ONLY": "given"}))).await;
    let transport = launcher
        .launch(&run_scope(), &spec_artifact_ref())
        .await
        .expect("the specification launches");
    let error =
        match tokio::time::timeout(HANDSHAKE_BOUND, connect_over(transport, &binding())).await {
            Ok(Err(error)) => error,
            Ok(Ok(_)) => panic!("a refused handshake opened no session"),
            Err(_) => panic!("a refused handshake answers inside the bound"),
        };
    assert!(
        error.to_string().contains("JsonRpcError(4001)"),
        "the child saw an environment other than its specification's: {error}"
    );
}

#[tokio::test]
async fn a_specification_refusal_names_no_command_argument_or_path() {
    let missing =
        TokioChildProcessLauncher::new(mcp_artifact_store(SharedArtifactStore::default()))
            .launch(&run_scope(), &spec_artifact_ref())
            .await
            .expect_err("no specification is stored");
    assert_eq!(missing.code(), "mcp-transport-unsupported");
    assert!(
        missing.to_string().contains("artifact-not-found"),
        "{missing}"
    );

    for (spec, expected) in [
        // serde's own message for this one quotes the value it met.
        (
            json!({"command": "/bin/cat", "args": "/opt/secret-sentinel"}),
            "not a { command, args, env } object",
        ),
        (
            json!({"command": "/bin/cat", "cwd": "/opt/secret-sentinel"}),
            "not a { command, args, env } object",
        ),
        (json!({"command": ""}), "names no command"),
        (
            json!({"command": "/opt/secret-sentinel/server", "args": ["--token=secret-sentinel"]}),
            "could not be started",
        ),
    ] {
        let error = launcher_with(&spec)
            .await
            .launch(&run_scope(), &spec_artifact_ref())
            .await
            .expect_err("the specification is refused");
        let message = error.to_string();
        assert_eq!(error.code(), "mcp-transport-unsupported", "{message}");
        assert!(message.contains(expected), "{message}");
        assert!(message.len() < MESSAGE_BOUND, "{message}");
        assert!(!message.contains("secret-sentinel"), "{message}");
    }
}
