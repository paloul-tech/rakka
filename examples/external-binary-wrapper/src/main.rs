#![forbid(unsafe_code)]

//! Self-contained example that wraps a line-json child process as a Rakka service.

use std::io::{BufRead, Write};
use std::time::Duration;

use rakka_core::ActorSystem;
use rakka_process::{
    spawn_stdio_actor, ExecutableAllowlist, LineJsonCodec, ProcessSpec, ProcessStdio, StdioCommand,
    StdioProtocolConfig,
};
use serde::{Deserialize, Serialize};

const CHILD_FLAG: &str = "--legacy-child";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LegacyRequest {
    command: String,
    value: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LegacyReply {
    service: String,
    result: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::args().any(|arg| arg == CHILD_FLAG) {
        run_legacy_child()?;
        return Ok(());
    }

    let executable = std::env::current_exe()?;
    let allowlist = ExecutableAllowlist::from_exact_paths([executable.clone()]);
    let spec = ProcessSpec::new(executable)
        .arg(CHILD_FLAG)
        .stdin(ProcessStdio::Piped)
        .stdout(ProcessStdio::Piped)
        .stderr(ProcessStdio::Piped)
        .shutdown_timeout(Duration::from_secs(1));

    let system = ActorSystem::new("external-binary-wrapper");
    let legacy = spawn_stdio_actor(
        &system,
        "legacy-calculator",
        spec,
        allowlist,
        LineJsonCodec::<LegacyRequest, LegacyReply>::new(),
        StdioProtocolConfig::new().default_request_timeout(Duration::from_secs(1)),
    )?;

    let reply = legacy
        .ask(
            |reply_to| StdioCommand::Request {
                request: LegacyRequest {
                    command: "increment".to_string(),
                    value: 41,
                },
                reply_to,
            },
            Duration::from_secs(2),
        )
        .await??;
    let stderr = legacy
        .ask(
            |reply_to| StdioCommand::Stderr { reply_to },
            Duration::from_secs(2),
        )
        .await?;

    println!(
        "Rakka wrapped {} and received result {}.",
        reply.service, reply.result
    );
    println!("Captured child stderr: {stderr:?}");

    legacy.stop()?;
    system.shutdown();
    Ok(())
}

fn run_legacy_child() -> Result<(), Box<dyn std::error::Error>> {
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    for line in stdin.lock().lines() {
        let line = line?;
        let frame: serde_json::Value = serde_json::from_str(&line)?;
        let request_id = frame
            .get("id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| std::io::Error::other("line-json request is missing id"))?;
        let request: LegacyRequest = serde_json::from_value(
            frame
                .get("payload")
                .cloned()
                .ok_or_else(|| std::io::Error::other("line-json request is missing payload"))?,
        )?;
        eprintln!("legacy child handled {}", request.command);
        let response = serde_json::json!({
            "id": request_id,
            "payload": {
                "service": "legacy-calculator",
                "result": request.value + 1,
            },
        });
        writeln!(stdout, "{response}")?;
        stdout.flush()?;
    }
    Ok(())
}
