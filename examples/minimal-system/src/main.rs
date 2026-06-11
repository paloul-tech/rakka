#![forbid(unsafe_code)]

//! Minimal executable that verifies the Phase 2 actor facade links together.

use std::time::Duration;

use rakka::prelude::*;

enum EchoMessage {
    Ping { reply_to: ReplyTo<&'static str> },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing::info!(
        target: rakka::actor::telemetry::TRACE_TARGET_PREFIX,
        runtime = rakka::runtime_name(),
        "minimal Rakka actor example started"
    );

    let system = ActorSystem::new("minimal");
    let echo = system.spawn(
        "echo",
        actor_fn(
            |_ctx: &mut ActorContext<EchoMessage>, msg: EchoMessage| match msg {
                EchoMessage::Ping { reply_to } => {
                    let _ = reply_to.reply("pong");
                    Ok(ActorAction::Continue)
                }
            },
        ),
    )?;
    let reply = echo
        .ask(
            |reply_to| EchoMessage::Ping { reply_to },
            Duration::from_secs(1),
        )
        .await?;

    println!(
        "Rakka Phase 2 actor facade replied with {reply} on {}.",
        rakka::runtime_name()
    );

    system.terminate().await?;
    Ok(())
}
