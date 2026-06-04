#![forbid(unsafe_code)]

//! Minimal executable that verifies the Phase 1 local actor kernel links together.

use std::time::Duration;

use rakka_core::{
    actor_future, Actor, ActorAction, ActorContext, ActorFuture, ActorSystem, ReplyTo,
};

enum EchoMessage {
    Ping { reply_to: ReplyTo<&'static str> },
}

struct EchoActor;

impl Actor for EchoActor {
    type Msg = EchoMessage;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        actor_future(async move {
            match msg {
                EchoMessage::Ping { reply_to } => {
                    let _ = reply_to.reply("pong");
                }
            }

            Ok(ActorAction::Continue)
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing::info!(
        target: rakka_core::telemetry::TRACE_TARGET_PREFIX,
        runtime = rakka_core::runtime_name(),
        "minimal Rakka actor example started"
    );

    let system = ActorSystem::new("minimal");
    let echo = system.spawn_actor("echo", EchoActor)?;
    let reply = echo
        .ask(
            |reply_to| EchoMessage::Ping { reply_to },
            Duration::from_secs(1),
        )
        .await?;

    println!(
        "Rakka Phase 1 actor replied with {reply} on {}.",
        rakka_core::runtime_name()
    );

    system.shutdown();
    Ok(())
}
