#![forbid(unsafe_code)]

//! Local receptionist and group router example.

use std::error::Error;
use std::io;
use std::time::Duration;

use rakka::prelude::*;
use tokio::sync::mpsc;

#[derive(Debug)]
enum WorkCommand {
    Work { id: u64 },
}

struct Worker {
    name: &'static str,
    delivered: mpsc::UnboundedSender<String>,
}

impl Actor for Worker {
    type Msg = WorkCommand;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        let name = self.name;
        let delivered = self.delivered.clone();
        actor_future(async move {
            match msg {
                WorkCommand::Work { id } => {
                    let _ = delivered.send(format!("{name}:{id}"));
                }
            }
            Ok(ActorAction::Continue)
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let system = ActorSystem::new("local-receptionist-router");
    let key = ServiceKey::<WorkCommand>::new("example.workers");
    let receptionist = Receptionist::get(&system);
    let (delivered, mut received) = mpsc::unbounded_channel();

    let worker_a = system.spawn_actor(
        "worker-a",
        Worker {
            name: "worker-a",
            delivered: delivered.clone(),
        },
    )?;
    let worker_b = system.spawn_actor(
        "worker-b",
        Worker {
            name: "worker-b",
            delivered,
        },
    )?;
    let _registration_a = receptionist.register(&key, worker_a)?;
    let _registration_b = receptionist.register(&key, worker_b)?;

    let group = Routers::group(key.clone())
        .with_round_robin()
        .spawn(&system, "worker-group")?;
    group.tell(WorkCommand::Work { id: 1 })?;
    group.tell(WorkCommand::Work { id: 2 })?;

    let first = receive_one(&mut received).await?;
    let second = receive_one(&mut received).await?;
    let listing = receptionist.find(&key)?;

    println!(
        "Rakka local receptionist group router delivered [{first}, {second}] across {} routees.",
        listing.len()
    );

    system.terminate().await?;
    Ok(())
}

async fn receive_one(
    received: &mut mpsc::UnboundedReceiver<String>,
) -> Result<String, Box<dyn Error>> {
    let value = tokio::time::timeout(Duration::from_secs(1), received.recv())
        .await?
        .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "delivery channel closed"))?;
    Ok(value)
}
