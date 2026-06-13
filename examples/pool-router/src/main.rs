#![forbid(unsafe_code)]

//! Local pool router worker-farm example.

use std::error::Error;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rakka::prelude::*;
use tokio::sync::mpsc;

#[derive(Debug)]
struct JobCommand {
    id: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let system = ActorSystem::new("pool-router");
    let (delivered, mut received) = mpsc::unbounded_channel();
    let next_worker = Arc::new(AtomicUsize::new(0));

    let router = Routers::pool("pool-worker", 3, {
        let next_worker = next_worker.clone();
        move || {
            let worker_id = next_worker.fetch_add(1, Ordering::SeqCst);
            let delivered = delivered.clone();
            actor_fn(
                move |_ctx: &mut ActorContext<JobCommand>, msg: JobCommand| {
                    let _ = delivered.send((msg.id, worker_id));
                    Ok(ActorAction::Continue)
                },
            )
        }
    })
    .with_round_robin()
    .spawn(&system)?;

    for id in 0..6 {
        router.tell(JobCommand { id })?;
    }

    let mut observed = Vec::new();
    for _ in 0..6 {
        observed.push(receive_one(&mut received).await?);
    }
    observed.sort_unstable_by_key(|(job_id, _worker_id)| *job_id);

    println!(
        "Rakka pool router sent {} jobs through {} routees: {:?}.",
        observed.len(),
        router.routee_count(),
        observed
    );

    system.terminate().await?;
    Ok(())
}

async fn receive_one(
    received: &mut mpsc::UnboundedReceiver<(u64, usize)>,
) -> Result<(u64, usize), Box<dyn Error>> {
    let value = tokio::time::timeout(Duration::from_secs(1), received.recv())
        .await?
        .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "delivery channel closed"))?;
    Ok(value)
}
