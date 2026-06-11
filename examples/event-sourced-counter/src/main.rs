#![forbid(unsafe_code)]

//! Event-sourced counter using the Phase 3 behavior facade.

use std::error::Error;
use std::time::Duration;

use rakka::prelude::*;

#[derive(Debug, Clone, PartialEq, Eq)]
struct CounterState {
    value: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CounterEvent {
    Incremented(i64),
}

#[derive(Debug)]
enum CounterCommand {
    Increment { by: i64, reply_to: ReplyTo<i64> },
    Get { reply_to: ReplyTo<i64> },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let system = ActorSystem::new("event-sourced-counter");
    let journal = InMemoryEventJournal::<CounterEvent>::new();
    let snapshots = InMemorySnapshotStore::<CounterState>::new();
    let persistence_id = PersistenceId::of("Counter", "example")?;
    let behavior = EventSourcedBehavior::builder(persistence_id, CounterState { value: 0 })
        .on_command(|state, command| match command {
            CounterCommand::Increment { by, reply_to } => {
                EventSourcedEffect::persist(CounterEvent::Incremented(by))
                    .then_reply(reply_to, state.value + by)
            }
            CounterCommand::Get { reply_to } => EventSourcedEffect::reply(reply_to, state.value),
        })
        .on_event(|state, event| match event {
            CounterEvent::Incremented(by) => CounterState {
                value: state.value + by,
            },
        })
        .build()?
        .with_retention_criteria(RetentionCriteria::snapshot_every(2).keep_snapshots(1));
    let counter = behavior.spawn(&system, "counter", journal, snapshots)?;

    let first = increment(&counter, 1).await?;
    let second = increment(&counter, 1).await?;
    let recovered = get(&counter).await?;

    println!("Rakka event-sourced counter values: {first}, {second}, recovered {recovered}.");
    system.terminate().await?;
    Ok(())
}

async fn increment(counter: &ActorRef<CounterCommand>, by: i64) -> Result<i64, Box<dyn Error>> {
    Ok(counter
        .ask(
            |reply_to| CounterCommand::Increment { by, reply_to },
            Duration::from_secs(1),
        )
        .await?)
}

async fn get(counter: &ActorRef<CounterCommand>) -> Result<i64, Box<dyn Error>> {
    Ok(counter
        .ask(
            |reply_to| CounterCommand::Get { reply_to },
            Duration::from_secs(1),
        )
        .await?)
}
