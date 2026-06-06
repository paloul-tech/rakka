#![forbid(unsafe_code)]

//! Minimal durable counter example using the in-memory durable state store.

use std::time::Duration;

use rakka_core::{ActorRef, ActorSystem, ReplyTo};
use rakka_persistence::{
    durable_actor_future, spawn_durable_actor, DurableActor, DurableActorContext, DurableEffect,
    InMemoryDurableStateStore, PersistenceId,
};

#[derive(Clone)]
struct CounterState {
    value: u64,
}

enum CounterCommand {
    Increment { reply_to: ReplyTo<u64> },
    Get { reply_to: ReplyTo<u64> },
}

struct CounterActor {
    persistence_id: PersistenceId,
}

impl DurableActor for CounterActor {
    type Command = CounterCommand;
    type State = CounterState;

    fn persistence_id(&self) -> PersistenceId {
        self.persistence_id.clone()
    }

    fn empty_state(&self) -> Self::State {
        CounterState { value: 0 }
    }

    fn handle_command<'a>(
        &'a mut self,
        ctx: &'a mut DurableActorContext<'a, Self::Command>,
        state: &'a Self::State,
        command: Self::Command,
    ) -> rakka_persistence::DurableActorFuture<'a, Self::State> {
        match command {
            CounterCommand::Increment { reply_to } => {
                let next = CounterState {
                    value: state.value + 1,
                };
                let reply = ctx.reply_after_commit(reply_to, next.value);
                durable_actor_future(
                    async move { Ok(DurableEffect::persist(next).then_run(reply)) },
                )
            }
            CounterCommand::Get { reply_to } => {
                let reply = ctx.reply_after_commit(reply_to, state.value);
                durable_actor_future(async move { Ok(DurableEffect::none().then_run(reply)) })
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let system = ActorSystem::new("durable-counter");
    let store = InMemoryDurableStateStore::<CounterState>::new();
    let persistence_id = PersistenceId::new("examples/counter");

    let first = spawn_counter(&system, "counter-a", persistence_id.clone(), store.clone());
    increment(&first).await?;
    increment(&first).await?;
    first.stop()?;
    wait_until_terminated(&first).await;

    let second = spawn_counter(&system, "counter-b", persistence_id, store);
    let recovered = get(&second).await?;
    println!("Rakka durable counter recovered value {recovered}.");

    system.shutdown();
    Ok(())
}

fn spawn_counter(
    system: &ActorSystem,
    actor_name: &str,
    persistence_id: PersistenceId,
    store: InMemoryDurableStateStore<CounterState>,
) -> ActorRef<CounterCommand> {
    spawn_durable_actor(system, actor_name, CounterActor { persistence_id }, store)
        .expect("durable counter should spawn")
}

async fn increment(counter: &ActorRef<CounterCommand>) -> Result<u64, rakka_core::AskError> {
    counter
        .ask(
            |reply_to| CounterCommand::Increment { reply_to },
            Duration::from_secs(1),
        )
        .await
}

async fn get(counter: &ActorRef<CounterCommand>) -> Result<u64, rakka_core::AskError> {
    counter
        .ask(
            |reply_to| CounterCommand::Get { reply_to },
            Duration::from_secs(1),
        )
        .await
}

async fn wait_until_terminated<M>(actor: &ActorRef<M>)
where
    M: rakka_core::Message,
{
    for _ in 0..100 {
        if actor.is_terminated() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
