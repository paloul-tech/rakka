//! Durable state integration tests.

use std::time::Duration;

use rakka_core::{ActorRef, ActorSystem, ReplyTo};
use rakka_persistence::{
    durable_actor_future, spawn_durable_actor, DurableActor, DurableActorContext, DurableEffect,
    DurableStateStore, InMemoryDurableStateStore, PersistenceId, Revision,
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct CounterState {
    value: u64,
}

#[derive(Debug)]
enum CounterCommand {
    Increment { by: u64, reply_to: ReplyTo<u64> },
    Get { reply_to: ReplyTo<u64> },
    Delete { reply_to: ReplyTo<()> },
}

struct CounterActor {
    persistence_id: PersistenceId,
}

impl CounterActor {
    fn new(persistence_id: impl Into<String>) -> Self {
        Self {
            persistence_id: PersistenceId::new(persistence_id),
        }
    }
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
            CounterCommand::Increment { by, reply_to } => {
                let next = CounterState {
                    value: state.value + by,
                };
                let reply = ctx.reply_after_commit(reply_to, next.value);
                durable_actor_future(
                    async move { Ok(DurableEffect::persist(next).then_run(reply)) },
                )
            }
            CounterCommand::Get { reply_to } => {
                let value = state.value;
                let reply = ctx.reply_after_commit(reply_to, value);
                durable_actor_future(async move { Ok(DurableEffect::none().then_run(reply)) })
            }
            CounterCommand::Delete { reply_to } => {
                let reply = ctx.reply_after_commit(reply_to, ());
                durable_actor_future(async move { Ok(DurableEffect::delete().then_run(reply)) })
            }
        }
    }
}

#[tokio::test]
async fn in_memory_store_enforces_revision_fencing() {
    let store = InMemoryDurableStateStore::<CounterState>::new();
    let id = PersistenceId::new("counter/fenced");

    let first = store
        .compare_and_set(&id, Revision::INITIAL, CounterState { value: 1 })
        .await
        .unwrap();
    assert_eq!(first.revision, Revision::new(1));

    let conflict = store
        .compare_and_set(&id, Revision::INITIAL, CounterState { value: 2 })
        .await
        .unwrap_err();
    assert!(matches!(
        conflict,
        rakka_persistence::DurableError::RevisionConflict { .. }
    ));

    let second = store
        .compare_and_set(&id, first.revision, CounterState { value: 2 })
        .await
        .unwrap();
    assert_eq!(second.revision, Revision::new(2));
    assert_eq!(second.state.value, 2);
}

#[tokio::test]
async fn durable_actor_persists_commands_sequentially() {
    let system = ActorSystem::new("durable-sequential");
    let store = InMemoryDurableStateStore::<CounterState>::new();
    let counter = spawn_counter(&system, "counter", "counter/sequential", store.clone());

    let one = increment(&counter, 1).await;
    let two = increment(&counter, 1).await;
    let loaded = store
        .load(&PersistenceId::new("counter/sequential"))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(one, 1);
    assert_eq!(two, 2);
    assert_eq!(loaded.state.value, 2);
    assert_eq!(loaded.revision, Revision::new(2));
    system.shutdown();
}

#[tokio::test]
async fn durable_actor_recovers_latest_state_after_restart() {
    let system = ActorSystem::new("durable-recovery");
    let store = InMemoryDurableStateStore::<CounterState>::new();
    let first = spawn_counter(&system, "counter-a", "counter/recover", store.clone());

    assert_eq!(increment(&first, 5).await, 5);
    first.stop().unwrap();
    wait_until_terminated(&first).await;

    let second = spawn_counter(&system, "counter-b", "counter/recover", store);
    assert_eq!(get(&second).await, 5);
    system.shutdown();
}

#[tokio::test]
async fn durable_actor_delete_resets_recovered_state() {
    let system = ActorSystem::new("durable-delete");
    let store = InMemoryDurableStateStore::<CounterState>::new();
    let first = spawn_counter(&system, "counter-a", "counter/delete", store.clone());

    assert_eq!(increment(&first, 7).await, 7);
    delete(&first).await;
    first.stop().unwrap();
    wait_until_terminated(&first).await;

    let second = spawn_counter(&system, "counter-b", "counter/delete", store.clone());
    assert_eq!(get(&second).await, 0);
    assert_eq!(
        store
            .load(&PersistenceId::new("counter/delete"))
            .await
            .unwrap(),
        None
    );
    system.shutdown();
}

#[tokio::test]
async fn revision_conflict_prevents_reply_side_effect() {
    let system = ActorSystem::new("durable-conflict");
    let store = InMemoryDurableStateStore::<CounterState>::new();
    let id = PersistenceId::new("counter/conflict");
    let counter = spawn_counter(&system, "counter", id.as_str(), store.clone());

    assert_eq!(increment(&counter, 1).await, 1);
    store
        .compare_and_set(&id, Revision::new(1), CounterState { value: 99 })
        .await
        .unwrap();

    let result = counter
        .ask(
            |reply_to| CounterCommand::Increment { by: 1, reply_to },
            Duration::from_secs(1),
        )
        .await;

    assert!(matches!(result, Err(rakka_core::AskError::ReplyDropped)));
    let loaded = store.load(&id).await.unwrap().unwrap();
    assert_eq!(loaded.state.value, 99);
    assert_eq!(loaded.revision, Revision::new(2));
    system.shutdown();
}

fn spawn_counter(
    system: &ActorSystem,
    actor_name: &str,
    persistence_id: &str,
    store: InMemoryDurableStateStore<CounterState>,
) -> ActorRef<CounterCommand> {
    spawn_durable_actor(system, actor_name, CounterActor::new(persistence_id), store).unwrap()
}

async fn increment(counter: &ActorRef<CounterCommand>, by: u64) -> u64 {
    counter
        .ask(
            |reply_to| CounterCommand::Increment { by, reply_to },
            Duration::from_secs(1),
        )
        .await
        .unwrap()
}

async fn get(counter: &ActorRef<CounterCommand>) -> u64 {
    counter
        .ask(
            |reply_to| CounterCommand::Get { reply_to },
            Duration::from_secs(1),
        )
        .await
        .unwrap()
}

async fn delete(counter: &ActorRef<CounterCommand>) {
    counter
        .ask(
            |reply_to| CounterCommand::Delete { reply_to },
            Duration::from_secs(1),
        )
        .await
        .unwrap()
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
    panic!("actor did not terminate");
}
