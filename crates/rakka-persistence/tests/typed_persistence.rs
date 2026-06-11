//! Typed persistence store tests.

use std::time::Duration;

use rakka_core::{ActorRef, ActorSystem, Message, ReplyTo};
use rakka_persistence::{
    current_events_by_tag, current_persistence_ids, event_sourced_actor_future,
    spawn_event_sourced_actor, DurableEffect, DurableError, DurableStateBehavior,
    DurableStateStore, EventJournal, EventSourcedActor, EventSourcedActorContext,
    EventSourcedActorFuture, EventSourcedBehavior, EventSourcedEffect, InMemoryDurableStateStore,
    InMemoryEventJournal, InMemorySnapshotStore, PersistenceId, RetentionCriteria, Revision,
    SequenceNr, SnapshotSelection, SnapshotStore, TaggedEvent,
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct CounterState {
    value: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CounterEvent {
    Incremented(i64),
    Decremented(i64),
}

#[derive(Debug)]
enum CounterCommand {
    Increment { by: i64, reply_to: ReplyTo<i64> },
    Get { reply_to: ReplyTo<i64> },
    Snapshot { reply_to: ReplyTo<()> },
}

struct EventSourcedCounter {
    persistence_id: PersistenceId,
}

impl EventSourcedCounter {
    fn new(persistence_id: PersistenceId) -> Self {
        Self { persistence_id }
    }
}

impl EventSourcedActor for EventSourcedCounter {
    type Command = CounterCommand;
    type Event = CounterEvent;
    type State = CounterState;

    fn persistence_id(&self) -> PersistenceId {
        self.persistence_id.clone()
    }

    fn empty_state(&self) -> Self::State {
        CounterState { value: 0 }
    }

    fn handle_command<'a>(
        &'a mut self,
        ctx: &'a mut EventSourcedActorContext<'a, Self::Command>,
        state: &'a Self::State,
        command: Self::Command,
    ) -> EventSourcedActorFuture<'a, Self::Event> {
        match command {
            CounterCommand::Increment { by, reply_to } => {
                let reply = ctx.reply_after_persist(reply_to, state.value + by);
                event_sourced_actor_future(async move {
                    Ok(EventSourcedEffect::persist_tagged(TaggedEvent::with_tags(
                        CounterEvent::Incremented(by),
                        ["counter"],
                    ))
                    .then_run(reply))
                })
            }
            CounterCommand::Get { reply_to } => {
                let reply = ctx.reply_after_persist(reply_to, state.value);
                event_sourced_actor_future(
                    async move { Ok(EventSourcedEffect::none().then_run(reply)) },
                )
            }
            CounterCommand::Snapshot { reply_to } => {
                let reply = ctx.reply_after_persist(reply_to, ());
                event_sourced_actor_future(async move {
                    Ok(EventSourcedEffect::none().then_snapshot().then_run(reply))
                })
            }
        }
    }

    fn apply_event(&mut self, state: &Self::State, event: &Self::Event) -> Self::State {
        match event {
            CounterEvent::Incremented(by) => CounterState {
                value: state.value + by,
            },
            CounterEvent::Decremented(by) => CounterState {
                value: state.value - by,
            },
        }
    }
}

#[tokio::test]
async fn persistence_id_of_validates_entity_parts() {
    let id = PersistenceId::of("counter", "abc-123").expect("id should be valid");

    assert_eq!(id.as_str(), "counter|abc-123");
    assert_eq!(id.entity_parts(), Some(("counter", "abc-123")));

    assert!(matches!(
        PersistenceId::of("", "abc-123"),
        Err(DurableError::InvalidPersistenceId { .. })
    ));
    assert!(matches!(
        PersistenceId::of("counter", "abc|123"),
        Err(DurableError::InvalidPersistenceId { .. })
    ));
}

#[tokio::test]
async fn in_memory_journal_appends_replays_deletes_and_queries_tags() {
    let journal = InMemoryEventJournal::<CounterEvent>::new();
    let id = PersistenceId::of("counter", "journal").expect("id should be valid");

    let appended = journal
        .append(
            &id,
            SequenceNr::INITIAL,
            vec![
                TaggedEvent::with_tags(CounterEvent::Incremented(1), ["counter", "hot"]),
                TaggedEvent::new(CounterEvent::Incremented(2)),
            ],
        )
        .await
        .expect("append should succeed");

    assert_eq!(appended.len(), 2);
    assert_eq!(appended[0].metadata.sequence_nr, SequenceNr::FIRST);
    assert_eq!(appended[1].metadata.sequence_nr, SequenceNr::new(2));
    assert_eq!(
        journal.highest_sequence_nr(&id).await.unwrap(),
        SequenceNr::new(2)
    );

    let conflict = journal
        .append(
            &id,
            SequenceNr::INITIAL,
            vec![TaggedEvent::new(CounterEvent::Decremented(1))],
        )
        .await
        .unwrap_err();
    assert!(matches!(conflict, DurableError::SequenceConflict { .. }));

    let replayed = journal
        .replay(&id, SequenceNr::FIRST, SequenceNr::MAX)
        .await
        .expect("replay should succeed");
    assert_eq!(
        replayed
            .iter()
            .map(|record| &record.event)
            .collect::<Vec<_>>(),
        vec![&CounterEvent::Incremented(1), &CounterEvent::Incremented(2)]
    );

    let tagged = journal
        .events_by_tag("hot")
        .await
        .expect("tag query should succeed");
    assert_eq!(tagged.len(), 1);
    assert_eq!(tagged[0].event, CounterEvent::Incremented(1));

    journal
        .delete_to(&id, SequenceNr::FIRST)
        .await
        .expect("delete should succeed");
    let after_delete = journal
        .replay(&id, SequenceNr::FIRST, SequenceNr::MAX)
        .await
        .expect("replay should succeed");
    assert_eq!(after_delete.len(), 1);
    assert_eq!(after_delete[0].metadata.sequence_nr, SequenceNr::new(2));
    assert_eq!(
        journal.highest_sequence_nr(&id).await.unwrap(),
        SequenceNr::new(2)
    );

    let next = journal
        .append(
            &id,
            SequenceNr::new(2),
            vec![TaggedEvent::with_tags(
                CounterEvent::Decremented(1),
                ["hot"],
            )],
        )
        .await
        .expect("append after delete should succeed");
    assert_eq!(next[0].metadata.sequence_nr, SequenceNr::new(3));
    assert_eq!(journal.persistence_ids().await.unwrap(), vec![id]);
}

#[tokio::test]
async fn in_memory_snapshot_store_selects_lists_and_deletes_snapshots() {
    let snapshots = InMemorySnapshotStore::<CounterState>::new();
    let id = PersistenceId::of("counter", "snapshot").expect("id should be valid");

    snapshots
        .save(&id, SequenceNr::FIRST, CounterState { value: 1 })
        .await
        .expect("first snapshot should save");
    snapshots
        .save(&id, SequenceNr::new(5), CounterState { value: 5 })
        .await
        .expect("second snapshot should save");

    let latest = snapshots
        .load(&id, SnapshotSelection::latest())
        .await
        .expect("snapshot load should succeed")
        .expect("latest snapshot should exist");
    assert_eq!(latest.snapshot.value, 5);
    assert_eq!(latest.metadata.sequence_nr, SequenceNr::new(5));

    let earlier = snapshots
        .load(&id, SnapshotSelection::up_to(SequenceNr::new(2)))
        .await
        .expect("snapshot load should succeed")
        .expect("earlier snapshot should exist");
    assert_eq!(earlier.snapshot.value, 1);

    let listed = snapshots
        .list(&id, SnapshotSelection::latest())
        .await
        .expect("snapshot list should succeed");
    assert_eq!(
        listed
            .iter()
            .map(|metadata| metadata.sequence_nr)
            .collect::<Vec<_>>(),
        vec![SequenceNr::new(5), SequenceNr::FIRST]
    );

    let removed = snapshots
        .delete(&id, SnapshotSelection::up_to(SequenceNr::new(2)))
        .await
        .expect("snapshot delete should succeed");
    assert_eq!(removed, 1);
    assert_eq!(snapshots.len(), 1);
}

#[tokio::test]
async fn in_memory_durable_state_lists_persistence_ids() {
    let store = InMemoryDurableStateStore::<CounterState>::new();
    let id = PersistenceId::of("counter", "state").expect("id should be valid");

    store
        .compare_and_set(&id, Revision::INITIAL, CounterState { value: 7 })
        .await
        .expect("state write should succeed");

    assert_eq!(store.persistence_ids().await.unwrap(), vec![id]);
}

#[tokio::test]
async fn event_sourced_actor_recovers_from_snapshot_and_journal() {
    let system = ActorSystem::new("event-sourced-recovery");
    let journal = InMemoryEventJournal::<CounterEvent>::new();
    let snapshots = InMemorySnapshotStore::<CounterState>::new();
    let id = PersistenceId::of("counter", "actor").expect("id should be valid");
    let first = spawn_event_sourced_actor(
        &system,
        "counter-a",
        EventSourcedCounter::new(id.clone()),
        journal.clone(),
        snapshots.clone(),
    )
    .expect("event-sourced actor should spawn");

    assert_eq!(increment_event_sourced(&first, 2).await, 2);
    snapshot_event_sourced(&first).await;
    assert_eq!(increment_event_sourced(&first, 3).await, 5);
    first.stop().expect("actor should stop");
    wait_until_terminated(&first).await;

    let second = spawn_event_sourced_actor(
        &system,
        "counter-b",
        EventSourcedCounter::new(id.clone()),
        journal.clone(),
        snapshots.clone(),
    )
    .expect("event-sourced actor should respawn");

    assert_eq!(get_event_sourced(&second).await, 5);
    assert_eq!(
        journal.highest_sequence_nr(&id).await.unwrap(),
        SequenceNr::new(2)
    );
    assert_eq!(
        snapshots
            .load(&id, SnapshotSelection::latest())
            .await
            .unwrap()
            .unwrap()
            .metadata
            .sequence_nr,
        SequenceNr::FIRST
    );
    system.shutdown();
}

#[tokio::test]
async fn event_sourced_behavior_builder_spawns_with_retention_and_recovery() {
    let system = ActorSystem::new("event-sourced-behavior");
    let journal = InMemoryEventJournal::<CounterEvent>::new();
    let snapshots = InMemorySnapshotStore::<CounterState>::new();
    let id = PersistenceId::of("counter", "behavior").expect("id should be valid");
    let behavior = counter_event_sourced_behavior(id.clone()).with_retention_criteria(
        RetentionCriteria::snapshot_every(2)
            .keep_snapshots(1)
            .delete_events_on_snapshot(),
    );
    let first = behavior
        .clone()
        .spawn(
            &system,
            "counter-behavior-a",
            journal.clone(),
            snapshots.clone(),
        )
        .expect("behavior should spawn");

    assert_eq!(increment_event_sourced(&first, 1).await, 1);
    assert_eq!(increment_event_sourced(&first, 2).await, 3);
    assert_eq!(
        snapshots
            .load(&id, SnapshotSelection::latest())
            .await
            .unwrap()
            .unwrap()
            .metadata
            .sequence_nr,
        SequenceNr::new(2)
    );
    assert!(
        journal
            .replay(&id, SequenceNr::FIRST, SequenceNr::MAX)
            .await
            .unwrap()
            .is_empty(),
        "retention should delete events covered by the retained snapshot"
    );

    assert_eq!(increment_event_sourced(&first, 3).await, 6);
    first.stop().expect("actor should stop");
    wait_until_terminated(&first).await;

    let second = behavior
        .spawn(
            &system,
            "counter-behavior-b",
            journal.clone(),
            snapshots.clone(),
        )
        .expect("behavior should respawn");
    assert_eq!(get_event_sourced(&second).await, 6);
    system.shutdown();
}

#[tokio::test]
async fn durable_state_behavior_facade_persists_state() {
    let system = ActorSystem::new("durable-state-behavior");
    let store = InMemoryDurableStateStore::<CounterState>::new();
    let id = PersistenceId::of("counter", "durable-behavior").expect("id should be valid");
    let behavior = DurableStateBehavior::builder(id.clone(), CounterState { value: 0 })
        .on_command(|state, command: CounterCommand| match command {
            CounterCommand::Increment { by, reply_to } => {
                let next = CounterState {
                    value: state.value + by,
                };
                DurableEffect::persist(next.clone()).then_reply(reply_to, next.value)
            }
            CounterCommand::Get { reply_to } => DurableEffect::reply(reply_to, state.value),
            CounterCommand::Snapshot { reply_to } => DurableEffect::reply(reply_to, ()),
        })
        .build()
        .expect("durable behavior should build");
    let actor = behavior
        .spawn(&system, "durable-behavior", store.clone())
        .expect("durable behavior should spawn");

    assert_eq!(increment_event_sourced(&actor, 4).await, 4);
    assert_eq!(
        store.load(&id).await.unwrap().unwrap().state,
        CounterState { value: 4 }
    );
    system.shutdown();
}

#[tokio::test]
async fn persistence_query_helpers_return_bounded_streams() {
    let journal = InMemoryEventJournal::<CounterEvent>::new();
    let id = PersistenceId::of("counter", "query").expect("id should be valid");
    journal
        .append(
            &id,
            SequenceNr::INITIAL,
            vec![TaggedEvent::with_tags(
                CounterEvent::Incremented(5),
                ["query"],
            )],
        )
        .await
        .expect("append should succeed");

    let tagged = current_events_by_tag(journal.clone(), "query")
        .await
        .expect("query should build stream");
    let events = collect_query_stream(tagged).await;
    assert_eq!(events[0].event, CounterEvent::Incremented(5));

    let ids = current_persistence_ids::<CounterEvent, _>(journal)
        .await
        .expect("id query should build stream");
    assert_eq!(collect_query_stream(ids).await, vec![id]);
}

fn counter_event_sourced_behavior(
    persistence_id: PersistenceId,
) -> EventSourcedBehavior<CounterCommand, CounterEvent, CounterState> {
    EventSourcedBehavior::builder(persistence_id, CounterState { value: 0 })
        .on_command(|state, command| match command {
            CounterCommand::Increment { by, reply_to } => EventSourcedEffect::persist_tagged(
                TaggedEvent::with_tags(CounterEvent::Incremented(by), ["counter"]),
            )
            .then_reply(reply_to, state.value + by),
            CounterCommand::Get { reply_to } => EventSourcedEffect::reply(reply_to, state.value),
            CounterCommand::Snapshot { reply_to } => EventSourcedEffect::none()
                .then_snapshot()
                .then_reply(reply_to, ()),
        })
        .on_event(|state, event| match event {
            CounterEvent::Incremented(by) => CounterState {
                value: state.value + by,
            },
            CounterEvent::Decremented(by) => CounterState {
                value: state.value - by,
            },
        })
        .build()
        .expect("event-sourced behavior should build")
}

async fn collect_query_stream<T>(source: rakka_stream::StreamSource<T>) -> Vec<T> {
    let mut items = Vec::new();
    while let Some(item) = source.next().await.expect("query stream should read") {
        items.push(item);
    }
    items
}

async fn increment_event_sourced(counter: &ActorRef<CounterCommand>, by: i64) -> i64 {
    counter
        .ask(
            |reply_to| CounterCommand::Increment { by, reply_to },
            Duration::from_secs(1),
        )
        .await
        .expect("increment ask should reply")
}

async fn get_event_sourced(counter: &ActorRef<CounterCommand>) -> i64 {
    counter
        .ask(
            |reply_to| CounterCommand::Get { reply_to },
            Duration::from_secs(1),
        )
        .await
        .expect("get ask should reply")
}

async fn snapshot_event_sourced(counter: &ActorRef<CounterCommand>) {
    counter
        .ask(
            |reply_to| CounterCommand::Snapshot { reply_to },
            Duration::from_secs(1),
        )
        .await
        .expect("snapshot ask should reply")
}

async fn wait_until_terminated<M>(actor: &ActorRef<M>)
where
    M: Message,
{
    for _ in 0..100 {
        if actor.is_terminated() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("actor did not terminate");
}
