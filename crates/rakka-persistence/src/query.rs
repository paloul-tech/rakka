//! Persistence query helpers backed by `rakka-stream`.

use rakka_stream::{bounded_channel, StreamSink, StreamSource};

use crate::error::{DurableError, DurableResult};
use crate::store::{
    DurableState, DurableStateStore, EventJournal, EventRecord, PersistenceEvent, PersistenceId,
    SequenceNr, StateRecord, StoreFuture,
};

/// Default bounded buffer capacity used by persistence query streams.
pub const DEFAULT_PERSISTENCE_QUERY_BUFFER_CAPACITY: usize = 1024;

/// Streams current events for one persistence id.
pub async fn current_events_by_persistence_id<E, Journal>(
    journal: Journal,
    persistence_id: &PersistenceId,
    from: SequenceNr,
    to: SequenceNr,
) -> DurableResult<StreamSource<EventRecord<E>>>
where
    E: PersistenceEvent,
    Journal: EventJournal<E>,
{
    let records = journal.replay(persistence_id, from, to).await?;
    stream_items(records, DEFAULT_PERSISTENCE_QUERY_BUFFER_CAPACITY).await
}

/// Alias for current event-by-persistence-id query.
pub async fn events_by_persistence_id<E, Journal>(
    journal: Journal,
    persistence_id: &PersistenceId,
    from: SequenceNr,
    to: SequenceNr,
) -> DurableResult<StreamSource<EventRecord<E>>>
where
    E: PersistenceEvent,
    Journal: EventJournal<E>,
{
    current_events_by_persistence_id(journal, persistence_id, from, to).await
}

/// Streams current events carrying a tag.
pub async fn current_events_by_tag<E, Journal>(
    journal: Journal,
    tag: &str,
) -> DurableResult<StreamSource<EventRecord<E>>>
where
    E: PersistenceEvent,
    Journal: EventJournal<E>,
{
    let records = journal.events_by_tag(tag).await?;
    stream_items(records, DEFAULT_PERSISTENCE_QUERY_BUFFER_CAPACITY).await
}

/// Alias for current event-by-tag query.
pub async fn events_by_tag<E, Journal>(
    journal: Journal,
    tag: &str,
) -> DurableResult<StreamSource<EventRecord<E>>>
where
    E: PersistenceEvent,
    Journal: EventJournal<E>,
{
    current_events_by_tag(journal, tag).await
}

/// Streams current event-sourced persistence ids.
pub async fn current_persistence_ids<E, Journal>(
    journal: Journal,
) -> DurableResult<StreamSource<PersistenceId>>
where
    E: PersistenceEvent,
    Journal: EventJournal<E>,
{
    let ids = journal.persistence_ids().await?;
    stream_items(ids, DEFAULT_PERSISTENCE_QUERY_BUFFER_CAPACITY).await
}

/// Alias for current event-sourced persistence id query.
pub async fn persistence_ids<E, Journal>(
    journal: Journal,
) -> DurableResult<StreamSource<PersistenceId>>
where
    E: PersistenceEvent,
    Journal: EventJournal<E>,
{
    current_persistence_ids(journal).await
}

/// Streams current durable-state persistence ids.
pub async fn current_durable_state_ids<S, Store>(
    store: Store,
) -> DurableResult<StreamSource<PersistenceId>>
where
    S: DurableState,
    Store: DurableStateStore<S>,
{
    let ids = store.persistence_ids().await?;
    stream_items(ids, DEFAULT_PERSISTENCE_QUERY_BUFFER_CAPACITY).await
}

/// Streams the current durable-state record for one persistence id when present.
pub async fn current_durable_state_by_id<S, Store>(
    store: Store,
    persistence_id: &PersistenceId,
) -> DurableResult<StreamSource<StateRecord<S>>>
where
    S: DurableState,
    Store: DurableStateStore<S>,
{
    let records = store
        .load(persistence_id)
        .await?
        .map_or_else(Vec::new, |record| vec![record]);
    stream_items(records, DEFAULT_PERSISTENCE_QUERY_BUFFER_CAPACITY).await
}

/// Future returned by query store callbacks.
pub type QueryFuture<'a, T> = StoreFuture<'a, T>;

async fn stream_items<T>(items: Vec<T>, capacity: usize) -> DurableResult<StreamSource<T>>
where
    T: Send + 'static,
{
    let (sink, source) = bounded_channel(capacity)
        .map_err(|error| DurableError::store("query-stream", error.to_string()))?;
    send_all(sink, items).await?;
    Ok(source)
}

async fn send_all<T>(sink: StreamSink<T>, items: Vec<T>) -> DurableResult<()>
where
    T: Send + 'static,
{
    for item in items {
        sink.send(item)
            .await
            .map_err(|error| DurableError::store("query-stream", error.to_string()))?;
    }
    sink.drain()
        .map_err(|error| DurableError::store("query-stream", error.to_string()))?;
    Ok(())
}
