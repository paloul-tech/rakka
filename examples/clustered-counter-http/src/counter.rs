//! Counter entity, durable actor, and example-local file persistence.

use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::Arc;

use rakka::persistence::{DurableError, DurableResult, StateRecord, StoreFuture};
use rakka::prelude::*;
use rakka::sharding::ClusterNodeRuntime;
use serde::{Deserialize, Serialize};

use crate::model::{CounterAction, CounterOperation, CounterValue};
use crate::support::{current_timestamp_millis, hex_encode};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CounterState {
    value: i64,
    initialized: bool,
}

pub enum CounterCommand {
    Apply {
        operation: CounterOperation,
        reply_to: ReplyTo<CounterValue>,
    },
}

// Each sharded counter is backed by one durable actor. A first request for a
// counter name creates this actor on the node that owns the name's shard.
struct CounterDurableActor {
    persistence_id: PersistenceId,
    owner_node: String,
}

impl DurableActor for CounterDurableActor {
    type Command = CounterCommand;
    type State = CounterState;

    fn persistence_id(&self) -> PersistenceId {
        self.persistence_id.clone()
    }

    fn empty_state(&self) -> Self::State {
        CounterState {
            value: 0,
            initialized: false,
        }
    }

    fn handle_command<'a>(
        &'a mut self,
        ctx: &'a mut DurableActorContext<'a, Self::Command>,
        state: &'a Self::State,
        command: Self::Command,
    ) -> DurableActorFuture<'a, Self::State> {
        match command {
            CounterCommand::Apply {
                operation,
                reply_to,
            } => {
                let created = !state.initialized;
                let current_revision = ctx.revision();
                let owner_node = self.owner_node.clone();
                let name = operation.name;
                let amount = operation.amount;

                match operation.action {
                    CounterAction::Initiate if state.initialized => {
                        let reply = ctx.reply_after_commit(
                            reply_to,
                            CounterValue {
                                name,
                                value: state.value,
                                revision: current_revision.get(),
                                initialized: true,
                                created: false,
                                owner_node,
                            },
                        );
                        durable_actor_future(
                            async move { Ok(DurableEffect::none().then_run(reply)) },
                        )
                    }
                    CounterAction::Initiate => {
                        let next = CounterState {
                            value: amount,
                            initialized: true,
                        };
                        persist_counter_reply(ctx, reply_to, name, next, created, owner_node)
                    }
                    CounterAction::Increase => {
                        let next = CounterState {
                            value: state.value.saturating_add(amount),
                            initialized: true,
                        };
                        persist_counter_reply(ctx, reply_to, name, next, created, owner_node)
                    }
                    CounterAction::Decrease => {
                        let next = CounterState {
                            value: state.value.saturating_sub(amount),
                            initialized: true,
                        };
                        persist_counter_reply(ctx, reply_to, name, next, created, owner_node)
                    }
                    CounterAction::Get => {
                        let reply = ctx.reply_after_commit(
                            reply_to,
                            CounterValue {
                                name,
                                value: state.value,
                                revision: current_revision.get(),
                                initialized: state.initialized,
                                created: false,
                                owner_node,
                            },
                        );
                        durable_actor_future(
                            async move { Ok(DurableEffect::none().then_run(reply)) },
                        )
                    }
                }
            }
        }
    }
}

fn persist_counter_reply<'a>(
    ctx: &DurableActorContext<'a, CounterCommand>,
    reply_to: ReplyTo<CounterValue>,
    name: String,
    next: CounterState,
    created: bool,
    owner_node: String,
) -> DurableActorFuture<'a, CounterState> {
    let revision = ctx.revision().next().get();
    let reply_value = CounterValue {
        name,
        value: next.value,
        revision,
        initialized: next.initialized,
        created,
        owner_node,
    };
    let reply = ctx.reply_after_commit(reply_to, reply_value);
    durable_actor_future(async move { Ok(DurableEffect::persist(next).then_run(reply)) })
}

struct CounterEntity<Store>
where
    Store: DurableStateStore<CounterState>,
{
    child: ActorRef<CounterCommand>,
    _store: PhantomData<Store>,
}

impl<Store> CounterEntity<Store>
where
    Store: DurableStateStore<CounterState>,
{
    fn new(
        system: ActorSystem,
        context: EntityContext<CounterCommand>,
        store: Store,
        owner_node: String,
    ) -> Self {
        // The sharded entity is only a stable routing shell. The durable child
        // owns the persistence id so it can be stopped/restarted or moved.
        let persistence_id =
            PersistenceId::of(context.entity_type().as_str(), context.entity_id().as_str())
                .expect("counter entity ids are validated before routing");
        let actor_name = format!("{}-durable", context.actor_name());
        let actor_persistence_id = persistence_id.clone();
        let actor_owner_node = owner_node.clone();
        let child = spawn_durable_actor_factory(
            &system,
            actor_name,
            move || CounterDurableActor {
                persistence_id: actor_persistence_id.clone(),
                owner_node: actor_owner_node.clone(),
            },
            store,
        )
        .expect("counter durable actor should spawn");

        Self {
            child,
            _store: PhantomData,
        }
    }
}

impl<Store> Actor for CounterEntity<Store>
where
    Store: DurableStateStore<CounterState>,
{
    type Msg = CounterCommand;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        let child = self.child.clone();
        actor_future(async move {
            child.tell(msg).map_err(|_error| {
                RakkaError::core(
                    "counter-forward-failed",
                    "counter durable child mailbox was unavailable",
                )
            })?;
            Ok(ActorAction::Continue)
        })
    }

    fn stopped<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        _reason: &'a TerminationReason,
    ) -> ActorFuture<'a> {
        let child = self.child.clone();
        actor_future(async move {
            let _ = child.stop();
            Ok(ActorAction::Continue)
        })
    }
}

#[derive(Debug, Clone)]
pub struct FileCounterStateStore {
    root: Arc<PathBuf>,
}

// This intentionally tiny store keeps the example self-contained. Use a shared
// durable backend for real deployments; the example uses a shared directory so
// another local process can recover the counter after shard handoff.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredCounterState {
    revision: u64,
    value: i64,
    initialized: bool,
}

impl FileCounterStateStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: Arc::new(root.into()),
        }
    }

    fn record_path(&self, persistence_id: &PersistenceId) -> PathBuf {
        self.root
            .join(format!("{}.json", hex_encode(persistence_id.as_str())))
    }

    fn load_record(
        &self,
        persistence_id: &PersistenceId,
    ) -> DurableResult<Option<StateRecord<CounterState>>> {
        let path = self.record_path(persistence_id);
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(file_store_error(error)),
        };
        let stored: StoredCounterState = serde_json::from_slice(&bytes)
            .map_err(|error| DurableError::codec(error.to_string()))?;
        Ok(Some(StateRecord::new(
            CounterState {
                value: stored.value,
                initialized: stored.initialized,
            },
            Revision::new(stored.revision),
        )))
    }

    fn write_record(
        &self,
        persistence_id: &PersistenceId,
        record: &StateRecord<CounterState>,
    ) -> DurableResult<()> {
        std::fs::create_dir_all(self.root.as_ref()).map_err(file_store_error)?;
        let path = self.record_path(persistence_id);
        let temp = path.with_extension(format!("json.tmp.{}", current_timestamp_millis()));
        let stored = StoredCounterState {
            revision: record.revision.get(),
            value: record.state.value,
            initialized: record.state.initialized,
        };
        let bytes = serde_json::to_vec_pretty(&stored)
            .map_err(|error| DurableError::codec(error.to_string()))?;
        std::fs::write(&temp, bytes).map_err(file_store_error)?;
        std::fs::rename(&temp, &path).map_err(file_store_error)?;
        Ok(())
    }
}

impl DurableStateStore<CounterState> for FileCounterStateStore {
    fn backend_name(&self) -> &'static str {
        "example-file"
    }

    fn load<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
    ) -> StoreFuture<'a, Option<StateRecord<CounterState>>> {
        Box::pin(async move { self.load_record(persistence_id) })
    }

    fn compare_and_set<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        expected_revision: Revision,
        state: CounterState,
    ) -> StoreFuture<'a, StateRecord<CounterState>> {
        Box::pin(async move {
            let actual = self
                .load_record(persistence_id)?
                .map_or(Revision::INITIAL, |record| record.revision);
            if actual != expected_revision {
                return Err(DurableError::revision_conflict(
                    persistence_id.clone(),
                    expected_revision,
                    actual,
                ));
            }

            let record = StateRecord::new(state, expected_revision.next());
            self.write_record(persistence_id, &record)?;
            Ok(record)
        })
    }

    fn delete<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        expected_revision: Revision,
    ) -> StoreFuture<'a, Revision> {
        Box::pin(async move {
            let actual = self
                .load_record(persistence_id)?
                .map_or(Revision::INITIAL, |record| record.revision);
            if actual != expected_revision {
                return Err(DurableError::revision_conflict(
                    persistence_id.clone(),
                    expected_revision,
                    actual,
                ));
            }

            match std::fs::remove_file(self.record_path(persistence_id)) {
                Ok(()) => Ok(Revision::INITIAL),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Revision::INITIAL),
                Err(error) => Err(file_store_error(error)),
            }
        })
    }
}

pub fn init_counter_entity(
    system: ActorSystem,
    runtime: &mut ClusterNodeRuntime,
    sharding: &ClusterSharding,
    key: EntityTypeKey<CounterCommand>,
    store: FileCounterStateStore,
    owner_node: String,
) -> crate::support::ExampleResult<()> {
    // The facade registers one entity type across the cluster. Every counter
    // name hashes to a shard, and the shard owner lazily starts the durable
    // actor for that specific counter.
    sharding.init_remote_with_ask(
        runtime,
        Entity::of(key, {
            let system = system.clone();
            let store = store.clone();
            let owner_node = owner_node.clone();
            move |context: EntityContext<CounterCommand>| {
                CounterEntity::new(system.clone(), context, store.clone(), owner_node.clone())
            }
        }),
        |operation: CounterOperation, reply_to| CounterCommand::Apply {
            operation,
            reply_to,
        },
    )?;
    Ok(())
}

fn file_store_error(error: impl ToString) -> DurableError {
    DurableError::store("example-file", error.to_string())
}
