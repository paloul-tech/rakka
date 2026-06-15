#![forbid(unsafe_code)]

//! Sharded cart persistence example with recovery after shard movement.

use std::collections::BTreeMap;
use std::env;
use std::error::Error;
use std::marker::PhantomData;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rakka::prelude::*;
use rakka_cluster::{
    ClusterMembership, ClusterNode, DiscoverySnapshot, MembershipConfig, NodeAddress, NodeId,
};
use rakka_persistence::{DurableError, DurableResult, StateCodec};
use rakka_persistence_postgres::{PostgresEventJournal, PostgresSnapshotStore};
use rakka_sharding::{
    AsyncShardCoordinatorStore, ClusterShardingRuntime, ShardCoordinatorStore, ShardId,
};
use rakka_sharding_postgres::PostgresShardCoordinatorStore;
use tokio_postgres::NoTls;

type ExampleResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct CartState {
    items: BTreeMap<String, u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CartEvent {
    ItemAdded { sku: String, quantity: u32 },
}

#[derive(Debug)]
enum CartCommand {
    Add {
        sku: String,
        quantity: u32,
        reply_to: ReplyTo<u32>,
    },
    GetTotal {
        reply_to: ReplyTo<u32>,
    },
}

#[derive(Debug, Clone)]
struct MovementReport {
    backend: &'static str,
    entity_type: String,
    entity_id: String,
    persistence_id: String,
    shard_id: ShardId,
    initial_owner: String,
    moved_owner: String,
    written_total: u32,
    recovered_total: u32,
    coordinator_revision: u64,
    persisted_revision: u64,
}

struct MovementInputs<CoordinatorStore, Journal, Snapshots> {
    backend: &'static str,
    system_a: ActorSystem,
    system_b: ActorSystem,
    node_a: ClusterNode,
    node_b: ClusterNode,
    key: EntityTypeKey<CartCommand>,
    coordinator_store: CoordinatorStore,
    journal: Journal,
    snapshots: Snapshots,
}

struct CartEntity<Journal, Snapshots>
where
    Journal: EventJournal<CartEvent>,
    Snapshots: SnapshotStore<CartState>,
{
    child: ActorRef<CartCommand>,
    _stores: PhantomData<fn() -> (Journal, Snapshots)>,
}

impl<Journal, Snapshots> CartEntity<Journal, Snapshots>
where
    Journal: EventJournal<CartEvent>,
    Snapshots: SnapshotStore<CartState>,
{
    fn new(
        system: ActorSystem,
        context: EntityContext<CartCommand>,
        journal: Journal,
        snapshots: Snapshots,
    ) -> Self {
        let persistence_id =
            PersistenceId::of(context.entity_type().as_str(), context.entity_id().as_str())
                .expect("sharded cart persistence id should be valid");
        let actor_name = format!("{}-persistent", context.actor_name());
        let child = cart_behavior(persistence_id)
            .spawn(&system, actor_name, journal, snapshots)
            .expect("sharded cart persistent child should spawn");

        Self {
            child,
            _stores: PhantomData,
        }
    }
}

impl<Journal, Snapshots> Actor for CartEntity<Journal, Snapshots>
where
    Journal: EventJournal<CartEvent>,
    Snapshots: SnapshotStore<CartState>,
{
    type Msg = CartCommand;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        let child = self.child.clone();
        actor_future(async move {
            child
                .tell(msg)
                .map_err(|error| RakkaError::core("cart-forward-failed", format!("{error:?}")))?;
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

#[derive(Debug, Clone, Copy)]
struct CartEventCodec;

impl StateCodec<CartEvent> for CartEventCodec {
    fn encode(&self, event: &CartEvent) -> DurableResult<Vec<u8>> {
        match event {
            CartEvent::ItemAdded { sku, quantity } => {
                Ok(format!("item-added\n{sku}\n{quantity}").into_bytes())
            }
        }
    }

    fn decode(&self, bytes: &[u8]) -> DurableResult<CartEvent> {
        let text =
            std::str::from_utf8(bytes).map_err(|error| DurableError::codec(error.to_string()))?;
        let mut lines = text.lines();
        let kind = lines
            .next()
            .ok_or_else(|| DurableError::codec("missing cart event kind"))?;
        let sku = lines
            .next()
            .ok_or_else(|| DurableError::codec("missing cart item sku"))?;
        let quantity = lines
            .next()
            .ok_or_else(|| DurableError::codec("missing cart item quantity"))?
            .parse::<u32>()
            .map_err(|error| DurableError::codec(error.to_string()))?;
        if kind != "item-added" || lines.next().is_some() {
            return Err(DurableError::codec("invalid cart event payload"));
        }

        Ok(CartEvent::ItemAdded {
            sku: sku.to_string(),
            quantity,
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct CartStateCodec;

impl StateCodec<CartState> for CartStateCodec {
    fn encode(&self, state: &CartState) -> DurableResult<Vec<u8>> {
        let mut encoded = String::new();
        for (sku, quantity) in &state.items {
            encoded.push_str(sku);
            encoded.push('\t');
            encoded.push_str(&quantity.to_string());
            encoded.push('\n');
        }
        Ok(encoded.into_bytes())
    }

    fn decode(&self, bytes: &[u8]) -> DurableResult<CartState> {
        let text =
            std::str::from_utf8(bytes).map_err(|error| DurableError::codec(error.to_string()))?;
        let mut items = BTreeMap::new();
        for line in text.lines() {
            let (sku, quantity) = line
                .split_once('\t')
                .ok_or_else(|| DurableError::codec("invalid cart state payload"))?;
            let quantity = quantity
                .parse::<u32>()
                .map_err(|error| DurableError::codec(error.to_string()))?;
            items.insert(sku.to_string(), quantity);
        }
        Ok(CartState { items })
    }
}

#[tokio::main]
async fn main() -> ExampleResult<()> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    let report = match args.as_slice() {
        [] => run_in_memory_movement_scenario().await?,
        [mode] if mode == "--postgres" => {
            let dsn = env::var("RAKKA_POSTGRES_TEST_DSN").map_err(|_error| {
                example_error("set RAKKA_POSTGRES_TEST_DSN to run the PostgreSQL movement example")
            })?;
            run_postgres_movement_scenario(&dsn).await?
        }
        _ => {
            return Err(example_error(
                "usage: cargo run -p rakka-example-sharded-cart-persistence [--postgres]",
            ));
        }
    };

    print_report(&report);
    Ok(())
}

async fn run_in_memory_movement_scenario() -> ExampleResult<MovementReport> {
    let system_a = ActorSystem::new("cart-node-a");
    let system_b = ActorSystem::new("cart-node-b");
    let (node_a, node_b) = example_nodes();
    let key = EntityTypeKey::new("CartMovement").with_number_of_shards(8)?;
    let coordinator_store = InMemoryShardCoordinatorStore::new();
    let journal = InMemoryEventJournal::<CartEvent>::new();
    let snapshots = InMemorySnapshotStore::<CartState>::new();

    let (report, terminate_a, terminate_b) = run_movement_with_sync_coordinator(MovementInputs {
        backend: "in-memory",
        system_a,
        system_b,
        node_a,
        node_b,
        key,
        coordinator_store,
        journal,
        snapshots,
    })
    .await?;

    terminate_a.terminate().await?;
    terminate_b.terminate().await?;
    Ok(report)
}

async fn run_movement_with_sync_coordinator<Journal, Snapshots>(
    inputs: MovementInputs<InMemoryShardCoordinatorStore, Journal, Snapshots>,
) -> ExampleResult<(MovementReport, ActorSystem, ActorSystem)>
where
    Journal: EventJournal<CartEvent>,
    Snapshots: SnapshotStore<CartState>,
{
    let MovementInputs {
        backend,
        system_a,
        system_b,
        node_a,
        node_b,
        key,
        coordinator_store,
        journal,
        snapshots,
    } = inputs;
    let membership_a = membership_for(&node_a, [&node_a, &node_b])?;
    let membership_b = membership_for(&node_b, [&node_a, &node_b])?;
    let sharding_a = ClusterSharding::from_membership_with_coordinator_store(
        &system_a,
        node_a.clone(),
        membership_a,
        coordinator_store.clone(),
    );
    let sharding_b = ClusterSharding::from_membership_with_coordinator_store(
        &system_b,
        node_b.clone(),
        membership_b,
        coordinator_store.clone(),
    );

    let registration_a = init_cart_entity(
        &sharding_a,
        &system_a,
        key.clone(),
        journal.clone(),
        snapshots.clone(),
    )?;
    let registration_b = init_cart_entity(&sharding_b, &system_b, key.clone(), journal, snapshots)?;
    let runtime = sharding_a.runtime();
    runtime
        .lock()
        .await
        .register_region(registration_b.region().clone())?;

    let (entity_id, shard_id, initial_owner, initial_revision) = {
        let runtime = runtime.lock().await;
        entity_owned_by(&runtime, &key, node_a.id())?
    };
    let cart_on_a = registration_a.entity_ref_for(entity_id.as_str());
    let written_total = add(&cart_on_a, "apple", 2).await?;

    {
        runtime.lock().await.mark_leaving(node_a.id(), 3)?;
    }
    tokio::time::sleep(Duration::from_millis(25)).await;

    let (moved_owner, coordinator_revision) = {
        let runtime = runtime.lock().await;
        owner_and_revision(&runtime, &key, &entity_id)?
    };
    if moved_owner != *node_b.id() {
        return Err(example_error(format!(
            "expected shard {shard_id} to move to {}, but owner is {moved_owner}",
            node_b.id()
        )));
    }

    let cart_on_b = registration_b.entity_ref_for(entity_id.as_str());
    let recovered_total = get_total(&cart_on_b).await?;
    let persisted = ShardCoordinatorStore::load(&coordinator_store, key.entity_type())?
        .ok_or_else(|| example_error("coordinator state was not persisted"))?;
    let report = MovementReport {
        backend,
        entity_type: key.entity_type().to_string(),
        entity_id: entity_id.to_string(),
        persistence_id: PersistenceId::of(key.entity_type().as_str(), entity_id.as_str())?
            .to_string(),
        shard_id,
        initial_owner: initial_owner.to_string(),
        moved_owner: moved_owner.to_string(),
        written_total,
        recovered_total,
        coordinator_revision: coordinator_revision.max(initial_revision),
        persisted_revision: persisted.snapshot().revision(),
    };

    Ok((report, system_a, system_b))
}

async fn run_postgres_movement_scenario(dsn: &str) -> ExampleResult<MovementReport> {
    let system_a = ActorSystem::new("cart-node-a-postgres");
    let system_b = ActorSystem::new("cart-node-b-postgres");
    let (node_a, node_b) = example_nodes();
    let namespace = unique_name("cart-movement");
    let entity_type_name = format!("CartMovement{}", namespace.replace('-', "_"));
    let key = EntityTypeKey::new(entity_type_name).with_number_of_shards(8)?;

    let coordinator = PostgresShardCoordinatorStore::builder(connect_postgres(dsn).await?)
        .with_namespace(namespace)
        .migrate()
        .await?;
    let journal = PostgresEventJournal::new(connect_postgres(dsn).await?, CartEventCodec);
    journal.migrate().await?;
    let snapshots = PostgresSnapshotStore::new(connect_postgres(dsn).await?, CartStateCodec);
    snapshots.migrate().await?;

    let (report, terminate_a, terminate_b) = run_movement_with_async_coordinator(MovementInputs {
        backend: "postgres",
        system_a,
        system_b,
        node_a,
        node_b,
        key,
        coordinator_store: coordinator,
        journal,
        snapshots,
    })
    .await?;

    terminate_a.terminate().await?;
    terminate_b.terminate().await?;
    Ok(report)
}

async fn run_movement_with_async_coordinator<Journal, Snapshots>(
    inputs: MovementInputs<PostgresShardCoordinatorStore, Journal, Snapshots>,
) -> ExampleResult<(MovementReport, ActorSystem, ActorSystem)>
where
    Journal: EventJournal<CartEvent>,
    Snapshots: SnapshotStore<CartState>,
{
    let MovementInputs {
        backend,
        system_a,
        system_b,
        node_a,
        node_b,
        key,
        coordinator_store,
        journal,
        snapshots,
    } = inputs;
    let membership_a = membership_for(&node_a, [&node_a, &node_b])?;
    let membership_b = membership_for(&node_b, [&node_a, &node_b])?;
    let sharding_a = ClusterSharding::from_membership_with_async_coordinator_store(
        &system_a,
        node_a.clone(),
        membership_a,
        coordinator_store.clone(),
    );
    let sharding_b = ClusterSharding::from_membership_with_async_coordinator_store(
        &system_b,
        node_b.clone(),
        membership_b,
        coordinator_store.clone(),
    );

    let registration_a = init_cart_entity_async(
        &sharding_a,
        &system_a,
        key.clone(),
        journal.clone(),
        snapshots.clone(),
    )
    .await?;
    let registration_b =
        init_cart_entity_async(&sharding_b, &system_b, key.clone(), journal, snapshots).await?;
    let runtime = sharding_a.runtime();
    runtime
        .lock()
        .await
        .register_region_async(registration_b.region().clone())
        .await?;

    let (entity_id, shard_id, initial_owner, initial_revision) = {
        let runtime = runtime.lock().await;
        entity_owned_by(&runtime, &key, node_a.id())?
    };
    let cart_on_a = registration_a.entity_ref_for(entity_id.as_str());
    let written_total = add(&cart_on_a, "apple", 2).await?;

    {
        runtime
            .lock()
            .await
            .mark_leaving_async(node_a.id(), 3)
            .await?;
    }
    tokio::time::sleep(Duration::from_millis(25)).await;

    let (moved_owner, coordinator_revision) = {
        let runtime = runtime.lock().await;
        owner_and_revision(&runtime, &key, &entity_id)?
    };
    if moved_owner != *node_b.id() {
        return Err(example_error(format!(
            "expected shard {shard_id} to move to {}, but owner is {moved_owner}",
            node_b.id()
        )));
    }

    let cart_on_b = registration_b.entity_ref_for(entity_id.as_str());
    let recovered_total = get_total(&cart_on_b).await?;
    let persisted = AsyncShardCoordinatorStore::load(&coordinator_store, key.entity_type())
        .await?
        .ok_or_else(|| example_error("coordinator state was not persisted"))?;
    let report = MovementReport {
        backend,
        entity_type: key.entity_type().to_string(),
        entity_id: entity_id.to_string(),
        persistence_id: PersistenceId::of(key.entity_type().as_str(), entity_id.as_str())?
            .to_string(),
        shard_id,
        initial_owner: initial_owner.to_string(),
        moved_owner: moved_owner.to_string(),
        written_total,
        recovered_total,
        coordinator_revision: coordinator_revision.max(initial_revision),
        persisted_revision: persisted.snapshot().revision(),
    };

    Ok((report, system_a, system_b))
}

fn init_cart_entity<Journal, Snapshots>(
    sharding: &ClusterSharding,
    system: &ActorSystem,
    key: EntityTypeKey<CartCommand>,
    journal: Journal,
    snapshots: Snapshots,
) -> ExampleResult<EntityTypeRegistration<CartCommand>>
where
    Journal: EventJournal<CartEvent>,
    Snapshots: SnapshotStore<CartState>,
{
    let system = system.clone();
    Ok(sharding.init(Entity::of(key, move |context| {
        CartEntity::new(system.clone(), context, journal.clone(), snapshots.clone())
    }))?)
}

async fn init_cart_entity_async<Journal, Snapshots>(
    sharding: &ClusterSharding,
    system: &ActorSystem,
    key: EntityTypeKey<CartCommand>,
    journal: Journal,
    snapshots: Snapshots,
) -> ExampleResult<EntityTypeRegistration<CartCommand>>
where
    Journal: EventJournal<CartEvent>,
    Snapshots: SnapshotStore<CartState>,
{
    let system = system.clone();
    Ok(sharding
        .init_async(Entity::of(key, move |context| {
            CartEntity::new(system.clone(), context, journal.clone(), snapshots.clone())
        }))
        .await?)
}

fn cart_behavior(
    persistence_id: PersistenceId,
) -> EventSourcedBehavior<CartCommand, CartEvent, CartState> {
    EventSourcedBehavior::builder(persistence_id, CartState::default())
        .on_command(|state, command| match command {
            CartCommand::Add {
                sku,
                quantity,
                reply_to,
            } => {
                let total = state.items.values().sum::<u32>() + quantity;
                EventSourcedEffect::persist_tagged(TaggedEvent::with_tags(
                    CartEvent::ItemAdded { sku, quantity },
                    ["cart"],
                ))
                .then_reply(reply_to, total)
            }
            CartCommand::GetTotal { reply_to } => {
                EventSourcedEffect::reply(reply_to, state.items.values().sum::<u32>())
            }
        })
        .on_event(|state, event| {
            let mut next = state.clone();
            match event {
                CartEvent::ItemAdded { sku, quantity } => {
                    *next.items.entry(sku.clone()).or_default() += quantity;
                }
            }
            next
        })
        .build()
        .expect("cart event-sourced behavior should build")
}

async fn add(
    cart: &ShardedEntityRef<CartCommand>,
    sku: impl Into<String>,
    quantity: u32,
) -> ExampleResult<u32> {
    let sku = sku.into();
    Ok(cart
        .ask(
            |reply_to| CartCommand::Add {
                sku,
                quantity,
                reply_to,
            },
            Duration::from_secs(1),
        )
        .await?)
}

async fn get_total(cart: &ShardedEntityRef<CartCommand>) -> ExampleResult<u32> {
    Ok(cart
        .ask(
            |reply_to| CartCommand::GetTotal { reply_to },
            Duration::from_secs(1),
        )
        .await?)
}

fn example_nodes() -> (ClusterNode, ClusterNode) {
    (
        ClusterNode::new(
            NodeId::new("rakka-0", "uid-a"),
            NodeAddress::new("127.0.0.1", 25520),
        ),
        ClusterNode::new(
            NodeId::new("rakka-1", "uid-b"),
            NodeAddress::new("127.0.0.1", 25521),
        ),
    )
}

fn membership_for<'a>(
    local_node: &ClusterNode,
    nodes: impl IntoIterator<Item = &'a ClusterNode>,
) -> ExampleResult<ClusterMembership> {
    let nodes = nodes.into_iter().cloned().collect::<Vec<_>>();
    let mut membership = ClusterMembership::new(
        local_node.clone(),
        MembershipConfig::new(
            nodes.len(),
            Duration::from_secs(10),
            Duration::from_secs(30),
        ),
    );
    membership.record_discovery(DiscoverySnapshot::new("cart-example", 1, nodes.clone()))?;
    for node in nodes {
        membership.mark_up(node.id(), 2)?;
    }
    Ok(membership)
}

fn entity_owned_by(
    runtime: &ClusterShardingRuntime,
    key: &EntityTypeKey<CartCommand>,
    owner: &NodeId,
) -> ExampleResult<(EntityId, ShardId, NodeId, u64)> {
    let coordinator = runtime
        .coordinator(key.entity_type())
        .ok_or_else(|| example_error("cart coordinator was not initialized"))?;
    for index in 0..256 {
        let entity_id = EntityId::new(format!("cart-{index}"));
        let entity_owner = coordinator.owner_for_entity(&entity_id)?;
        if entity_owner == owner {
            let shard_id = coordinator.shard_for_entity(&entity_id);
            return Ok((
                entity_id,
                shard_id,
                entity_owner.clone(),
                coordinator.revision(),
            ));
        }
    }

    Err(example_error(format!(
        "no candidate cart entity was initially owned by {owner}"
    )))
}

fn owner_and_revision(
    runtime: &ClusterShardingRuntime,
    key: &EntityTypeKey<CartCommand>,
    entity_id: &EntityId,
) -> ExampleResult<(NodeId, u64)> {
    let coordinator = runtime
        .coordinator(key.entity_type())
        .ok_or_else(|| example_error("cart coordinator was not initialized"))?;
    Ok((
        coordinator.owner_for_entity(entity_id)?.clone(),
        coordinator.revision(),
    ))
}

async fn connect_postgres(dsn: &str) -> ExampleResult<tokio_postgres::Client> {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("postgres connection task failed: {error}");
        }
    });
    Ok(client)
}

fn unique_name(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{prefix}-{}-{nanos}", std::process::id())
}

fn print_report(report: &MovementReport) {
    println!(
        "Rakka sharded cart movement ({}) used entity type {} and persistence id {}.",
        report.backend, report.entity_type, report.persistence_id
    );
    println!(
        "node A initially owned {} on shard {} and wrote cart total {}.",
        report.entity_id, report.shard_id, report.written_total
    );
    println!(
        "ownership moved from {} to {} at coordinator revision {}.",
        report.initial_owner, report.moved_owner, report.coordinator_revision
    );
    println!(
        "node B recovered cart total {} from persistence; persisted coordinator revision {} was reloadable.",
        report.recovered_total, report.persisted_revision
    );
}

fn example_error(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(std::io::Error::other(message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn in_memory_movement_recovers_state_after_shard_moves() {
        let report = run_in_memory_movement_scenario()
            .await
            .expect("in-memory movement should recover cart state");

        assert_ne!(report.initial_owner, report.moved_owner);
        assert_eq!(report.written_total, 2);
        assert_eq!(report.recovered_total, report.written_total);
        assert!(report.persisted_revision >= report.coordinator_revision);
    }

    #[tokio::test]
    async fn postgres_movement_recovers_state_when_dsn_is_set() {
        let Ok(dsn) = env::var("RAKKA_POSTGRES_TEST_DSN") else {
            return;
        };
        let report = run_postgres_movement_scenario(&dsn)
            .await
            .expect("postgres movement should recover cart state");

        assert_ne!(report.initial_owner, report.moved_owner);
        assert_eq!(report.written_total, 2);
        assert_eq!(report.recovered_total, report.written_total);
        assert!(report.persisted_revision >= report.coordinator_revision);
    }
}
