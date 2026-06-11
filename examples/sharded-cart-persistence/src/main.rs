#![forbid(unsafe_code)]

//! Sharded cart persistence example using entity ids and event-sourced behavior.

use std::collections::BTreeMap;
use std::error::Error;
use std::time::Duration;

use rakka::prelude::*;
use rakka::sharding::ShardId;

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let system = ActorSystem::new("sharded-cart-persistence");
    let entity_type = EntityType::new("Cart");
    let shard_config = ShardingConfig::new(8)?;
    let cart_a = EntityId::new("cart-a");
    let cart_b = EntityId::new("cart-b");
    let cart_a_shard = ShardId::for_entity(&entity_type, &cart_a, &shard_config);
    let cart_b_shard = ShardId::for_entity(&entity_type, &cart_b, &shard_config);

    let cart_a_actor = spawn_cart(&system, &entity_type, &cart_a)?;
    let cart_b_actor = spawn_cart(&system, &entity_type, &cart_b)?;

    add(&cart_a_actor, "apple", 2).await?;
    let b_total = add(&cart_b_actor, "banana", 1).await?;
    let a_total = get_total(&cart_a_actor).await?;

    println!(
        "Rakka sharded cart persistence wrote cart-a total {a_total} on shard {cart_a_shard} and cart-b total {b_total} on shard {cart_b_shard}."
    );
    system.terminate().await?;
    Ok(())
}

fn spawn_cart(
    system: &ActorSystem,
    entity_type: &EntityType,
    entity_id: &EntityId,
) -> Result<ActorRef<CartCommand>, Box<dyn Error>> {
    let persistence_id = PersistenceId::of(entity_type.as_str(), entity_id.as_str())?;
    let journal = InMemoryEventJournal::<CartEvent>::new();
    let snapshots = InMemorySnapshotStore::<CartState>::new();
    let behavior = EventSourcedBehavior::builder(
        persistence_id,
        CartState {
            items: BTreeMap::new(),
        },
    )
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
    .build()?;

    Ok(behavior.spawn(system, entity_id.as_str(), journal, snapshots)?)
}

async fn add(
    cart: &ActorRef<CartCommand>,
    sku: impl Into<String>,
    quantity: u32,
) -> Result<u32, Box<dyn Error>> {
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

async fn get_total(cart: &ActorRef<CartCommand>) -> Result<u32, Box<dyn Error>> {
    Ok(cart
        .ask(
            |reply_to| CartCommand::GetTotal { reply_to },
            Duration::from_secs(1),
        )
        .await?)
}
