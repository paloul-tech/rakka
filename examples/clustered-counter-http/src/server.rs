//! Rakka runtime and HTTP server bootstrap.

use std::sync::Arc;
use std::time::Duration;

use rakka::cluster::MembershipConfig;
use rakka::http::{serve_with_graceful_shutdown, HttpServerConfig};
use rakka::prelude::*;
use rakka::remote::{SerializationRegistry, TcpRemoteTransportConfig};
use rakka::sharding::ClusterNodeRuntime;
use tokio::sync::Mutex as AsyncMutex;

use crate::api::{counter_router, CounterHttp};
use crate::codec::JsonPayloadCodec;
use crate::config::ExampleConfig;
use crate::counter::{init_counter_entity, CounterCommand, FileCounterStateStore};
use crate::discovery::{discovery_loop, publish_and_apply_discovery, remove_discovery_record};
use crate::model::{CounterOperation, CounterValue};
use crate::support::{
    current_timestamp_millis, ExampleResult, DEFAULT_CONNECT_TIMEOUT, DEFAULT_IDLE_TIMEOUT,
    DEFAULT_RECONNECT_BACKOFF, ENTITY_TYPE,
};

pub async fn run() -> ExampleResult<()> {
    let config = ExampleConfig::from_env()?;
    let local_node = config.local_node();
    let system = ActorSystem::new(format!("clustered-counter-http-{}", config.node_logical_id));

    // REST handles JSON at the edge, and this registry teaches Rakka remoting
    // how to move the same operation/reply payloads between cluster nodes.
    let mut registry = SerializationRegistry::new();
    registry.register::<CounterOperation, _>(JsonPayloadCodec::<CounterOperation>::new(
        "rakka.examples.clustered_counter_http.CounterOperation",
    ))?;
    registry.register::<CounterValue, _>(JsonPayloadCodec::<CounterValue>::new(
        "rakka.examples.clustered_counter_http.CounterValue",
    ))?;

    let mut runtime = ClusterNodeRuntime::builder(local_node.clone())
        .with_membership_config(MembershipConfig::new(
            1,
            Duration::from_secs(10),
            Duration::from_secs(30),
        ))
        .with_transport_config(
            TcpRemoteTransportConfig::new()
                .bind_addr(config.tcp_bind_addr())
                .connect_timeout(DEFAULT_CONNECT_TIMEOUT)
                .reconnect_backoff(DEFAULT_RECONNECT_BACKOFF)
                .idle_timeout(DEFAULT_IDLE_TIMEOUT),
        )
        .with_registry(registry)
        .build()
        .await?;

    // The application-facing facade keeps the example compact: one runtime for
    // membership/remoting, one sharding facade for stable entity references.
    let ask_client = runtime.ask_client();
    let sharding = ClusterSharding::for_node_runtime(&system, &runtime)?;
    let key = EntityTypeKey::<CounterCommand>::new(ENTITY_TYPE).with_number_of_shards(32)?;
    init_counter_entity(
        system.clone(),
        &mut runtime,
        &sharding,
        key.clone(),
        FileCounterStateStore::new(config.counter_store_dir.clone()),
        local_node.id().to_string(),
    )?;

    publish_and_apply_discovery(&config, &local_node, &mut runtime)?;
    let runtime = Arc::new(AsyncMutex::new(runtime));
    let discovery_task = tokio::spawn(discovery_loop(
        runtime.clone(),
        config.clone(),
        local_node.clone(),
    ));
    let app = CounterHttp::new(sharding, key, ask_client);
    let http_addr = config.http_bind_addr();
    let router = counter_router(app);

    println!(
        "Rakka clustered counter HTTP node {} listening: remoting {} / HTTP {}",
        local_node.id(),
        config.tcp_bind_addr(),
        http_addr
    );
    println!(
        "Discovery dir: {}; counter state dir: {}",
        config.discovery_dir.display(),
        config.counter_store_dir.display()
    );

    serve_with_graceful_shutdown(router, HttpServerConfig::new(http_addr), async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await?;

    discovery_task.abort();
    let _ = remove_discovery_record(&config.discovery_dir, local_node.id().logical_id());
    if let Ok(mut runtime) = runtime.try_lock() {
        let _ = runtime.leave_local(current_timestamp_millis());
    }
    system.terminate().await?;
    Ok(())
}
