#![forbid(unsafe_code)]

//! Clustered sharded-entity A2A agent example.
//!
//! This example is intentionally an incubator, not a reusable `rakka-a2a`
//! crate. It preserves the public A2A request-handler boundary while routing
//! durable run ownership through Rakka cluster sharding.

mod a2a_handler;
mod a2a_mapping;
mod agent_card;
#[cfg(test)]
mod cluster_tests;
mod codec;
mod config;
mod discovery;
mod durable_stores;
mod etcd_discovery;
mod protocol;
mod push_config;
mod reachability;
mod server;
mod sharded_run_entity;
mod stream_limits;
mod support;
mod task_projection;
mod workflow;

use support::ExampleResult;

#[tokio::main]
async fn main() -> ExampleResult<()> {
    server::run().await
}
