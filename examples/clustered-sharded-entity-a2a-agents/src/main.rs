#![forbid(unsafe_code)]

//! Phase 3 A2A agent example: clustered Rakka boot plus durable A2A handling.
//!
//! This example is intentionally an incubator, not a reusable `rakka-a2a`
//! crate. Phase 3 adds owner-routed sharded run hosting while preserving the
//! public A2A request-handler boundary.

mod a2a_handler;
mod a2a_mapping;
mod agent_card;
mod codec;
mod config;
mod discovery;
mod durable_stores;
mod etcd_discovery;
mod protocol;
mod reachability;
mod server;
mod sharded_run_entity;
mod support;
mod task_projection;
mod workflow;

use support::ExampleResult;

#[tokio::main]
async fn main() -> ExampleResult<()> {
    server::run().await
}
