#![forbid(unsafe_code)]

//! Phase 2 A2A agent example: clustered Rakka boot plus durable A2A handling.
//!
//! This example is intentionally an incubator, not a reusable `rakka-a2a`
//! crate. Phase 2 adds local durable A2A command acceptance and projection
//! recovery while clustered A2A routing remains deferred.

mod a2a_handler;
mod a2a_mapping;
mod agent_card;
mod codec;
mod config;
mod discovery;
mod durable_stores;
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
