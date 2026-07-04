#![forbid(unsafe_code)]

//! Phase 1 A2A agent example: clustered Rakka boot plus A2A conversion mapping.
//!
//! This example is intentionally an incubator, not a reusable `rakka-a2a`
//! crate. Phase 1 adds command-draft conversion and task projection boundaries
//! while durable A2A command acceptance remains unimplemented.

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
