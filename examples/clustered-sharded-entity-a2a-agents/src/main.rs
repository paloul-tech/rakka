#![forbid(unsafe_code)]

//! Phase 0 A2A agent example: clustered Rakka boot plus A2A router mounting.
//!
//! This example is intentionally an incubator, not a reusable `rakka-a2a`
//! crate. Phase 0 proves the A2A SDK can share one HTTP server with Rakka
//! runtime health routes while durable A2A commands remain unimplemented.

mod a2a_handler;
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
