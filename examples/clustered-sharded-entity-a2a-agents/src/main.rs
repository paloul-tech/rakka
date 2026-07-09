#![forbid(unsafe_code)]

//! Clustered sharded-entity A2A agent example.
//!
//! This example is a thin product composition over the reusable `rakka-a2a`
//! crate: it supplies a demo workflow, environment configuration, file/etcd
//! discovery, Kubernetes manifests, and local run instructions, while all the
//! reusable A2A adapter behavior (durable request handling, task projection,
//! streaming replay, push persistence, and sharded run ownership) lives in
//! `rakka-a2a`.

mod config;
mod discovery;
mod durable_stores;
mod etcd_discovery;
mod reachability;
mod server;
mod support;
mod workflow;

use support::ExampleResult;

#[tokio::main]
async fn main() -> ExampleResult<()> {
    server::run().await
}
