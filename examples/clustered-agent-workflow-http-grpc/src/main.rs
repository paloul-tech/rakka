#![forbid(unsafe_code)]

//! Clustered, sharded, durable agent-workflow execution behind a public ingress.
//!
//! Run several instances in separate terminals with different ports and a shared
//! discovery directory. They form one cluster, and each submitted compiled
//! workflow runs on whichever node owns its run id. Any node accepts any request
//! and routes it to the owning node over `rakka-remote` TCP.
//!
//! Each process exposes exactly one public ingress, chosen by a CLI argument:
//! `http` (default) or `grpc`. HTTP and gRPC are ingress only; node-to-node
//! communication always uses `rakka-remote`.
//!
//! The example is organized by concern: configuration, file discovery, durable
//! storage, the demo workflow and graph driver, the sharded run entity and its
//! remote registration, the protocol-neutral ingress core, the HTTP and gRPC
//! adapters, and the server bootstrap.

mod codec;
mod config;
mod discovery;
mod generated;
mod grpc;
mod http;
mod ingress;
mod model;
mod run_entity;
mod server;
mod store;
mod support;
mod workflow;

use std::env;

use support::{example_error, ExampleResult};

#[tokio::main]
async fn main() -> ExampleResult<()> {
    match env::args().nth(1).as_deref() {
        Some("grpc") => server::run_grpc().await,
        Some("http") | None => server::run_http().await,
        Some(other) => Err(example_error(format!("unknown ingress '{other}'\n{}", usage())).into()),
    }
}

fn usage() -> String {
    [
        "usage: choose exactly one public ingress",
        "  cargo run -p rakka-example-clustered-agent-workflow-http-grpc -- http",
        "  cargo run -p rakka-example-clustered-agent-workflow-http-grpc -- grpc",
        "(no argument defaults to http)",
    ]
    .join("\n")
}
