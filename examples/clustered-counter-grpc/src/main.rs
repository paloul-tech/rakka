#![forbid(unsafe_code)]

//! Clustered, sharded, persistent counter exposed through generated gRPC.
//!
//! The example is intentionally split by concern so the Rakka flow is visible:
//! configuration builds a node id, discovery joins processes, sharding routes by
//! counter name, and the gRPC service is only the public transport boundary.

mod api;
mod client;
mod config;
mod counter;
mod discovery;
mod generated;
mod server;
mod support;

use std::env;

use support::{example_error, ExampleResult};

#[tokio::main]
async fn main() -> ExampleResult<()> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    match args.first().map(String::as_str) {
        Some("client") => client::run(&args[1..]).await,
        Some("serve") | None => server::run().await,
        _ => Err(example_error(usage()).into()),
    }
}

fn usage() -> String {
    [
        "usage:",
        "  cargo run -p rakka-example-clustered-counter-grpc",
        "  cargo run -p rakka-example-clustered-counter-grpc -- serve",
        "  cargo run -p rakka-example-clustered-counter-grpc -- client initiate <name> [initial]",
        "  cargo run -p rakka-example-clustered-counter-grpc -- client get <name>",
        "  cargo run -p rakka-example-clustered-counter-grpc -- client increase <name> [amount]",
        "  cargo run -p rakka-example-clustered-counter-grpc -- client decrease <name> [amount]",
    ]
    .join("\n")
}
