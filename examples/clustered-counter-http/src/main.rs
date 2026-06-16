#![forbid(unsafe_code)]

//! Clustered, sharded, persistent counter exposed through REST/JSON.
//!
//! The example is organized around the Rakka concepts it demonstrates:
//! configuration, discovery, durable entity behavior, cluster boot, REST routes,
//! and the tiny CLI client used by the README.

mod api;
mod client;
mod codec;
mod config;
mod counter;
mod discovery;
mod http_client;
mod model;
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
        "  cargo run -p rakka-example-clustered-counter-http",
        "  cargo run -p rakka-example-clustered-counter-http -- serve",
        "  cargo run -p rakka-example-clustered-counter-http -- client initiate <name> [initial]",
        "  cargo run -p rakka-example-clustered-counter-http -- client get <name>",
        "  cargo run -p rakka-example-clustered-counter-http -- client increase <name> [amount]",
        "  cargo run -p rakka-example-clustered-counter-http -- client decrease <name> [amount]",
    ]
    .join("\n")
}
