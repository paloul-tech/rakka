//! Tiny command-line HTTP client used by the README examples.

use std::env;

use crate::http_client::{get_counter_json, post_counter_json};
use crate::model::{ChangeCounterRequest, InitiateCounterRequest};
use crate::support::{env_u16, example_error, ExampleResult, DEFAULT_RAKKA_TCP_PORT};

pub async fn run(args: &[String]) -> ExampleResult<()> {
    let endpoint = env::var("RAKKA_HTTP_ENDPOINT").unwrap_or_else(|_| {
        let port = env_u16(
            "RAKKA_HTTP_PORT",
            DEFAULT_RAKKA_TCP_PORT.saturating_add(10_000),
        )
        .unwrap_or(DEFAULT_RAKKA_TCP_PORT.saturating_add(10_000));
        format!("http://127.0.0.1:{port}")
    });
    let value = match args {
        [operation, name] if operation == "initiate" => {
            post_counter_json(
                &endpoint,
                &format!("/counters/{name}/initiate"),
                &InitiateCounterRequest { initial_value: 0 },
            )
            .await?
        }
        [operation, name, value] if operation == "initiate" => {
            post_counter_json(
                &endpoint,
                &format!("/counters/{name}/initiate"),
                &InitiateCounterRequest {
                    initial_value: value.parse()?,
                },
            )
            .await?
        }
        [operation, name] if operation == "get" => {
            get_counter_json(&endpoint, &format!("/counters/{name}")).await?
        }
        [operation, name] if operation == "increase" => {
            post_counter_json(
                &endpoint,
                &format!("/counters/{name}/increase"),
                &ChangeCounterRequest { amount: 1 },
            )
            .await?
        }
        [operation, name, amount] if operation == "increase" => {
            post_counter_json(
                &endpoint,
                &format!("/counters/{name}/increase"),
                &ChangeCounterRequest {
                    amount: amount.parse()?,
                },
            )
            .await?
        }
        [operation, name] if operation == "decrease" => {
            post_counter_json(
                &endpoint,
                &format!("/counters/{name}/decrease"),
                &ChangeCounterRequest { amount: 1 },
            )
            .await?
        }
        [operation, name, amount] if operation == "decrease" => {
            post_counter_json(
                &endpoint,
                &format!("/counters/{name}/decrease"),
                &ChangeCounterRequest {
                    amount: amount.parse()?,
                },
            )
            .await?
        }
        _ => return Err(example_error(crate::usage()).into()),
    };

    println!(
        "{}={} revision={} initialized={} created={} owner={}",
        value.name, value.value, value.revision, value.initialized, value.created, value.owner_node
    );
    Ok(())
}
