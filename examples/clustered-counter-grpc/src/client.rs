//! Tiny command-line gRPC client used by the README examples.

use std::env;

use tonic::Request;

use crate::generated::counter_api::counter_service_client::CounterServiceClient;
use crate::generated::counter_api::{
    ChangeCounterRequest, GetCounterRequest, InitiateCounterRequest,
};
use crate::support::{env_u16, example_error, ExampleResult, DEFAULT_RAKKA_TCP_PORT};

pub async fn run(args: &[String]) -> ExampleResult<()> {
    let endpoint = env::var("RAKKA_GRPC_ENDPOINT").unwrap_or_else(|_| {
        let port = env_u16(
            "RAKKA_GRPC_PORT",
            DEFAULT_RAKKA_TCP_PORT.saturating_add(10_000),
        )
        .unwrap_or(DEFAULT_RAKKA_TCP_PORT.saturating_add(10_000));
        format!("http://127.0.0.1:{port}")
    });
    let mut client = CounterServiceClient::connect(endpoint).await?;
    let value = match args {
        [operation, name] if operation == "initiate" => client
            .initiate(Request::new(InitiateCounterRequest {
                name: name.clone(),
                initial_value: 0,
            }))
            .await?
            .into_inner(),
        [operation, name, value] if operation == "initiate" => client
            .initiate(Request::new(InitiateCounterRequest {
                name: name.clone(),
                initial_value: value.parse()?,
            }))
            .await?
            .into_inner(),
        [operation, name] if operation == "get" => client
            .get(Request::new(GetCounterRequest { name: name.clone() }))
            .await?
            .into_inner(),
        [operation, name] if operation == "increase" => client
            .increase(Request::new(ChangeCounterRequest {
                name: name.clone(),
                amount: 1,
            }))
            .await?
            .into_inner(),
        [operation, name, amount] if operation == "increase" => client
            .increase(Request::new(ChangeCounterRequest {
                name: name.clone(),
                amount: amount.parse()?,
            }))
            .await?
            .into_inner(),
        [operation, name] if operation == "decrease" => client
            .decrease(Request::new(ChangeCounterRequest {
                name: name.clone(),
                amount: 1,
            }))
            .await?
            .into_inner(),
        [operation, name, amount] if operation == "decrease" => client
            .decrease(Request::new(ChangeCounterRequest {
                name: name.clone(),
                amount: amount.parse()?,
            }))
            .await?
            .into_inner(),
        _ => return Err(example_error(crate::usage()).into()),
    };

    println!(
        "{}={} revision={} initialized={} created={} owner={}",
        value.name, value.value, value.revision, value.initialized, value.created, value.owner_node
    );
    Ok(())
}
