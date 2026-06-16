//! gRPC service implementation.

use std::time::Duration;

use rakka::grpc::{effective_request_timeout, validation_status, GrpcError, GrpcUnaryConfig};
use rakka::prelude::*;
use rakka::remote::RemoteRequestError;
use rakka::sharding::{RemoteEntityAskClient, RemoteEntityAskError};
use tonic::{Request, Response, Status};

use crate::counter::CounterCommand;
use crate::generated::counter_api::counter_service_server::CounterService;
use crate::generated::counter_api::{
    ChangeCounterRequest, CounterAction, CounterOperation, CounterValue, GetCounterRequest,
    InitiateCounterRequest,
};

#[derive(Clone)]
pub struct CounterGrpc {
    sharding: ClusterSharding,
    key: EntityTypeKey<CounterCommand>,
    ask_client: RemoteEntityAskClient<rakka::remote::TcpRemoteTransport>,
}

impl CounterGrpc {
    pub fn new(
        sharding: ClusterSharding,
        key: EntityTypeKey<CounterCommand>,
        ask_client: RemoteEntityAskClient<rakka::remote::TcpRemoteTransport>,
    ) -> Self {
        Self {
            sharding,
            key,
            ask_client,
        }
    }

    async fn apply(
        &self,
        operation: CounterOperation,
        timeout: Duration,
    ) -> Result<Response<CounterValue>, Status> {
        let entity = self
            .sharding
            .entity_ref_for(&self.key, operation.name.clone())
            .map_err(|error| GrpcError::service(error.to_string()).into_status())?;
        let (owner, _shard_id) = entity
            .region()
            .resolve(entity.entity_ref())
            .map_err(|error| {
                GrpcError::EntityNoRoute {
                    message: error.to_string(),
                }
                .into_status()
            })?;
        let is_local = entity
            .region()
            .local_node_id()
            .is_some_and(|local_node_id| local_node_id == &owner);

        // Local asks go straight to the entity actor. Remote asks use Rakka's
        // TCP transport but keep the same logical entity name, so callers can
        // talk to any node in the cluster.
        let value = if is_local {
            entity
                .ask(
                    |reply_to| CounterCommand::Apply {
                        operation,
                        reply_to,
                    },
                    timeout,
                )
                .await
                .map_err(|error| GrpcError::from_entity_ask(error).into_status())?
        } else {
            entity
                .remote_ask(&self.ask_client, operation, timeout)
                .await
                .map_err(remote_ask_status)?
        };
        Ok(Response::new(value))
    }
}

#[tonic::async_trait]
impl CounterService for CounterGrpc {
    async fn initiate(
        &self,
        request: Request<InitiateCounterRequest>,
    ) -> Result<Response<CounterValue>, Status> {
        let timeout = effective_request_timeout(&request, GrpcUnaryConfig::default());
        let request = request.into_inner();
        validate_counter_name(&request.name).map_err(validation_status)?;
        self.apply(
            CounterOperation {
                name: request.name,
                amount: request.initial_value,
                action: CounterAction::Initiate as i32,
            },
            timeout,
        )
        .await
    }

    async fn get(
        &self,
        request: Request<GetCounterRequest>,
    ) -> Result<Response<CounterValue>, Status> {
        let timeout = effective_request_timeout(&request, GrpcUnaryConfig::default());
        let request = request.into_inner();
        validate_counter_name(&request.name).map_err(validation_status)?;
        self.apply(
            CounterOperation {
                name: request.name,
                amount: 0,
                action: CounterAction::Get as i32,
            },
            timeout,
        )
        .await
    }

    async fn increase(
        &self,
        request: Request<ChangeCounterRequest>,
    ) -> Result<Response<CounterValue>, Status> {
        let timeout = effective_request_timeout(&request, GrpcUnaryConfig::default());
        let request = request.into_inner();
        validate_counter_name(&request.name).map_err(validation_status)?;
        validate_non_negative_amount(request.amount).map_err(validation_status)?;
        self.apply(
            CounterOperation {
                name: request.name,
                amount: request.amount,
                action: CounterAction::Increase as i32,
            },
            timeout,
        )
        .await
    }

    async fn decrease(
        &self,
        request: Request<ChangeCounterRequest>,
    ) -> Result<Response<CounterValue>, Status> {
        let timeout = effective_request_timeout(&request, GrpcUnaryConfig::default());
        let request = request.into_inner();
        validate_counter_name(&request.name).map_err(validation_status)?;
        validate_non_negative_amount(request.amount).map_err(validation_status)?;
        self.apply(
            CounterOperation {
                name: request.name,
                amount: request.amount,
                action: CounterAction::Decrease as i32,
            },
            timeout,
        )
        .await
    }
}

fn validate_counter_name(name: &str) -> Result<(), &'static str> {
    if name.is_empty() {
        return Err("counter name must not be empty");
    }
    if name.contains('|') {
        return Err("counter name must not contain '|'");
    }
    Ok(())
}

fn validate_non_negative_amount(amount: i64) -> Result<(), &'static str> {
    if amount < 0 {
        return Err("amount must be non-negative");
    }
    Ok(())
}

fn remote_ask_status(error: RemoteEntityAskError) -> Status {
    match error {
        RemoteEntityAskError::NoRoute { error } => GrpcError::EntityNoRoute {
            message: error.to_string(),
        },
        RemoteEntityAskError::Encode { error } => GrpcError::EntityRemoteEncode {
            message: error.to_string(),
        },
        RemoteEntityAskError::Register { error } => GrpcError::Service {
            message: error.to_string(),
        },
        RemoteEntityAskError::Send { message } => GrpcError::EntityRemoteSend { message },
        RemoteEntityAskError::Reply { error } => match error {
            RemoteRequestError::Timeout => GrpcError::EntityTimeout,
            RemoteRequestError::ReplyDropped => GrpcError::EntityReplyDropped,
            RemoteRequestError::Decode { error } => GrpcError::EntityRemoteEncode {
                message: error.to_string(),
            },
            other => GrpcError::Service {
                message: other.to_string(),
            },
        },
    }
    .into_status()
}
