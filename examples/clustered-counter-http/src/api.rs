//! REST route handlers and Rakka entity routing.

use std::time::Duration;

use axum::extract::{Path as AxumPath, State};
use axum::routing::{get, post};
use axum::Json;
use rakka::http::{HttpError, HttpRouteConfig};
use rakka::prelude::*;
use rakka::remote::RemoteRequestError;
use rakka::sharding::{EntityAskError, RemoteEntityAskClient, RemoteEntityAskError};

use crate::counter::CounterCommand;
use crate::model::{
    ChangeCounterRequest, CounterAction, CounterOperation, CounterValue, InitiateCounterRequest,
};

#[derive(Clone)]
pub struct CounterHttp {
    sharding: ClusterSharding,
    key: EntityTypeKey<CounterCommand>,
    ask_client: RemoteEntityAskClient<rakka::remote::TcpRemoteTransport>,
}

impl CounterHttp {
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
    ) -> Result<CounterValue, HttpError> {
        let entity = self
            .sharding
            .entity_ref_for(&self.key, operation.name.clone())
            .map_err(|error| HttpError::service(error.to_string()))?;
        let (owner, _shard_id) = entity
            .region()
            .resolve(entity.entity_ref())
            .map_err(|error| HttpError::EntityNoRoute {
                message: error.to_string(),
            })?;
        let is_local = entity
            .region()
            .local_node_id()
            .is_some_and(|local_node_id| local_node_id == &owner);

        // Local asks go straight to the entity actor. Remote asks use Rakka's
        // TCP transport but keep the same logical entity name, so callers can
        // talk to any HTTP node in the cluster.
        if is_local {
            entity
                .ask(
                    |reply_to| CounterCommand::Apply {
                        operation,
                        reply_to,
                    },
                    timeout,
                )
                .await
                .map_err(entity_ask_http_error)
        } else {
            entity
                .remote_ask(&self.ask_client, operation, timeout)
                .await
                .map_err(remote_ask_http_error)
        }
    }
}

pub fn counter_router(app: CounterHttp) -> rakka::http::Router {
    counter_routes().with_state(app)
}

// Kept state-free so tests can construct the route table without booting the
// cluster; axum validates path syntax inside `route()`.
fn counter_routes() -> rakka::http::Router<CounterHttp> {
    rakka::http::Router::new()
        .route("/counters/{name}", get(get_counter))
        .route("/counters/{name}/initiate", post(initiate_counter))
        .route("/counters/{name}/increase", post(increase_counter))
        .route("/counters/{name}/decrease", post(decrease_counter))
}

async fn get_counter(
    State(app): State<CounterHttp>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<CounterValue>, HttpError> {
    validate_counter_name(&name)?;
    let value = app
        .apply(
            CounterOperation {
                name,
                amount: 0,
                action: CounterAction::Get,
            },
            HttpRouteConfig::default().request_timeout_value(),
        )
        .await?;
    Ok(Json(value))
}

async fn initiate_counter(
    State(app): State<CounterHttp>,
    AxumPath(name): AxumPath<String>,
    Json(request): Json<InitiateCounterRequest>,
) -> Result<Json<CounterValue>, HttpError> {
    validate_counter_name(&name)?;
    let value = app
        .apply(
            CounterOperation {
                name,
                amount: request.initial_value,
                action: CounterAction::Initiate,
            },
            HttpRouteConfig::default().request_timeout_value(),
        )
        .await?;
    Ok(Json(value))
}

async fn increase_counter(
    State(app): State<CounterHttp>,
    AxumPath(name): AxumPath<String>,
    Json(request): Json<ChangeCounterRequest>,
) -> Result<Json<CounterValue>, HttpError> {
    validate_counter_name(&name)?;
    validate_non_negative_amount(request.amount)?;
    let value = app
        .apply(
            CounterOperation {
                name,
                amount: request.amount,
                action: CounterAction::Increase,
            },
            HttpRouteConfig::default().request_timeout_value(),
        )
        .await?;
    Ok(Json(value))
}

async fn decrease_counter(
    State(app): State<CounterHttp>,
    AxumPath(name): AxumPath<String>,
    Json(request): Json<ChangeCounterRequest>,
) -> Result<Json<CounterValue>, HttpError> {
    validate_counter_name(&name)?;
    validate_non_negative_amount(request.amount)?;
    let value = app
        .apply(
            CounterOperation {
                name,
                amount: request.amount,
                action: CounterAction::Decrease,
            },
            HttpRouteConfig::default().request_timeout_value(),
        )
        .await?;
    Ok(Json(value))
}

fn validate_counter_name(name: &str) -> Result<(), HttpError> {
    if name.is_empty() {
        return Err(HttpError::JsonDecode {
            message: "counter name must not be empty".to_string(),
        });
    }
    if name.contains('|') || name.contains('/') {
        return Err(HttpError::JsonDecode {
            message: "counter name must not contain '|' or '/'".to_string(),
        });
    }
    Ok(())
}

fn validate_non_negative_amount(amount: i64) -> Result<(), HttpError> {
    if amount < 0 {
        return Err(HttpError::JsonDecode {
            message: "amount must be non-negative".to_string(),
        });
    }
    Ok(())
}

fn entity_ask_http_error(error: EntityAskError) -> HttpError {
    match error {
        EntityAskError::NoRoute(error) => HttpError::EntityNoRoute {
            message: error.to_string(),
        },
        EntityAskError::MailboxFull => HttpError::EntityMailboxFull,
        EntityAskError::MailboxClosed => HttpError::EntityMailboxClosed,
        EntityAskError::NotLocal { owner } => HttpError::EntityNotLocal {
            owner: owner.to_string(),
        },
        EntityAskError::SpawnFailed(message) => HttpError::EntitySpawnFailed { message },
        EntityAskError::RemoteEncode(message) => HttpError::EntityRemoteEncode { message },
        EntityAskError::RemoteSend(message) => HttpError::EntityRemoteSend { message },
        EntityAskError::ShardHandoff { shard_id, state } => HttpError::EntityShardHandoff {
            shard_id: shard_id.to_string(),
            state: state.to_string(),
        },
        EntityAskError::ShardBufferFull { shard_id, capacity } => {
            HttpError::EntityShardBufferFull {
                shard_id: shard_id.to_string(),
                capacity,
            }
        }
        EntityAskError::Rejected(message) => HttpError::EntityRejected { message },
        EntityAskError::Timeout => HttpError::EntityTimeout,
        EntityAskError::ReplyDropped => HttpError::EntityReplyDropped,
    }
}

fn remote_ask_http_error(error: RemoteEntityAskError) -> HttpError {
    match error {
        RemoteEntityAskError::NoRoute { error } => HttpError::EntityNoRoute {
            message: error.to_string(),
        },
        RemoteEntityAskError::Encode { error } => HttpError::EntityRemoteEncode {
            message: error.to_string(),
        },
        RemoteEntityAskError::Register { error } => HttpError::service(error.to_string()),
        RemoteEntityAskError::Send { message } => HttpError::EntityRemoteSend { message },
        RemoteEntityAskError::Reply { error } => match error {
            RemoteRequestError::Timeout => HttpError::EntityTimeout,
            RemoteRequestError::ReplyDropped => HttpError::EntityReplyDropped,
            RemoteRequestError::Decode { error } => HttpError::EntityRemoteEncode {
                message: error.to_string(),
            },
            other => HttpError::service(other.to_string()),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::counter_routes;

    /// Route paths must satisfy the axum syntax rules (`{param}` captures);
    /// axum only enforces them at router construction, so build the table in
    /// CI instead of discovering a panic at example startup.
    #[test]
    fn counter_routes_construct_under_current_axum() {
        let _ = counter_routes();
    }
}
