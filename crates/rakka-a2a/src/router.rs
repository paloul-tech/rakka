//! Cluster-backed owner routing (the `sharding` feature's [`A2ARunRoute`]).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use rakka_remote::{RemoteRequestError, TcpRemoteTransport};
use rakka_sharding::{
    ClusterSharding, EntityAskError, EntityTypeKey, RemoteEntityAskClient, RemoteEntityAskError,
};

use crate::error::RakkaA2AHandlerError;
use crate::host::A2ARunEntityCommand;
use crate::protocol::{A2ARunRequest, A2ARunResponse};
use crate::routing::{A2APeerReachabilityObserver, A2ARunRoute, NoopPeerReachabilityObserver};

/// Cluster routing helper for owner-only A2A run requests.
///
/// Public ingress nodes route serializable [`A2ARunRequest`] values to the
/// task's shard owner: locally through an actor ask when this node owns the
/// shard, otherwise over Rakka remoting. Remoting is at-most-once, so callers
/// treat failures per the returned failure class rather than as proof the
/// owner did not act.
#[derive(Clone)]
pub struct A2ARunRouter {
    sharding: ClusterSharding,
    key: EntityTypeKey<A2ARunEntityCommand>,
    ask_client: RemoteEntityAskClient<TcpRemoteTransport>,
    ask_timeout: Duration,
    reachability: Arc<dyn A2APeerReachabilityObserver>,
}

impl A2ARunRouter {
    /// Creates a router over the shared cluster sharding facade.
    #[must_use]
    pub fn new(
        sharding: ClusterSharding,
        key: EntityTypeKey<A2ARunEntityCommand>,
        ask_client: RemoteEntityAskClient<TcpRemoteTransport>,
        ask_timeout: Duration,
    ) -> Self {
        Self {
            sharding,
            key,
            ask_client,
            ask_timeout,
            reachability: Arc::new(NoopPeerReachabilityObserver),
        }
    }

    /// Installs a reachability observer for self-fencing.
    #[must_use]
    pub fn with_reachability_observer(
        mut self,
        observer: Arc<dyn A2APeerReachabilityObserver>,
    ) -> Self {
        self.reachability = observer;
        self
    }
}

#[async_trait]
impl A2ARunRoute for A2ARunRouter {
    async fn route(&self, request: A2ARunRequest) -> Result<A2ARunResponse, RakkaA2AHandlerError> {
        let entity = self
            .sharding
            .entity_ref_for(&self.key, request.task_id.clone())
            .map_err(|error| RakkaA2AHandlerError::Unavailable {
                message: error.to_string(),
            })?;
        let (owner, _shard) = entity
            .region()
            .resolve(entity.entity_ref())
            .map_err(|error| RakkaA2AHandlerError::Unavailable {
                message: error.to_string(),
            })?;
        let is_local = entity
            .region()
            .local_node_id()
            .is_some_and(|local| local == &owner);

        if is_local {
            entity
                .ask(
                    |reply_to| A2ARunEntityCommand::Handle { request, reply_to },
                    self.ask_timeout,
                )
                .await
                .map_err(entity_ask_error)
        } else {
            let outcome = entity
                .remote_ask(&self.ask_client, request, self.ask_timeout)
                .await;
            record_remote_outcome(self.reachability.as_ref(), &outcome);
            outcome.map_err(remote_ask_error)
        }
    }

    fn local_node_owns(&self, task_id: &str) -> bool {
        let Ok(entity) = self.sharding.entity_ref_for(&self.key, task_id.to_string()) else {
            return false;
        };
        let Ok((owner, _shard)) = entity.region().resolve(entity.entity_ref()) else {
            return false;
        };
        entity
            .region()
            .local_node_id()
            .is_some_and(|local| local == &owner)
    }
}

fn record_remote_outcome(
    reachability: &dyn A2APeerReachabilityObserver,
    outcome: &Result<A2ARunResponse, RemoteEntityAskError>,
) {
    match outcome {
        Ok(_) => reachability.record(true),
        // Only transport send failures qualify as peer-unreachability
        // evidence: reply timeouts are neutral because the ingress ask budget
        // can elapse while a healthy owner is still doing durable work, so
        // counting timeouts would let a slow peer fence a healthy ingress
        // node out of the cluster.
        Err(RemoteEntityAskError::Send { .. }) => reachability.record(false),
        Err(_) => {}
    }
}

fn entity_ask_error(error: EntityAskError) -> RakkaA2AHandlerError {
    match error {
        EntityAskError::NoRoute(error) => RakkaA2AHandlerError::Unavailable {
            message: error.to_string(),
        },
        EntityAskError::NotLocal { owner } => RakkaA2AHandlerError::Unavailable {
            message: format!("entity owned by {owner}"),
        },
        EntityAskError::MailboxFull => RakkaA2AHandlerError::Unavailable {
            message: "entity mailbox full".to_string(),
        },
        EntityAskError::MailboxClosed => RakkaA2AHandlerError::Unavailable {
            message: "entity mailbox closed".to_string(),
        },
        EntityAskError::ShardHandoff { shard_id, state } => RakkaA2AHandlerError::Unavailable {
            message: format!("shard {shard_id} is {state}"),
        },
        EntityAskError::ShardBufferFull { shard_id, .. } => RakkaA2AHandlerError::Unavailable {
            message: format!("shard {shard_id} buffer full"),
        },
        EntityAskError::Timeout => RakkaA2AHandlerError::Unavailable {
            message: "entity ask timed out".to_string(),
        },
        EntityAskError::ReplyDropped => RakkaA2AHandlerError::OwnerAsk {
            message: "entity reply dropped".to_string(),
        },
        EntityAskError::SpawnFailed(message)
        | EntityAskError::RemoteEncode(message)
        | EntityAskError::RemoteSend(message)
        | EntityAskError::Rejected(message) => RakkaA2AHandlerError::OwnerAsk { message },
    }
}

fn remote_ask_error(error: RemoteEntityAskError) -> RakkaA2AHandlerError {
    match error {
        RemoteEntityAskError::NoRoute { error } => RakkaA2AHandlerError::Unavailable {
            message: error.to_string(),
        },
        RemoteEntityAskError::Send { message } => RakkaA2AHandlerError::Unavailable { message },
        RemoteEntityAskError::Encode { error } => RakkaA2AHandlerError::OwnerAsk {
            message: error.to_string(),
        },
        RemoteEntityAskError::Register { error } => RakkaA2AHandlerError::OwnerAsk {
            message: error.to_string(),
        },
        RemoteEntityAskError::Reply { error } => match error {
            RemoteRequestError::Timeout => RakkaA2AHandlerError::Unavailable {
                message: "remote ask timed out".to_string(),
            },
            RemoteRequestError::ReplyDropped => RakkaA2AHandlerError::OwnerAsk {
                message: "remote reply dropped".to_string(),
            },
            other => RakkaA2AHandlerError::OwnerAsk {
                message: other.to_string(),
            },
        },
    }
}
