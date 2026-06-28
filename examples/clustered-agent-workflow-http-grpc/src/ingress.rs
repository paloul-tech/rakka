//! Protocol-neutral ingress core shared by the HTTP and gRPC adapters.
//!
//! Both ingresses are thin: they translate their wire types to/from the neutral
//! `model` types and call these functions. All cluster routing lives here — the
//! receiving node resolves the run's owner through the sharding region and either
//! asks the local run entity or routes to the owner over `rakka-remote` TCP.

use std::sync::Arc;

use rakka::agent_workflow::AgentWorkflow;
use rakka::prelude::{ClusterSharding, EntityTypeKey};
use rakka::remote::{RemoteRequestError, TcpRemoteTransport};
use rakka::sharding::{EntityAskError, RemoteEntityAskClient, RemoteEntityAskError};

use crate::discovery::{membership_snapshot, MembershipView};
use crate::model::{ClusterView, RunRequest, SubmitWorkflowRequest, WorkflowRunView};
use crate::run_entity::RunEntityCommand;
use crate::support::{NUMBER_OF_SHARDS, RUN_ASK_TIMEOUT};
use crate::workflow::{self, CompiledSubmission};

/// Shared state both ingresses operate over.
#[derive(Clone)]
pub struct AppState {
    pub sharding: ClusterSharding,
    pub key: EntityTypeKey<RunEntityCommand>,
    pub ask_client: RemoteEntityAskClient<TcpRemoteTransport>,
    pub workflow: AgentWorkflow,
    pub node_label: String,
    pub membership: MembershipView,
}

/// Protocol-neutral ingress failure, mapped to HTTP/gRPC status by each adapter.
#[derive(Debug)]
pub enum IngressError {
    /// Invalid input (bad graph). HTTP 400 / gRPC `InvalidArgument`.
    BadRequest(String),
    /// Owner could not be reached. HTTP 503 / gRPC `Unavailable`.
    Unavailable(String),
    /// Internal failure. HTTP 500 / gRPC `Internal`.
    Internal(String),
}

/// Outcome of a successfully delivered run view, classified by run status.
pub enum ViewOutcome {
    /// Run exists; carry the view through as success.
    Completed(WorkflowRunView),
    /// Run id is not known to its owner.
    NotFound(WorkflowRunView),
    /// Run reached an error status; carry the diagnostic message.
    Failed(String),
}

/// Classifies a delivered run view into a protocol-neutral outcome.
#[must_use]
pub fn classify(view: WorkflowRunView) -> ViewOutcome {
    match view.status.as_str() {
        "not-found" => ViewOutcome::NotFound(view),
        "error" => ViewOutcome::Failed(
            view.message
                .clone()
                .unwrap_or_else(|| "run failed".to_string()),
        ),
        _ => ViewOutcome::Completed(view),
    }
}

/// Compiles and runs a submitted workflow on its owning node.
pub async fn submit(
    state: &AppState,
    request: SubmitWorkflowRequest,
) -> Result<WorkflowRunView, IngressError> {
    let submission = workflow::compile_submission(&request, &state.workflow)
        .map_err(|error| IngressError::BadRequest(error.to_string()))?;
    drive_on_owner(state, submission).await
}

/// Fetches the current run view from a run's owning node.
pub async fn get_run(state: &AppState, run_id: String) -> Result<WorkflowRunView, IngressError> {
    let (entity, is_local) = resolve(state, run_id)?;
    let mut view = if is_local {
        entity
            .ask(
                |reply_to| RunEntityCommand::Query { reply_to },
                RUN_ASK_TIMEOUT,
            )
            .await
            .map_err(entity_ask_error)?
    } else {
        entity
            .remote_ask(&state.ask_client, RunRequest::Query, RUN_ASK_TIMEOUT)
            .await
            .map_err(remote_ask_error)?
    };
    view.served_by = state.node_label.clone();
    view.executed_locally = is_local;
    Ok(view)
}

/// Builds this node's view of the cluster from the shared membership snapshot.
#[must_use]
pub fn cluster(state: &AppState) -> ClusterView {
    let up_nodes = membership_snapshot(&state.membership);
    ClusterView {
        this_node: state.node_label.clone(),
        member_count: up_nodes.len(),
        up_nodes,
        number_of_shards: NUMBER_OF_SHARDS,
    }
}

type RunEntityRef = rakka::sharding::ShardedEntityRef<RunEntityCommand>;

fn resolve(state: &AppState, run_id: String) -> Result<(RunEntityRef, bool), IngressError> {
    let entity = state
        .sharding
        .entity_ref_for(&state.key, run_id)
        .map_err(|error| IngressError::Unavailable(error.to_string()))?;
    let (owner, _shard) = entity
        .region()
        .resolve(entity.entity_ref())
        .map_err(|error| IngressError::Unavailable(error.to_string()))?;
    let is_local = entity
        .region()
        .local_node_id()
        .is_some_and(|local| local == &owner);
    Ok((entity, is_local))
}

async fn drive_on_owner(
    state: &AppState,
    submission: CompiledSubmission,
) -> Result<WorkflowRunView, IngressError> {
    let (entity, is_local) = resolve(state, submission.run_id.as_str().to_string())?;
    // Local: ask the run entity directly. Remote: route the serializable plan to
    // the owner over rakka-remote TCP in one round trip.
    let mut view = if is_local {
        entity
            .ask(
                |reply_to| RunEntityCommand::Drive {
                    plan: Arc::new(submission.plan),
                    reply_to,
                },
                RUN_ASK_TIMEOUT,
            )
            .await
            .map_err(entity_ask_error)?
    } else {
        entity
            .remote_ask(
                &state.ask_client,
                RunRequest::Drive {
                    plan: Box::new(submission.plan),
                },
                RUN_ASK_TIMEOUT,
            )
            .await
            .map_err(remote_ask_error)?
    };
    view.served_by = state.node_label.clone();
    view.executed_locally = is_local;
    Ok(view)
}

fn entity_ask_error(error: EntityAskError) -> IngressError {
    match error {
        EntityAskError::NoRoute(error) => IngressError::Unavailable(error.to_string()),
        EntityAskError::NotLocal { owner } => {
            IngressError::Unavailable(format!("entity owned by {owner}"))
        }
        EntityAskError::MailboxFull => IngressError::Unavailable("entity mailbox full".to_string()),
        EntityAskError::MailboxClosed => {
            IngressError::Unavailable("entity mailbox closed".to_string())
        }
        EntityAskError::ShardHandoff { shard_id, state } => {
            IngressError::Unavailable(format!("shard {shard_id} is {state}"))
        }
        EntityAskError::ShardBufferFull { shard_id, .. } => {
            IngressError::Unavailable(format!("shard {shard_id} buffer full"))
        }
        EntityAskError::Timeout => IngressError::Unavailable("entity ask timed out".to_string()),
        EntityAskError::ReplyDropped => IngressError::Internal("entity reply dropped".to_string()),
        EntityAskError::SpawnFailed(message)
        | EntityAskError::RemoteEncode(message)
        | EntityAskError::RemoteSend(message)
        | EntityAskError::Rejected(message) => IngressError::Internal(message),
    }
}

fn remote_ask_error(error: RemoteEntityAskError) -> IngressError {
    match error {
        RemoteEntityAskError::NoRoute { error } => IngressError::Unavailable(error.to_string()),
        RemoteEntityAskError::Send { message } => IngressError::Unavailable(message),
        RemoteEntityAskError::Encode { error } => IngressError::Internal(error.to_string()),
        RemoteEntityAskError::Register { error } => IngressError::Internal(error.to_string()),
        RemoteEntityAskError::Reply { error } => match error {
            RemoteRequestError::Timeout => {
                IngressError::Unavailable("remote ask timed out".to_string())
            }
            RemoteRequestError::ReplyDropped => {
                IngressError::Internal("remote reply dropped".to_string())
            }
            other => IngressError::Internal(other.to_string()),
        },
    }
}
