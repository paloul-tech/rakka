//! gRPC ingress adapter (public ingress only).
//!
//! Thin translation between the generated tonic service and the protocol-neutral
//! [`crate::ingress`] core: convert protobuf <-> neutral `model` types, call the
//! core, and map outcomes to `tonic::Status` with `rakka::grpc` helpers.

use rakka::grpc::{validation_status, GrpcError};
use tonic::{Request, Response, Status};

use crate::generated::agent_api::agent_workflow_ingress_server::AgentWorkflowIngress;
use crate::generated::agent_api::{
    ClusterView as ProtoClusterView, GetClusterRequest, GetWorkflowRequest,
    NodeView as ProtoNodeView, SubmitWorkflowRequest as ProtoSubmitWorkflowRequest,
    WorkflowRunView as ProtoWorkflowRunView,
};
use crate::ingress::{self, AppState, IngressError, ViewOutcome};
use crate::model;

/// gRPC service backed by the shared ingress core.
#[derive(Clone)]
pub struct AgentWorkflowGrpc {
    state: AppState,
}

impl AgentWorkflowGrpc {
    /// Creates a gRPC service over shared ingress state.
    #[must_use]
    pub fn new(state: AppState) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl AgentWorkflowIngress for AgentWorkflowGrpc {
    async fn submit_workflow(
        &self,
        request: Request<ProtoSubmitWorkflowRequest>,
    ) -> Result<Response<ProtoWorkflowRunView>, Status> {
        let request = to_model_request(request.into_inner());
        let view = ingress::submit(&self.state, request)
            .await
            .map_err(grpc_status)?;
        view_response(view)
    }

    async fn get_workflow(
        &self,
        request: Request<GetWorkflowRequest>,
    ) -> Result<Response<ProtoWorkflowRunView>, Status> {
        let run_id = request.into_inner().run_id;
        let view = ingress::get_run(&self.state, run_id)
            .await
            .map_err(grpc_status)?;
        view_response(view)
    }

    async fn get_cluster(
        &self,
        _request: Request<GetClusterRequest>,
    ) -> Result<Response<ProtoClusterView>, Status> {
        Ok(Response::new(to_proto_cluster(ingress::cluster(
            &self.state,
        ))))
    }
}

// `tonic::Status` is intentionally large; this mirrors the generated async trait
// methods (which the lint exempts), so allow the large `Err` on this helper.
#[allow(clippy::result_large_err)]
fn view_response(view: model::WorkflowRunView) -> Result<Response<ProtoWorkflowRunView>, Status> {
    match ingress::classify(view) {
        ViewOutcome::Completed(view) => Ok(Response::new(to_proto_view(view))),
        ViewOutcome::NotFound(view) => {
            Err(Status::not_found(format!("run {} not found", view.run_id)))
        }
        ViewOutcome::Failed(message) => Err(GrpcError::service(message).into_status()),
    }
}

fn grpc_status(error: IngressError) -> Status {
    match error {
        IngressError::BadRequest(message) => validation_status(message),
        IngressError::Unavailable(message) => GrpcError::EntityNoRoute { message }.into_status(),
        IngressError::Internal(message) => GrpcError::service(message).into_status(),
    }
}

fn to_model_request(request: ProtoSubmitWorkflowRequest) -> model::SubmitWorkflowRequest {
    model::SubmitWorkflowRequest {
        run_id: optional(request.run_id),
        plan_id: optional(request.plan_id),
        nodes: request
            .nodes
            .into_iter()
            .map(|node| model::NodeSpec {
                id: node.id,
                kind: optional(node.kind),
            })
            .collect(),
        edges: request
            .edges
            .into_iter()
            .map(|edge| model::EdgeSpec {
                from: edge.from,
                to: edge.to,
            })
            .collect(),
        entry_nodes: (!request.entry_nodes.is_empty()).then_some(request.entry_nodes),
    }
}

fn to_proto_view(view: model::WorkflowRunView) -> ProtoWorkflowRunView {
    ProtoWorkflowRunView {
        run_id: view.run_id,
        owner_node: view.owner_node,
        executed_locally: view.executed_locally,
        served_by: view.served_by,
        status: view.status,
        plan_id: view.plan_id,
        plan_fingerprint: view.plan_fingerprint,
        node_count: view.node_count as u64,
        completed_node_count: view.completed_node_count as u64,
        terminal_node_count: view.terminal_node_count as u64,
        nodes: view
            .nodes
            .into_iter()
            .map(|node| ProtoNodeView {
                node_id: node.node_id,
                kind: node.kind,
                status: node.status,
            })
            .collect(),
    }
}

fn to_proto_cluster(view: model::ClusterView) -> ProtoClusterView {
    ProtoClusterView {
        this_node: view.this_node,
        up_nodes: view.up_nodes,
        member_count: view.member_count as u64,
        number_of_shards: view.number_of_shards,
    }
}

fn optional(value: String) -> Option<String> {
    (!value.trim().is_empty()).then_some(value)
}
