//! Sharded run entity and its registration over `rakka-remote`.
//!
//! `RunEntity` is the cluster-addressable unit. It is registered with
//! [`ClusterSharding::init_remote_with_ask`], so a non-owning node routes a
//! serializable [`RunRequest`] to the owner over `rakka-remote` TCP in one round
//! trip. On the owner, the entity hosts a local child [`AgentRunActor`] and runs
//! the (chatty, process-local) graph driver against it, then replies with a
//! [`WorkflowRunView`]. Ownership and routing are handled by Rakka's region.

use std::sync::Arc;

use rakka::agent_workflow::substrate::WorkflowState;
use rakka::agent_workflow::{
    AgentCompiledExecutionPlan, AgentRunActor, AgentRunActorCommand, AgentRunId, AgentRunState,
    AgentWorkflow,
};
use rakka::prelude::*;
use rakka::sharding::ClusterNodeRuntime;

use crate::model::{RunRequest, WorkflowRunView};
use crate::support::ExampleResult;
use crate::workflow;

/// Local message protocol for the run entity (not sent across the network).
pub enum RunEntityCommand {
    /// Compile-and-run, or resume, the run with the given plan.
    Drive {
        /// Compiled execution plan to run.
        plan: Arc<AgentCompiledExecutionPlan>,
        /// Reply channel for the resulting run view.
        reply_to: ReplyTo<WorkflowRunView>,
    },
    /// Return the current run view.
    Query {
        /// Reply channel for the current run view.
        reply_to: ReplyTo<WorkflowRunView>,
    },
}

/// Sharded entity that owns and drives one durable run.
pub struct RunEntity {
    run_id: AgentRunId,
    workflow: AgentWorkflow,
    node_label: String,
    child: ActorRef<AgentRunActorCommand>,
}

impl RunEntity {
    fn new<RunStore, WorkflowStore>(
        system: ActorSystem,
        context: EntityContext<RunEntityCommand>,
        workflow: AgentWorkflow,
        run_store: RunStore,
        workflow_store: WorkflowStore,
        node_label: String,
    ) -> Self
    where
        RunStore: DurableStateStore<AgentRunState>,
        WorkflowStore: DurableStateStore<WorkflowState>,
    {
        // The sharded entity is a stable routing shell; the durable child owns
        // recovery from the run/workflow state stores.
        let run_id = AgentRunId::new(context.entity_id().as_str());
        let child = system
            .spawn(
                format!("{}-run", context.actor_name()),
                AgentRunActor::new(workflow.clone(), run_id.clone(), run_store, workflow_store),
            )
            .expect("agent run child actor should spawn");
        Self {
            run_id,
            workflow,
            node_label,
            child,
        }
    }
}

impl Actor for RunEntity {
    type Msg = RunEntityCommand;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        let child = self.child.clone();
        let workflow = self.workflow.clone();
        let run_id = self.run_id.clone();
        let node_label = self.node_label.clone();
        actor_future(async move {
            match msg {
                RunEntityCommand::Drive { plan, reply_to } => {
                    let view =
                        match workflow::drive_to_terminal(&child, &workflow, &run_id, plan).await {
                            Ok(snapshot) => {
                                workflow::run_view(&snapshot, node_label.clone(), true, node_label)
                            }
                            Err(error) => workflow::status_view(
                                run_id.as_str(),
                                &node_label,
                                &node_label,
                                "error",
                                Some(error.to_string()),
                            ),
                        };
                    let _reply_dropped = reply_to.reply(view);
                }
                RunEntityCommand::Query { reply_to } => {
                    let view = match workflow::fetch_snapshot(&child).await {
                        Ok(snapshot) if snapshot.run_state.is_some() => {
                            workflow::run_view(&snapshot, node_label.clone(), true, node_label)
                        }
                        Ok(_) => workflow::status_view(
                            run_id.as_str(),
                            &node_label,
                            &node_label,
                            "not-found",
                            None,
                        ),
                        Err(error) => workflow::status_view(
                            run_id.as_str(),
                            &node_label,
                            &node_label,
                            "error",
                            Some(error.to_string()),
                        ),
                    };
                    let _reply_dropped = reply_to.reply(view);
                }
            }
            Ok(ActorAction::Continue)
        })
    }

    fn stopped<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        _reason: &'a TerminationReason,
    ) -> ActorFuture<'a> {
        let child = self.child.clone();
        actor_future(async move {
            let _ = child.stop();
            Ok(ActorAction::Continue)
        })
    }
}

/// Per-node configuration for hosting run entities.
pub struct RunHost<RunStore, WorkflowStore> {
    /// Workflow definition every run entity hosts.
    pub workflow: AgentWorkflow,
    /// Durable store for run state.
    pub run_store: RunStore,
    /// Durable store for inbox/outbox workflow state.
    pub workflow_store: WorkflowStore,
    /// Logical id of this node, embedded in run views.
    pub node_label: String,
}

/// Registers the run entity for cluster-wide remote ask routing.
///
/// `RunRequest`/`WorkflowRunView` are serialized over `rakka-remote`; the local
/// `RunEntityCommand` is mapped from the request on the owning node.
pub fn init_run_sharding<RunStore, WorkflowStore>(
    system: &ActorSystem,
    runtime: &mut ClusterNodeRuntime,
    sharding: &ClusterSharding,
    key: EntityTypeKey<RunEntityCommand>,
    host: RunHost<RunStore, WorkflowStore>,
) -> ExampleResult<EntityTypeRegistration<RunEntityCommand>>
where
    RunStore: DurableStateStore<AgentRunState>,
    WorkflowStore: DurableStateStore<WorkflowState>,
{
    let RunHost {
        workflow,
        run_store,
        workflow_store,
        node_label,
    } = host;
    let registration = sharding.init_remote_with_ask(
        runtime,
        Entity::of(key, {
            let system = system.clone();
            move |context: EntityContext<RunEntityCommand>| {
                RunEntity::new(
                    system.clone(),
                    context,
                    workflow.clone(),
                    run_store.clone(),
                    workflow_store.clone(),
                    node_label.clone(),
                )
            }
        }),
        |request: RunRequest, reply_to: ReplyTo<WorkflowRunView>| match request {
            RunRequest::Drive { plan } => RunEntityCommand::Drive {
                plan: Arc::new(*plan),
                reply_to,
            },
            RunRequest::Query => RunEntityCommand::Query { reply_to },
        },
    )?;
    Ok(registration)
}
