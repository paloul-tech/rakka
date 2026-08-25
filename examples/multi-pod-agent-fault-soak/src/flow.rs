//! The workload one pod runs, and the durable facts the driver asserts on.
//!
//! A pod drives only the entities whose shards it owns. That is the deployment
//! shape [specification 15](../../../docs/plans/rakka-agent/spec.md) describes —
//! recovery scanning routes to the current owner — and it is what makes the
//! cross-pod exchange real: a task on one pod owing a run-creation to a run on
//! another has to put that envelope on the wire.

use std::path::Path;
use std::sync::atomic::Ordering;
use std::time::Duration;

use rakka_agent::testkit::{run_entity, ScriptedDispatcher, SharedAtomicWorkflowClock};
use rakka_agent::{
    load_agent_task_state, run_id_for_assignment, AgentAssignmentGeneration,
    AgentAuthorityEnvelope, AgentBudgetCeilings, AgentDefinition, AgentDefinitionId,
    AgentEntityCommand, AgentEntityStore, AgentId, AgentOperationId, AgentOperationKind,
    AgentRevisionNumber, AgentRevisionProvenance, AgentRunScope, AgentSchemaId, AgentSchemaPolicy,
    AgentSchemaRef, AgentScope, AgentSettings, AgentTaskContent, AgentTaskCreation,
    AgentTaskDefinition, AgentTaskDefinitionId, AgentTaskEntityCommand, AgentTaskEntityStore,
    AgentTaskId, AgentTaskResultCheck, AgentTaskResultRule, AgentTaskRuleId, AgentTaskScope,
    AgentTaskStatus, TenantId,
};
use rakka_agent_workflow::{
    AgentAuditEventId, AgentCausationId, AgentTimestampMillis, PrincipalRef,
};

use crate::external::LedgerModelAdapter;
use crate::wiring::Pod;

/// The tenant every identity in this harness is scoped to.
pub const TENANT: &str = "acme";
/// The one agent the harness runs.
pub const AGENT: &str = "support-agent";
/// The one task the harness drives to completion.
pub const TASK: &str = "ticket-1";
/// The task definition the agent's envelope declares.
pub const TASK_DEFINITION: &str = "resolve-ticket";

/// The agent's scope.
#[must_use]
pub fn agent_scope() -> AgentScope {
    AgentScope::new(TenantId::new(TENANT), agent_id()).expect("the agent scope is valid")
}

/// The agent's identity.
#[must_use]
pub fn agent_id() -> AgentId {
    AgentId::new(AGENT).expect("the agent id is valid")
}

/// The task's scope.
#[must_use]
pub fn task_scope() -> AgentTaskScope {
    AgentTaskScope::new(
        TenantId::new(TENANT),
        AgentTaskId::new(TASK).expect("valid"),
    )
    .expect("the task scope is valid")
}

/// The run's scope, derived from the task and its first assignment generation
/// exactly as the task entity derives it.
#[must_use]
pub fn run_scope() -> AgentRunScope {
    let run = run_id_for_assignment(
        &AgentTaskId::new(TASK).expect("valid"),
        AgentAssignmentGeneration::new(1),
    )
    .expect("the run id derives");
    AgentRunScope::new(TenantId::new(TENANT), agent_id(), run).expect("the run scope is valid")
}

fn schema(id: &str) -> AgentSchemaRef {
    AgentSchemaRef::new(
        AgentSchemaId::new(id).expect("the schema id is valid"),
        AgentRevisionNumber::INITIAL,
    )
}

fn task_definition() -> AgentTaskDefinition {
    AgentTaskDefinition::new(
        AgentTaskDefinitionId::new(TASK_DEFINITION).expect("the definition id is valid"),
        "Resolve one customer support ticket.",
        schema("ticket-input"),
        schema("ticket-result"),
    )
    .expect("the task definition is valid")
    .with_result_rule(AgentTaskResultRule::new(
        AgentTaskRuleId::new("answer-present").expect("the rule id is valid"),
        AgentTaskResultCheck::NonEmptyString {
            pointer: "/answer".to_string(),
        },
    ))
    .with_budgets(AgentBudgetCeilings {
        max_loop_iterations: Some(3),
        ..AgentBudgetCeilings::unbounded()
    })
}

fn provenance(at: u64) -> AgentRevisionProvenance {
    AgentRevisionProvenance {
        principal: PrincipalRef {
            principal_type: "service".to_string(),
            principal_id: "ingress".to_string(),
            display_name: None,
        },
        accepted_at: AgentTimestampMillis::new(at),
        causation_id: AgentCausationId::new(format!("cause-{at}")),
        audit_ref: AgentAuditEventId::new(format!("audit-{at}")),
    }
}

/// Instantiates the agent and creates its task.
///
/// Both commands deduplicate on derived operation ids, so a pod that dies part
/// way through and is replaced by one that seeds again produces one agent and
/// one task — never two.
pub async fn seed(pod: &Pod) -> Result<(), String> {
    let mut envelope = AgentAuthorityEnvelope::empty();
    envelope
        .task_definitions
        .insert(AgentTaskDefinitionId::new(TASK_DEFINITION).expect("the definition id is valid"));
    let definition = AgentDefinition::new(
        AgentDefinitionId::new("support-v1").expect("the definition id is valid"),
        "Resolves customer support tickets end to end.",
        envelope,
    )
    .map_err(|error| error.to_string())?;

    let mut agent = AgentEntityStore::new(agent_scope(), pod.stores.agents.clone());
    agent.recover().await.map_err(|error| error.to_string())?;
    agent
        .apply(AgentEntityCommand::Instantiate {
            operation_id: AgentOperationId::for_agent(
                AgentOperationKind::DefinitionUpdate,
                &agent_scope(),
                "1",
            )
            .map_err(|error| error.to_string())?,
            definition: Box::new(definition),
            settings: Box::new(AgentSettings::default()),
            provenance: Box::new(provenance(1)),
        })
        .await
        .map_err(|error| error.to_string())?;

    let mut task = task_store(pod);
    task.recover(now(pod))
        .await
        .map_err(|error| error.to_string())?;
    task.apply(
        AgentTaskEntityCommand::Create {
            operation_id: AgentOperationId::new(
                AgentOperationKind::TaskCreation,
                [TENANT, TASK, "1"],
            )
            .map_err(|error| error.to_string())?,
            creation: Box::new(AgentTaskCreation {
                definition: task_definition(),
                input: AgentTaskContent::inline(serde_json::json!({ "ticket": 1 }))
                    .map_err(|error| error.to_string())?,
                assignee: Some(agent_id()),
                team: None,
                goal: None,
                goal_mode: Default::default(),
                goal_spec: None,
                parent: None,
                dependencies: Vec::new(),
                escrow: None,
                wake: None,
                telemetry: Default::default(),
                delegation: None,
            }),
        },
        &pod.router,
        now(pod),
    )
    .await
    .map_err(|error| error.to_string())?;
    Ok(())
}

fn now(pod: &Pod) -> AgentTimestampMillis {
    AgentTimestampMillis::new(pod.clock.fetch_add(1, Ordering::SeqCst))
}

fn task_store(
    pod: &Pod,
) -> AgentTaskEntityStore<
    crate::stores::PodCrashStore<rakka_agent::AgentTaskState>,
    crate::stores::PodCrashStore<rakka_agent::AgentEntityState>,
    rakka_agent::InMemoryAgentTaskHistoryStore,
> {
    AgentTaskEntityStore::new(
        task_scope(),
        pod.stores.tasks.clone(),
        pod.stores.agents.clone(),
        pod.stores.task_history.clone(),
    )
}

/// Whether this pod ever reconciled itself as the only member.
pub static TOOK_OVER: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// The file a departing pod's absence is announced through.
///
/// A real deployment learns this from its membership provider — etcd, DNS, the
/// Kubernetes API. What matters for the harness is that it is *announced*
/// rather than inferred from a timeout, so shard movement happens at a
/// determined moment instead of a timing-dependent one. Until the survivor
/// reconciles, it correctly refuses to drive shards it does not own: a pod
/// that guessed would be the second writer specification 15 forbids.
pub const DEPARTED: &str = "departed";

/// Whether the driver has announced that the other pod is gone.
fn peer_departed(pod: &Pod) -> bool {
    pod.root.join(DEPARTED).exists()
}

/// Downs the departed member, which refreshes ownership onto this pod.
///
/// `mark_down` rather than `mark_leaving`: a killed pod never got to leave, and
/// a downing decision is what an operator or a failure detector actually
/// produces for one. The shards it hosted move here, and the entities are
/// re-materialized from the shared record — which is the whole claim of
/// [specification 15](../../../docs/plans/rakka-agent/spec.md).
fn take_over(pod: &mut Pod) -> Result<(), String> {
    let announcement =
        std::fs::read_to_string(pod.root.join(DEPARTED)).map_err(|error| error.to_string())?;
    let mut parts = announcement.split_whitespace();
    let (Some(logical), Some(incarnation)) = (parts.next(), parts.next()) else {
        return Err(format!(
            "malformed departure announcement: {announcement:?}"
        ));
    };
    let departed = rakka_cluster::NodeId::new(logical, incarnation);
    let observed_at = pod.clock.fetch_add(1, Ordering::SeqCst);
    pod.runtime
        .mark_down(&departed, observed_at)
        .map_err(|error| error.to_string())?;
    Ok(())
}

/// Drives every entity this pod owns until the task is terminal, the deadline
/// passes, or `rounds` passes have gone by.
///
/// A pod that owns neither entity still runs: it is the peer the other pod's
/// exchanges are routed to, and it must stay up to answer them — and when that
/// other pod dies, it is the one that has to finish the work.
///
/// Returns whether the task reached a terminal status.
pub async fn drive(pod: &mut Pod, rounds: usize, deadline: Duration) -> Result<bool, String> {
    let _ = &TOOK_OVER;
    let started = std::time::Instant::now();
    let mut took_over = false;
    let adapter = LedgerModelAdapter::new(pod.root.clone(), "resolved");
    let dispatcher = ScriptedDispatcher::with_adapter(adapter);

    for _round in 0..rounds {
        if started.elapsed() >= deadline {
            return Ok(terminal(&pod.stores.tasks).await);
        }
        if !took_over && peer_departed(pod) {
            if let Err(error) = take_over(pod) {
                eprintln!("takeover failed: {error}");
            }
            took_over = true;
            TOOK_OVER.store(true, Ordering::SeqCst);
        }
        let mut progressed = false;

        if pod.owns_task(task_scope().entity_id().as_str()) {
            let mut task = task_store(pod);
            if task.recover(now(pod)).await.is_ok() {
                if let Ok(progress) = task.settle_side_effects(&pod.router, now(pod)).await {
                    progressed |= progress.settled > 0 || progress.assigned;
                }
            }
        }

        if pod.owns_run(run_scope().entity_id().as_str()) {
            let mut run = run_entity(
                &run_scope(),
                &pod.stores.runs,
                &rakka_agent::WorkflowAgentRunEffectSink::new(
                    pod.stores.workflow.clone(),
                    SharedAtomicWorkflowClock::new(pod.clock.clone()),
                ),
            );
            if run.recover(now(pod)).await.is_ok() {
                if let Ok(progress) = run.settle_side_effects(&pod.router, now(pod)).await {
                    progressed |= progress.settled > 0 || progress.transitions > 0;
                }
                if let Ok(answered) = dispatcher.drive(&mut run, &pod.router, now(pod)).await {
                    progressed |= answered > 0;
                }
            }
        }

        if terminal(&pod.stores.tasks).await {
            return Ok(true);
        }
        if !progressed {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    Ok(terminal(&pod.stores.tasks).await)
}

async fn terminal<S>(store: &S) -> bool
where
    S: rakka_persistence::DurableStateStore<rakka_agent::AgentTaskState>,
{
    load_agent_task_state(store, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .ok()
        .flatten()
        .and_then(|state| state.snapshot())
        .is_some_and(|snapshot| snapshot.status.is_terminal())
}

/// The task's durable status, read straight from the shared directory.
///
/// This is what the driver asserts on: not what a pod said, and not what a
/// pod's memory held — what the shared record says after every pod is gone.
pub async fn task_status(root: &Path) -> Option<AgentTaskStatus> {
    let store =
        crate::stores::SharedFileStore::<rakka_agent::AgentTaskState>::new(root.join("tasks"));
    load_agent_task_state(&store, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .ok()
        .flatten()
        .and_then(|state| state.snapshot())
        .map(|snapshot| snapshot.status)
}
