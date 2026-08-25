//! The workload one pod runs, and the durable facts the driver asserts on.
//!
//! Some of the fixture below restates `crates/rakka-agent/tests/common/mod.rs`.
//! That module is a test target of another crate and cannot be imported here;
//! sharing it would mean promoting the fixture into `rakka_agent::testkit`,
//! which is owed work rather than a copy to delete.
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
/// How many times the sharded agent command is retried while the peer starts.
const AGENT_COMMAND_ATTEMPTS: usize = 5;

/// The agent's instantiation, built from constants alone.
///
/// Every input is fixed, so the derived operation id is the same on every pod
/// and on every call — which is what lets the same command be replayed through
/// the sharded command surface without creating a second agent.
fn instantiate_command() -> Result<AgentEntityCommand, String> {
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

    Ok(AgentEntityCommand::Instantiate {
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
}

/// Replays the instantiation through the agent entity's *sharded* command
/// surface, and reports whether it crossed the wire.
///
/// The instantiation above is a direct store write, which is what keeps seeding
/// alive on a pod whose peer is dead. That path never touches
/// `init_agent_entity_remote_sharding`, so the agent class's remote arm — and
/// the payload codecs it requires — could be broken with nothing noticing. This
/// replays the identical command through the shard: it deduplicates on the same
/// derived operation id and returns the original outcome, so it changes no
/// durable state, and on the pod that does not own the agent it travels the
/// wire and back through both codecs.
///
/// Reported rather than asserted here, because in an armed world the owner may
/// already be gone. The driver asserts it in the crash-free reference, where
/// exactly one pod must report `remote`.
pub async fn prove_remote_agent_command(pod: &Pod) -> &'static str {
    let command = match instantiate_command() {
        Ok(command) => command,
        Err(error) => {
            eprintln!("building the agent command failed: {error}");
            return "failed";
        }
    };
    for attempt in 0..AGENT_COMMAND_ATTEMPTS {
        match pod
            .command_agent_entity(agent_scope().entity_id().as_str(), command.clone())
            .await
        {
            Ok(true) => return "remote",
            Ok(false) => return "local",
            // The driver staggers the two pods, so the first one up resolves
            // the agent's owner correctly and then asks a peer that is not
            // listening yet. Only the crash-free reference world runs this, so
            // the retries cost nothing in the sweep.
            Err(error) if attempt + 1 == AGENT_COMMAND_ATTEMPTS => {
                eprintln!("the sharded agent command failed: {error}");
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
        }
    }
    "failed"
}

/// Instantiates the agent and creates its task.
///
/// Both commands deduplicate on derived operation ids, so a pod that dies part
/// way through and is replaced by one that seeds again produces one agent and
/// one task — never two.
///
/// The agent's instantiation is a direct store write rather than a sharded
/// command, which is what keeps seeding alive on a pod whose peer is gone;
/// [`prove_remote_agent_command`] is what exercises the sharded surface.
pub async fn seed(pod: &Pod) -> Result<(), String> {
    let mut agent = AgentEntityStore::new(agent_scope(), pod.stores.agents.clone());
    agent.recover().await.map_err(|error| error.to_string())?;
    agent
        .apply(instantiate_command()?)
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

/// The file a departing pod's absence is announced through.
///
/// A real deployment learns this from its membership provider — etcd, DNS, the
/// Kubernetes API. What matters for the harness is that it is *announced*
/// rather than inferred from a timeout, so shard movement happens at a
/// determined moment instead of a timing-dependent one. Until the survivor
/// reconciles, it correctly refuses to drive shards it does not own: a pod
/// that guessed would be the second writer specification 15 forbids.
pub const DEPARTED: &str = "departed";

/// Publishes `departed` as one atomic announcement.
///
/// `std::fs::write` creates the file at length zero and fills it in a second
/// syscall, while the survivor tests only `peer_departed` and then reads in a
/// third: a reader landing between the two sees an empty announcement, and
/// `take_over` can only refuse it. The rename is the publish — the file never
/// exists in a state a reader can observe as incomplete.
pub fn announce_departure(root: &Path, logical: &str, incarnation: &str) -> std::io::Result<()> {
    let temp = root.join(format!("{DEPARTED}.tmp"));
    std::fs::write(&temp, format!("{logical} {incarnation}"))?;
    std::fs::rename(&temp, root.join(DEPARTED))
}

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
pub async fn drive(
    pod: &mut Pod,
    rounds: usize,
    deadline: Duration,
) -> Result<DriveOutcome, String> {
    let mut started = std::time::Instant::now();
    let mut took_over = false;
    let mut errors = DriveErrors::default();
    let adapter = LedgerModelAdapter::new(pod.root.clone(), "resolved");
    let dispatcher = ScriptedDispatcher::with_adapter(adapter);

    for _round in 0..rounds {
        if started.elapsed() >= deadline {
            return Ok(DriveOutcome {
                terminal: terminal(&pod.stores.tasks).await,
                last_error: errors.last,
                lost_writes: errors.lost_writes,
                took_over,
            });
        }
        if !took_over && peer_departed(pod) {
            match take_over(pod) {
                Ok(()) => {
                    took_over = true;
                    // The deadline restarts here. It was measured from this
                    // pod's own boot, so the later its peer died the less time
                    // it had left for the recovery the window exists to prove
                    // — and running out reported as "the task did not converge
                    // on Completed", pointing at the agent domain rather than
                    // at the harness's own budget.
                    started = std::time::Instant::now();
                }
                // Not latched. `mark_down` refuses a node the membership has
                // not admitted yet, and the shard coordinator lease has to be
                // reachable — both are states the next round can find changed.
                // A takeover swallowed here is unrecoverable: the dead pod
                // stays Up, this pod keeps resolving it as the owner and
                // correctly refuses to drive shards it does not own, and the
                // world fails as a convergence failure with the real cause
                // gone.
                Err(error) => eprintln!("takeover failed, retrying next round: {error}"),
            }
        }
        let mut progressed = false;

        if pod.owns_task(task_scope().entity_id().as_str()) {
            let mut task = task_store(pod);
            // `map(|_| ())` drops the state reference `recover` hands back,
            // which borrows the entity the settle below needs.
            let recovered = task.recover(now(pod)).await.map(|_| ());
            match recovered {
                Ok(()) => match task.settle_side_effects(&pod.router, now(pod)).await {
                    Ok(progress) => progressed |= progress.settled > 0 || progress.assigned,
                    Err(error) => errors.record("task settle", &error, task_lost_the_write(&error)),
                },
                Err(error) => errors.record("task recover", &error, task_lost_the_write(&error)),
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
            let recovered = run.recover(now(pod)).await.map(|_| ());
            match recovered {
                Ok(()) => {
                    match run.settle_side_effects(&pod.router, now(pod)).await {
                        Ok(progress) => {
                            progressed |= progress.settled > 0 || progress.transitions > 0
                        }
                        Err(error) => {
                            errors.record("run settle", &error, run_lost_the_write(&error))
                        }
                    }
                    match dispatcher.drive(&mut run, &pod.router, now(pod)).await {
                        Ok(answered) => progressed |= answered > 0,
                        Err(error) => {
                            errors.record("run drive", &error, run_lost_the_write(&error))
                        }
                    }
                }
                Err(error) => errors.record("run recover", &error, run_lost_the_write(&error)),
            }
        }

        if terminal(&pod.stores.tasks).await {
            return Ok(DriveOutcome {
                terminal: true,
                last_error: errors.last,
                lost_writes: errors.lost_writes,
                took_over,
            });
        }
        if !progressed {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    Ok(DriveOutcome {
        terminal: terminal(&pod.stores.tasks).await,
        last_error: errors.last,
        lost_writes: errors.lost_writes,
        took_over,
    })
}

/// What a pod's drive loop ended up doing.
pub struct DriveOutcome {
    /// Whether the task reached a terminal status.
    pub terminal: bool,
    /// The last error any round produced that was not an ordinary lost write.
    pub last_error: Option<String>,
    /// How many rounds lost their compare-and-set to the other pod.
    pub lost_writes: usize,
    /// Whether this pod downed a departed peer and took over its shards.
    ///
    /// Returned rather than published through a static. The static existed
    /// because the caller prints it after `drive` has returned, which is what
    /// the return value is for.
    pub took_over: bool,
}

/// The errors a drive loop saw, reported once each.
///
/// Unlike the in-process sweeps — which swallow errors deliberately, because
/// there the injected loss *is* a returned error — the loss injected here is
/// `process::abort()`. A returned error is therefore never the fault and is
/// always something genuinely wrong: a codec failure, a schema rejection, or a
/// revision-conflict storm makes every round a silent no-op, the pod spins to
/// its deadline, and the only symptom is "the task did not converge on
/// Completed" with the code that named the subsystem already dropped.
///
/// Errors are not fatal. Two pods writing one record is the documented topology
/// here, so a revision conflict is an ordinary round that lost and will be
/// retried. What they must not be is invisible.
#[derive(Default)]
struct DriveErrors {
    seen: std::collections::BTreeSet<String>,
    lost_writes: usize,
    last: Option<String>,
}

impl DriveErrors {
    /// Records one error. `lost_write` marks the ordinary kind.
    ///
    /// Two pods writing one record is the documented topology here, so a round
    /// that loses its compare-and-set is not a fault — it is counted and
    /// retried, never printed. Printing it would bury the errors that matter
    /// under the ones that do not, which is the same silence in a louder form.
    fn record(&mut self, what: &str, error: &impl std::fmt::Display, lost_write: bool) {
        if lost_write {
            self.lost_writes += 1;
            return;
        }
        let message = format!("{what}: {error}");
        if self.seen.insert(message.clone()) {
            eprintln!("drive error: {message}");
        }
        self.last = Some(message);
    }
}

/// Whether a task error is this pod losing a compare-and-set to the other.
///
/// A settle reaches the store through the choreography host, so the conflict
/// arrives nested rather than at the top level — matching only the direct
/// variant classifies every real lost write as a fault.
fn task_lost_the_write(error: &rakka_agent::AgentTaskError) -> bool {
    use rakka_agent::AgentTaskError;
    match error {
        AgentTaskError::Persistence(persistence) => is_revision_conflict(persistence),
        AgentTaskError::Choreography(nested) => choreography_lost_the_write(nested),
        _ => false,
    }
}

/// Whether a run error is this pod losing a compare-and-set to the other.
fn run_lost_the_write(error: &rakka_agent::AgentRunError) -> bool {
    use rakka_agent::AgentRunError;
    match error {
        AgentRunError::Persistence(persistence) => is_revision_conflict(persistence),
        AgentRunError::Choreography(nested) => choreography_lost_the_write(nested),
        AgentRunError::Task(nested) => task_lost_the_write(nested),
        _ => false,
    }
}

fn choreography_lost_the_write(error: &rakka_agent::AgentChoreographyError) -> bool {
    match error {
        rakka_agent::AgentChoreographyError::Persistence(persistence) => {
            is_revision_conflict(persistence)
        }
        _ => false,
    }
}

const fn is_revision_conflict(error: &rakka_persistence::DurableError) -> bool {
    matches!(
        error,
        rakka_persistence::DurableError::RevisionConflict { .. }
    )
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
