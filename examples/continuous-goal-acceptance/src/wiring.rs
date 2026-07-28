//! The continuous world: durable stores, the exchange router over in-process
//! entity transports, the shared wake-timer index, its scanner, and the
//! re-wake parker — everything rebuilt from durable state on every use, so
//! every step of the walk is already a pod restart.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use rakka_agent::testkit::{
    CrashingStateStore, DeferredExchangeRouter, DeterministicModelAdapter,
    InProcessRunEntityTransport, InProcessTaskEntityTransport, InProcessWakeDelivery,
    ScriptedDispatcher, SharedAtomicWorkflowClock,
};
use rakka_agent::{
    AgentAuthorityEnvelope, AgentBudgetAllocation, AgentBudgetCeilings, AgentBudgetDimension,
    AgentBudgetWindow, AgentContinuousGoalSpec, AgentDefinition, AgentDefinitionId,
    AgentEffectPolicies, AgentEntityClass, AgentEntityCommand, AgentEntityState, AgentEntityStore,
    AgentEpochSpec, AgentExchangeRouter, AgentGoalId, AgentGoalMode, AgentGoalWindowCeiling,
    AgentId, AgentModelTurn, AgentOperationId, AgentOperationKind, AgentPolicyRef,
    AgentRevisionNumber, AgentRevisionProvenance, AgentRunScope, AgentRunState, AgentSchemaId,
    AgentSchemaPolicy, AgentSchemaRef, AgentScope, AgentSettings, AgentTaskContent,
    AgentTaskCreation, AgentTaskDefinition, AgentTaskDefinitionId, AgentTaskEntityCommand,
    AgentTaskEntityReply, AgentTaskEntityStore, AgentTaskError, AgentTaskScope, AgentTaskState,
    AgentWakeBackoffPolicy, AgentWakeBinding, AgentWakeControllerState, AgentWakeLifecyclePolicy,
    AgentWakeOccurrence, AgentWakePolicy, AgentWakePolicyRevision, AgentWakeRenewalPolicy,
    AgentWakeRetirementPolicy, AgentWakeRewakeParker, AgentWakeScanner, AgentWakeScannerSettings,
    AgentWakeTimerEntry, AgentWakeTimerStore, AgentWakeTimerStoreState, AgentWakeTriggerKind,
    InMemoryAgentRunEffectSink, InMemoryAgentTaskHistoryStore, ScheduleRevision,
    SharedWakeTimerParker, TenantId, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::{
    AgentAuditEventId, AgentCausationId, AgentTimestampMillis, PrincipalRef,
};

/// Durable store for the task entity class; crash-armable for the fault
/// windows.
pub type TaskStore = CrashingStateStore<AgentTaskState>;
/// Durable store for the agent entity class.
pub type AgentStore = CrashingStateStore<AgentEntityState>;
/// Durable store for the run entity class.
pub type RunStore = CrashingStateStore<AgentRunState>;
/// Durable store for the shared wake-timer index.
pub type WakeStore = CrashingStateStore<AgentWakeTimerStoreState>;
/// The wake delivery the scanner injects admission commands through.
pub type WakeDelivery = InProcessWakeDelivery<TaskStore, AgentStore, InMemoryAgentTaskHistoryStore>;
/// The scanner the walk drives.
pub type WakeScanner = AgentWakeScanner<WakeStore, WakeDelivery, SharedAtomicWorkflowClock>;

/// The walk's tenant.
pub const TENANT: &str = "acme";
/// The one agent every epoch is assigned to.
pub const AGENT: &str = "reconciliation-agent";
/// The stable root control task.
pub const TASK: &str = "reconciliation-root";
/// The epoch task definition.
pub const TASK_DEFINITION: &str = "reconcile-once";
/// The one continuous goal.
pub const GOAL: &str = "nightly-reconciliation";
/// The goal window's rolling length.
pub const WINDOW_MS: u64 = 3_600_000;

/// The walk's tenant identity.
pub fn tenant() -> TenantId {
    TenantId::new(TENANT)
}

/// The epoch assignee.
pub fn agent_id() -> AgentId {
    AgentId::new(AGENT).expect("the agent id is valid")
}

/// The assignee's scope.
pub fn agent_scope() -> AgentScope {
    AgentScope::new(tenant(), agent_id()).expect("the agent scope is valid")
}

/// The root control task's scope.
pub fn task_scope() -> AgentTaskScope {
    AgentTaskScope::new(
        tenant(),
        rakka_agent::AgentTaskId::new(TASK).expect("the task id is valid"),
    )
    .expect("the task scope is valid")
}

/// The goal identity.
pub fn goal_id() -> AgentGoalId {
    AgentGoalId::new(GOAL).expect("the goal id is valid")
}

/// A schema reference at its initial revision.
pub fn schema(id: &str) -> AgentSchemaRef {
    AgentSchemaRef::new(
        AgentSchemaId::new(id).expect("the schema id is valid"),
        AgentRevisionNumber::INITIAL,
    )
}

/// The epoch task definition: one reconciliation pass, a non-empty answer,
/// at most three loop iterations.
pub fn task_definition() -> AgentTaskDefinition {
    AgentTaskDefinition::new(
        AgentTaskDefinitionId::new(TASK_DEFINITION).expect("the definition id is valid"),
        "Reconcile one nightly batch.",
        schema("reconcile-input"),
        schema("reconcile-result"),
    )
    .expect("the task definition is valid")
    .with_result_rule(rakka_agent::AgentTaskResultRule::new(
        rakka_agent::AgentTaskRuleId::new("answer-present").expect("the rule id is valid"),
        rakka_agent::AgentTaskResultCheck::NonEmptyString {
            pointer: "/answer".to_string(),
        },
    ))
    .with_budgets(AgentBudgetCeilings {
        max_loop_iterations: Some(3),
        ..AgentBudgetCeilings::unbounded()
    })
}

/// Provenance for a revisioned acceptance at logical time `at`.
pub fn provenance(at: u64) -> AgentRevisionProvenance {
    AgentRevisionProvenance {
        principal: PrincipalRef {
            principal_type: "service".to_string(),
            principal_id: "operations".to_string(),
            display_name: None,
        },
        accepted_at: AgentTimestampMillis::new(at),
        causation_id: AgentCausationId::new(format!("cause-{at}")),
        audit_ref: AgentAuditEventId::new(format!("audit-{at}")),
    }
}

/// How late a scheduled occurrence may arrive and still admit directly.
pub const ADMISSION_WINDOW_MS: u64 = 10_000;
/// How late a scheduled occurrence may arrive before it counts as missed
/// and the downtime-backlog policy takes over.
pub const MAXIMUM_LATENESS_MS: u64 = 30_000;

/// The initial wake policy: durable-timer trigger, a bounded per-epoch model
/// budget, a one-minute epoch deadline, a ten-second admission window, a
/// thirty-second missed-occurrence bound, and failure backoff that escalates
/// after two consecutive failures. No goal window yet — the revision-2
/// policy introduces it.
pub fn initial_policy() -> AgentWakePolicy {
    let mut budget = AgentBudgetAllocation::unbounded();
    budget.set(AgentBudgetDimension::ModelCalls, Some(16));
    AgentWakePolicy::new([AgentWakeTriggerKind::DurableTimer], budget, Some(60_000))
        .expect("the wake policy is valid")
        .with_admission_window(ADMISSION_WINDOW_MS)
        .expect("the admission window is valid")
        .with_maximum_lateness(MAXIMUM_LATENESS_MS)
        .expect("the maximum lateness is valid")
        .with_failure_backoff(AgentWakeBackoffPolicy {
            escalate_after_failures: Some(2),
            ..AgentWakeBackoffPolicy::DEFAULT
        })
        .expect("the backoff policy is valid")
}

/// The revision-2 policy the schedule update takes into force: everything
/// the initial policy had, plus a rolling goal window that pays for exactly
/// one epoch, an expiry that renewal must extend, and retirement after the
/// ninth admitted occurrence.
pub fn windowed_policy() -> AgentWakePolicy {
    let mut ceiling = AgentBudgetAllocation::unbounded();
    ceiling.set(AgentBudgetDimension::ModelCalls, Some(16));
    initial_policy()
        .with_goal_window(AgentGoalWindowCeiling {
            window: AgentBudgetWindow::Rolling {
                length_millis: WINDOW_MS,
            },
            ceiling,
        })
        .expect("the windowed policy is valid")
        .with_lifecycle(AgentWakeLifecyclePolicy {
            expires_at: Some(AgentTimestampMillis::new(5_000_000)),
            renewal: AgentWakeRenewalPolicy::RequiredBefore {
                window_millis: 4_999_000,
            },
            retirement: AgentWakeRetirementPolicy::AfterOccurrences { occurrences: 9 },
            ..AgentWakeLifecyclePolicy::DEFAULT
        })
        .expect("the lifecycle policy is valid")
}

/// The continuous goal mode over `policy` at the initial schedule revision,
/// with the standard epoch contract.
pub fn continuous_goal_mode(policy: AgentWakePolicy) -> AgentGoalMode {
    AgentGoalMode::Continuous(Box::new(AgentContinuousGoalSpec {
        schedule_revision: ScheduleRevision::INITIAL,
        wake_policy: AgentWakePolicyRevision::initial(policy, provenance(1))
            .expect("the wake policy revision is valid"),
        health_condition: AgentPolicyRef::new("nightly-health").expect("the policy ref is valid"),
        epoch: Some(Box::new(AgentEpochSpec {
            definition: task_definition(),
            assignee: agent_id(),
            observation_scope: None,
        })),
    }))
}

/// A scheduled wake binding for the goal, due at `due_at` under `revision`.
pub fn scheduled_binding(due_at: u64, revision: ScheduleRevision) -> AgentWakeBinding {
    AgentWakeBinding::new(
        tenant(),
        goal_id(),
        revision,
        AgentWakeOccurrence::Scheduled {
            due_at: AgentTimestampMillis::new(due_at),
        },
        AgentWakeTriggerKind::DurableTimer,
        AgentTimestampMillis::new(due_at),
        AgentRevisionNumber::INITIAL,
    )
    .expect("the wake binding is valid")
}

/// The model turn every epoch answers with: a proposal the result rule
/// accepts on the first iteration.
fn proposing_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Reconciled.")
        .with_proposal(
            AgentTaskContent::inline(serde_json::json!({ "answer": "reconciled" }))
                .expect("the proposal is inline-bounded"),
        )
}

/// The whole continuous world the walk drives. Nothing but the stores
/// survives between calls.
pub struct World {
    /// Durable task records — the root controller and every epoch.
    pub tasks: TaskStore,
    /// Durable agent records.
    pub agents: AgentStore,
    /// Durable run records — the epochs' derived runs.
    pub runs: RunStore,
    /// The shared durable wake-timer index.
    pub wakes: WakeStore,
    /// Append-only task history.
    pub history: InMemoryAgentTaskHistoryStore,
    /// The delivery the scanner injects admission commands through.
    pub wake_delivery: WakeDelivery,
    /// The parker settle passes park controller-originated re-wakes through.
    pub rewake_parker: Arc<dyn AgentWakeRewakeParker>,
    /// The exchange router over the in-process entity transports.
    pub router: AgentExchangeRouter,
    /// The scripted dispatcher answering every epoch model call.
    pub dispatcher: ScriptedDispatcher<DeterministicModelAdapter>,
    /// The run-side effect sink.
    pub effects: InMemoryAgentRunEffectSink,
    /// The shared logical clock behind every timestamp.
    pub clock: Arc<AtomicU64>,
}

impl World {
    /// Builds the world: stores, transports, router, parker, delivery.
    #[must_use]
    pub fn new() -> Self {
        let tasks = TaskStore::new();
        let agents = AgentStore::new();
        let runs = RunStore::new();
        let wakes = WakeStore::new();
        let history = InMemoryAgentTaskHistoryStore::new();
        let effects = InMemoryAgentRunEffectSink::new();
        let clock = Arc::new(AtomicU64::new(1));

        let deferred = DeferredExchangeRouter::new();
        let task_transport = InProcessTaskEntityTransport::new(
            tasks.clone(),
            agents.clone(),
            history.clone(),
            deferred.as_router(),
            clock.clone(),
        );
        let run_transport = InProcessRunEntityTransport::new(
            runs.clone(),
            effects.clone(),
            deferred.as_router(),
            clock.clone(),
        )
        .with_effect_policies(AgentEffectPolicies::default());
        let router = AgentExchangeRouter::new()
            .with_route(AgentEntityClass::Task, Arc::new(task_transport))
            .with_route(AgentEntityClass::Run, Arc::new(run_transport));
        deferred.install(router.clone());

        let rewake_parker: Arc<dyn AgentWakeRewakeParker> =
            Arc::new(SharedWakeTimerParker::new(wakes.clone()));
        let wake_delivery = InProcessWakeDelivery::new(
            tasks.clone(),
            agents.clone(),
            history.clone(),
            router.clone(),
            clock.clone(),
        )
        .with_wake_timers(rewake_parker.clone());

        Self {
            tasks,
            agents,
            runs,
            wakes,
            history,
            wake_delivery,
            rewake_parker,
            router,
            dispatcher: ScriptedDispatcher::new().with_turn(proposing_turn()),
            effects,
            clock,
        }
    }

    /// One tick of the shared clock.
    pub fn now(&self) -> AgentTimestampMillis {
        AgentTimestampMillis::new(self.clock.fetch_add(1, Ordering::SeqCst))
    }

    /// Instantiates the epoch assignee with the walk's definition authorized.
    pub async fn instantiate_agent(&self) {
        let mut envelope = AgentAuthorityEnvelope::empty();
        envelope
            .task_definitions
            .insert(task_definition().definition_id);
        let definition = AgentDefinition::new(
            AgentDefinitionId::new("reconciler-v1").expect("the definition id is valid"),
            "Reconciles nightly batches.",
            envelope,
        )
        .expect("the agent definition is valid");
        let mut agent = AgentEntityStore::new(agent_scope(), self.agents.clone());
        agent.recover().await.expect("the agent recovers");
        agent
            .apply(AgentEntityCommand::Instantiate {
                operation_id: AgentOperationId::for_agent(
                    AgentOperationKind::DefinitionUpdate,
                    &agent_scope(),
                    "1",
                )
                .expect("the operation id derives"),
                definition: Box::new(definition),
                settings: Box::new(AgentSettings::default()),
                provenance: Box::new(provenance(1)),
            })
            .await
            .expect("the agent instantiates");
    }

    /// Creates the human-owned continuous root control task.
    pub async fn create_root(&self) {
        let reply = self
            .apply_root_command(AgentTaskEntityCommand::Create {
                operation_id: AgentOperationId::new(
                    AgentOperationKind::TaskCreation,
                    [TENANT, TASK, "1"],
                )
                .expect("the operation id derives"),
                creation: Box::new(AgentTaskCreation {
                    definition: task_definition()
                        .with_ownership(rakka_agent::AgentTaskOwnership::Human),
                    input: AgentTaskContent::inline(serde_json::json!({ "goal": GOAL }))
                        .expect("the input is inline-bounded"),
                    assignee: None,
                    goal: Some(goal_id()),
                    goal_mode: continuous_goal_mode(initial_policy()),
                    parent: None,
                    dependencies: Vec::new(),
                    escrow: None,
                    wake: None,
                    telemetry: Default::default(),
                }),
            })
            .await
            .expect("the root creation applies");
        assert!(
            matches!(reply, AgentTaskEntityReply::Applied { .. }),
            "the root is created, got {reply:?}"
        );
    }

    /// Applies one command to the root through a freshly materialized entity —
    /// every call is already a restart.
    pub async fn apply_root_command(
        &self,
        command: AgentTaskEntityCommand,
    ) -> Result<AgentTaskEntityReply, AgentTaskError> {
        let mut root = self.root_store();
        root.recover(self.now()).await?;
        root.apply(command, &self.router, self.now()).await
    }

    /// A fresh entity store over the root's durable record.
    #[must_use]
    pub fn root_store(
        &self,
    ) -> AgentTaskEntityStore<TaskStore, AgentStore, InMemoryAgentTaskHistoryStore> {
        AgentTaskEntityStore::new(
            task_scope(),
            self.tasks.clone(),
            self.agents.clone(),
            self.history.clone(),
        )
        .with_wake_timers(self.rewake_parker.clone())
    }

    /// Settles one task entity, the way a recovery sweep would.
    pub async fn settle_task(&self, scope: &AgentTaskScope) -> Result<(), String> {
        let mut task = AgentTaskEntityStore::new(
            scope.clone(),
            self.tasks.clone(),
            self.agents.clone(),
            self.history.clone(),
        )
        .with_wake_timers(self.rewake_parker.clone());
        task.recover(self.now())
            .await
            .map_err(|error| error.code().to_string())?;
        task.settle_side_effects(&self.router, self.now())
            .await
            .map_err(|error| error.code().to_string())?;
        Ok(())
    }

    /// A wake scanner over the durable wake index, its in-process delivery,
    /// and the shared clock. Built fresh per pass — a scanner holds nothing.
    #[must_use]
    pub fn scanner(&self) -> WakeScanner {
        AgentWakeScanner::with_clock_and_metrics(
            AgentWakeTimerStore::new(self.wakes.clone()),
            self.wake_delivery.clone(),
            SharedAtomicWorkflowClock::new(self.clock.clone()),
            AgentWakeScannerSettings::default(),
            Arc::new(rakka_core::NoopMetricsRecorder),
        )
    }

    /// Durably parks one scheduled occurrence, as the application's schedule
    /// layer would, and returns its binding.
    pub async fn schedule(&self, due_at: u64, revision: ScheduleRevision) -> AgentWakeBinding {
        let binding = scheduled_binding(due_at, revision);
        let entry = AgentWakeTimerEntry::new(
            binding.clone(),
            task_scope().task().clone(),
            AgentTimestampMillis::new(due_at),
        );
        AgentWakeTimerStore::new(self.wakes.clone())
            .schedule_occurrence(entry)
            .await
            .expect("the occurrence parks");
        binding
    }

    /// The controller's durable state, read fresh.
    pub async fn controller(&self) -> AgentWakeControllerState {
        let state = rakka_agent::load_agent_task_state(
            &self.tasks,
            &task_scope(),
            &AgentSchemaPolicy::default(),
        )
        .await
        .expect("the root state loads")
        .expect("the root exists");
        state
            .task()
            .expect("the root is created")
            .wake_controller
            .clone()
            .expect("the controller exists")
    }

    /// Drives the root, one epoch task, and that epoch's run until the run
    /// terminates and every owed exchange settles. Every entity is rebuilt
    /// from durable state each round.
    pub async fn pump_epoch(
        &self,
        epoch: &AgentTaskScope,
        run: &AgentRunScope,
    ) -> Result<(), String> {
        for _round in 0..64 {
            let mut outstanding = 0;
            for scope in [task_scope(), epoch.clone()] {
                let mut task = AgentTaskEntityStore::new(
                    scope,
                    self.tasks.clone(),
                    self.agents.clone(),
                    self.history.clone(),
                )
                .with_wake_timers(self.rewake_parker.clone());
                task.recover(self.now())
                    .await
                    .map_err(|error| error.code().to_string())?;
                let progress = task
                    .settle_side_effects(&self.router, self.now())
                    .await
                    .map_err(|error| error.code().to_string())?;
                outstanding += progress.outstanding;
            }

            let mut entity = rakka_agent::testkit::run_entity(run, &self.runs, &self.effects)
                .with_effect_policies(AgentEffectPolicies::default());
            let (progress, answered, terminal) = match entity.recover(self.now()).await {
                // The epoch's run may not exist yet: the creation and
                // assignment exchanges are still in flight.
                Err(_) => (Default::default(), 0, false),
                Ok(_) => {
                    let progress = entity
                        .settle_side_effects(&self.router, self.now())
                        .await
                        .map_err(|error| error.code().to_string())?;
                    let answered = self
                        .dispatcher
                        .drive(&mut entity, &self.router, self.now())
                        .await
                        .map_err(|error| error.code().to_string())?;
                    let terminal = entity
                        .state()
                        .ok()
                        .and_then(|state| state.status())
                        .is_some_and(rakka_agent::AgentRunStatus::is_terminal);
                    (progress, answered, terminal)
                }
            };

            if terminal && outstanding == 0 && progress.outstanding == 0 && answered == 0 {
                return Ok(());
            }
        }
        Err("the continuous world did not converge".to_string())
    }
}

impl Default for World {
    fn default() -> Self {
        Self::new()
    }
}
