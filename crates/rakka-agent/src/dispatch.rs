//! The durable effect dispatch pipeline over the `rakka-agent-workflow`
//! outbox and dispatcher fleet.
//!
//! This module is the dispatch half of the effect model
//! ([specification 11.4](../../../docs/plans/rakka-agent/spec.md),
//! [11.5](../../../docs/plans/rakka-agent/spec.md)): the run's transitions
//! commit effect intents into the run's own durable state
//! ([`crate::effect`]), and this pipeline carries each generation's dispatch
//! ticket through the substrate that already knows how to lease, fence, retry,
//! and cancel durable work — the per-run agent-workflow outbox and the
//! dispatcher fleet of `rakka-agent-workflow`'s `dispatcher.rs`.
//!
//! # The dispatch invariants, and where each one lives
//!
//! - **Durable `Started` with a lease and fence before invocation.** A worker
//!   first claims the fleet entry (a durable lease with a monotonic fencing
//!   token), then marks the outbox row `Dispatching` — both committed before
//!   any external call. A worker that dies afterwards leaves exactly that
//!   durable evidence, which is what recovery reads.
//! - **Dispatch-time credential resolution only.** The ticket names a logical
//!   [`crate::definition::AgentCredentialBindingRef`]; the pipeline resolves it
//!   through an [`AgentEffectCredentialResolver`] inside the bounded attempt,
//!   hands the [`rakka_agent_workflow::AgentEphemeralCredential`] to the
//!   executor, and drops it with the attempt. Nothing resolved is ever
//!   persisted, and the resolver is consulted only between `Started` and the
//!   invocation.
//! - **Stale-result rejection.** Every result command carries the effect id,
//!   generation, attempt, and the claim's fencing token; the run's own fence
//!   refuses what does not match ([`crate::run`]). The pipeline additionally
//!   refuses stale *tickets*: a claimed ticket whose run-side intent has moved
//!   on — resolved, superseded by a newer generation, or gone — settles as
//!   cancelled without invoking anything.
//! - **A dispatch grant before every attempt.** The required
//!   [`AgentDispatchAuthority`] is consulted before durable `Started`, per
//!   attempt, against the agent's *current* durable authority state: tool
//!   binding, definition/setup envelope, immediate-safety revocations,
//!   credential class, execution-policy routing, checkpoint requirements, and
//!   the guardrail chain ([specification 11.8](../../../docs/plans/rakka-agent/spec.md),
//!   [16](../../../docs/plans/rakka-agent/spec.md)). A refused attempt
//!   invokes nothing. A *definitive* refusal fails the effect with the
//!   refusal's stable code — unless a prior attempt of the generation may
//!   already have executed, in which case the generation parks
//!   `Indeterminate`, because "nothing was invoked" would be a lie. A
//!   *transient* refusal — a suspension — spends nothing: the outbox row is
//!   untouched and only the fleet entry is rescheduled, so the intent's
//!   attempt budget keeps meaning "external invocation attempts" and a
//!   resumed agent's next attempt rechecks and proceeds, whatever the budget.
//!
//! # Crash and timeout recovery per safety class
//!
//! The outbox row's status is the ambiguity marker. A row still `Scheduled`
//! proves the lost worker never reached the invocation — `Dispatching` is
//! committed first — so redispatch is safe for every class (scenario 5). A
//! row found `Dispatching` under an expired lease means the invocation *may*
//! have happened, and the class decides
//! ([specification 11.5](../../../docs/plans/rakka-agent/spec.md)):
//!
//! | Class | Recovery |
//! | --- | --- |
//! | `ReadOnly` | retry under the intent's bounded attempt policy |
//! | `Idempotent` | retry, reusing the generation's external idempotency key |
//! | `Reconcileable` | query the protocol; retry only when proven absent |
//! | `NonIdempotent` | one durable `Indeterminate`; never auto-retry |
//!
//! A retry-scheduled row proves only that no attempt is in flight *now* — a
//! prior attempt may still have reached the target. A `ReadOnly` or
//! `Idempotent` retry is safe by class; a `Reconcileable` retry re-queries
//! the protocol first and invokes only when proven absent, so an `Unknown`
//! finding burns an attempt without ever converting the ambiguity into a
//! routine retry; and a `NonIdempotent` row is never retry-scheduled, because
//! its validated single-attempt budget exhausts on the first failure.
//!
//! A parked `Indeterminate` generation revokes its own dispatch eligibility:
//! the outbox row and fleet entry settle as cancelled, so no worker can ever
//! claim it again, and only the explicit reconciliation decision of
//! [`crate::effect::AgentEffectResolution`] — which mints a *new* generation
//! and therefore a new ticket — can put the work back in flight.
//!
//! # Cancellation fencing
//!
//! When a run is winding down, [`AgentRunEffectDispatcher::pump_run`] fences
//! at the dispatch layer: tickets that provably never started are cancelled
//! (with a tombstone row planted for a ticket a laggard flush might still
//! write), attempts under an active lease finish truthfully, and an ambiguous
//! attempt is treated exactly as above — a cancelled run's ambiguous
//! non-idempotent effect still parks as `Indeterminate`, which is what keeps
//! the run nonterminal in reconciliation
//! ([specification 8.7](../../../docs/plans/rakka-agent/spec.md); scenario 57).
//!
//! Everything the pipeline does is idempotent: outbox rows deduplicate on the
//! generation's dispatch ticket id, fleet transitions are fenced by the claim
//! token, and result delivery deduplicates on the derived result operation id,
//! backed by the run's own effect fence. Running a pass twice — or from two
//! workers racing across a partition — converges on the same durable state.

use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use rakka_agent_workflow::substrate::{
    OutboxMessageId, OutboxStatus, RetryPolicy, WorkflowClock, WorkflowState,
    WorkflowTelemetryEvent,
};
use rakka_agent_workflow::{
    agent_effect_to_outbox_command, AgentDispatchClaim, AgentDispatcherError, AgentDispatcherFleet,
    AgentDispatcherFleetSettings, AgentDispatcherFleetState, AgentDispatcherWorkerId, AgentEffect,
    AgentEphemeralCredential, AgentInboxError, AgentOutboxError, AgentRunId as WorkflowRunId,
    AgentRunInbox,
};
use rakka_persistence::DurableStateStore;

use crate::agent::{load_agent_entity_state, AgentEntityError, AgentEntityState};
use crate::definition::{AgentCredentialBindingRef, AgentEffectSafetyClass, AgentSetupRevision};
use crate::effect::{
    compensation_call_id, AgentEffectError, AgentEffectGeneration, AgentReconciliationProtocolRef,
    AgentRunEffect, AgentRunEffectOutcome, AgentRunEffectRequest, AgentRunEffectSink,
    ATTR_AGENT_EFFECT_GENERATION, ATTR_AGENT_EFFECT_ID,
};
use crate::identity::{AgentIdentityError, AgentRunScope, AgentScope};
use crate::model::{AgentModelAdapter, AgentModelRequest, AgentToolCallRequest};
use crate::run::{
    load_agent_run_state, AgentRun, AgentRunEntityCommand, AgentRunEntityReply, AgentRunError,
    AgentRunState,
};
use crate::schema::{AgentSchemaError, AgentSchemaPolicy};
use crate::task::AgentTaskContent;
use crate::tools::{
    AgentAuthorityContext, AgentAuthorityRefusal, AgentGrantedDispatch, AgentToolAuthority,
};

/// Result type for dispatch pipeline operations.
pub type AgentDispatchResult<T> = Result<T, AgentDispatchError>;

/// The detail carried by an `Indeterminate` outcome parked for an ambiguity
/// the pipeline could not resolve.
const INDETERMINATE_OUTCOME_MESSAGE: &str =
    "the attempt's outcome could not be established; an explicit reconciliation decision is owed";

/// Boxed future returned by the pipeline's pluggable collaborators.
pub type AgentDispatchFuture<'a, T> =
    Pin<Box<dyn Future<Output = AgentDispatchResult<T>> + Send + 'a>>;

/// Maps one agent-domain run scope onto the workflow substrate's run id.
///
/// The scope key is injective, so two distinct runs can never share a durable
/// outbox, and the mapping is pure, so any node derives the same id.
#[must_use]
pub fn workflow_run_id(scope: &AgentRunScope) -> WorkflowRunId {
    WorkflowRunId::new(scope.key())
}

/// The [`AgentRunEffectSink`] backed by the run's durable agent-workflow
/// outbox.
///
/// [`AgentRunEffectSink::dispatch`] schedules the generation's dispatch ticket
/// as an outbox row, deduplicated on the ticket id, with the row's retry
/// budget aligned to the intent's attempt bound — so the outbox can never
/// retry an attempt the intent's policy does not permit
/// ([specification 11.4](../../../docs/plans/rakka-agent/spec.md)).
pub struct WorkflowAgentRunEffectSink<Store, Clock>
where
    Store: DurableStateStore<WorkflowState>,
    Clock: WorkflowClock,
{
    store: Store,
    clock: Clock,
    retry_backoff_ms: u64,
}

impl<Store, Clock> WorkflowAgentRunEffectSink<Store, Clock>
where
    Store: DurableStateStore<WorkflowState>,
    Clock: WorkflowClock,
{
    /// Creates a sink over the given durable workflow store.
    #[must_use]
    pub fn new(store: Store, clock: Clock) -> Self {
        Self {
            store,
            clock,
            retry_backoff_ms: 0,
        }
    }

    /// Sets the backoff applied between an attempt's failure and its retry.
    #[must_use]
    pub const fn with_retry_backoff_ms(mut self, retry_backoff_ms: u64) -> Self {
        self.retry_backoff_ms = retry_backoff_ms;
        self
    }
}

impl<Store, Clock> Clone for WorkflowAgentRunEffectSink<Store, Clock>
where
    Store: DurableStateStore<WorkflowState>,
    Clock: WorkflowClock,
{
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            clock: self.clock.clone(),
            retry_backoff_ms: self.retry_backoff_ms,
        }
    }
}

impl<Store, Clock> Debug for WorkflowAgentRunEffectSink<Store, Clock>
where
    Store: DurableStateStore<WorkflowState>,
    Clock: WorkflowClock,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkflowAgentRunEffectSink")
            .field("backend", &self.store.backend_name())
            .field("retry_backoff_ms", &self.retry_backoff_ms)
            .finish_non_exhaustive()
    }
}

impl<Store, Clock> AgentRunEffectSink for WorkflowAgentRunEffectSink<Store, Clock>
where
    Store: DurableStateStore<WorkflowState>,
    Clock: WorkflowClock,
{
    fn backend_name(&self) -> &'static str {
        "agent-workflow-outbox"
    }

    fn dispatch<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        effect: &'a AgentEffect,
    ) -> crate::effect::AgentEffectFuture<'a, ()> {
        Box::pin(async move {
            let mut inbox = AgentRunInbox::with_clock(
                workflow_run_id(scope),
                self.store.clone(),
                self.clock.clone(),
            );
            inbox.recover().await.map_err(sink_error)?;

            let mut command = agent_effect_to_outbox_command(effect).map_err(sink_error)?;
            let max_attempts = effect
                .target
                .attributes
                .get(crate::effect::ATTR_AGENT_EFFECT_MAX_ATTEMPTS)
                .and_then(|value| value.parse::<u32>().ok())
                .unwrap_or(1)
                .max(1);
            command = command.retry_policy(RetryPolicy::new(
                max_attempts,
                self.retry_backoff_ms,
                self.retry_backoff_ms.max(1),
            ));

            // Scheduling deduplicates on the ticket id, so a re-driven flush —
            // or two owners racing across a shard movement — lands one row.
            let _acceptance =
                inbox
                    .inner_mut()
                    .schedule_outbox(command)
                    .await
                    .map_err(|error| AgentEffectError::Sink {
                        code: "outbox-schedule-failed".to_string(),
                        message: error.to_string(),
                    })?;
            Ok(())
        })
    }
}

fn sink_error(error: impl Display) -> AgentEffectError {
    AgentEffectError::Sink {
        code: "outbox-schedule-failed".to_string(),
        message: error.to_string(),
    }
}

/// Delivers durable result commands to the owning run entity.
///
/// In production this is a sharded ask to the run entity's owner; in tests it
/// is an in-process application against the entity store. Either way the
/// command is deduplicated by its derived operation id and fenced by the run's
/// own state, so delivering twice cannot advance the run twice.
pub trait AgentRunResultDelivery: Send + Sync {
    /// Delivers one command, returning the entity's reply.
    fn deliver<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        command: AgentRunEntityCommand,
    ) -> AgentDispatchFuture<'a, AgentRunEntityReply>;
}

/// Executes one tool call inside a bounded dispatch attempt.
///
/// This is the interim execution surface until the slice 1.8 tool registry.
/// The intent rides along because the *target* needs parts of it: the
/// idempotency key every retry of the generation must hand over unchanged
/// ([specification 11.4](../../../docs/plans/rakka-agent/spec.md)), and the
/// attempt's timeout. The resolved credential — when the intent names a
/// binding — lives only for the call and is never persisted.
pub trait AgentDispatchToolExecutor: Send + Sync {
    /// Performs the call and returns its bounded result.
    fn execute<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        intent: &'a AgentRunEffect,
        call: &'a AgentToolCallRequest,
        credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentDispatchFuture<'a, AgentTaskContent>;
}

/// Executes one explicitly scheduled compensation inside a bounded dispatch
/// attempt ([specification 12.5](../../../docs/plans/rakka-agent/spec.md)).
///
/// Rakka persists and routes the
/// [`crate::checkpoints::AgentCompensationRef`]; the application owns the
/// compensation behind it. The resolved credential — when the intent names a
/// binding — lives only for the call and is never persisted.
pub trait AgentCompensationExecutor: Send + Sync {
    /// Performs the compensation and returns its bounded result.
    fn execute<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        intent: &'a AgentRunEffect,
        compensation: &'a crate::checkpoints::AgentCompensationRef,
        credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentDispatchFuture<'a, AgentTaskContent>;
}

/// Resolves a logical credential binding for one dispatch attempt
/// ([specification 16](../../../docs/plans/rakka-agent/spec.md)).
///
/// The resolver is consulted only after the attempt's durable `Started`, and
/// the resolved value is dropped with the attempt.
pub trait AgentEffectCredentialResolver: Send + Sync {
    /// Resolves the binding into an ephemeral in-memory credential.
    fn resolve<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        binding: &'a AgentCredentialBindingRef,
        effect: &'a AgentRunEffect,
    ) -> AgentDispatchFuture<'a, AgentEphemeralCredential>;
}

/// What a reconciliation protocol established about an ambiguous attempt.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum AgentReconciliationFinding {
    /// The invocation happened; this is its authoritative outcome.
    Executed {
        /// The established outcome.
        outcome: Box<AgentRunEffectOutcome>,
    },
    /// The invocation provably never happened.
    NotExecuted,
    /// The protocol could not establish the outcome yet.
    Unknown,
}

/// Queries the authoritative outcome of an ambiguous `Reconcileable` attempt
/// ([specification 11.5](../../../docs/plans/rakka-agent/spec.md)).
pub trait AgentEffectReconciler: Send + Sync {
    /// Runs the named protocol against the external system of record.
    fn reconcile<'a>(
        &'a self,
        protocol: &'a AgentReconciliationProtocolRef,
        scope: &'a AgentRunScope,
        effect: &'a AgentRunEffect,
    ) -> AgentDispatchFuture<'a, AgentReconciliationFinding>;
}

/// What the authority decided about one dispatch attempt.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum AgentDispatchDecision {
    /// The attempt is authorized under the carried grant.
    Granted(Box<AgentGrantedDispatch>),
    /// The attempt is refused; the effect stays undispatchable.
    Refused(AgentAuthorityRefusal),
}

/// Issues (or refuses) the dispatch grant one attempt needs, from the agent's
/// current durable authority state
/// ([specification 11.8](../../../docs/plans/rakka-agent/spec.md),
/// [16](../../../docs/plans/rakka-agent/spec.md)).
///
/// The pipeline consults it before every attempt's durable `Started` — never
/// once per effect — which is what makes immediate revocation and grant
/// validity per-attempt facts rather than commit-time ones. There is no
/// permissive default: a pipeline cannot be constructed without an authority,
/// because a dispatcher that skips the check is exactly the universally
/// privileged worker [specification 16](../../../docs/plans/rakka-agent/spec.md)
/// forbids claiming isolation from.
pub trait AgentDispatchAuthority: Send + Sync {
    /// Authorizes one dispatch attempt of one effect intent, or refuses it.
    ///
    /// `attempt` is the 1-based attempt number the pipeline is about to make:
    /// a checkpoint grant's allowed use count is enforced against it, so a
    /// spent grant does not cover a retry
    /// ([specification 12.3](../../../docs/plans/rakka-agent/spec.md)).
    fn authorize<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        run: &'a AgentRunState,
        intent: &'a AgentRunEffect,
        attempt: u32,
        now: rakka_agent_workflow::AgentTimestampMillis,
    ) -> AgentDispatchFuture<'a, AgentDispatchDecision>;
}

/// Resolves the setup revision one run was created under, so the dispatch
/// gate can enforce the *right* setup for whichever run's ticket a fleet
/// worker happens to claim
/// ([specification 7.3](../../../docs/plans/rakka-agent/spec.md)).
///
/// The claim batch is fleet-wide: one worker serves every run whose tickets
/// are due, so a setup fixed per authority instance would be enforced against
/// runs it does not govern — a false, terminal `setup-excludes-tool` refusal
/// for one run, and a silently skipped narrowing for another. Until runs
/// carry a durable setup reference of their own, this resolver is the seam a
/// deployment maps run scopes onto setups through; returning `None` means the
/// run was created under no setup.
pub trait AgentRunSetupResolver: Send + Sync {
    /// The setup revision the given run was created under, when any.
    fn setup_for(&self, scope: &AgentRunScope) -> Option<AgentSetupRevision>;
}

/// An [`AgentRunSetupResolver`] binding one setup to exactly one run.
struct SingleRunSetup {
    scope: AgentRunScope,
    setup: AgentSetupRevision,
}

impl AgentRunSetupResolver for SingleRunSetup {
    fn setup_for(&self, scope: &AgentRunScope) -> Option<AgentSetupRevision> {
        (scope == &self.scope).then(|| self.setup.clone())
    }
}

/// The [`AgentDispatchAuthority`] backed by the agent entity's durable state.
///
/// It reads the same record the [`crate::agent::AgentEntity`] transitions —
/// definition revision, current settings revision, lifecycle status — so a
/// suspension or an immediate-safety revocation accepted by the entity is
/// honored by the very next dispatch attempt, with the agent fully
/// passivated. The run's setup is resolved per claimed run through the
/// [`AgentRunSetupResolver`] until runs carry their setup reference; a
/// deployment that creates runs under setups wires the resolver that knows
/// which run got which.
pub struct AgentEntityAuthority<Agents>
where
    Agents: DurableStateStore<AgentEntityState>,
{
    agents: Agents,
    authority: AgentToolAuthority,
    schema_policy: AgentSchemaPolicy,
    setups: Option<Arc<dyn AgentRunSetupResolver>>,
}

impl<Agents> AgentEntityAuthority<Agents>
where
    Agents: DurableStateStore<AgentEntityState>,
{
    /// Creates an authority gate over the agent entity store.
    #[must_use]
    pub fn new(agents: Agents, authority: AgentToolAuthority) -> Self {
        Self {
            agents,
            authority,
            schema_policy: AgentSchemaPolicy::default(),
            setups: None,
        }
    }

    /// Uses an explicit schema-compatibility policy for the agent states it
    /// reads.
    #[must_use]
    pub const fn with_schema_policy(mut self, policy: AgentSchemaPolicy) -> Self {
        self.schema_policy = policy;
        self
    }

    /// Resolves run setups at dispatch through the given resolver
    /// ([specification 7.3](../../../docs/plans/rakka-agent/spec.md)).
    #[must_use]
    pub fn with_setup_resolver(mut self, setups: Arc<dyn AgentRunSetupResolver>) -> Self {
        self.setups = Some(setups);
        self
    }

    /// Enforces the given run setup at dispatch, for exactly the given run —
    /// never for the other runs a fleet worker's claim batch may carry.
    #[must_use]
    pub fn with_setup_for_run(self, scope: AgentRunScope, setup: AgentSetupRevision) -> Self {
        self.with_setup_resolver(Arc::new(SingleRunSetup { scope, setup }))
    }
}

impl<Agents> Debug for AgentEntityAuthority<Agents>
where
    Agents: DurableStateStore<AgentEntityState>,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentEntityAuthority")
            .field("agents", &self.agents.backend_name())
            .field("authority", &self.authority)
            .finish_non_exhaustive()
    }
}

impl<Agents> AgentDispatchAuthority for AgentEntityAuthority<Agents>
where
    Agents: DurableStateStore<AgentEntityState>,
{
    fn authorize<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        run: &'a AgentRunState,
        intent: &'a AgentRunEffect,
        attempt: u32,
        now: rakka_agent_workflow::AgentTimestampMillis,
    ) -> AgentDispatchFuture<'a, AgentDispatchDecision> {
        Box::pin(async move {
            let agent_scope = AgentScope::new(scope.tenant().clone(), scope.agent().clone())?;
            let state = load_agent_entity_state(&self.agents, &agent_scope, &self.schema_policy)
                .await
                .map_err(agent_state_error)?;
            let Some(state) = state else {
                // No durable agent, no authority to dispatch under: fail
                // closed rather than inventing a permissive default.
                return Ok(AgentDispatchDecision::Refused(AgentAuthorityRefusal::of(
                    "agent-state-missing",
                    format!("no durable agent state exists for {agent_scope:?}"),
                )));
            };

            // The setup is a per-run fact: resolve it for the run this claim
            // belongs to, not for whichever run the authority happened to be
            // built beside.
            let setup = self
                .setups
                .as_ref()
                .and_then(|setups| setups.setup_for(scope));
            let mut context = AgentAuthorityContext::for_entity(&state);
            if let Some(setup) = &setup {
                context = context.with_setup(setup);
            }
            // A checkpoint-gated effect carries no grant until the run has
            // resolved its checkpoint; the run holds the digest-bound grant in
            // its own durable state, and the authority revalidates it against
            // the exact intent before dispatch
            // ([specification 12.3](../../../docs/plans/rakka-agent/spec.md)).
            if let Some(grant) = run
                .loop_state()
                .and_then(|loop_state| loop_state.grant_for(intent))
            {
                context = context.with_checkpoint_grant(grant);
            }
            let task = run.run().map(AgentRun::task);
            let goal = run.loop_state().and_then(|loop_state| loop_state.goal());
            let decision = match self
                .authority
                .authorize(&context, scope, task, goal, intent, attempt, now)
            {
                Ok(granted) => AgentDispatchDecision::Granted(Box::new(granted)),
                Err(refusal) => AgentDispatchDecision::Refused(refusal),
            };
            Ok(decision)
        })
    }
}

fn agent_state_error(error: AgentEntityError) -> AgentDispatchError {
    AgentDispatchError::Collaborator {
        code: error.code().to_string(),
        message: error.to_string(),
    }
}

/// The durable boundaries of one dispatch attempt, in order.
///
/// Each window sits between two durable writes, so killing a worker at one
/// leaves exactly the durable evidence a real crash leaves there. The probe
/// that observes them exists for fault injection; slice 1.14 extends the same
/// windows across the whole M1 suite.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AgentDispatchWindow {
    /// The claim is leased; durable `Started` has not been written.
    BeforeStarted,
    /// Durable `Started` is written; the target has not been invoked.
    AfterStarted,
    /// The target committed; no receipt has been recorded anywhere.
    AfterInvocation,
    /// The run durably holds the result; the outbox row is not yet settled.
    AfterResultDelivery,
}

impl AgentDispatchWindow {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::BeforeStarted => "before-started",
            Self::AfterStarted => "after-started",
            Self::AfterInvocation => "after-invocation",
            Self::AfterResultDelivery => "after-result-delivery",
        }
    }
}

/// Observes the durable boundaries of every dispatch attempt.
pub trait AgentDispatchProbe: Send + Sync {
    /// Returns false to kill the worker at this window: the pass abandons the
    /// attempt exactly where a crash would, leaving its lease to expire.
    fn survives(&self, window: AgentDispatchWindow) -> bool;
}

/// What one [`AgentRunEffectDispatcher::pump_run`] pass did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AgentDispatchPass {
    /// Dispatch tickets registered into the fleet from the run's outbox.
    pub registered: usize,
    /// Fleet claims taken.
    pub claimed: usize,
    /// External invocations performed.
    pub invoked: usize,
    /// Result commands durably delivered to run entities.
    pub delivered: usize,
    /// Tickets settled as cancelled without invocation.
    pub cancelled: usize,
    /// Generations parked as indeterminate.
    pub parked_indeterminate: usize,
    /// Attempts recorded as failed (retry scheduled or exhausted).
    pub failed_attempts: usize,
    /// Claims deferred by a transient refusal, spending nothing durable.
    pub deferred: usize,
    /// True when the probe killed the worker mid-pass.
    pub died: bool,
}

/// How one claimed ticket was concluded.
enum ClaimConclusion {
    /// The attempt ran to a settled outbox row.
    Settled,
    /// The probe killed the worker; durable state is exactly as a crash
    /// leaves it.
    Died,
}

/// The durable effect dispatcher for agent runs.
///
/// It composes the run's agent-workflow outbox (the dispatch tickets), the
/// dispatcher fleet (leases and fencing), the model adapter and tool executor
/// (the bounded external I/O), the credential resolver (dispatch-time
/// resolution only), the reconciler (the `Reconcileable` protocol), and the
/// result delivery back to the owning run entity.
///
/// The pipeline holds no state of its own beyond the durable stores it reads;
/// killing it at any point and building a new one is recovery, not repair.
pub struct AgentRunEffectDispatcher<Flow, Fleet, Runs, Clock>
where
    Flow: DurableStateStore<WorkflowState>,
    Fleet: DurableStateStore<AgentDispatcherFleetState>,
    Runs: DurableStateStore<AgentRunState>,
    Clock: WorkflowClock,
{
    worker_id: AgentDispatcherWorkerId,
    workflow_store: Flow,
    fleet_store: Fleet,
    fleet: AgentDispatcherFleet<Fleet, Clock>,
    runs: Runs,
    clock: Clock,
    schema_policy: AgentSchemaPolicy,
    retry_backoff_ms: u64,
    model: Arc<dyn AgentModelAdapter>,
    tools: Arc<dyn AgentDispatchToolExecutor>,
    authority: Arc<dyn AgentDispatchAuthority>,
    credentials: Option<Arc<dyn AgentEffectCredentialResolver>>,
    reconciler: Option<Arc<dyn AgentEffectReconciler>>,
    compensations: Option<Arc<dyn AgentCompensationExecutor>>,
    delivery: Arc<dyn AgentRunResultDelivery>,
    probe: Option<Arc<dyn AgentDispatchProbe>>,
}

impl<Flow, Fleet, Runs, Clock> AgentRunEffectDispatcher<Flow, Fleet, Runs, Clock>
where
    Flow: DurableStateStore<WorkflowState>,
    Fleet: DurableStateStore<AgentDispatcherFleetState>,
    Runs: DurableStateStore<AgentRunState>,
    Clock: WorkflowClock,
{
    /// Creates a dispatcher worker over the durable stores it reads.
    ///
    /// The authority is a required collaborator, not an option: every attempt
    /// is authorized against it before durable `Started`
    /// ([specification 11.8](../../../docs/plans/rakka-agent/spec.md)), and a
    /// pipeline that could skip that check would be the universally
    /// privileged worker [specification 16](../../../docs/plans/rakka-agent/spec.md)
    /// forbids.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        worker_id: AgentDispatcherWorkerId,
        workflow_store: Flow,
        fleet_store: Fleet,
        runs: Runs,
        clock: Clock,
        model: Arc<dyn AgentModelAdapter>,
        tools: Arc<dyn AgentDispatchToolExecutor>,
        authority: Arc<dyn AgentDispatchAuthority>,
        delivery: Arc<dyn AgentRunResultDelivery>,
    ) -> Self {
        let fleet = AgentDispatcherFleet::with_clock_and_metrics(
            fleet_store.clone(),
            rakka_agent_workflow::agent_dispatcher_fleet_persistence_id(),
            AgentDispatcherFleetSettings::default(),
            clock.clone(),
            Arc::new(rakka_core::NoopMetricsRecorder),
        );
        Self {
            worker_id,
            workflow_store,
            fleet_store,
            fleet,
            runs,
            clock,
            schema_policy: AgentSchemaPolicy::default(),
            retry_backoff_ms: 0,
            model,
            tools,
            authority,
            credentials: None,
            reconciler: None,
            compensations: None,
            delivery,
            probe: None,
        }
    }

    /// Uses explicit fleet settings (lease duration, batch size, concurrency).
    #[must_use]
    pub fn with_fleet_settings(mut self, settings: AgentDispatcherFleetSettings) -> Self {
        self.fleet = AgentDispatcherFleet::with_clock_and_metrics(
            self.fleet_store.clone(),
            rakka_agent_workflow::agent_dispatcher_fleet_persistence_id(),
            settings,
            self.clock.clone(),
            Arc::new(rakka_core::NoopMetricsRecorder),
        );
        self
    }

    /// Uses an explicit schema-compatibility policy for the run states it
    /// reads.
    #[must_use]
    pub const fn with_schema_policy(mut self, policy: AgentSchemaPolicy) -> Self {
        self.schema_policy = policy;
        self
    }

    /// Resolves credential bindings through the given resolver.
    #[must_use]
    pub fn with_credential_resolver(
        mut self,
        resolver: Arc<dyn AgentEffectCredentialResolver>,
    ) -> Self {
        self.credentials = Some(resolver);
        self
    }

    /// Reconciles `Reconcileable` effects through the given protocol runner.
    #[must_use]
    pub fn with_reconciler(mut self, reconciler: Arc<dyn AgentEffectReconciler>) -> Self {
        self.reconciler = Some(reconciler);
        self
    }

    /// Executes operator-scheduled compensation effects through the given
    /// application-owned executor
    /// ([specification 12.5](../../../docs/plans/rakka-agent/spec.md)). Without
    /// one, a compensation dispatch fails closed with a stable code.
    #[must_use]
    pub fn with_compensation_executor(
        mut self,
        compensations: Arc<dyn AgentCompensationExecutor>,
    ) -> Self {
        self.compensations = Some(compensations);
        self
    }

    /// Observes (and, in tests, kills) the worker at each durable boundary.
    #[must_use]
    pub fn with_probe(mut self, probe: Arc<dyn AgentDispatchProbe>) -> Self {
        self.probe = Some(probe);
        self
    }

    /// The sink runs served by this pipeline should flush through.
    #[must_use]
    pub fn sink(&self) -> WorkflowAgentRunEffectSink<Flow, Clock> {
        WorkflowAgentRunEffectSink::new(self.workflow_store.clone(), self.clock.clone())
            .with_retry_backoff_ms(self.retry_backoff_ms)
    }

    fn inbox(&self, scope: &AgentRunScope) -> AgentRunInbox<Flow, Clock> {
        AgentRunInbox::with_clock(
            workflow_run_id(scope),
            self.workflow_store.clone(),
            self.clock.clone(),
        )
    }

    fn survives(&self, window: AgentDispatchWindow) -> bool {
        self.probe
            .as_ref()
            .is_none_or(|probe| probe.survives(window))
    }

    async fn run_state(&self, scope: &AgentRunScope) -> AgentDispatchResult<Option<AgentRunState>> {
        Ok(load_agent_run_state(&self.runs, scope, &self.schema_policy).await?)
    }

    /// One bounded dispatch pass: register the given run's due tickets and
    /// fence it if it is winding down, then claim and execute what the fleet
    /// has due.
    ///
    /// Only the registration and the wind-down fence are scoped to the given
    /// run. The claim batch is *fleet-wide* — a worker serves every run whose
    /// tickets are due, so a pass may execute tickets other runs registered;
    /// each claim re-derives its own run scope and is recovered under that
    /// run's intent.
    ///
    /// The pass reads only durable state, so calling it after a crash, after a
    /// clock advance past a lease, or twice in a row are all the same
    /// operation.
    pub async fn pump_run(
        &mut self,
        scope: &AgentRunScope,
    ) -> AgentDispatchResult<AgentDispatchPass> {
        let mut pass = AgentDispatchPass::default();
        self.fleet.recover().await?;

        // Register the run's due dispatch tickets into the fleet.
        let mut inbox = self.inbox(scope);
        inbox.recover().await?;
        let due = inbox.due_effects()?;
        if !due.is_empty() {
            let registration = self
                .fleet
                .register_due_effects(workflow_run_id(scope), None, due)
                .await?;
            pass.registered = registration.registered_effects;
        }

        // Fence a winding-down run at the dispatch layer before anything can
        // be claimed on its behalf.
        let run_state = self.run_state(scope).await?;
        if let Some(state) = &run_state {
            let winding_down = state
                .run()
                .is_some_and(|run| run.terminal_reason.is_some() || run.status.is_terminal());
            if winding_down {
                self.fence_run(scope, state, &mut pass).await?;
            }
        }

        // Claim and execute what is due — including expired-lease entries,
        // whose re-claim under a fresh fencing token *is* the recovery path.
        let batch = self.fleet.claim_due(self.worker_id.clone()).await?;
        pass.claimed = batch.claims.len();
        for claim in batch.claims {
            let claim_scope = AgentRunScope::parse(claim.run_id.as_str())?;
            match self.execute_claim(&claim_scope, claim, &mut pass).await? {
                ClaimConclusion::Settled => {}
                ClaimConclusion::Died => {
                    pass.died = true;
                    break;
                }
            }
        }
        Ok(pass)
    }

    /// Executes one claimed dispatch ticket to a settled conclusion.
    async fn execute_claim(
        &mut self,
        scope: &AgentRunScope,
        claim: AgentDispatchClaim,
        pass: &mut AgentDispatchPass,
    ) -> AgentDispatchResult<ClaimConclusion> {
        let message_id = OutboxMessageId::new(claim.effect_id.as_str());
        let mut inbox = self.inbox(scope);
        inbox.recover().await?;

        let Some(row) = inbox
            .inner()
            .state()
            .map_err(AgentInboxError::from)?
            .outbox_entry(&message_id)
            .cloned()
        else {
            // A fleet entry without its outbox row cannot be executed; settle
            // it so it is never claimed again.
            self.settle_ticket_cancelled(scope, &claim, "ticket-row-missing", pass)
                .await?;
            return Ok(ClaimConclusion::Settled);
        };
        let attempt = row.attempts().attempts().saturating_add(1);

        // The run's durable intent is the source of truth for what this ticket
        // is and what its policy permits.
        let run_state = self.run_state(scope).await?;
        let ticket: AgentEffect = serde_json::from_slice(row.payload()).map_err(|error| {
            AgentDispatchError::TicketUndecodable {
                message: error.to_string(),
            }
        })?;
        let intent = run_state.as_ref().and_then(|state| {
            let effect_id = ticket.target.attributes.get(ATTR_AGENT_EFFECT_ID)?;
            let generation = ticket
                .target
                .attributes
                .get(ATTR_AGENT_EFFECT_GENERATION)?
                .parse::<u32>()
                .ok()
                .map(AgentEffectGeneration::new)?;
            let effect = state
                .loop_state()?
                .effects()
                .iter()
                .find(|effect| effect.effect_id.as_str() == effect_id)?
                .clone();
            (effect.generation == generation).then_some(effect)
        });

        // Stale-ticket rejection: a ticket whose intent has moved on — the
        // generation was superseded, the effect resolved, or the run is gone —
        // is settled without invoking anything.
        let Some(intent) = intent else {
            self.settle_ticket_cancelled(scope, &claim, "ticket-stale", pass)
                .await?;
            return Ok(ClaimConclusion::Settled);
        };
        let state = run_state
            .as_ref()
            .expect("a resolved intent implies the run state it was read from");
        if intent.status.is_resolved() {
            self.settle_ticket_cancelled(scope, &claim, "intent-already-resolved", pass)
                .await?;
            return Ok(ClaimConclusion::Settled);
        }

        let winding_down = run_state
            .as_ref()
            .and_then(AgentRunState::run)
            .is_some_and(|run| run.terminal_reason.is_some() || run.status.is_terminal());

        // `Dispatching` is the durable ambiguity marker: a previous attempt
        // wrote its `Started` and disappeared, so the invocation may have
        // happened ([specification 11.5]). A `Reconcileable` intent is
        // ambiguous on a retry-scheduled row too: burning an attempt — for an
        // ambiguous loss the protocol could not yet resolve, or a failure
        // whose report may itself be wrong about the target — rewrites the
        // row to a retryable failure without proving anything about what the
        // prior attempt did, and a retry of this class is only ever safe when
        // the protocol proves absence. So every non-first attempt re-queries
        // before it may invoke.
        let ambiguous = row.status() == OutboxStatus::Dispatching
            || (row.status() == OutboxStatus::Failed
                && intent.safety.class() == AgentEffectSafetyClass::Reconcileable);
        if ambiguous {
            return self
                .recover_ambiguous(scope, claim, state, &intent, attempt, winding_down, pass)
                .await;
        }

        if winding_down {
            // The fence: a ticket that provably never started is cancelled and
            // its intent settled, never dispatched after the cancellation.
            self.settle_ticket_cancelled(scope, &claim, "run-cancelled", pass)
                .await?;
            self.deliver_outcome(
                scope,
                &intent,
                attempt,
                claim.fencing_token,
                AgentRunEffectOutcome::Cancelled {
                    reason: "run-cancelled".to_string(),
                },
                pass,
            )
            .await?;
            return Ok(ClaimConclusion::Settled);
        }

        self.attempt_invocation(scope, claim, state, &intent, attempt, pass)
            .await
    }

    /// One fresh, unambiguous dispatch attempt: authorization, durable
    /// `Started`, bounded invocation, result delivery, settlement.
    async fn attempt_invocation(
        &mut self,
        scope: &AgentRunScope,
        claim: AgentDispatchClaim,
        state: &AgentRunState,
        intent: &AgentRunEffect,
        attempt: u32,
        pass: &mut AgentDispatchPass,
    ) -> AgentDispatchResult<ClaimConclusion> {
        let message_id = OutboxMessageId::new(claim.effect_id.as_str());

        // Whether an earlier attempt of this generation may already have
        // reached the target. For an idempotent effect every non-first
        // attempt follows a possibly-delivered one — that is why its retries
        // reuse the external key — so an undispatchable verdict on such an
        // attempt must park the ambiguity, not assert that nothing was
        // invoked. Read-only history is inconsequential, a reconcileable
        // retry runs only once absence is proven, and a non-idempotent effect
        // never retries.
        let possibly_executed =
            intent.safety.class() == AgentEffectSafetyClass::Idempotent && attempt > 1;

        // The adapter's declared retry policy is re-enforced at dispatch
        // ([specification 11.2](../../../docs/plans/rakka-agent/spec.md): the
        // adapter supplies the permitted safety declaration). An intent whose
        // configured policy is *weaker* — a laxer safety class, or more
        // attempts than the adapter permits — fails closed before durable
        // `Started` is even written: a ticket the ceiling refuses must never
        // be recorded as possibly in flight, and recovery would read the
        // intent and retry what the adapter declared unsafe.
        if matches!(intent.request, AgentRunEffectRequest::Model { .. }) {
            let declared = self.model.retry_policy();
            let weaker = intent.safety.class().strictness() < declared.safety_class.strictness()
                || intent.max_attempts > declared.max_attempts;
            if weaker {
                let refusal = AgentAuthorityRefusal::of(
                    "model-policy-conflict",
                    format!(
                        "the effect's policy ({} class, {} attempts) is weaker than the \
                         adapter's declaration ({} class, {} attempts)",
                        intent.safety.class(),
                        intent.max_attempts,
                        declared.safety_class,
                        declared.max_attempts
                    ),
                );
                return self
                    .settle_undispatchable(
                        scope,
                        &claim,
                        intent,
                        attempt,
                        &refusal,
                        possibly_executed,
                        pass,
                    )
                    .await;
            }
        }

        // Layer 4, the dispatch grant ([specification 11.8]): every attempt is
        // authorized against the agent's *current* durable authority state
        // before durable `Started` is written, and the issued grant is
        // revalidated against the exact intent it must cover. A transient
        // refusal — a suspension — defers the ticket without spending the
        // intent's budget, so a resumed agent's next attempt rechecks and
        // proceeds whatever that budget is; a definitive refusal — an
        // undeclared or revoked tool, a widened intent, a missing checkpoint,
        // an unroutable execution policy, a blocked guardrail — settles the
        // generation (scenario 54).
        let now = rakka_agent_workflow::AgentTimestampMillis::new(self.clock.now().as_millis());
        let decision = self
            .authority
            .authorize(scope, state, intent, attempt, now)
            .await?;
        let granted = match decision {
            AgentDispatchDecision::Granted(granted) => {
                if let Err(refusal) = granted.grant.validate_for(scope, intent, attempt, now) {
                    return self
                        .settle_refusal(
                            scope,
                            &claim,
                            intent,
                            attempt,
                            &refusal,
                            possibly_executed,
                            pass,
                        )
                        .await;
                }
                granted
            }
            AgentDispatchDecision::Refused(refusal) => {
                return self
                    .settle_refusal(
                        scope,
                        &claim,
                        intent,
                        attempt,
                        &refusal,
                        possibly_executed,
                        pass,
                    )
                    .await;
            }
        };

        // Transform determinism across the generation ([specification 16]):
        // when a guardrail transform decides the executed payload — or a
        // prior attempt of this generation may already have sent one — the
        // chain evaluated now must be provably the chain the intent was
        // committed under, or one external idempotency key could carry two
        // different payloads.
        if matches!(intent.request, AgentRunEffectRequest::Tool { .. })
            && (granted.tool_call.is_some() || attempt > 1)
            && intent.guardrail_revision != granted.grant.guardrail_revision
        {
            let refusal = AgentAuthorityRefusal::of(
                "guardrail-revision-mismatch",
                match (intent.guardrail_revision, granted.grant.guardrail_revision) {
                    (None, _) => "the intent pins no guardrail chain revision while the \
                                  deployment evaluates one; commit intents through the \
                                  chain-pinned policies of AgentToolAuthority::effect_policies"
                        .to_string(),
                    (Some(pinned), Some(current)) => format!(
                        "the intent was committed under guardrail chain revision {pinned} and \
                         the deployment now evaluates revision {current}; the executed payload \
                         could differ across attempts of one generation"
                    ),
                    (Some(pinned), None) => format!(
                        "the intent was committed under guardrail chain revision {pinned} and \
                         the deployment no longer evaluates a chain"
                    ),
                },
            );
            return self
                .settle_refusal(
                    scope,
                    &claim,
                    intent,
                    attempt,
                    &refusal,
                    possibly_executed,
                    pass,
                )
                .await;
        }

        // Applied transforms and report-only findings are recorded, not
        // silent: they surface on the dispatch trace before the invocation.
        for transform in &granted.transforms {
            tracing::info!(
                effect_id = intent.effect_id.as_str(),
                generation = %intent.generation,
                stage = %transform.stage,
                stage_revision = %transform.revision,
                reason_code = %transform.reason_code,
                "guardrail transform applied to the dispatched call"
            );
        }
        for report in &granted.reports {
            tracing::info!(
                effect_id = intent.effect_id.as_str(),
                generation = %intent.generation,
                stage = %report.stage,
                stage_revision = %report.revision,
                reason_code = %report.reason_code,
                evidence = report
                    .evidence
                    .as_ref()
                    .map(|artifact| artifact.artifact_id.as_str()),
                "guardrail report-only finding"
            );
        }

        if !self.survives(AgentDispatchWindow::BeforeStarted) {
            return Ok(ClaimConclusion::Died);
        }

        // Durable `Started`: the outbox row turns `Dispatching` under the
        // claim's lease and fence, before any external call
        // ([specification 11.4]).
        let mut inbox = self.inbox(scope);
        inbox.recover().await?;
        inbox
            .inner_mut()
            .mark_outbox_dispatching(&message_id)
            .await
            .map_err(AgentInboxError::from)?;

        if !self.survives(AgentDispatchWindow::AfterStarted) {
            return Ok(ClaimConclusion::Died);
        }

        // Dispatch-time credential resolution, inside the bounded attempt. The
        // resolved value never outlives `outcome` below.
        let credential = match &intent.credential_binding {
            None => None,
            Some(binding) => match &self.credentials {
                None => {
                    // Fail closed: an intent that names a binding no resolver
                    // can honor is a definitive failure, not a retry loop.
                    let refusal = AgentAuthorityRefusal::of(
                        "credential-resolver-missing",
                        format!("no credential resolver is configured for binding {binding}"),
                    );
                    return self
                        .settle_undispatchable(
                            scope,
                            &claim,
                            intent,
                            attempt,
                            &refusal,
                            possibly_executed,
                            pass,
                        )
                        .await;
                }
                Some(resolver) => match resolver.resolve(scope, binding, intent).await {
                    Ok(credential) => Some(credential),
                    Err(error) => {
                        // Resolution failures may be transient: burn the
                        // attempt under the intent's policy.
                        return self
                            .record_attempt_failure(
                                scope,
                                &claim,
                                intent,
                                attempt,
                                "credential-resolution-failed",
                                &error.to_string(),
                                pass,
                            )
                            .await;
                    }
                },
            },
        };

        pass.invoked += 1;
        let invoked = self
            .invoke(scope, intent, &granted, credential.as_ref())
            .await;
        drop(credential);

        if !self.survives(AgentDispatchWindow::AfterInvocation) {
            return Ok(ClaimConclusion::Died);
        }

        let outcome = match invoked {
            Ok(outcome) => outcome,
            Err(error) => {
                return self
                    .record_attempt_failure(
                        scope,
                        &claim,
                        intent,
                        attempt,
                        error.code(),
                        &error.to_string(),
                        pass,
                    )
                    .await;
            }
        };

        self.deliver_outcome(scope, intent, attempt, claim.fencing_token, outcome, pass)
            .await?;

        if !self.survives(AgentDispatchWindow::AfterResultDelivery) {
            return Ok(ClaimConclusion::Died);
        }

        let mut inbox = self.inbox(scope);
        inbox.recover().await?;
        inbox
            .inner_mut()
            .record_outbox_success(&message_id)
            .await
            .map_err(AgentInboxError::from)?;
        self.fleet.complete_claim(&claim).await?;
        Ok(ClaimConclusion::Settled)
    }

    /// Recovery of an attempt that wrote durable `Started` and disappeared,
    /// per the intent's safety class
    /// ([specification 11.5](../../../docs/plans/rakka-agent/spec.md)).
    #[allow(clippy::too_many_arguments)]
    async fn recover_ambiguous(
        &mut self,
        scope: &AgentRunScope,
        claim: AgentDispatchClaim,
        state: &AgentRunState,
        intent: &AgentRunEffect,
        attempt: u32,
        winding_down: bool,
        pass: &mut AgentDispatchPass,
    ) -> AgentDispatchResult<ClaimConclusion> {
        match intent.safety.class() {
            AgentEffectSafetyClass::ReadOnly => {
                if winding_down {
                    // A read-only outcome cannot matter to a cancelled run:
                    // discarding it abandons nothing consequential.
                    self.settle_ticket_cancelled(scope, &claim, "run-cancelled", pass)
                        .await?;
                    self.deliver_outcome(
                        scope,
                        intent,
                        attempt,
                        claim.fencing_token,
                        AgentRunEffectOutcome::Cancelled {
                            reason: "run-cancelled".to_string(),
                        },
                        pass,
                    )
                    .await?;
                    return Ok(ClaimConclusion::Settled);
                }
                self.retry_ambiguous(scope, claim, state, intent, pass)
                    .await
            }
            AgentEffectSafetyClass::Idempotent => {
                // Retry with the generation's external idempotency key — the
                // key rides the durable intent, so every attempt of this
                // generation hands the target the same one. Under a wind-down
                // the retry still runs: it is the truthful way to learn what
                // the ambiguous attempt did, and the target deduplicates it.
                self.retry_ambiguous(scope, claim, state, intent, pass)
                    .await
            }
            AgentEffectSafetyClass::Reconcileable => {
                self.reconcile_ambiguous(scope, claim, state, intent, attempt, winding_down, pass)
                    .await
            }
            AgentEffectSafetyClass::NonIdempotent => {
                // Exactly one durable Indeterminate, and no automatic
                // re-invocation — cancellation requested or not. Delivery is
                // deduplicated on the derived result operation id and fenced
                // by the run's effect record, so a second recovery pass
                // resolves to the same single outcome.
                self.deliver_outcome(
                    scope,
                    intent,
                    attempt,
                    claim.fencing_token,
                    AgentRunEffectOutcome::Indeterminate {
                        code: "dispatcher-lost-after-started".to_string(),
                        message: "the attempt may have invoked the target; its outcome must be \
                                  reconciled"
                            .to_string(),
                    },
                    pass,
                )
                .await?;
                pass.parked_indeterminate += 1;
                self.settle_ticket_cancelled(scope, &claim, "indeterminate", pass)
                    .await?;
                Ok(ClaimConclusion::Settled)
            }
        }
    }

    /// Burns one attempt for the ambiguous loss, then — if the intent's budget
    /// still permits — re-invokes.
    async fn retry_ambiguous(
        &mut self,
        scope: &AgentRunScope,
        claim: AgentDispatchClaim,
        state: &AgentRunState,
        intent: &AgentRunEffect,
        pass: &mut AgentDispatchPass,
    ) -> AgentDispatchResult<ClaimConclusion> {
        let message_id = OutboxMessageId::new(claim.effect_id.as_str());
        let mut inbox = self.inbox(scope);
        inbox.recover().await?;
        let event = inbox
            .inner_mut()
            .record_outbox_failure(&message_id, "dispatcher lost after started", true)
            .await
            .map_err(AgentInboxError::from)?;
        pass.failed_attempts += 1;

        if let WorkflowTelemetryEvent::OutboxDispatchExhausted { attempts, .. } = &event {
            let attempts = *attempts;
            self.fleet.record_claim_failure(&claim, &event).await?;
            self.deliver_outcome(
                scope,
                intent,
                attempts,
                claim.fencing_token,
                AgentRunEffectOutcome::Exhausted {
                    code: "dispatcher-lost-after-started".to_string(),
                    message: "the retry budget was spent recovering ambiguous attempts".to_string(),
                },
                pass,
            )
            .await?;
            return Ok(ClaimConclusion::Settled);
        }

        // The budget permits another attempt: run it now, under this claim,
        // as a fresh invocation. The attempt number moves past the burned one.
        let attempt = {
            let mut inbox = self.inbox(scope);
            inbox.recover().await?;
            inbox
                .inner()
                .state()
                .map_err(AgentInboxError::from)?
                .outbox_entry(&message_id)
                .map(|row| row.attempts().attempts())
                .unwrap_or(0)
                .saturating_add(1)
        };
        self.attempt_invocation(scope, claim, state, intent, attempt, pass)
            .await
    }

    /// Queries the reconciliation protocol before any retry
    /// ([specification 11.5](../../../docs/plans/rakka-agent/spec.md):
    /// "retry only when proven absent").
    #[allow(clippy::too_many_arguments)]
    async fn reconcile_ambiguous(
        &mut self,
        scope: &AgentRunScope,
        claim: AgentDispatchClaim,
        state: &AgentRunState,
        intent: &AgentRunEffect,
        attempt: u32,
        winding_down: bool,
        pass: &mut AgentDispatchPass,
    ) -> AgentDispatchResult<ClaimConclusion> {
        let Some(protocol) = intent.safety.reconciliation_protocol().cloned() else {
            // The spec validation makes this unreachable; parking is the only
            // honest fallback for a record that somehow lost its protocol.
            return self
                .park_indeterminate(
                    scope,
                    claim,
                    intent,
                    attempt,
                    "reconciliation-protocol-missing",
                    INDETERMINATE_OUTCOME_MESSAGE,
                    pass,
                )
                .await;
        };
        let Some(reconciler) = self.reconciler.clone() else {
            return self
                .park_indeterminate(
                    scope,
                    claim,
                    intent,
                    attempt,
                    "reconciler-missing",
                    INDETERMINATE_OUTCOME_MESSAGE,
                    pass,
                )
                .await;
        };

        let finding = reconciler.reconcile(&protocol, scope, intent).await?;
        match finding {
            AgentReconciliationFinding::Executed { outcome } => {
                // The authoritative outcome, recorded without re-invocation.
                self.deliver_outcome(scope, intent, attempt, claim.fencing_token, *outcome, pass)
                    .await?;
                let message_id = OutboxMessageId::new(claim.effect_id.as_str());
                let mut inbox = self.inbox(scope);
                inbox.recover().await?;
                inbox
                    .inner_mut()
                    .record_outbox_success(&message_id)
                    .await
                    .map_err(AgentInboxError::from)?;
                self.fleet.complete_claim(&claim).await?;
                Ok(ClaimConclusion::Settled)
            }
            AgentReconciliationFinding::NotExecuted => {
                if winding_down {
                    // Proven absent, and the run wants nothing further.
                    self.settle_ticket_cancelled(scope, &claim, "run-cancelled", pass)
                        .await?;
                    self.deliver_outcome(
                        scope,
                        intent,
                        attempt,
                        claim.fencing_token,
                        AgentRunEffectOutcome::Cancelled {
                            reason: "run-cancelled".to_string(),
                        },
                        pass,
                    )
                    .await?;
                    return Ok(ClaimConclusion::Settled);
                }
                // Proven absent: a retry is a fresh invocation, under budget.
                self.retry_ambiguous(scope, claim, state, intent, pass)
                    .await
            }
            AgentReconciliationFinding::Unknown => {
                if winding_down {
                    // A cancelled run cannot wait for a better answer forever;
                    // the outcome is unknown and consequential, so it parks
                    // for the explicit decision (scenario 57).
                    return self
                        .park_indeterminate(
                            scope,
                            claim,
                            intent,
                            attempt,
                            "reconciliation-unknown",
                            INDETERMINATE_OUTCOME_MESSAGE,
                            pass,
                        )
                        .await;
                }
                // Burn an attempt and leave the row retry-scheduled. A
                // `Reconcileable` row re-enters recovery as ambiguous, so the
                // later claim queries the protocol again rather than
                // invoking; a spent budget parks the generation.
                let message_id = OutboxMessageId::new(claim.effect_id.as_str());
                let mut inbox = self.inbox(scope);
                inbox.recover().await?;
                let event = inbox
                    .inner_mut()
                    .record_outbox_failure(&message_id, "reconciliation outcome unknown", true)
                    .await
                    .map_err(AgentInboxError::from)?;
                pass.failed_attempts += 1;
                if matches!(
                    event,
                    WorkflowTelemetryEvent::OutboxDispatchExhausted { .. }
                ) {
                    self.fleet.record_claim_failure(&claim, &event).await?;
                    return self
                        .park_indeterminate(
                            scope,
                            claim,
                            intent,
                            attempt,
                            "reconciliation-exhausted",
                            INDETERMINATE_OUTCOME_MESSAGE,
                            pass,
                        )
                        .await;
                }
                self.fleet.record_claim_failure(&claim, &event).await?;
                Ok(ClaimConclusion::Settled)
            }
        }
    }

    /// Parks one generation as indeterminate and revokes its dispatch
    /// eligibility.
    #[allow(clippy::too_many_arguments)]
    async fn park_indeterminate(
        &mut self,
        scope: &AgentRunScope,
        claim: AgentDispatchClaim,
        intent: &AgentRunEffect,
        attempt: u32,
        code: &str,
        message: &str,
        pass: &mut AgentDispatchPass,
    ) -> AgentDispatchResult<ClaimConclusion> {
        self.deliver_outcome(
            scope,
            intent,
            attempt,
            claim.fencing_token,
            AgentRunEffectOutcome::Indeterminate {
                code: code.to_string(),
                message: message.to_string(),
            },
            pass,
        )
        .await?;
        pass.parked_indeterminate += 1;
        self.settle_ticket_cancelled(scope, &claim, "indeterminate", pass)
            .await?;
        Ok(ClaimConclusion::Settled)
    }

    /// Routes one authority refusal to its truthful settlement: a transient
    /// refusal defers the ticket without spending anything durable; a
    /// definitive one settles the generation per
    /// [`Self::settle_undispatchable`].
    #[allow(clippy::too_many_arguments)]
    async fn settle_refusal(
        &mut self,
        scope: &AgentRunScope,
        claim: &AgentDispatchClaim,
        intent: &AgentRunEffect,
        attempt: u32,
        refusal: &AgentAuthorityRefusal,
        possibly_executed: bool,
        pass: &mut AgentDispatchPass,
    ) -> AgentDispatchResult<ClaimConclusion> {
        if refusal.retryable {
            return self
                .defer_dispatch(claim, attempt, &refusal.code, &refusal.message, pass)
                .await;
        }
        self.settle_undispatchable(
            scope,
            claim,
            intent,
            attempt,
            refusal,
            possibly_executed,
            pass,
        )
        .await
    }

    /// Settles one undispatchable attempt truthfully. When no prior attempt
    /// of the generation can have executed, the generation fails with the
    /// refusal's stable code and the ticket cancels — nothing was invoked.
    /// When a prior attempt *may* have executed — the refusal arrived on the
    /// recovery retry of an ambiguous idempotent loss — the generation parks
    /// `Indeterminate` under the same code instead: the refusal prevented the
    /// truth-finding retry, so "failed, nothing invoked" would erase exactly
    /// the ambiguity [specification 11.5](../../../docs/plans/rakka-agent/spec.md)
    /// preserves, and the explicit reconciliation decision stays owed.
    #[allow(clippy::too_many_arguments)]
    async fn settle_undispatchable(
        &mut self,
        scope: &AgentRunScope,
        claim: &AgentDispatchClaim,
        intent: &AgentRunEffect,
        attempt: u32,
        refusal: &AgentAuthorityRefusal,
        possibly_executed: bool,
        pass: &mut AgentDispatchPass,
    ) -> AgentDispatchResult<ClaimConclusion> {
        if possibly_executed {
            let message = format!(
                "a prior attempt may have invoked the target, and the recovery retry was \
                 refused ({}); an explicit reconciliation decision is owed",
                refusal.message
            );
            return self
                .park_indeterminate(
                    scope,
                    claim.clone(),
                    intent,
                    attempt,
                    &refusal.code,
                    &message,
                    pass,
                )
                .await;
        }
        self.refuse_dispatch(scope, claim, intent, attempt, refusal, pass)
            .await
    }

    /// Defers one claimed ticket without spending anything durable at the
    /// outbox: no attempt is burned — the budget keeps meaning "external
    /// invocation attempts" — and no `Failed` row is written that recovery
    /// could misread as a possibly-executed attempt. Only the fleet entry is
    /// rescheduled, so the ticket is claimable again once the transient
    /// condition may have cleared.
    async fn defer_dispatch(
        &mut self,
        claim: &AgentDispatchClaim,
        attempt: u32,
        code: &str,
        message: &str,
        pass: &mut AgentDispatchPass,
    ) -> AgentDispatchResult<ClaimConclusion> {
        let event = WorkflowTelemetryEvent::OutboxDispatchRetried {
            message_id: OutboxMessageId::new(claim.effect_id.as_str()),
            attempt: attempt.saturating_sub(1),
            next_retry_at: self.clock.now().add_millis(self.retry_backoff_ms),
            message: format!("deferred: {code}: {message}"),
        };
        self.fleet.record_claim_failure(claim, &event).await?;
        pass.deferred += 1;
        Ok(ClaimConclusion::Settled)
    }

    /// Records one failed attempt against the outbox's aligned retry budget,
    /// delivering the generation's `Exhausted` word when the budget is spent.
    #[allow(clippy::too_many_arguments)]
    async fn record_attempt_failure(
        &mut self,
        scope: &AgentRunScope,
        claim: &AgentDispatchClaim,
        intent: &AgentRunEffect,
        attempt: u32,
        code: &str,
        message: &str,
        pass: &mut AgentDispatchPass,
    ) -> AgentDispatchResult<ClaimConclusion> {
        let message_id = OutboxMessageId::new(claim.effect_id.as_str());
        let mut inbox = self.inbox(scope);
        inbox.recover().await?;
        let event = inbox
            .inner_mut()
            .record_outbox_failure(&message_id, format!("{code}: {message}"), false)
            .await
            .map_err(AgentInboxError::from)?;
        pass.failed_attempts += 1;
        self.fleet.record_claim_failure(claim, &event).await?;

        if matches!(
            event,
            WorkflowTelemetryEvent::OutboxDispatchExhausted { .. }
        ) {
            self.deliver_outcome(
                scope,
                intent,
                attempt,
                claim.fencing_token,
                AgentRunEffectOutcome::Exhausted {
                    code: code.to_string(),
                    message: message.to_string(),
                },
                pass,
            )
            .await?;
        }
        Ok(ClaimConclusion::Settled)
    }

    /// Performs the bounded external invocation for one authorized intent.
    ///
    /// The grant is where the turn-bound settings of
    /// [specification 7.2](../../../docs/plans/rakka-agent/spec.md) reach the
    /// model call: the authority resolved the profile and sampling the current
    /// settings revision selects, and the request carries them along with the
    /// revision it validated. A guardrail-transformed tool call executes the
    /// transformed arguments, re-derived identically on every attempt of the
    /// generation — identically because the attempt was refused upstream
    /// unless the current chain revision matches the one the intent pinned at
    /// commit.
    async fn invoke(
        &self,
        scope: &AgentRunScope,
        intent: &AgentRunEffect,
        granted: &AgentGrantedDispatch,
        credential: Option<&AgentEphemeralCredential>,
    ) -> AgentDispatchResult<AgentRunEffectOutcome> {
        match &intent.request {
            AgentRunEffectRequest::Model { context, profile } => {
                let mut request = AgentModelRequest::new(context.clone(), intent.turn)
                    .with_settings_revision(granted.grant.settings_revision);
                if let Some(sampling) = granted.sampling {
                    request = request.with_sampling(sampling);
                }
                let profile = granted.model_profile.clone().or_else(|| profile.clone());
                if let Some(profile) = profile {
                    request = request.with_profile(profile);
                }
                let turn = self.model.call(&request).await.map_err(|error| {
                    AgentDispatchError::Invocation {
                        code: error.code(),
                        message: error.to_string(),
                    }
                })?;
                turn.validate()
                    .map_err(|error| AgentDispatchError::Invocation {
                        code: error.code(),
                        message: error.to_string(),
                    })?;
                Ok(AgentRunEffectOutcome::Model {
                    turn: Box::new(turn),
                })
            }
            AgentRunEffectRequest::Tool { call } => {
                let call: &AgentToolCallRequest = granted.tool_call.as_deref().unwrap_or(call);
                let content = self.tools.execute(scope, intent, call, credential).await?;
                Ok(AgentRunEffectOutcome::Tool {
                    call_id: call.call_id.clone(),
                    content,
                })
            }
            AgentRunEffectRequest::Compensation { compensation, .. } => {
                let Some(executor) = self.compensations.as_ref() else {
                    // Fail closed, definitively: nothing was invoked, an absent
                    // executor will not appear mid-generation, and the run's
                    // wind-down settles truthfully on the failure.
                    return Ok(AgentRunEffectOutcome::Failed {
                        code: "compensation-executor-missing".to_string(),
                        message: "no compensation executor is configured for this dispatcher"
                            .to_string(),
                    });
                };
                let content = executor
                    .execute(scope, intent, compensation, credential)
                    .await?;
                Ok(AgentRunEffectOutcome::Tool {
                    call_id: compensation_call_id(intent),
                    content,
                })
            }
        }
    }

    /// Settles one definitively refused dispatch: the generation fails with
    /// the refusal's stable code, and the ticket is cancelled so it can never
    /// be claimed again. Nothing was invoked.
    async fn refuse_dispatch(
        &mut self,
        scope: &AgentRunScope,
        claim: &AgentDispatchClaim,
        intent: &AgentRunEffect,
        attempt: u32,
        refusal: &AgentAuthorityRefusal,
        pass: &mut AgentDispatchPass,
    ) -> AgentDispatchResult<ClaimConclusion> {
        self.deliver_outcome(
            scope,
            intent,
            attempt,
            claim.fencing_token,
            AgentRunEffectOutcome::Failed {
                code: refusal.code.clone(),
                message: refusal.message.clone(),
            },
            pass,
        )
        .await?;
        self.settle_ticket_cancelled(scope, claim, &refusal.code, pass)
            .await?;
        Ok(ClaimConclusion::Settled)
    }

    /// Delivers one generation-final outcome to the owning run entity.
    ///
    /// Delivery is idempotent end to end: the operation id derives from the
    /// effect and its generation, and the run's own fence refuses what the
    /// log has forgotten. A refusal for an effect the run has already
    /// resolved is convergence, not an error.
    async fn deliver_outcome(
        &mut self,
        scope: &AgentRunScope,
        intent: &AgentRunEffect,
        attempt: u32,
        fence: u64,
        outcome: AgentRunEffectOutcome,
        pass: &mut AgentDispatchPass,
    ) -> AgentDispatchResult<()> {
        let command = AgentRunEntityCommand::RecordEffectResult {
            operation_id: intent.result_operation_id(scope)?,
            effect_id: intent.effect_id.clone(),
            generation: intent.generation,
            attempt,
            fence,
            outcome: Box::new(outcome),
        };
        match self.delivery.deliver(scope, command).await {
            Ok(_reply) => {
                pass.delivered += 1;
                Ok(())
            }
            Err(AgentDispatchError::Run(error))
                if matches!(
                    error.as_ref(),
                    AgentRunError::StaleEffectResult { .. }
                        | AgentRunError::StaleEffectGeneration { .. }
                        | AgentRunError::UnknownEffect { .. }
                        | AgentRunError::Terminal { .. }
                ) =>
            {
                // The run already holds this generation's word — or has moved
                // past it. That is the fence doing its job.
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    /// Settles one claimed ticket as cancelled at both durable layers, so it
    /// can never be dispatched or claimed again.
    async fn settle_ticket_cancelled(
        &mut self,
        scope: &AgentRunScope,
        claim: &AgentDispatchClaim,
        reason: &str,
        pass: &mut AgentDispatchPass,
    ) -> AgentDispatchResult<()> {
        let message_id = OutboxMessageId::new(claim.effect_id.as_str());
        let mut inbox = self.inbox(scope);
        inbox.recover().await?;
        let event = inbox
            .inner_mut()
            .record_outbox_cancelled(&message_id, reason)
            .await
            .map_err(AgentInboxError::from)?
            .unwrap_or_else(|| WorkflowTelemetryEvent::OutboxDispatchCancelled {
                message_id: message_id.clone(),
                at: self.clock.now(),
                message: reason.to_string(),
            });
        self.fleet.record_claim_failure(claim, &event).await?;
        pass.cancelled += 1;
        Ok(())
    }

    /// Fences a winding-down run at the dispatch layer
    /// ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// The ordinary claim path already applies the fence to every registered
    /// ticket — it re-reads the run's wind-down state under its claim, cancels
    /// what provably never started, and recovers an ambiguous attempt per its
    /// safety class, which is what keeps a cancelled run's ambiguous
    /// non-idempotent effect in reconciliation rather than terminally
    /// cancelled (scenario 57). This pass repairs only what the claim path
    /// can never see:
    ///
    /// - a `Ready` effect with **no outbox row** — the flush never landed. A
    ///   tombstone is planted (schedule the ticket idempotently, then cancel
    ///   it) so a laggard flush racing this fence lands on a terminal row
    ///   instead of creating dispatchable work post-fence, and the cancelled
    ///   word is delivered to the run;
    /// - a `Ready` effect whose row is **already cancelled** — an earlier
    ///   fence died between settling the row and delivering the word.
    async fn fence_run(
        &mut self,
        scope: &AgentRunScope,
        state: &AgentRunState,
        pass: &mut AgentDispatchPass,
    ) -> AgentDispatchResult<()> {
        let Some(loop_state) = state.loop_state() else {
            return Ok(());
        };

        for intent in loop_state.ready_effects() {
            let ticket_id = intent.dispatch_ticket_id();
            let message_id = OutboxMessageId::new(ticket_id.as_str());
            let mut inbox = self.inbox(scope);
            inbox.recover().await?;
            let status = inbox
                .inner()
                .state()
                .map_err(AgentInboxError::from)?
                .outbox_entry(&message_id)
                .map(|row| row.status());

            match status {
                None => {
                    let ticket = intent.to_workflow_effect(scope);
                    self.sink()
                        .dispatch(scope, &ticket)
                        .await
                        .map_err(|error| AgentDispatchError::Effect(Box::new(error)))?;
                    let mut inbox = self.inbox(scope);
                    inbox.recover().await?;
                    let _event = inbox
                        .inner_mut()
                        .record_outbox_cancelled(&message_id, "run-cancelled")
                        .await
                        .map_err(AgentInboxError::from)?;
                    pass.cancelled += 1;
                    self.deliver_outcome(
                        scope,
                        &intent,
                        intent.attempts,
                        0,
                        AgentRunEffectOutcome::Cancelled {
                            reason: "run-cancelled".to_string(),
                        },
                        pass,
                    )
                    .await?;
                }
                Some(OutboxStatus::Cancelled) => {
                    self.deliver_outcome(
                        scope,
                        &intent,
                        intent.attempts,
                        0,
                        AgentRunEffectOutcome::Cancelled {
                            reason: "run-cancelled".to_string(),
                        },
                        pass,
                    )
                    .await?;
                }
                Some(_) => {
                    // Registered, claimable, or settling: the claim path owns
                    // it, fence included.
                }
            }
        }
        Ok(())
    }
}

/// Rejection of a dispatch pipeline operation.
#[derive(Debug)]
#[non_exhaustive]
pub enum AgentDispatchError {
    /// An identifier or scope key was malformed.
    Identity(AgentIdentityError),
    /// A persisted record carried an unsupported schema version.
    Schema(AgentSchemaError),
    /// The dispatcher fleet rejected an operation.
    Fleet(AgentDispatcherError),
    /// The run's durable outbox rejected an operation.
    Inbox(AgentInboxError),
    /// An outbox effect could not be scheduled or read.
    Outbox(AgentOutboxError),
    /// The run entity rejected a delivered command.
    Run(Box<AgentRunError>),
    /// An effect record could not be composed or projected.
    Effect(Box<AgentEffectError>),
    /// A dispatch ticket's payload could not be decoded.
    TicketUndecodable {
        /// The decode failure detail.
        message: String,
    },
    /// The bounded external invocation failed.
    Invocation {
        /// Stable machine-readable code.
        code: &'static str,
        /// The failure detail.
        message: String,
    },
    /// A pluggable collaborator — executor, resolver, reconciler, delivery —
    /// failed.
    Collaborator {
        /// Stable machine-readable code.
        code: String,
        /// The failure detail.
        message: String,
    },
}

impl AgentDispatchError {
    /// Stable machine-readable error code.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::Identity(error) => error.code(),
            Self::Schema(error) => error.code(),
            Self::Fleet(error) => error.code(),
            Self::Inbox(error) => error.code(),
            Self::Outbox(error) => error.code(),
            Self::Run(error) => error.code(),
            Self::Effect(error) => error.code(),
            Self::TicketUndecodable { .. } => "dispatch-ticket-undecodable",
            Self::Invocation { code, .. } => code,
            Self::Collaborator { .. } => "dispatch-collaborator-failed",
        }
    }

    /// A collaborator failure with a stable code.
    #[must_use]
    pub fn collaborator(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Collaborator {
            code: code.into(),
            message: message.into(),
        }
    }
}

impl Display for AgentDispatchError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Identity(error) => Display::fmt(error, f),
            Self::Schema(error) => Display::fmt(error, f),
            Self::Fleet(error) => Display::fmt(error, f),
            Self::Inbox(error) => Display::fmt(error, f),
            Self::Outbox(error) => Display::fmt(error, f),
            Self::Run(error) => Display::fmt(error, f),
            Self::Effect(error) => Display::fmt(error, f),
            Self::TicketUndecodable { message } => {
                write!(f, "the dispatch ticket could not be decoded: {message}")
            }
            Self::Invocation { code, message } => {
                write!(f, "the invocation failed ({code}): {message}")
            }
            Self::Collaborator { code, message } => {
                write!(f, "a dispatch collaborator failed ({code}): {message}")
            }
        }
    }
}

impl Error for AgentDispatchError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Identity(error) => Some(error),
            Self::Schema(error) => Some(error),
            Self::Fleet(error) => Some(error),
            Self::Inbox(error) => Some(error),
            Self::Outbox(error) => Some(error),
            Self::Run(error) => Some(error),
            Self::Effect(error) => Some(error),
            _ => None,
        }
    }
}

impl From<AgentIdentityError> for AgentDispatchError {
    fn from(error: AgentIdentityError) -> Self {
        Self::Identity(error)
    }
}

impl From<AgentSchemaError> for AgentDispatchError {
    fn from(error: AgentSchemaError) -> Self {
        Self::Schema(error)
    }
}

impl From<AgentDispatcherError> for AgentDispatchError {
    fn from(error: AgentDispatcherError) -> Self {
        Self::Fleet(error)
    }
}

impl From<AgentInboxError> for AgentDispatchError {
    fn from(error: AgentInboxError) -> Self {
        Self::Inbox(error)
    }
}

impl From<AgentOutboxError> for AgentDispatchError {
    fn from(error: AgentOutboxError) -> Self {
        Self::Outbox(error)
    }
}

impl From<AgentRunError> for AgentDispatchError {
    fn from(error: AgentRunError) -> Self {
        Self::Run(Box::new(error))
    }
}

impl From<AgentEffectError> for AgentDispatchError {
    fn from(error: AgentEffectError) -> Self {
        Self::Effect(Box::new(error))
    }
}
