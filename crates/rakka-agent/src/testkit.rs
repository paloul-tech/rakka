//! Deterministic test support.
//!
//! Owns the deterministic model adapter — scripted text and results, structured
//! task-result proposals, tool and delegation requests, and responses
//! conditional on prior messages or tool results — plus the fake tools, peers,
//! and crash points the recovery scenarios drive. The adapter implements the
//! trait in [`crate::model`] and is available without the `rig` feature, and it
//! exercises the same durable effect path as a production adapter rather than a
//! shortcut around it.
//!
//! Specification: sections 10.4 and 18. The model adapter is filled by slice 1.6
//! and extended with the fault-injection crash points in slice 1.14.
//!
//! Slice 1.3 landed the first part: [`ChoreographyProbe`], a real choreography
//! participant, and [`InProcessExchangeTransport`], which drives exchanges
//! between two participants while injecting the failure windows of
//! [specification 9.8](../../../docs/plans/rakka-agent/spec.md). Together they
//! let the substrate's convergence be tested on its own, before the task and run
//! entities exist, and they are the worked reference those entities follow.
//!
//! Slice 1.5 landed [`InProcessRunEntityTransport`] and [`ScriptedDispatcher`].
//! The dispatcher is the *scripted transition stub* the plan calls for: it plays
//! exactly the role a real dispatcher plays — read the effects a run committed,
//! perform the (here, scripted) bounded I/O, and return the outcome as a durable
//! result command through the entity's command surface
//! ([specification 9.5](../../../docs/plans/rakka-agent/spec.md)) — so the
//! durable loop is driven end to end, and its recovery proven, over the same path
//! production uses.
//!
//! Slice 1.6 landed [`DeterministicModelAdapter`], the deterministic
//! implementation of the Rakka-owned [`AgentModelAdapter`]
//! ([specification 10.4](../../../docs/plans/rakka-agent/spec.md)).
//! [`ScriptedDispatcher`] now produces its model turns *through* an adapter
//! rather than off a private script, so a turn a test scripts travels exactly the
//! path a provider's turn travels; the dispatcher is generic over the adapter, so
//! the Rig-backed adapter of [`crate::rig`] rides the same effect substrate under
//! the `rig` feature.
//!
//! Slice 1.7 landed the real dispatch pipeline in [`crate::dispatch`] — the
//! adapter contract did not change — plus the doubles that drive it here:
//! [`InProcessRunResultDelivery`], [`RecordingToolExecutor`] (whose invocation
//! log is the external system the recovery scenarios assert about),
//! [`ScriptedReconciler`], [`ScriptedCredentialResolver`],
//! [`SharedAtomicWorkflowClock`] for deliberate lease expiry, and
//! [`KillSwitchProbe`], which kills a dispatch worker at an exact durable
//! boundary. [`ScriptedDispatcher`] remains the lightweight in-process driver
//! for tests that exercise the loop rather than the dispatch layer.

use std::collections::{BTreeMap, VecDeque};
use std::fmt::{self, Debug};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use rakka_agent_workflow::{AgentCorrelationId, AgentTimestampMillis};
use rakka_core::MetricsRecorder;
use rakka_persistence::DurableStateStore;
use serde::{Deserialize, Serialize};

use rakka_agent_workflow::AgentEphemeralCredential;

use crate::agent::AgentEntityState;
use crate::choreography::{
    AgentChoreographyError, AgentChoreographyResult, AgentEntityAddress, AgentEntityClass,
    AgentExchangeDeliveryError, AgentExchangeDeliveryFuture, AgentExchangeEnvelope,
    AgentExchangeHost, AgentExchangeJournal, AgentExchangeKind, AgentExchangeParticipant,
    AgentExchangePayload, AgentExchangeReply, AgentExchangeResult, AgentExchangeRouter,
    AgentExchangeState, AgentExchangeTransition, AgentExchangeTransport,
};
use crate::definition::{AgentCredentialBindingRef, AgentModelProfileId, AgentRevisionNumber};
use crate::dispatch::{
    AgentDispatchError, AgentDispatchFuture, AgentDispatchProbe, AgentDispatchToolExecutor,
    AgentDispatchWindow, AgentEffectCredentialResolver, AgentEffectReconciler,
    AgentGoalEvaluationExecutor, AgentGoalEvaluationFinding, AgentMemoryPromotionExecutor,
    AgentMemoryPromotionFinding, AgentReconciliationFinding, AgentRunResultDelivery,
};
use crate::effect::{
    AgentEffectPolicies, AgentMemoryPromotionRequest, AgentReconciliationProtocolRef,
    AgentRunEffect, AgentRunEffectOutcome, AgentRunEffectRequest, AgentRunEffectSink,
    AgentRunEffectStatus,
};
use crate::identity::{AgentOperationId, AgentRunScope, AgentTaskScope};
use crate::loop_runtime::{AgentLoopState, CURRENT_AGENT_LOOP_ADAPTER_VERSION};
use crate::memory::{AgentContextSnapshotRef, AgentRunMemory, MemoryError};
use crate::model::{
    AgentModelAdapter, AgentModelFuture, AgentModelRequest, AgentModelResult,
    AgentModelRetryPolicy, AgentModelTurn, AgentToolCallRequest,
};
use crate::observability::AgentDecisionEventSink;
use crate::run::{
    AgentRunEntityCommand, AgentRunEntityReply, AgentRunEntityStore, AgentRunError, AgentRunState,
};
use crate::schema::{AgentSchemaError, AgentSchemaPolicy};
use crate::task::{
    AgentRunAcceptance, AgentRunAssignment, AgentTaskContent, AgentTaskEntityCommand,
    AgentTaskEntityReply, AgentTaskEntityStore, AgentTaskError, AgentTaskHistoryStore,
    AgentTaskState, AGENT_RUN_ACCEPTANCE_PAYLOAD_TYPE, AGENT_RUN_ASSIGNMENT_PAYLOAD_TYPE,
};
use crate::wake_scanner::{AgentWakeDelivery, AgentWakeDeliveryFuture};
use crate::wake_timers::AgentWakeRewakeParker;

/// Payload type of a [`ProbeCreation`] command.
pub const PROBE_CREATION_TYPE: &str = "rakka.agent.testkit.ProbeCreation";

/// Payload type of a [`ProbeAssignment`] command.
pub const PROBE_ASSIGNMENT_TYPE: &str = "rakka.agent.testkit.ProbeAssignment";

/// Payload type of a [`ProbeProposal`] command.
pub const PROBE_PROPOSAL_TYPE: &str = "rakka.agent.testkit.ProbeProposal";

/// Payload type of a [`ProbeLedgerEntry`] command.
pub const PROBE_LEDGER_TYPE: &str = "rakka.agent.testkit.ProbeLedgerEntry";

/// Payload type of a [`ProbeOutcome`] result.
pub const PROBE_OUTCOME_TYPE: &str = "rakka.agent.testkit.ProbeOutcome";

/// How many decisions a probe remembers.
pub const PROBE_DECISION_CAPACITY: usize = 32;

/// Creation command carried by an [`AgentExchangeKind::Creation`] exchange.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbeCreation {
    /// Bounded label of the created work.
    pub label: String,
}

/// Assignment command carried by an [`AgentExchangeKind::Assignment`] exchange.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbeAssignment {
    /// Monotonic assignment generation. A generation the receiver has already
    /// passed is rejected, which is the domain fence that makes a replay older
    /// than the deduplication window safe.
    pub generation: u64,
}

/// Result proposal carried by an [`AgentExchangeKind::ResultProposal`] exchange.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbeProposal {
    /// Whether the proposal satisfies the receiver's deterministic result rule.
    pub valid: bool,
}

/// Ledger command carried by the budget exchanges.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbeLedgerEntry {
    /// Amount to allocate, settle, or return.
    pub amount: i64,
}

/// Result payload every probe exchange returns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbeOutcome {
    /// Bounded description of what the receiver did.
    pub detail: String,
}

/// One decision a probe settled as an initiator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbeDecision {
    /// Exchange the decision resolved.
    pub kind: AgentExchangeKind,
    /// Whether the receiver accepted the exchange.
    pub accepted: bool,
    /// Rejection code, when the receiver refused.
    pub rejection_code: Option<String>,
}

/// Durable state of one [`ChoreographyProbe`].
///
/// It is deliberately shaped like a real participant's state: a small domain
/// (created, assignment generation, ledger balance) that fences its own
/// transitions, plus the [`AgentExchangeJournal`] the substrate writes in the
/// same compare-and-set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChoreographyProbeState {
    address: AgentEntityAddress,
    created: bool,
    label: Option<String>,
    assignment_generation: u64,
    balance: i64,
    applied: BTreeMap<AgentExchangeKind, u32>,
    settled: BTreeMap<AgentExchangeKind, u32>,
    decisions: Vec<ProbeDecision>,
    journal: AgentExchangeJournal,
}

impl ChoreographyProbeState {
    /// Address of the probe.
    #[must_use]
    pub const fn address(&self) -> &AgentEntityAddress {
        &self.address
    }

    /// Whether a creation exchange has been applied.
    #[must_use]
    pub const fn is_created(&self) -> bool {
        self.created
    }

    /// Label the creation exchange carried.
    #[must_use]
    pub fn label(&self) -> Option<&str> {
        self.label.as_deref()
    }

    /// Highest assignment generation the probe has accepted.
    #[must_use]
    pub const fn assignment_generation(&self) -> u64 {
        self.assignment_generation
    }

    /// Current ledger balance.
    ///
    /// A double-credited allocation or a double-debited settlement shows up here
    /// and nowhere else, which is exactly what scenario 61 is about.
    #[must_use]
    pub const fn balance(&self) -> i64 {
        self.balance
    }

    /// Debits the ledger.
    ///
    /// This is the parent-local escrow debit an allocation applies *inside its
    /// own creating transition*, before the allocation is ever sent
    /// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)). An
    /// entry-point transition that calls it must guard itself with
    /// [`AgentExchangeJournal::has_initiated`], or a replay would debit twice.
    pub fn debit(&mut self, amount: i64) {
        self.balance = self.balance.saturating_sub(amount);
    }

    /// How many exchanges of one kind this probe has applied as a receiver.
    #[must_use]
    pub fn applied_count(&self, kind: AgentExchangeKind) -> u32 {
        self.applied.get(&kind).copied().unwrap_or(0)
    }

    /// How many exchanges of one kind this probe has settled as an initiator.
    #[must_use]
    pub fn settled_count(&self, kind: AgentExchangeKind) -> u32 {
        self.settled.get(&kind).copied().unwrap_or(0)
    }

    /// Every decision this probe settled, oldest first.
    #[must_use]
    pub fn decisions(&self) -> &[ProbeDecision] {
        &self.decisions
    }

    /// The probe's durable saga record.
    #[must_use]
    pub const fn journal(&self) -> &AgentExchangeJournal {
        &self.journal
    }
}

impl AgentExchangeState for ChoreographyProbeState {
    fn exchange_journal(&self) -> &AgentExchangeJournal {
        &self.journal
    }

    fn exchange_journal_mut(&mut self) -> &mut AgentExchangeJournal {
        &mut self.journal
    }

    fn check_schema(&self, _policy: &AgentSchemaPolicy) -> Result<(), AgentSchemaError> {
        // The probe persists no versioned record of its own; the host has
        // already checked the exchange journal. A real participant checks its
        // own definition, settings, and state versions here.
        Ok(())
    }
}

/// A choreography participant that records what it was asked to do.
///
/// Its transitions are fenced on its own durable state — a second creation, or
/// an assignment for a generation it has already passed, is rejected — so a
/// replay that has aged out of the journal's bounded deduplication window is
/// still refused rather than applied twice.
#[derive(Debug, Clone, Copy, Default)]
pub struct ChoreographyProbe;

impl ChoreographyProbe {
    /// Builds the exchange envelope this probe expects for one exchange kind.
    pub fn envelope(
        kind: AgentExchangeKind,
        operation_id: AgentOperationId,
        initiator: AgentEntityAddress,
        target: AgentEntityAddress,
        now: AgentTimestampMillis,
    ) -> AgentChoreographyResult<AgentExchangeEnvelope> {
        Self::envelope_with(
            kind,
            operation_id,
            initiator,
            target,
            now,
            |kind| match kind {
                AgentExchangeKind::Creation => AgentExchangePayload::encode(
                    PROBE_CREATION_TYPE,
                    &ProbeCreation {
                        label: "probe".to_string(),
                    },
                ),
                AgentExchangeKind::Assignment => AgentExchangePayload::encode(
                    PROBE_ASSIGNMENT_TYPE,
                    &ProbeAssignment { generation: 1 },
                ),
                AgentExchangeKind::ResultProposal => AgentExchangePayload::encode(
                    PROBE_PROPOSAL_TYPE,
                    &ProbeProposal { valid: true },
                ),
                _ => AgentExchangePayload::encode(
                    PROBE_LEDGER_TYPE,
                    &ProbeLedgerEntry { amount: 10 },
                ),
            },
        )
    }

    /// Builds an envelope with an explicit payload.
    pub fn envelope_with<F>(
        kind: AgentExchangeKind,
        operation_id: AgentOperationId,
        initiator: AgentEntityAddress,
        target: AgentEntityAddress,
        now: AgentTimestampMillis,
        payload: F,
    ) -> AgentChoreographyResult<AgentExchangeEnvelope>
    where
        F: FnOnce(AgentExchangeKind) -> AgentChoreographyResult<AgentExchangePayload>,
    {
        let correlation_id = AgentCorrelationId::new(operation_id.as_str());
        AgentExchangeEnvelope::new(
            operation_id,
            kind,
            initiator,
            target,
            payload(kind)?,
            correlation_id,
            now,
        )
    }
}

impl AgentExchangeParticipant for ChoreographyProbe {
    type State = ChoreographyProbeState;

    fn initialize(&self, address: &AgentEntityAddress, _now: AgentTimestampMillis) -> Self::State {
        ChoreographyProbeState {
            address: address.clone(),
            created: false,
            label: None,
            assignment_generation: 0,
            balance: 0,
            applied: BTreeMap::new(),
            settled: BTreeMap::new(),
            decisions: Vec::new(),
            journal: AgentExchangeJournal::new(),
        }
    }

    fn apply(
        &self,
        state: &mut Self::State,
        envelope: &AgentExchangeEnvelope,
        _now: AgentTimestampMillis,
    ) -> AgentExchangeTransition {
        let kind = envelope.kind();
        let result = match kind {
            AgentExchangeKind::Creation => apply_creation(state, envelope),
            AgentExchangeKind::Assignment => apply_assignment(state, envelope),
            AgentExchangeKind::ResultProposal => apply_proposal(state, envelope),
            AgentExchangeKind::BudgetAllocation | AgentExchangeKind::BudgetReturn => {
                apply_ledger(state, envelope, 1)
            }
            AgentExchangeKind::BudgetSettlement => apply_ledger(state, envelope, -1),
            // An epoch result, a goal evaluation, a delegation result, or a
            // cancellation request is a durable transition but not a balance
            // movement: the probe records the application without crediting.
            AgentExchangeKind::EpochResult
            | AgentExchangeKind::GoalEvaluation
            | AgentExchangeKind::DelegationResult
            | AgentExchangeKind::RunCancel
            | AgentExchangeKind::DelegationCancel => apply_ledger(state, envelope, 0),
        };

        // Every applied exchange is a transition, whether it accepted or
        // rejected: a rejection is a durable decision, so replaying it must not
        // decide again.
        *state.applied.entry(kind).or_insert(0) += 1;
        AgentExchangeTransition::new(result)
    }

    fn settle(
        &self,
        state: &mut Self::State,
        envelope: &AgentExchangeEnvelope,
        result: &AgentExchangeResult,
        _now: AgentTimestampMillis,
    ) -> Vec<AgentExchangeEnvelope> {
        let kind = envelope.kind();
        *state.settled.entry(kind).or_insert(0) += 1;
        state.decisions.push(ProbeDecision {
            kind,
            accepted: result.is_accepted(),
            rejection_code: result.status().rejection_code().map(ToString::to_string),
        });
        while state.decisions.len() > PROBE_DECISION_CAPACITY {
            state.decisions.remove(0);
        }
        Vec::new()
    }
}

fn apply_creation(
    state: &mut ChoreographyProbeState,
    envelope: &AgentExchangeEnvelope,
) -> AgentExchangeResult {
    let command: ProbeCreation = match envelope.payload().decode(PROBE_CREATION_TYPE) {
        Ok(command) => command,
        Err(error) => return rejected(error.code(), error.to_string()),
    };
    if state.created {
        // The domain fence: a replay that aged out of the journal's window is
        // still refused, because the probe already exists.
        return rejected("already-created", "the probe is already created");
    }
    state.created = true;
    state.label = Some(command.label.clone());
    accepted(&format!("created {}", command.label))
}

fn apply_assignment(
    state: &mut ChoreographyProbeState,
    envelope: &AgentExchangeEnvelope,
) -> AgentExchangeResult {
    let command: ProbeAssignment = match envelope.payload().decode(PROBE_ASSIGNMENT_TYPE) {
        Ok(command) => command,
        Err(error) => return rejected(error.code(), error.to_string()),
    };
    if !state.created {
        return rejected("not-created", "the probe is not created");
    }
    if command.generation <= state.assignment_generation {
        return rejected(
            "stale-generation",
            format!(
                "generation {} is not newer than the accepted generation {}",
                command.generation, state.assignment_generation
            ),
        );
    }
    state.assignment_generation = command.generation;
    accepted(&format!("accepted generation {}", command.generation))
}

fn apply_proposal(
    state: &mut ChoreographyProbeState,
    envelope: &AgentExchangeEnvelope,
) -> AgentExchangeResult {
    let command: ProbeProposal = match envelope.payload().decode(PROBE_PROPOSAL_TYPE) {
        Ok(command) => command,
        Err(error) => return rejected(error.code(), error.to_string()),
    };
    if !state.created {
        return rejected("not-created", "the probe is not created");
    }
    if command.valid {
        accepted("the proposed result satisfies the result rule")
    } else {
        // A deterministic result rule refused the proposal. That is a durable
        // decision the initiator must record, not a failure it may retry.
        rejected(
            "result-rule-violation",
            "the proposed result violates the result rule",
        )
    }
}

fn apply_ledger(
    state: &mut ChoreographyProbeState,
    envelope: &AgentExchangeEnvelope,
    sign: i64,
) -> AgentExchangeResult {
    let command: ProbeLedgerEntry = match envelope.payload().decode(PROBE_LEDGER_TYPE) {
        Ok(command) => command,
        Err(error) => return rejected(error.code(), error.to_string()),
    };
    state.balance = state.balance.saturating_add(sign * command.amount);
    accepted(&format!("balance is now {}", state.balance))
}

fn accepted(detail: &str) -> AgentExchangeResult {
    AgentExchangeResult::accepted(outcome(detail))
}

fn rejected(code: &str, message: impl Into<String>) -> AgentExchangeResult {
    AgentExchangeResult::rejected(code, message, outcome(code))
}

fn outcome(detail: &str) -> AgentExchangePayload {
    AgentExchangePayload::encode(
        PROBE_OUTCOME_TYPE,
        &ProbeOutcome {
            detail: detail.to_string(),
        },
    )
    .unwrap_or_else(|_| AgentExchangePayload::empty(PROBE_OUTCOME_TYPE))
}

/// A failure window to inject into the next delivery.
///
/// These are the windows [specification 9.8](../../../docs/plans/rakka-agent/spec.md)
/// requires every exchange to converge across.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ExchangeFault {
    /// The envelope never reaches the receiver. The initiator cannot tell this
    /// apart from a lost reply, and must not try to.
    LoseEnvelope,
    /// The receiver durably accepts and transitions, and then the reply is lost
    /// — the receiver crashed after acceptance, or the network dropped the
    /// answer. Indistinguishable, from the initiator, from [`Self::LoseEnvelope`].
    LoseReply,
    /// The envelope is delivered twice.
    DeliverTwice,
}

/// Delivers exchanges to a participant hosted over a shared durable store.
///
/// Every delivery builds a *fresh* [`AgentExchangeHost`] and recovers it from
/// the store, so the receiver is re-materialized from durable state alone on
/// each attempt — which is what a shard move or a pod restart looks like from
/// the outside. Nothing in-memory carries over between deliveries.
pub struct InProcessExchangeTransport<P, Store>
where
    P: AgentExchangeParticipant + Clone,
    Store: DurableStateStore<P::State>,
{
    participant: P,
    store: Store,
    clock: Arc<AtomicU64>,
    faults: Arc<Mutex<VecDeque<ExchangeFault>>>,
    deliveries: Arc<AtomicUsize>,
    acceptances: Arc<AtomicUsize>,
}

impl<P, Store> Clone for InProcessExchangeTransport<P, Store>
where
    P: AgentExchangeParticipant + Clone,
    Store: DurableStateStore<P::State>,
{
    fn clone(&self) -> Self {
        Self {
            participant: self.participant.clone(),
            store: self.store.clone(),
            clock: self.clock.clone(),
            faults: self.faults.clone(),
            deliveries: self.deliveries.clone(),
            acceptances: self.acceptances.clone(),
        }
    }
}

impl<P, Store> InProcessExchangeTransport<P, Store>
where
    P: AgentExchangeParticipant + Clone,
    Store: DurableStateStore<P::State>,
{
    /// Creates a transport that delivers to participants in one durable store.
    #[must_use]
    pub fn new(participant: P, store: Store, clock: Arc<AtomicU64>) -> Self {
        Self {
            participant,
            store,
            clock,
            faults: Arc::new(Mutex::new(VecDeque::new())),
            deliveries: Arc::new(AtomicUsize::new(0)),
            acceptances: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Queues a fault to inject into the next delivery.
    pub fn inject(&self, fault: ExchangeFault) {
        self.faults
            .lock()
            .expect("the fault queue should not be poisoned")
            .push_back(fault);
    }

    /// How many deliveries were attempted.
    #[must_use]
    pub fn deliveries(&self) -> usize {
        self.deliveries.load(Ordering::SeqCst)
    }

    /// How many envelopes actually reached a receiver's durable accept path,
    /// including the ones whose reply was then lost.
    #[must_use]
    pub fn acceptances(&self) -> usize {
        self.acceptances.load(Ordering::SeqCst)
    }

    fn take_fault(&self) -> Option<ExchangeFault> {
        self.faults
            .lock()
            .expect("the fault queue should not be poisoned")
            .pop_front()
    }

    fn now(&self) -> AgentTimestampMillis {
        AgentTimestampMillis::new(self.clock.fetch_add(1, Ordering::SeqCst))
    }
}

impl<P, Store> AgentExchangeTransport for InProcessExchangeTransport<P, Store>
where
    P: AgentExchangeParticipant + Clone,
    Store: DurableStateStore<P::State>,
{
    fn deliver<'a>(
        &'a self,
        envelope: &'a AgentExchangeEnvelope,
    ) -> AgentExchangeDeliveryFuture<'a> {
        // The fault is taken before the future is awaited, so no lock is held
        // across a suspension point.
        let fault = self.take_fault();
        self.deliveries.fetch_add(1, Ordering::SeqCst);

        Box::pin(async move {
            if matches!(fault, Some(ExchangeFault::LoseEnvelope)) {
                return Err(AgentExchangeDeliveryError::new(
                    "injected-lost-envelope",
                    "the envelope never reached the receiver",
                ));
            }

            let mut host = AgentExchangeHost::new(
                envelope.target().clone(),
                self.participant.clone(),
                self.store.clone(),
            );
            let now = self.now();
            host.recover(now).await.map_err(delivery_error)?;

            let mut reply = {
                self.acceptances.fetch_add(1, Ordering::SeqCst);
                host.accept(envelope, now).await.map_err(delivery_error)?
            };

            if matches!(fault, Some(ExchangeFault::DeliverTwice)) {
                // The same envelope arrives again at the same durable receiver.
                // The reply the initiator sees is the *second* one, which must
                // carry the same logical result as the first.
                let now = self.now();
                self.acceptances.fetch_add(1, Ordering::SeqCst);
                reply = host.accept(envelope, now).await.map_err(delivery_error)?;
            }

            if matches!(fault, Some(ExchangeFault::LoseReply)) {
                return Err(AgentExchangeDeliveryError::new(
                    "injected-lost-reply",
                    "the receiver accepted the exchange, and its reply was lost",
                ));
            }

            Ok(reply)
        })
    }
}

fn delivery_error(error: AgentChoreographyError) -> AgentExchangeDeliveryError {
    AgentExchangeDeliveryError::new(error.code(), error.to_string())
}

/// A minimal run that answers the assignment exchange and nothing else.
///
/// It receives the task entity's real [`AgentRunAssignment`] command and answers
/// with a real [`AgentRunAcceptance`]. Slice 1.5 landed the run entity itself, and
/// this probe stayed, because the two prove different things: the task's tests use
/// it to hold the *other* side of the assignment exchange still — a run that always
/// accepts, or always refuses, with no loop and no proposal of its own — so that a
/// failure in a task test is a failure in the task.
///
/// A test that wants the real thing on both sides uses
/// [`InProcessRunEntityTransport`], which delivers to an [`AgentRunEntityStore`].
#[derive(Debug, Clone, Copy)]
pub struct RunAcceptanceProbe {
    accepts: bool,
}

impl RunAcceptanceProbe {
    /// A run that durably accepts every assignment.
    #[must_use]
    pub const fn accepting() -> Self {
        Self { accepts: true }
    }

    /// A run that refuses every assignment, retiring its generation.
    #[must_use]
    pub const fn refusing() -> Self {
        Self { accepts: false }
    }
}

impl Default for RunAcceptanceProbe {
    fn default() -> Self {
        Self::accepting()
    }
}

/// The durable state of one [`RunAcceptanceProbe`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunAcceptanceProbeState {
    address: AgentEntityAddress,
    accepted_generations: Vec<u64>,
    decisions: Vec<ProbeDecision>,
    journal: AgentExchangeJournal,
}

impl RunAcceptanceProbeState {
    /// Every assignment generation this run durably accepted, in order.
    ///
    /// A replayed assignment must not add a second entry here: that is what "one
    /// run per assignment generation" means in durable terms.
    #[must_use]
    pub fn accepted_generations(&self) -> &[u64] {
        &self.accepted_generations
    }

    /// Every decision this run settled as an initiator, oldest first.
    #[must_use]
    pub fn decisions(&self) -> &[ProbeDecision] {
        &self.decisions
    }

    /// The run's durable saga record.
    #[must_use]
    pub const fn journal(&self) -> &AgentExchangeJournal {
        &self.journal
    }
}

impl AgentExchangeState for RunAcceptanceProbeState {
    fn exchange_journal(&self) -> &AgentExchangeJournal {
        &self.journal
    }

    fn exchange_journal_mut(&mut self) -> &mut AgentExchangeJournal {
        &mut self.journal
    }

    fn check_schema(&self, _policy: &AgentSchemaPolicy) -> Result<(), AgentSchemaError> {
        Ok(())
    }
}

impl AgentExchangeParticipant for RunAcceptanceProbe {
    type State = RunAcceptanceProbeState;

    fn initialize(&self, address: &AgentEntityAddress, _now: AgentTimestampMillis) -> Self::State {
        RunAcceptanceProbeState {
            address: address.clone(),
            accepted_generations: Vec::new(),
            decisions: Vec::new(),
            journal: AgentExchangeJournal::new(),
        }
    }

    fn apply(
        &self,
        state: &mut Self::State,
        envelope: &AgentExchangeEnvelope,
        now: AgentTimestampMillis,
    ) -> AgentExchangeTransition {
        if envelope.kind() == AgentExchangeKind::RunCancel {
            // The probe models a run with nothing outstanding: the request
            // is durably recorded and the wind-down is instantly terminal.
            let receipt = crate::task::AgentRunCancelReceipt {
                run: match &state.address {
                    AgentEntityAddress::Run(scope) => scope.clone(),
                    other => AgentRunScope::new(
                        other.tenant().clone(),
                        crate::AgentId::new("probe").expect("the literal is a valid agent id"),
                        crate::AgentRunId::new("probe").expect("the literal is a valid run id"),
                    )
                    .expect("the probe scope is well formed"),
                },
                status: crate::run::AgentRunStatus::Cancelled,
            };
            return AgentExchangeTransition::new(AgentExchangeResult::accepted(
                AgentExchangePayload::encode(
                    crate::task::AGENT_RUN_CANCEL_RECEIPT_PAYLOAD_TYPE,
                    &receipt,
                )
                .unwrap_or_else(|_| {
                    AgentExchangePayload::empty(crate::task::AGENT_RUN_CANCEL_RECEIPT_PAYLOAD_TYPE)
                }),
            ));
        }
        if envelope.kind() != AgentExchangeKind::Assignment {
            return AgentExchangeTransition::new(AgentExchangeResult::rejected(
                "unsupported-exchange",
                format!("a run does not receive a {} exchange", envelope.kind()),
                AgentExchangePayload::empty(AGENT_RUN_ACCEPTANCE_PAYLOAD_TYPE),
            ));
        }

        let assignment: AgentRunAssignment =
            match envelope.payload().decode(AGENT_RUN_ASSIGNMENT_PAYLOAD_TYPE) {
                Ok(assignment) => assignment,
                Err(error) => {
                    return AgentExchangeTransition::new(AgentExchangeResult::rejected(
                        error.code(),
                        error.to_string(),
                        AgentExchangePayload::empty(AGENT_RUN_ACCEPTANCE_PAYLOAD_TYPE),
                    ))
                }
            };

        if !self.accepts {
            return AgentExchangeTransition::new(AgentExchangeResult::rejected(
                "run-refused-assignment",
                "the run refused its assignment",
                AgentExchangePayload::empty(AGENT_RUN_ACCEPTANCE_PAYLOAD_TYPE),
            ));
        }

        state.accepted_generations.push(assignment.generation.get());
        let acceptance = AgentRunAcceptance {
            run: assignment.run.clone(),
            generation: assignment.generation,
            accepted_at: now,
        };
        let payload = AgentExchangePayload::encode(AGENT_RUN_ACCEPTANCE_PAYLOAD_TYPE, &acceptance)
            .unwrap_or_else(|_| AgentExchangePayload::empty(AGENT_RUN_ACCEPTANCE_PAYLOAD_TYPE));
        AgentExchangeTransition::new(AgentExchangeResult::accepted(payload))
    }

    fn settle(
        &self,
        state: &mut Self::State,
        envelope: &AgentExchangeEnvelope,
        result: &AgentExchangeResult,
        _now: AgentTimestampMillis,
    ) -> Vec<AgentExchangeEnvelope> {
        state.decisions.push(ProbeDecision {
            kind: envelope.kind(),
            accepted: result.is_accepted(),
            rejection_code: result.status().rejection_code().map(ToString::to_string),
        });
        while state.decisions.len() > PROBE_DECISION_CAPACITY {
            state.decisions.remove(0);
        }
        Vec::new()
    }
}

/// Delivers exchanges to a real [`AgentTaskEntityStore`] over a shared durable
/// store.
///
/// Every delivery re-materializes the entity from durable state alone, which is
/// what a shard move or a pod restart looks like from the outside, and it goes
/// through the entity's full path — accept, decide, flush, drive — rather than
/// poking the choreography host underneath it.
pub struct InProcessTaskEntityTransport<Store, Agents, History>
where
    Store: DurableStateStore<AgentTaskState>,
    Agents: DurableStateStore<AgentEntityState>,
    History: AgentTaskHistoryStore,
{
    store: Store,
    agents: Agents,
    history: History,
    router: AgentExchangeRouter,
    clock: Arc<AtomicU64>,
    faults: Arc<Mutex<VecDeque<ExchangeFault>>>,
    acceptances: Arc<AtomicUsize>,
}

impl<Store, Agents, History> Clone for InProcessTaskEntityTransport<Store, Agents, History>
where
    Store: DurableStateStore<AgentTaskState>,
    Agents: DurableStateStore<AgentEntityState>,
    History: AgentTaskHistoryStore,
{
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            agents: self.agents.clone(),
            history: self.history.clone(),
            router: self.router.clone(),
            clock: self.clock.clone(),
            faults: self.faults.clone(),
            acceptances: self.acceptances.clone(),
        }
    }
}

impl<Store, Agents, History> InProcessTaskEntityTransport<Store, Agents, History>
where
    Store: DurableStateStore<AgentTaskState>,
    Agents: DurableStateStore<AgentEntityState>,
    History: AgentTaskHistoryStore,
{
    /// Creates a transport that delivers to task entities in one durable store.
    #[must_use]
    pub fn new(
        store: Store,
        agents: Agents,
        history: History,
        router: AgentExchangeRouter,
        clock: Arc<AtomicU64>,
    ) -> Self {
        Self {
            store,
            agents,
            history,
            router,
            clock,
            faults: Arc::new(Mutex::new(VecDeque::new())),
            acceptances: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Queues a fault to inject into the next delivery.
    pub fn inject(&self, fault: ExchangeFault) {
        self.faults
            .lock()
            .expect("the fault queue should not be poisoned")
            .push_back(fault);
    }

    /// How many envelopes reached a task entity's durable accept path, including
    /// the ones whose reply was then lost.
    #[must_use]
    pub fn acceptances(&self) -> usize {
        self.acceptances.load(Ordering::SeqCst)
    }

    fn take_fault(&self) -> Option<ExchangeFault> {
        self.faults
            .lock()
            .expect("the fault queue should not be poisoned")
            .pop_front()
    }

    fn now(&self) -> AgentTimestampMillis {
        AgentTimestampMillis::new(self.clock.fetch_add(1, Ordering::SeqCst))
    }
}

impl<Store, Agents, History> AgentExchangeTransport
    for InProcessTaskEntityTransport<Store, Agents, History>
where
    Store: DurableStateStore<AgentTaskState>,
    Agents: DurableStateStore<AgentEntityState>,
    History: AgentTaskHistoryStore,
{
    fn deliver<'a>(
        &'a self,
        envelope: &'a AgentExchangeEnvelope,
    ) -> AgentExchangeDeliveryFuture<'a> {
        let fault = self.take_fault();

        Box::pin(async move {
            if matches!(fault, Some(ExchangeFault::LoseEnvelope)) {
                return Err(AgentExchangeDeliveryError::new(
                    "injected-lost-envelope",
                    "the envelope never reached the task entity",
                ));
            }

            let AgentEntityAddress::Task(scope) = envelope.target().clone() else {
                return Err(AgentExchangeDeliveryError::new(
                    "exchange-no-route",
                    "this transport serves task entities only",
                ));
            };

            let mut entity = AgentTaskEntityStore::new(
                scope,
                self.store.clone(),
                self.agents.clone(),
                self.history.clone(),
            );

            self.acceptances.fetch_add(1, Ordering::SeqCst);
            let now = self.now();
            let mut reply = entity
                .accept(envelope, &self.router, now)
                .await
                .map_err(task_delivery_error)?;

            if matches!(fault, Some(ExchangeFault::DeliverTwice)) {
                // The same envelope arrives again at the same durable receiver.
                // The reply the initiator sees is the second one, which must carry
                // the same logical result as the first.
                self.acceptances.fetch_add(1, Ordering::SeqCst);
                let now = self.now();
                reply = entity
                    .accept(envelope, &self.router, now)
                    .await
                    .map_err(task_delivery_error)?;
            }

            if matches!(fault, Some(ExchangeFault::LoseReply)) {
                return Err(AgentExchangeDeliveryError::new(
                    "injected-lost-reply",
                    "the task accepted the exchange, and its reply was lost",
                ));
            }

            Ok(reply)
        })
    }
}

fn task_delivery_error(error: AgentTaskError) -> AgentExchangeDeliveryError {
    AgentExchangeDeliveryError::new(error.code(), error.to_string())
}

/// Delivers wake admission commands to a real [`AgentTaskEntityStore`] over a
/// shared durable store.
///
/// Every delivery re-materializes the entity from durable state alone — which
/// is what a scanner delivering to a passivated controller looks like from the
/// outside. The same [`ExchangeFault`] queue the exchange transports use
/// injects the wake failure windows: a command lost before the entity, a
/// duplicate delivery of the same derived operation id, and a reply lost after
/// the controller dispositioned the wake — the window that leaves a timer
/// entry pending for the next pass to redeliver.
pub struct InProcessWakeDelivery<Store, Agents, History>
where
    Store: DurableStateStore<AgentTaskState>,
    Agents: DurableStateStore<AgentEntityState>,
    History: AgentTaskHistoryStore,
{
    store: Store,
    agents: Agents,
    history: History,
    router: AgentExchangeRouter,
    clock: Arc<AtomicU64>,
    faults: Arc<Mutex<VecDeque<ExchangeFault>>>,
    deliveries: Arc<AtomicUsize>,
    rewake_parker: Option<Arc<dyn AgentWakeRewakeParker>>,
}

impl<Store, Agents, History> Clone for InProcessWakeDelivery<Store, Agents, History>
where
    Store: DurableStateStore<AgentTaskState>,
    Agents: DurableStateStore<AgentEntityState>,
    History: AgentTaskHistoryStore,
{
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            agents: self.agents.clone(),
            history: self.history.clone(),
            router: self.router.clone(),
            clock: self.clock.clone(),
            faults: self.faults.clone(),
            deliveries: self.deliveries.clone(),
            rewake_parker: self.rewake_parker.clone(),
        }
    }
}

impl<Store, Agents, History> InProcessWakeDelivery<Store, Agents, History>
where
    Store: DurableStateStore<AgentTaskState>,
    Agents: DurableStateStore<AgentEntityState>,
    History: AgentTaskHistoryStore,
{
    /// Creates a delivery into task entities over one durable store.
    #[must_use]
    pub fn new(
        store: Store,
        agents: Agents,
        history: History,
        router: AgentExchangeRouter,
        clock: Arc<AtomicU64>,
    ) -> Self {
        Self {
            store,
            agents,
            history,
            router,
            clock,
            faults: Arc::new(Mutex::new(VecDeque::new())),
            deliveries: Arc::new(AtomicUsize::new(0)),
            rewake_parker: None,
        }
    }

    /// Wires the wake-timer parker the delivered entities' settle passes park
    /// controller-originated re-wakes through.
    #[must_use]
    pub fn with_wake_timers(mut self, parker: Arc<dyn AgentWakeRewakeParker>) -> Self {
        self.rewake_parker = Some(parker);
        self
    }

    /// Queues a fault to inject into the next delivery.
    pub fn inject(&self, fault: ExchangeFault) {
        self.faults
            .lock()
            .expect("the fault queue should not be poisoned")
            .push_back(fault);
    }

    /// How many commands reached a task entity's durable apply path, including
    /// the ones whose reply was then lost.
    #[must_use]
    pub fn deliveries(&self) -> usize {
        self.deliveries.load(Ordering::SeqCst)
    }

    fn take_fault(&self) -> Option<ExchangeFault> {
        self.faults
            .lock()
            .expect("the fault queue should not be poisoned")
            .pop_front()
    }

    fn now(&self) -> AgentTimestampMillis {
        AgentTimestampMillis::new(self.clock.fetch_add(1, Ordering::SeqCst))
    }

    async fn apply_once(
        &self,
        scope: &AgentTaskScope,
        command: AgentTaskEntityCommand,
    ) -> AgentTaskEntityReply {
        let mut entity = AgentTaskEntityStore::new(
            scope.clone(),
            self.store.clone(),
            self.agents.clone(),
            self.history.clone(),
        );
        if let Some(parker) = self.rewake_parker.clone() {
            entity = entity.with_wake_timers(parker);
        }
        self.deliveries.fetch_add(1, Ordering::SeqCst);
        let now = self.now();
        match entity.apply(command, &self.router, now).await {
            Ok(reply) => reply,
            // The entity actor answers a domain refusal as a rejection reply,
            // so the delivery does too: a scanner must see the same protocol
            // either way.
            Err(error) => AgentTaskEntityReply::Rejected {
                code: error.code().to_string(),
                message: error.to_string(),
            },
        }
    }
}

impl<Store, Agents, History> AgentWakeDelivery for InProcessWakeDelivery<Store, Agents, History>
where
    Store: DurableStateStore<AgentTaskState>,
    Agents: DurableStateStore<AgentEntityState>,
    History: AgentTaskHistoryStore,
{
    fn deliver<'a>(
        &'a self,
        scope: &'a AgentTaskScope,
        command: AgentTaskEntityCommand,
    ) -> AgentWakeDeliveryFuture<'a> {
        let fault = self.take_fault();

        Box::pin(async move {
            if matches!(fault, Some(ExchangeFault::LoseEnvelope)) {
                return Err(AgentExchangeDeliveryError::new(
                    "injected-lost-command",
                    "the command never reached the task entity",
                ));
            }

            let mut reply = self.apply_once(scope, command.clone()).await;

            if matches!(fault, Some(ExchangeFault::DeliverTwice)) {
                // The same command arrives again at the same durable receiver.
                // The reply the scanner sees is the second one, which must
                // carry the same logical result as the first.
                reply = self.apply_once(scope, command).await;
            }

            if matches!(fault, Some(ExchangeFault::LoseReply)) {
                return Err(AgentExchangeDeliveryError::new(
                    "injected-lost-reply",
                    "the controller dispositioned the wake, and its reply was lost",
                ));
            }

            Ok(reply)
        })
    }
}

fn run_delivery_error(error: AgentRunError) -> AgentExchangeDeliveryError {
    AgentExchangeDeliveryError::new(error.code(), error.to_string())
}

/// Delivers exchanges to a real [`AgentRunEntityStore`] over a shared durable
/// store.
///
/// Every delivery re-materializes the entity from durable state alone — which is
/// what a shard move or a pod restart looks like from the outside — and goes
/// through the entity's full path: accept, crank the loop, dispatch, drive.
pub struct InProcessRunEntityTransport<Store, Effects>
where
    Store: DurableStateStore<AgentRunState>,
    Effects: AgentRunEffectSink,
{
    store: Store,
    effects: Effects,
    router: AgentExchangeRouter,
    clock: Arc<AtomicU64>,
    policies: AgentEffectPolicies,
    memory: Arc<Mutex<Option<AgentRunMemory>>>,
    decisions: Arc<Mutex<Option<Arc<dyn AgentDecisionEventSink>>>>,
    metrics: Arc<Mutex<Option<Arc<dyn MetricsRecorder>>>>,
    delegation: Arc<Mutex<Option<crate::delegation::AgentRunDelegationConfig>>>,
    workflow_tools: Arc<Mutex<Option<crate::workflow_tool::AgentRunWorkflowConfig>>>,
    faults: Arc<Mutex<VecDeque<ExchangeFault>>>,
    acceptances: Arc<AtomicUsize>,
}

impl<Store, Effects> Clone for InProcessRunEntityTransport<Store, Effects>
where
    Store: DurableStateStore<AgentRunState>,
    Effects: AgentRunEffectSink,
{
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            effects: self.effects.clone(),
            router: self.router.clone(),
            clock: self.clock.clone(),
            policies: self.policies.clone(),
            memory: self.memory.clone(),
            decisions: self.decisions.clone(),
            metrics: self.metrics.clone(),
            delegation: self.delegation.clone(),
            workflow_tools: self.workflow_tools.clone(),
            faults: self.faults.clone(),
            acceptances: self.acceptances.clone(),
        }
    }
}

impl<Store, Effects> InProcessRunEntityTransport<Store, Effects>
where
    Store: DurableStateStore<AgentRunState>,
    Effects: AgentRunEffectSink,
{
    /// Creates a transport that delivers to run entities in one durable store.
    #[must_use]
    pub fn new(
        store: Store,
        effects: Effects,
        router: AgentExchangeRouter,
        clock: Arc<AtomicU64>,
    ) -> Self {
        Self {
            store,
            effects,
            router,
            clock,
            policies: AgentEffectPolicies::default(),
            memory: Arc::new(Mutex::new(None)),
            decisions: Arc::new(Mutex::new(None)),
            metrics: Arc::new(Mutex::new(None)),
            delegation: Arc::new(Mutex::new(None)),
            workflow_tools: Arc::new(Mutex::new(None)),
            faults: Arc::new(Mutex::new(VecDeque::new())),
            acceptances: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Wires every run entity this transport builds with a session-memory
    /// backend.
    ///
    /// The slot is shared across clones — including a clone already installed in
    /// a router — so a test can wire memory after the router is assembled, the
    /// way the deferred router late-binds. Every driver of a run must share one
    /// wiring: an entity that advances the loop unwired records nothing to
    /// session memory for the transitions it commits.
    pub fn install_memory(&self, memory: AgentRunMemory) {
        *self
            .memory
            .lock()
            .expect("the memory slot should not be poisoned") = Some(memory);
    }

    /// Wires every run entity this transport builds with a decision-event
    /// sink, under the same shared-slot rule as [`Self::install_memory`]:
    /// every driver of a run must share one wiring, because an entity that
    /// advances the loop unwired records no decisions for the transitions it
    /// commits.
    pub fn install_decisions(&self, sink: Arc<dyn AgentDecisionEventSink>) {
        *self
            .decisions
            .lock()
            .expect("the decision slot should not be poisoned") = Some(sink);
    }

    /// Wires every run entity this transport builds with a metrics recorder,
    /// under the same shared-slot rule as [`Self::install_memory`].
    pub fn install_metrics(&self, metrics: Arc<dyn MetricsRecorder>) {
        *self
            .metrics
            .lock()
            .expect("the metrics slot should not be poisoned") = Some(metrics);
    }

    /// Wires every run entity this transport builds to serve delegation,
    /// under the same shared-slot rule as [`Self::install_memory`]: every
    /// driver of a run must share one wiring, because an entity that
    /// advances the loop unwired refuses the coordination tool.
    pub fn install_delegation(&self, config: crate::delegation::AgentRunDelegationConfig) {
        *self
            .delegation
            .lock()
            .expect("the delegation slot should not be poisoned") = Some(config);
    }

    /// Wires every run entity this transport builds to serve workflow tools,
    /// under the same shared-slot rule as [`Self::install_memory`].
    pub fn install_workflow_tools(&self, config: crate::workflow_tool::AgentRunWorkflowConfig) {
        *self
            .workflow_tools
            .lock()
            .expect("the workflow-tool slot should not be poisoned") = Some(config);
    }

    /// Uses explicit effect specs for the effects hosted runs commit.
    ///
    /// The transport settles the entities it delivers to, and a settle pass is
    /// what commits the loop's effects — so the policies a test configures must
    /// reach it here as well as on the entities the test drives directly.
    #[must_use]
    pub fn with_effect_policies(mut self, policies: AgentEffectPolicies) -> Self {
        self.policies = policies;
        self
    }

    /// Queues a fault to inject into the next delivery.
    pub fn inject(&self, fault: ExchangeFault) {
        self.faults
            .lock()
            .expect("the fault queue should not be poisoned")
            .push_back(fault);
    }

    /// How many envelopes reached a run entity's durable accept path, including
    /// the ones whose reply was then lost.
    #[must_use]
    pub fn acceptances(&self) -> usize {
        self.acceptances.load(Ordering::SeqCst)
    }

    fn take_fault(&self) -> Option<ExchangeFault> {
        self.faults
            .lock()
            .expect("the fault queue should not be poisoned")
            .pop_front()
    }

    fn now(&self) -> AgentTimestampMillis {
        AgentTimestampMillis::new(self.clock.fetch_add(1, Ordering::SeqCst))
    }
}

impl<Store, Effects> AgentExchangeTransport for InProcessRunEntityTransport<Store, Effects>
where
    Store: DurableStateStore<AgentRunState>,
    Effects: AgentRunEffectSink,
{
    fn deliver<'a>(
        &'a self,
        envelope: &'a AgentExchangeEnvelope,
    ) -> AgentExchangeDeliveryFuture<'a> {
        let fault = self.take_fault();

        Box::pin(async move {
            if matches!(fault, Some(ExchangeFault::LoseEnvelope)) {
                return Err(AgentExchangeDeliveryError::new(
                    "injected-lost-envelope",
                    "the envelope never reached the run entity",
                ));
            }

            let AgentEntityAddress::Run(scope) = envelope.target().clone() else {
                return Err(AgentExchangeDeliveryError::new(
                    "exchange-no-route",
                    "this transport serves run entities only",
                ));
            };

            let mut entity =
                AgentRunEntityStore::new(scope, self.store.clone(), self.effects.clone())
                    .with_effect_policies(self.policies.clone());
            let memory = self
                .memory
                .lock()
                .expect("the memory slot should not be poisoned")
                .clone();
            if let Some(memory) = memory {
                entity = entity.with_memory(memory);
            }
            let decisions = self
                .decisions
                .lock()
                .expect("the decision slot should not be poisoned")
                .clone();
            if let Some(decisions) = decisions {
                entity = entity.with_decision_events(decisions);
            }
            let metrics = self
                .metrics
                .lock()
                .expect("the metrics slot should not be poisoned")
                .clone();
            if let Some(metrics) = metrics {
                entity = entity.with_metrics(metrics);
            }
            let delegation = self
                .delegation
                .lock()
                .expect("the delegation slot should not be poisoned")
                .clone();
            if let Some(delegation) = delegation {
                entity = entity.with_delegation(delegation);
            }
            let workflow_tools = self
                .workflow_tools
                .lock()
                .expect("the workflow-tool slot should not be poisoned")
                .clone();
            if let Some(workflow_tools) = workflow_tools {
                entity = entity.with_workflow_tools(workflow_tools);
            }

            self.acceptances.fetch_add(1, Ordering::SeqCst);
            let now = self.now();
            let mut reply = entity
                .accept(envelope, &self.router, now)
                .await
                .map_err(run_delivery_error)?;

            if matches!(fault, Some(ExchangeFault::DeliverTwice)) {
                // The same envelope arrives again at the same durable receiver.
                // The reply the initiator sees is the second one, which must carry
                // the same logical result as the first.
                self.acceptances.fetch_add(1, Ordering::SeqCst);
                let now = self.now();
                reply = entity
                    .accept(envelope, &self.router, now)
                    .await
                    .map_err(run_delivery_error)?;
            }

            if matches!(fault, Some(ExchangeFault::LoseReply)) {
                return Err(AgentExchangeDeliveryError::new(
                    "injected-lost-reply",
                    "the run accepted the exchange, and its reply was lost",
                ));
            }

            Ok(reply)
        })
    }
}

/// The deterministic model adapter of
/// [specification 10.4](../../../docs/plans/rakka-agent/spec.md).
///
/// It implements the Rakka-owned [`AgentModelAdapter`] without any provider and
/// without the `rig` feature. A model call returns, in order of preference: a
/// turn scripted for that turn *number* (the interim form of a response
/// conditional on prior progress), then the next turn in the ordered script,
/// then — when the script is spent — an empty turn that proposes nothing and asks
/// for nothing, which lets a test drive a run to its iteration ceiling without
/// scripting every turn.
///
/// It produces turns synchronously, because a script has no I/O to await; the
/// trait's [`call`](AgentModelAdapter::call) defers that production to the
/// future's first poll, so a future built and dropped unpolled consumes nothing
/// — exactly as a provider-backed call performs no I/O for a future never
/// polled. The adapter is *not* itself idempotent — each call consumes the next
/// scripted turn — because idempotency is the effect's job: the run's effect id
/// and the dispatcher's per-effect memoization are what make a re-invoked call
/// return the answer it first gave ([specification 11.4]).
#[derive(Debug, Clone)]
pub struct DeterministicModelAdapter {
    adapter_version: AgentRevisionNumber,
    retry_policy: AgentModelRetryPolicy,
    turns: Arc<Mutex<VecDeque<AgentModelTurn>>>,
    by_turn: Arc<Mutex<BTreeMap<u64, AgentModelTurn>>>,
    calls: Arc<AtomicUsize>,
}

impl DeterministicModelAdapter {
    /// An adapter with an empty script and the default retry policy.
    #[must_use]
    pub fn new() -> Self {
        Self {
            adapter_version: CURRENT_AGENT_LOOP_ADAPTER_VERSION,
            retry_policy: AgentModelRetryPolicy::DEFAULT,
            turns: Arc::new(Mutex::new(VecDeque::new())),
            by_turn: Arc::new(Mutex::new(BTreeMap::new())),
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Scripts the turn the next unconditioned model call returns.
    #[must_use]
    pub fn with_turn(self, turn: AgentModelTurn) -> Self {
        self.script_turn(turn);
        self
    }

    /// Scripts the turn a model call for a specific turn *number* returns,
    /// regardless of order.
    ///
    /// This is the interim form of a response conditional on prior messages or
    /// tool results ([specification 10.4]): until slice 1.11 gives the adapter
    /// the context snapshot's content, the turn number is the only durable signal
    /// of prior progress it can read.
    #[must_use]
    pub fn with_turn_for(self, turn: u64, model_turn: AgentModelTurn) -> Self {
        self.by_turn
            .lock()
            .expect("the conditional turn script should not be poisoned")
            .insert(turn, model_turn);
        self
    }

    /// Declares the retry policy this adapter's model calls dispatch under,
    /// refusing one the crash-and-timeout rules could not honor
    /// ([specification 11.4](../../../docs/plans/rakka-agent/spec.md)).
    pub fn with_retry_policy(mut self, policy: AgentModelRetryPolicy) -> AgentModelResult<Self> {
        policy.validate()?;
        self.retry_policy = policy;
        Ok(self)
    }

    /// How many model turns the adapter has produced.
    ///
    /// Unlike [`ScriptedDispatcher::model_calls`], this counts *productions*, not
    /// billed calls: a re-invoked effect whose answer the dispatcher memoized does
    /// not reach the adapter a second time.
    #[must_use]
    pub fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn script_turn(&self, turn: AgentModelTurn) {
        self.turns
            .lock()
            .expect("the turn script should not be poisoned")
            .push_back(turn);
    }

    /// Produces the turn one model request resolves to, synchronously.
    #[must_use]
    pub fn produce(&self, request: &AgentModelRequest) -> AgentModelTurn {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if let Some(turn) = self
            .by_turn
            .lock()
            .expect("the conditional turn script should not be poisoned")
            .get(&request.turn)
        {
            return turn.clone();
        }
        self.turns
            .lock()
            .expect("the turn script should not be poisoned")
            .pop_front()
            .unwrap_or_else(|| AgentModelTurn::new(self.adapter_version))
    }
}

impl Default for DeterministicModelAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl AgentModelAdapter for DeterministicModelAdapter {
    fn adapter_version(&self) -> AgentRevisionNumber {
        self.adapter_version
    }

    fn retry_policy(&self) -> AgentModelRetryPolicy {
        self.retry_policy
    }

    fn call<'a>(&'a self, request: &'a AgentModelRequest) -> AgentModelFuture<'a> {
        // Production happens when the future is polled, not when it is built:
        // a caller that constructs and drops the future unpolled — a timeout, a
        // cancelled race — must not consume a scripted turn, exactly as the
        // Rig-backed adapter performs no provider call for a future never polled.
        Box::pin(async move { Ok(self.produce(request)) })
    }
}

/// Builds the bounded model request one model effect resolves to.
fn model_request(
    context: &AgentContextSnapshotRef,
    profile: Option<&AgentModelProfileId>,
    turn: u64,
) -> AgentModelRequest {
    let mut request = AgentModelRequest::new(context.clone(), turn);
    if let Some(profile) = profile {
        request = request.with_profile(profile.clone());
    }
    request
}

/// The in-process stand-in for the durable dispatch pipeline of
/// [`crate::dispatch`], driving effects through a model adapter.
///
/// It plays exactly the dispatcher's role and no other
/// ([specification 9.5](../../../docs/plans/rakka-agent/spec.md)): it reads the
/// effects a run durably committed and dispatched, performs the bounded work —
/// invoking the [`AgentModelAdapter`] for a model call, and a scripted tool for a
/// tool call — and returns each outcome as a durable result command through the
/// run entity's own command surface. It never reaches into the run's state, and
/// it never advances the loop itself.
///
/// That is what makes it a faithful stub: a turn the adapter produces travels the
/// exact path a provider's turn travels, so the recovery this proves is the
/// recovery production gets. It is generic over the adapter and defaults to
/// [`DeterministicModelAdapter`], so the Rig-backed adapter of [`crate::rig`]
/// rides the same substrate under the `rig` feature. What it deliberately does
/// not model is the dispatch layer itself — leases, fences, attempt retries,
/// ambiguity recovery — which is [`crate::dispatch::AgentRunEffectDispatcher`]'s
/// job and is tested against the real thing.
///
/// An adapter error — a provider failure, or a turn that cannot be bounded — is
/// returned as a failed effect: final for the generation, exactly as the real
/// pipeline reports a definitive failure.
#[derive(Clone)]
pub struct ScriptedDispatcher<A = DeterministicModelAdapter> {
    adapter: A,
    answered: Arc<Mutex<BTreeMap<String, AgentRunEffectOutcome>>>,
    tools: Arc<Mutex<BTreeMap<String, AgentTaskContent>>>,
    failures: Arc<Mutex<BTreeMap<String, (String, String)>>>,
    compensations: Arc<Mutex<BTreeMap<String, AgentTaskContent>>>,
    promotions: Arc<Mutex<Option<Arc<dyn AgentMemoryPromotionExecutor>>>>,
    evaluations: Arc<Mutex<Option<Arc<dyn AgentGoalEvaluationExecutor>>>>,
    a2a_sends: Arc<Mutex<Option<Arc<dyn crate::dispatch::AgentA2aSendExecutor>>>>,
    workflow_starts: Arc<Mutex<Option<Arc<dyn crate::dispatch::AgentWorkflowStartExecutor>>>>,
    workflow_cancels: Arc<Mutex<Option<Arc<dyn crate::dispatch::AgentWorkflowCancelExecutor>>>>,
    claim_appends: Arc<Mutex<Option<Arc<dyn crate::dispatch::AgentClaimAppendExecutor>>>>,
    model_calls: Arc<AtomicUsize>,
    tool_calls: Arc<AtomicUsize>,
}

impl<A: std::fmt::Debug> std::fmt::Debug for ScriptedDispatcher<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScriptedDispatcher")
            .field("adapter", &self.adapter)
            .field(
                "promotions_wired",
                &self
                    .promotions
                    .lock()
                    .map(|slot| slot.is_some())
                    .unwrap_or(false),
            )
            .field("model_calls", &self.model_calls.load(Ordering::SeqCst))
            .field("tool_calls", &self.tool_calls.load(Ordering::SeqCst))
            .finish_non_exhaustive()
    }
}

impl Default for ScriptedDispatcher<DeterministicModelAdapter> {
    fn default() -> Self {
        Self::new()
    }
}

impl ScriptedDispatcher<DeterministicModelAdapter> {
    /// A dispatcher with an empty deterministic script.
    #[must_use]
    pub fn new() -> Self {
        Self::with_adapter(DeterministicModelAdapter::new())
    }

    /// Scripts the turn the next model call returns.
    #[must_use]
    pub fn with_turn(self, turn: AgentModelTurn) -> Self {
        self.adapter.script_turn(turn);
        self
    }
}

/// The key one effect's answer is memoized under: the effect id and its
/// dispatch generation, matching the derivation of
/// [`AgentRunEffect::result_operation_id`]. A re-invocation of the same
/// generation replays the recorded answer; a later generation is a new attempt
/// entirely — slice 1.7's retry machinery mints one precisely so a transient
/// failure is not replayed forever — and it reaches the adapter again.
fn memo_key(effect: &AgentRunEffect) -> String {
    format!("{}#{}", effect.effect_id.as_str(), effect.generation)
}

/// Wraps what an adapter produced as the model effect's outcome.
///
/// An adapter error and an unboundable turn alike surface as a failed effect,
/// exactly as a real dispatcher surfaces one: the turn is validated here, where
/// the outcome is formed, so an out-of-bounds turn becomes a `Failed` outcome
/// the run records and winds down on — never a result command the entity
/// refuses, which would leave the effect outstanding forever.
fn model_outcome(produced: AgentModelResult<AgentModelTurn>) -> AgentRunEffectOutcome {
    let validated = produced.and_then(|turn| {
        turn.validate()?;
        Ok(turn)
    });
    match validated {
        Ok(turn) => AgentRunEffectOutcome::Model {
            turn: Box::new(turn),
        },
        Err(error) => AgentRunEffectOutcome::Failed {
            code: error.code().to_string(),
            message: error.to_string(),
        },
    }
}

impl<A> ScriptedDispatcher<A>
where
    A: AgentModelAdapter,
{
    /// A dispatcher that answers model calls through `adapter`.
    #[must_use]
    pub fn with_adapter(adapter: A) -> Self {
        Self {
            adapter,
            answered: Arc::new(Mutex::new(BTreeMap::new())),
            tools: Arc::new(Mutex::new(BTreeMap::new())),
            failures: Arc::new(Mutex::new(BTreeMap::new())),
            compensations: Arc::new(Mutex::new(BTreeMap::new())),
            promotions: Arc::new(Mutex::new(None)),
            evaluations: Arc::new(Mutex::new(None)),
            a2a_sends: Arc::new(Mutex::new(None)),
            workflow_starts: Arc::new(Mutex::new(None)),
            workflow_cancels: Arc::new(Mutex::new(None)),
            claim_appends: Arc::new(Mutex::new(None)),
            model_calls: Arc::new(AtomicUsize::new(0)),
            tool_calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// The model adapter this dispatcher answers model calls through.
    #[must_use]
    pub fn adapter(&self) -> &A {
        &self.adapter
    }

    /// Scripts the result one tool returns, however often it is called.
    #[must_use]
    pub fn with_tool_result(self, tool: &str, content: AgentTaskContent) -> Self {
        self.tools
            .lock()
            .expect("the tool script should not be poisoned")
            .insert(tool.to_string(), content);
        self
    }

    /// Scripts a failure one tool returns.
    #[must_use]
    pub fn with_tool_failure(self, tool: &str, code: &str, message: &str) -> Self {
        self.failures
            .lock()
            .expect("the failure script should not be poisoned")
            .insert(tool.to_string(), (code.to_string(), message.to_string()));
        self
    }

    /// Scripts the result one compensation returns, keyed by its
    /// [`crate::checkpoints::AgentCompensationRef`]. An unscripted compensation
    /// fails with a stable `compensation-unscripted` code.
    #[must_use]
    pub fn with_compensation_result(self, compensation: &str, content: AgentTaskContent) -> Self {
        self.compensations
            .lock()
            .expect("the compensation script should not be poisoned")
            .insert(compensation.to_string(), content);
        self
    }

    /// Executes memory-promotion effects through the given executor — usually
    /// a [`crate::dispatch::SessionMemoryPromotionExecutor`] over the
    /// fixture's session and private stores. An unwired promotion fails with
    /// the real pipeline's `memory-promotion-executor-missing` code.
    #[must_use]
    pub fn with_memory_promotion_executor(
        self,
        executor: Arc<dyn AgentMemoryPromotionExecutor>,
    ) -> Self {
        *self
            .promotions
            .lock()
            .expect("the promotion executor slot should not be poisoned") = Some(executor);
        self
    }

    /// Executes goal-evaluation effects through the given executor. An
    /// unwired evaluation fails with the real pipeline's
    /// `evaluation-executor-missing` code; a human review never consults the
    /// executor — its effect-bound approval grant is its verdict, exactly as
    /// in the real pipeline.
    #[must_use]
    pub fn with_goal_evaluation_executor(
        self,
        executor: Arc<dyn AgentGoalEvaluationExecutor>,
    ) -> Self {
        *self
            .evaluations
            .lock()
            .expect("the evaluation executor slot should not be poisoned") = Some(executor);
        self
    }

    /// Executes outbound A2A send effects through the given executor. An
    /// unwired send fails with the real pipeline's
    /// `a2a-send-executor-missing` code, exactly as the real dispatcher
    /// fails closed.
    #[must_use]
    pub fn with_a2a_send_executor(
        self,
        executor: Arc<dyn crate::dispatch::AgentA2aSendExecutor>,
    ) -> Self {
        *self
            .a2a_sends
            .lock()
            .expect("the A2A send executor slot should not be poisoned") = Some(executor);
        self
    }

    /// Executes workflow start effects through the given executor. An
    /// unwired start fails with the real pipeline's
    /// `workflow-start-executor-missing` code, exactly as the real
    /// dispatcher fails closed.
    #[must_use]
    pub fn with_workflow_start_executor(
        self,
        executor: Arc<dyn crate::dispatch::AgentWorkflowStartExecutor>,
    ) -> Self {
        *self
            .workflow_starts
            .lock()
            .expect("the workflow start executor slot should not be poisoned") = Some(executor);
        self
    }

    /// Executes workflow cancel effects through the given executor. An
    /// unwired cancel fails with the real pipeline's
    /// `workflow-cancel-executor-missing` code, exactly as the real
    /// dispatcher fails closed.
    #[must_use]
    pub fn with_workflow_cancel_executor(
        self,
        executor: Arc<dyn crate::dispatch::AgentWorkflowCancelExecutor>,
    ) -> Self {
        *self
            .workflow_cancels
            .lock()
            .expect("the workflow cancel executor slot should not be poisoned") = Some(executor);
        self
    }

    /// Executes communal claim appends through the given executor. An
    /// unwired append fails with the real pipeline's
    /// `claim-append-executor-missing` code, exactly as the real dispatcher
    /// fails closed.
    #[must_use]
    pub fn with_claim_append_executor(
        self,
        executor: Arc<dyn crate::dispatch::AgentClaimAppendExecutor>,
    ) -> Self {
        *self
            .claim_appends
            .lock()
            .expect("the claim-append executor slot should not be poisoned") = Some(executor);
        self
    }

    /// How many model calls the dispatcher has answered, re-invocations included.
    #[must_use]
    pub fn model_calls(&self) -> usize {
        self.model_calls.load(Ordering::SeqCst)
    }

    /// How many tool calls the dispatcher has answered, re-invocations included.
    #[must_use]
    pub fn tool_calls(&self) -> usize {
        self.tool_calls.load(Ordering::SeqCst)
    }

    /// Answers every effect the run is currently waiting on, once.
    ///
    /// Returns how many result commands it delivered. A run with nothing
    /// outstanding is answered zero times, which is how a caller knows the loop
    /// has reached a wait that is not an effect — a task decision, or a terminal
    /// status.
    pub async fn drive<Store, Effects>(
        &self,
        entity: &mut AgentRunEntityStore<Store, Effects>,
        router: &AgentExchangeRouter,
        now: AgentTimestampMillis,
    ) -> Result<usize, AgentRunError>
    where
        Store: DurableStateStore<AgentRunState>,
        Effects: AgentRunEffectSink,
    {
        let scope = entity.scope().clone();
        let dispatched = entity
            .state()?
            .loop_state()
            .map(dispatched_effects)
            .unwrap_or_default();

        let mut delivered = 0;
        for effect in dispatched {
            let outcome = match &effect.request {
                // A promotion executor reads the run's session store, so it
                // needs the scope only `drive` holds.
                AgentRunEffectRequest::MemoryPromotion { promotion } => {
                    self.promotion_outcome(&scope, &effect, promotion, now)
                        .await
                }
                // An append executor writes under the run scope only `drive`
                // holds.
                AgentRunEffectRequest::ClaimAppend { append, provenance } => {
                    self.claim_append_outcome(&scope, &effect, append, provenance, now)
                        .await
                }
                // An evaluation needs the scope, and — for a human review —
                // the grant the run's own checkpoint issued, exactly as the
                // real authority reads it from the loop state.
                AgentRunEffectRequest::Evaluation { evaluation } => {
                    let grant = entity
                        .state()?
                        .loop_state()
                        .and_then(|loop_state| loop_state.grant_for(&effect).cloned());
                    self.evaluation_outcome(&scope, &effect, evaluation, grant.as_ref(), now)
                        .await
                }
                _ => self.answer(&effect).await,
            };
            let command = AgentRunEntityCommand::RecordEffectResult {
                operation_id: effect.result_operation_id(&scope)?,
                effect_id: effect.effect_id.clone(),
                generation: effect.generation,
                attempt: effect.attempts.saturating_add(1),
                // The in-process driver holds no fleet lease; the fence a real
                // dispatcher carries is its claim's fencing token.
                fence: 0,
                outcome: Box::new(outcome),
            };
            entity.apply(command, router, now).await?;
            delivered += 1;
        }
        Ok(delivered)
    }

    /// What this dispatcher returns for one effect, awaiting the adapter for a
    /// model call. [`Self::drive`] answers everything outstanding through this.
    ///
    /// The answer — a model turn or a tool outcome alike — is memoized on the
    /// effect id and its dispatch generation, so re-invoking an effect whose
    /// result was lost returns *the same* outcome, while a later generation — a
    /// genuine new attempt — reaches the adapter again. That is what the
    /// effect's idempotency key means
    /// ([specification 11.4](../../../docs/plans/rakka-agent/spec.md)), and it
    /// is what makes a recovery test deterministic.
    ///
    /// The call *counters* are not memoized, because a re-invocation really is
    /// another call: it is billed again, and slice 1.9 charges it again.
    pub async fn answer(&self, effect: &AgentRunEffect) -> AgentRunEffectOutcome {
        match &effect.request {
            AgentRunEffectRequest::Model { context, profile } => {
                self.model_calls.fetch_add(1, Ordering::SeqCst);
                if let Some(outcome) = self.cached(effect) {
                    return outcome;
                }
                let request = model_request(context, profile.as_ref(), effect.turn);
                // A provider failure or an unboundable turn is a failed effect,
                // exactly as a real dispatcher surfaces one; the interim loop
                // stops the run on it, and slice 1.7's retry policy governs
                // whether the effect machine tries again.
                let outcome = model_outcome(self.adapter.call(&request).await);
                self.memoize(effect, outcome)
            }
            AgentRunEffectRequest::Tool { call } => {
                self.tool_calls.fetch_add(1, Ordering::SeqCst);
                if let Some(outcome) = self.cached(effect) {
                    return outcome;
                }
                let outcome = self.tool_outcome(call);
                self.memoize(effect, outcome)
            }
            AgentRunEffectRequest::Compensation { compensation, .. } => {
                self.tool_calls.fetch_add(1, Ordering::SeqCst);
                if let Some(outcome) = self.cached(effect) {
                    return outcome;
                }
                let scripted = self
                    .compensations
                    .lock()
                    .expect("the compensation script should not be poisoned")
                    .get(compensation.as_str())
                    .cloned();
                let outcome = match scripted {
                    Some(content) => AgentRunEffectOutcome::Tool {
                        call_id: crate::effect::compensation_call_id(effect),
                        content,
                    },
                    None => AgentRunEffectOutcome::Failed {
                        code: "compensation-unscripted".to_string(),
                        message: format!("no scripted result for compensation {compensation}"),
                    },
                };
                self.memoize(effect, outcome)
            }
            AgentRunEffectRequest::MemoryPromotion { .. } => {
                // A promotion needs the run scope its executor reads under,
                // which this signature does not carry. Answer it through
                // [`Self::drive`] or [`Self::promotion_outcome`]. The failure
                // is deliberately NOT memoized, so a later scoped answer can
                // still resolve the effect.
                if let Some(outcome) = self.cached(effect) {
                    return outcome;
                }
                AgentRunEffectOutcome::Failed {
                    code: "memory-promotion-unscoped".to_string(),
                    message: "a memory promotion is answered through drive or promotion_outcome, \
                              which carry the run scope"
                        .to_string(),
                }
            }
            AgentRunEffectRequest::Evaluation { .. } => {
                // An evaluation needs the run scope — and, for a human
                // review, the grant — that only `drive` or
                // [`Self::evaluation_outcome`] carry. Deliberately not
                // memoized, so a later scoped answer can still resolve it.
                if let Some(outcome) = self.cached(effect) {
                    return outcome;
                }
                AgentRunEffectOutcome::Failed {
                    code: "goal-evaluation-unscoped".to_string(),
                    message: "a goal evaluation is answered through drive or \
                              evaluation_outcome, which carry the run scope"
                        .to_string(),
                }
            }
            AgentRunEffectRequest::A2aSend { delegation } => {
                // The record carries its own parent scope, so the send needs
                // nothing `answer` does not hold. Memoized like a tool call:
                // a re-invocation of the same generation returns the same
                // receipt, which is exactly what the derived deduplication
                // key promises.
                self.tool_calls.fetch_add(1, Ordering::SeqCst);
                if let Some(outcome) = self.cached(effect) {
                    return outcome;
                }
                let executor = self
                    .a2a_sends
                    .lock()
                    .expect("the A2A send executor slot should not be poisoned")
                    .clone();
                let outcome = match executor {
                    None => AgentRunEffectOutcome::Failed {
                        code: "a2a-send-executor-missing".to_string(),
                        message: "no A2A send executor is wired into this dispatcher".to_string(),
                    },
                    Some(executor) => match executor
                        .execute(&delegation.parent_run, effect, delegation, None)
                        .await
                    {
                        Ok(crate::dispatch::AgentA2aSendFinding::Sent {
                            child_task,
                            child_run,
                            peer_status,
                        }) => AgentRunEffectOutcome::A2aSend {
                            receipt: crate::delegation::AgentA2aSendReceipt {
                                delegation: delegation.delegation.clone(),
                                child_task,
                                child_run,
                                peer_status,
                            },
                        },
                        Ok(crate::dispatch::AgentA2aSendFinding::Conflict { code, message })
                        | Ok(crate::dispatch::AgentA2aSendFinding::Refused { code, message }) => {
                            AgentRunEffectOutcome::Failed { code, message }
                        }
                        // The in-process driver has no attempt machinery: a
                        // retryable failure surfaces as a failed effect, the
                        // model-adapter precedent above.
                        Err(error) => AgentRunEffectOutcome::Failed {
                            code: "a2a-send-attempt-failed".to_string(),
                            message: error.to_string(),
                        },
                    },
                };
                self.memoize(effect, outcome)
            }
            AgentRunEffectRequest::WorkflowStart { invocation } => {
                // The record carries its own parent scope, so the start needs
                // nothing `answer` does not hold. Memoized like a send: a
                // re-invocation of the same generation returns the same
                // receipt, which is exactly what the derived generation-free
                // `StartRun` identities promise.
                self.tool_calls.fetch_add(1, Ordering::SeqCst);
                if let Some(outcome) = self.cached(effect) {
                    return outcome;
                }
                let executor = self
                    .workflow_starts
                    .lock()
                    .expect("the workflow start executor slot should not be poisoned")
                    .clone();
                let outcome = match executor {
                    None => AgentRunEffectOutcome::Failed {
                        code: "workflow-start-executor-missing".to_string(),
                        message: "no workflow start executor is wired into this dispatcher"
                            .to_string(),
                    },
                    Some(executor) => match executor
                        .execute(&invocation.parent_run, effect, invocation, None)
                        .await
                    {
                        Ok(
                            finding @ (crate::dispatch::AgentWorkflowStartFinding::Started
                            | crate::dispatch::AgentWorkflowStartFinding::Adopted),
                        ) => AgentRunEffectOutcome::WorkflowStart {
                            receipt: crate::workflow_tool::AgentWorkflowStartReceipt {
                                invocation: invocation.invocation.clone(),
                                child_run: invocation.child_run.clone(),
                                adopted: matches!(
                                    finding,
                                    crate::dispatch::AgentWorkflowStartFinding::Adopted
                                ),
                            },
                        },
                        // The real dispatcher's conflict normalization,
                        // verbatim: the cell's `Conflicted` settlement is
                        // structural, never an executor string convention.
                        Ok(crate::dispatch::AgentWorkflowStartFinding::Conflict {
                            code,
                            message,
                        }) => AgentRunEffectOutcome::Failed {
                            code: crate::workflow_tool::AGENT_WORKFLOW_INVOCATION_CONFLICT_CODE
                                .to_string(),
                            message: format!("{code}: {message}"),
                        },
                        Ok(crate::dispatch::AgentWorkflowStartFinding::Refused {
                            code,
                            message,
                        }) => AgentRunEffectOutcome::Failed { code, message },
                        // The in-process driver has no attempt machinery: a
                        // retryable failure surfaces as a failed effect, the
                        // model-adapter precedent above.
                        Err(error) => AgentRunEffectOutcome::Failed {
                            code: "workflow-start-attempt-failed".to_string(),
                            message: error.to_string(),
                        },
                    },
                };
                self.memoize(effect, outcome)
            }
            AgentRunEffectRequest::WorkflowCancel { invocation, reason } => {
                // The record carries its own parent scope, so the cancel
                // needs nothing `answer` does not hold. Memoized like a
                // start: a re-invocation of the same generation returns the
                // same outcome, which is exactly what the derived
                // generation-free `CancelRun` identities promise.
                self.tool_calls.fetch_add(1, Ordering::SeqCst);
                if let Some(outcome) = self.cached(effect) {
                    return outcome;
                }
                let executor = self
                    .workflow_cancels
                    .lock()
                    .expect("the workflow cancel executor slot should not be poisoned")
                    .clone();
                let outcome = match executor {
                    None => AgentRunEffectOutcome::Failed {
                        code: "workflow-cancel-executor-missing".to_string(),
                        message: "no workflow cancel executor is wired into this dispatcher"
                            .to_string(),
                    },
                    Some(executor) => match executor
                        .execute(&invocation.parent_run, effect, invocation, reason, None)
                        .await
                    {
                        Ok(crate::dispatch::AgentWorkflowCancelFinding::Requested) => {
                            AgentRunEffectOutcome::WorkflowCancel {
                                already_finished: false,
                            }
                        }
                        Ok(crate::dispatch::AgentWorkflowCancelFinding::AlreadyFinished) => {
                            AgentRunEffectOutcome::WorkflowCancel {
                                already_finished: true,
                            }
                        }
                        Ok(crate::dispatch::AgentWorkflowCancelFinding::Refused {
                            code,
                            message,
                        }) => AgentRunEffectOutcome::Failed { code, message },
                        // The in-process driver has no attempt machinery: a
                        // retryable failure surfaces as a failed effect, the
                        // model-adapter precedent above.
                        Err(error) => AgentRunEffectOutcome::Failed {
                            code: "workflow-cancel-attempt-failed".to_string(),
                            message: error.to_string(),
                        },
                    },
                };
                self.memoize(effect, outcome)
            }
            AgentRunEffectRequest::ClaimAppend { .. } => {
                // An append executor runs under the run scope only `drive`
                // holds. Answer it through [`Self::drive`] or
                // [`Self::claim_append_outcome`]. Deliberately not memoized,
                // so a later scoped answer can still resolve the effect.
                if let Some(outcome) = self.cached(effect) {
                    return outcome;
                }
                AgentRunEffectOutcome::Failed {
                    code: "claim-append-unscoped".to_string(),
                    message: "a claim append is answered through drive or claim_append_outcome, \
                              which carry the run scope"
                        .to_string(),
                }
            }
        }
    }

    /// What this dispatcher returns for one claim-append effect, running the
    /// wired executor under the run's scope. Memoized on the effect id and
    /// generation exactly like every other answer.
    pub async fn claim_append_outcome(
        &self,
        scope: &AgentRunScope,
        effect: &AgentRunEffect,
        append: &crate::effect::AgentClaimAppendRequest,
        provenance: &crate::effect::AgentClaimAppendProvenance,
        now: AgentTimestampMillis,
    ) -> AgentRunEffectOutcome {
        self.tool_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(outcome) = self.cached(effect) {
            return outcome;
        }
        let executor = self
            .claim_appends
            .lock()
            .expect("the claim-append executor slot should not be poisoned")
            .clone();
        let outcome = match executor {
            None => AgentRunEffectOutcome::Failed {
                code: "claim-append-executor-missing".to_string(),
                message: "no claim-append executor is wired into this dispatcher".to_string(),
            },
            Some(executor) => match executor
                .execute(scope, effect, append, provenance, now)
                .await
            {
                Ok(crate::dispatch::AgentClaimAppendFinding::Appended { claim }) => {
                    AgentRunEffectOutcome::ClaimAppend { claim }
                }
                Ok(crate::dispatch::AgentClaimAppendFinding::Refused { code, message }) => {
                    AgentRunEffectOutcome::Failed { code, message }
                }
                // The in-process driver has no attempt machinery: a
                // retryable failure surfaces as a failed effect, the
                // model-adapter precedent above.
                Err(error) => AgentRunEffectOutcome::Failed {
                    code: "claim-append-attempt-failed".to_string(),
                    message: error.to_string(),
                },
            },
        };
        self.memoize(effect, outcome)
    }

    /// What this dispatcher returns for one memory-promotion effect, running
    /// the wired executor under the run's scope. Memoized on the effect id
    /// and generation exactly like every other answer.
    pub async fn promotion_outcome(
        &self,
        scope: &AgentRunScope,
        effect: &AgentRunEffect,
        promotion: &AgentMemoryPromotionRequest,
        now: AgentTimestampMillis,
    ) -> AgentRunEffectOutcome {
        self.tool_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(outcome) = self.cached(effect) {
            return outcome;
        }
        let executor = self
            .promotions
            .lock()
            .expect("the promotion executor slot should not be poisoned")
            .clone();
        let outcome = match executor {
            None => AgentRunEffectOutcome::Failed {
                code: "memory-promotion-executor-missing".to_string(),
                message: "no memory-promotion executor is wired into this dispatcher".to_string(),
            },
            Some(executor) => match executor.execute(scope, effect, promotion, now).await {
                Ok(AgentMemoryPromotionFinding::Promoted { promoted }) => {
                    AgentRunEffectOutcome::MemoryPromotion { promoted }
                }
                Ok(AgentMemoryPromotionFinding::Refused { code, message }) => {
                    AgentRunEffectOutcome::Failed { code, message }
                }
                // The in-process driver has no attempt machinery: a retryable
                // failure surfaces as a failed effect, the model-adapter
                // precedent above.
                Err(error) => AgentRunEffectOutcome::Failed {
                    code: "memory-promotion-attempt-failed".to_string(),
                    message: error.to_string(),
                },
            },
        };
        self.memoize(effect, outcome)
    }

    /// What this dispatcher returns for one goal-evaluation effect, mirroring
    /// the real pipeline's evaluation arm: a human review's verdict is the
    /// effect-bound approval grant, a verification workflow fails closed as
    /// deferred, and everything else runs the wired executor or fails with the
    /// real `evaluation-executor-missing` code. Memoized on the effect id and
    /// generation exactly like every other answer.
    pub async fn evaluation_outcome(
        &self,
        scope: &AgentRunScope,
        effect: &AgentRunEffect,
        evaluation: &crate::evaluation::AgentGoalEvaluationRequest,
        grant: Option<&crate::checkpoints::AgentCheckpointGrant>,
        now: AgentTimestampMillis,
    ) -> AgentRunEffectOutcome {
        use crate::evaluation::{
            goal_evaluation_record_id, AgentGoalEvaluationMethod, AgentGoalEvaluationOutcome,
            AgentGoalEvaluationRecord, AgentGoalEvidenceRef,
        };

        self.tool_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(outcome) = self.cached(effect) {
            return outcome;
        }
        let evaluation_id =
            match goal_evaluation_record_id(scope, effect.turn, effect.slot, effect.generation) {
                Ok(evaluation_id) => evaluation_id,
                Err(error) => {
                    return self.memoize(
                        effect,
                        AgentRunEffectOutcome::Failed {
                            code: "evaluation-identity-invalid".to_string(),
                            message: error.to_string(),
                        },
                    );
                }
            };
        let build = |outcome: AgentGoalEvaluationOutcome,
                     reason_code: String,
                     evidence: Vec<AgentGoalEvidenceRef>,
                     evaluated_by| {
            AgentGoalEvaluationRecord::new(
                evaluation_id.clone(),
                evaluation.goal.clone(),
                evaluation.evaluator.clone(),
                evaluation.method.kind(),
                evaluation.criteria_revision,
                outcome,
                reason_code,
                evidence,
                evaluated_by,
                effect.effect_id.clone(),
                effect.generation,
                now,
            )
        };
        let finding = match &evaluation.method {
            AgentGoalEvaluationMethod::HumanReview => match grant {
                None => {
                    return self.memoize(
                        effect,
                        AgentRunEffectOutcome::Failed {
                            code: "evaluation-grant-missing".to_string(),
                            message: "a human-review evaluation dispatched without its approval \
                                      grant"
                                .to_string(),
                        },
                    );
                }
                Some(grant) => {
                    let mut evidence = evaluation.evidence.clone();
                    evidence.push(AgentGoalEvidenceRef {
                        class: crate::evaluation::AGENT_GOAL_EVALUATION_HUMAN_DECISION_CLASS
                            .to_string(),
                        artifact: None,
                        digest: Some(grant.argument_digest.clone()),
                    });
                    AgentGoalEvaluationFinding::Evaluated {
                        outcome: AgentGoalEvaluationOutcome::Satisfied,
                        reason_code: "human-approved".to_string(),
                        evidence,
                        evaluated_by: Some(grant.resolver.clone()),
                    }
                }
            },
            AgentGoalEvaluationMethod::VerificationWorkflow { .. } => {
                return self.memoize(
                    effect,
                    AgentRunEffectOutcome::Failed {
                        code: "evaluation-workflow-deferred".to_string(),
                        message: "a verification-workflow evaluation cannot execute until the \
                                  evaluation cell is bridged to the workflow-tool invocation path"
                            .to_string(),
                    },
                );
            }
            _ => {
                let executor = self
                    .evaluations
                    .lock()
                    .expect("the evaluation executor slot should not be poisoned")
                    .clone();
                match executor {
                    None => {
                        return self.memoize(
                            effect,
                            AgentRunEffectOutcome::Failed {
                                code: "evaluation-executor-missing".to_string(),
                                message: "no goal-evaluation executor is wired into this \
                                          dispatcher"
                                    .to_string(),
                            },
                        );
                    }
                    Some(executor) => {
                        match executor.execute(scope, effect, evaluation, None, now).await {
                            Ok(finding) => finding,
                            // The in-process driver has no attempt machinery:
                            // a retryable failure surfaces as a failed effect,
                            // the promotion precedent above.
                            Err(error) => {
                                return self.memoize(
                                    effect,
                                    AgentRunEffectOutcome::Failed {
                                        code: "evaluation-attempt-failed".to_string(),
                                        message: error.to_string(),
                                    },
                                );
                            }
                        }
                    }
                }
            }
        };
        let outcome = match finding {
            AgentGoalEvaluationFinding::Evaluated {
                outcome,
                reason_code,
                evidence,
                evaluated_by,
            } => match build(outcome, reason_code, evidence, evaluated_by) {
                Ok(record) => AgentRunEffectOutcome::Evaluation {
                    record: Box::new(record),
                },
                Err(error) => AgentRunEffectOutcome::Failed {
                    code: "evaluation-record-invalid".to_string(),
                    message: error.to_string(),
                },
            },
            AgentGoalEvaluationFinding::Refused { code, message } => {
                AgentRunEffectOutcome::Failed { code, message }
            }
        };
        self.memoize(effect, outcome)
    }

    fn lock_answered(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, AgentRunEffectOutcome>> {
        self.answered
            .lock()
            .expect("the answered script should not be poisoned")
    }

    fn cached(&self, effect: &AgentRunEffect) -> Option<AgentRunEffectOutcome> {
        self.lock_answered().get(&memo_key(effect)).cloned()
    }

    /// Records the outcome under the effect's memo key, first writer winning.
    ///
    /// The lock cannot be held across the adapter's await, so two invocations
    /// racing on one effect's *first* answer may both produce — but every caller
    /// returns the single outcome that was recorded, so the memoized answer
    /// stays one answer.
    fn memoize(
        &self,
        effect: &AgentRunEffect,
        outcome: AgentRunEffectOutcome,
    ) -> AgentRunEffectOutcome {
        self.lock_answered()
            .entry(memo_key(effect))
            .or_insert(outcome)
            .clone()
    }

    fn tool_outcome(&self, call: &AgentToolCallRequest) -> AgentRunEffectOutcome {
        let tool = call.tool.to_string();
        if let Some((code, message)) = self
            .failures
            .lock()
            .expect("the failure script should not be poisoned")
            .get(&tool)
            .cloned()
        {
            AgentRunEffectOutcome::Failed { code, message }
        } else {
            let content = self
                .tools
                .lock()
                .expect("the tool script should not be poisoned")
                .get(&tool)
                .cloned()
                .unwrap_or_else(|| {
                    AgentTaskContent::inline(serde_json::json!({ "tool": tool }))
                        .expect("the default tool result is inline-bounded")
                });
            AgentRunEffectOutcome::Tool {
                call_id: call.call_id.clone(),
                content,
            }
        }
    }
}

fn dispatched_effects(state: &AgentLoopState) -> Vec<AgentRunEffect> {
    state
        .effects()
        .iter()
        .filter(|effect| effect.status == AgentRunEffectStatus::Ready)
        .cloned()
        .collect()
}

/// Delivers dispatch-pipeline result commands to a real run entity over a
/// shared durable store.
///
/// Every delivery re-materializes the entity from durable state alone — which
/// is what a sharded ask looks like from the outside — and applies the command
/// through the entity's full path, so the deduplication and fencing the
/// pipeline relies on are the entity's own.
pub struct InProcessRunResultDelivery<Store, Effects>
where
    Store: DurableStateStore<AgentRunState>,
    Effects: AgentRunEffectSink,
{
    store: Store,
    effects: Effects,
    router: AgentExchangeRouter,
    clock: Arc<AtomicU64>,
    policies: AgentEffectPolicies,
    workflow_tools: Option<crate::workflow_tool::AgentRunWorkflowConfig>,
}

impl<Store, Effects> InProcessRunResultDelivery<Store, Effects>
where
    Store: DurableStateStore<AgentRunState>,
    Effects: AgentRunEffectSink,
{
    /// Creates a delivery over one durable run store.
    #[must_use]
    pub fn new(
        store: Store,
        effects: Effects,
        router: AgentExchangeRouter,
        clock: Arc<AtomicU64>,
    ) -> Self {
        Self {
            store,
            effects,
            router,
            clock,
            policies: AgentEffectPolicies::default(),
            workflow_tools: None,
        }
    }

    /// Uses explicit effect specs for the effects hosted runs commit.
    #[must_use]
    pub fn with_effect_policies(mut self, policies: AgentEffectPolicies) -> Self {
        self.policies = policies;
        self
    }

    /// Wires the entities this delivery builds to serve workflow tools —
    /// the delivered model result is what the loop evaluates, so the
    /// interception must be wired on this path too.
    #[must_use]
    pub fn with_workflow_tools(
        mut self,
        config: crate::workflow_tool::AgentRunWorkflowConfig,
    ) -> Self {
        self.workflow_tools = Some(config);
        self
    }
}

impl<Store, Effects> AgentRunResultDelivery for InProcessRunResultDelivery<Store, Effects>
where
    Store: DurableStateStore<AgentRunState>,
    Effects: AgentRunEffectSink,
{
    fn deliver<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        command: AgentRunEntityCommand,
    ) -> AgentDispatchFuture<'a, AgentRunEntityReply> {
        Box::pin(async move {
            let mut entity =
                AgentRunEntityStore::new(scope.clone(), self.store.clone(), self.effects.clone())
                    .with_effect_policies(self.policies.clone());
            if let Some(config) = &self.workflow_tools {
                entity = entity.with_workflow_tools(config.clone());
            }
            let now = AgentTimestampMillis::new(self.clock.fetch_add(1, Ordering::SeqCst));
            entity
                .apply(command, &self.router, now)
                .await
                .map_err(AgentDispatchError::from)
        })
    }
}

/// A tool executor that records every external invocation it performs.
///
/// The invocation log *is* the external system of the recovery scenarios: how
/// many times a target committed is exactly what scenarios 5 through 9 assert
/// about, and the recorded idempotency keys are what proves a retry reused the
/// generation's external key.
#[derive(Clone, Default)]
pub struct RecordingToolExecutor {
    results: Arc<Mutex<BTreeMap<String, AgentTaskContent>>>,
    failures: Arc<Mutex<BTreeMap<String, (String, String)>>>,
    invocations: Arc<Mutex<Vec<RecordedToolInvocation>>>,
}

/// One external invocation a [`RecordingToolExecutor`] performed.
#[derive(Debug, Clone, PartialEq)]
pub struct RecordedToolInvocation {
    /// The tool that was invoked.
    pub tool: String,
    /// The call the model asked for.
    pub call_id: String,
    /// The arguments the executor was handed — after any deterministic
    /// guardrail transform, which is how a test observes that the transformed
    /// input, not the model's original, reached the target.
    pub arguments: serde_json::Value,
    /// The idempotency key the target was handed.
    pub idempotency_key: String,
    /// The generation the attempt served.
    pub generation: u32,
    /// Whether a resolved credential accompanied the attempt.
    pub with_credential: bool,
}

impl RecordingToolExecutor {
    /// An executor with no scripted results: every tool echoes its name.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Scripts the result one tool returns.
    #[must_use]
    pub fn with_result(self, tool: &str, content: AgentTaskContent) -> Self {
        self.results
            .lock()
            .expect("the result script should not be poisoned")
            .insert(tool.to_string(), content);
        self
    }

    /// Scripts a failure one tool returns, however often it is invoked.
    #[must_use]
    pub fn with_failure(self, tool: &str, code: &str, message: &str) -> Self {
        self.failures
            .lock()
            .expect("the failure script should not be poisoned")
            .insert(tool.to_string(), (code.to_string(), message.to_string()));
        self
    }

    /// Every external invocation performed, in order.
    #[must_use]
    pub fn invocations(&self) -> Vec<RecordedToolInvocation> {
        self.invocations
            .lock()
            .expect("the invocation log should not be poisoned")
            .clone()
    }

    /// How many external invocations one tool has performed.
    #[must_use]
    pub fn invocation_count(&self, tool: &str) -> usize {
        self.invocations
            .lock()
            .expect("the invocation log should not be poisoned")
            .iter()
            .filter(|invocation| invocation.tool == tool)
            .count()
    }
}

impl Debug for RecordingToolExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecordingToolExecutor")
            .field("invocations", &self.invocations())
            .finish_non_exhaustive()
    }
}

impl AgentDispatchToolExecutor for RecordingToolExecutor {
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        intent: &'a AgentRunEffect,
        call: &'a AgentToolCallRequest,
        credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentDispatchFuture<'a, AgentTaskContent> {
        let with_credential = credential.is_some();
        Box::pin(async move {
            let tool = call.tool.to_string();
            // The commit happens before the failure check: a tool that fails
            // may still have touched the external system, which is exactly the
            // ambiguity the safety classes exist for.
            self.invocations
                .lock()
                .expect("the invocation log should not be poisoned")
                .push(RecordedToolInvocation {
                    tool: tool.clone(),
                    call_id: call.call_id.to_string(),
                    arguments: call.arguments.clone(),
                    idempotency_key: intent.idempotency_key.as_str().to_string(),
                    generation: intent.generation.get(),
                    with_credential,
                });

            if let Some((code, message)) = self
                .failures
                .lock()
                .expect("the failure script should not be poisoned")
                .get(&tool)
                .cloned()
            {
                return Err(AgentDispatchError::collaborator(code, message));
            }
            Ok(self
                .results
                .lock()
                .expect("the result script should not be poisoned")
                .get(&tool)
                .cloned()
                .unwrap_or_else(|| {
                    AgentTaskContent::inline(serde_json::json!({ "tool": tool }))
                        .expect("the default tool result is inline-bounded")
                }))
        })
    }
}

/// A reconciler that answers from a script, in order, and repeats its last
/// answer when the script is spent.
#[derive(Clone, Default)]
pub struct ScriptedReconciler {
    findings: Arc<Mutex<VecDeque<AgentReconciliationFinding>>>,
    queries: Arc<AtomicUsize>,
}

impl ScriptedReconciler {
    /// A reconciler whose every query answers `Unknown`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Scripts the next finding.
    #[must_use]
    pub fn with_finding(self, finding: AgentReconciliationFinding) -> Self {
        self.findings
            .lock()
            .expect("the finding script should not be poisoned")
            .push_back(finding);
        self
    }

    /// How many times a protocol was queried.
    #[must_use]
    pub fn queries(&self) -> usize {
        self.queries.load(Ordering::SeqCst)
    }
}

impl Debug for ScriptedReconciler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScriptedReconciler")
            .field("queries", &self.queries())
            .finish_non_exhaustive()
    }
}

impl AgentEffectReconciler for ScriptedReconciler {
    fn reconcile<'a>(
        &'a self,
        _protocol: &'a AgentReconciliationProtocolRef,
        _scope: &'a AgentRunScope,
        _effect: &'a AgentRunEffect,
    ) -> AgentDispatchFuture<'a, AgentReconciliationFinding> {
        Box::pin(async move {
            self.queries.fetch_add(1, Ordering::SeqCst);
            let mut findings = self
                .findings
                .lock()
                .expect("the finding script should not be poisoned");
            Ok(if findings.len() > 1 {
                findings.pop_front().expect("the script is non-empty")
            } else {
                findings
                    .front()
                    .cloned()
                    .unwrap_or(AgentReconciliationFinding::Unknown)
            })
        })
    }
}

/// A credential resolver that returns a scripted ephemeral credential and
/// counts its resolutions.
///
/// The count is what proves dispatch-time resolution *only*: exactly one
/// resolution per dispatch attempt, none at commit time, none at recovery
/// time, and the resolved value never appears in any durable record.
#[derive(Clone)]
pub struct ScriptedCredentialResolver {
    token: String,
    resolutions: Arc<AtomicUsize>,
}

impl ScriptedCredentialResolver {
    /// A resolver that mints bearer credentials carrying `token`.
    #[must_use]
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
            resolutions: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// How many bindings have been resolved.
    #[must_use]
    pub fn resolutions(&self) -> usize {
        self.resolutions.load(Ordering::SeqCst)
    }
}

impl Debug for ScriptedCredentialResolver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScriptedCredentialResolver")
            .field("resolutions", &self.resolutions())
            .field("token", &"<redacted>")
            .finish()
    }
}

impl AgentEffectCredentialResolver for ScriptedCredentialResolver {
    fn resolve<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _binding: &'a AgentCredentialBindingRef,
        _effect: &'a AgentRunEffect,
    ) -> AgentDispatchFuture<'a, AgentEphemeralCredential> {
        Box::pin(async move {
            self.resolutions.fetch_add(1, Ordering::SeqCst);
            Ok(AgentEphemeralCredential::bearer_token(self.token.clone()))
        })
    }
}

/// A workflow clock over a shared atomic counter, so a test can advance time
/// past a dispatch lease deliberately.
///
/// Reading it never advances it, which is what a clock must do when the
/// dispatch pipeline consults it many times within one pass.
#[derive(Clone, Debug)]
pub struct SharedAtomicWorkflowClock(Arc<AtomicU64>);

impl SharedAtomicWorkflowClock {
    /// A clock over the given counter, shared with whatever else stamps time.
    #[must_use]
    pub const fn new(counter: Arc<AtomicU64>) -> Self {
        Self(counter)
    }

    /// Advances the clock by `millis`.
    pub fn advance(&self, millis: u64) {
        self.0.fetch_add(millis, Ordering::SeqCst);
    }
}

impl rakka_agent_workflow::substrate::WorkflowClock for SharedAtomicWorkflowClock {
    fn now(&self) -> rakka_agent_workflow::substrate::WorkflowTimestamp {
        rakka_agent_workflow::substrate::WorkflowTimestamp::from_millis(
            self.0.load(Ordering::SeqCst),
        )
    }
}

/// A probe that kills the dispatch worker at one armed window, once.
///
/// Arming a window makes the next attempt die exactly there — between the two
/// durable writes the window sits between — and every later attempt survive,
/// which is what lets one test run the crash and then the recovery over the
/// same durable stores.
#[derive(Clone, Default)]
pub struct KillSwitchProbe {
    armed: Arc<Mutex<Option<AgentDispatchWindow>>>,
    deaths: Arc<AtomicUsize>,
}

impl KillSwitchProbe {
    /// A probe that never kills anything.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Arms the probe: the next attempt dies at this window.
    pub fn arm(&self, window: AgentDispatchWindow) {
        *self
            .armed
            .lock()
            .expect("the kill switch should not be poisoned") = Some(window);
    }

    /// How many times the probe has killed a worker.
    #[must_use]
    pub fn deaths(&self) -> usize {
        self.deaths.load(Ordering::SeqCst)
    }
}

impl Debug for KillSwitchProbe {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KillSwitchProbe")
            .field("deaths", &self.deaths())
            .finish_non_exhaustive()
    }
}

impl AgentDispatchProbe for KillSwitchProbe {
    fn survives(&self, window: AgentDispatchWindow) -> bool {
        let mut armed = self
            .armed
            .lock()
            .expect("the kill switch should not be poisoned");
        if *armed == Some(window) {
            *armed = None;
            self.deaths.fetch_add(1, Ordering::SeqCst);
            return false;
        }
        true
    }
}

/// Materializes one run entity from durable state alone, exactly as a shard owner
/// does after an activation.
///
/// Nothing in memory carries over between calls, which is the point: a test that
/// wants to prove a run survives a restart simply builds a new one.
#[must_use]
pub fn run_entity<Store, Effects>(
    scope: &AgentRunScope,
    store: &Store,
    effects: &Effects,
) -> AgentRunEntityStore<Store, Effects>
where
    Store: DurableStateStore<AgentRunState>,
    Effects: AgentRunEffectSink,
{
    AgentRunEntityStore::new(scope.clone(), store.clone(), effects.clone())
}

/// Where an entity's owner dies, relative to the compare-and-set it was
/// performing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CrashPoint {
    /// The owner died before the write reached the store, so the transition is
    /// lost entirely.
    BeforeWrite,
    /// The owner died after the write committed but before it could act on what
    /// it had just decided — dispatch the effect it persisted, send the exchange
    /// it owed, or answer its caller.
    ///
    /// This is the window that matters: the entity is durably committed to
    /// something it has not yet told anyone about, and recovery must find it.
    AfterWrite,
}

/// Runs one recovery scenario once per (write, crash point): the exhaustive
/// owner-kill sweep of the M1 acceptance suite.
///
/// `writes` is the durable write count a crash-free reference run of the same
/// flow observed; the sweep then kills the owner at every one of those writes,
/// on both sides of the compare-and-set. The closure owns everything
/// scenario-specific: it builds a fresh fixture, arms exactly one
/// [`CrashingStateStore`] with `crash_at(nth, point)`, drives the flow to the
/// injected loss (ignoring the surfaced error), calls `survive()`, re-drives
/// from durable state alone, and asserts the scenario's exactly-once
/// invariants — including `nth` and `point` in its panic messages so a
/// failing window names itself.
///
/// The harness owns only the loop skeleton. It takes no store list because a
/// faithful sweep iteration builds its fixture *inside* the closure — the
/// stores do not exist until then. A flow whose crash windows span several
/// stores is swept by one call per store, each against that store's own
/// reference write count.
pub async fn sweep_crash_points<F, Fut>(writes: usize, scenario: F)
where
    F: Fn(usize, CrashPoint) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    for point in [CrashPoint::AfterWrite, CrashPoint::BeforeWrite] {
        for nth in 1..=writes {
            scenario(nth, point).await;
        }
    }
}

/// A durable state store whose owner dies at the *n*-th write.
///
/// Nothing else about it is special, and that is the point: whatever it has
/// already committed is exactly what a real owner finds on the next activation,
/// so re-materializing an entity over it is a faithful restart.
///
/// Every store class in the M1 suite — run, task, agent, workflow outbox, and
/// dispatcher fleet — is wrapped in one of these, and [`sweep_crash_points`]
/// drives the kill through every write of a flow. Compare-and-set and delete
/// both count as writes, so the sweep stays exhaustive for flows that delete;
/// loads are never counted and never crash.
pub struct CrashingStateStore<S>
where
    S: rakka_persistence::DurableState,
{
    inner: rakka_persistence::InMemoryDurableStateStore<S>,
    writes: Arc<AtomicUsize>,
    crash_at: Arc<AtomicUsize>,
    crash_after: Arc<std::sync::atomic::AtomicBool>,
}

impl<S> Clone for CrashingStateStore<S>
where
    S: rakka_persistence::DurableState,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            writes: self.writes.clone(),
            crash_at: self.crash_at.clone(),
            crash_after: self.crash_after.clone(),
        }
    }
}

impl<S> std::fmt::Debug for CrashingStateStore<S>
where
    S: rakka_persistence::DurableState,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CrashingStateStore")
            .field("writes", &self.writes())
            .field("crash_at", &self.crash_at.load(Ordering::SeqCst))
            .finish_non_exhaustive()
    }
}

impl<S> Default for CrashingStateStore<S>
where
    S: rakka_persistence::DurableState,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<S> CrashingStateStore<S>
where
    S: rakka_persistence::DurableState,
{
    /// A store whose owner never dies.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: rakka_persistence::InMemoryDurableStateStore::new(),
            writes: Arc::new(AtomicUsize::new(0)),
            crash_at: Arc::new(AtomicUsize::new(0)),
            crash_after: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Kills the owner at the `nth` write from now on, and resets the counter.
    ///
    /// `nth` is 1-based: the next write is write 1. Zero is the internal
    /// "no crash armed" sentinel, so arming it would silently disarm — the
    /// debug assertion makes that call loud instead. Write ordinals are
    /// assigned at call time, so under concurrent writers the `nth` write is
    /// the nth *attempted*, not the nth committed; every current harness
    /// drives its stores sequentially, where the two orders coincide.
    pub fn crash_at(&self, nth: usize, point: CrashPoint) {
        debug_assert!(nth >= 1, "crash_at is 1-based; 0 disarms rather than arms");
        self.writes.store(0, Ordering::SeqCst);
        self.crash_at.store(nth, Ordering::SeqCst);
        self.crash_after
            .store(matches!(point, CrashPoint::AfterWrite), Ordering::SeqCst);
    }

    /// Stops killing the owner. The next activation recovers whatever the last
    /// committed write left behind.
    pub fn survive(&self) {
        self.crash_at.store(0, Ordering::SeqCst);
    }

    /// How many writes have been attempted since the counter was last reset.
    #[must_use]
    pub fn writes(&self) -> usize {
        self.writes.load(Ordering::SeqCst)
    }

    /// Resets the write counter without arming a crash.
    pub fn reset_writes(&self) {
        self.writes.store(0, Ordering::SeqCst);
    }

    /// Asserts the crash armed at the `nth` write was actually reached.
    ///
    /// A sweep iteration whose flow never attempts its armed write converges
    /// trivially and proves nothing — silent under-coverage that matters
    /// whenever the swept flow is shaped differently from the reference run
    /// that measured the write count. Call this after the crashed drive and
    /// before [`Self::survive`] to make every window's firing a hard fact.
    #[track_caller]
    pub fn assert_crash_fired(&self, nth: usize, point: CrashPoint) {
        let writes = self.writes();
        assert!(
            writes >= nth,
            "the crash {point:?} armed at write {nth} never fired: \
             the flow attempted only {writes} writes"
        );
    }
}

impl<S> rakka_persistence::DurableStateStore<S> for CrashingStateStore<S>
where
    S: rakka_persistence::DurableState,
{
    fn backend_name(&self) -> &'static str {
        "crashing-in-memory"
    }

    fn load<'a>(
        &'a self,
        persistence_id: &'a rakka_persistence::PersistenceId,
    ) -> rakka_persistence::StoreFuture<'a, Option<rakka_persistence::StateRecord<S>>> {
        self.inner.load(persistence_id)
    }

    fn compare_and_set<'a>(
        &'a self,
        persistence_id: &'a rakka_persistence::PersistenceId,
        expected_revision: rakka_persistence::Revision,
        state: S,
    ) -> rakka_persistence::StoreFuture<'a, rakka_persistence::StateRecord<S>> {
        let write = self.writes.fetch_add(1, Ordering::SeqCst) + 1;
        let crash_at = self.crash_at.load(Ordering::SeqCst);
        let after = self.crash_after.load(Ordering::SeqCst);

        Box::pin(async move {
            if crash_at != 0 && write == crash_at && !after {
                return Err(rakka_persistence::DurableError::store(
                    "crashing-in-memory",
                    "the owner was lost before the write reached the store",
                ));
            }
            let record = self
                .inner
                .compare_and_set(persistence_id, expected_revision, state)
                .await?;
            if crash_at != 0 && write == crash_at && after {
                return Err(rakka_persistence::DurableError::store(
                    "crashing-in-memory",
                    "the owner was lost after the write committed",
                ));
            }
            Ok(record)
        })
    }

    fn delete<'a>(
        &'a self,
        persistence_id: &'a rakka_persistence::PersistenceId,
        expected_revision: rakka_persistence::Revision,
    ) -> rakka_persistence::StoreFuture<'a, rakka_persistence::Revision> {
        // A delete is a durable write too: it is counted and crash-armable
        // exactly like a compare-and-set, so a sweep stays exhaustive the day
        // a flow starts deleting (no M1 flow does yet).
        let write = self.writes.fetch_add(1, Ordering::SeqCst) + 1;
        let crash_at = self.crash_at.load(Ordering::SeqCst);
        let after = self.crash_after.load(Ordering::SeqCst);

        Box::pin(async move {
            if crash_at != 0 && write == crash_at && !after {
                return Err(rakka_persistence::DurableError::store(
                    "crashing-in-memory",
                    "the owner was lost before the delete reached the store",
                ));
            }
            let revision = self.inner.delete(persistence_id, expected_revision).await?;
            if crash_at != 0 && write == crash_at && after {
                return Err(rakka_persistence::DurableError::store(
                    "crashing-in-memory",
                    "the owner was lost after the delete committed",
                ));
            }
            Ok(revision)
        })
    }
}

/// A router whose routes are installed after the transports that use them exist.
///
/// Two entity classes that exchange with each other — a task assigns a run, that
/// run proposes its result back to the task — need routers that name one another,
/// and a value cannot be built out of itself.
///
/// Production has no such problem, and it is worth being precise about why:
/// [`crate::choreography::ShardedExchangeRoute`] resolves its target through the
/// sharding registry *at delivery time*, so it never holds the router it belongs
/// to. This deferred router plays exactly that part in process — it is late
/// binding, not a shortcut. The durable path an exchange travels is unchanged.
#[derive(Clone, Default)]
pub struct DeferredExchangeRouter {
    routes: Arc<Mutex<Option<AgentExchangeRouter>>>,
}

impl std::fmt::Debug for DeferredExchangeRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeferredExchangeRouter")
            .field("installed", &self.installed())
            .finish()
    }
}

impl DeferredExchangeRouter {
    /// A router with nothing installed yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Installs the real routes. Every delivery from now on resolves through
    /// them.
    pub fn install(&self, router: AgentExchangeRouter) {
        *self
            .routes
            .lock()
            .expect("the deferred router should not be poisoned") = Some(router);
    }

    /// Whether the real routes have been installed.
    #[must_use]
    pub fn installed(&self) -> bool {
        self.routes
            .lock()
            .expect("the deferred router should not be poisoned")
            .is_some()
    }

    /// A router that sends every entity class through this deferred router.
    ///
    /// Hand it to a transport that must be constructed *before* the routes it
    /// will need.
    #[must_use]
    pub fn as_router(&self) -> AgentExchangeRouter {
        let transport: Arc<dyn AgentExchangeTransport> = Arc::new(self.clone());
        AgentExchangeRouter::new()
            .with_route(AgentEntityClass::Agent, transport.clone())
            .with_route(AgentEntityClass::Task, transport.clone())
            .with_route(AgentEntityClass::Run, transport)
    }
}

impl AgentExchangeTransport for DeferredExchangeRouter {
    fn deliver<'a>(
        &'a self,
        envelope: &'a AgentExchangeEnvelope,
    ) -> AgentExchangeDeliveryFuture<'a> {
        // The router is cloned out before the delivery is awaited, so no lock is
        // held across a suspension point.
        let installed = self
            .routes
            .lock()
            .expect("the deferred router should not be poisoned")
            .clone();

        Box::pin(async move {
            let Some(router) = installed else {
                return Err(AgentExchangeDeliveryError::new(
                    "exchange-no-route",
                    "no routes have been installed on the deferred router",
                ));
            };
            router.deliver(envelope).await
        })
    }
}

/// Routes exchanges to one class of sharded entity on a single-node system —
/// the local arm of the production `ShardedExchangeRoute`, without the
/// `rakka-remote` ask client the other arm needs.
///
/// It resolves the target's shard owner and asks the local entity through the
/// production route's own functions — not a copy of them — delivering the
/// same envelope to the same durable [`AgentExchangeHost::accept`]: colocation
/// changes the transport, never the durable path. An owner that is not local
/// is an explicit error, because a single-node test that reaches that branch
/// has mis-wired its sharding, not discovered a remote peer.
pub struct LocalShardedExchangeRoute<M>
where
    M: rakka_core::Message,
{
    sharding: rakka_sharding::ClusterSharding,
    key: rakka_sharding::EntityTypeKey<M>,
    ask_timeout: std::time::Duration,
    #[allow(clippy::type_complexity)]
    build: Arc<
        dyn Fn(AgentExchangeEnvelope, rakka_core::ReplyTo<AgentExchangeReply>) -> M + Send + Sync,
    >,
}

impl<M> Debug for LocalShardedExchangeRoute<M>
where
    M: rakka_core::Message,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalShardedExchangeRoute")
            .field("entity_type", self.key.entity_type())
            .field("ask_timeout", &self.ask_timeout)
            .finish_non_exhaustive()
    }
}

impl<M> LocalShardedExchangeRoute<M>
where
    M: rakka_core::Message,
{
    /// Creates a route to one sharded entity type on the local node.
    ///
    /// `build` reconstructs the entity's own message from the envelope and a
    /// node-local reply channel, exactly as the production route's does.
    pub fn new(
        sharding: rakka_sharding::ClusterSharding,
        key: rakka_sharding::EntityTypeKey<M>,
        ask_timeout: std::time::Duration,
        build: impl Fn(AgentExchangeEnvelope, rakka_core::ReplyTo<AgentExchangeReply>) -> M
            + Send
            + Sync
            + 'static,
    ) -> Self {
        Self {
            sharding,
            key,
            ask_timeout,
            build: Arc::new(build),
        }
    }
}

impl<M> AgentExchangeTransport for LocalShardedExchangeRoute<M>
where
    M: rakka_core::Message + Sync,
{
    fn deliver<'a>(
        &'a self,
        envelope: &'a AgentExchangeEnvelope,
    ) -> AgentExchangeDeliveryFuture<'a> {
        Box::pin(async move {
            let (entity, owner, is_local) = crate::choreography::resolve_sharded_exchange_target(
                &self.sharding,
                &self.key,
                envelope,
            )?;
            if !is_local {
                return Err(AgentExchangeDeliveryError::new(
                    "exchange-not-local",
                    format!(
                        "the shard owner of {} is {owner}, which is not this node; \
                         the local route serves single-node systems only",
                        envelope.target().entity_id().as_str()
                    ),
                ));
            }
            crate::choreography::ask_local_sharded_entity(
                &entity,
                &self.build,
                envelope,
                self.ask_timeout,
            )
            .await
        })
    }
}

/// A deterministic [`AgentMemoryEmbedder`](crate::retrieval::AgentMemoryEmbedder)
/// for tests: a fixed-dimension token-hash bag-of-words embedding, no network,
/// no model.
///
/// The same text always embeds to the same vector, related texts share
/// components (each token increments the component its hash selects), and the
/// declared [`crate::memory::MemoryEmbeddingRef`] identity is stable — which
/// is exactly what retrieval and index-drift tests need. The version is
/// configurable so a test can prove an embedder upgrade makes old vectors
/// non-candidates.
#[derive(Debug, Clone)]
pub struct DeterministicEmbedder {
    dimensions: u32,
    version: AgentRevisionNumber,
}

impl DeterministicEmbedder {
    /// The model name the embedder declares.
    pub const MODEL: &'static str = "deterministic-test-embedder";

    /// An embedder with eight dimensions at the initial version.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            dimensions: 8,
            version: AgentRevisionNumber::INITIAL,
        }
    }

    /// Uses an explicit dimension count (clamped to at least one).
    #[must_use]
    pub const fn with_dimensions(mut self, dimensions: u32) -> Self {
        self.dimensions = if dimensions == 0 { 1 } else { dimensions };
        self
    }

    /// Uses an explicit pipeline version.
    #[must_use]
    pub const fn with_version(mut self, version: AgentRevisionNumber) -> Self {
        self.version = version;
        self
    }
}

impl Default for DeterministicEmbedder {
    fn default() -> Self {
        Self::new()
    }
}

impl crate::retrieval::AgentMemoryEmbedder for DeterministicEmbedder {
    fn embedding_ref(&self) -> crate::memory::MemoryEmbeddingRef {
        crate::memory::MemoryEmbeddingRef {
            model: Self::MODEL.to_string(),
            dimensions: self.dimensions,
            version: self.version,
        }
    }

    fn embed<'a>(&'a self, text: &'a str) -> crate::memory::MemoryFuture<'a, Vec<f32>> {
        Box::pin(async move {
            let mut vector = vec![0f32; self.dimensions as usize];
            for token in text
                .split(|character: char| !character.is_alphanumeric())
                .filter(|token| !token.is_empty())
            {
                // FNV-1a over the lowercased token selects the component;
                // deterministic across platforms and runs.
                let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
                for byte in token.to_lowercase().bytes() {
                    hash ^= u64::from(byte);
                    hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
                }
                let index = (hash % u64::from(self.dimensions)) as usize;
                vector[index] += 1.0;
            }
            Ok(vector)
        })
    }
}

/// A scripted
/// [`AgentPrivateMemoryRetriever`](crate::retrieval::AgentPrivateMemoryRetriever)
/// for tests: queued outcomes, answered in order, with every call recorded.
///
/// An exhausted script answers an empty outcome, so a test scripts only the
/// calls it is proving. Clones share the queue and the counters, matching the
/// testkit's shared-slot convention.
#[derive(Clone)]
pub struct ScriptedPrivateMemoryRetriever {
    outcomes: Arc<Mutex<VecDeque<Result<crate::retrieval::MemoryRetrievalOutcome, MemoryError>>>>,
    calls: Arc<AtomicUsize>,
    version: AgentRevisionNumber,
}

impl Default for ScriptedPrivateMemoryRetriever {
    fn default() -> Self {
        Self::new()
    }
}

impl ScriptedPrivateMemoryRetriever {
    /// A retriever with an empty script at the initial version.
    #[must_use]
    pub fn new() -> Self {
        Self {
            outcomes: Arc::new(Mutex::new(VecDeque::new())),
            calls: Arc::new(AtomicUsize::new(0)),
            version: AgentRevisionNumber::INITIAL,
        }
    }

    /// Queues one retrieval outcome.
    #[must_use]
    pub fn with_outcome(self, outcome: crate::retrieval::MemoryRetrievalOutcome) -> Self {
        self.outcomes
            .lock()
            .expect("the scripted retriever should not be poisoned")
            .push_back(Ok(outcome));
        self
    }

    /// Queues one retrieval failure — the outage the assembly path degrades
    /// on.
    #[must_use]
    pub fn with_error(self, error: MemoryError) -> Self {
        self.outcomes
            .lock()
            .expect("the scripted retriever should not be poisoned")
            .push_back(Err(error));
        self
    }

    /// Uses an explicit retriever version, so a test can bump it and prove a
    /// retried model input does not move.
    #[must_use]
    pub const fn with_retriever_version(mut self, version: AgentRevisionNumber) -> Self {
        self.version = version;
        self
    }

    /// How many retrievals have been asked of this retriever.
    #[must_use]
    pub fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl crate::retrieval::AgentPrivateMemoryRetriever for ScriptedPrivateMemoryRetriever {
    fn backend_name(&self) -> &'static str {
        "scripted"
    }

    fn retriever_version(&self) -> AgentRevisionNumber {
        self.version
    }

    fn retrieve<'a>(
        &'a self,
        _scope: &'a crate::identity::AgentScope,
        _query: &'a crate::retrieval::MemoryRetrievalQuery,
        _now: rakka_agent_workflow::AgentTimestampMillis,
    ) -> crate::memory::MemoryFuture<'a, crate::retrieval::MemoryRetrievalOutcome> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.outcomes
                .lock()
                .expect("the scripted retriever should not be poisoned")
                .pop_front()
                .unwrap_or_else(|| {
                    Ok(crate::retrieval::MemoryRetrievalOutcome {
                        memories: Vec::new(),
                        index_watermark: None,
                    })
                })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::definition::AgentToolId;
    use crate::effect::AgentRunEffectRequest;
    use crate::identity::{AgentId, AgentRunId, TenantId};
    use crate::model::AgentToolCallId;

    fn run_scope() -> AgentRunScope {
        AgentRunScope::new(
            TenantId::new("acme"),
            AgentId::new("support").expect("the agent id is valid"),
            AgentRunId::new("t-gen-1").expect("the run id is valid"),
        )
        .expect("the scope is valid")
    }

    fn model_request_for(turn: u64) -> AgentModelRequest {
        AgentModelRequest::new(
            AgentContextSnapshotRef::for_turn(&run_scope(), turn).expect("the reference derives"),
            turn,
        )
    }

    #[tokio::test]
    async fn the_deterministic_adapter_conditions_a_turn_on_its_turn_number() {
        let first = AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION).with_text("first");
        let second = AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION).with_text("second");
        let adapter = DeterministicModelAdapter::new()
            .with_turn_for(2, second)
            .with_turn(first);

        // A call for turn 2 gets the turn scripted for that number regardless of
        // the ordered script; a call for turn 1 falls through to the ordered
        // script. The trait's async `call` produces the same turn as `produce`.
        let turn_two = adapter
            .call(&model_request_for(2))
            .await
            .expect("the turn is produced");
        assert_eq!(turn_two.text.as_deref(), Some("second"));
        assert_eq!(
            adapter.produce(&model_request_for(1)).text.as_deref(),
            Some("first")
        );
        assert_eq!(adapter.calls(), 2);
    }

    fn tool_effect(scope: &AgentRunScope, slot: usize, call_id: &str) -> AgentRunEffect {
        let call = AgentToolCallRequest::new(
            AgentToolCallId::new(call_id).expect("the call id is valid"),
            AgentToolId::new("search").expect("the tool id is valid"),
            serde_json::json!({ "query": slot }),
        )
        .expect("the call is bounded");
        AgentRunEffect::new(
            scope,
            1,
            slot,
            AgentRunEffectRequest::Tool {
                call: Box::new(call),
            },
            &crate::effect::AgentEffectSpec::non_idempotent(),
            crate::definition::AgentRevisionNumber::INITIAL,
            AgentTimestampMillis::new(1),
        )
        .expect("the effect derives")
    }

    #[tokio::test]
    async fn a_tool_answer_is_memoized_on_the_effect_id() {
        let scope = AgentRunScope::new(
            TenantId::new("acme"),
            AgentId::new("support").expect("the agent id is valid"),
            AgentRunId::new("t-gen-1").expect("the run id is valid"),
        )
        .expect("the scope is valid");
        let effect = tool_effect(&scope, 1, "call-1");

        let dispatcher = ScriptedDispatcher::new();
        let first = dispatcher.answer(&effect).await;

        // The script changes under the dispatcher — the tool now fails — but
        // re-invoking the same effect returns the answer already given: that is
        // what the effect's idempotency key means, and what keeps a recovery
        // test deterministic.
        let dispatcher = dispatcher.with_tool_failure("search", "tool-unavailable", "down");
        assert_eq!(first, dispatcher.answer(&effect).await);

        // A different effect is a different invocation and sees the new script.
        let sibling = tool_effect(&scope, 2, "call-2");
        assert_eq!(
            dispatcher.answer(&sibling).await.failure_code(),
            Some("tool-unavailable")
        );

        // Re-invocations are still counted: each really is another call.
        assert_eq!(dispatcher.tool_calls(), 3);
    }

    fn model_effect(scope: &AgentRunScope, slot: usize) -> AgentRunEffect {
        let context = AgentContextSnapshotRef::for_turn(scope, 1).expect("the reference derives");
        AgentRunEffect::new(
            scope,
            1,
            slot,
            AgentRunEffectRequest::Model {
                context,
                profile: None,
            },
            &crate::effect::AgentEffectSpec::read_only(),
            crate::definition::AgentRevisionNumber::INITIAL,
            AgentTimestampMillis::new(1),
        )
        .expect("the effect derives")
    }

    #[test]
    fn a_dropped_unpolled_call_consumes_no_scripted_turn() {
        let adapter = DeterministicModelAdapter::new()
            .with_turn(AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION).with_text("kept"));

        // A future built and dropped unpolled — a timeout, a cancelled race —
        // performs no production, exactly as the Rig adapter performs no
        // provider call for a future never polled.
        drop(adapter.call(&model_request_for(1)));
        assert_eq!(adapter.calls(), 0);

        assert_eq!(
            adapter.produce(&model_request_for(1)).text.as_deref(),
            Some("kept"),
            "the scripted turn is still there for the call that runs"
        );
    }

    #[tokio::test]
    async fn an_unboundable_scripted_turn_surfaces_as_a_failed_effect() {
        use crate::model::AGENT_MODEL_TEXT_MAX_LENGTH;

        let oversized = AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
            .with_text("x".repeat(AGENT_MODEL_TEXT_MAX_LENGTH + 1));
        let dispatcher =
            ScriptedDispatcher::with_adapter(DeterministicModelAdapter::new().with_turn(oversized));

        // The turn is refused where the outcome is formed, as a failed effect
        // the run records and winds down on — not as a result command the
        // entity would refuse, which would leave the effect outstanding forever.
        let outcome = dispatcher.answer(&model_effect(&run_scope(), 0)).await;
        assert_eq!(outcome.failure_code(), Some("model-text-too-long"));
    }

    #[tokio::test]
    async fn a_later_dispatch_generation_reaches_the_adapter_again() {
        let first = AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION).with_text("first");
        let second = AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION).with_text("second");
        let dispatcher = ScriptedDispatcher::with_adapter(
            DeterministicModelAdapter::new()
                .with_turn(first)
                .with_turn(second),
        );

        let effect = model_effect(&run_scope(), 0);
        let answered = dispatcher.answer(&effect).await;
        // The same generation replays the memoized answer without producing.
        assert_eq!(answered, dispatcher.answer(&effect).await);
        assert_eq!(dispatcher.adapter().calls(), 1);

        // A later generation is a new attempt entirely — the retry slice 1.7
        // mints one for — so it reaches the adapter instead of replaying the
        // recorded (possibly failed) answer forever.
        let mut retried = effect;
        retried.generation = retried.generation.next();
        let retried_answer = dispatcher.answer(&retried).await;
        assert_ne!(answered, retried_answer);
        assert_eq!(dispatcher.adapter().calls(), 2);
    }
}
