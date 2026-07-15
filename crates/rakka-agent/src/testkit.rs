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
//! production uses. Slice 1.6 replaces the script with the deterministic model
//! adapter without changing that path.

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use rakka_agent_workflow::{AgentCorrelationId, AgentTimestampMillis};
use rakka_persistence::DurableStateStore;
use serde::{Deserialize, Serialize};

use crate::agent::AgentEntityState;
use crate::choreography::{
    AgentChoreographyError, AgentChoreographyResult, AgentEntityAddress, AgentEntityClass,
    AgentExchangeDeliveryError, AgentExchangeDeliveryFuture, AgentExchangeEnvelope,
    AgentExchangeHost, AgentExchangeJournal, AgentExchangeKind, AgentExchangeParticipant,
    AgentExchangePayload, AgentExchangeResult, AgentExchangeRouter, AgentExchangeState,
    AgentExchangeTransition, AgentExchangeTransport,
};
use crate::effect::{
    AgentRunEffect, AgentRunEffectOutcome, AgentRunEffectRequest, AgentRunEffectSink,
    AgentRunEffectStatus,
};
use crate::identity::{AgentOperationId, AgentRunScope};
use crate::loop_runtime::AgentLoopState;
use crate::model::{AgentModelTurn, AgentToolCallRequest};
use crate::run::{AgentRunEntityCommand, AgentRunEntityStore, AgentRunError, AgentRunState};
use crate::schema::{AgentSchemaError, AgentSchemaPolicy};
use crate::task::{
    AgentRunAcceptance, AgentRunAssignment, AgentTaskContent, AgentTaskEntityStore, AgentTaskError,
    AgentTaskHistoryStore, AgentTaskState, AGENT_RUN_ACCEPTANCE_PAYLOAD_TYPE,
    AGENT_RUN_ASSIGNMENT_PAYLOAD_TYPE,
};

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
                AgentRunEntityStore::new(scope, self.store.clone(), self.effects.clone());

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

/// The scripted stand-in for the dispatcher of slice 1.7.
///
/// It plays exactly the dispatcher's role and no other
/// ([specification 9.5](../../../docs/plans/rakka-agent/spec.md)): it reads the
/// effects a run durably committed and dispatched, performs the bounded work —
/// here, reading it off a script instead of calling a provider — and returns each
/// outcome as a durable result command through the run entity's own command
/// surface. It never reaches into the run's state, and it never advances the loop
/// itself.
///
/// That is what makes it a faithful stub. The path a scripted turn travels is the
/// path a real one travels, so the recovery this proves is the recovery
/// production gets. Slice 1.6 replaces the script with the deterministic model
/// adapter, and slice 1.7 replaces this driver with the real dispatcher; neither
/// changes the durable path underneath.
///
/// A model call with no script left answers with a turn that proposes nothing and
/// asks for nothing, which lets a test drive a run to its iteration ceiling
/// without scripting every turn.
#[derive(Debug, Clone, Default)]
pub struct ScriptedDispatcher {
    turns: Arc<Mutex<VecDeque<AgentModelTurn>>>,
    answered: Arc<Mutex<BTreeMap<String, AgentModelTurn>>>,
    tools: Arc<Mutex<BTreeMap<String, AgentTaskContent>>>,
    failures: Arc<Mutex<BTreeMap<String, (String, String)>>>,
    model_calls: Arc<AtomicUsize>,
    tool_calls: Arc<AtomicUsize>,
}

impl ScriptedDispatcher {
    /// A dispatcher with an empty script.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Scripts the turn the next model call returns.
    #[must_use]
    pub fn with_turn(self, turn: AgentModelTurn) -> Self {
        self.turns
            .lock()
            .expect("the turn script should not be poisoned")
            .push_back(turn);
        self
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

    /// How many model calls the dispatcher has answered.
    #[must_use]
    pub fn model_calls(&self) -> usize {
        self.model_calls.load(Ordering::SeqCst)
    }

    /// How many tool calls the dispatcher has answered.
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
            let outcome = self.answer(&effect);
            let command = AgentRunEntityCommand::RecordEffectResult {
                operation_id: effect.result_operation_id(&scope)?,
                effect_id: effect.effect_id.clone(),
                outcome: Box::new(outcome),
            };
            entity.apply(command, router, now).await?;
            delivered += 1;
        }
        Ok(delivered)
    }

    /// What this dispatcher returns for one effect.
    ///
    /// The answer is memoized on the effect id, so re-invoking an effect whose
    /// result was lost — because the run's owner died before it could record it —
    /// returns *the same* turn. That is what the effect's idempotency key means
    /// ([specification 11.4](../../../docs/plans/rakka-agent/spec.md)), and it is
    /// what makes a recovery test deterministic: the run resumes against the
    /// answer it would have had, not against a different one the script happened
    /// to hand out next.
    ///
    /// The call *counters* are not memoized, because a re-invocation really is
    /// another call: it is billed again, and slice 1.9 charges it again.
    #[must_use]
    pub fn answer(&self, effect: &AgentRunEffect) -> AgentRunEffectOutcome {
        match &effect.request {
            AgentRunEffectRequest::Model { .. } => {
                self.model_calls.fetch_add(1, Ordering::SeqCst);
                AgentRunEffectOutcome::Model {
                    turn: Box::new(self.answer_model(effect)),
                }
            }
            AgentRunEffectRequest::Tool { call } => {
                self.tool_calls.fetch_add(1, Ordering::SeqCst);
                self.answer_tool(call)
            }
        }
    }

    fn answer_model(&self, effect: &AgentRunEffect) -> AgentModelTurn {
        let key = effect.effect_id.as_str().to_string();
        let mut answered = self
            .answered
            .lock()
            .expect("the answered script should not be poisoned");
        if let Some(turn) = answered.get(&key) {
            return turn.clone();
        }
        let turn = self
            .turns
            .lock()
            .expect("the turn script should not be poisoned")
            .pop_front()
            .unwrap_or_else(|| {
                AgentModelTurn::new(crate::loop_runtime::CURRENT_AGENT_LOOP_ADAPTER_VERSION)
            });
        answered.insert(key, turn.clone());
        turn
    }

    fn answer_tool(&self, call: &AgentToolCallRequest) -> AgentRunEffectOutcome {
        let tool = call.tool.to_string();
        if let Some((code, message)) = self
            .failures
            .lock()
            .expect("the failure script should not be poisoned")
            .get(&tool)
            .cloned()
        {
            return AgentRunEffectOutcome::Failed { code, message };
        }

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

fn dispatched_effects(state: &AgentLoopState) -> Vec<AgentRunEffect> {
    state
        .effects()
        .iter()
        .filter(|effect| effect.status == AgentRunEffectStatus::Dispatched)
        .cloned()
        .collect()
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

/// A durable state store whose owner dies at the *n*-th write.
///
/// Nothing else about it is special, and that is the point: whatever it has
/// already committed is exactly what a real owner finds on the next activation,
/// so re-materializing an entity over it is a faithful restart.
///
/// Slice 1.14 extends these crash points across the rest of the M1 suite.
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
    pub fn crash_at(&self, nth: usize, point: CrashPoint) {
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
        self.inner.delete(persistence_id, expected_revision)
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
