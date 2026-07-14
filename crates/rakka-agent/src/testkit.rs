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

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use rakka_agent_workflow::{AgentCorrelationId, AgentTimestampMillis};
use rakka_persistence::DurableStateStore;
use serde::{Deserialize, Serialize};

use crate::choreography::{
    AgentChoreographyError, AgentChoreographyResult, AgentEntityAddress,
    AgentExchangeDeliveryError, AgentExchangeDeliveryFuture, AgentExchangeEnvelope,
    AgentExchangeHost, AgentExchangeJournal, AgentExchangeKind, AgentExchangeParticipant,
    AgentExchangePayload, AgentExchangeResult, AgentExchangeState, AgentExchangeTransition,
    AgentExchangeTransport,
};
use crate::identity::AgentOperationId;
use crate::schema::{AgentSchemaError, AgentSchemaPolicy};

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
