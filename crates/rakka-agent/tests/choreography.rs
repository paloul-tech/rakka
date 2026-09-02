//! Inter-entity choreography: one logical transition per operation id.
//!
//! Specification: sections 9.8 and 6.10; scenario 58 of section 18, with
//! scenario 59's rejection clause and scenario 61's replay clause driven
//! through the same failure windows. Every exchange this phase implements is driven across each of the four failure
//! windows the specification names — initiator loss before send, receiver loss
//! after acceptance, reply loss, and duplicate delivery — and each one must
//! converge on exactly one transition per side.
//!
//! The failure-window table these tests satisfy is the doc section of
//! `rakka_agent::choreography`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use rakka_agent::testkit::{
    ChoreographyProbe, ChoreographyProbeState, ExchangeFault, InProcessExchangeTransport,
    ProbeAssignment, ProbeCreation, ProbeLedgerEntry, ProbeProposal, PROBE_ASSIGNMENT_TYPE,
    PROBE_CREATION_TYPE, PROBE_LEDGER_TYPE, PROBE_PROPOSAL_TYPE,
};
use rakka_agent::{
    drive_pending_exchanges, AgentEntityAddress, AgentExchangeEnvelope, AgentExchangeHost,
    AgentExchangeInitiation, AgentExchangeKind, AgentExchangePayload, AgentExchangeReply,
    AgentExchangeSettlement, AgentExchangeState, AgentId, AgentOperationId, AgentOperationKind,
    AgentRunId, AgentRunScope, AgentTaskId, AgentTaskScope, TenantId, AGENT_EXCHANGE_LOG_CAPACITY,
    AGENT_EXCHANGE_PAYLOAD_MAX_BYTES, AGENT_EXCHANGE_PENDING_CAPACITY,
};
use rakka_agent_workflow::AgentTimestampMillis;
use rakka_persistence::InMemoryDurableStateStore;

type Store = InMemoryDurableStateStore<ChoreographyProbeState>;
type Host = AgentExchangeHost<ChoreographyProbe, Store>;

const TENANT: &str = "acme";

fn tenant() -> TenantId {
    TenantId::new(TENANT)
}

/// The task entity of the canonical flow.
fn task_address() -> AgentEntityAddress {
    AgentEntityAddress::Task(
        AgentTaskScope::new(
            tenant(),
            AgentTaskId::new("ticket-1").expect("task id should be valid"),
        )
        .expect("task scope should be valid"),
    )
}

/// The run entity of the canonical flow.
fn run_address() -> AgentEntityAddress {
    AgentEntityAddress::Run(
        AgentRunScope::new(
            tenant(),
            AgentId::new("support-agent").expect("agent id should be valid"),
            AgentRunId::new("run-1").expect("run id should be valid"),
        )
        .expect("run scope should be valid"),
    )
}

fn operation(label: &str) -> AgentOperationId {
    AgentOperationId::new(AgentOperationKind::Command, [TENANT, label])
        .expect("operation id should be derivable")
}

/// One durable store, one clock, and one transport, shared by the participants.
///
/// Hosts are created on demand and thrown away, because that is what a sharded
/// entity does: it is materialized on its owner, transitions, and passivates.
/// Nothing but the store survives between them.
struct Fixture {
    store: Store,
    clock: Arc<AtomicU64>,
    transport: InProcessExchangeTransport<ChoreographyProbe, Store>,
}

impl Fixture {
    fn new() -> Self {
        let store = Store::new();
        let clock = Arc::new(AtomicU64::new(1));
        let transport =
            InProcessExchangeTransport::new(ChoreographyProbe, store.clone(), clock.clone());
        Self {
            store,
            clock,
            transport,
        }
    }

    fn now(&self) -> AgentTimestampMillis {
        AgentTimestampMillis::new(self.clock.fetch_add(1, Ordering::SeqCst))
    }

    /// Materializes one participant from durable state alone.
    async fn host(&self, address: AgentEntityAddress) -> Host {
        let mut host = AgentExchangeHost::new(address, ChoreographyProbe, self.store.clone());
        host.recover(self.now())
            .await
            .expect("the participant should recover");
        host
    }

    /// Reads one participant's durable state without holding a writer.
    async fn state(&self, address: AgentEntityAddress) -> ChoreographyProbeState {
        self.host(address)
            .await
            .state()
            .expect("the participant should be recovered")
            .clone()
    }

    fn envelope(&self, kind: AgentExchangeKind, label: &str) -> AgentExchangeEnvelope {
        ChoreographyProbe::envelope(
            kind,
            operation(label),
            run_address(),
            task_address(),
            self.now(),
        )
        .expect("the envelope should be valid")
    }
}

/// Records one exchange as an entry-point transition.
async fn initiate(
    host: &mut Host,
    envelope: AgentExchangeEnvelope,
    now: AgentTimestampMillis,
) -> AgentExchangeInitiation {
    host.initiate(now, |_state| Ok(vec![envelope]))
        .await
        .expect("the initiating transition should commit")
        .pop()
        .expect("one exchange was recorded")
}

fn envelope_label(kind: AgentExchangeKind) -> String {
    format!("exchange-{}", kind.as_label())
}

#[tokio::test]
async fn replaying_every_exchange_produces_one_logical_transition_per_operation_id() {
    // Scenario 58. The probe is the same participant on both ends, so a single
    // directed pair exercises every exchange kind; which real entity initiates
    // which exchange is the business of slices 1.4, 1.5, and 1.9.
    let fx = Fixture::new();
    let mut initiator = fx.host(run_address()).await;

    for kind in AgentExchangeKind::ALL {
        let envelope = fx.envelope(kind, &envelope_label(kind));

        let initiation = initiate(&mut initiator, envelope.clone(), fx.now()).await;
        assert_eq!(initiation, AgentExchangeInitiation::Recorded, "{kind}");

        let report = drive_pending_exchanges(&mut initiator, &fx.transport, fx.now())
            .await
            .expect("the courier should run");
        assert_eq!(report.settled, 1, "{kind} should settle once");

        // The initiator is replayed: the same transition runs again, and the
        // courier re-drives. Neither may produce a second logical transition.
        let initiation = initiate(&mut initiator, envelope.clone(), fx.now()).await;
        assert!(
            matches!(initiation, AgentExchangeInitiation::AlreadySettled { .. }),
            "{kind}: a replayed initiation must resolve to the original result, got {initiation:?}"
        );

        let report = drive_pending_exchanges(&mut initiator, &fx.transport, fx.now())
            .await
            .expect("the courier should run");
        assert_eq!(
            report.outstanding, 0,
            "{kind}: a settled exchange is not owed again"
        );
    }

    let receiver = fx.state(task_address()).await;
    let initiator = initiator.state().expect("the initiator is recovered");
    for kind in AgentExchangeKind::ALL {
        assert_eq!(
            receiver.applied_count(kind),
            1,
            "{kind}: exactly one transition on the receiver"
        );
        assert_eq!(
            initiator.settled_count(kind),
            1,
            "{kind}: exactly one settlement on the initiator"
        );
    }

    // And the receiver's domain state moved exactly once, not twice.
    assert!(receiver.is_created());
    assert_eq!(receiver.assignment_generation(), 1);
    // One allocation (+10), one settlement (-10), one return (+10).
    assert_eq!(receiver.balance(), 10);
}

#[tokio::test]
async fn an_initiator_lost_before_sending_re_drives_the_same_operation_id() {
    // Window 1: the initiating transition committed, and then the entity was
    // lost before the courier ever ran.
    let fx = Fixture::new();
    let mut initiator = fx.host(run_address()).await;

    let mut expected = Vec::new();
    for kind in AgentExchangeKind::ALL {
        let envelope = fx.envelope(kind, &envelope_label(kind));
        expected.push(envelope.operation_id().clone());
        initiate(&mut initiator, envelope, fx.now()).await;
    }
    assert_eq!(
        fx.transport.deliveries(),
        0,
        "nothing was sent before the initiator was lost"
    );
    drop(initiator);

    // The entity is re-materialized on another shard owner. It knows what it
    // owes because the exchanges were committed with the transitions that owed
    // them, and it re-drives them under the operation ids it first minted — it
    // never mints new ones.
    let mut recovered = fx.host(run_address()).await;
    let outstanding: Vec<AgentOperationId> = recovered
        .outstanding()
        .expect("the recovered entity knows what it owes")
        .iter()
        .map(|envelope| envelope.operation_id().clone())
        .collect();
    assert_eq!(outstanding, expected);

    let report = drive_pending_exchanges(&mut recovered, &fx.transport, fx.now())
        .await
        .expect("the courier should run");
    assert_eq!(report.settled, AgentExchangeKind::ALL.len());

    let receiver = fx.state(task_address()).await;
    for kind in AgentExchangeKind::ALL {
        assert_eq!(receiver.applied_count(kind), 1, "{kind}");
    }
}

#[tokio::test]
async fn a_receiver_lost_after_accepting_returns_its_original_result() {
    // Window 2: the receiver durably accepted and transitioned, and was then
    // lost before its reply left. Re-delivering the same operation id must
    // return the *original* result rather than transition again.
    let fx = Fixture::new();

    for kind in AgentExchangeKind::ALL {
        let envelope = fx.envelope(kind, &envelope_label(kind));

        let mut receiver = fx.host(task_address()).await;
        let first = receiver
            .accept(&envelope, fx.now())
            .await
            .expect("the receiver should accept");
        assert!(!first.is_replayed(), "{kind}");
        drop(receiver);

        // Lost, and re-materialized from durable state alone.
        let mut recovered = fx.host(task_address()).await;
        let second = recovered
            .accept(&envelope, fx.now())
            .await
            .expect("the receiver should answer the replay");

        assert!(
            second.is_replayed(),
            "{kind}: the replay must be recognized"
        );
        assert_eq!(
            second.result(),
            first.result(),
            "{kind}: a replay must return the original logical result"
        );
        assert_eq!(
            recovered.state().expect("recovered").applied_count(kind),
            1,
            "{kind}: the replay must not transition a second time"
        );
    }
}

#[tokio::test]
async fn a_lost_reply_settles_once_on_re_drive() {
    // Window 3: the receiver applied the exchange, and the reply was lost. The
    // initiator cannot tell this apart from an envelope that never arrived, and
    // must not try: it keeps the exchange outstanding and re-drives it.
    let fx = Fixture::new();
    let mut initiator = fx.host(run_address()).await;

    for kind in AgentExchangeKind::ALL {
        let envelope = fx.envelope(kind, &envelope_label(kind));
        let operation_id = envelope.operation_id().clone();
        initiate(&mut initiator, envelope, fx.now()).await;

        fx.transport.inject(ExchangeFault::LoseReply);
        let report = drive_pending_exchanges(&mut initiator, &fx.transport, fx.now())
            .await
            .expect("the courier should run");
        assert_eq!(report.failed, 1, "{kind}");
        assert_eq!(report.settled, 0, "{kind}");

        // The exchange is still owed, under the same operation id, and the
        // failed attempt is recorded against it.
        let pending = initiator
            .state()
            .expect("recovered")
            .journal()
            .pending_exchange(&operation_id)
            .expect("the exchange is still outstanding")
            .clone();
        assert_eq!(pending.attempts(), 1, "{kind}");
        assert_eq!(pending.last_failure_code(), Some("injected-lost-reply"));

        let report = drive_pending_exchanges(&mut initiator, &fx.transport, fx.now())
            .await
            .expect("the courier should run");
        assert_eq!(report.settled, 1, "{kind}: the re-drive settles it");
    }

    let receiver = fx.state(task_address()).await;
    let initiator = initiator.state().expect("recovered");
    for kind in AgentExchangeKind::ALL {
        assert_eq!(
            receiver.applied_count(kind),
            1,
            "{kind}: the receiver transitioned once, though it was asked twice"
        );
        assert_eq!(initiator.settled_count(kind), 1, "{kind}");
    }
    assert_eq!(
        fx.transport.acceptances(),
        2 * AgentExchangeKind::ALL.len(),
        "every exchange reached the receiver's accept path twice"
    );
}

#[tokio::test]
async fn duplicate_delivery_produces_one_logical_transition() {
    // Window 4: the same envelope is delivered twice. The receiver deduplicates
    // and the second delivery returns the first delivery's result.
    let fx = Fixture::new();
    let mut initiator = fx.host(run_address()).await;

    for kind in AgentExchangeKind::ALL {
        let envelope = fx.envelope(kind, &envelope_label(kind));
        initiate(&mut initiator, envelope, fx.now()).await;

        fx.transport.inject(ExchangeFault::DeliverTwice);
        let report = drive_pending_exchanges(&mut initiator, &fx.transport, fx.now())
            .await
            .expect("the courier should run");
        assert_eq!(report.settled, 1, "{kind}: one settlement");
    }

    let receiver = fx.state(task_address()).await;
    for kind in AgentExchangeKind::ALL {
        assert_eq!(
            receiver.applied_count(kind),
            1,
            "{kind}: one transition, though the envelope arrived twice"
        );
    }
    assert_eq!(fx.transport.acceptances(), 2 * AgentExchangeKind::ALL.len());
    assert_eq!(receiver.balance(), 10, "no ledger entry was applied twice");
}

#[tokio::test]
async fn a_lost_rejection_is_recovered_not_dropped() {
    // Scenario 59's rejection clause. A rejection is a durable decision, not a
    // failure, so losing the reply that carried it must not lose the decision
    // and must not cause a second validation.
    let fx = Fixture::new();
    let mut initiator = fx.host(run_address()).await;

    // The task must exist before it can validate a result.
    let creation = fx.envelope(AgentExchangeKind::Creation, "creation");
    initiate(&mut initiator, creation, fx.now()).await;
    drive_pending_exchanges(&mut initiator, &fx.transport, fx.now())
        .await
        .expect("the courier should run");

    // A proposal the receiver's deterministic result rule refuses.
    let proposal = ChoreographyProbe::envelope_with(
        AgentExchangeKind::ResultProposal,
        operation("bad-proposal"),
        run_address(),
        task_address(),
        fx.now(),
        |_| AgentExchangePayload::encode(PROBE_PROPOSAL_TYPE, &ProbeProposal { valid: false }),
    )
    .expect("the envelope should be valid");
    initiate(&mut initiator, proposal, fx.now()).await;

    // The receiver decides — and the answer is lost on the way home.
    fx.transport.inject(ExchangeFault::LoseReply);
    let report = drive_pending_exchanges(&mut initiator, &fx.transport, fx.now())
        .await
        .expect("the courier should run");
    assert_eq!(report.failed, 1);

    // The re-drive recovers the original rejection rather than validating again.
    let report = drive_pending_exchanges(&mut initiator, &fx.transport, fx.now())
        .await
        .expect("the courier should run");
    assert_eq!(report.settled, 1);

    let receiver = fx.state(task_address()).await;
    assert_eq!(
        receiver.applied_count(AgentExchangeKind::ResultProposal),
        1,
        "the proposal must be validated exactly once"
    );

    let decisions = initiator.state().expect("recovered").decisions();
    let rejection = decisions
        .iter()
        .find(|decision| decision.kind == AgentExchangeKind::ResultProposal)
        .expect("the rejection reached the initiator");
    assert!(!rejection.accepted);
    assert_eq!(
        rejection.rejection_code.as_deref(),
        Some("result-rule-violation"),
        "the rejection was recovered, not dropped"
    );
}

#[tokio::test]
async fn a_replayed_ledger_exchange_never_double_debits_or_double_credits() {
    // Scenario 61's replay clause. The probe's ledger is a counter whose only
    // job is to make a double-apply visible; slice 1.9 owns the escrow itself.
    //
    // The parent debits inside its own creating transition — before the
    // allocation is ever sent — so that transition has to guard itself against
    // replay. The child credits on acceptance, where the substrate guards it.
    let fx = Fixture::new();
    let allocation = ChoreographyProbe::envelope_with(
        AgentExchangeKind::BudgetAllocation,
        operation("allocation-1"),
        task_address(),
        run_address(),
        fx.now(),
        |_| AgentExchangePayload::encode(PROBE_LEDGER_TYPE, &ProbeLedgerEntry { amount: 25 }),
    )
    .expect("the envelope should be valid");

    {
        let mut parent = fx.host(task_address()).await;

        // The entry-point transition is replayed three times, as a crashing
        // ingress would replay it.
        for _ in 0..3 {
            let envelope = allocation.clone();
            parent
                .initiate(fx.now(), move |state| {
                    if state.journal().has_initiated(envelope.operation_id()) {
                        return Ok(Vec::new());
                    }
                    state.debit(25);
                    Ok(vec![envelope])
                })
                .await
                .expect("the allocating transition should commit");
        }

        // And delivery goes wrong in both directions before it goes right.
        fx.transport.inject(ExchangeFault::DeliverTwice);
        fx.transport.inject(ExchangeFault::LoseReply);
        for _ in 0..3 {
            drive_pending_exchanges(&mut parent, &fx.transport, fx.now())
                .await
                .expect("the courier should run");
        }
        assert_eq!(parent.outstanding().expect("recovered").len(), 0);
    }

    // The parent passivates while the child works. The child returns what it did
    // not consume, debiting its own ledger in the transition that owes the
    // return.
    let mut child = fx.host(run_address()).await;
    let ret = ChoreographyProbe::envelope_with(
        AgentExchangeKind::BudgetReturn,
        operation("return-1"),
        run_address(),
        task_address(),
        fx.now(),
        |_| AgentExchangePayload::encode(PROBE_LEDGER_TYPE, &ProbeLedgerEntry { amount: 10 }),
    )
    .expect("the envelope should be valid");

    for _ in 0..2 {
        let envelope = ret.clone();
        child
            .initiate(fx.now(), move |state| {
                if state.journal().has_initiated(envelope.operation_id()) {
                    return Ok(Vec::new());
                }
                state.debit(10);
                Ok(vec![envelope])
            })
            .await
            .expect("the returning transition should commit");
    }
    fx.transport.inject(ExchangeFault::DeliverTwice);
    for _ in 0..2 {
        drive_pending_exchanges(&mut child, &fx.transport, fx.now())
            .await
            .expect("the courier should run");
    }
    drop(child);

    let parent = fx.state(task_address()).await;
    let child = fx.state(run_address()).await;

    assert_eq!(
        parent.balance(),
        -25 + 10,
        "the parent debited its allocation once and was credited its return once"
    );
    assert_eq!(
        child.balance(),
        25 - 10,
        "the child was credited its allocation once and debited its return once"
    );
    assert_eq!(parent.applied_count(AgentExchangeKind::BudgetReturn), 1);
    assert_eq!(child.applied_count(AgentExchangeKind::BudgetAllocation), 1);
}

#[tokio::test]
async fn a_delivery_failure_leaves_the_exchange_outstanding() {
    // A transport failure is never evidence that the receiver did not apply the
    // exchange, so the only safe response is to keep owing it.
    let fx = Fixture::new();
    let mut initiator = fx.host(run_address()).await;

    let envelope = fx.envelope(AgentExchangeKind::Creation, "creation");
    let operation_id = envelope.operation_id().clone();
    initiate(&mut initiator, envelope, fx.now()).await;

    fx.transport.inject(ExchangeFault::LoseEnvelope);
    let report = drive_pending_exchanges(&mut initiator, &fx.transport, fx.now())
        .await
        .expect("the courier should run");
    assert_eq!(report.failed, 1);
    assert_eq!(
        fx.transport.acceptances(),
        0,
        "nothing reached the receiver"
    );

    // Still owed, still the same operation id, and the attempt is on the record.
    let pending = initiator
        .state()
        .expect("recovered")
        .journal()
        .pending_exchange(&operation_id)
        .expect("the exchange is still outstanding")
        .clone();
    assert_eq!(pending.attempts(), 1);
    assert_eq!(pending.last_failure_code(), Some("injected-lost-envelope"));

    let report = drive_pending_exchanges(&mut initiator, &fx.transport, fx.now())
        .await
        .expect("the courier should run");
    assert_eq!(report.settled, 1);
    assert!(fx.state(task_address()).await.is_created());
}

#[tokio::test]
async fn a_structurally_undeliverable_envelope_costs_one_revision_not_one_per_sweep() {
    // `exchange-no-route` is the delivery failure whose answer never moves:
    // for as long as the deployment hosts no route for the target class,
    // every sweep gets the identical error, and nothing classifies a delivery
    // failure the way the settle rules classify a refusal, so no ceiling ever
    // ends it. A terminal task naming a governing team in a deployment that
    // wires no `Team` route owes its terminal notice exactly this way.
    //
    // The exchange must stay outstanding — a route can appear on the next
    // deploy, and delivering is the only thing that discovers it — so the
    // cost has to be bounded some other way: an unchanged failure code
    // persists nothing, the `record_unsettleable_refusal` rule.
    let fx = Fixture::new();
    let mut initiator = fx.host(run_address()).await;

    let envelope = fx.envelope(AgentExchangeKind::Creation, "creation");
    let operation_id = envelope.operation_id().clone();
    initiate(&mut initiator, envelope, fx.now()).await;

    // A router with no routes at all: the target class is simply unhosted.
    let unrouted = rakka_agent::AgentExchangeRouter::new();
    let mut revisions = Vec::new();
    const SWEEPS: usize = 5;
    for sweep in 0..SWEEPS {
        let report = drive_pending_exchanges(&mut initiator, &unrouted, fx.now())
            .await
            .expect("one undeliverable envelope does not fail the pass");
        assert_eq!(report.failed, 1, "sweep {sweep}");
        assert_eq!(report.settled, 0, "sweep {sweep}");
        revisions.push(revision_of(&fx, run_address()).await);
    }

    // Legible: the code is on the record and every sweep counts the failure,
    // so a standing wedge is alertable as a rate.
    let pending = initiator
        .state()
        .expect("recovered")
        .journal()
        .pending_exchange(&operation_id)
        .expect("the exchange is still outstanding")
        .clone();
    assert_eq!(pending.last_failure_code(), Some("exchange-no-route"));
    assert_eq!(
        pending.attempts(),
        1,
        "an unchanged delivery failure writes nothing further"
    );

    // And free: only the first sweep moved the durable revision.
    assert!(
        revisions.windows(2).all(|pair| pair[0] == pair[1]),
        "standing undeliverable is not also a standing write: {revisions:?}"
    );
}

/// The durable revision one participant's record currently stands at.
async fn revision_of(fx: &Fixture, address: AgentEntityAddress) -> rakka_persistence::Revision {
    use rakka_persistence::DurableStateStore;

    DurableStateStore::load(&fx.store, &address.persistence_id())
        .await
        .expect("the record loads")
        .expect("the participant has durable state")
        .revision
}

#[tokio::test]
async fn a_replay_older_than_the_deduplication_window_is_refused_by_the_domain_fence() {
    // Durable state is bounded, so the journal's deduplication ring is bounded
    // too. A replay that has aged out of it reaches the participant's `apply` —
    // which is why `apply` must be fenced by the domain's own state.
    let fx = Fixture::new();
    let mut receiver = fx.host(task_address()).await;

    let creation = fx.envelope(AgentExchangeKind::Creation, "creation");
    receiver
        .accept(&creation, fx.now())
        .await
        .expect("the creation should apply");

    let assignment = ChoreographyProbe::envelope_with(
        AgentExchangeKind::Assignment,
        operation("assignment-1"),
        run_address(),
        task_address(),
        fx.now(),
        |_| AgentExchangePayload::encode(PROBE_ASSIGNMENT_TYPE, &ProbeAssignment { generation: 7 }),
    )
    .expect("the envelope should be valid");
    let first = receiver
        .accept(&assignment, fx.now())
        .await
        .expect("the assignment should apply");
    assert!(first.result().is_accepted());

    // Age both operations out of the ring.
    for index in 0..AGENT_EXCHANGE_LOG_CAPACITY {
        let filler = ChoreographyProbe::envelope_with(
            AgentExchangeKind::BudgetAllocation,
            operation(&format!("filler-{index}")),
            run_address(),
            task_address(),
            fx.now(),
            |_| AgentExchangePayload::encode(PROBE_LEDGER_TYPE, &ProbeLedgerEntry { amount: 0 }),
        )
        .expect("the envelope should be valid");
        receiver
            .accept(&filler, fx.now())
            .await
            .expect("the filler should apply");
    }
    assert_eq!(
        receiver
            .state()
            .expect("recovered")
            .journal()
            .applied_count(),
        AGENT_EXCHANGE_LOG_CAPACITY
    );

    // The replay is no longer recognized as a duplicate — and is refused anyway,
    // because the task has already passed that assignment generation. The old
    // creation is refused for the same reason: the task exists.
    let replayed_assignment = receiver
        .accept(&assignment, fx.now())
        .await
        .expect("the replay is answered");
    assert!(!replayed_assignment.is_replayed());
    assert_eq!(
        replayed_assignment.result().status().rejection_code(),
        Some("stale-generation")
    );

    let replayed_creation = receiver
        .accept(&creation, fx.now())
        .await
        .expect("the replay is answered");
    assert_eq!(
        replayed_creation.result().status().rejection_code(),
        Some("already-created")
    );

    let state = receiver.state().expect("recovered");
    assert_eq!(
        state.assignment_generation(),
        7,
        "the stale replay did not move the generation"
    );
    assert_eq!(
        state.label(),
        Some("probe"),
        "the stale replay did not re-create the task"
    );
}

#[tokio::test]
async fn an_exchange_delivered_to_the_wrong_entity_is_refused() {
    // An envelope's target is part of the envelope. An entity that is not it
    // fails closed rather than applying another entity's command — a tenant
    // isolation property, not a routing nicety.
    let fx = Fixture::new();
    let mut wrong = fx.host(run_address()).await;
    let envelope = fx.envelope(AgentExchangeKind::Creation, "creation");

    let error = wrong
        .accept(&envelope, fx.now())
        .await
        .expect_err("an entity may not apply an exchange addressed to another");
    assert_eq!(error.code(), "exchange-misrouted");
}

#[tokio::test]
async fn an_exchange_may_not_cross_a_tenant_boundary() {
    let other_tenant = AgentEntityAddress::Task(
        AgentTaskScope::new(
            TenantId::new("initech"),
            AgentTaskId::new("ticket-1").expect("task id should be valid"),
        )
        .expect("task scope should be valid"),
    );

    let error = ChoreographyProbe::envelope(
        AgentExchangeKind::Creation,
        operation("creation"),
        run_address(),
        other_tenant,
        AgentTimestampMillis::new(1),
    )
    .expect_err("an exchange may not address another tenant");
    assert_eq!(error.code(), "exchange-cross-tenant");
}

#[tokio::test]
async fn an_entity_may_not_owe_an_exchange_it_did_not_initiate() {
    // The initiator address is the reply address, so an entity that recorded an
    // exchange it does not own would wait forever for a reply that goes
    // elsewhere.
    let fx = Fixture::new();
    let mut host = fx.host(run_address()).await;

    let foreign = ChoreographyProbe::envelope(
        AgentExchangeKind::Creation,
        operation("creation"),
        task_address(),
        run_address(),
        fx.now(),
    )
    .expect("the envelope should be valid");

    let error = host
        .initiate(fx.now(), |_state| Ok(vec![foreign]))
        .await
        .expect_err("an entity may not owe another entity's exchange");
    assert_eq!(error.code(), "exchange-foreign-initiator");
}

#[tokio::test]
async fn one_operation_id_may_not_name_two_exchanges() {
    // Deduplication keys on the operation id alone, so reusing one for a
    // different exchange would return the wrong logical result. It is an explicit
    // conflict instead.
    let fx = Fixture::new();
    let mut host = fx.host(run_address()).await;

    let creation = fx.envelope(AgentExchangeKind::Creation, "shared");
    initiate(&mut host, creation, fx.now()).await;

    let assignment = fx.envelope(AgentExchangeKind::Assignment, "shared");
    let error = host
        .initiate(fx.now(), |_state| Ok(vec![assignment]))
        .await
        .expect_err("one operation id may not name two exchanges");
    assert_eq!(error.code(), "exchange-operation-conflict");
}

#[tokio::test]
async fn an_entity_may_not_owe_more_exchanges_than_durable_state_holds() {
    // Durable state stays bounded, so the initiating transition fails closed
    // rather than dropping an exchange it would then never re-drive.
    let fx = Fixture::new();
    let mut host = fx.host(run_address()).await;

    for index in 0..AGENT_EXCHANGE_PENDING_CAPACITY {
        let envelope = fx.envelope(
            AgentExchangeKind::BudgetSettlement,
            &format!("owed-{index}"),
        );
        initiate(&mut host, envelope, fx.now()).await;
    }

    let overflow = fx.envelope(AgentExchangeKind::BudgetSettlement, "overflow");
    let error = host
        .initiate(fx.now(), |_state| Ok(vec![overflow]))
        .await
        .expect_err("an entity may not owe an unbounded number of exchanges");
    assert_eq!(error.code(), "exchange-pending-overflow");
}

#[tokio::test]
async fn a_withdrawn_exchange_frees_its_slot_and_is_never_re_driven() {
    // Withdrawal is the escape valve for an exchange whose result the
    // initiator can no longer consume: the slot returns to the bounded pending
    // list, and the envelope is gone from the re-drive list for good.
    let fx = Fixture::new();
    let mut host = fx.host(run_address()).await;

    for index in 0..AGENT_EXCHANGE_PENDING_CAPACITY {
        let envelope = fx.envelope(
            AgentExchangeKind::BudgetSettlement,
            &format!("owed-{index}"),
        );
        initiate(&mut host, envelope, fx.now()).await;
    }
    let withdrawn = operation("owed-0");

    host.initiate(fx.now(), |state| {
        assert!(state.exchange_journal_mut().withdraw(&withdrawn));
        // Withdrawing what is not owed reports it, and changes nothing.
        assert!(!state
            .exchange_journal_mut()
            .withdraw(&operation("never-owed")));
        Ok(Vec::new())
    })
    .await
    .expect("the withdrawal commits");

    let outstanding = host.outstanding().expect("the participant is recovered");
    assert_eq!(outstanding.len(), AGENT_EXCHANGE_PENDING_CAPACITY - 1);
    assert!(
        !outstanding
            .iter()
            .any(|pending| pending.operation_id() == &operation("owed-0")),
        "the withdrawn envelope left the re-drive list"
    );

    // The freed slot is usable, which is the point of the withdrawal.
    let replacement = fx.envelope(AgentExchangeKind::BudgetSettlement, "replacement");
    initiate(&mut host, replacement, fx.now()).await;
    assert_eq!(
        host.outstanding()
            .expect("the participant is recovered")
            .len(),
        AGENT_EXCHANGE_PENDING_CAPACITY
    );

    // And it survives passivation: withdrawal is a durable edit, not a
    // materialized one.
    let recovered = fx.host(run_address()).await;
    assert_eq!(
        recovered
            .outstanding()
            .expect("the participant is recovered")
            .len(),
        AGENT_EXCHANGE_PENDING_CAPACITY
    );
}

#[test]
fn an_oversized_payload_is_refused() {
    // Exchange payloads are commands and results, not content; anything larger
    // belongs behind an artifact reference.
    let error = AgentExchangePayload::encode(
        PROBE_CREATION_TYPE,
        &ProbeCreation {
            label: "x".repeat(AGENT_EXCHANGE_PAYLOAD_MAX_BYTES),
        },
    )
    .expect_err("an oversized payload must be refused");
    assert_eq!(error.code(), "exchange-payload-too-large");
}

#[test]
fn a_payload_of_an_unexpected_type_is_refused() {
    let payload = AgentExchangePayload::encode(
        PROBE_CREATION_TYPE,
        &ProbeCreation {
            label: "probe".to_string(),
        },
    )
    .expect("the payload should encode");

    let error = payload
        .decode::<ProbeAssignment>(PROBE_ASSIGNMENT_TYPE)
        .expect_err("a receiver must not decode a payload it did not expect");
    assert_eq!(error.code(), "exchange-payload-type-mismatch");
}

#[test]
fn the_exchange_protocol_is_serializable() {
    // Standing constraint 5: the exchange protocol crosses `rakka-remote` from
    // the first commit, so both the envelope and the reply are wire types.
    let envelope = ChoreographyProbe::envelope(
        AgentExchangeKind::Assignment,
        operation("assignment-1"),
        task_address(),
        run_address(),
        AgentTimestampMillis::new(7),
    )
    .expect("the envelope should be valid");

    let encoded = serde_json::to_vec(&envelope).expect("the envelope should serialize");
    let decoded: AgentExchangeEnvelope =
        serde_json::from_slice(&encoded).expect("the envelope should deserialize");
    assert_eq!(decoded, envelope);

    let reply = AgentExchangeReply::applied(
        &envelope,
        rakka_agent::AgentExchangeResult::accepted(AgentExchangePayload::empty("rakka.agent.None")),
        AgentTimestampMillis::new(8),
    );
    let encoded = serde_json::to_vec(&reply).expect("the reply should serialize");
    let decoded: AgentExchangeReply =
        serde_json::from_slice(&encoded).expect("the reply should deserialize");
    assert_eq!(decoded, reply);

    // A persisted address is re-parsed and re-validated, never trusted field by
    // field.
    assert_eq!(envelope.target().key(), "run/acme/support-agent/run-1");
    assert_eq!(envelope.initiator().key(), "task/acme/ticket-1");
}

#[tokio::test]
async fn a_duplicate_reply_settles_nothing_and_an_unknown_reply_is_refused() {
    let fx = Fixture::new();
    let mut initiator = fx.host(run_address()).await;

    let envelope = fx.envelope(AgentExchangeKind::Creation, "creation");
    initiate(&mut initiator, envelope.clone(), fx.now()).await;

    let mut receiver = fx.host(task_address()).await;
    let reply = receiver
        .accept(&envelope, fx.now())
        .await
        .expect("the receiver should accept");
    drop(receiver);

    let settlement = initiator
        .settle(&reply, fx.now())
        .await
        .expect("the reply should settle");
    assert!(settlement.is_settled());

    // The same reply again: the consequence must not run twice.
    let settlement = initiator
        .settle(&reply, fx.now())
        .await
        .expect("a duplicate reply is answered from the journal");
    assert!(matches!(
        settlement,
        AgentExchangeSettlement::AlreadySettled { .. }
    ));
    assert_eq!(
        initiator
            .state()
            .expect("recovered")
            .settled_count(AgentExchangeKind::Creation),
        1
    );

    // A reply for something this entity never owed is not acted on at all.
    let stray = fx.envelope(AgentExchangeKind::Creation, "never-owed");
    let stray_reply = AgentExchangeReply::applied(
        &stray,
        rakka_agent::AgentExchangeResult::accepted(AgentExchangePayload::empty("rakka.agent.None")),
        fx.now(),
    );
    let settlement = initiator
        .settle(&stray_reply, fx.now())
        .await
        .expect("an unknown reply is reported, not applied");
    assert_eq!(settlement, AgentExchangeSettlement::Unknown);
}

#[tokio::test]
async fn an_oversized_reply_payload_is_refused_before_it_enters_durable_state() {
    // A reply is decoded from the wire like an envelope, but serde does not run
    // the payload bound. Settlement must re-validate it, or a misbehaving peer
    // could push an unbounded result payload into the initiator's journal.
    let fx = Fixture::new();
    let mut initiator = fx.host(run_address()).await;

    let envelope = fx.envelope(AgentExchangeKind::Creation, "creation");
    let operation_id = envelope.operation_id().clone();
    initiate(&mut initiator, envelope.clone(), fx.now()).await;

    let reply = AgentExchangeReply::applied(
        &envelope,
        rakka_agent::AgentExchangeResult::accepted(AgentExchangePayload::empty("rakka.agent.None")),
        fx.now(),
    );
    let mut tampered = serde_json::to_value(&reply).expect("the reply should serialize");
    tampered["result"]["payload"]["bytes"] =
        serde_json::to_value(vec![0_u8; AGENT_EXCHANGE_PAYLOAD_MAX_BYTES + 1])
            .expect("the oversized bytes should serialize");
    let oversized: AgentExchangeReply = serde_json::from_value(tampered)
        .expect("decoding alone does not enforce the bound, which is the point");

    let error = initiator
        .settle(&oversized, fx.now())
        .await
        .expect_err("an oversized reply payload must be refused");
    assert_eq!(error.code(), "exchange-payload-too-large");

    // Refusing the reply settles nothing: the exchange stays owed under the
    // same operation id, exactly like a delivery failure.
    assert!(initiator
        .state()
        .expect("recovered")
        .journal()
        .pending_exchange(&operation_id)
        .is_some());
}
