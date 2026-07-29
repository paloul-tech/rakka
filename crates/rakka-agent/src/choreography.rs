//! The inter-entity choreography substrate.
//!
//! Every cross-entity exchange in this crate — creation, assignment, run
//! acceptance, result proposal and decision, budget allocation, and settlement
//! or return — is a deduplicated outbox/inbox saga re-driven by the initiator.
//! This module owns the primitives: the initiator's pending-exchange record, the
//! operation-identifier re-drive on recovery, and the receiver-side
//! deduplication that replays the original logical result rather than acting
//! twice.
//!
//! There is no colocated shortcut. The durable path is identical whether the two
//! entities share a node or not, so a shard move can never change the observable
//! outcome of an exchange.
//!
//! Specification: sections 9.8 and 6.10.
//!
//! # Where the saga lives
//!
//! [`AgentExchangeJournal`] is a *component of each participant's own durable
//! state*, not a separate record. That placement is the whole design:
//!
//! - The initiator persists the exchange it owes in the **same
//!   compare-and-set** as the domain transition that created it
//!   ([specification 9.8](../../../docs/plans/rakka-agent/spec.md): the sender
//!   "persists the command intent through its outbox as part of its own
//!   transition"). There is no window in which an entity has transitioned but
//!   forgotten that it owes an exchange.
//! - The receiver records the accepted operation id and the logical result it
//!   returned in the same compare-and-set as the transition that produced it,
//!   so a replay can be answered from durable state without a second
//!   transition.
//!
//! A separate durable inbox/outbox record — the `rakka-agent-workflow`
//! `WorkflowState` shape — cannot give either property, because the entity's
//! state and that record are two independent compare-and-set writes. That
//! substrate stays where it belongs: external effects (slice 1.7), where the
//! side effect really is outside the entity and a two-phase record is what makes
//! it recoverable.
//!
//! # The saga
//!
//! ```text
//! initiator                                     receiver
//! ---------                                     --------
//! transition + journal.initiate(op)   [1 CAS]
//!        │
//!        │  transport (mailbox or TCP; at-most-once)
//!        ▼
//!                                      journal.applied(op)?
//!                                        ├─ yes → reply with the original
//!                                        │        result, no transition
//!                                        └─ no  → transition + record the
//!                                                 result            [1 CAS]
//!        ◄─────────────── reply (result) ──────────────
//!        │
//! journal.settle(op) + domain consequence  [1 CAS]
//! ```
//!
//! Delivery is at-most-once, exactly as the layer below promises. Convergence
//! comes from two rules and nothing else: **the initiator re-drives an
//! unsettled exchange with the same operation id, forever**, and **the receiver
//! deduplicates on that operation id and returns its original logical result**.
//! Neither side ever treats a missing reply as evidence that the exchange did
//! not execute.
//!
//! An inter-entity exchange is therefore always safe to retry — unlike an
//! external effect, whose safety class governs whether a retry is legal at all
//! ([specification 11.2](../../../docs/plans/rakka-agent/spec.md)). The receiver
//! is a single writer that has already recorded what it did.
//!
//! # Rejections are results, not failures
//!
//! A receiver that rejects an exchange — a task's deterministic rules reject a
//! proposed result — has made a *durable decision*. It is recorded in the
//! journal, returned on replay like any other result, and settled by the
//! initiator. Only a transport failure leaves an exchange outstanding. This is
//! what makes "a lost rejection" impossible
//! ([specification 18](../../../docs/plans/rakka-agent/spec.md) scenario 59).
//!
//! # Deduplication windows are bounded, so transitions are also fenced
//!
//! Durable state must stay bounded, so [`AgentExchangeJournal`] remembers a
//! bounded ring of resolved operations. A replay older than that window is not
//! recognized as a duplicate, so a participant's
//! [`AgentExchangeParticipant::apply`] MUST additionally be fenced by its own
//! domain state — a task already in progress rejects a second assignment for a
//! generation it has passed, exactly as the agent entity fences a settings
//! update on the revision it expects to succeed. The ring is the fast path; the
//! fence is the guarantee.
//!
//! # Failure windows
//!
//! Every exchange this phase implements, against each of the four windows
//! [specification 9.8](../../../docs/plans/rakka-agent/spec.md) requires. The
//! convergence argument is a property of the substrate, so each window is proven
//! once for every exchange kind by a test that loops over
//! [`AgentExchangeKind::ALL`].
//!
//! | Exchange | Window | Converges by | Test |
//! | --- | --- | --- | --- |
//! | Creation | initiator lost before send | the exchange is in the initiator's state, committed with the transition; recovery re-drives the same operation id | `an_initiator_lost_before_sending_re_drives_the_same_operation_id` |
//! | Creation | receiver lost after acceptance | acceptance *is* the transition; a re-driven operation id is answered from the applied log | `a_receiver_lost_after_accepting_returns_its_original_result` |
//! | Creation | reply lost | the exchange stays pending; re-drive returns the original result and settles once | `a_lost_reply_settles_once_on_re_drive` |
//! | Creation | duplicate delivery | receiver-side dedup: one transition, original result returned | `duplicate_delivery_produces_one_logical_transition` |
//! | Assignment | initiator lost before send | as above | `an_initiator_lost_before_sending_re_drives_the_same_operation_id` |
//! | Assignment | receiver lost after acceptance | as above | `a_receiver_lost_after_accepting_returns_its_original_result` |
//! | Assignment | reply lost | as above | `a_lost_reply_settles_once_on_re_drive` |
//! | Assignment | duplicate delivery | as above | `duplicate_delivery_produces_one_logical_transition` |
//! | ResultProposal | initiator lost before send | as above | `an_initiator_lost_before_sending_re_drives_the_same_operation_id` |
//! | ResultProposal | receiver lost after acceptance | the validation decision is durable before the reply, so it is never re-validated | `a_receiver_lost_after_accepting_returns_its_original_result` |
//! | ResultProposal | reply lost | the original decision — accept *or* reject — is returned on re-drive | `a_lost_rejection_is_recovered_not_dropped` |
//! | ResultProposal | duplicate delivery | as above | `duplicate_delivery_produces_one_logical_transition` |
//! | BudgetAllocation | initiator lost before send | as above | `an_initiator_lost_before_sending_re_drives_the_same_operation_id` |
//! | BudgetAllocation | receiver lost after acceptance | as above | `a_receiver_lost_after_accepting_returns_its_original_result` |
//! | BudgetAllocation | reply lost | settlement is deduplicated, so the parent is never double-debited | `a_replayed_ledger_exchange_never_double_debits_or_double_credits` |
//! | BudgetAllocation | duplicate delivery | receiver-side dedup, so the child is never double-credited | `a_replayed_ledger_exchange_never_double_debits_or_double_credits` |
//! | BudgetSettlement | initiator lost before send | as above | `an_initiator_lost_before_sending_re_drives_the_same_operation_id` |
//! | BudgetSettlement | receiver lost after acceptance | as above | `a_receiver_lost_after_accepting_returns_its_original_result` |
//! | BudgetSettlement | reply lost | as above | `a_replayed_ledger_exchange_never_double_debits_or_double_credits` |
//! | BudgetSettlement | duplicate delivery | as above | `a_replayed_ledger_exchange_never_double_debits_or_double_credits` |
//! | BudgetReturn | initiator lost before send | as above | `an_initiator_lost_before_sending_re_drives_the_same_operation_id` |
//! | BudgetReturn | receiver lost after acceptance | as above | `a_receiver_lost_after_accepting_returns_its_original_result` |
//! | BudgetReturn | reply lost | as above | `a_replayed_ledger_exchange_never_double_debits_or_double_credits` |
//! | BudgetReturn | duplicate delivery | as above | `a_replayed_ledger_exchange_never_double_debits_or_double_credits` |
//!
//! The reply halves named by [specification 9.8](../../../docs/plans/rakka-agent/spec.md)
//! — run acceptance and the result decision — are the replies of the
//! [`AgentExchangeKind::Assignment`] and [`AgentExchangeKind::ResultProposal`]
//! rows: they carry the receiver's logical result back to the initiator and are
//! covered by the reply-loss and duplicate-delivery windows of those exchanges.
//!
//! Every test above lives in `tests/choreography.rs`; the colocated and
//! split-across-nodes exchanges of scenario 60 live in
//! `tests/choreography_cluster.rs`.
//!
//! The table proves the *substrate* converges, once per exchange kind. Each
//! exchange is proven again on the real entities that own it, because a
//! participant's own domain fence is half of the argument (see the deduplication
//! window note above) and only the entity has one:
//!
//! | Exchange | Entities | Test |
//! | --- | --- | --- |
//! | Creation, Assignment | ingress → task → run | `tests/task_entity.rs` |
//! | Assignment, ResultProposal | task ⇄ run, with either side lost at every durable write | `tests/run_result_exchange.rs` (scenario 59) |
//! | BudgetSettlement, BudgetReturn | run → task, a terminal run handing its escrow back | `tests/escrow_ledger.rs` (scenario 61) |

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use rakka_agent_workflow::{
    AgentCorrelationId, AgentTelemetryContext, AgentTimestampMillis, StateSchemaVersion,
};
use rakka_core::{Message, ReplyTo};
use rakka_persistence::{
    DurableError, DurableState, DurableStateStore, PersistenceId, Revision, StateRecord,
};
use rakka_remote::{
    PayloadCodec, RemoteError, RemoteResult, RemoteTransport, SerializationRegistry,
};
use rakka_sharding::{ClusterSharding, EntityTypeKey, RemoteEntityAskClient};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::identity::{
    AgentIdentityError, AgentIdentityResult, AgentOperationId, AgentRunScope, AgentScope,
    AgentTaskScope, TenantId, AGENT_SCOPE_SEPARATOR,
};
use crate::schema::{
    AgentRecordKind, AgentSchemaError, AgentSchemaPolicy, VersionedAgentRecord,
    CURRENT_AGENT_EXCHANGE_ENVELOPE_SCHEMA_VERSION, CURRENT_AGENT_EXCHANGE_JOURNAL_SCHEMA_VERSION,
    CURRENT_AGENT_EXCHANGE_REPLY_SCHEMA_VERSION,
};

/// Largest exchange payload that may travel in an envelope or a reply, in
/// bytes.
///
/// Exchange payloads are commands and results, not content. Anything larger
/// belongs behind an artifact reference, so that durable state, mailboxes, and
/// remote envelopes all stay bounded
/// ([specification 9.6](../../../docs/plans/rakka-agent/spec.md)). The bound is
/// enforced at encoding, and re-enforced when an envelope is accepted and when
/// a reply is settled, because a value decoded from the wire has not passed
/// through [`AgentExchangePayload::encode`].
pub const AGENT_EXCHANGE_PAYLOAD_MAX_BYTES: usize = 32 * 1024;

/// How many exchanges one participant may owe at once.
///
/// Exceeding it fails the initiating transition closed rather than dropping an
/// owed exchange: an unbounded pending list is an unbounded durable record.
pub const AGENT_EXCHANGE_PENDING_CAPACITY: usize = 64;

/// How many resolved operation ids a journal remembers on each side.
///
/// See the module documentation: the ring is the deduplication fast path, and a
/// participant's own domain fence is what makes a replay older than the window
/// safe.
pub const AGENT_EXCHANGE_LOG_CAPACITY: usize = 64;

/// Result type for choreography operations.
pub type AgentChoreographyResult<T> = Result<T, AgentChoreographyError>;

/// Class of entity an exchange addresses.
///
/// The class is the first segment of an [`AgentEntityAddress`] key and selects
/// the transport route, so two entity classes can never collide even when their
/// scope keys coincide.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentEntityClass {
    /// The sharded agent entity, keyed `(TenantId, AgentId)`.
    Agent,
    /// The sharded task entity, keyed `(TenantId, AgentTaskId)`.
    Task,
    /// The sharded run entity, keyed `(TenantId, AgentId, AgentRunId)`.
    Run,
}

impl AgentEntityClass {
    /// Stable kebab-case label, used as the first segment of an address key.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Agent => "agent",
            Self::Task => "task",
            Self::Run => "run",
        }
    }

    /// Parses a class label.
    #[must_use]
    pub fn from_label(label: &str) -> Option<Self> {
        match label {
            "agent" => Some(Self::Agent),
            "task" => Some(Self::Task),
            "run" => Some(Self::Run),
            _ => None,
        }
    }
}

impl Display for AgentEntityClass {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// The durable address of one choreography participant.
///
/// An address is a routing key and a durable record locator at once: it resolves
/// to the sharded [`rakka_sharding::EntityId`] that owns the entity and to the
/// [`PersistenceId`] of its state. It serializes as its flattened key, so a
/// persisted address is re-parsed and re-validated on load rather than trusted
/// segment by segment.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum AgentEntityAddress {
    /// One agent entity.
    Agent(AgentScope),
    /// One typed task entity.
    Task(AgentTaskScope),
    /// One run entity.
    Run(AgentRunScope),
}

impl AgentEntityAddress {
    /// Class of the addressed entity.
    #[must_use]
    pub const fn class(&self) -> AgentEntityClass {
        match self {
            Self::Agent(_) => AgentEntityClass::Agent,
            Self::Task(_) => AgentEntityClass::Task,
            Self::Run(_) => AgentEntityClass::Run,
        }
    }

    /// Tenant boundary of the addressed entity.
    #[must_use]
    pub const fn tenant(&self) -> &TenantId {
        match self {
            Self::Agent(scope) => scope.tenant(),
            Self::Task(scope) => scope.tenant(),
            Self::Run(scope) => scope.tenant(),
        }
    }

    /// Sharded entity id that routes to the owning shard.
    #[must_use]
    pub fn entity_id(&self) -> rakka_sharding::EntityId {
        match self {
            Self::Agent(scope) => scope.entity_id(),
            Self::Task(scope) => scope.entity_id(),
            Self::Run(scope) => scope.entity_id(),
        }
    }

    /// Durable persistence id of the addressed entity's state.
    #[must_use]
    pub fn persistence_id(&self) -> PersistenceId {
        match self {
            Self::Agent(scope) => scope.persistence_id(),
            Self::Task(scope) => scope.persistence_id(),
            Self::Run(scope) => scope.persistence_id(),
        }
    }

    /// Flattened, injective key: the class label followed by the scope key.
    #[must_use]
    pub fn key(&self) -> String {
        let scope = match self {
            Self::Agent(scope) => scope.key(),
            Self::Task(scope) => scope.key(),
            Self::Run(scope) => scope.key(),
        };
        format!("{}{AGENT_SCOPE_SEPARATOR}{scope}", self.class().as_label())
    }

    /// Rebuilds the address of a sharded entity from its class and entity id.
    ///
    /// A sharded entity knows its class from the entity type it was registered
    /// under, and its scope from its entity id. An entity id that does not parse
    /// into that class's scope cannot address a durable record, so it fails
    /// closed rather than guessing.
    pub fn from_entity_id(
        class: AgentEntityClass,
        entity_id: &rakka_sharding::EntityId,
    ) -> AgentIdentityResult<Self> {
        Ok(match class {
            AgentEntityClass::Agent => Self::Agent(AgentScope::from_entity_id(entity_id)?),
            AgentEntityClass::Task => Self::Task(AgentTaskScope::from_entity_id(entity_id)?),
            AgentEntityClass::Run => Self::Run(AgentRunScope::from_entity_id(entity_id)?),
        })
    }

    /// Parses a flattened address key, failing closed on a malformed value.
    pub fn parse(key: &str) -> AgentIdentityResult<Self> {
        let (class, scope) = key.split_once(AGENT_SCOPE_SEPARATOR).ok_or({
            AgentIdentityError::MalformedScopeKey {
                field: ADDRESS_FIELD,
                expected_segments: 3,
                actual_segments: 1,
            }
        })?;
        let class =
            AgentEntityClass::from_label(class).ok_or(AgentIdentityError::MalformedScopeKey {
                field: ADDRESS_FIELD,
                expected_segments: 3,
                actual_segments: key.split(AGENT_SCOPE_SEPARATOR).count(),
            })?;
        Ok(match class {
            AgentEntityClass::Agent => Self::Agent(AgentScope::parse(scope)?),
            AgentEntityClass::Task => Self::Task(AgentTaskScope::parse(scope)?),
            AgentEntityClass::Run => Self::Run(AgentRunScope::parse(scope)?),
        })
    }
}

impl Display for AgentEntityAddress {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.key())
    }
}

impl Serialize for AgentEntityAddress {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.key())
    }
}

impl<'de> Deserialize<'de> for AgentEntityAddress {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let key = String::deserialize(deserializer)?;
        Self::parse(&key).map_err(serde::de::Error::custom)
    }
}

const ADDRESS_FIELD: &str = "entity address";

/// One inter-entity exchange defined by
/// [specification 9.8](../../../docs/plans/rakka-agent/spec.md).
///
/// Each variant is a request/reply saga owned by its initiator. The reply halves
/// the specification names separately — run acceptance and the result decision —
/// are the replies of [`Self::Assignment`] and [`Self::ResultProposal`]; they
/// carry the receiver's logical result home and are not separate sagas.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentExchangeKind {
    /// Durable creation of a typed task. Initiated by an ingress or a delegating
    /// run against the task entity.
    Creation,
    /// Assignment of a task to an agent, creating the run that serves it.
    /// Initiated by the task entity against the run entity; the reply is the
    /// run's durable acceptance.
    Assignment,
    /// A run's proposal of a typed task result. Initiated by the run entity
    /// against the task entity; the reply is the task's validation decision, and
    /// that decision — not the run — is what makes the public task terminal.
    ResultProposal,
    /// An escrow allocation debited from a parent scope and credited to a child.
    /// Initiated by the parent
    /// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
    BudgetAllocation,
    /// Settlement of consumed budget back to the parent scope. Initiated by the
    /// child.
    BudgetSettlement,
    /// Return of unconsumed budget to the parent scope. Initiated by the child.
    BudgetReturn,
    /// A completed epoch task returning its terminal outcome, consumption, and
    /// evidence reference to the continuous root control task that admitted
    /// its wake ([specification 8.2](../../../docs/plans/rakka-agent/spec.md)).
    /// Initiated by the epoch task; the reply is the controller's release.
    EpochResult,
    /// A completed goal evaluation carried to the coordinating root task
    /// ([specification 8.3](../../../docs/plans/rakka-agent/spec.md)).
    /// Initiated by the run that committed the evaluation effect; the reply is
    /// the decision door's acceptance or its refusal, and under a configured
    /// evaluator this exchange is the only ingress a criteria decision has.
    GoalEvaluation,
}

impl AgentExchangeKind {
    /// Every exchange this phase implements.
    pub const ALL: [Self; 8] = [
        Self::Creation,
        Self::Assignment,
        Self::ResultProposal,
        Self::BudgetAllocation,
        Self::BudgetSettlement,
        Self::BudgetReturn,
        Self::EpochResult,
        Self::GoalEvaluation,
    ];

    /// Stable kebab-case label for errors, logs, and bounded metric labels.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Creation => "creation",
            Self::Assignment => "assignment",
            Self::ResultProposal => "result-proposal",
            Self::BudgetAllocation => "budget-allocation",
            Self::BudgetSettlement => "budget-settlement",
            Self::BudgetReturn => "budget-return",
            Self::EpochResult => "epoch-result",
            Self::GoalEvaluation => "goal-evaluation",
        }
    }
}

impl Display for AgentExchangeKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// A bounded, typed exchange payload.
///
/// The substrate does not interpret the payload: the task and run entities own
/// their own command and result shapes. It carries a stable type name so a
/// receiver fails closed on a payload it does not recognize instead of decoding
/// it into the wrong command.
///
/// A payload never contains resolved credentials or secret material
/// ([specification 16](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentExchangePayload {
    payload_type: String,
    bytes: Vec<u8>,
}

impl AgentExchangePayload {
    /// Encodes a typed payload, rejecting one that exceeds
    /// [`AGENT_EXCHANGE_PAYLOAD_MAX_BYTES`].
    pub fn encode<T>(payload_type: impl Into<String>, value: &T) -> AgentChoreographyResult<Self>
    where
        T: Serialize,
    {
        let bytes =
            serde_json::to_vec(value).map_err(|error| AgentChoreographyError::PayloadEncoding {
                message: error.to_string(),
            })?;
        let payload = Self {
            payload_type: payload_type.into(),
            bytes,
        };
        payload.validate()?;
        Ok(payload)
    }

    /// An empty payload of a named type, for an exchange whose result carries no
    /// content of its own.
    pub fn empty(payload_type: impl Into<String>) -> Self {
        Self {
            payload_type: payload_type.into(),
            bytes: Vec::new(),
        }
    }

    /// Stable type name of the encoded value.
    #[must_use]
    pub fn payload_type(&self) -> &str {
        &self.payload_type
    }

    /// Encoded bytes.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Whether the payload carries no content.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// Decodes the payload, requiring the expected type name.
    ///
    /// The type check is not decoration: two commands can be structurally
    /// compatible and semantically different, and a receiver that decoded the
    /// wrong one would transition wrongly rather than fail.
    pub fn decode<T>(&self, expected_type: &str) -> AgentChoreographyResult<T>
    where
        T: DeserializeOwned,
    {
        if self.payload_type != expected_type {
            return Err(AgentChoreographyError::PayloadTypeMismatch {
                expected: expected_type.to_string(),
                actual: self.payload_type.clone(),
            });
        }
        serde_json::from_slice(&self.bytes).map_err(|error| {
            AgentChoreographyError::PayloadDecoding {
                payload_type: self.payload_type.clone(),
                message: error.to_string(),
            }
        })
    }

    fn validate(&self) -> AgentChoreographyResult<()> {
        if self.payload_type.is_empty() {
            return Err(AgentChoreographyError::PayloadEncoding {
                message: "an exchange payload must declare a type name".to_string(),
            });
        }
        if self.bytes.len() > AGENT_EXCHANGE_PAYLOAD_MAX_BYTES {
            return Err(AgentChoreographyError::PayloadTooLarge {
                payload_type: self.payload_type.clone(),
                bytes: self.bytes.len(),
                maximum: AGENT_EXCHANGE_PAYLOAD_MAX_BYTES,
            });
        }
        Ok(())
    }
}

/// One cross-entity command: what the initiator owes and what it sends.
///
/// The envelope is both a durable record (the initiator's pending exchange) and
/// a wire message (the remote request an owning node receives), which is why it
/// carries a schema version and re-validates on load.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentExchangeEnvelope {
    schema_version: StateSchemaVersion,
    operation_id: AgentOperationId,
    kind: AgentExchangeKind,
    initiator: AgentEntityAddress,
    target: AgentEntityAddress,
    payload: AgentExchangePayload,
    correlation_id: AgentCorrelationId,
    created_at: AgentTimestampMillis,
    /// Trace context of the segment that committed the exchange, so the
    /// receiver's acceptance span can link to the initiating segment
    /// ([specification 17.5](../../../docs/plans/rakka-agent/spec.md)). A
    /// re-driven exchange re-sends this persisted envelope, so the original
    /// context rides every re-drive. Observability only, never correctness:
    /// an envelope persisted before this field decodes to the empty context.
    #[serde(default)]
    telemetry: AgentTelemetryContext,
}

impl AgentExchangeEnvelope {
    /// Creates an exchange envelope.
    ///
    /// The two addresses must share a tenant: an exchange is never a
    /// cross-tenant channel
    /// ([specification 16](../../../docs/plans/rakka-agent/spec.md)).
    pub fn new(
        operation_id: AgentOperationId,
        kind: AgentExchangeKind,
        initiator: AgentEntityAddress,
        target: AgentEntityAddress,
        payload: AgentExchangePayload,
        correlation_id: AgentCorrelationId,
        created_at: AgentTimestampMillis,
    ) -> AgentChoreographyResult<Self> {
        let envelope = Self {
            schema_version: CURRENT_AGENT_EXCHANGE_ENVELOPE_SCHEMA_VERSION,
            operation_id,
            kind,
            initiator,
            target,
            payload,
            correlation_id,
            created_at,
            telemetry: AgentTelemetryContext::default(),
        };
        envelope.validate()?;
        Ok(envelope)
    }

    /// Stamps the trace context of the segment committing this exchange.
    ///
    /// The context is admitted through
    /// [`crate::observability::sanitize_agent_telemetry_context`]: strict on
    /// write so the read side never has to fail closed over telemetry.
    #[must_use]
    pub fn with_telemetry(mut self, telemetry: AgentTelemetryContext) -> Self {
        self.telemetry = crate::observability::sanitize_agent_telemetry_context(telemetry);
        self
    }

    /// Whether this envelope carries any trace context at all.
    #[must_use]
    pub fn has_telemetry(&self) -> bool {
        self.telemetry.trace_parent.is_some() || !self.telemetry.span_links.is_empty()
    }

    /// Trace context of the segment that committed the exchange.
    #[must_use]
    pub const fn telemetry(&self) -> &AgentTelemetryContext {
        &self.telemetry
    }

    /// Stable operation id every side of this exchange deduplicates on.
    #[must_use]
    pub const fn operation_id(&self) -> &AgentOperationId {
        &self.operation_id
    }

    /// Which exchange this is.
    #[must_use]
    pub const fn kind(&self) -> AgentExchangeKind {
        self.kind
    }

    /// Entity that owns the saga and receives the reply.
    #[must_use]
    pub const fn initiator(&self) -> &AgentEntityAddress {
        &self.initiator
    }

    /// Entity that applies the exchange.
    #[must_use]
    pub const fn target(&self) -> &AgentEntityAddress {
        &self.target
    }

    /// Command payload.
    #[must_use]
    pub const fn payload(&self) -> &AgentExchangePayload {
        &self.payload
    }

    /// Correlation id shared by every record of this exchange.
    #[must_use]
    pub const fn correlation_id(&self) -> &AgentCorrelationId {
        &self.correlation_id
    }

    /// When the initiator recorded the exchange.
    #[must_use]
    pub const fn created_at(&self) -> AgentTimestampMillis {
        self.created_at
    }

    fn validate(&self) -> AgentChoreographyResult<()> {
        self.payload.validate()?;
        if self.initiator.tenant() != self.target.tenant() {
            return Err(AgentChoreographyError::CrossTenantExchange {
                initiator: Box::new(self.initiator.clone()),
                target: Box::new(self.target.clone()),
            });
        }
        Ok(())
    }
}

impl VersionedAgentRecord for AgentExchangeEnvelope {
    const RECORD_KIND: AgentRecordKind = AgentRecordKind::ExchangeEnvelope;

    fn schema_version(&self) -> StateSchemaVersion {
        self.schema_version
    }
}

/// Carries the causing exchange's trace context onto an owed envelope that
/// has none of its own ([specification 17.5](../../../docs/plans/rakka-agent/spec.md)).
///
/// A consequence committed inside an acceptance is caused by the exchange
/// being accepted, and a consequence of a settlement by the exchange that
/// settled — so context flows down the choreography chain (creation ->
/// assignment -> run acceptance) without any participant doing per-exchange
/// work. An envelope the participant stamped itself is left alone: the
/// participant knows its own segment better than the substrate does. An
/// entity never invents context, so a chain whose ingress carried none stays
/// context-free and every segment starts a root.
fn propagate_exchange_telemetry(
    cause: &AgentTelemetryContext,
    owed: Vec<AgentExchangeEnvelope>,
) -> Vec<AgentExchangeEnvelope> {
    if cause.trace_parent.is_none() && cause.span_links.is_empty() {
        return owed;
    }
    owed.into_iter()
        .map(|envelope| {
            if envelope.has_telemetry() {
                envelope
            } else {
                envelope.with_telemetry(cause.clone())
            }
        })
        .collect()
}

/// The receiver's decision about one exchange.
///
/// A rejection is a decision, not a failure: it is durable, it is returned
/// unchanged on replay, and the initiator settles on it. Only a transport
/// failure leaves an exchange outstanding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentExchangeStatus {
    /// The receiver applied the exchange.
    Accepted,
    /// The receiver durably refused the exchange.
    Rejected {
        /// Stable machine-readable reason code.
        code: String,
        /// Human-readable detail.
        message: String,
    },
}

impl AgentExchangeStatus {
    /// Whether the receiver applied the exchange.
    #[must_use]
    pub const fn is_accepted(&self) -> bool {
        matches!(self, Self::Accepted)
    }

    /// Rejection code, when the exchange was refused.
    #[must_use]
    pub fn rejection_code(&self) -> Option<&str> {
        match self {
            Self::Accepted => None,
            Self::Rejected { code, .. } => Some(code),
        }
    }
}

/// The logical result of one exchange: the receiver's decision and whatever the
/// initiator needs to act on it.
///
/// This is the value a replayed operation id must return again, byte for byte.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentExchangeResult {
    status: AgentExchangeStatus,
    payload: AgentExchangePayload,
}

impl AgentExchangeResult {
    /// The receiver applied the exchange and returns this result payload.
    #[must_use]
    pub const fn accepted(payload: AgentExchangePayload) -> Self {
        Self {
            status: AgentExchangeStatus::Accepted,
            payload,
        }
    }

    /// The receiver durably refused the exchange.
    pub fn rejected(
        code: impl Into<String>,
        message: impl Into<String>,
        payload: AgentExchangePayload,
    ) -> Self {
        Self {
            status: AgentExchangeStatus::Rejected {
                code: code.into(),
                message: message.into(),
            },
            payload,
        }
    }

    /// The receiver's decision.
    #[must_use]
    pub const fn status(&self) -> &AgentExchangeStatus {
        &self.status
    }

    /// Result payload.
    #[must_use]
    pub const fn payload(&self) -> &AgentExchangePayload {
        &self.payload
    }

    /// Whether the receiver applied the exchange.
    #[must_use]
    pub const fn is_accepted(&self) -> bool {
        self.status.is_accepted()
    }
}

/// The reply that carries one exchange's logical result home.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentExchangeReply {
    schema_version: StateSchemaVersion,
    operation_id: AgentOperationId,
    kind: AgentExchangeKind,
    result: AgentExchangeResult,
    replayed: bool,
    replied_at: AgentTimestampMillis,
}

impl AgentExchangeReply {
    /// The receiver applied the exchange for the first time.
    #[must_use]
    pub fn applied(
        envelope: &AgentExchangeEnvelope,
        result: AgentExchangeResult,
        replied_at: AgentTimestampMillis,
    ) -> Self {
        Self::new(envelope, result, false, replied_at)
    }

    /// The receiver recognized a replayed operation id and returned its original
    /// result without transitioning again.
    #[must_use]
    pub fn replayed(
        envelope: &AgentExchangeEnvelope,
        result: AgentExchangeResult,
        replied_at: AgentTimestampMillis,
    ) -> Self {
        Self::new(envelope, result, true, replied_at)
    }

    fn new(
        envelope: &AgentExchangeEnvelope,
        result: AgentExchangeResult,
        replayed: bool,
        replied_at: AgentTimestampMillis,
    ) -> Self {
        Self {
            schema_version: CURRENT_AGENT_EXCHANGE_REPLY_SCHEMA_VERSION,
            operation_id: envelope.operation_id.clone(),
            kind: envelope.kind,
            result,
            replayed,
            replied_at,
        }
    }

    /// Operation this reply resolves.
    #[must_use]
    pub const fn operation_id(&self) -> &AgentOperationId {
        &self.operation_id
    }

    /// Which exchange this reply resolves.
    #[must_use]
    pub const fn kind(&self) -> AgentExchangeKind {
        self.kind
    }

    /// The exchange's logical result.
    #[must_use]
    pub const fn result(&self) -> &AgentExchangeResult {
        &self.result
    }

    /// Whether the receiver answered from its deduplication log rather than
    /// transitioning.
    ///
    /// This is observability, never correctness: the initiator settles a replayed
    /// reply exactly as it settles a first one, because it carries the same
    /// logical result.
    #[must_use]
    pub const fn is_replayed(&self) -> bool {
        self.replayed
    }

    /// When the receiver produced the reply.
    #[must_use]
    pub const fn replied_at(&self) -> AgentTimestampMillis {
        self.replied_at
    }

    fn validate(&self) -> AgentChoreographyResult<()> {
        self.result.payload.validate()
    }
}

impl VersionedAgentRecord for AgentExchangeReply {
    const RECORD_KIND: AgentRecordKind = AgentRecordKind::ExchangeReply;

    fn schema_version(&self) -> StateSchemaVersion {
        self.schema_version
    }
}

/// One exchange an initiator owes and has not yet settled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingExchange {
    envelope: AgentExchangeEnvelope,
    attempts: u32,
    recorded_at: AgentTimestampMillis,
    last_attempt_at: Option<AgentTimestampMillis>,
    last_failure_code: Option<String>,
}

impl PendingExchange {
    /// The exchange to re-drive, with the operation id it was first recorded
    /// under.
    #[must_use]
    pub const fn envelope(&self) -> &AgentExchangeEnvelope {
        &self.envelope
    }

    /// How many delivery attempts have failed.
    #[must_use]
    pub const fn attempts(&self) -> u32 {
        self.attempts
    }

    /// When the initiating transition recorded the exchange.
    #[must_use]
    pub const fn recorded_at(&self) -> AgentTimestampMillis {
        self.recorded_at
    }

    /// When delivery was last attempted.
    #[must_use]
    pub const fn last_attempt_at(&self) -> Option<AgentTimestampMillis> {
        self.last_attempt_at
    }

    /// Stable code of the last delivery failure.
    #[must_use]
    pub fn last_failure_code(&self) -> Option<&str> {
        self.last_failure_code.as_deref()
    }
}

/// One resolved operation and the logical result it produced.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ResolvedExchange {
    operation_id: AgentOperationId,
    kind: AgentExchangeKind,
    result: AgentExchangeResult,
    resolved_at: AgentTimestampMillis,
}

/// The durable saga record of one choreography participant.
///
/// It is the entity's outbox and inbox at once, and it lives inside the entity's
/// own state so that recording an owed exchange, or recording the result of one,
/// is part of the very transition that caused it. See the module documentation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentExchangeJournal {
    schema_version: StateSchemaVersion,
    pending: Vec<PendingExchange>,
    settled: Vec<ResolvedExchange>,
    applied: Vec<ResolvedExchange>,
}

impl AgentExchangeJournal {
    /// An empty journal.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            schema_version: CURRENT_AGENT_EXCHANGE_JOURNAL_SCHEMA_VERSION,
            pending: Vec::new(),
            settled: Vec::new(),
            applied: Vec::new(),
        }
    }

    /// Records an exchange this entity owes.
    ///
    /// Call it inside the transition that causes the exchange, so the two commit
    /// together. It is idempotent by operation id: re-recording an exchange that
    /// is already pending, or one that has already been settled, changes nothing
    /// and reports what was already true.
    pub fn initiate(
        &mut self,
        envelope: AgentExchangeEnvelope,
        now: AgentTimestampMillis,
    ) -> AgentChoreographyResult<AgentExchangeInitiation> {
        envelope.validate()?;
        let operation_id = envelope.operation_id.clone();

        if let Some(settled) = find_resolved(&self.settled, &operation_id) {
            check_kind(settled, envelope.kind)?;
            return Ok(AgentExchangeInitiation::AlreadySettled {
                result: settled.result.clone(),
            });
        }
        if let Some(pending) = self
            .pending
            .iter()
            .find(|pending| pending.envelope.operation_id == operation_id)
        {
            if pending.envelope.kind != envelope.kind {
                return Err(AgentChoreographyError::ConflictingOperation {
                    operation_id,
                    recorded: pending.envelope.kind,
                    offered: envelope.kind,
                });
            }
            return Ok(AgentExchangeInitiation::AlreadyPending);
        }
        if self.pending.len() >= AGENT_EXCHANGE_PENDING_CAPACITY {
            return Err(AgentChoreographyError::PendingOverflow {
                maximum: AGENT_EXCHANGE_PENDING_CAPACITY,
            });
        }

        self.pending.push(PendingExchange {
            envelope,
            attempts: 0,
            recorded_at: now,
            last_attempt_at: None,
            last_failure_code: None,
        });
        Ok(AgentExchangeInitiation::Recorded)
    }

    /// Every exchange this entity still owes, in the order it recorded them.
    ///
    /// This is the re-drive list. It comes from durable state, so an entity that
    /// was lost and re-materialized on another shard owner re-drives exactly the
    /// exchanges it owed, under the operation ids it first minted.
    #[must_use]
    pub fn outstanding(&self) -> &[PendingExchange] {
        &self.pending
    }

    /// The pending record for one operation, if the entity still owes it.
    #[must_use]
    pub fn pending_exchange(&self, operation_id: &AgentOperationId) -> Option<&PendingExchange> {
        self.pending
            .iter()
            .find(|pending| &pending.envelope.operation_id == operation_id)
    }

    /// Records a failed delivery attempt.
    ///
    /// The exchange stays outstanding. A delivery failure is never evidence that
    /// the receiver did not apply it
    /// ([specification 9.8](../../../docs/plans/rakka-agent/spec.md)), so the
    /// only safe response is to re-drive the same operation id and let the
    /// receiver deduplicate.
    pub fn record_delivery_failure(
        &mut self,
        operation_id: &AgentOperationId,
        code: impl Into<String>,
        now: AgentTimestampMillis,
    ) -> bool {
        let Some(pending) = self
            .pending
            .iter_mut()
            .find(|pending| &pending.envelope.operation_id == operation_id)
        else {
            return false;
        };
        pending.attempts = pending.attempts.saturating_add(1);
        pending.last_attempt_at = Some(now);
        pending.last_failure_code = Some(code.into());
        true
    }

    /// Settles one exchange with the result its receiver returned.
    ///
    /// Call it inside the transition that applies the result. A duplicate reply
    /// for an operation that is already settled reports the original result and
    /// does not settle twice; a reply for an operation this entity does not owe
    /// and has never settled is unknown, and the caller must not act on it.
    pub fn settle(
        &mut self,
        operation_id: &AgentOperationId,
        kind: AgentExchangeKind,
        result: AgentExchangeResult,
        now: AgentTimestampMillis,
    ) -> AgentChoreographyResult<AgentExchangeSettlement> {
        if let Some(settled) = find_resolved(&self.settled, operation_id) {
            check_kind(settled, kind)?;
            return Ok(AgentExchangeSettlement::AlreadySettled {
                result: settled.result.clone(),
            });
        }

        let Some(position) = self
            .pending
            .iter()
            .position(|pending| &pending.envelope.operation_id == operation_id)
        else {
            return Ok(AgentExchangeSettlement::Unknown);
        };
        let pending = self.pending.remove(position);
        if pending.envelope.kind != kind {
            return Err(AgentChoreographyError::ConflictingOperation {
                operation_id: operation_id.clone(),
                recorded: pending.envelope.kind,
                offered: kind,
            });
        }

        push_resolved(
            &mut self.settled,
            ResolvedExchange {
                operation_id: operation_id.clone(),
                kind,
                result: result.clone(),
                resolved_at: now,
            },
        );
        Ok(AgentExchangeSettlement::Settled {
            envelope: Box::new(pending.envelope),
            result,
        })
    }

    /// Whether this entity has already initiated one operation, whether it is
    /// still outstanding or already settled.
    ///
    /// This is the guard an *entry-point* transition uses to stay idempotent.
    /// A transition caused by an accepted exchange needs no such guard — the
    /// substrate has already deduplicated its cause — but one started from
    /// outside the saga (an ingress command, an operator action) is only as
    /// idempotent as its caller makes it.
    #[must_use]
    pub fn has_initiated(&self, operation_id: &AgentOperationId) -> bool {
        self.pending_exchange(operation_id).is_some()
            || find_resolved(&self.settled, operation_id).is_some()
    }

    /// The result this entity settled for an operation it initiated, if the
    /// operation is still inside the deduplication window.
    pub fn settled_result(
        &self,
        operation_id: &AgentOperationId,
        kind: AgentExchangeKind,
    ) -> AgentChoreographyResult<Option<&AgentExchangeResult>> {
        let Some(settled) = find_resolved(&self.settled, operation_id) else {
            return Ok(None);
        };
        check_kind(settled, kind)?;
        Ok(Some(&settled.result))
    }

    /// The result this entity already returned for a received operation, if the
    /// operation is still inside the deduplication window.
    pub fn applied_result(
        &self,
        operation_id: &AgentOperationId,
        kind: AgentExchangeKind,
    ) -> AgentChoreographyResult<Option<&AgentExchangeResult>> {
        let Some(applied) = find_resolved(&self.applied, operation_id) else {
            return Ok(None);
        };
        check_kind(applied, kind)?;
        Ok(Some(&applied.result))
    }

    /// Records the result this entity returned for a received operation.
    ///
    /// Call it inside the transition that produced the result, so the two commit
    /// together and a replay can never see a transition whose result was not
    /// recorded.
    pub fn record_applied(
        &mut self,
        operation_id: AgentOperationId,
        kind: AgentExchangeKind,
        result: AgentExchangeResult,
        now: AgentTimestampMillis,
    ) {
        push_resolved(
            &mut self.applied,
            ResolvedExchange {
                operation_id,
                kind,
                result,
                resolved_at: now,
            },
        );
    }

    /// How many exchanges this entity owes.
    #[must_use]
    pub fn outstanding_count(&self) -> usize {
        self.pending.len()
    }

    /// How many resolved received operations are inside the deduplication
    /// window.
    #[must_use]
    pub fn applied_count(&self) -> usize {
        self.applied.len()
    }

    /// How many settled initiated operations are inside the deduplication
    /// window.
    #[must_use]
    pub fn settled_count(&self) -> usize {
        self.settled.len()
    }
}

impl Default for AgentExchangeJournal {
    fn default() -> Self {
        Self::new()
    }
}

impl VersionedAgentRecord for AgentExchangeJournal {
    const RECORD_KIND: AgentRecordKind = AgentRecordKind::ExchangeJournal;

    fn schema_version(&self) -> StateSchemaVersion {
        self.schema_version
    }
}

fn find_resolved<'a>(
    log: &'a [ResolvedExchange],
    operation_id: &AgentOperationId,
) -> Option<&'a ResolvedExchange> {
    log.iter()
        .find(|resolved| &resolved.operation_id == operation_id)
}

fn check_kind(
    resolved: &ResolvedExchange,
    offered: AgentExchangeKind,
) -> AgentChoreographyResult<()> {
    if resolved.kind == offered {
        return Ok(());
    }
    Err(AgentChoreographyError::ConflictingOperation {
        operation_id: resolved.operation_id.clone(),
        recorded: resolved.kind,
        offered,
    })
}

fn push_resolved(log: &mut Vec<ResolvedExchange>, resolved: ResolvedExchange) {
    log.push(resolved);
    while log.len() > AGENT_EXCHANGE_LOG_CAPACITY {
        log.remove(0);
    }
}

/// What recording an owed exchange did.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AgentExchangeInitiation {
    /// The exchange is now outstanding.
    Recorded,
    /// The entity already owed this exchange; nothing changed.
    AlreadyPending,
    /// The exchange already completed; its result is returned again rather than
    /// re-driven.
    AlreadySettled {
        /// The result the receiver originally returned.
        result: AgentExchangeResult,
    },
}

/// What settling a reply did.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AgentExchangeSettlement {
    /// The exchange was outstanding and is now settled; the initiator applied
    /// the result's consequence exactly once.
    Settled {
        /// The exchange that was settled.
        envelope: Box<AgentExchangeEnvelope>,
        /// The result the receiver returned.
        result: AgentExchangeResult,
    },
    /// A duplicate reply for an exchange that was already settled. The original
    /// result is returned and no consequence is applied a second time.
    AlreadySettled {
        /// The result the initiator originally settled on.
        result: AgentExchangeResult,
    },
    /// A reply for an operation this entity does not owe and has no record of
    /// settling. The caller must not act on it: it is either a reply that aged
    /// out of the deduplication window or one that was never owed.
    Unknown,
}

impl AgentExchangeSettlement {
    /// Whether this settlement applied a consequence.
    #[must_use]
    pub const fn is_settled(&self) -> bool {
        matches!(self, Self::Settled { .. })
    }

    /// The exchange's logical result, when it is known.
    #[must_use]
    pub const fn result(&self) -> Option<&AgentExchangeResult> {
        match self {
            Self::Settled { result, .. } | Self::AlreadySettled { result } => Some(result),
            Self::Unknown => None,
        }
    }
}

/// Durable state that carries an exchange journal.
///
/// Implement it on a participant's own state record. The journal must be a
/// persisted field of that record — not derived, not stored elsewhere — because
/// the substrate's guarantees rest on it being written in the same
/// compare-and-set as the domain transition.
pub trait AgentExchangeState: DurableState {
    /// The participant's durable saga record.
    fn exchange_journal(&self) -> &AgentExchangeJournal;

    /// Mutable access, used by the substrate inside a transition.
    fn exchange_journal_mut(&mut self) -> &mut AgentExchangeJournal;

    /// Fails closed when this state carries an unsupported schema version.
    ///
    /// The host already checks the exchange journal; implement this to check the
    /// participant's own versioned records
    /// ([specification 20](../../../docs/plans/rakka-agent/spec.md)).
    fn check_schema(&self, policy: &AgentSchemaPolicy) -> Result<(), AgentSchemaError>;
}

/// What applying one exchange did: the logical result, and any exchange the
/// receiver now owes as a consequence.
///
/// The two travel together because they commit together. A task that accepts a
/// creation and, in the same transition, decides an assignment owes the run's
/// creation command *atomically with the decision that caused it* — which is why
/// the canonical flow of
/// [specification 9.8](../../../docs/plans/rakka-agent/spec.md) can never
/// produce a task that assigned work it forgot to send.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentExchangeTransition {
    result: AgentExchangeResult,
    owed: Vec<AgentExchangeEnvelope>,
}

impl AgentExchangeTransition {
    /// A transition that returns a result and owes nothing further.
    #[must_use]
    pub const fn new(result: AgentExchangeResult) -> Self {
        Self {
            result,
            owed: Vec::new(),
        }
    }

    /// Adds an exchange this entity now owes.
    #[must_use]
    pub fn owing(mut self, envelope: AgentExchangeEnvelope) -> Self {
        self.owed.push(envelope);
        self
    }

    /// The logical result a replay must return again.
    #[must_use]
    pub const fn result(&self) -> &AgentExchangeResult {
        &self.result
    }

    /// Exchanges the transition now owes.
    #[must_use]
    pub fn owed(&self) -> &[AgentExchangeEnvelope] {
        &self.owed
    }

    fn into_parts(self) -> (AgentExchangeResult, Vec<AgentExchangeEnvelope>) {
        (self.result, self.owed)
    }
}

/// The domain half of a choreography participant.
///
/// The substrate owns durability, deduplication, re-drive, and routing. This
/// trait owns only the bounded, in-memory state transitions each exchange
/// causes, which is exactly the execution rule of
/// [specification 9.5](../../../docs/plans/rakka-agent/spec.md): a handler
/// performs a bounded transition and nothing else. No method here may perform
/// I/O, and none may fail transiently — a rejection is a durable decision, and
/// there is nothing else to report.
pub trait AgentExchangeParticipant: Send + Sync + 'static {
    /// Durable state this participant owns.
    type State: AgentExchangeState;

    /// The state of an entity that has never been written.
    ///
    /// The first exchange an entity receives — a task's creation, for instance —
    /// transitions this initial state, so creation needs no separate write path.
    fn initialize(&self, address: &AgentEntityAddress, now: AgentTimestampMillis) -> Self::State;

    /// Applies one exchange, returning the logical result that a replay of the
    /// same operation id must return again, plus any exchange this entity now
    /// owes as a consequence.
    ///
    /// The substrate deduplicates within a bounded window, so this transition
    /// MUST also be fenced by the participant's own durable state: a replay that
    /// has aged out of the window has to be rejected on the domain's terms (a
    /// stale generation, a lifecycle that has moved on) rather than applied a
    /// second time. See the module documentation.
    ///
    /// `now` is when this transition commits on the receiving owner. Durable
    /// timestamps the transition writes come from it, never from
    /// [`AgentExchangeEnvelope::created_at`], which is the *initiator's* clock
    /// at the earlier moment the envelope was recorded.
    fn apply(
        &self,
        state: &mut Self::State,
        envelope: &AgentExchangeEnvelope,
        now: AgentTimestampMillis,
    ) -> AgentExchangeTransition;

    /// Validates a reply before it settles, failing closed without a write.
    ///
    /// [`Self::settle`] may not fail: by the time it runs, the exchange is
    /// settled in the same compare-and-set. This hook runs first, and an error
    /// here persists nothing — the exchange stays outstanding and is re-driven
    /// later. It exists so a reply this binary cannot interpret (a payload
    /// serialized by a newer binary during a rolling upgrade, for instance) is
    /// refused where it enters rather than converted into a durable guess.
    fn check_settle(
        &self,
        _envelope: &AgentExchangeEnvelope,
        _result: &AgentExchangeResult,
    ) -> AgentChoreographyResult<()> {
        Ok(())
    }

    /// Applies the consequence of a result one of this entity's own exchanges
    /// returned, and returns any exchange that consequence now owes.
    ///
    /// It runs exactly once per operation id: the substrate settles the pending
    /// exchange in the same transition, and a duplicate reply never reaches it —
    /// nor does a reply that [`Self::check_settle`] refused.
    /// `now` is when the settlement commits, exactly as on [`Self::apply`].
    fn settle(
        &self,
        state: &mut Self::State,
        envelope: &AgentExchangeEnvelope,
        result: &AgentExchangeResult,
        now: AgentTimestampMillis,
    ) -> Vec<AgentExchangeEnvelope>;
}

/// The durable host of one choreography participant.
///
/// It owns recovery, the fail-closed schema check, the compare-and-set write of
/// every transition, and the journal bookkeeping that makes an exchange
/// converge. A participant supplies only bounded state transitions, and an actor
/// — sharded or not — is a thin shell over this type.
pub struct AgentExchangeHost<P, Store>
where
    P: AgentExchangeParticipant,
    Store: DurableStateStore<P::State>,
{
    address: AgentEntityAddress,
    persistence_id: PersistenceId,
    participant: P,
    store: Store,
    policy: AgentSchemaPolicy,
    record: Option<StateRecord<P::State>>,
}

impl<P, Store> Debug for AgentExchangeHost<P, Store>
where
    P: AgentExchangeParticipant,
    Store: DurableStateStore<P::State>,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentExchangeHost")
            .field("address", &self.address)
            .field("backend", &self.store.backend_name())
            .field("recovered", &self.record.is_some())
            .finish_non_exhaustive()
    }
}

impl<P, Store> AgentExchangeHost<P, Store>
where
    P: AgentExchangeParticipant,
    Store: DurableStateStore<P::State>,
{
    /// Creates a durable host for one participant address.
    #[must_use]
    pub fn new(address: AgentEntityAddress, participant: P, store: Store) -> Self {
        let persistence_id = address.persistence_id();
        Self {
            address,
            persistence_id,
            participant,
            store,
            policy: AgentSchemaPolicy::default(),
            record: None,
        }
    }

    /// Uses an explicit schema-compatibility policy.
    #[must_use]
    pub fn with_schema_policy(mut self, policy: AgentSchemaPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// The schema-compatibility policy in force.
    #[must_use]
    pub const fn schema_policy(&self) -> &AgentSchemaPolicy {
        &self.policy
    }

    /// Address this host serves.
    #[must_use]
    pub const fn address(&self) -> &AgentEntityAddress {
        &self.address
    }

    /// Durable persistence id of this participant's state.
    #[must_use]
    pub const fn persistence_id(&self) -> &PersistenceId {
        &self.persistence_id
    }

    /// Loads durable state, failing closed on an unsupported schema version.
    ///
    /// An address that has never been written recovers to the participant's
    /// initial state at [`Revision::INITIAL`]; nothing is persisted until a
    /// transition needs it.
    pub async fn recover(
        &mut self,
        now: AgentTimestampMillis,
    ) -> AgentChoreographyResult<&P::State> {
        let loaded = self.store.load(&self.persistence_id).await?;
        let record = match loaded {
            Some(record) => {
                self.policy.check_record(record.state.exchange_journal())?;
                record.state.check_schema(&self.policy)?;
                record
            }
            None => StateRecord::missing(self.participant.initialize(&self.address, now)),
        };
        self.record = Some(record);
        Ok(&self
            .record
            .as_ref()
            .expect("the record was just recovered")
            .state)
    }

    /// Currently recovered state.
    pub fn state(&self) -> AgentChoreographyResult<&P::State> {
        self.record
            .as_ref()
            .map(|record| &record.state)
            .ok_or_else(|| AgentChoreographyError::NotRecovered {
                address: Box::new(self.address.clone()),
            })
    }

    /// Every exchange this entity still owes, from durable state.
    pub fn outstanding(&self) -> AgentChoreographyResult<Vec<AgentExchangeEnvelope>> {
        Ok(self
            .state()?
            .exchange_journal()
            .outstanding()
            .iter()
            .map(|pending| pending.envelope().clone())
            .collect())
    }

    /// Starts a saga: runs one bounded transition that records the exchanges it
    /// owes, and commits both in a single compare-and-set.
    ///
    /// After it returns, the exchanges are durable. Losing the entity now, before
    /// anything was sent, costs nothing: recovery finds them in
    /// [`Self::outstanding`] and re-drives them under the same operation ids.
    ///
    /// This is the saga's *entry point* — an A2A ingress creating a task, an
    /// operator starting work — and it is the one place the substrate cannot
    /// deduplicate the cause for you: it has no accepted operation id to key on.
    /// An entry-point transition that mutates domain state (an escrow debit, for
    /// instance) must therefore guard itself with
    /// [`AgentExchangeJournal::has_initiated`], or its caller must accept the
    /// command durably first. Every transition *inside* a saga —
    /// [`Self::accept`] and [`Self::settle`] — is already deduplicated, and may
    /// owe further exchanges without any such care.
    pub async fn initiate<F>(
        &mut self,
        now: AgentTimestampMillis,
        transition: F,
    ) -> AgentChoreographyResult<Vec<AgentExchangeInitiation>>
    where
        F: FnOnce(&mut P::State) -> AgentChoreographyResult<Vec<AgentExchangeEnvelope>>,
    {
        let record = self.recovered_record()?;
        let mut state = record.state.clone();
        let envelopes = transition(&mut state)?;
        let initiations = self.record_owed(&mut state, envelopes, now)?;
        self.persist(state, record.revision).await?;
        Ok(initiations)
    }

    /// Accepts one delivered exchange: the receiver's half of the saga.
    ///
    /// A replayed operation id is answered from the journal — the original
    /// logical result, no transition, no write. A new one transitions the
    /// participant and records its result in the same compare-and-set, so the
    /// result is durable before the reply leaves.
    pub async fn accept(
        &mut self,
        envelope: &AgentExchangeEnvelope,
        now: AgentTimestampMillis,
    ) -> AgentChoreographyResult<AgentExchangeReply> {
        self.policy.check_record(envelope)?;
        envelope.validate()?;
        if envelope.target() != &self.address {
            return Err(AgentChoreographyError::Misrouted {
                target: Box::new(envelope.target().clone()),
                host: Box::new(self.address.clone()),
            });
        }

        let record = self.recovered_record()?;
        if let Some(result) = record
            .state
            .exchange_journal()
            .applied_result(envelope.operation_id(), envelope.kind())?
        {
            return Ok(AgentExchangeReply::replayed(envelope, result.clone(), now));
        }

        let mut state = record.state.clone();
        let (result, owed) = self
            .participant
            .apply(&mut state, envelope, now)
            .into_parts();
        let owed = propagate_exchange_telemetry(envelope.telemetry(), owed);
        state.exchange_journal_mut().record_applied(
            envelope.operation_id().clone(),
            envelope.kind(),
            result.clone(),
            now,
        );
        // The result and whatever it now owes commit together, so a receiver can
        // never be durably committed to a decision whose onward exchange it
        // forgot to record.
        self.record_owed(&mut state, owed, now)?;
        self.persist(state, record.revision).await?;
        Ok(AgentExchangeReply::applied(envelope, result, now))
    }

    /// Settles one reply and applies its consequence, in a single
    /// compare-and-set.
    ///
    /// A duplicate reply settles nothing and applies no consequence; an unknown
    /// one is refused without a write. The reply's result payload is
    /// re-validated before anything is read from it, exactly as an envelope is
    /// on [`Self::accept`]: a reply decoded from the wire must not carry an
    /// unbounded payload into durable state.
    pub async fn settle(
        &mut self,
        reply: &AgentExchangeReply,
        now: AgentTimestampMillis,
    ) -> AgentChoreographyResult<AgentExchangeSettlement> {
        self.policy.check_record(reply)?;
        reply.validate()?;

        let record = self.recovered_record()?;
        let mut state = record.state.clone();
        let settlement = state.exchange_journal_mut().settle(
            reply.operation_id(),
            reply.kind(),
            reply.result().clone(),
            now,
        )?;

        let AgentExchangeSettlement::Settled { envelope, result } = &settlement else {
            // Nothing changed, so nothing is written: a duplicate reply must not
            // burn a revision, and an unknown one must not touch state at all.
            return Ok(settlement);
        };

        // The participant's fail-closed gate. An error here persists nothing —
        // the journal settle above mutated only this discarded clone — so the
        // exchange stays durably outstanding and is re-driven later, possibly
        // by a binary that can interpret the reply.
        self.participant.check_settle(envelope, result)?;

        let owed = self.participant.settle(&mut state, envelope, result, now);
        let owed = propagate_exchange_telemetry(envelope.telemetry(), owed);
        self.record_owed(&mut state, owed, now)?;
        self.persist(state, record.revision).await?;
        Ok(settlement)
    }

    /// Records the exchanges a transition now owes, rejecting any that this
    /// entity has no standing to initiate.
    fn record_owed(
        &self,
        state: &mut P::State,
        envelopes: Vec<AgentExchangeEnvelope>,
        now: AgentTimestampMillis,
    ) -> AgentChoreographyResult<Vec<AgentExchangeInitiation>> {
        let mut initiations = Vec::with_capacity(envelopes.len());
        for envelope in envelopes {
            if envelope.initiator() != &self.address {
                return Err(AgentChoreographyError::ForeignInitiator {
                    initiator: Box::new(envelope.initiator().clone()),
                    host: Box::new(self.address.clone()),
                });
            }
            initiations.push(state.exchange_journal_mut().initiate(envelope, now)?);
        }
        Ok(initiations)
    }

    /// Records a failed delivery attempt against one outstanding exchange.
    ///
    /// The exchange stays outstanding: a transport failure is not evidence that
    /// the receiver did not apply it.
    pub async fn record_delivery_failure(
        &mut self,
        operation_id: &AgentOperationId,
        code: &str,
        now: AgentTimestampMillis,
    ) -> AgentChoreographyResult<()> {
        let record = self.recovered_record()?;
        let mut state = record.state.clone();
        if !state
            .exchange_journal_mut()
            .record_delivery_failure(operation_id, code, now)
        {
            return Ok(());
        }
        self.persist(state, record.revision).await?;
        Ok(())
    }

    fn recovered_record(&self) -> AgentChoreographyResult<StateRecord<P::State>> {
        self.record
            .clone()
            .ok_or_else(|| AgentChoreographyError::NotRecovered {
                address: Box::new(self.address.clone()),
            })
    }

    async fn persist(
        &mut self,
        state: P::State,
        expected_revision: Revision,
    ) -> AgentChoreographyResult<()> {
        match self
            .store
            .compare_and_set(&self.persistence_id, expected_revision, state)
            .await
        {
            Ok(persisted) => {
                self.record = Some(persisted);
                Ok(())
            }
            Err(error) => {
                if matches!(error, DurableError::RevisionConflict { .. }) {
                    // Someone else wrote this entity's state, so every transition
                    // computed from the cached record is now wrong. Drop it: the
                    // next call recovers the authoritative record instead of
                    // failing forever against a revision that no longer exists.
                    self.record = None;
                }
                Err(error.into())
            }
        }
    }
}

/// Boxed future returned by an exchange transport.
pub type AgentExchangeDeliveryFuture<'a> =
    Pin<Box<dyn Future<Output = AgentExchangeDeliveryResult> + Send + 'a>>;

/// The outcome of one delivery attempt.
pub type AgentExchangeDeliveryResult = Result<AgentExchangeReply, AgentExchangeDeliveryError>;

/// Delivers an exchange envelope to the entity that owns its target address.
///
/// Delivery is at-most-once, like every other message in Rakka. A transport
/// implementation must never compensate for that by retrying internally in a way
/// that changes the envelope, and a caller must never read a delivery failure as
/// evidence that the receiver did not apply the exchange
/// ([specification 9.8](../../../docs/plans/rakka-agent/spec.md)). The only safe
/// response to a failure is to keep the exchange outstanding and re-drive the
/// same operation id.
pub trait AgentExchangeTransport: Send + Sync {
    /// Attempts one delivery.
    fn deliver<'a>(
        &'a self,
        envelope: &'a AgentExchangeEnvelope,
    ) -> AgentExchangeDeliveryFuture<'a>;
}

/// A delivery attempt that did not produce a reply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentExchangeDeliveryError {
    code: String,
    message: String,
}

impl AgentExchangeDeliveryError {
    /// Creates a delivery failure.
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }

    /// Stable machine-readable code, used as the pending exchange's last failure
    /// code and as a bounded metric label.
    #[must_use]
    pub fn code(&self) -> &str {
        &self.code
    }

    /// Human-readable detail.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl Display for AgentExchangeDeliveryError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl Error for AgentExchangeDeliveryError {}

/// What one pass of the courier did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AgentExchangeDriveReport {
    /// Exchanges that were outstanding when the pass started.
    pub outstanding: usize,
    /// Exchanges settled by this pass.
    pub settled: usize,
    /// Replies for exchanges that were already settled.
    pub duplicate_replies: usize,
    /// Replies the entity had no record of owing.
    pub unknown_replies: usize,
    /// Delivery attempts that failed and left their exchange outstanding.
    pub failed: usize,
}

/// Drives every exchange one entity owes, once.
///
/// This is the courier, and it is the only thing that ever sends. It is safe to
/// call at any time and from any node: it re-drives from durable state under the
/// operation ids the initiating transition minted, so calling it after a
/// transition, after recovery, or on a timer are the same operation. Exchanges
/// that fail to deliver stay outstanding for the next pass.
pub async fn drive_pending_exchanges<P, Store, T>(
    host: &mut AgentExchangeHost<P, Store>,
    transport: &T,
    now: AgentTimestampMillis,
) -> AgentChoreographyResult<AgentExchangeDriveReport>
where
    P: AgentExchangeParticipant,
    Store: DurableStateStore<P::State>,
    T: AgentExchangeTransport + ?Sized,
{
    let outstanding = host.outstanding()?;
    let mut report = AgentExchangeDriveReport {
        outstanding: outstanding.len(),
        ..AgentExchangeDriveReport::default()
    };

    for envelope in outstanding {
        match transport.deliver(&envelope).await {
            Ok(reply) => {
                if reply.operation_id() != envelope.operation_id() {
                    return Err(AgentChoreographyError::MismatchedReply {
                        expected: envelope.operation_id().clone(),
                        actual: reply.operation_id().clone(),
                    });
                }
                match host.settle(&reply, now).await? {
                    AgentExchangeSettlement::Settled { .. } => report.settled += 1,
                    AgentExchangeSettlement::AlreadySettled { .. } => {
                        report.duplicate_replies += 1;
                    }
                    AgentExchangeSettlement::Unknown => report.unknown_replies += 1,
                }
            }
            Err(error) => {
                host.record_delivery_failure(envelope.operation_id(), error.code(), now)
                    .await?;
                report.failed += 1;
            }
        }
    }

    Ok(report)
}

/// The process-local message every choreography participant entity accepts.
///
/// It pairs the serializable envelope with a node-local reply channel, exactly
/// as [`crate::agent::AgentEntityMessage`] does: the channel never crosses a node
/// boundary, and [`ShardedExchangeRoute`] reconstructs it on the owning node from
/// the envelope that arrived over `rakka-remote`.
pub struct AgentExchangeMessage {
    /// The exchange to apply.
    pub envelope: AgentExchangeEnvelope,
    /// Where the reply goes.
    pub reply_to: ReplyTo<AgentExchangeReply>,
}

impl Debug for AgentExchangeMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentExchangeMessage")
            .field("envelope", &self.envelope)
            .finish_non_exhaustive()
    }
}

/// Routes exchanges to one class of sharded entity.
///
/// The route resolves the target's shard owner and then either asks the local
/// entity or asks the owning node over `rakka-remote`. Both paths deliver the
/// same envelope to the same durable [`AgentExchangeHost::accept`], which is what
/// "no colocated shortcut" means in practice: colocation changes the transport,
/// never the durable path, so an exchange cannot behave differently after its
/// entities move apart.
pub struct ShardedExchangeRoute<M, T>
where
    M: Message,
    T: RemoteTransport,
{
    sharding: ClusterSharding,
    key: EntityTypeKey<M>,
    ask_client: RemoteEntityAskClient<T>,
    ask_timeout: Duration,
    #[allow(clippy::type_complexity)]
    build: Arc<dyn Fn(AgentExchangeEnvelope, ReplyTo<AgentExchangeReply>) -> M + Send + Sync>,
}

impl<M, T> Debug for ShardedExchangeRoute<M, T>
where
    M: Message,
    T: RemoteTransport,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardedExchangeRoute")
            .field("entity_type", self.key.entity_type())
            .field("ask_timeout", &self.ask_timeout)
            .finish_non_exhaustive()
    }
}

impl<M, T> ShardedExchangeRoute<M, T>
where
    M: Message,
    T: RemoteTransport,
{
    /// Creates a route to one sharded entity type.
    ///
    /// `build` reconstructs the entity's own message from the envelope and a
    /// node-local reply channel, so an entity is free to accept exchanges
    /// alongside its other commands.
    pub fn new(
        sharding: ClusterSharding,
        key: EntityTypeKey<M>,
        ask_client: RemoteEntityAskClient<T>,
        ask_timeout: Duration,
        build: impl Fn(AgentExchangeEnvelope, ReplyTo<AgentExchangeReply>) -> M + Send + Sync + 'static,
    ) -> Self {
        Self {
            sharding,
            key,
            ask_client,
            ask_timeout,
            build: Arc::new(build),
        }
    }
}

/// Resolves an exchange envelope's sharded target and whether this node owns
/// its shard. The middle element is the owning node rendered for diagnostics,
/// since `NodeId` itself is not part of this crate's dependency surface.
///
/// Both [`ShardedExchangeRoute`] and the testkit's
/// [`crate::testkit::LocalShardedExchangeRoute`] resolve through this one
/// function, so the local arm a single-node test exercises cannot drift from
/// the arm production takes when the owner is colocated.
pub(crate) fn resolve_sharded_exchange_target<M>(
    sharding: &ClusterSharding,
    key: &EntityTypeKey<M>,
    envelope: &AgentExchangeEnvelope,
) -> Result<(rakka_sharding::ShardedEntityRef<M>, String, bool), AgentExchangeDeliveryError>
where
    M: Message,
{
    let entity = sharding
        .entity_ref_for(key, envelope.target().entity_id().as_str().to_string())
        .map_err(|error| AgentExchangeDeliveryError::new("exchange-no-route", error.to_string()))?;
    let (owner, _shard) = entity
        .region()
        .resolve(entity.entity_ref())
        .map_err(|error| AgentExchangeDeliveryError::new("exchange-no-route", error.to_string()))?;
    let is_local = entity
        .region()
        .local_node_id()
        .is_some_and(|local| local == &owner);
    Ok((entity, format!("{owner:?}"), is_local))
}

/// Delivers the envelope to a locally owned sharded entity by ask.
///
/// This is the production local arm; the testkit's single-node route calls
/// the same function, never a reimplementation of it.
pub(crate) async fn ask_local_sharded_entity<M>(
    entity: &rakka_sharding::ShardedEntityRef<M>,
    build: &Arc<dyn Fn(AgentExchangeEnvelope, ReplyTo<AgentExchangeReply>) -> M + Send + Sync>,
    envelope: &AgentExchangeEnvelope,
    ask_timeout: Duration,
) -> Result<AgentExchangeReply, AgentExchangeDeliveryError>
where
    M: Message,
{
    let envelope = envelope.clone();
    let build = build.clone();
    entity
        .ask(move |reply_to| (build)(envelope, reply_to), ask_timeout)
        .await
        .map_err(|error| AgentExchangeDeliveryError::new("exchange-ask-failed", error.to_string()))
}

impl<M, T> AgentExchangeTransport for ShardedExchangeRoute<M, T>
where
    M: Message + Sync,
    T: RemoteTransport,
{
    fn deliver<'a>(
        &'a self,
        envelope: &'a AgentExchangeEnvelope,
    ) -> AgentExchangeDeliveryFuture<'a> {
        Box::pin(async move {
            let (entity, _owner, is_local) =
                resolve_sharded_exchange_target(&self.sharding, &self.key, envelope)?;

            if is_local {
                ask_local_sharded_entity(&entity, &self.build, envelope, self.ask_timeout).await
            } else {
                entity
                    .remote_ask::<AgentExchangeEnvelope, AgentExchangeReply, T>(
                        &self.ask_client,
                        envelope.clone(),
                        self.ask_timeout,
                    )
                    .await
                    .map_err(|error| {
                        AgentExchangeDeliveryError::new(
                            "exchange-remote-ask-failed",
                            error.to_string(),
                        )
                    })
            }
        })
    }
}

/// Routes each exchange to the transport that serves its target's entity class.
///
/// Production registers a [`ShardedExchangeRoute`] per class; a test may register
/// an in-process transport for the same class. The initiator's code is identical
/// either way, and so is the durable path.
#[derive(Clone, Default)]
pub struct AgentExchangeRouter {
    routes: BTreeMap<AgentEntityClass, Arc<dyn AgentExchangeTransport>>,
}

impl Debug for AgentExchangeRouter {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentExchangeRouter")
            .field("classes", &self.routes.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl AgentExchangeRouter {
    /// A router with no routes.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers the transport that serves one entity class.
    #[must_use]
    pub fn with_route(
        mut self,
        class: AgentEntityClass,
        transport: Arc<dyn AgentExchangeTransport>,
    ) -> Self {
        self.routes.insert(class, transport);
        self
    }
}

impl AgentExchangeTransport for AgentExchangeRouter {
    fn deliver<'a>(
        &'a self,
        envelope: &'a AgentExchangeEnvelope,
    ) -> AgentExchangeDeliveryFuture<'a> {
        Box::pin(async move {
            let class = envelope.target().class();
            let Some(transport) = self.routes.get(&class) else {
                return Err(AgentExchangeDeliveryError::new(
                    "exchange-no-route",
                    format!("no exchange route is registered for the {class} entity class"),
                ));
            };
            transport.deliver(envelope).await
        })
    }
}

/// Stable codec id of the inter-entity exchange protocol.
pub const AGENT_EXCHANGE_CODEC_ID: &str = "rakka-agent-json";

/// Stable remote message type id of [`AgentExchangeEnvelope`].
pub const AGENT_EXCHANGE_ENVELOPE_TYPE_ID: &str = "rakka.agent.ExchangeEnvelope";

/// Stable remote message type id of [`AgentExchangeReply`].
pub const AGENT_EXCHANGE_REPLY_TYPE_ID: &str = "rakka.agent.ExchangeReply";

/// Wire schema version of the inter-entity exchange protocol.
pub const AGENT_EXCHANGE_REMOTE_SCHEMA_VERSION: u32 = 1;

/// Registers the exchange envelope and reply codecs.
///
/// Call it on every node that hosts or addresses a choreography participant. The
/// codec id, the message type ids, and the wire schema version are compatibility
/// commitments: nodes on adjacent versions must agree on them to interoperate
/// ([specification 20](../../../docs/plans/rakka-agent/spec.md)).
pub fn register_agent_exchange_codecs(registry: &mut SerializationRegistry) -> RemoteResult<()> {
    registry.register::<AgentExchangeEnvelope, _>(
        JsonExchangeCodec::<AgentExchangeEnvelope>::new(AGENT_EXCHANGE_ENVELOPE_TYPE_ID),
    )?;
    registry.register::<AgentExchangeReply, _>(JsonExchangeCodec::<AgentExchangeReply>::new(
        AGENT_EXCHANGE_REPLY_TYPE_ID,
    ))?;
    Ok(())
}

struct JsonExchangeCodec<T> {
    message_type_id: &'static str,
    marker: PhantomData<fn() -> T>,
}

impl<T> JsonExchangeCodec<T> {
    const fn new(message_type_id: &'static str) -> Self {
        Self {
            message_type_id,
            marker: PhantomData,
        }
    }
}

impl<T> PayloadCodec<T> for JsonExchangeCodec<T>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn codec_id(&self) -> &str {
        AGENT_EXCHANGE_CODEC_ID
    }

    fn message_type_id(&self) -> &str {
        self.message_type_id
    }

    fn schema_version(&self) -> u32 {
        AGENT_EXCHANGE_REMOTE_SCHEMA_VERSION
    }

    fn encode(&self, message: &T) -> RemoteResult<Vec<u8>> {
        serde_json::to_vec(message).map_err(|error| RemoteError::Encode {
            codec_id: AGENT_EXCHANGE_CODEC_ID.to_string(),
            message: error.to_string(),
        })
    }

    fn decode(&self, payload: &[u8]) -> RemoteResult<T> {
        serde_json::from_slice(payload).map_err(|error| RemoteError::Decode {
            codec_id: AGENT_EXCHANGE_CODEC_ID.to_string(),
            message: error.to_string(),
        })
    }
}

/// Rejection of a choreography operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentChoreographyError {
    /// An address or identifier was malformed.
    Identity(AgentIdentityError),
    /// A record carried an unsupported schema version.
    Schema(AgentSchemaError),
    /// The durable store rejected a load or write.
    Persistence(DurableError),
    /// A transition reached the host before its state was recovered.
    NotRecovered {
        /// Address of the participant.
        address: Box<AgentEntityAddress>,
    },
    /// An envelope was delivered to an entity other than its target.
    Misrouted {
        /// Address the envelope targets.
        target: Box<AgentEntityAddress>,
        /// Address of the entity that received it.
        host: Box<AgentEntityAddress>,
    },
    /// A transition tried to owe an exchange on another entity's behalf.
    ForeignInitiator {
        /// Initiator named by the envelope.
        initiator: Box<AgentEntityAddress>,
        /// Address of the entity that recorded it.
        host: Box<AgentEntityAddress>,
    },
    /// An exchange named two different tenants.
    CrossTenantExchange {
        /// Initiator address.
        initiator: Box<AgentEntityAddress>,
        /// Target address.
        target: Box<AgentEntityAddress>,
    },
    /// One operation id was reused for two different exchanges.
    ConflictingOperation {
        /// The reused operation id.
        operation_id: AgentOperationId,
        /// Exchange the operation id was first recorded under.
        recorded: AgentExchangeKind,
        /// Exchange it was later offered for.
        offered: AgentExchangeKind,
    },
    /// A reply resolved an operation the courier did not send.
    MismatchedReply {
        /// Operation that was delivered.
        expected: AgentOperationId,
        /// Operation the reply claims to resolve.
        actual: AgentOperationId,
    },
    /// The entity already owes as many exchanges as durable state may hold.
    PendingOverflow {
        /// Maximum number of outstanding exchanges.
        maximum: usize,
    },
    /// An exchange payload exceeded the bounded size.
    PayloadTooLarge {
        /// Declared payload type.
        payload_type: String,
        /// Size of the rejected payload, in bytes.
        bytes: usize,
        /// Maximum accepted size, in bytes.
        maximum: usize,
    },
    /// An exchange payload could not be encoded.
    PayloadEncoding {
        /// Encoding failure detail.
        message: String,
    },
    /// An exchange payload could not be decoded.
    PayloadDecoding {
        /// Declared payload type.
        payload_type: String,
        /// Decoding failure detail.
        message: String,
    },
    /// An exchange payload declared a type the receiver did not expect.
    PayloadTypeMismatch {
        /// Type the receiver expected.
        expected: String,
        /// Type the payload declared.
        actual: String,
    },
    /// A refusal that does not carry the durable answer the initiator needs, so
    /// settling on it would convert a receiver's inability into a decision. The
    /// exchange stays outstanding and is re-driven until an owner that can
    /// answer it does — the same fail-closed rule
    /// [`AgentExchangeParticipant::check_settle`] applies to a payload this
    /// binary cannot decode.
    UnsettleableRefusal {
        /// Kind of the exchange.
        kind: AgentExchangeKind,
        /// Refusal code the reply carried.
        code: String,
    },
}

impl AgentChoreographyError {
    /// Stable machine-readable error code.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::Identity(error) => error.code(),
            Self::Schema(error) => error.code(),
            Self::Persistence(error) => error.code(),
            Self::NotRecovered { .. } => "exchange-not-recovered",
            Self::Misrouted { .. } => "exchange-misrouted",
            Self::ForeignInitiator { .. } => "exchange-foreign-initiator",
            Self::CrossTenantExchange { .. } => "exchange-cross-tenant",
            Self::ConflictingOperation { .. } => "exchange-operation-conflict",
            Self::MismatchedReply { .. } => "exchange-mismatched-reply",
            Self::PendingOverflow { .. } => "exchange-pending-overflow",
            Self::PayloadTooLarge { .. } => "exchange-payload-too-large",
            Self::PayloadEncoding { .. } => "exchange-payload-encoding",
            Self::PayloadDecoding { .. } => "exchange-payload-decoding",
            Self::PayloadTypeMismatch { .. } => "exchange-payload-type-mismatch",
            Self::UnsettleableRefusal { .. } => "exchange-unsettleable-refusal",
        }
    }
}

impl Display for AgentChoreographyError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Identity(error) => Display::fmt(error, f),
            Self::Schema(error) => Display::fmt(error, f),
            Self::Persistence(error) => Display::fmt(error, f),
            Self::NotRecovered { address } => {
                write!(f, "entity {address} transitioned before its state recovered")
            }
            Self::Misrouted { target, host } => write!(
                f,
                "an exchange targeting {target} was delivered to {host}"
            ),
            Self::ForeignInitiator { initiator, host } => write!(
                f,
                "entity {host} may not owe an exchange initiated by {initiator}"
            ),
            Self::CrossTenantExchange { initiator, target } => write!(
                f,
                "an exchange may not cross a tenant boundary: {initiator} to {target}"
            ),
            Self::ConflictingOperation {
                operation_id,
                recorded,
                offered,
            } => write!(
                f,
                "operation {operation_id} is recorded as a {recorded} exchange and cannot be reused for a {offered} exchange"
            ),
            Self::MismatchedReply { expected, actual } => write!(
                f,
                "a reply for operation {actual} arrived for the delivery of operation {expected}"
            ),
            Self::PendingOverflow { maximum } => write!(
                f,
                "an entity may not owe more than {maximum} outstanding exchanges"
            ),
            Self::PayloadTooLarge {
                payload_type,
                bytes,
                maximum,
            } => write!(
                f,
                "a {payload_type} exchange payload is {bytes} bytes, which exceeds the {maximum} byte limit"
            ),
            Self::PayloadEncoding { message } => {
                write!(f, "an exchange payload could not be encoded: {message}")
            }
            Self::PayloadDecoding {
                payload_type,
                message,
            } => write!(
                f,
                "a {payload_type} exchange payload could not be decoded: {message}"
            ),
            Self::PayloadTypeMismatch { expected, actual } => write!(
                f,
                "an exchange payload declared type {actual} where {expected} was expected"
            ),
            Self::UnsettleableRefusal { kind, code } => write!(
                f,
                "a {kind} refusal carrying {code} is not the durable answer this exchange needs, so it stays outstanding"
            ),
        }
    }
}

impl Error for AgentChoreographyError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Identity(error) => Some(error),
            Self::Schema(error) => Some(error),
            Self::Persistence(error) => Some(error),
            _ => None,
        }
    }
}

impl From<AgentIdentityError> for AgentChoreographyError {
    fn from(error: AgentIdentityError) -> Self {
        Self::Identity(error)
    }
}

impl From<AgentSchemaError> for AgentChoreographyError {
    fn from(error: AgentSchemaError) -> Self {
        Self::Schema(error)
    }
}

impl From<DurableError> for AgentChoreographyError {
    fn from(error: DurableError) -> Self {
        Self::Persistence(error)
    }
}
