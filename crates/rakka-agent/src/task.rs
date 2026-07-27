//! The typed task entity, its definition, and its lifecycle.
//!
//! Owns [`AgentTaskEntity`], keyed by `(TenantId, AgentTaskId)`, with a
//! serializable command protocol; the versioned [`AgentTaskDefinition`] carrying
//! typed input and result schema references, deterministic result rules,
//! rejection limits, dependency policy, and per-task budgets; the task lifecycle
//! and its durable, bounded, acyclic dependencies; and the bounded materialized
//! state that keeps history and content behind cursors and artifact references.
//!
//! Result proposals are validated in-entity by deterministic rules only. Model-
//! assisted evaluation is a durable effect, never in-entity I/O. Tasks
//! deliberately left unassigned to an agent and completed by an authenticated
//! human or service travel the same typed validation path.
//!
//! Specification: sections 6.4, 9.1, 9.2, 9.6, and 9.8; human-owned tasks in
//! section 8.12. Filled by slice 1.4; human-owned completion by slice 5.4.
//!
//! # The entity is a choreography participant
//!
//! The task's durable state carries the [`AgentExchangeJournal`], so an exchange
//! it owes is persisted in the *same* compare-and-set as the domain transition
//! that owed it, and a decision it returns is persisted in the same
//! compare-and-set as the transition that produced it. Every cross-entity step
//! travels the substrate of [`crate::choreography`]; there is no colocated
//! shortcut.
//!
//! ```text
//! ingress (durable, deduplicated)
//!     -> Create command (operation id from the ingress)      [1 CAS]
//!     -> assignment decision, read from the agent's durable
//!        definition and admission state — never a synchronous
//!        round trip through AgentEntity                      [1 CAS + run-creation owed]
//!     -> run-creation exchange -> AgentRunEntity
//!     -> run acceptance reply -> task InProgress             [1 CAS]
//!     ...
//!     -> result-proposal exchange from the run
//!     -> deterministic validation, durable accept/reject     [1 CAS]
//!     -> the decision travels home as the exchange's reply
//! ```
//!
//! A delegating run (M4) creates a child task through the
//! [`AgentExchangeKind::Creation`] exchange instead of an ingress command; both
//! reach the same bounded transition, so the two paths cannot diverge.
//!
//! # The assignment decision is a separate transition
//!
//! Creating a task and deciding its assignment are two transitions, because the
//! decision needs the agent's durable definition and admission state and reading
//! it is I/O. The entity performs that bounded durable read *outside* the
//! transition — [`load_agent_entity_state`], never a command round trip through
//! [`crate::agent::AgentEntity`] ([specification 9.8](../../../docs/plans/rakka-agent/spec.md))
//! — and then runs a pure, bounded transition over the facts it read. The
//! transition is idempotent: it is fenced on the task's own status and current
//! assignment, and the run-creation operation id is derived from the task and
//! the assignment generation, so a replay resolves to the same run rather than a
//! second one.
//!
//! # Bounded state
//!
//! The materialized record ([`AgentTask`]) holds only what the next legal
//! transition needs: identity, status, revisions, a bounded dependency summary,
//! the *current* assignment, pending references, the accepted result reference,
//! and the terminal reason. It never accumulates: superseded assignments, result
//! proposals, and rejection decisions leave the record and become
//! [`AgentTaskHistoryEntry`] rows, readable only through the bounded, authorized
//! cursor of an [`AgentTaskHistoryStore`]
//! ([specification 9.6](../../../docs/plans/rakka-agent/spec.md)). Content beyond
//! [`AGENT_TASK_INLINE_CONTENT_MAX_BYTES`] must arrive as an artifact reference;
//! a creation whose materialized record would exceed
//! [`AGENT_TASK_MATERIALIZED_MAX_BYTES`] is rejected rather than persisted.
//!
//! History is written through a bounded outbox in the entity's own state, for
//! the same reason the exchange journal is: an entry is committed by the
//! transition that produced it, and
//! [`AgentTaskEntityStore::settle_side_effects`] appends it to the sink
//! afterwards. An append is idempotent on `(scope, sequence)`, so a flush
//! interrupted anywhere is safe to re-drive, and no transition can commit while
//! forgetting the history it owes.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rakka_agent_workflow::{
    AgentCausationId, AgentCorrelationId, AgentTelemetryContext, AgentTimestampMillis, ArtifactRef,
    StateSchemaVersion,
};
use rakka_core::{
    actor_future, Actor, ActorAction, ActorContext, ActorFuture, ActorOptions, ReplyTo,
};
use rakka_persistence::{DurableError, DurableStateStore, PersistenceId};
use rakka_sharding::{
    ClusterNodeRuntime, ClusterNodeRuntimeResult, ClusterSharding, ClusterShardingResult, Entity,
    EntityContext, EntityId, EntityTypeKey, EntityTypeRegistration, ShardBufferConfig,
    ShardedEntityRef,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::admission::AgentAdmissionRefusal;
use crate::agent::{load_agent_entity_state, AgentEntityState, AgentLifecycleStatus};
use crate::budget::{
    AgentBudgetAllocation, AgentBudgetConsumption, AgentBudgetExhaustion, AgentBudgetGrant,
    AgentEscrowChildId, AgentEscrowError, AgentEscrowLedger, AGENT_ESCROW_CHILD_CAPACITY,
};
use crate::choreography::{
    drive_pending_exchanges, AgentChoreographyError, AgentEntityAddress, AgentExchangeEnvelope,
    AgentExchangeHost, AgentExchangeJournal, AgentExchangeKind, AgentExchangeParticipant,
    AgentExchangePayload, AgentExchangeReply, AgentExchangeResult, AgentExchangeRouter,
    AgentExchangeState, AgentExchangeTransition,
};
use crate::definition::{
    AgentBudgetCeilings, AgentCapabilityId, AgentOperationClass, AgentPolicyRefs,
    AgentRevisionNumber, AgentTaskDefinitionId,
};
use crate::goal::AgentGoalMode;
use crate::identity::{
    validated_id, AgentGoalId, AgentId, AgentIdentityError, AgentOperationId, AgentOperationKind,
    AgentRunId, AgentRunScope, AgentScope, AgentTaskId, AgentTaskScope, AgentWakeId, TenantId,
    AGENT_IDENTITY_MAX_LENGTH,
};
use crate::schema::{
    AgentRecordKind, AgentSchemaError, AgentSchemaPolicy, VersionedAgentRecord,
    CURRENT_AGENT_TASK_DEFINITION_SCHEMA_VERSION, CURRENT_AGENT_TASK_HISTORY_SCHEMA_VERSION,
    CURRENT_AGENT_TASK_STATE_SCHEMA_VERSION,
};
use crate::wake::{
    AgentWakeBinding, AgentWakeControllerState, AgentWakeError, AgentWakeOutcome,
    AgentWakePolicyRevision, AgentWakeStatusView, ScheduleRevision,
};

/// Default sharded entity type of the typed-task entity.
pub const DEFAULT_AGENT_TASK_ENTITY_TYPE: &str = "RakkaAgentTask";

/// Maximum length, in bytes, of a task definition's outcome-oriented
/// description.
pub const AGENT_TASK_DESCRIPTION_MAX_LENGTH: usize = 1024;

/// Maximum length, in bytes, of any bounded free-text detail persisted by the
/// task: a rejection detail, a refusal detail, a cancellation reason, or the
/// sanitized feedback returned to a run.
pub const AGENT_TASK_DETAIL_MAX_LENGTH: usize = 512;

/// Largest inline task input or result content, in bytes, once serialized.
///
/// Anything larger must arrive as an [`ArtifactRef`]. The bound is what keeps a
/// task's materialized state, its mailbox, and the exchange payload that carries
/// its assignment all within their own limits
/// ([specification 9.6](../../../docs/plans/rakka-agent/spec.md)).
pub const AGENT_TASK_INLINE_CONTENT_MAX_BYTES: usize = 8 * 1024;

/// Largest materialized task record, in bytes, once serialized.
///
/// Checked before a creation is persisted, so an oversized definition or input
/// is refused at admission rather than discovered when durable state has already
/// grown unbounded. It bounds [`AgentTask`], the task's own domain record; the
/// exchange journal alongside it is bounded by the substrate's own constants.
pub const AGENT_TASK_MATERIALIZED_MAX_BYTES: usize = 32 * 1024;

/// Bytes of growth headroom an admitted task record keeps below
/// [`AGENT_TASK_MATERIALIZED_MAX_BYTES`].
///
/// After admission, the materialized record may still grow by at most one live
/// assignment, one assignment refusal, one rejection decision, one terminal
/// reason, and one resolution outcome per declared dependency — each of them
/// individually bounded, and together under this reserve. Creation and
/// post-creation dependency declarations therefore enforce the materialized
/// bound *minus* this reserve: a task the entity admits can never later be
/// refused its own assignment, rejection, or terminal reason because the
/// record it was admitted with left no room. It is the same reservation
/// [`AGENT_TASK_ASSIGNABLE_ID_MAX_LENGTH`] makes for derived run ids.
pub const AGENT_TASK_STATE_GROWTH_RESERVE_BYTES: usize = 6 * 1024;

/// Maximum number of deterministic result rules one task definition may carry.
pub const AGENT_TASK_MAX_RESULT_RULES: usize = 32;

/// Maximum length, in bytes, of the JSON pointer one result rule inspects.
pub const AGENT_TASK_RULE_POINTER_MAX_LENGTH: usize = 256;

/// Maximum number of permitted values one one-of result rule may carry.
pub const AGENT_TASK_RULE_ONE_OF_MAX_VALUES: usize = 32;

/// Maximum length, in bytes, of one permitted one-of value.
pub const AGENT_TASK_RULE_VALUE_MAX_LENGTH: usize = 256;

/// Maximum number of dependencies one task may declare.
pub const AGENT_TASK_MAX_DEPENDENCIES: usize = 32;

/// Maximum declared ancestor depth carried by a dependency declaration.
///
/// The ancestors are what make the dependency graph acyclic without a global
/// read: a task refuses a dependency that already lists the task itself as an
/// ancestor. The chain is bounded because durable state is.
pub const AGENT_TASK_MAX_DEPENDENCY_DEPTH: usize = 32;

/// Maximum number of evidence artifact references one result proposal may carry.
pub const AGENT_TASK_MAX_EVIDENCE_ARTIFACTS: usize = 8;

/// How many resolved operation ids the task entity remembers for deduplication.
///
/// The window is the fast path; every command transition is additionally fenced
/// by the task's own durable state, so a replay older than the window is still
/// refused rather than applied twice.
pub const AGENT_TASK_OPERATION_LOG_CAPACITY: usize = 64;

/// How many history entries the task may owe its sink at once.
///
/// An owed entry is never dropped — that would silently lose audit history — so
/// this is a bound the entity enforces at its door: a task whose sink has been
/// unreachable long enough to fill the outbox refuses further transitions rather
/// than growing an unbounded durable record.
pub const AGENT_TASK_PENDING_HISTORY_CAPACITY: usize = 64;

/// The most history entries any one transition can record.
///
/// A creation is the worst case: one row for the task, one for each dependency
/// it declares, and one for the assignment its own eligibility may decide. The
/// entity requires this much headroom before it runs a transition, which is what
/// lets recording an entry be infallible — an owed entry is never dropped, and a
/// backed-up sink is refused at the entity's door instead.
pub const AGENT_TASK_MAX_HISTORY_PER_TRANSITION: usize = AGENT_TASK_MAX_DEPENDENCIES + 2;

/// Largest page one history cursor may request.
pub const AGENT_TASK_HISTORY_MAX_PAGE_SIZE: usize = 64;

/// Default page size of a history cursor.
pub const AGENT_TASK_HISTORY_DEFAULT_PAGE_SIZE: usize = 16;

/// Payload type of an [`AgentTaskCreation`] exchange command.
pub const AGENT_TASK_CREATION_PAYLOAD_TYPE: &str = "rakka.agent.TaskCreation";

/// Payload type of an [`AgentRunAssignment`] exchange command.
pub const AGENT_RUN_ASSIGNMENT_PAYLOAD_TYPE: &str = "rakka.agent.RunAssignment";

/// Payload type of an [`AgentRunAcceptance`] exchange result.
pub const AGENT_RUN_ACCEPTANCE_PAYLOAD_TYPE: &str = "rakka.agent.RunAcceptance";

/// Payload type of an [`AgentTaskResultProposal`] exchange command.
pub const AGENT_TASK_RESULT_PROPOSAL_PAYLOAD_TYPE: &str = "rakka.agent.TaskResultProposal";

/// Payload type of an [`AgentTaskDecision`] exchange result.
pub const AGENT_TASK_DECISION_PAYLOAD_TYPE: &str = "rakka.agent.TaskDecision";

/// Refusal code of an [`AgentTaskDecision::Refused`] fenced by a newer
/// assignment generation.
///
/// It is the one refusal the run maps to a distinct terminal status
/// ([`crate::run::AgentRunStatus::Superseded`]), so both sides name it through
/// this constant rather than each holding its own copy of the literal. The
/// string is wire and durable surface — it never changes.
pub const AGENT_TASK_REFUSAL_STALE_GENERATION: &str = "stale-assignment-generation";

/// Payload type of the [`AgentTaskOutcome`] an accepted
/// [`AgentExchangeKind::Creation`] reply carries.
///
/// A refused creation carries an [`AgentTaskDecision::Refused`] under
/// [`AGENT_TASK_DECISION_PAYLOAD_TYPE`] instead; an initiator settling a
/// creation branches on the reply's payload type, and each type names exactly
/// one wire shape.
pub const AGENT_TASK_CREATION_OUTCOME_PAYLOAD_TYPE: &str = "rakka.agent.TaskCreationOutcome";

/// The longest `-gen-{generation}` suffix a derived run id can carry: the
/// separator plus a full-width `u64` generation.
const AGENT_TASK_RUN_SUFFIX_MAX_LENGTH: usize = "-gen-".len() + 20;

/// Longest id an agent-owned task may use, in bytes.
///
/// The run serving one assignment generation derives its id as
/// `{task}-gen-{generation}` ([`run_id_for_assignment`]), and the derived id
/// must itself satisfy [`AGENT_IDENTITY_MAX_LENGTH`]. Creation reserves room
/// for the longest possible suffix, so a task admitted here can never become
/// unassignable when its assignment is decided.
pub const AGENT_TASK_ASSIGNABLE_ID_MAX_LENGTH: usize =
    AGENT_IDENTITY_MAX_LENGTH - AGENT_TASK_RUN_SUFFIX_MAX_LENGTH;

const DEFAULT_AGENT_TASK_PASSIVATION_BUFFER_DURATION: Duration = Duration::from_millis(25);

/// The source of the durable timestamps a task's transitions are stamped with.
///
/// It is injectable because a durable timestamp is part of the record a
/// transition writes: a test that drives recovery has to be able to control it,
/// exactly as the choreography tests control theirs.
pub type AgentTaskClock = Arc<dyn Fn() -> AgentTimestampMillis + Send + Sync>;

/// A clock reading the system's wall clock.
#[must_use]
pub fn system_task_clock() -> AgentTaskClock {
    Arc::new(|| {
        let millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| {
                u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
            });
        AgentTimestampMillis::new(millis)
    })
}

/// Result type for typed-task operations.
pub type AgentTaskResult<T> = Result<T, AgentTaskError>;

validated_id! {
    /// Stable identity of one schema a task's input or result is validated
    /// against ([specification 9.1](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// The schema itself is application-owned. The task persists only the
    /// reference, so a durable result can always be interpreted against the
    /// exact schema revision that accepted it.
    pub AgentSchemaId, "agent_schema_id"
}

validated_id! {
    /// Stable identity of one deterministic result rule.
    ///
    /// It is persisted on every rejection, so an operator can always name the
    /// rule that refused a result
    /// ([specification 9.2](../../../docs/plans/rakka-agent/spec.md)).
    pub AgentTaskRuleId, "agent_task_rule_id"
}

/// A versioned reference to the schema a typed value must satisfy.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct AgentSchemaRef {
    /// Stable schema identity.
    pub schema_id: AgentSchemaId,
    /// Monotonic schema revision.
    pub version: AgentRevisionNumber,
}

impl AgentSchemaRef {
    /// Creates a schema reference.
    #[must_use]
    pub const fn new(schema_id: AgentSchemaId, version: AgentRevisionNumber) -> Self {
        Self { schema_id, version }
    }
}

impl Display for AgentSchemaRef {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.schema_id, self.version)
    }
}

/// Monotonic assignment generation of one task
/// ([specification 9.3](../../../docs/plans/rakka-agent/spec.md)).
///
/// Every assignment decision advances it, and it fences the run it created: a
/// proposal or acceptance carrying a generation the task has passed is refused,
/// so a superseded run can neither schedule work nor complete the public task.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct AgentAssignmentGeneration(u64);

impl AgentAssignmentGeneration {
    /// The generation of a task that has never been assigned.
    pub const UNASSIGNED: Self = Self(0);

    /// Creates a generation.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Numeric value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// The next generation.
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

impl Display for AgentAssignmentGeneration {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, f)
    }
}

/// Monotonic sequence of one task history entry.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct AgentTaskHistorySequence(u64);

impl AgentTaskHistorySequence {
    /// The first sequence a task's history uses.
    pub const FIRST: Self = Self(1);

    /// Creates a sequence.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Numeric value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// The next sequence.
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

impl Display for AgentTaskHistorySequence {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, f)
    }
}

/// Algorithm that produced an [`AgentContentDigest`].
///
/// The algorithm travels with the digest so it can be strengthened without
/// rewriting the records that already carry one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentDigestAlgorithm {
    /// FNV-1a over canonical JSON: a stable content fingerprint, and nothing
    /// more.
    Fnv1a128,
    /// SHA-256 over canonical JSON: a collision-resistant digest suitable for a
    /// security decision. This is the algorithm a digest-bound authorization
    /// grant binds ([specification 12.3](../../../docs/plans/rakka-agent/spec.md)):
    /// only a second-preimage-resistant digest can decide whether an approval
    /// still binds the exact arguments a dispatch is about to send.
    Sha256,
}

impl AgentDigestAlgorithm {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Fnv1a128 => "fnv1a-128",
            Self::Sha256 => "sha2-256",
        }
    }

    /// Whether the algorithm is a cryptographic digest a security decision may
    /// rely on. FNV-1a is a fingerprint and must never gate authorization.
    #[must_use]
    pub const fn is_cryptographic(self) -> bool {
        match self {
            Self::Fnv1a128 => false,
            Self::Sha256 => true,
        }
    }
}

impl Display for AgentDigestAlgorithm {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// A stable fingerprint of bounded content.
///
/// It identifies *which* value a durable decision was made about — the proposal
/// a rejection refused, the input an assignment carried — so an operator reading
/// history can tell one proposal from another and detect a value that changed
/// under a stable identifier
/// ([specification 9.2](../../../docs/plans/rakka-agent/spec.md)).
///
/// The default [`AgentContentDigest::of_json`] fingerprint is deliberately
/// **not** a security boundary. The digest-bound authorization grants of
/// [specification 12.3](../../../docs/plans/rakka-agent/spec.md) need a
/// cryptographic digest, so [`AgentContentDigest::sha256_of_json`] produces a
/// [`AgentDigestAlgorithm::Sha256`] digest for exactly that use; the FNV
/// fingerprint must not be used to decide whether an approval still binds.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct AgentContentDigest {
    /// Algorithm that produced the value.
    pub algorithm: AgentDigestAlgorithm,
    /// Lowercase hexadecimal digest value.
    pub value: String,
}

impl AgentContentDigest {
    /// Fingerprints a JSON value over its canonical encoding.
    ///
    /// Canonical means object keys in sorted order, so two structurally equal
    /// values always fingerprint alike regardless of how they were built or
    /// which binary serialized them.
    #[must_use]
    pub fn of_json(value: &Value) -> Self {
        let mut canonical = String::new();
        write_canonical_json(value, &mut canonical);
        Self::of_bytes(canonical.as_bytes())
    }

    /// Fingerprints raw bytes.
    #[must_use]
    pub fn of_bytes(bytes: &[u8]) -> Self {
        const OFFSET: u128 = 0x6c62_272e_07bb_0142_62b8_2175_6295_c58d;
        const PRIME: u128 = 0x0000_0000_0100_0000_0000_0000_0000_013b;

        let mut hash = OFFSET;
        for byte in bytes {
            hash ^= u128::from(*byte);
            hash = hash.wrapping_mul(PRIME);
        }
        Self {
            algorithm: AgentDigestAlgorithm::Fnv1a128,
            value: format!("{hash:032x}"),
        }
    }

    /// Computes the cryptographic [`AgentDigestAlgorithm::Sha256`] digest of a
    /// JSON value over its canonical encoding.
    ///
    /// This is the constructor a digest-bound authorization grant uses
    /// ([specification 12.3](../../../docs/plans/rakka-agent/spec.md)): the
    /// canonicalization matches [`Self::of_json`], so the same structural value
    /// always produces the same digest, and SHA-256 makes a changed argument
    /// computationally impossible to disguise under an unchanged digest.
    #[must_use]
    pub fn sha256_of_json(value: &Value) -> Self {
        let mut canonical = String::new();
        write_canonical_json(value, &mut canonical);
        Self::sha256_of_bytes(canonical.as_bytes())
    }

    /// Computes the cryptographic [`AgentDigestAlgorithm::Sha256`] digest of raw
    /// bytes.
    #[must_use]
    pub fn sha256_of_bytes(bytes: &[u8]) -> Self {
        Self {
            algorithm: AgentDigestAlgorithm::Sha256,
            value: sha256_hex(bytes),
        }
    }
}

impl Display for AgentContentDigest {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.algorithm, self.value)
    }
}

/// Writes a JSON value in a canonical form: object keys sorted, no insignificant
/// whitespace.
///
/// `serde_json` already orders map keys, but only while its `preserve_order`
/// feature is off — a feature any crate in a build may turn on. The canonical
/// form is written explicitly so a durable fingerprint can never depend on which
/// features a binary happened to unify.
fn write_canonical_json(value: &Value, out: &mut String) {
    match value {
        Value::Object(map) => {
            let ordered: BTreeMap<&String, &Value> = map.iter().collect();
            out.push('{');
            for (index, (key, entry)) in ordered.into_iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(&Value::String(key.clone()).to_string());
                out.push(':');
                write_canonical_json(entry, out);
            }
            out.push('}');
        }
        Value::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_canonical_json(item, out);
            }
            out.push(']');
        }
        other => out.push_str(&other.to_string()),
    }
}

/// Computes the SHA-256 digest of `bytes` as a lowercase hexadecimal string.
///
/// SHA-256 is implemented inline in safe Rust — exactly as the FNV-1a
/// fingerprint above is — so a security-relevant digest depends on no external
/// crate and stays fully reviewable under this crate's `forbid(unsafe_code)`.
/// The algorithm is FIPS 180-4; the round constants and initial hash values are
/// the standard ones, and the unit tests pin the empty-string and `"abc"`
/// vectors.
#[must_use]
fn sha256_hex(bytes: &[u8]) -> String {
    // First 32 bits of the fractional parts of the square roots of the first
    // eight primes.
    let mut h: [u32; 8] = [
        0x6a09_e667,
        0xbb67_ae85,
        0x3c6e_f372,
        0xa54f_f53a,
        0x510e_527f,
        0x9b05_688c,
        0x1f83_d9ab,
        0x5be0_cd19,
    ];
    // First 32 bits of the fractional parts of the cube roots of the first
    // sixty-four primes.
    const K: [u32; 64] = [
        0x428a_2f98,
        0x7137_4491,
        0xb5c0_fbcf,
        0xe9b5_dba5,
        0x3956_c25b,
        0x59f1_11f1,
        0x923f_82a4,
        0xab1c_5ed5,
        0xd807_aa98,
        0x1283_5b01,
        0x2431_85be,
        0x550c_7dc3,
        0x72be_5d74,
        0x80de_b1fe,
        0x9bdc_06a7,
        0xc19b_f174,
        0xe49b_69c1,
        0xefbe_4786,
        0x0fc1_9dc6,
        0x240c_a1cc,
        0x2de9_2c6f,
        0x4a74_84aa,
        0x5cb0_a9dc,
        0x76f9_88da,
        0x983e_5152,
        0xa831_c66d,
        0xb003_27c8,
        0xbf59_7fc7,
        0xc6e0_0bf3,
        0xd5a7_9147,
        0x06ca_6351,
        0x1429_2967,
        0x27b7_0a85,
        0x2e1b_2138,
        0x4d2c_6dfc,
        0x5338_0d13,
        0x650a_7354,
        0x766a_0abb,
        0x81c2_c92e,
        0x9272_2c85,
        0xa2bf_e8a1,
        0xa81a_664b,
        0xc24b_8b70,
        0xc76c_51a3,
        0xd192_e819,
        0xd699_0624,
        0xf40e_3585,
        0x106a_a070,
        0x19a4_c116,
        0x1e37_6c08,
        0x2748_774c,
        0x34b0_bcb5,
        0x391c_0cb3,
        0x4ed8_aa4a,
        0x5b9c_ca4f,
        0x682e_6ff3,
        0x748f_82ee,
        0x78a5_636f,
        0x84c8_7814,
        0x8cc7_0208,
        0x90be_fffa,
        0xa450_6ceb,
        0xbef9_a3f7,
        0xc671_78f2,
    ];

    // Padding: 0x80, then zeros to 56 mod 64, then the 64-bit big-endian bit
    // length.
    let bit_len = (bytes.len() as u64).wrapping_mul(8);
    let mut message = bytes.to_vec();
    message.push(0x80);
    while message.len() % 64 != 56 {
        message.push(0);
    }
    message.extend_from_slice(&bit_len.to_be_bytes());

    for chunk in message.chunks_exact(64) {
        let mut w = [0u32; 64];
        for (i, word) in w.iter_mut().enumerate().take(16) {
            let base = i * 4;
            *word = u32::from_be_bytes([
                chunk[base],
                chunk[base + 1],
                chunk[base + 2],
                chunk[base + 3],
            ]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }

        let mut a = h[0];
        let mut b = h[1];
        let mut c = h[2];
        let mut d = h[3];
        let mut e = h[4];
        let mut f = h[5];
        let mut g = h[6];
        let mut hh = h[7];

        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let temp1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(maj);

            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(temp1);
            d = c;
            c = b;
            b = a;
            a = temp1.wrapping_add(temp2);
        }

        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
        h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g);
        h[7] = h[7].wrapping_add(hh);
    }

    let mut hex = String::with_capacity(64);
    for word in h {
        hex.push_str(&format!("{word:08x}"));
    }
    hex
}

/// Bounded task content: an inline value, or a reference to one.
///
/// Attachments and results are immutable: an artifact reference carries its own
/// digest, media type, size, classification, and provenance, and content loading
/// happens through bounded adapters, never inside a task transition. A resolved
/// credential never appears in either variant
/// ([specification 9.2](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentTaskContent {
    /// A bounded inline value, at most [`AGENT_TASK_INLINE_CONTENT_MAX_BYTES`]
    /// serialized bytes.
    Inline(Value),
    /// An immutable reference to application-owned content.
    Artifact(Box<ArtifactRef>),
}

impl AgentTaskContent {
    /// Creates inline content, rejecting a value that exceeds the inline bound.
    pub fn inline(value: Value) -> AgentTaskResult<Self> {
        let content = Self::Inline(value);
        content.validate()?;
        Ok(content)
    }

    /// Creates content held behind an artifact reference.
    #[must_use]
    pub fn artifact(artifact: ArtifactRef) -> Self {
        Self::Artifact(Box::new(artifact))
    }

    /// The inline value, when the content is inline.
    #[must_use]
    pub const fn inline_value(&self) -> Option<&Value> {
        match self {
            Self::Inline(value) => Some(value),
            Self::Artifact(_) => None,
        }
    }

    /// The artifact reference, when the content is held behind one.
    #[must_use]
    pub fn artifact_ref(&self) -> Option<&ArtifactRef> {
        match self {
            Self::Inline(_) => None,
            Self::Artifact(artifact) => Some(artifact),
        }
    }

    /// Stable fingerprint of the content.
    ///
    /// Inline content is fingerprinted over its canonical encoding; artifact
    /// content is fingerprinted over its immutable reference, because the task
    /// never loads the bytes.
    #[must_use]
    pub fn digest(&self) -> AgentContentDigest {
        match self {
            Self::Inline(value) => AgentContentDigest::of_json(value),
            Self::Artifact(artifact) => {
                let identity = format!(
                    "{}|{}|{}",
                    artifact.artifact_id,
                    artifact.uri,
                    artifact.checksum.as_deref().unwrap_or_default()
                );
                AgentContentDigest::of_bytes(identity.as_bytes())
            }
        }
    }

    /// Serialized size of the content, in bytes.
    #[must_use]
    pub fn size_bytes(&self) -> usize {
        serde_json::to_vec(self)
            .map(|bytes| bytes.len())
            .unwrap_or(0)
    }

    /// Rejects inline content that exceeds the inline bound.
    pub fn validate(&self) -> AgentTaskResult<()> {
        let Self::Inline(value) = self else {
            return Ok(());
        };
        let bytes = serde_json::to_vec(value)
            .map_err(|error| AgentTaskError::Encoding {
                message: error.to_string(),
            })?
            .len();
        if bytes > AGENT_TASK_INLINE_CONTENT_MAX_BYTES {
            return Err(AgentTaskError::ContentTooLarge {
                bytes,
                maximum: AGENT_TASK_INLINE_CONTENT_MAX_BYTES,
            });
        }
        Ok(())
    }
}

/// One deterministic check a proposed result must pass
/// ([specification 9.2](../../../docs/plans/rakka-agent/spec.md)).
///
/// Every variant is a pure function of the proposed value. Nothing here can call
/// a model, load an artifact, or reach a service: a model-assisted or external
/// evaluator is a durable effect that returns evidence to the task, never I/O
/// inside the task's transition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentTaskResultCheck {
    /// The value at the JSON pointer must be present and not null.
    Required {
        /// JSON pointer into the proposed result.
        pointer: String,
    },
    /// The value at the JSON pointer must be absent or null.
    Forbidden {
        /// JSON pointer into the proposed result.
        pointer: String,
    },
    /// The value at the JSON pointer must be a non-empty string.
    NonEmptyString {
        /// JSON pointer into the proposed result.
        pointer: String,
    },
    /// The value at the JSON pointer must be an integer within an inclusive
    /// range.
    IntegerRange {
        /// JSON pointer into the proposed result.
        pointer: String,
        /// Inclusive lower bound, when bounded.
        minimum: Option<i64>,
        /// Inclusive upper bound, when bounded.
        maximum: Option<i64>,
    },
    /// The string at the JSON pointer must be one of a bounded set.
    OneOf {
        /// JSON pointer into the proposed result.
        pointer: String,
        /// Permitted values.
        values: BTreeSet<String>,
    },
    /// The proposal must carry at least one evidence artifact.
    EvidenceRequired,
}

impl AgentTaskResultCheck {
    /// Rejects a check whose content cannot be bounded.
    ///
    /// A rule travels inside the definition, and the definition is part of the
    /// materialized record, so an unbounded pointer or value set would let a
    /// definition grow the record without limit.
    fn validate(&self) -> AgentTaskResult<()> {
        let pointer = match self {
            Self::Required { pointer }
            | Self::Forbidden { pointer }
            | Self::NonEmptyString { pointer }
            | Self::IntegerRange { pointer, .. }
            | Self::OneOf { pointer, .. } => Some(pointer),
            Self::EvidenceRequired => None,
        };
        if let Some(pointer) = pointer {
            if pointer.len() > AGENT_TASK_RULE_POINTER_MAX_LENGTH {
                return Err(AgentTaskError::InvalidDefinition {
                    detail: format!(
                        "a result rule's JSON pointer is {} bytes, which exceeds the \
                         {AGENT_TASK_RULE_POINTER_MAX_LENGTH} byte limit",
                        pointer.len()
                    ),
                });
            }
        }
        if let Self::OneOf { values, .. } = self {
            if values.len() > AGENT_TASK_RULE_ONE_OF_MAX_VALUES {
                return Err(AgentTaskError::InvalidDefinition {
                    detail: format!(
                        "a one-of result rule may permit at most \
                         {AGENT_TASK_RULE_ONE_OF_MAX_VALUES} values"
                    ),
                });
            }
            if let Some(value) = values
                .iter()
                .find(|value| value.len() > AGENT_TASK_RULE_VALUE_MAX_LENGTH)
            {
                return Err(AgentTaskError::InvalidDefinition {
                    detail: format!(
                        "a permitted one-of value is {} bytes, which exceeds the \
                         {AGENT_TASK_RULE_VALUE_MAX_LENGTH} byte limit",
                        value.len()
                    ),
                });
            }
        }
        Ok(())
    }

    /// Stable kebab-case label of the check, used as the rejection reason code.
    #[must_use]
    pub const fn as_label(&self) -> &'static str {
        match self {
            Self::Required { .. } => "required-field-missing",
            Self::Forbidden { .. } => "forbidden-field-present",
            Self::NonEmptyString { .. } => "empty-string-field",
            Self::IntegerRange { .. } => "integer-out-of-range",
            Self::OneOf { .. } => "value-not-permitted",
            Self::EvidenceRequired => "evidence-missing",
        }
    }
}

/// One versioned, deterministic result rule.
///
/// The rule's identity and version are persisted on every rejection, so a
/// rejection can always be traced to the exact rule revision that produced it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTaskResultRule {
    /// Stable rule identity.
    pub rule_id: AgentTaskRuleId,
    /// Monotonic rule revision.
    pub version: AgentRevisionNumber,
    /// The deterministic check.
    pub check: AgentTaskResultCheck,
}

impl AgentTaskResultRule {
    /// Creates a result rule at its initial revision.
    #[must_use]
    pub const fn new(rule_id: AgentTaskRuleId, check: AgentTaskResultCheck) -> Self {
        Self {
            rule_id,
            version: AgentRevisionNumber::INITIAL,
            check,
        }
    }

    /// Sets the rule revision.
    #[must_use]
    pub const fn with_version(mut self, version: AgentRevisionNumber) -> Self {
        self.version = version;
        self
    }

    /// Evaluates the rule against a proposed result.
    ///
    /// Artifact-backed content carries no inspectable value, so a rule that
    /// needs one fails closed: the task cannot claim a result satisfied a rule
    /// it was never able to evaluate.
    fn evaluate(&self, content: &AgentTaskContent, evidence: &[ArtifactRef]) -> Option<String> {
        if matches!(self.check, AgentTaskResultCheck::EvidenceRequired) {
            return evidence
                .is_empty()
                .then(|| "the proposal carries no evidence artifact".to_string());
        }

        let Some(value) = content.inline_value() else {
            return Some(
                "the rule needs an inspectable result, and the proposal is held behind an artifact \
                 reference"
                    .to_string(),
            );
        };

        match &self.check {
            AgentTaskResultCheck::EvidenceRequired => None,
            AgentTaskResultCheck::Required { pointer } => match value.pointer(pointer) {
                None | Some(Value::Null) => Some(format!("{pointer} is required")),
                Some(_) => None,
            },
            AgentTaskResultCheck::Forbidden { pointer } => match value.pointer(pointer) {
                None | Some(Value::Null) => None,
                Some(_) => Some(format!("{pointer} is forbidden")),
            },
            AgentTaskResultCheck::NonEmptyString { pointer } => match value.pointer(pointer) {
                Some(Value::String(text)) if !text.trim().is_empty() => None,
                _ => Some(format!("{pointer} must be a non-empty string")),
            },
            AgentTaskResultCheck::IntegerRange {
                pointer,
                minimum,
                maximum,
            } => match value.pointer(pointer).and_then(Value::as_i64) {
                None => Some(format!("{pointer} must be an integer")),
                Some(number) => {
                    if minimum.is_some_and(|minimum| number < minimum) {
                        Some(format!(
                            "{pointer} is {number}, below the minimum {}",
                            minimum.unwrap_or_default()
                        ))
                    } else if maximum.is_some_and(|maximum| number > maximum) {
                        Some(format!(
                            "{pointer} is {number}, above the maximum {}",
                            maximum.unwrap_or_default()
                        ))
                    } else {
                        None
                    }
                }
            },
            AgentTaskResultCheck::OneOf { pointer, values } => {
                match value.pointer(pointer).and_then(Value::as_str) {
                    Some(text) if values.contains(text) => None,
                    _ => Some(format!("{pointer} is not one of the permitted values")),
                }
            }
        }
    }
}

/// What happens to a task when one of its dependencies does not complete
/// ([specification 9.2](../../../docs/plans/rakka-agent/spec.md)).
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentDependencyFailurePolicy {
    /// The default: a failed or cancelled dependency cancels its dependents.
    #[default]
    CancelDependents,
    /// The dependent proceeds, and the dependency's outcome becomes evidence
    /// its run must account for. It must be chosen explicitly.
    ContinueWithEvidence,
}

impl AgentDependencyFailurePolicy {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::CancelDependents => "cancel-dependents",
            Self::ContinueWithEvidence => "continue-with-evidence",
        }
    }
}

impl Display for AgentDependencyFailurePolicy {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// Who may complete a task
/// ([specification 9.1](../../../docs/plans/rakka-agent/spec.md)).
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentTaskOwnership {
    /// An agent run, created by the task's assignment decision.
    #[default]
    Agent,
    /// An authenticated human or service, through the same typed validation
    /// path. The task is deliberately never assigned to an agent
    /// ([specification 8.12](../../../docs/plans/rakka-agent/spec.md)); the
    /// authenticated completion command lands with slice 5.4.
    Human,
}

impl AgentTaskOwnership {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Agent => "agent",
            Self::Human => "human",
        }
    }
}

impl Display for AgentTaskOwnership {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// The bounds a task definition places on its own durable state
/// ([specification 9.6](../../../docs/plans/rakka-agent/spec.md)).
///
/// Each field may only tighten the crate-level maximum it corresponds to; a
/// definition cannot raise a bound that keeps durable state finite.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTaskLimits {
    /// How many result rejections the task tolerates before it fails.
    pub max_result_rejections: u32,
    /// How many assignment generations the task may consume, counting the
    /// first: a run that refuses its assignment costs one.
    pub max_assignments: u32,
    /// How many dependencies the task may declare.
    pub max_dependencies: usize,
}

impl AgentTaskLimits {
    /// The default bounds: three rejections, three assignments, and the
    /// crate-level dependency maximum.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            max_result_rejections: 3,
            max_assignments: 3,
            max_dependencies: AGENT_TASK_MAX_DEPENDENCIES,
        }
    }

    /// Sets how many result rejections the task tolerates.
    #[must_use]
    pub const fn with_max_result_rejections(mut self, maximum: u32) -> Self {
        self.max_result_rejections = maximum;
        self
    }

    /// Sets how many assignment generations the task may consume.
    #[must_use]
    pub const fn with_max_assignments(mut self, maximum: u32) -> Self {
        self.max_assignments = maximum;
        self
    }

    /// Sets how many dependencies the task may declare.
    #[must_use]
    pub const fn with_max_dependencies(mut self, maximum: usize) -> Self {
        self.max_dependencies = maximum;
        self
    }

    fn validate(&self) -> AgentTaskResult<()> {
        if self.max_result_rejections == 0 {
            return Err(AgentTaskError::InvalidDefinition {
                detail: "a task must tolerate at least one result rejection".to_string(),
            });
        }
        if self.max_assignments == 0 {
            return Err(AgentTaskError::InvalidDefinition {
                detail: "a task must permit at least one assignment".to_string(),
            });
        }
        if self.max_dependencies > AGENT_TASK_MAX_DEPENDENCIES {
            return Err(AgentTaskError::InvalidDefinition {
                detail: format!(
                    "a task may not raise the dependency bound above {AGENT_TASK_MAX_DEPENDENCIES}"
                ),
            });
        }
        Ok(())
    }
}

impl Default for AgentTaskLimits {
    fn default() -> Self {
        Self::new()
    }
}

/// One versioned typed task definition
/// ([specification 9.1](../../../docs/plans/rakka-agent/spec.md)).
///
/// The record is deliberately not generic: durable state and the A2A projection
/// carry stable versioned schema references and bounded serialized values, never
/// a Rust type. [`TypedTask`] is where generics live, as compile-time ergonomics
/// over this same record.
///
/// The fields are public so a definition can be assembled directly, so
/// construction alone cannot guarantee the bounded invariants.
/// [`AgentTaskDefinition::validate`] therefore runs inside
/// [`AgentTaskDefinition::new`], on deserialization — an out-of-bounds definition
/// can neither cross the wire nor load from a durable record — and again at the
/// entity's creation path before anything is persisted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AgentTaskDefinition {
    schema_version: StateSchemaVersion,
    /// Stable definition identity.
    pub definition_id: AgentTaskDefinitionId,
    /// Monotonic definition revision. A result proposed under a different
    /// revision fails closed.
    pub version: AgentRevisionNumber,
    /// Bounded, outcome-oriented description.
    pub description: String,
    /// Schema the task input is expressed in.
    pub input_schema: AgentSchemaRef,
    /// Schema a proposed result must be expressed in.
    pub result_schema: AgentSchemaRef,
    /// The deterministic rules every proposed result must pass.
    pub result_rules: Vec<AgentTaskResultRule>,
    /// Bounds on the task's own durable state.
    pub limits: AgentTaskLimits,
    /// Per-task budget ceilings, which a run allocation may only narrow.
    ///
    /// This is the escrow the task holds: every run it assigns is debited from
    /// it, and a run's settlement and return are credited back to it
    /// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
    pub budgets: AgentBudgetCeilings,
    /// What each run is escrowed when the task assigns it, or `None` to escrow
    /// everything the task can still afford.
    ///
    /// Declaring a portion is what makes a top-up meaningful: a task that hands
    /// its whole escrow to one run has nothing left to grant when that run
    /// exhausts, so the run can only stop. Reserving the rest lets the task
    /// answer a top-up request under the same ceilings, which is exactly the
    /// parent-local allocation decision
    /// [specification 9.7](../../../docs/plans/rakka-agent/spec.md) describes.
    pub run_allocation: Option<AgentBudgetAllocation>,
    /// What happens to this task when a dependency does not complete.
    pub dependency_policy: AgentDependencyFailurePolicy,
    /// Who may complete the task.
    pub ownership: AgentTaskOwnership,
    /// The operating behavior the task's runs execute under
    /// ([specification 7.4](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// It is a property of the work, not of the agent: the same agent may serve
    /// an interactive task with a human in the loop and an unattended one that
    /// must be admitted first. The agent's envelope declares which classes it
    /// may be given, and its admission decision declares which it may currently
    /// run.
    pub operation_class: AgentOperationClass,
    /// Skills an assignee agent must declare. An empty set requires none.
    pub required_skills: BTreeSet<AgentCapabilityId>,
    /// Application-owned retention, audit, guardrail, and escalation policies.
    pub policies: AgentPolicyRefs,
}

impl AgentTaskDefinition {
    /// Creates a task definition at its initial revision, rejecting one that
    /// cannot be bounded.
    pub fn new(
        definition_id: AgentTaskDefinitionId,
        description: impl Into<String>,
        input_schema: AgentSchemaRef,
        result_schema: AgentSchemaRef,
    ) -> AgentTaskResult<Self> {
        let definition = Self {
            schema_version: CURRENT_AGENT_TASK_DEFINITION_SCHEMA_VERSION,
            definition_id,
            version: AgentRevisionNumber::INITIAL,
            description: description.into(),
            input_schema,
            result_schema,
            result_rules: Vec::new(),
            limits: AgentTaskLimits::new(),
            budgets: AgentBudgetCeilings::unbounded(),
            run_allocation: None,
            dependency_policy: AgentDependencyFailurePolicy::default(),
            ownership: AgentTaskOwnership::default(),
            // Attended is the safe default: an unattended class is the one that
            // must be admitted, so defaulting to it would make the fail-closed
            // check something a caller opts *out* of.
            operation_class: AgentOperationClass::Interactive,
            required_skills: BTreeSet::new(),
            policies: AgentPolicyRefs::default(),
        };
        definition.validate()?;
        Ok(definition)
    }

    /// Adds a deterministic result rule.
    #[must_use]
    pub fn with_result_rule(mut self, rule: AgentTaskResultRule) -> Self {
        self.result_rules.push(rule);
        self
    }

    /// Sets the task's durable-state bounds.
    #[must_use]
    pub const fn with_limits(mut self, limits: AgentTaskLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Sets the per-task budget ceilings.
    #[must_use]
    pub const fn with_budgets(mut self, budgets: AgentBudgetCeilings) -> Self {
        self.budgets = budgets;
        self
    }

    /// Sets what each run is escrowed when the task assigns it.
    #[must_use]
    pub const fn with_run_allocation(mut self, allocation: AgentBudgetAllocation) -> Self {
        self.run_allocation = Some(allocation);
        self
    }

    /// What one run should be escrowed, before the task's own headroom narrows
    /// it.
    ///
    /// An undeclared per-run allocation asks for everything: the task then
    /// escrows whatever it can still afford, which is the right default for the
    /// one-run-at-a-time task of this phase and wastes nothing.
    #[must_use]
    pub fn run_allocation_request(&self) -> AgentBudgetAllocation {
        self.run_allocation
            .unwrap_or_else(AgentBudgetAllocation::unbounded)
    }

    /// Sets the failed-dependency policy.
    #[must_use]
    pub const fn with_dependency_policy(mut self, policy: AgentDependencyFailurePolicy) -> Self {
        self.dependency_policy = policy;
        self
    }

    /// Sets who may complete the task.
    #[must_use]
    pub const fn with_ownership(mut self, ownership: AgentTaskOwnership) -> Self {
        self.ownership = ownership;
        self
    }

    /// Sets the operating behavior the task's runs execute under.
    #[must_use]
    pub const fn with_operation_class(mut self, class: AgentOperationClass) -> Self {
        self.operation_class = class;
        self
    }

    /// Sets the definition revision.
    #[must_use]
    pub const fn with_version(mut self, version: AgentRevisionNumber) -> Self {
        self.version = version;
        self
    }

    /// Requires an assignee agent to declare one skill.
    #[must_use]
    pub fn with_required_skill(mut self, skill: AgentCapabilityId) -> Self {
        self.required_skills.insert(skill);
        self
    }

    /// Rejects a definition that cannot be bounded.
    pub fn validate(&self) -> AgentTaskResult<()> {
        if self.description.is_empty() {
            return Err(AgentTaskError::InvalidDefinition {
                detail: "a task definition must carry an outcome-oriented description".to_string(),
            });
        }
        if self.description.len() > AGENT_TASK_DESCRIPTION_MAX_LENGTH {
            return Err(AgentTaskError::InvalidDefinition {
                detail: format!(
                    "the description is {} bytes, which exceeds the {AGENT_TASK_DESCRIPTION_MAX_LENGTH} byte limit",
                    self.description.len()
                ),
            });
        }
        if self.result_rules.len() > AGENT_TASK_MAX_RESULT_RULES {
            return Err(AgentTaskError::InvalidDefinition {
                detail: format!(
                    "a task definition may declare at most {AGENT_TASK_MAX_RESULT_RULES} result rules"
                ),
            });
        }
        for rule in &self.result_rules {
            rule.check.validate()?;
        }
        self.limits.validate()
    }

    /// Whether the task is completed by an agent run rather than a human.
    #[must_use]
    pub const fn is_agent_owned(&self) -> bool {
        matches!(self.ownership, AgentTaskOwnership::Agent)
    }

    /// Validates a proposed result against the definition's schema reference,
    /// revision, and every deterministic rule, returning the first rule that
    /// refused it.
    ///
    /// It is a pure function: the same proposal always produces the same
    /// decision, on any node, after any restart.
    fn validate_proposal(
        &self,
        proposal: &AgentTaskResultProposal,
    ) -> Result<(), AgentTaskRejectionCause> {
        if proposal.definition_id != self.definition_id
            || proposal.definition_version != self.version
        {
            return Err(AgentTaskRejectionCause::definition_mismatch(format!(
                "the result was proposed under task definition {}@{} but the task runs {}@{}",
                proposal.definition_id,
                proposal.definition_version,
                self.definition_id,
                self.version
            )));
        }
        if proposal.result_schema != self.result_schema {
            return Err(AgentTaskRejectionCause::schema_mismatch(format!(
                "the result was proposed under schema {} but the task requires {}",
                proposal.result_schema, self.result_schema
            )));
        }
        if let Err(error) = proposal.content.validate() {
            return Err(AgentTaskRejectionCause::malformed(error.to_string()));
        }
        if proposal.evidence.len() > AGENT_TASK_MAX_EVIDENCE_ARTIFACTS {
            return Err(AgentTaskRejectionCause::malformed(format!(
                "a proposal may carry at most {AGENT_TASK_MAX_EVIDENCE_ARTIFACTS} evidence artifacts"
            )));
        }

        for rule in &self.result_rules {
            if let Some(detail) = rule.evaluate(&proposal.content, &proposal.evidence) {
                return Err(AgentTaskRejectionCause::rule(rule, detail));
            }
        }
        Ok(())
    }
}

impl<'de> Deserialize<'de> for AgentTaskDefinition {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Wire {
            schema_version: StateSchemaVersion,
            definition_id: AgentTaskDefinitionId,
            version: AgentRevisionNumber,
            description: String,
            input_schema: AgentSchemaRef,
            result_schema: AgentSchemaRef,
            result_rules: Vec<AgentTaskResultRule>,
            limits: AgentTaskLimits,
            budgets: AgentBudgetCeilings,
            run_allocation: Option<AgentBudgetAllocation>,
            dependency_policy: AgentDependencyFailurePolicy,
            ownership: AgentTaskOwnership,
            operation_class: AgentOperationClass,
            required_skills: BTreeSet<AgentCapabilityId>,
            policies: AgentPolicyRefs,
        }

        let wire = Wire::deserialize(deserializer)?;
        let definition = Self {
            schema_version: wire.schema_version,
            definition_id: wire.definition_id,
            version: wire.version,
            description: wire.description,
            input_schema: wire.input_schema,
            result_schema: wire.result_schema,
            result_rules: wire.result_rules,
            limits: wire.limits,
            budgets: wire.budgets,
            run_allocation: wire.run_allocation,
            dependency_policy: wire.dependency_policy,
            ownership: wire.ownership,
            operation_class: wire.operation_class,
            required_skills: wire.required_skills,
            policies: wire.policies,
        };
        definition.validate().map_err(serde::de::Error::custom)?;
        Ok(definition)
    }
}

impl VersionedAgentRecord for AgentTaskDefinition {
    const RECORD_KIND: AgentRecordKind = AgentRecordKind::TaskDefinition;

    fn schema_version(&self) -> StateSchemaVersion {
        self.schema_version
    }
}

/// A task definition bound to the Rust types of its input and result.
///
/// This is the whole of the "generics as compile-time ergonomics" clause of
/// [specification 9.1](../../../docs/plans/rakka-agent/spec.md): the handle
/// encodes and decodes application types against the definition's schema
/// references, and everything it produces is the same bounded, versioned,
/// non-generic record the durable state already holds. A result decoded under a
/// mismatched definition or schema revision fails closed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypedTask<I, R> {
    definition: AgentTaskDefinition,
    marker: std::marker::PhantomData<fn(I) -> R>,
}

impl<I, R> TypedTask<I, R>
where
    I: Serialize,
    R: Serialize + serde::de::DeserializeOwned,
{
    /// Binds Rust input and result types to a task definition.
    #[must_use]
    pub const fn new(definition: AgentTaskDefinition) -> Self {
        Self {
            definition,
            marker: std::marker::PhantomData,
        }
    }

    /// The bounded, versioned definition this handle wraps.
    #[must_use]
    pub const fn definition(&self) -> &AgentTaskDefinition {
        &self.definition
    }

    /// Encodes a typed input as bounded task content.
    pub fn input(&self, input: &I) -> AgentTaskResult<AgentTaskContent> {
        let value = serde_json::to_value(input).map_err(|error| AgentTaskError::Encoding {
            message: error.to_string(),
        })?;
        AgentTaskContent::inline(value)
    }

    /// Encodes a typed result as bounded task content.
    pub fn result(&self, result: &R) -> AgentTaskResult<AgentTaskContent> {
        let value = serde_json::to_value(result).map_err(|error| AgentTaskError::Encoding {
            message: error.to_string(),
        })?;
        AgentTaskContent::inline(value)
    }

    /// Decodes the task's accepted result, failing closed when it was accepted
    /// under a different definition or schema revision than this handle's.
    pub fn decode_accepted(&self, accepted: &AgentAcceptedResult) -> AgentTaskResult<R> {
        if accepted.definition_id != self.definition.definition_id
            || accepted.definition_version != self.definition.version
        {
            return Err(AgentTaskError::DefinitionMismatch {
                expected: format!(
                    "{}@{}",
                    self.definition.definition_id, self.definition.version
                ),
                actual: format!("{}@{}", accepted.definition_id, accepted.definition_version),
            });
        }
        if accepted.result_schema != self.definition.result_schema {
            return Err(AgentTaskError::SchemaMismatch {
                expected: self.definition.result_schema.to_string(),
                actual: accepted.result_schema.to_string(),
            });
        }
        let Some(value) = accepted.content.inline_value() else {
            return Err(AgentTaskError::ResultBehindArtifact);
        };
        serde_json::from_value(value.clone()).map_err(|error| AgentTaskError::Decoding {
            message: error.to_string(),
        })
    }
}

/// Public lifecycle of one typed task
/// ([specification 9.2](../../../docs/plans/rakka-agent/spec.md)).
///
/// The status is independent of whether any actor or run is resident: a task may
/// be `Blocked`, `Assigned`, or `WaitingForInput` with no live execution
/// resource anywhere in the cluster
/// ([specification 6.11](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentTaskStatus {
    /// Created, eligible, and not yet assigned.
    Created,
    /// Waiting on a dependency.
    Blocked,
    /// Assigned to an agent; its run has not yet durably accepted.
    Assigned,
    /// The assigned run has durably accepted and owns the work.
    InProgress,
    /// Deliberately unassigned to an agent, waiting for an authenticated human
    /// or service ([specification 8.12](../../../docs/plans/rakka-agent/spec.md)).
    WaitingForInput,
    /// Terminal: a proposed result passed every deterministic rule.
    Completed,
    /// Terminal: the task cannot produce an accepted result.
    Failed,
    /// Terminal: the task was cancelled.
    Cancelled,
}

impl AgentTaskStatus {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Blocked => "blocked",
            Self::Assigned => "assigned",
            Self::InProgress => "in-progress",
            Self::WaitingForInput => "waiting-for-input",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    /// Whether the status is terminal.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }

    /// Whether the task is waiting to be assigned to an agent.
    #[must_use]
    pub const fn is_assignable(self) -> bool {
        matches!(self, Self::Created)
    }
}

impl Display for AgentTaskStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// Why a task reached a terminal status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentTaskTerminalReason {
    /// A proposed result passed every deterministic rule.
    ResultAccepted,
    /// Every tolerated result rejection was consumed without an accepted result.
    ResultRejectionsExhausted {
        /// How many rejections the task recorded.
        rejections: u32,
    },
    /// Every tolerated assignment generation was consumed.
    AssignmentsExhausted {
        /// How many assignment generations the task consumed.
        assignments: u32,
    },
    /// A dependency did not complete, and the task's policy cancels dependents.
    DependencyNotSatisfied {
        /// The dependency that did not complete.
        dependency: AgentTaskId,
        /// Its outcome.
        outcome: AgentTaskDependencyOutcome,
    },
    /// The task was cancelled by an authorized command.
    CancellationRequested {
        /// Bounded, stable reason.
        reason: String,
    },
}

impl AgentTaskTerminalReason {
    /// Stable machine-readable code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::ResultAccepted => "result-accepted",
            Self::ResultRejectionsExhausted { .. } => "result-rejections-exhausted",
            Self::AssignmentsExhausted { .. } => "assignments-exhausted",
            Self::DependencyNotSatisfied { .. } => "dependency-not-satisfied",
            Self::CancellationRequested { .. } => "cancellation-requested",
        }
    }

    /// The task status this reason terminates the task in.
    ///
    /// A dependency that did not complete *cancels* its dependents rather than
    /// failing them: the dependent never got to try
    /// ([specification 9.2](../../../docs/plans/rakka-agent/spec.md)).
    #[must_use]
    pub const fn status(&self) -> AgentTaskStatus {
        match self {
            Self::ResultAccepted => AgentTaskStatus::Completed,
            Self::ResultRejectionsExhausted { .. } | Self::AssignmentsExhausted { .. } => {
                AgentTaskStatus::Failed
            }
            Self::DependencyNotSatisfied { .. } | Self::CancellationRequested { .. } => {
                AgentTaskStatus::Cancelled
            }
        }
    }
}

/// How one dependency resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentTaskDependencyOutcome {
    /// The dependency produced an accepted result.
    Completed,
    /// The dependency failed.
    Failed,
    /// The dependency was cancelled.
    Cancelled,
}

impl AgentTaskDependencyOutcome {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    /// Whether the outcome satisfies a dependency rule.
    #[must_use]
    pub const fn is_satisfied(self) -> bool {
        matches!(self, Self::Completed)
    }
}

impl Display for AgentTaskDependencyOutcome {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// A declared dependency of one task on another.
///
/// The declared ancestors are what keep the graph acyclic without a global read:
/// a task refuses a dependency whose ancestry already contains the task itself.
/// The chain is bounded by [`AGENT_TASK_MAX_DEPENDENCY_DEPTH`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTaskDependencyDeclaration {
    /// The task that must resolve first.
    pub dependency: AgentTaskId,
    /// The dependency's own declared ancestry, nearest first.
    pub ancestors: Vec<AgentTaskId>,
    /// What happens to the dependent when this dependency does not complete.
    pub policy: AgentDependencyFailurePolicy,
}

impl AgentTaskDependencyDeclaration {
    /// Declares a dependency under the default failed-dependency policy.
    #[must_use]
    pub fn new(dependency: AgentTaskId) -> Self {
        Self {
            dependency,
            ancestors: Vec::new(),
            policy: AgentDependencyFailurePolicy::default(),
        }
    }

    /// Records the dependency's declared ancestry.
    #[must_use]
    pub fn with_ancestors(mut self, ancestors: Vec<AgentTaskId>) -> Self {
        self.ancestors = ancestors;
        self
    }

    /// Sets the failed-dependency policy for this edge.
    #[must_use]
    pub const fn with_policy(mut self, policy: AgentDependencyFailurePolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Rejects a declaration that would create a cycle or exceed the bounds.
    fn validate(&self, task: &AgentTaskId) -> AgentTaskResult<()> {
        if &self.dependency == task {
            return Err(AgentTaskError::DependencyCycle {
                dependency: self.dependency.clone(),
            });
        }
        if self.ancestors.contains(task) {
            return Err(AgentTaskError::DependencyCycle {
                dependency: self.dependency.clone(),
            });
        }
        if self.ancestors.len() > AGENT_TASK_MAX_DEPENDENCY_DEPTH {
            return Err(AgentTaskError::DependencyDepthExceeded {
                depth: self.ancestors.len(),
                maximum: AGENT_TASK_MAX_DEPENDENCY_DEPTH,
            });
        }
        Ok(())
    }
}

/// The bounded durable summary of one dependency edge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTaskDependency {
    /// The task that must resolve first.
    pub dependency: AgentTaskId,
    /// What happens to this task when the dependency does not complete.
    pub policy: AgentDependencyFailurePolicy,
    /// How the dependency resolved, once it has.
    pub outcome: Option<AgentTaskDependencyOutcome>,
    /// The operation that declared the edge.
    pub declared_by: AgentOperationId,
    /// When the edge was declared.
    pub declared_at: AgentTimestampMillis,
}

impl AgentTaskDependency {
    /// Whether the edge permits the dependent to become eligible.
    ///
    /// A `ContinueWithEvidence` edge is satisfied by any resolution: the
    /// dependency's outcome becomes evidence the dependent's run must account
    /// for, rather than a block.
    #[must_use]
    pub fn is_satisfied(&self) -> bool {
        match self.outcome {
            None => false,
            Some(outcome) => {
                outcome.is_satisfied()
                    || matches!(
                        self.policy,
                        AgentDependencyFailurePolicy::ContinueWithEvidence
                    )
            }
        }
    }

    /// Whether the edge cancels its dependent.
    #[must_use]
    pub fn cancels_dependent(&self) -> bool {
        matches!(self.policy, AgentDependencyFailurePolicy::CancelDependents)
            && self.outcome.is_some_and(|outcome| !outcome.is_satisfied())
    }
}

/// Whether the run an assignment created has durably accepted it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentAssignmentStatus {
    /// The run-creation exchange is owed or in flight; the run has not replied.
    Offered,
    /// The run durably accepted its assignment.
    Accepted,
}

impl AgentAssignmentStatus {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Offered => "offered",
            Self::Accepted => "accepted",
        }
    }
}

impl Display for AgentAssignmentStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// The task's *current* assignment.
///
/// Only the current one is materialized. A superseded assignment leaves the
/// record and becomes a history entry
/// ([specification 9.6](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTaskAssignment {
    /// The generation this assignment owns. It fences every earlier run.
    pub generation: AgentAssignmentGeneration,
    /// The assigned agent.
    pub agent: AgentId,
    /// The run created to serve this generation.
    pub run: AgentRunId,
    /// Whether the run has durably accepted.
    pub status: AgentAssignmentStatus,
    /// The agent definition revision the decision was made against.
    pub agent_definition_revision: AgentRevisionNumber,
    /// The agent settings revision the decision was made against.
    pub agent_settings_revision: AgentRevisionNumber,
    /// The run-creation operation this assignment owes or owed.
    pub operation_id: AgentOperationId,
    /// When the decision was recorded.
    pub assigned_at: AgentTimestampMillis,
}

/// Why an assignment decision refused an agent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentAssignmentRefusalReason {
    /// The agent has no durable state: it was never instantiated.
    AgentNotInstantiated,
    /// The agent is suspended or terminated, so it may not dispatch.
    AgentNotActive,
    /// The agent's definition envelope does not declare this task definition.
    TaskDefinitionNotPermitted,
    /// The agent's definition envelope does not declare the unattended
    /// operation class the task runs under
    /// ([specification 7.4](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// It is a distinct reason from [`Self::TaskDefinitionNotPermitted`]
    /// because the remedies differ: the task definition may be fully declared
    /// while the *class* of autonomy it asks for is not.
    OperationClassNotDeclared,
    /// The agent's definition envelope does not declare a skill the task
    /// requires.
    SkillNotDeclared,
    /// The agent has no autonomy admission decision that admits this work, or
    /// the one it has no longer admits it
    /// ([specification 7.4](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// This is the fail-closed default for an unattended class: an agent is
    /// unadmitted until something says otherwise, and a widening update makes it
    /// unadmitted again.
    NotAdmitted,
    /// The task cannot escrow a run from what its own ledger still holds
    /// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
    BudgetUnavailable,
    /// The run refused the assignment.
    RunRefusedAssignment,
}

impl AgentAssignmentRefusalReason {
    /// Stable machine-readable code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::AgentNotInstantiated => "agent-not-instantiated",
            Self::AgentNotActive => "agent-not-active",
            Self::TaskDefinitionNotPermitted => "task-definition-not-permitted",
            Self::OperationClassNotDeclared => "operation-class-not-declared",
            Self::SkillNotDeclared => "skill-not-declared",
            Self::NotAdmitted => "agent-not-admitted",
            Self::BudgetUnavailable => "task-budget-unavailable",
            Self::RunRefusedAssignment => "run-refused-assignment",
        }
    }
}

impl Display for AgentAssignmentRefusalReason {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.code())
    }
}

/// The bounded record of the most recent refused assignment.
///
/// A refusal is not terminal: the task stays logically available and assignable,
/// so resuming a suspended agent or widening its envelope lets the next
/// assignment attempt succeed without re-creating the task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentAssignmentRefusal {
    /// The agent that was refused.
    pub agent: AgentId,
    /// Why it was refused.
    pub reason: AgentAssignmentRefusalReason,
    /// Bounded detail.
    pub detail: String,
    /// When the refusal was recorded.
    pub refused_at: AgentTimestampMillis,
}

/// The stable cause of one result rejection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTaskRejectionCause {
    /// Stable machine-readable reason code.
    pub reason: String,
    /// The rule that refused the result, when a rule did.
    pub rule_id: Option<AgentTaskRuleId>,
    /// The revision of that rule.
    pub rule_version: Option<AgentRevisionNumber>,
    /// Bounded detail.
    pub detail: String,
}

impl AgentTaskRejectionCause {
    fn rule(rule: &AgentTaskResultRule, detail: String) -> Self {
        Self {
            reason: rule.check.as_label().to_string(),
            rule_id: Some(rule.rule_id.clone()),
            rule_version: Some(rule.version),
            detail: bounded_detail(detail),
        }
    }

    fn definition_mismatch(detail: String) -> Self {
        Self::without_rule("definition-version-mismatch", detail)
    }

    fn schema_mismatch(detail: String) -> Self {
        Self::without_rule("result-schema-mismatch", detail)
    }

    fn malformed(detail: String) -> Self {
        Self::without_rule("malformed-result", detail)
    }

    fn without_rule(reason: &str, detail: String) -> Self {
        Self {
            reason: reason.to_string(),
            rule_id: None,
            rule_version: None,
            detail: bounded_detail(detail),
        }
    }
}

/// One durable rejection decision
/// ([specification 9.2](../../../docs/plans/rakka-agent/spec.md)).
///
/// Only the most recent rejection stays materialized. Every rejection is also a
/// history entry, so the full sequence is readable through the bounded cursor
/// without the task's state growing with it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTaskRejection {
    /// The proposal that was refused.
    pub proposal_id: AgentOperationId,
    /// The fingerprint of the refused content.
    pub digest: AgentContentDigest,
    /// Why it was refused.
    pub cause: AgentTaskRejectionCause,
    /// How many rejections the task had recorded once this one was persisted.
    pub rejection_count: u32,
    /// What caused the proposal.
    pub causation_id: AgentCausationId,
    /// When the decision was recorded.
    pub rejected_at: AgentTimestampMillis,
}

/// The task's accepted typed result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentAcceptedResult {
    /// The proposal that produced it.
    pub proposal_id: AgentOperationId,
    /// The run that proposed it.
    pub run: AgentRunId,
    /// The task definition it was validated under.
    pub definition_id: AgentTaskDefinitionId,
    /// The revision of that definition.
    pub definition_version: AgentRevisionNumber,
    /// The schema it is expressed in.
    pub result_schema: AgentSchemaRef,
    /// The bounded content, inline or behind an artifact reference.
    pub content: AgentTaskContent,
    /// Its fingerprint.
    pub digest: AgentContentDigest,
    /// Evidence artifacts the proposal carried.
    pub evidence: Vec<ArtifactRef>,
    /// When the task accepted it.
    pub accepted_at: AgentTimestampMillis,
}

fn bounded_detail(detail: impl Into<String>) -> String {
    let mut detail = detail.into();
    if detail.len() > AGENT_TASK_DETAIL_MAX_LENGTH {
        detail.truncate(
            (0..=AGENT_TASK_DETAIL_MAX_LENGTH)
                .rev()
                .find(|index| detail.is_char_boundary(*index))
                .unwrap_or(0),
        );
    }
    detail
}

/// What one history entry records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentTaskHistoryKind {
    /// The task was created.
    Created,
    /// A dependency edge was declared.
    DependencyDeclared,
    /// A dependency resolved.
    DependencyResolved,
    /// An assignment generation was decided.
    AssignmentDecided,
    /// An assignment was refused, and no generation was consumed.
    AssignmentRefused,
    /// The assigned run durably accepted.
    AssignmentAccepted,
    /// The assigned run refused its assignment, retiring the generation.
    AssignmentReleased,
    /// A run proposed a typed result.
    ResultProposed,
    /// A proposal passed every deterministic rule.
    ResultAccepted,
    /// A proposal was refused by a deterministic rule.
    ResultRejected,
    /// The task reached a terminal status.
    Terminated,
}

impl AgentTaskHistoryKind {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::DependencyDeclared => "dependency-declared",
            Self::DependencyResolved => "dependency-resolved",
            Self::AssignmentDecided => "assignment-decided",
            Self::AssignmentRefused => "assignment-refused",
            Self::AssignmentAccepted => "assignment-accepted",
            Self::AssignmentReleased => "assignment-released",
            Self::ResultProposed => "result-proposed",
            Self::ResultAccepted => "result-accepted",
            Self::ResultRejected => "result-rejected",
            Self::Terminated => "terminated",
        }
    }
}

impl Display for AgentTaskHistoryKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// One append-only entry in a task's durable domain history
/// ([specification 9.6](../../../docs/plans/rakka-agent/spec.md)).
///
/// It is a bounded record of *what happened*: identities, digests, and artifact
/// references. It never carries messages, observations, tool payloads, prompts,
/// memory records, or resolved credentials — those live behind the artifact
/// references it points at, under the application's own retention policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTaskHistoryEntry {
    schema_version: StateSchemaVersion,
    /// Monotonic sequence within the task, and the append's idempotency key.
    pub sequence: AgentTaskHistorySequence,
    /// What the entry records.
    pub kind: AgentTaskHistoryKind,
    /// The operation that produced it.
    pub operation_id: AgentOperationId,
    /// The task's status once the transition committed.
    pub status: AgentTaskStatus,
    /// The assignment generation in force, when one was.
    pub generation: Option<AgentAssignmentGeneration>,
    /// The agent involved, when one was.
    pub agent: Option<AgentId>,
    /// The run involved, when one was.
    pub run: Option<AgentRunId>,
    /// The fingerprint of the content involved, when there was any.
    pub digest: Option<AgentContentDigest>,
    /// Bounded detail: the rejection reason, the refusal code, the terminal
    /// reason.
    pub detail: String,
    /// When the transition committed.
    pub at: AgentTimestampMillis,
}

impl AgentTaskHistoryEntry {
    fn new(
        sequence: AgentTaskHistorySequence,
        kind: AgentTaskHistoryKind,
        operation_id: AgentOperationId,
        status: AgentTaskStatus,
        at: AgentTimestampMillis,
    ) -> Self {
        Self {
            schema_version: CURRENT_AGENT_TASK_HISTORY_SCHEMA_VERSION,
            sequence,
            kind,
            operation_id,
            status,
            generation: None,
            agent: None,
            run: None,
            digest: None,
            detail: String::new(),
            at,
        }
    }

    fn with_assignment(mut self, assignment: &AgentTaskAssignment) -> Self {
        self.generation = Some(assignment.generation);
        self.agent = Some(assignment.agent.clone());
        self.run = Some(assignment.run.clone());
        self
    }

    fn with_digest(mut self, digest: AgentContentDigest) -> Self {
        self.digest = Some(digest);
        self
    }

    fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = bounded_detail(detail);
        self
    }
}

impl VersionedAgentRecord for AgentTaskHistoryEntry {
    const RECORD_KIND: AgentRecordKind = AgentRecordKind::TaskHistoryEntry;

    fn schema_version(&self) -> StateSchemaVersion {
        self.schema_version
    }
}

/// A bounded, authorized read over a task's history.
///
/// The cursor is the only way to reach history a task's materialized state no
/// longer holds. Its page size is clamped to
/// [`AGENT_TASK_HISTORY_MAX_PAGE_SIZE`], so no reader can ask a store for an
/// unbounded page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentTaskHistoryCursor {
    after: Option<AgentTaskHistorySequence>,
    limit: usize,
}

impl AgentTaskHistoryCursor {
    /// A cursor over the whole history, from the beginning.
    #[must_use]
    pub const fn start() -> Self {
        Self {
            after: None,
            limit: AGENT_TASK_HISTORY_DEFAULT_PAGE_SIZE,
        }
    }

    /// A cursor resuming after one sequence.
    #[must_use]
    pub const fn after(sequence: AgentTaskHistorySequence) -> Self {
        Self {
            after: Some(sequence),
            limit: AGENT_TASK_HISTORY_DEFAULT_PAGE_SIZE,
        }
    }

    /// Sets the page size, clamped to [`AGENT_TASK_HISTORY_MAX_PAGE_SIZE`].
    #[must_use]
    pub const fn with_limit(mut self, limit: usize) -> Self {
        self.limit = if limit == 0 {
            1
        } else if limit > AGENT_TASK_HISTORY_MAX_PAGE_SIZE {
            AGENT_TASK_HISTORY_MAX_PAGE_SIZE
        } else {
            limit
        };
        self
    }

    /// The sequence this page resumes after.
    #[must_use]
    pub const fn position(&self) -> Option<AgentTaskHistorySequence> {
        self.after
    }

    /// The clamped page size.
    #[must_use]
    pub const fn limit(&self) -> usize {
        self.limit
    }
}

impl Default for AgentTaskHistoryCursor {
    fn default() -> Self {
        Self::start()
    }
}

/// One bounded page of task history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentTaskHistoryPage {
    /// The entries, oldest first.
    pub entries: Vec<AgentTaskHistoryEntry>,
    /// The cursor that resumes after this page, when more history exists.
    pub next: Option<AgentTaskHistoryCursor>,
}

impl AgentTaskHistoryPage {
    /// Whether more history follows this page.
    #[must_use]
    pub const fn has_more(&self) -> bool {
        self.next.is_some()
    }
}

/// Boxed future returned by an [`AgentTaskHistoryStore`].
pub type AgentTaskHistoryFuture<'a, T> =
    Pin<Box<dyn Future<Output = AgentTaskResult<T>> + Send + 'a>>;

/// The append-only durable history of every task, separate from the bounded
/// materialized state that drives transitions
/// ([specification 9.6](../../../docs/plans/rakka-agent/spec.md)).
///
/// An append is idempotent on `(scope, sequence)`: the entity assigns the
/// sequence inside the transition that produced the entry, so re-driving an
/// interrupted flush writes the same entry to the same slot rather than
/// duplicating it. A store that finds a different entry already at a sequence
/// must fail closed rather than overwrite it — that would mean two different
/// transitions claimed one slot.
///
/// Reads are tenant-scoped by the [`AgentTaskScope`] they address, so a caller
/// cannot learn whether another tenant's task exists.
pub trait AgentTaskHistoryStore: Clone + Send + Sync + 'static {
    /// Stable backend name, used in telemetry.
    fn backend_name(&self) -> &'static str;

    /// Appends one entry, idempotently.
    fn append<'a>(
        &'a self,
        scope: &'a AgentTaskScope,
        entry: &'a AgentTaskHistoryEntry,
    ) -> AgentTaskHistoryFuture<'a, ()>;

    /// Reads one bounded page.
    fn read<'a>(
        &'a self,
        scope: &'a AgentTaskScope,
        cursor: AgentTaskHistoryCursor,
    ) -> AgentTaskHistoryFuture<'a, AgentTaskHistoryPage>;
}

/// An in-memory task history, for tests and single-process deployments.
#[derive(Debug, Clone, Default)]
pub struct InMemoryAgentTaskHistoryStore {
    entries: Arc<Mutex<BTreeMap<String, BTreeMap<u64, AgentTaskHistoryEntry>>>>,
}

impl InMemoryAgentTaskHistoryStore {
    /// Creates an empty history.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// How many entries one task has.
    #[must_use]
    pub fn len(&self, scope: &AgentTaskScope) -> usize {
        self.entries
            .lock()
            .expect("the task history should not be poisoned")
            .get(&scope.key())
            .map_or(0, BTreeMap::len)
    }

    /// Whether one task has no history.
    #[must_use]
    pub fn is_empty(&self, scope: &AgentTaskScope) -> bool {
        self.len(scope) == 0
    }
}

impl AgentTaskHistoryStore for InMemoryAgentTaskHistoryStore {
    fn backend_name(&self) -> &'static str {
        "in-memory"
    }

    fn append<'a>(
        &'a self,
        scope: &'a AgentTaskScope,
        entry: &'a AgentTaskHistoryEntry,
    ) -> AgentTaskHistoryFuture<'a, ()> {
        Box::pin(async move {
            let mut entries = self
                .entries
                .lock()
                .expect("the task history should not be poisoned");
            let task = entries.entry(scope.key()).or_default();
            match task.get(&entry.sequence.get()) {
                // A re-driven flush writes the same entry to the same slot.
                Some(existing) if existing == entry => Ok(()),
                Some(_) => Err(AgentTaskError::HistoryConflict {
                    sequence: entry.sequence,
                }),
                None => {
                    task.insert(entry.sequence.get(), entry.clone());
                    Ok(())
                }
            }
        })
    }

    fn read<'a>(
        &'a self,
        scope: &'a AgentTaskScope,
        cursor: AgentTaskHistoryCursor,
    ) -> AgentTaskHistoryFuture<'a, AgentTaskHistoryPage> {
        Box::pin(async move {
            let entries = self
                .entries
                .lock()
                .expect("the task history should not be poisoned");
            let Some(task) = entries.get(&scope.key()) else {
                return Ok(AgentTaskHistoryPage {
                    entries: Vec::new(),
                    next: None,
                });
            };

            let start = cursor.position().map_or(0, |after| after.get() + 1);
            let mut page: Vec<AgentTaskHistoryEntry> = task
                .range(start..)
                .map(|(_, entry)| entry.clone())
                .take(cursor.limit() + 1)
                .collect();

            let next = (page.len() > cursor.limit())
                .then(|| {
                    page.pop();
                    page.last().map(|entry| {
                        AgentTaskHistoryCursor::after(entry.sequence).with_limit(cursor.limit())
                    })
                })
                .flatten();

            Ok(AgentTaskHistoryPage {
                entries: page,
                next,
            })
        })
    }
}

/// The command an [`AgentExchangeKind::Creation`] exchange carries.
///
/// This is the delegating-run path of
/// [specification 9.8](../../../docs/plans/rakka-agent/spec.md): a parent run
/// creates a child task through the durable substrate. An ingress creates a task
/// through the equivalent [`AgentTaskEntityCommand::Create`], and both reach the
/// same bounded transition, so the two paths cannot diverge.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentTaskCreation {
    /// The typed contract of the created task.
    pub definition: AgentTaskDefinition,
    /// Its bounded input.
    pub input: AgentTaskContent,
    /// The agent the task should be assigned to, when it is agent-owned.
    pub assignee: Option<AgentId>,
    /// The collaborative goal it contributes to.
    pub goal: Option<AgentGoalId>,
    /// Whether the task coordinates a finite or continuous goal
    /// ([specification 8.1](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// Continuous mode makes this the root control task the wake controller of
    /// slice 3.2 drives, and it requires a goal binding. Records persisted
    /// before this field load as finite.
    #[serde(default)]
    pub goal_mode: AgentGoalMode,
    /// The task that created it.
    pub parent: Option<AgentTaskId>,
    /// Dependencies declared with the creation.
    pub dependencies: Vec<AgentTaskDependencyDeclaration>,
    /// Trace context of the ingress that created the task — what the A2A
    /// surface extracted before durable acceptance
    /// ([specification 17.5](../../../docs/plans/rakka-agent/spec.md)).
    /// Context flows with commands: the creating transition records it, and
    /// the assignment the task later owes carries it onward. Observability
    /// only, never correctness; a creation without one starts a root.
    #[serde(default)]
    pub telemetry: AgentTelemetryContext,
}

/// The command an [`AgentExchangeKind::Assignment`] exchange carries to the run
/// entity: create the run that serves one assignment generation.
///
/// The run entity of slice 1.5 is its receiver. It is deduplicated by the task
/// and the generation, so replaying it resolves to the same run rather than a
/// second one.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentRunAssignment {
    /// The task the run will serve for its entire lifetime.
    pub task: AgentTaskScope,
    /// The run to create.
    pub run: AgentRunScope,
    /// The generation this run owns.
    pub generation: AgentAssignmentGeneration,
    /// The typed contract the run must satisfy.
    pub definition: AgentTaskDefinition,
    /// The task's bounded input.
    pub input: AgentTaskContent,
    /// The collaborative goal the run contributes to.
    pub goal: Option<AgentGoalId>,
    /// The escrow the task debited from its own ledger for this run, carried on
    /// the creation command exactly as
    /// [specification 9.7](../../../docs/plans/rakka-agent/spec.md) requires.
    ///
    /// The grant travels *with* the command rather than being fetched by the
    /// run, because the debit and the command commit in one transition: a run
    /// that exists holding this grant is a run whose parent has already paid
    /// for it.
    pub budget: AgentBudgetGrant,
    /// The agent definition revision the assignment was decided against.
    pub agent_definition_revision: AgentRevisionNumber,
    /// The agent settings revision the assignment was decided against.
    pub agent_settings_revision: AgentRevisionNumber,
    /// When the decision was recorded.
    pub assigned_at: AgentTimestampMillis,
}

/// The run's durable acceptance, returned as the assignment exchange's reply.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunAcceptance {
    /// The run that accepted.
    pub run: AgentRunScope,
    /// The generation it accepted.
    pub generation: AgentAssignmentGeneration,
    /// When it durably accepted.
    pub accepted_at: AgentTimestampMillis,
}

/// A run's report of what it finally consumed, carried by an
/// [`AgentExchangeKind::BudgetSettlement`] exchange
/// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
///
/// It travels only after a known terminal run outcome, and it is what makes the
/// parent's own consumption — and, in a deeper hierarchy, its parent's — the
/// truth about what the work cost.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentBudgetSettlement {
    /// The run that settled.
    pub run: AgentRunScope,
    /// The generation it served.
    pub generation: AgentAssignmentGeneration,
    /// What it consumed, per conserved dimension.
    pub consumed: AgentBudgetConsumption,
}

/// A run's release of the escrow it held and will not use, carried by an
/// [`AgentExchangeKind::BudgetReturn`] exchange
/// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
///
/// It carries no amount. The parent recorded what it escrowed and has already
/// applied the settlement, so it — not the child — computes the remainder. A
/// child that named its own refund would be asking the parent to trust the
/// child's arithmetic about the parent's ledger.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentBudgetReturn {
    /// The run releasing its escrow.
    pub run: AgentRunScope,
    /// The generation it served.
    pub generation: AgentAssignmentGeneration,
}

/// A run's request for more escrow, carried by an
/// [`AgentExchangeKind::BudgetAllocation`] exchange
/// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
///
/// The run *asks*; the grant is an ordinary parent-local allocation decision
/// under the same ceilings, and a grant of nothing is a legitimate answer. This
/// is the one allocation that is its own exchange: a run's first allocation
/// rides its assignment, because the debit and the command that carries it
/// commit in the parent's one transition, and only a later top-up needs a
/// command of its own.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentBudgetTopUpRequest {
    /// The run asking.
    pub run: AgentRunScope,
    /// The generation it serves.
    pub generation: AgentAssignmentGeneration,
    /// Which request this is, counting from one.
    ///
    /// It fences the replay window the exchange journal's bounded ring cannot:
    /// the parent's escrow records the sequence it last granted, so a re-driven
    /// request returns the original grant and an older one is refused.
    pub sequence: u64,
    /// The ceiling the run reached, so the parent decides on facts.
    pub exhaustion: AgentBudgetExhaustion,
}

/// What a ledger exchange returns to the run that initiated it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentBudgetLedgerOutcome {
    /// The escrow the parent granted, for a top-up request. It is
    /// [`AgentBudgetAllocation::nothing`] when the parent has nothing left, and
    /// absent for a settlement or a return, which grant nothing.
    pub granted: Option<AgentBudgetAllocation>,
}

/// Payload type of an [`AgentBudgetSettlement`] exchange command.
pub const AGENT_BUDGET_SETTLEMENT_PAYLOAD_TYPE: &str = "rakka.agent.BudgetSettlement";

/// Payload type of an [`AgentBudgetReturn`] exchange command.
pub const AGENT_BUDGET_RETURN_PAYLOAD_TYPE: &str = "rakka.agent.BudgetReturn";

/// Payload type of an [`AgentBudgetTopUpRequest`] exchange command.
pub const AGENT_BUDGET_TOP_UP_PAYLOAD_TYPE: &str = "rakka.agent.BudgetTopUpRequest";

/// Payload type of an [`AgentBudgetLedgerOutcome`] exchange result.
pub const AGENT_BUDGET_LEDGER_OUTCOME_PAYLOAD_TYPE: &str = "rakka.agent.BudgetLedgerOutcome";

/// A run's proposal of a typed task result
/// ([specification 9.8](../../../docs/plans/rakka-agent/spec.md)).
///
/// The run persists the proposal before it sends, and the task's decision — not
/// the run's state — is what makes the public task terminal.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentTaskResultProposal {
    /// The stable proposal identity. Replaying it returns the original decision.
    pub proposal_id: AgentOperationId,
    /// The agent that proposed it.
    pub agent: AgentId,
    /// The run that proposed it.
    pub run: AgentRunId,
    /// The generation the run owns. A proposal from a superseded generation is
    /// fenced.
    pub generation: AgentAssignmentGeneration,
    /// The task definition the run validated against.
    pub definition_id: AgentTaskDefinitionId,
    /// The revision of that definition. A mismatch fails closed.
    pub definition_version: AgentRevisionNumber,
    /// The schema the result is expressed in.
    pub result_schema: AgentSchemaRef,
    /// The bounded proposed content.
    pub content: AgentTaskContent,
    /// Evidence artifacts supporting the result.
    pub evidence: Vec<ArtifactRef>,
    /// What caused the proposal. At most [`AGENT_IDENTITY_MAX_LENGTH`] bytes:
    /// a rejection persists it, and a longer id is refused without a
    /// validation decision.
    pub causation_id: AgentCausationId,
    /// When the run proposed it.
    pub proposed_at: AgentTimestampMillis,
}

/// The task's durable decision on one result proposal, returned as the
/// proposal exchange's reply.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentTaskDecision {
    /// The proposal passed every deterministic rule; the task is `Completed`.
    Accepted {
        /// The accepted typed result.
        result: Box<AgentAcceptedResult>,
    },
    /// A deterministic rule refused the proposal.
    Rejected {
        /// The durable rejection decision.
        rejection: Box<AgentTaskRejection>,
        /// Sanitized feedback the run may use for another bounded iteration.
        feedback: String,
        /// How many further proposals the task will still consider. Zero means
        /// the rejection budget is spent and the task has failed.
        remaining_iterations: u32,
        /// The task's status after the decision.
        status: AgentTaskStatus,
    },
    /// The proposal was refused without a validation decision: it was fenced by
    /// a newer assignment generation, or the task is already terminal. It costs
    /// the live run nothing, and it consumes none of the rejection budget.
    Refused {
        /// Stable machine-readable code.
        code: String,
        /// The task's current status.
        status: AgentTaskStatus,
    },
}

/// The agent facts one assignment decision is made against.
///
/// It is read from the agent's *durable state* — never a command round trip
/// through [`crate::agent::AgentEntity`], which would serialize every run of a
/// popular agent through one mailbox
/// ([specification 9.8](../../../docs/plans/rakka-agent/spec.md)). It is bounded
/// and carries no credential material.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentAssignmentReadiness {
    /// The agent the decision is about.
    pub agent: AgentId,
    /// Its durable lifecycle status, when it is instantiated.
    pub status: Option<AgentLifecycleStatus>,
    /// The definition revision in force.
    pub definition_revision: Option<AgentRevisionNumber>,
    /// The settings revision in force.
    pub settings_revision: Option<AgentRevisionNumber>,
    /// Whether the agent's definition envelope declares this task definition.
    pub permits_task_definition: bool,
    /// Whether the agent's definition envelope declares the operation class the
    /// task runs under.
    pub permits_operation_class: bool,
    /// Whether the agent's definition envelope declares every skill the task
    /// requires.
    pub declares_required_skills: bool,
    /// Why the agent's autonomy admission does not admit this work, when it
    /// does not ([specification 7.4](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// It is `Some` for an agent with no admission at all, which is what makes
    /// the check fail closed: unattended work needs a decision that says it may
    /// run, and the absence of one is not permission.
    pub admission_refusal: Option<AgentAdmissionRefusal>,
}

impl AgentAssignmentReadiness {
    /// Reads the decision-relevant facts out of an agent's durable state.
    ///
    /// An agent whose envelope declares no task definitions is authorized for
    /// none: the check fails closed rather than treating an empty declaration as
    /// permission for everything.
    ///
    /// The admission answer is *derived* here rather than read from a flag: the
    /// decision on record is asked whether it admits this task's operation class
    /// under the agent's definition envelope as it stands now
    /// ([specification 7.4](../../../docs/plans/rakka-agent/spec.md)). A
    /// definition published since the admission that widened anything therefore
    /// stops assignment without anything having had to notice the update.
    #[must_use]
    pub fn from_agent_state(
        state: &AgentEntityState,
        definition: &AgentTaskDefinition,
        now: AgentTimestampMillis,
    ) -> Self {
        let envelope = state.definition().envelope();
        Self {
            agent: state.scope().agent().clone(),
            status: Some(state.status()),
            definition_revision: Some(state.definition().revision()),
            settings_revision: Some(state.settings().revision()),
            permits_task_definition: envelope
                .task_definitions
                .contains(&definition.definition_id),
            permits_operation_class: !definition.operation_class.is_unattended()
                || envelope
                    .operation_classes
                    .contains(&definition.operation_class),
            declares_required_skills: definition.required_skills.iter().all(|skill| {
                envelope
                    .tools
                    .values()
                    .any(|tool| tool.capabilities.contains(skill))
            }),
            admission_refusal: definition
                .operation_class
                .is_unattended()
                .then(|| {
                    state
                        .admission()
                        .map_or(Some(AgentAdmissionRefusal::Missing), |decision| {
                            // The full enforcement point: the decision is
                            // re-derived against the agent definition now in
                            // force, so a republish that dropped a verified
                            // requirement (a policy is not in the envelope) stops
                            // assignment even when the envelope did not widen.
                            decision
                                .admits_definition(
                                    definition.operation_class,
                                    state.definition().definition(),
                                    now,
                                )
                                .err()
                        })
                })
                .flatten(),
        }
    }

    /// The facts of an agent that has no durable state at all.
    ///
    /// It is refused for having no state before any of the rest is consulted,
    /// so every other fact is the fail-closed one.
    #[must_use]
    pub fn not_instantiated(agent: AgentId) -> Self {
        Self {
            agent,
            status: None,
            definition_revision: None,
            settings_revision: None,
            permits_task_definition: false,
            permits_operation_class: false,
            declares_required_skills: false,
            admission_refusal: Some(AgentAdmissionRefusal::Missing),
        }
    }

    /// Why the decision must refuse this agent, when it must.
    #[must_use]
    pub fn refusal(&self) -> Option<(AgentAssignmentRefusalReason, String)> {
        let Some(status) = self.status else {
            return Some((
                AgentAssignmentRefusalReason::AgentNotInstantiated,
                format!("agent {} has no durable state", self.agent),
            ));
        };
        if !status.permits_dispatch() {
            return Some((
                AgentAssignmentRefusalReason::AgentNotActive,
                format!("agent {} is {status}", self.agent),
            ));
        }
        if !self.permits_task_definition {
            return Some((
                AgentAssignmentRefusalReason::TaskDefinitionNotPermitted,
                format!(
                    "agent {} does not declare this task definition in its authority envelope",
                    self.agent
                ),
            ));
        }
        if !self.permits_operation_class {
            return Some((
                AgentAssignmentRefusalReason::OperationClassNotDeclared,
                format!(
                    "agent {} does not declare this task's operation class in its authority envelope",
                    self.agent
                ),
            ));
        }
        if !self.declares_required_skills {
            return Some((
                AgentAssignmentRefusalReason::SkillNotDeclared,
                format!(
                    "agent {} does not declare every skill this task requires",
                    self.agent
                ),
            ));
        }
        if let Some(refusal) = &self.admission_refusal {
            // The fail-closed rule of specification 7.4. It is checked last of
            // the envelope facts and first of nothing: an agent may be
            // instantiated, active, and fully declared, and still not admitted
            // to run unattended.
            return Some((
                AgentAssignmentRefusalReason::NotAdmitted,
                refusal.to_string(),
            ));
        }
        None
    }
}

/// The bounded materialized record of one created task
/// ([specification 9.6](../../../docs/plans/rakka-agent/spec.md)).
///
/// It holds what the next legal transition needs and nothing else: no messages,
/// no observations, no tool payloads, no superseded assignments, no result
/// proposals, no audit events, no memory records. Those are history entries and
/// artifact references.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentTask {
    /// The typed contract.
    pub definition: AgentTaskDefinition,
    /// The bounded input.
    pub input: AgentTaskContent,
    /// The public lifecycle status.
    pub status: AgentTaskStatus,
    /// The collaborative goal this task contributes to.
    pub goal: Option<AgentGoalId>,
    /// Whether it coordinates a finite or continuous goal. Records persisted
    /// before this field load as finite.
    #[serde(default)]
    pub goal_mode: AgentGoalMode,
    /// The wake controller's durable state, once the task coordinates a
    /// continuous goal. Records persisted before this field load with no
    /// controller activity, and a finite task never carries one.
    #[serde(default)]
    pub wake_controller: Option<AgentWakeControllerState>,
    /// The task that created it.
    pub parent: Option<AgentTaskId>,
    /// The agent the task is meant for, when it is agent-owned.
    pub assignee: Option<AgentId>,
    /// The bounded dependency summary.
    pub dependencies: BTreeMap<AgentTaskId, AgentTaskDependency>,
    /// The escrow this task holds and debits every run it assigns from
    /// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// It is a component of the task's own record, so an allocation is debited
    /// in the same compare-and-set as the assignment that carries it. There is
    /// no window in which a run exists holding budget its parent did not debit,
    /// and no second writer that could debit it twice.
    pub escrow: AgentEscrowLedger,
    /// The current assignment, if any.
    pub assignment: Option<AgentTaskAssignment>,
    /// The highest assignment generation the task has decided.
    pub assignment_generation: AgentAssignmentGeneration,
    /// How many assignment generations the task has consumed.
    pub assignments: u32,
    /// The most recent assignment refusal.
    pub last_refusal: Option<AgentAssignmentRefusal>,
    /// The accepted typed result.
    pub accepted_result: Option<Box<AgentAcceptedResult>>,
    /// How many result proposals deterministic rules have refused.
    pub rejection_count: u32,
    /// The most recent rejection decision. Earlier ones are history.
    pub last_rejection: Option<Box<AgentTaskRejection>>,
    /// Why the task reached its terminal status.
    pub terminal_reason: Option<AgentTaskTerminalReason>,
    /// When the creation committed, stamped by the owner that wrote it — never
    /// by the initiator's clock, exactly as
    /// [`crate::choreography::AgentExchangeParticipant::apply`] requires of
    /// every durable timestamp.
    pub created_at: AgentTimestampMillis,
    /// Trace context of the ingress that created the task, held so the
    /// assignment decided in a *later* transition can carry the causal chain
    /// onward ([specification 17.5](../../../docs/plans/rakka-agent/spec.md)).
    /// Observability only, never correctness: a task persisted before this
    /// field decodes to the empty context, and no transition reads it to
    /// decide anything.
    #[serde(default)]
    pub telemetry: AgentTelemetryContext,
}

impl AgentTask {
    /// Whether every dependency permits the task to be worked on.
    #[must_use]
    pub fn dependencies_satisfied(&self) -> bool {
        self.dependencies
            .values()
            .all(AgentTaskDependency::is_satisfied)
    }

    /// The dependency that cancels this task, when one does.
    #[must_use]
    pub fn cancelling_dependency(&self) -> Option<&AgentTaskDependency> {
        self.dependencies
            .values()
            .find(|dependency| dependency.cancels_dependent())
    }

    /// Whether the task is waiting for an agent assignment decision.
    #[must_use]
    pub fn awaits_assignment(&self) -> bool {
        self.definition.is_agent_owned()
            && self.status.is_assignable()
            && self.assignment.is_none()
            && self.dependencies_satisfied()
    }

    /// Serialized size of the materialized record, in bytes.
    ///
    /// This is what [`AGENT_TASK_MATERIALIZED_MAX_BYTES`] bounds, and what
    /// scenario 55 asserts stays inside its limit however long the task runs.
    #[must_use]
    pub fn materialized_size_bytes(&self) -> usize {
        serde_json::to_vec(self)
            .map(|bytes| bytes.len())
            .unwrap_or(0)
    }

    /// Rejects a record that exceeds its bounds, keeping `reserve` bytes below
    /// the materialized maximum.
    ///
    /// Admission-time transitions — creation and dependency declaration — pass
    /// [`AGENT_TASK_STATE_GROWTH_RESERVE_BYTES`], so every record they admit
    /// still has room for the assignment, refusal, rejection, and terminal
    /// reason its lifecycle may add; the transitions that add exactly that
    /// reserved growth pass zero.
    fn check_bounds(&self, reserve: usize) -> AgentTaskResult<()> {
        if self.dependencies.len() > self.definition.limits.max_dependencies {
            return Err(AgentTaskError::DependencyLimitExceeded {
                maximum: self.definition.limits.max_dependencies,
            });
        }
        let bytes = self.materialized_size_bytes();
        let maximum = AGENT_TASK_MATERIALIZED_MAX_BYTES.saturating_sub(reserve);
        if bytes > maximum {
            return Err(AgentTaskError::MaterializedStateTooLarge { bytes, maximum });
        }
        Ok(())
    }
}

/// The compact result of one accepted task transition.
///
/// A replayed operation returns this again rather than transitioning twice.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTaskOutcome {
    /// The task's status after the transition.
    pub status: AgentTaskStatus,
    /// The assignment generation in force.
    pub assignment_generation: AgentAssignmentGeneration,
    /// The assigned agent, when the task has one.
    pub agent: Option<AgentId>,
    /// The run serving the current assignment, when the task has one.
    pub run: Option<AgentRunId>,
    /// How many result rejections the task has recorded.
    pub rejection_count: u32,
    /// Whether every dependency permits the task to be worked on.
    pub dependencies_satisfied: bool,
    /// What a wake transition recorded, when this outcome answers one.
    /// Outcomes persisted before this field load with no wake record.
    #[serde(default)]
    pub wake: Option<AgentWakeOutcome>,
}

/// Bounded log of resolved operation ids and the outcome each produced.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AgentTaskOperationLog {
    entries: VecDeque<AgentTaskOperationLogEntry>,
}

impl AgentTaskOperationLog {
    /// The outcome a previously applied operation produced, if it is still in
    /// the window.
    #[must_use]
    pub fn outcome(&self, operation_id: &AgentOperationId) -> Option<&AgentTaskOutcome> {
        self.entries
            .iter()
            .find(|entry| &entry.operation_id == operation_id)
            .map(|entry| &entry.outcome)
    }

    /// How many operations are remembered.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether no operation is remembered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn record(&mut self, operation_id: AgentOperationId, outcome: AgentTaskOutcome) {
        self.entries.push_back(AgentTaskOperationLogEntry {
            operation_id,
            outcome,
        });
        while self.entries.len() > AGENT_TASK_OPERATION_LOG_CAPACITY {
            self.entries.pop_front();
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AgentTaskOperationLogEntry {
    operation_id: AgentOperationId,
    outcome: AgentTaskOutcome,
}

/// The durable state of one typed-task entity.
///
/// It is the task's materialized record, the history it owes its sink, the
/// operations it has resolved, and the exchange journal the choreography
/// substrate writes — all in one compare-and-set.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentTaskState {
    schema_version: StateSchemaVersion,
    scope: AgentTaskScope,
    task: Option<AgentTask>,
    applied_operations: AgentTaskOperationLog,
    pending_history: Vec<AgentTaskHistoryEntry>,
    next_history_sequence: AgentTaskHistorySequence,
    journal: AgentExchangeJournal,
    updated_at: AgentTimestampMillis,
}

impl AgentTaskState {
    /// The state of a task that has never been created.
    #[must_use]
    pub fn uncreated(scope: AgentTaskScope, now: AgentTimestampMillis) -> Self {
        Self {
            schema_version: CURRENT_AGENT_TASK_STATE_SCHEMA_VERSION,
            scope,
            task: None,
            applied_operations: AgentTaskOperationLog::default(),
            pending_history: Vec::new(),
            next_history_sequence: AgentTaskHistorySequence::FIRST,
            journal: AgentExchangeJournal::new(),
            updated_at: now,
        }
    }

    /// The scope this state belongs to.
    #[must_use]
    pub const fn scope(&self) -> &AgentTaskScope {
        &self.scope
    }

    /// The tenant boundary of this task.
    #[must_use]
    pub const fn tenant(&self) -> &TenantId {
        self.scope.tenant()
    }

    /// The materialized task, once it has been created.
    #[must_use]
    pub const fn task(&self) -> Option<&AgentTask> {
        self.task.as_ref()
    }

    /// Whether the task has been created.
    #[must_use]
    pub const fn is_created(&self) -> bool {
        self.task.is_some()
    }

    /// The task's public status, once it has been created.
    #[must_use]
    pub fn status(&self) -> Option<AgentTaskStatus> {
        self.task.as_ref().map(|task| task.status)
    }

    /// The bounded log of resolved operations.
    #[must_use]
    pub const fn applied_operations(&self) -> &AgentTaskOperationLog {
        &self.applied_operations
    }

    /// The history entries the task owes its sink.
    #[must_use]
    pub fn pending_history(&self) -> &[AgentTaskHistoryEntry] {
        &self.pending_history
    }

    /// The time of the last accepted transition.
    #[must_use]
    pub const fn updated_at(&self) -> AgentTimestampMillis {
        self.updated_at
    }

    /// The compact outcome describing the current state.
    #[must_use]
    pub fn outcome(&self) -> AgentTaskOutcome {
        let Some(task) = &self.task else {
            return AgentTaskOutcome {
                status: AgentTaskStatus::Created,
                assignment_generation: AgentAssignmentGeneration::UNASSIGNED,
                agent: None,
                run: None,
                rejection_count: 0,
                dependencies_satisfied: true,
                wake: None,
            };
        };
        AgentTaskOutcome {
            status: task.status,
            assignment_generation: task.assignment_generation,
            agent: task
                .assignment
                .as_ref()
                .map(|assignment| assignment.agent.clone()),
            run: task
                .assignment
                .as_ref()
                .map(|assignment| assignment.run.clone()),
            rejection_count: task.rejection_count,
            dependencies_satisfied: task.dependencies_satisfied(),
            wake: None,
        }
    }

    /// A bounded, credential-free projection of this state.
    #[must_use]
    pub fn snapshot(&self) -> Option<AgentTaskSnapshot> {
        let task = self.task.as_ref()?;
        Some(AgentTaskSnapshot {
            scope: self.scope.clone(),
            definition_id: task.definition.definition_id.clone(),
            definition_version: task.definition.version,
            status: task.status,
            goal: task.goal.clone(),
            parent: task.parent.clone(),
            assignment: task.assignment.clone(),
            assignment_generation: task.assignment_generation,
            dependencies: task.dependencies.values().cloned().collect(),
            dependencies_satisfied: task.dependencies_satisfied(),
            rejection_count: task.rejection_count,
            last_rejection: task.last_rejection.clone(),
            last_refusal: task.last_refusal.clone(),
            accepted_result: task.accepted_result.clone(),
            terminal_reason: task.terminal_reason.clone(),
            history_entries: self.next_history_sequence.get().saturating_sub(1),
            updated_at: self.updated_at,
            wake: task.goal_mode.continuous().map(|spec| {
                let controller = task.wake_controller.as_ref();
                AgentWakeStatusView {
                    schedule_revision: spec.schedule_revision,
                    policy_revision: spec.wake_policy.revision(),
                    active: controller
                        .map(|state| {
                            state
                                .active()
                                .iter()
                                .map(|active| active.binding().wake_id().clone())
                                .collect()
                        })
                        .unwrap_or_default(),
                    pending: controller
                        .map(|state| {
                            state
                                .pending()
                                .iter()
                                .map(|binding| binding.wake_id().clone())
                                .collect()
                        })
                        .unwrap_or_default(),
                    last_admitted: controller.and_then(|state| state.last_admitted().cloned()),
                    last_admitted_at: controller
                        .and_then(AgentWakeControllerState::last_admitted_at),
                    counters: controller
                        .map(|state| *state.counters())
                        .unwrap_or_default(),
                }
            }),
        })
    }

    /// How many further history entries the task may record before its outbox is
    /// full.
    #[must_use]
    pub fn history_headroom(&self) -> usize {
        AGENT_TASK_PENDING_HISTORY_CAPACITY.saturating_sub(self.pending_history.len())
    }

    /// Records one history entry the task now owes its sink.
    ///
    /// It cannot drop an entry, because dropping one would silently lose audit
    /// history. It cannot fail either, and it does not have to: the entity
    /// guarantees [`AGENT_TASK_MAX_HISTORY_PER_TRANSITION`] entries of headroom
    /// before it runs any transition, and no transition records more than that.
    /// A backlog is refused at the entity's door — where the exchange can still be
    /// re-driven — rather than in here, where the only ways out would be losing an
    /// entry or turning a transient sink failure into a durable decision.
    fn record_history(
        &mut self,
        build: impl FnOnce(AgentTaskHistorySequence) -> AgentTaskHistoryEntry,
    ) {
        let sequence = self.next_history_sequence;
        self.next_history_sequence = sequence.next();
        self.pending_history.push(build(sequence));
    }

    /// Drops the entries a flush has durably appended.
    fn clear_flushed_history(&mut self, flushed: &[AgentTaskHistorySequence]) {
        self.pending_history
            .retain(|entry| !flushed.contains(&entry.sequence));
    }

    fn task_mut(&mut self) -> AgentTaskResult<&mut AgentTask> {
        self.task
            .as_mut()
            .ok_or_else(|| AgentTaskError::NotCreated {
                scope: self.scope.clone(),
            })
    }
}

impl AgentExchangeState for AgentTaskState {
    fn exchange_journal(&self) -> &AgentExchangeJournal {
        &self.journal
    }

    fn exchange_journal_mut(&mut self) -> &mut AgentExchangeJournal {
        &mut self.journal
    }

    fn check_schema(&self, policy: &AgentSchemaPolicy) -> Result<(), AgentSchemaError> {
        policy.check_record(self)?;
        if let Some(task) = &self.task {
            policy.check_record(&task.definition)?;
            if let Some(spec) = task.goal_mode.continuous() {
                policy.check_record(&spec.wake_policy)?;
            }
        }
        for entry in &self.pending_history {
            policy.check_record(entry)?;
        }
        Ok(())
    }
}

impl VersionedAgentRecord for AgentTaskState {
    const RECORD_KIND: AgentRecordKind = AgentRecordKind::TaskState;

    fn schema_version(&self) -> StateSchemaVersion {
        self.schema_version
    }
}

/// A bounded, credential-free projection of one task's durable state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentTaskSnapshot {
    /// The task's scope.
    pub scope: AgentTaskScope,
    /// Its definition identity.
    pub definition_id: AgentTaskDefinitionId,
    /// The revision of that definition.
    pub definition_version: AgentRevisionNumber,
    /// Its public lifecycle status.
    pub status: AgentTaskStatus,
    /// The goal it contributes to.
    pub goal: Option<AgentGoalId>,
    /// The task that created it.
    pub parent: Option<AgentTaskId>,
    /// Its current assignment.
    pub assignment: Option<AgentTaskAssignment>,
    /// The highest assignment generation it has decided.
    pub assignment_generation: AgentAssignmentGeneration,
    /// Its bounded dependency summary.
    pub dependencies: Vec<AgentTaskDependency>,
    /// Whether every dependency permits the task to be worked on.
    pub dependencies_satisfied: bool,
    /// How many result proposals its rules have refused.
    pub rejection_count: u32,
    /// The most recent rejection decision.
    pub last_rejection: Option<Box<AgentTaskRejection>>,
    /// The most recent assignment refusal.
    pub last_refusal: Option<AgentAssignmentRefusal>,
    /// Its accepted typed result.
    pub accepted_result: Option<Box<AgentAcceptedResult>>,
    /// Why it reached its terminal status.
    pub terminal_reason: Option<AgentTaskTerminalReason>,
    /// How many history entries it has produced. The entries themselves are
    /// read through [`AgentTaskHistoryStore::read`].
    pub history_entries: u64,
    /// The time of its last accepted transition.
    pub updated_at: AgentTimestampMillis,
    /// The continuous goal's wake state, when the task coordinates one.
    /// Snapshots persisted before this field load without it.
    #[serde(default)]
    pub wake: Option<AgentWakeStatusView>,
}

/// Loads one task's durable state without waking its entity.
///
/// This is the authoritative point read of
/// [specification 17.18](../../../docs/plans/rakka-agent/spec.md): it is correct
/// while the task is passivated and while telemetry is unavailable, because it
/// reads the same record the entity transitions. The schema check applies here
/// too, so a stale reader fails closed rather than projecting a record it cannot
/// interpret.
pub async fn load_agent_task_state<Store>(
    store: &Store,
    scope: &AgentTaskScope,
    policy: &AgentSchemaPolicy,
) -> AgentTaskResult<Option<AgentTaskState>>
where
    Store: DurableStateStore<AgentTaskState>,
{
    let Some(record) = store.load(&scope.persistence_id()).await? else {
        return Ok(None);
    };
    record.state.check_schema(policy)?;
    Ok(Some(record.state))
}

/// Derives the run that serves one assignment generation.
///
/// The identity is *derived*, not generated, so replaying an assignment resolves
/// to the same run rather than creating a second one — which is what
/// "deduplicated by task id + assignment generation"
/// ([specification 9.8](../../../docs/plans/rakka-agent/spec.md)) requires of
/// the run-creation command. The derivation cannot exceed the identity bound
/// for a task the entity admitted: creation caps an agent-owned task's id at
/// [`AGENT_TASK_ASSIGNABLE_ID_MAX_LENGTH`], which reserves room for the
/// longest possible suffix.
pub fn run_id_for_assignment(
    task: &AgentTaskId,
    generation: AgentAssignmentGeneration,
) -> Result<AgentRunId, AgentIdentityError> {
    AgentRunId::new(format!("{task}-gen-{generation}"))
}

/// Derives the stable operation id of one assignment's run-creation command.
pub fn assignment_operation_id(
    scope: &AgentTaskScope,
    generation: AgentAssignmentGeneration,
) -> Result<AgentOperationId, AgentIdentityError> {
    AgentOperationId::new(
        AgentOperationKind::RunCreation,
        [
            scope.tenant().as_str(),
            scope.task().as_str(),
            &generation.to_string(),
        ],
    )
}

/// Creates the task, or fails closed.
///
/// It is the one transition both creation paths reach: the ingress
/// [`AgentTaskEntityCommand::Create`] and the delegating run's
/// [`AgentExchangeKind::Creation`] exchange.
fn create_task(
    state: &mut AgentTaskState,
    operation_id: &AgentOperationId,
    creation: AgentTaskCreation,
    now: AgentTimestampMillis,
) -> AgentTaskResult<AgentTaskOutcome> {
    if state.task.is_some() {
        // The domain fence. A replay inside the deduplication window never
        // reaches here, and one that has aged out of it is still refused.
        return Err(AgentTaskError::AlreadyCreated {
            scope: state.scope.clone(),
        });
    }

    // The definition's fields are public, so its bounded invariants cannot be
    // assumed from construction; they are re-checked before anything is
    // persisted.
    creation.definition.validate()?;
    creation.input.validate()?;

    let mut dependencies = BTreeMap::new();
    for declaration in &creation.dependencies {
        declaration.validate(state.scope.task())?;
        if let Some(existing) = dependencies.insert(
            declaration.dependency.clone(),
            AgentTaskDependency {
                dependency: declaration.dependency.clone(),
                policy: declaration.policy,
                outcome: None,
                declared_by: operation_id.clone(),
                declared_at: now,
            },
        ) {
            // The same rule as a post-creation redeclaration: repeating an edge
            // is idempotent, and it may not silently change the failure policy
            // the dependent committed to.
            if existing.policy != declaration.policy {
                return Err(AgentTaskError::DependencyConflict {
                    dependency: declaration.dependency.clone(),
                });
            }
        }
    }

    // The limit counts declared edges, not declarations, exactly as
    // `declare_dependency` does after creation.
    if dependencies.len() > creation.definition.limits.max_dependencies {
        return Err(AgentTaskError::DependencyLimitExceeded {
            maximum: creation.definition.limits.max_dependencies,
        });
    }

    if creation.goal_mode.is_continuous() && creation.goal.is_none() {
        // A continuous root control task exists to admit epochs for a goal;
        // without the goal binding there is nothing for the wake controller
        // to fence, budget, or retire against.
        return Err(AgentTaskError::ContinuousWithoutGoal);
    }

    let agent_owned = creation.definition.is_agent_owned();
    if agent_owned && creation.assignee.is_none() {
        return Err(AgentTaskError::MissingAssignee);
    }
    if agent_owned && state.scope.task().as_str().len() > AGENT_TASK_ASSIGNABLE_ID_MAX_LENGTH {
        // Every assignment derives a run id from this id plus a generation
        // suffix; an id the suffix would push past the identity bound is
        // refused here, where the caller can still choose another, rather than
        // at decision time, where the task would be created and unassignable.
        return Err(AgentTaskError::TaskIdTooLong {
            length: state.scope.task().as_str().len(),
            maximum: AGENT_TASK_ASSIGNABLE_ID_MAX_LENGTH,
        });
    }

    let status = if !dependencies.is_empty() {
        AgentTaskStatus::Blocked
    } else if agent_owned {
        AgentTaskStatus::Created
    } else {
        // A human-owned task is never assigned to an agent; it waits for an
        // authenticated completion through the same typed validation path.
        AgentTaskStatus::WaitingForInput
    };

    // The task with no parent scope to debit is escrowed exactly its ceilings:
    // it is the top of the hierarchy until the goal scope of phase 4 sits above
    // it ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)). A
    // delegated creation will carry its escrow on the creation command instead,
    // debited from the delegating run inside the run's own transition.
    let escrow = AgentEscrowLedger::new(AgentBudgetGrant::from_ceilings(
        &creation.definition.budgets,
    ));

    let wake_controller = creation
        .goal_mode
        .is_continuous()
        .then(AgentWakeControllerState::new);
    let task = AgentTask {
        definition: creation.definition,
        input: creation.input,
        status,
        goal: creation.goal,
        goal_mode: creation.goal_mode,
        wake_controller,
        parent: creation.parent,
        assignee: creation.assignee,
        dependencies,
        escrow,
        assignment: None,
        assignment_generation: AgentAssignmentGeneration::UNASSIGNED,
        assignments: 0,
        last_refusal: None,
        accepted_result: None,
        rejection_count: 0,
        last_rejection: None,
        terminal_reason: None,
        created_at: now,
        telemetry: crate::observability::sanitize_agent_telemetry_context(creation.telemetry),
    };
    // Admission reserves growth headroom: a record accepted here must still be
    // able to hold everything its own lifecycle may add.
    task.check_bounds(AGENT_TASK_STATE_GROWTH_RESERVE_BYTES)?;

    state.task = Some(task);
    state.updated_at = now;
    state.record_history(|sequence| {
        AgentTaskHistoryEntry::new(
            sequence,
            AgentTaskHistoryKind::Created,
            operation_id.clone(),
            status,
            now,
        )
    });
    // Each declared edge is its own history row, so scenario 37's "one
    // dependency edge" is observable in history as well as in state.
    let declared: Vec<AgentTaskId> = state
        .task
        .as_ref()
        .expect("the task was just created")
        .dependencies
        .keys()
        .cloned()
        .collect();
    for dependency in declared {
        state.record_history(|sequence| {
            AgentTaskHistoryEntry::new(
                sequence,
                AgentTaskHistoryKind::DependencyDeclared,
                operation_id.clone(),
                status,
                now,
            )
            .with_detail(dependency.to_string())
        });
    }

    Ok(state.outcome())
}

/// Declares one dependency edge after creation.
fn declare_dependency(
    state: &mut AgentTaskState,
    operation_id: &AgentOperationId,
    declaration: &AgentTaskDependencyDeclaration,
    now: AgentTimestampMillis,
) -> AgentTaskResult<AgentTaskOutcome> {
    let scope_task = state.scope.task().clone();
    let task = state.task_mut()?;
    if task.status.is_terminal() {
        return Err(AgentTaskError::Terminal {
            status: task.status,
        });
    }
    declaration.validate(&scope_task)?;

    if let Some(existing) = task.dependencies.get(&declaration.dependency) {
        // The edge exists. Redeclaring it is idempotent, and it may not silently
        // change the policy a dependent already committed to.
        if existing.policy != declaration.policy {
            return Err(AgentTaskError::DependencyConflict {
                dependency: declaration.dependency.clone(),
            });
        }
        return Ok(state.outcome());
    }

    if task.dependencies.len() >= task.definition.limits.max_dependencies {
        return Err(AgentTaskError::DependencyLimitExceeded {
            maximum: task.definition.limits.max_dependencies,
        });
    }

    task.dependencies.insert(
        declaration.dependency.clone(),
        AgentTaskDependency {
            dependency: declaration.dependency.clone(),
            policy: declaration.policy,
            outcome: None,
            declared_by: operation_id.clone(),
            declared_at: now,
        },
    );
    // A task that has become dependent again is no longer eligible.
    if matches!(task.status, AgentTaskStatus::Created) {
        task.status = AgentTaskStatus::Blocked;
    }
    // A late edge grows the admitted record, so it keeps the same growth
    // headroom the creation reserved.
    task.check_bounds(AGENT_TASK_STATE_GROWTH_RESERVE_BYTES)?;

    let status = task.status;
    let detail = declaration.dependency.to_string();
    state.updated_at = now;
    state.record_history(|sequence| {
        AgentTaskHistoryEntry::new(
            sequence,
            AgentTaskHistoryKind::DependencyDeclared,
            operation_id.clone(),
            status,
            now,
        )
        .with_detail(detail)
    });
    Ok(state.outcome())
}

/// Records how one dependency resolved, and propagates its policy.
///
/// The default failed-dependency policy cancels the dependent
/// ([specification 9.2](../../../docs/plans/rakka-agent/spec.md)). The command
/// is the receiving half of the propagation; the sending half — an upstream task
/// notifying its dependents when it goes terminal — is cancellation propagation,
/// and it lands with the coordination slices that own the dependents registry.
fn record_dependency_outcome(
    state: &mut AgentTaskState,
    operation_id: &AgentOperationId,
    dependency: &AgentTaskId,
    outcome: AgentTaskDependencyOutcome,
    now: AgentTimestampMillis,
) -> AgentTaskResult<AgentTaskOutcome> {
    let task = state.task_mut()?;
    if task.status.is_terminal() {
        return Err(AgentTaskError::Terminal {
            status: task.status,
        });
    }

    let edge =
        task.dependencies
            .get_mut(dependency)
            .ok_or_else(|| AgentTaskError::UnknownDependency {
                dependency: dependency.clone(),
            })?;

    match edge.outcome {
        // A dependency resolves once. A replayed outcome is idempotent; a
        // *different* outcome for a resolved dependency is a conflict, not a
        // correction, and it fails closed.
        Some(existing) if existing == outcome => return Ok(state.outcome()),
        Some(_) => {
            return Err(AgentTaskError::DependencyConflict {
                dependency: dependency.clone(),
            })
        }
        None => edge.outcome = Some(outcome),
    }

    let cancelling = task
        .cancelling_dependency()
        .map(|edge| (edge.dependency.clone(), edge.outcome));

    if let Some((dependency, Some(outcome))) = cancelling {
        terminate(
            state,
            operation_id,
            AgentTaskTerminalReason::DependencyNotSatisfied {
                dependency,
                outcome,
            },
            now,
        )?;
        return Ok(state.outcome());
    }

    let task = state.task_mut()?;
    if task.dependencies_satisfied() && matches!(task.status, AgentTaskStatus::Blocked) {
        // Eligible at last. The entity's next pass decides the assignment; it
        // cannot happen here, because the decision needs the agent's durable
        // state and reading it is I/O.
        task.status = if task.definition.is_agent_owned() {
            AgentTaskStatus::Created
        } else {
            AgentTaskStatus::WaitingForInput
        };
    }

    let status = task.status;
    let detail = format!("{dependency} {outcome}");
    state.updated_at = now;
    state.record_history(|sequence| {
        AgentTaskHistoryEntry::new(
            sequence,
            AgentTaskHistoryKind::DependencyResolved,
            operation_id.clone(),
            status,
            now,
        )
        .with_detail(detail)
    });
    Ok(state.outcome())
}

/// Moves the task to a terminal status and records why.
fn terminate(
    state: &mut AgentTaskState,
    operation_id: &AgentOperationId,
    reason: AgentTaskTerminalReason,
    now: AgentTimestampMillis,
) -> AgentTaskResult<()> {
    let task = state.task_mut()?;
    if task.status.is_terminal() {
        return Err(AgentTaskError::Terminal {
            status: task.status,
        });
    }
    task.status = reason.status();
    task.terminal_reason = Some(reason.clone());
    // A terminal task fences its run: the assignment is retired, so a late
    // proposal from it can no longer be validated.
    task.assignment = None;

    let status = task.status;
    state.updated_at = now;
    state.record_history(|sequence| {
        AgentTaskHistoryEntry::new(
            sequence,
            AgentTaskHistoryKind::Terminated,
            operation_id.clone(),
            status,
            now,
        )
        .with_detail(reason.code())
    });
    Ok(())
}

/// The continuous root control task a wake command must address, or the
/// refusal it answers instead.
///
/// Every wake transition shares these fences: the task must exist, must not be
/// terminal, and must coordinate a continuous goal.
fn continuous_task_mut(state: &mut AgentTaskState) -> AgentTaskResult<&mut AgentTask> {
    let scope = state.scope.clone();
    let task = state
        .task
        .as_mut()
        .ok_or(AgentTaskError::NotCreated { scope })?;
    if task.status.is_terminal() {
        return Err(AgentTaskError::Terminal {
            status: task.status,
        });
    }
    if !task.goal_mode.is_continuous() {
        return Err(AgentTaskError::WakeNotContinuous);
    }
    Ok(task)
}

/// Dispositions one delivered wake occurrence, or fails closed.
///
/// The operation id must be the one the binding itself derives — every trigger
/// path reconstructs it, so a delivery whose id disagrees with its own binding
/// is not a redelivery, it is a forgery, and it is refused before any state is
/// read. The disposition — including a fence or a skip — is a recorded
/// transition, which is what makes the wake counters exact and a replayed
/// delivery a [`AgentTaskEntityReply::Duplicate`] instead of a second epoch.
fn admit_wake(
    state: &mut AgentTaskState,
    operation_id: &AgentOperationId,
    binding: AgentWakeBinding,
    now: AgentTimestampMillis,
) -> AgentTaskResult<AgentWakeOutcome> {
    let expected = binding.admission_operation_id()?;
    if *operation_id != expected {
        return Err(AgentTaskError::WakeOperationMismatch);
    }
    let task = continuous_task_mut(state)?;
    if task.goal.as_ref() != Some(binding.goal()) {
        return Err(AgentTaskError::WakeGoalMismatch {
            offered: binding.goal().clone(),
        });
    }
    let spec = task
        .goal_mode
        .continuous()
        .expect("continuous_task_mut proved the mode");
    let current_revision = spec.schedule_revision;
    let policy = spec.wake_policy.policy().clone();
    let disposition = task
        .wake_controller
        .get_or_insert_with(AgentWakeControllerState::new)
        .admit(&policy, current_revision, binding, now)?;
    // Admission stores at most the bounded slots, but the record must still
    // keep its lifecycle growth reserve free.
    task.check_bounds(AGENT_TASK_STATE_GROWTH_RESERVE_BYTES)?;
    state.updated_at = now;
    Ok(AgentWakeOutcome::Disposition(disposition))
}

/// Releases the active occurrence a completed execution owned, promoting the
/// oldest parked occurrence in the same transition.
///
/// Slice 3.3's epoch-result path drives this same transition; the command
/// exists so the release is a durable, deduplicated act rather than an
/// implicit consequence of anything resident.
fn complete_wake_occurrence(
    state: &mut AgentTaskState,
    wake: &AgentWakeId,
    now: AgentTimestampMillis,
) -> AgentTaskResult<AgentWakeOutcome> {
    let task = continuous_task_mut(state)?;
    let release = task
        .wake_controller
        .get_or_insert_with(AgentWakeControllerState::new)
        .release(wake, now)?;
    state.updated_at = now;
    Ok(AgentWakeOutcome::Release(release))
}

/// Takes a schedule update into force, fencing every parked occurrence the
/// old schedule created.
///
/// The revision must move strictly forward — a replayed update inside the
/// deduplication window answers as a duplicate, and one outside it is refused
/// here, so a restart can never reset the revision
/// ([specification 6.9](../../../docs/plans/rakka-agent/spec.md)). A policy
/// update rides the same transition and must also move strictly forward.
fn update_continuous_schedule(
    state: &mut AgentTaskState,
    schedule_revision: ScheduleRevision,
    wake_policy: Option<AgentWakePolicyRevision>,
    now: AgentTimestampMillis,
) -> AgentTaskResult<AgentWakeOutcome> {
    let task = continuous_task_mut(state)?;
    let AgentGoalMode::Continuous(spec) = &mut task.goal_mode else {
        unreachable!("continuous_task_mut proved the mode");
    };
    if schedule_revision <= spec.schedule_revision {
        return Err(AgentTaskError::ScheduleNotMonotonic {
            offered: schedule_revision,
            current: spec.schedule_revision,
        });
    }
    if let Some(policy) = wake_policy {
        if policy.revision() <= spec.wake_policy.revision() {
            return Err(AgentTaskError::WakePolicyNotNewer {
                offered: policy.revision(),
                current: spec.wake_policy.revision(),
            });
        }
        policy.policy().validate()?;
        spec.wake_policy = policy;
    }
    spec.schedule_revision = schedule_revision;
    let policy_revision = spec.wake_policy.revision();
    let fenced = task
        .wake_controller
        .get_or_insert_with(AgentWakeControllerState::new)
        .fence_obsolete_pending(schedule_revision);
    task.check_bounds(AGENT_TASK_STATE_GROWTH_RESERVE_BYTES)?;
    state.updated_at = now;
    Ok(AgentWakeOutcome::ScheduleUpdated {
        schedule_revision,
        policy_revision,
        fenced,
    })
}

/// Why the task cannot escrow a run right now, when it cannot.
///
/// The affordability answer is derived from the ledger without touching it, so
/// both the decision and the settle pass that predicts the decision ask the same
/// question of the same record. It predicts *every* way `open_child` can refuse
/// — an exhausted dimension and a full child set alike — so the decision path
/// records a stable refusal and stays assignable rather than failing the
/// transition on an error the prediction never saw.
fn task_budget_refusal(task: &AgentTask) -> Option<(AgentAssignmentRefusalReason, String)> {
    if task.escrow.outstanding().count() >= AGENT_ESCROW_CHILD_CAPACITY {
        return Some((
            AgentAssignmentRefusalReason::BudgetUnavailable,
            format!(
                "the task cannot escrow a run: its ledger already holds \
                 {AGENT_ESCROW_CHILD_CAPACITY} outstanding children"
            ),
        ));
    }
    let request = task.definition.run_allocation_request();
    let affordable = request.narrowed_to(&task.escrow.available_allocation());
    let dimension = affordable.first_empty_for(&request)?;
    // The exhaustion reports what is *spoken for* — consumption and
    // still-outstanding child escrow together — because that is what the
    // headroom is measured against. Reporting consumption alone would tell an
    // operator "consumed 0 of 10" about a ledger whose 10 are all escrowed to
    // a child that has not settled yet.
    let limit = task.escrow.allocation().get(dimension).unwrap_or(0);
    let available = task.escrow.available(dimension).unwrap_or(0);
    let exhaustion = AgentBudgetExhaustion::new(dimension, limit, limit.saturating_sub(available));
    Some((
        AgentAssignmentRefusalReason::BudgetUnavailable,
        format!("the task cannot escrow a run: {exhaustion}"),
    ))
}

/// The refusal one assignment decision would record now, if it would record
/// one.
///
/// It mirrors [`decide_assignment`]'s order, so the settle pass can predict the
/// decision without running it. It deliberately reports nothing when the
/// decision would *terminate* the task: an exhausted assignment limit is a
/// change worth a transition, not a refusal to deduplicate against.
fn pending_assignment_refusal(
    task: &AgentTask,
    readiness: &AgentAssignmentReadiness,
) -> Option<(AgentAssignmentRefusalReason, String)> {
    if let Some(refusal) = readiness.refusal() {
        return Some(refusal);
    }
    if task.assignments >= task.definition.limits.max_assignments {
        return None;
    }
    task_budget_refusal(task)
}

/// Whether the refusal on record already states this fact, so recording it
/// again would add nothing.
fn assignment_refusal_is_current(
    task: &AgentTask,
    agent: &AgentId,
    reason: AgentAssignmentRefusalReason,
    detail: &str,
) -> bool {
    task.last_refusal
        .as_ref()
        .is_some_and(|last| last.agent == *agent && last.reason == reason && last.detail == detail)
}

/// Records one assignment refusal and stays assignable.
///
/// Failing closed here is not failing the task: resuming the agent, widening
/// its envelope, re-admitting it, or an earlier generation settling its escrow
/// all let the next decision succeed without re-creating anything. The refusal
/// deduplicates against the one on record, because the decision runs inside the
/// command transition and again on every settle pass, and the same agent
/// refused for the same reason is not a new fact.
fn refuse_assignment(
    state: &mut AgentTaskState,
    readiness: &AgentAssignmentReadiness,
    reason: AgentAssignmentRefusalReason,
    detail: impl Into<String>,
    now: AgentTimestampMillis,
) -> AgentTaskResult<Option<AgentExchangeEnvelope>> {
    let scope = state.scope.clone();
    let detail = bounded_detail(detail);
    let task = state.task_mut()?;
    if assignment_refusal_is_current(task, &readiness.agent, reason, &detail) {
        // Re-recording it — on the settle pass of the same command, or on every
        // recovery sweep while the agent stays refused — would grow the
        // append-only history without a new fact.
        return Ok(None);
    }

    task.last_refusal = Some(AgentAssignmentRefusal {
        agent: readiness.agent.clone(),
        reason,
        detail: detail.clone(),
        refused_at: now,
    });
    let status = task.status;
    let operation_id = assignment_operation_id(&scope, task.assignment_generation.next())?;
    state.updated_at = now;
    state.record_history(|sequence| {
        AgentTaskHistoryEntry::new(
            sequence,
            AgentTaskHistoryKind::AssignmentRefused,
            operation_id.clone(),
            status,
            now,
        )
        .with_detail(format!("{}: {detail}", reason.code()))
    });
    Ok(None)
}

/// Decides one assignment against the agent facts the entity read from durable
/// state, and returns the run-creation exchange the decision owes.
///
/// The transition is idempotent on the task's own state: a task that already has
/// an assignment is not assignable, so a replay produces no second generation.
fn decide_assignment(
    state: &mut AgentTaskState,
    readiness: &AgentAssignmentReadiness,
    now: AgentTimestampMillis,
) -> AgentTaskResult<Option<AgentExchangeEnvelope>> {
    let scope = state.scope.clone();
    let task = state.task_mut()?;
    if !task.awaits_assignment() {
        return Ok(None);
    }

    if let Some((reason, detail)) = readiness.refusal() {
        return refuse_assignment(state, readiness, reason, detail, now);
    }

    if task.assignments >= task.definition.limits.max_assignments {
        let assignments = task.assignments;
        let operation_id = assignment_operation_id(&scope, task.assignment_generation)?;
        terminate(
            state,
            &operation_id,
            AgentTaskTerminalReason::AssignmentsExhausted { assignments },
            now,
        )?;
        return Ok(None);
    }

    let (agent_definition_revision, agent_settings_revision) =
        match (readiness.definition_revision, readiness.settings_revision) {
            (Some(definition), Some(settings)) => (definition, settings),
            _ => return Err(AgentTaskError::NotReady),
        };

    let generation = task.assignment_generation.next();
    let run = run_id_for_assignment(scope.task(), generation)?;
    let run_scope =
        AgentRunScope::new(scope.tenant().clone(), readiness.agent.clone(), run.clone())?;
    let operation_id = assignment_operation_id(&scope, generation)?;

    // The escrow debit, inside the creating transition
    // ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)). It is
    // decided against the same record it is written to, which is what makes
    // oversubscription impossible without any distributed transaction: the task
    // is the ledger's only writer, and the assignment that carries the grant
    // commits with the debit.
    //
    // Affordability is settled before anything is debited, so the refusal path
    // leaves the ledger exactly as it found it. Refusing is not failing: the
    // task stays assignable, and an earlier generation's settlement may restore
    // the headroom, so this takes the same path a suspended agent's refusal
    // takes.
    if let Some((reason, detail)) = task_budget_refusal(task) {
        return refuse_assignment(state, readiness, reason, detail, now);
    }
    let request = task.definition.run_allocation_request();
    let allocation = task
        .escrow
        .open_child(AgentEscrowChildId::for_run(&run)?, &request)?;
    let budget = AgentBudgetGrant::new(allocation, *task.escrow.limits());

    let assignment = AgentTaskAssignment {
        generation,
        agent: readiness.agent.clone(),
        run,
        status: AgentAssignmentStatus::Offered,
        agent_definition_revision,
        agent_settings_revision,
        operation_id: operation_id.clone(),
        assigned_at: now,
    };

    let command = AgentRunAssignment {
        task: scope.clone(),
        run: run_scope.clone(),
        generation,
        definition: task.definition.clone(),
        input: task.input.clone(),
        goal: task.goal.clone(),
        budget,
        agent_definition_revision,
        agent_settings_revision,
        assigned_at: now,
    };
    // The payload is bounded by the substrate. A definition and input that do
    // not fit belong behind an artifact reference, and the decision fails closed
    // rather than persisting an assignment whose command can never be delivered.
    let payload = AgentExchangePayload::encode(AGENT_RUN_ASSIGNMENT_PAYLOAD_TYPE, &command)?;
    // The assignment carries the ingress's causal chain onward: the segment
    // that created the task is the cause of the assignment it decided
    // ([specification 17.5]). Stamping the empty context is stamping nothing.
    let envelope = AgentExchangeEnvelope::new(
        operation_id.clone(),
        AgentExchangeKind::Assignment,
        AgentEntityAddress::Task(scope),
        AgentEntityAddress::Run(run_scope),
        payload,
        AgentCorrelationId::new(operation_id.as_str()),
        now,
    )?
    .with_telemetry(task.telemetry.clone());

    task.assignment_generation = generation;
    task.assignments += 1;
    task.status = AgentTaskStatus::Assigned;
    task.last_refusal = None;
    task.assignment = Some(assignment.clone());
    // The assignment spends growth headroom admission reserved, so it checks
    // the full bound: it cannot fail for a record admission accepted.
    task.check_bounds(0)?;

    state.updated_at = now;
    state.record_history(|sequence| {
        AgentTaskHistoryEntry::new(
            sequence,
            AgentTaskHistoryKind::AssignmentDecided,
            operation_id.clone(),
            AgentTaskStatus::Assigned,
            now,
        )
        .with_assignment(&assignment)
    });
    Ok(Some(envelope))
}

/// Validates one proposed result by deterministic rules alone, and records the
/// durable decision.
///
/// Nothing here performs I/O: a model-assisted or external evaluator is a
/// durable effect that returns evidence to the task, never a call from inside
/// this transition ([specification 9.2](../../../docs/plans/rakka-agent/spec.md)).
fn apply_result_proposal(
    state: &mut AgentTaskState,
    envelope: &AgentExchangeEnvelope,
    now: AgentTimestampMillis,
) -> AgentExchangeResult {
    let proposal: AgentTaskResultProposal = match envelope
        .payload()
        .decode(AGENT_TASK_RESULT_PROPOSAL_PAYLOAD_TYPE)
    {
        Ok(proposal) => proposal,
        Err(error) => return refuse(state, error.code(), error.to_string()),
    };

    if proposal.causation_id.as_str().len() > AGENT_IDENTITY_MAX_LENGTH {
        // The causation id is the one externally supplied field a rejection
        // persists whose type does not bound itself, and the admission-time
        // growth reserve is sized against bounded fields only. Refusing costs
        // the run nothing; a corrected proposal is a new decision.
        return refuse(
            state,
            "proposal-causation-too-long",
            format!(
                "the proposal's causation id is {} bytes, and the task persists at most \
                 {AGENT_IDENTITY_MAX_LENGTH}",
                proposal.causation_id.as_str().len()
            ),
        );
    }

    let Some(task) = state.task.as_ref() else {
        return refuse(
            state,
            "task-not-created",
            "the task does not exist".to_string(),
        );
    };

    if task.status.is_terminal() {
        return refuse(
            state,
            "task-terminal",
            format!("the task is already {}", task.status),
        );
    }

    // The assignment fence. A run of a superseded generation may not complete the
    // public task, and its proposal must not consume the live run's rejection
    // budget ([specification 9.3](../../../docs/plans/rakka-agent/spec.md)).
    let Some(assignment) = task.assignment.as_ref() else {
        return refuse(
            state,
            "task-not-assigned",
            "the task has no current assignment".to_string(),
        );
    };
    if assignment.generation != proposal.generation || assignment.run != proposal.run {
        return refuse(
            state,
            AGENT_TASK_REFUSAL_STALE_GENERATION,
            format!(
                "generation {} is not the current generation {}",
                proposal.generation, assignment.generation
            ),
        );
    }

    let digest = proposal.content.digest();
    let proposal_id = proposal.proposal_id.clone();
    let status_before = task.status;
    state.record_history(|sequence| {
        AgentTaskHistoryEntry::new(
            sequence,
            AgentTaskHistoryKind::ResultProposed,
            proposal_id.clone(),
            status_before,
            now,
        )
        .with_digest(digest.clone())
        .with_detail(proposal.run.to_string())
    });

    let task = state.task.as_ref().expect("the task exists on this path");
    match task.definition.validate_proposal(&proposal) {
        Ok(()) => accept_result(state, &proposal, digest, now),
        Err(cause) => reject_result(state, &proposal, digest, cause, now),
    }
}

fn accept_result(
    state: &mut AgentTaskState,
    proposal: &AgentTaskResultProposal,
    digest: AgentContentDigest,
    now: AgentTimestampMillis,
) -> AgentExchangeResult {
    let accepted = AgentAcceptedResult {
        proposal_id: proposal.proposal_id.clone(),
        run: proposal.run.clone(),
        definition_id: proposal.definition_id.clone(),
        definition_version: proposal.definition_version,
        result_schema: proposal.result_schema.clone(),
        content: proposal.content.clone(),
        digest: digest.clone(),
        evidence: proposal.evidence.clone(),
        accepted_at: now,
    };

    if state.task.is_none() {
        return refuse(
            state,
            "task-not-created",
            "the task does not exist".to_string(),
        );
    }

    let bounded = {
        let task = state.task.as_mut().expect("the task exists on this path");
        task.accepted_result = Some(Box::new(accepted.clone()));
        // An accepted result is not covered by the admission reserve: unlike
        // an assignment or a rejection, an oversized one has a graceful retry
        // — the run resubmits it behind an artifact reference.
        task.check_bounds(0)
    };
    if let Err(error) = bounded {
        // The accepted result would push the materialized record past its bound.
        // Refusing is the only safe answer: a task must never persist a record it
        // cannot bound, and the run must resubmit the result behind an artifact
        // reference.
        state
            .task
            .as_mut()
            .expect("the task exists on this path")
            .accepted_result = None;
        return refuse(state, error.code(), error.to_string());
    }

    let proposal_id = proposal.proposal_id.clone();
    if terminate(
        state,
        &proposal_id,
        AgentTaskTerminalReason::ResultAccepted,
        now,
    )
    .is_err()
    {
        return refuse(
            state,
            "task-terminal",
            "the task is already terminal".to_string(),
        );
    }

    state.record_history(|sequence| {
        AgentTaskHistoryEntry::new(
            sequence,
            AgentTaskHistoryKind::ResultAccepted,
            proposal_id.clone(),
            AgentTaskStatus::Completed,
            now,
        )
        .with_digest(digest)
    });
    state.updated_at = now;

    decision(AgentTaskDecision::Accepted {
        result: Box::new(accepted),
    })
}

fn reject_result(
    state: &mut AgentTaskState,
    proposal: &AgentTaskResultProposal,
    digest: AgentContentDigest,
    cause: AgentTaskRejectionCause,
    now: AgentTimestampMillis,
) -> AgentExchangeResult {
    if state.task.is_none() {
        return refuse(
            state,
            "task-not-created",
            "the task does not exist".to_string(),
        );
    }
    let task = state.task.as_mut().expect("the task exists on this path");

    task.rejection_count += 1;
    let rejection = AgentTaskRejection {
        proposal_id: proposal.proposal_id.clone(),
        digest: digest.clone(),
        cause: cause.clone(),
        rejection_count: task.rejection_count,
        causation_id: proposal.causation_id.clone(),
        rejected_at: now,
    };
    task.last_rejection = Some(Box::new(rejection.clone()));

    let exhausted = task.rejection_count >= task.definition.limits.max_result_rejections;
    let remaining = task
        .definition
        .limits
        .max_result_rejections
        .saturating_sub(task.rejection_count);
    let rejections = task.rejection_count;
    let proposal_id = proposal.proposal_id.clone();

    let status_after = if exhausted {
        // The rejection budget is spent. The task fails; it never silently
        // accepts the proposal it just refused
        // ([specification 9.2](../../../docs/plans/rakka-agent/spec.md)).
        let _ = terminate(
            state,
            &proposal_id,
            AgentTaskTerminalReason::ResultRejectionsExhausted { rejections },
            now,
        );
        AgentTaskStatus::Failed
    } else {
        state
            .task
            .as_ref()
            .map_or(AgentTaskStatus::Failed, |task| task.status)
    };

    state.record_history(|sequence| {
        AgentTaskHistoryEntry::new(
            sequence,
            AgentTaskHistoryKind::ResultRejected,
            proposal_id.clone(),
            status_after,
            now,
        )
        .with_digest(digest)
        .with_detail(format!("{}: {}", cause.reason, cause.detail))
    });
    state.updated_at = now;

    let feedback = bounded_detail(format!("{}: {}", cause.reason, cause.detail));
    let code = cause.reason.clone();
    let payload = decision_payload(&AgentTaskDecision::Rejected {
        rejection: Box::new(rejection),
        feedback,
        remaining_iterations: remaining,
        status: status_after,
    });
    // A rule rejection is a durable *decision*, not a failure: it travels home as
    // the exchange's result, is returned unchanged on replay, and the run settles
    // on it.
    AgentExchangeResult::rejected(code, "the proposed result was refused", payload)
}

/// The task's half of the three ledger exchanges
/// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
///
/// Each is idempotent on the task's own escrow record rather than on the
/// journal's bounded ring, which is what makes a replay that has outlived the
/// window still safe: the ledger answers from what it already holds
/// ([specification 18](../../../docs/plans/rakka-agent/spec.md) scenario 61).
fn apply_ledger_exchange(
    state: &mut AgentTaskState,
    envelope: &AgentExchangeEnvelope,
    now: AgentTimestampMillis,
) -> AgentExchangeResult {
    let kind = envelope.kind();
    let outcome = match kind {
        AgentExchangeKind::BudgetSettlement => {
            match envelope
                .payload()
                .decode::<AgentBudgetSettlement>(AGENT_BUDGET_SETTLEMENT_PAYLOAD_TYPE)
            {
                Ok(settlement) => apply_settlement(state, &settlement),
                Err(error) => return refuse(state, error.code(), error.to_string()),
            }
        }
        AgentExchangeKind::BudgetReturn => {
            match envelope
                .payload()
                .decode::<AgentBudgetReturn>(AGENT_BUDGET_RETURN_PAYLOAD_TYPE)
            {
                Ok(release) => apply_return(state, &release),
                Err(error) => return refuse(state, error.code(), error.to_string()),
            }
        }
        AgentExchangeKind::BudgetAllocation => {
            match envelope
                .payload()
                .decode::<AgentBudgetTopUpRequest>(AGENT_BUDGET_TOP_UP_PAYLOAD_TYPE)
            {
                Ok(request) => apply_top_up(state, &request),
                Err(error) => return refuse(state, error.code(), error.to_string()),
            }
        }
        other => Err(AgentTaskError::InvalidDefinition {
            detail: format!("{other} is not a ledger exchange"),
        }),
    };

    match outcome {
        Ok(granted) => {
            state.updated_at = now;
            ledger_outcome(granted)
        }
        Err(error) => refuse(state, error.code(), error.to_string()),
    }
}

fn apply_settlement(
    state: &mut AgentTaskState,
    settlement: &AgentBudgetSettlement,
) -> AgentTaskResult<Option<AgentBudgetAllocation>> {
    let child = AgentEscrowChildId::for_run(settlement.run.run())?;
    let task = state.task_mut()?;
    task.escrow.settle_child(&child, &settlement.consumed)?;
    Ok(None)
}

fn apply_return(
    state: &mut AgentTaskState,
    release: &AgentBudgetReturn,
) -> AgentTaskResult<Option<AgentBudgetAllocation>> {
    let child = AgentEscrowChildId::for_run(release.run.run())?;
    let task = state.task_mut()?;
    // The ledger refuses a return the settlement has not preceded, so a lost or
    // reordered settlement can never release headroom that was already spent.
    task.escrow.return_child(&child)?;
    Ok(None)
}

fn apply_top_up(
    state: &mut AgentTaskState,
    request: &AgentBudgetTopUpRequest,
) -> AgentTaskResult<Option<AgentBudgetAllocation>> {
    let child = AgentEscrowChildId::for_run(request.run.run())?;
    let task = state.task_mut()?;
    // The grant is the parent-local decision of specification 9.7: what the run
    // asks for, narrowed to what this task can still afford under its own
    // ceilings. Nothing here reads another scope's ledger.
    let wanted = task.definition.run_allocation_request();
    let granted = task
        .escrow
        .top_up_child(&child, request.sequence, &wanted)?;
    Ok(Some(granted))
}

fn ledger_outcome(granted: Option<AgentBudgetAllocation>) -> AgentExchangeResult {
    let outcome = AgentBudgetLedgerOutcome { granted };
    let payload = AgentExchangePayload::encode(AGENT_BUDGET_LEDGER_OUTCOME_PAYLOAD_TYPE, &outcome)
        .unwrap_or_else(|_| AgentExchangePayload::empty(AGENT_BUDGET_LEDGER_OUTCOME_PAYLOAD_TYPE));
    AgentExchangeResult::accepted(payload)
}

/// Refuses an exchange without making a validation decision.
fn refuse(state: &AgentTaskState, code: &str, message: String) -> AgentExchangeResult {
    let status = state.status().unwrap_or(AgentTaskStatus::Created);
    let payload = decision_payload(&AgentTaskDecision::Refused {
        code: code.to_string(),
        status,
    });
    AgentExchangeResult::rejected(code, message, payload)
}

fn decision(decision: AgentTaskDecision) -> AgentExchangeResult {
    AgentExchangeResult::accepted(decision_payload(&decision))
}

fn decision_payload(decision: &AgentTaskDecision) -> AgentExchangePayload {
    AgentExchangePayload::encode(AGENT_TASK_DECISION_PAYLOAD_TYPE, decision)
        .unwrap_or_else(|_| AgentExchangePayload::empty(AGENT_TASK_DECISION_PAYLOAD_TYPE))
}

/// The domain half of the typed-task entity.
///
/// It supplies bounded, pure transitions and nothing else; the choreography
/// substrate owns durability, deduplication, re-drive, and routing.
#[derive(Debug, Clone, Copy, Default)]
pub struct AgentTaskParticipant;

impl AgentExchangeParticipant for AgentTaskParticipant {
    type State = AgentTaskState;

    fn initialize(&self, address: &AgentEntityAddress, now: AgentTimestampMillis) -> Self::State {
        let scope = match address {
            AgentEntityAddress::Task(scope) => scope.clone(),
            // The host builds a participant for the address it serves, and the
            // entity refuses an id that does not parse into a task scope, so this
            // is unreachable in practice. Panicking would take down a shard owner
            // over a routing bug; an uncreated task under an address that can
            // never receive a creation is inert instead.
            other => AgentTaskScope::new(other.tenant().clone(), unroutable_task_id())
                .expect("the unroutable task scope is well formed"),
        };
        AgentTaskState::uncreated(scope, now)
    }

    fn apply(
        &self,
        state: &mut Self::State,
        envelope: &AgentExchangeEnvelope,
        now: AgentTimestampMillis,
    ) -> AgentExchangeTransition {
        let result = match envelope.kind() {
            AgentExchangeKind::Creation => apply_creation_exchange(state, envelope, now),
            AgentExchangeKind::ResultProposal => apply_result_proposal(state, envelope, now),
            AgentExchangeKind::BudgetAllocation
            | AgentExchangeKind::BudgetSettlement
            | AgentExchangeKind::BudgetReturn => apply_ledger_exchange(state, envelope, now),
            kind => refuse(
                state,
                "unsupported-exchange",
                format!("a task entity does not receive a {kind} exchange"),
            ),
        };
        AgentExchangeTransition::new(result)
    }

    fn settle(
        &self,
        state: &mut Self::State,
        envelope: &AgentExchangeEnvelope,
        result: &AgentExchangeResult,
        now: AgentTimestampMillis,
    ) -> Vec<AgentExchangeEnvelope> {
        if envelope.kind() == AgentExchangeKind::Assignment {
            settle_assignment(state, envelope, result, now);
        }
        Vec::new()
    }
}

fn unroutable_task_id() -> AgentTaskId {
    AgentTaskId::new("unroutable").expect("the literal is a valid task id")
}

/// Creates a task from a delegating run's [`AgentExchangeKind::Creation`]
/// exchange.
fn apply_creation_exchange(
    state: &mut AgentTaskState,
    envelope: &AgentExchangeEnvelope,
    now: AgentTimestampMillis,
) -> AgentExchangeResult {
    let creation: AgentTaskCreation =
        match envelope.payload().decode(AGENT_TASK_CREATION_PAYLOAD_TYPE) {
            Ok(creation) => creation,
            Err(error) => return refuse(state, error.code(), error.to_string()),
        };

    match create_task(state, envelope.operation_id(), creation, now) {
        Ok(outcome) => AgentExchangeResult::accepted(
            AgentExchangePayload::encode(AGENT_TASK_CREATION_OUTCOME_PAYLOAD_TYPE, &outcome)
                .unwrap_or_else(|_| {
                    AgentExchangePayload::empty(AGENT_TASK_CREATION_OUTCOME_PAYLOAD_TYPE)
                }),
        ),
        Err(error) => refuse(state, error.code(), error.to_string()),
    }
}

/// Settles the run's answer to an assignment.
///
/// An acceptance moves the task to `InProgress`. A refusal retires the
/// generation and leaves the task assignable, so the next decision creates a new
/// run rather than reusing a run that refused.
fn settle_assignment(
    state: &mut AgentTaskState,
    envelope: &AgentExchangeEnvelope,
    result: &AgentExchangeResult,
    now: AgentTimestampMillis,
) {
    let operation_id = envelope.operation_id().clone();
    let Some(task) = state.task.as_mut() else {
        return;
    };
    let Some(assignment) = task.assignment.as_mut() else {
        return;
    };
    if assignment.operation_id != operation_id {
        // The reply belongs to a generation this task has already moved past.
        return;
    }

    if result.is_accepted() {
        assignment.status = AgentAssignmentStatus::Accepted;
        let assignment = assignment.clone();
        task.status = AgentTaskStatus::InProgress;
        state.updated_at = now;
        state.record_history(|sequence| {
            AgentTaskHistoryEntry::new(
                sequence,
                AgentTaskHistoryKind::AssignmentAccepted,
                operation_id.clone(),
                AgentTaskStatus::InProgress,
                now,
            )
            .with_assignment(&assignment)
        });
        return;
    }

    let code = result
        .status()
        .rejection_code()
        .unwrap_or("run-refused-assignment")
        .to_string();
    let assignment = assignment.clone();
    task.assignment = None;
    // A dependency may have been declared while the assignment was
    // outstanding, so the retired task is only assignable again if the
    // dependency summary still permits it.
    task.status = if task.dependencies_satisfied() {
        AgentTaskStatus::Created
    } else {
        AgentTaskStatus::Blocked
    };
    let status = task.status;
    task.last_refusal = Some(AgentAssignmentRefusal {
        agent: assignment.agent.clone(),
        reason: AgentAssignmentRefusalReason::RunRefusedAssignment,
        detail: bounded_detail(code.clone()),
        refused_at: now,
    });
    state.updated_at = now;
    state.record_history(|sequence| {
        AgentTaskHistoryEntry::new(
            sequence,
            AgentTaskHistoryKind::AssignmentReleased,
            operation_id.clone(),
            status,
            now,
        )
        .with_assignment(&assignment)
        .with_detail(code)
    });
}

/// The durable facade over one typed-task entity.
///
/// It owns the three things a bounded transition cannot do for itself: the
/// durable read of the agent's admission state that an assignment decision needs,
/// the flush of the history the task owes its sink, and the courier that drives
/// the exchanges it owes. Every decision is still a pure transition, and the
/// actor is a thin shell over this type.
pub struct AgentTaskEntityStore<Store, Agents, History>
where
    Store: DurableStateStore<AgentTaskState>,
    Agents: DurableStateStore<AgentEntityState>,
    History: AgentTaskHistoryStore,
{
    scope: AgentTaskScope,
    host: AgentExchangeHost<AgentTaskParticipant, Store>,
    agents: Agents,
    history: History,
    policy: AgentSchemaPolicy,
    recovered: bool,
}

impl<Store, Agents, History> Debug for AgentTaskEntityStore<Store, Agents, History>
where
    Store: DurableStateStore<AgentTaskState>,
    Agents: DurableStateStore<AgentEntityState>,
    History: AgentTaskHistoryStore,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentTaskEntityStore")
            .field("scope", &self.scope)
            .field("history", &self.history.backend_name())
            .field("recovered", &self.recovered)
            .finish_non_exhaustive()
    }
}

impl<Store, Agents, History> AgentTaskEntityStore<Store, Agents, History>
where
    Store: DurableStateStore<AgentTaskState>,
    Agents: DurableStateStore<AgentEntityState>,
    History: AgentTaskHistoryStore,
{
    /// Creates a durable facade for one task scope.
    #[must_use]
    pub fn new(scope: AgentTaskScope, store: Store, agents: Agents, history: History) -> Self {
        let host = AgentExchangeHost::new(
            AgentEntityAddress::Task(scope.clone()),
            AgentTaskParticipant,
            store,
        );
        Self {
            scope,
            host,
            agents,
            history,
            policy: AgentSchemaPolicy::default(),
            recovered: false,
        }
    }

    /// Uses an explicit schema-compatibility policy.
    #[must_use]
    pub fn with_schema_policy(mut self, policy: AgentSchemaPolicy) -> Self {
        self.policy = policy;
        self.host = self.host.with_schema_policy(policy);
        self
    }

    /// The scope this facade addresses.
    #[must_use]
    pub const fn scope(&self) -> &AgentTaskScope {
        &self.scope
    }

    /// The durable persistence id of this task's state.
    #[must_use]
    pub fn persistence_id(&self) -> PersistenceId {
        self.scope.persistence_id()
    }

    /// Loads the task's durable state, failing closed on an unsupported schema
    /// version.
    pub async fn recover(&mut self, now: AgentTimestampMillis) -> AgentTaskResult<&AgentTaskState> {
        let state = self.host.recover(now).await?;
        self.recovered = true;
        Ok(state)
    }

    /// The currently recovered state.
    pub fn state(&self) -> AgentTaskResult<&AgentTaskState> {
        Ok(self.host.state()?)
    }

    /// The bounded projection of the task, once it has been created.
    pub fn snapshot(&self) -> AgentTaskResult<Option<AgentTaskSnapshot>> {
        Ok(self.state()?.snapshot())
    }

    /// Applies one command, then settles whatever it made possible: the
    /// assignment decision, the history the transition owes, and the exchanges it
    /// owes.
    ///
    /// # Errors
    ///
    /// An error does not prove the command was not applied. The transition
    /// commits before the settle pass, so a settlement failure — a history-sink
    /// outage, a delivery fault — surfaces here after the command has durably
    /// applied. Retrying with the same operation id is always safe: a command
    /// that committed answers [`AgentTaskEntityReply::Duplicate`] with its
    /// original outcome rather than transitioning twice.
    pub async fn apply(
        &mut self,
        command: AgentTaskEntityCommand,
        router: &AgentExchangeRouter,
        now: AgentTimestampMillis,
    ) -> AgentTaskResult<AgentTaskEntityReply> {
        self.ensure_recovered(now).await?;

        if let Some(operation_id) = command.operation_id() {
            if let Some(outcome) = self
                .state()?
                .applied_operations
                .outcome(operation_id)
                .cloned()
            {
                // The command already ran. Its original outcome is returned, and
                // no second transition happens — which is what makes a replayed
                // creation, dependency, or cancellation converge on one task, one
                // edge, and one decision.
                return Ok(AgentTaskEntityReply::Duplicate { outcome });
            }
        }

        if matches!(command, AgentTaskEntityCommand::Describe) {
            return Ok(AgentTaskEntityReply::Snapshot(
                self.snapshot()?.map(Box::new),
            ));
        }
        self.require_history_headroom(now).await?;

        // The agent's durable admission state is read *before* the transition, so
        // a command that makes the task eligible can create it, decide its
        // assignment, and record the run-creation command it owes in one
        // compare-and-set. Reading it is I/O, and a transition may not do I/O
        // ([specification 9.5](../../../docs/plans/rakka-agent/spec.md)); deciding
        // on what was read is pure.
        let readiness = self.resolve_readiness(&command, now).await?;

        let reply = match command {
            AgentTaskEntityCommand::Describe => unreachable!("handled above"),
            AgentTaskEntityCommand::Create {
                operation_id,
                creation,
            } => {
                self.transition(now, readiness, move |state| {
                    create_task(state, &operation_id, *creation, now)?;
                    Ok((operation_id, None))
                })
                .await?
            }
            AgentTaskEntityCommand::DeclareDependency {
                operation_id,
                declaration,
            } => {
                self.transition(now, readiness, move |state| {
                    declare_dependency(state, &operation_id, &declaration, now)?;
                    Ok((operation_id, None))
                })
                .await?
            }
            AgentTaskEntityCommand::RecordDependencyOutcome {
                operation_id,
                dependency,
                outcome,
            } => {
                self.transition(now, readiness, move |state| {
                    record_dependency_outcome(state, &operation_id, &dependency, outcome, now)?;
                    Ok((operation_id, None))
                })
                .await?
            }
            AgentTaskEntityCommand::Cancel {
                operation_id,
                reason,
            } => {
                self.transition(now, None, move |state| {
                    terminate(
                        state,
                        &operation_id,
                        AgentTaskTerminalReason::CancellationRequested {
                            reason: bounded_detail(reason),
                        },
                        now,
                    )?;
                    Ok((operation_id, None))
                })
                .await?
            }
            AgentTaskEntityCommand::AdmitWake {
                operation_id,
                binding,
            } => {
                self.transition(now, None, move |state| {
                    let wake = admit_wake(state, &operation_id, *binding, now)?;
                    Ok((operation_id, Some(wake)))
                })
                .await?
            }
            AgentTaskEntityCommand::CompleteWakeOccurrence { operation_id, wake } => {
                self.transition(now, None, move |state| {
                    let outcome = complete_wake_occurrence(state, &wake, now)?;
                    Ok((operation_id, Some(outcome)))
                })
                .await?
            }
            AgentTaskEntityCommand::UpdateContinuousSchedule {
                operation_id,
                schedule_revision,
                wake_policy,
            } => {
                self.transition(now, None, move |state| {
                    let outcome = update_continuous_schedule(
                        state,
                        schedule_revision,
                        wake_policy.map(|policy| *policy),
                        now,
                    )?;
                    Ok((operation_id, Some(outcome)))
                })
                .await?
            }
        };

        self.settle_side_effects(router, now).await?;
        Ok(reply)
    }

    /// Reads the agent facts an assignment decision would need, when this command
    /// could make the task assignable.
    ///
    /// A task that is human-owned, already assigned, or terminal can never be
    /// assigned by this command, so its agent is not read at all.
    async fn resolve_readiness(
        &self,
        command: &AgentTaskEntityCommand,
        now: AgentTimestampMillis,
    ) -> AgentTaskResult<Option<AgentAssignmentReadiness>> {
        let (assignee, definition) = match command {
            AgentTaskEntityCommand::Create { creation, .. } => (
                creation.assignee.clone(),
                creation
                    .definition
                    .is_agent_owned()
                    .then(|| creation.definition.clone()),
            ),
            // Wake transitions never change assignability, so they read no
            // agent state at all: a scanner delivering to a passivated
            // controller costs one entity transition, not an extra durable
            // read.
            AgentTaskEntityCommand::Describe
            | AgentTaskEntityCommand::Cancel { .. }
            | AgentTaskEntityCommand::AdmitWake { .. }
            | AgentTaskEntityCommand::CompleteWakeOccurrence { .. }
            | AgentTaskEntityCommand::UpdateContinuousSchedule { .. } => return Ok(None),
            _ => {
                let Some(task) = self.state()?.task() else {
                    return Ok(None);
                };
                if task.assignment.is_some() || task.status.is_terminal() {
                    return Ok(None);
                }
                (
                    task.assignee.clone(),
                    task.definition
                        .is_agent_owned()
                        .then(|| task.definition.clone()),
                )
            }
        };

        let (Some(assignee), Some(definition)) = (assignee, definition) else {
            return Ok(None);
        };
        self.read_readiness(&assignee, &definition, now)
            .await
            .map(Some)
    }

    /// The bounded durable read of [specification 9.8](../../../docs/plans/rakka-agent/spec.md):
    /// the assignment decision reads the agent's definition and admission state,
    /// and never round-trips through the agent entity's mailbox.
    async fn read_readiness(
        &self,
        assignee: &AgentId,
        definition: &AgentTaskDefinition,
        now: AgentTimestampMillis,
    ) -> AgentTaskResult<AgentAssignmentReadiness> {
        let agent_scope = AgentScope::new(self.scope.tenant().clone(), assignee.clone())?;
        let agent_state = load_agent_entity_state(&self.agents, &agent_scope, &self.policy)
            .await
            .map_err(|error| AgentTaskError::AgentRead {
                agent: assignee.clone(),
                message: error.to_string(),
            })?;

        Ok(agent_state.as_ref().map_or_else(
            || AgentAssignmentReadiness::not_instantiated(assignee.clone()),
            |state| AgentAssignmentReadiness::from_agent_state(state, definition, now),
        ))
    }

    /// Accepts one delivered exchange, then settles what it made possible.
    pub async fn accept(
        &mut self,
        envelope: &AgentExchangeEnvelope,
        router: &AgentExchangeRouter,
        now: AgentTimestampMillis,
    ) -> AgentTaskResult<AgentExchangeReply> {
        self.ensure_recovered(now).await?;
        // A transition the task cannot record the history of must not happen at
        // all — and it must not happen *as a rejection* either. Whatever a
        // participant returns for an exchange becomes the task's durable decision
        // and is replayed forever, so a transient sink outage answered with
        // "rejected" would refuse that proposal for good. Failing before the
        // transition leaves the exchange outstanding on its initiator instead,
        // which re-drives it once the sink is back.
        self.require_history_headroom(now).await?;
        let reply = self.host.accept(envelope, now).await?;
        // Accepting a delivered exchange makes *local* progress only: it may
        // decide an assignment freed escrow now permits and flush the history it
        // owes, both of which touch only the task's own state and its history
        // sink. It does **not** deliver the cross-entity exchanges that decision
        // committed (the assignment to the run). Those are drained by the
        // courier — a command's settle pass, a recovery sweep, `pump` — never
        // synchronously from inside a delivery.
        //
        // The initiator of `envelope` is mid-delivery to this task right now, so
        // driving an owed exchange back to it here would re-enter its `accept`
        // before this reply settles. A run that owes this task a settlement, and
        // a task that re-drives that run's still-outstanding assignment, would
        // otherwise recurse without bound (see [`crate::run`]'s `accept`).
        let _ = router;
        self.make_local_progress(now).await?;
        Ok(reply)
    }

    /// Decides an assignment the task now permits and flushes owed history,
    /// without delivering any owed cross-entity exchange.
    ///
    /// This is the half of [`Self::settle_side_effects`] that touches only the
    /// task's own state and its history sink. It is what a delivered exchange is
    /// allowed to trigger; see [`Self::accept`] for why the drive half is not.
    async fn make_local_progress(&mut self, now: AgentTimestampMillis) -> AgentTaskResult<()> {
        self.require_history_headroom(now).await?;
        self.decide_assignment(now).await?;
        self.flush_history(now).await?;
        Ok(())
    }

    /// Flushes whatever history the task owes, and fails closed if the outbox
    /// still cannot hold what the next transition may record.
    async fn require_history_headroom(&mut self, now: AgentTimestampMillis) -> AgentTaskResult<()> {
        if self.state()?.history_headroom() >= AGENT_TASK_MAX_HISTORY_PER_TRANSITION {
            return Ok(());
        }

        // The outbox is backed up, so try to drain it before refusing.
        self.flush_history(now).await?;

        let state = self.state()?;
        if state.history_headroom() >= AGENT_TASK_MAX_HISTORY_PER_TRANSITION {
            return Ok(());
        }
        Err(AgentTaskError::HistoryBacklog {
            pending: state.pending_history().len(),
            maximum: AGENT_TASK_PENDING_HISTORY_CAPACITY,
        })
    }

    /// Decides the assignment when the task awaits one, flushes the history the
    /// task owes, and drives the exchanges it owes.
    ///
    /// It is safe to call at any time and from any node: every step reads what it
    /// needs from durable state, so calling it after a transition, after recovery,
    /// or on a timer are the same operation.
    pub async fn settle_side_effects(
        &mut self,
        router: &AgentExchangeRouter,
        now: AgentTimestampMillis,
    ) -> AgentTaskResult<AgentTaskProgress> {
        self.ensure_recovered(now).await?;
        // The settle pass commits transitions of its own — an assignment
        // decision here, a settlement inside the drive — so it stands behind
        // the same headroom fence as every command and exchange: a backlog is
        // refused (after an attempt to drain it) rather than pushed past the
        // pending-history bound.
        self.require_history_headroom(now).await?;
        let assigned = self.decide_assignment(now).await?;
        let flushed = self.flush_history(now).await?;
        let report = drive_pending_exchanges(&mut self.host, router, now).await?;
        Ok(AgentTaskProgress {
            assigned,
            history_flushed: flushed,
            settled: report.settled,
            failed: report.failed,
            outstanding: self.host.outstanding()?.len(),
        })
    }

    /// Reads the agent's durable admission state and, if the task awaits one,
    /// decides its assignment.
    ///
    /// This is the recovery path: a task whose earlier decision was refused —
    /// because its agent was suspended, or did not exist yet — is decided here
    /// when that changes, with no new command and no lost work.
    async fn decide_assignment(&mut self, now: AgentTimestampMillis) -> AgentTaskResult<bool> {
        let Some((assignee, definition)) = self.pending_assignment()? else {
            return Ok(false);
        };
        let readiness = self.read_readiness(&assignee, &definition, now).await?;
        if self.state()?.task().is_some_and(|task| {
            pending_assignment_refusal(task, &readiness).is_some_and(|(reason, detail)| {
                assignment_refusal_is_current(
                    task,
                    &readiness.agent,
                    reason,
                    &bounded_detail(detail),
                )
            })
        }) {
            // Deciding would record nothing new, so the pass skips the write:
            // a settle sweep over a still-refused task must not burn a
            // revision per pass.
            return Ok(false);
        }

        let mut assigned = false;
        let mut rejection = None;
        let committed = self
            .host
            .initiate(now, |state| {
                match decide_assignment(state, &readiness, now) {
                    Ok(envelope) => {
                        assigned = envelope.is_some();
                        Ok(envelope.into_iter().collect())
                    }
                    Err(error) => {
                        let carried = AgentChoreographyError::from(error.clone());
                        rejection = Some(error);
                        Err(carried)
                    }
                }
            })
            .await;

        if let Some(rejection) = rejection {
            return Err(rejection);
        }
        committed?;
        Ok(assigned)
    }

    /// Appends the history the task owes its sink, then drops the entries the
    /// sink durably accepted.
    ///
    /// A crash between the append and the clearing re-drives the append, which is
    /// idempotent on the entry's sequence; a crash before the append leaves the
    /// entry owed in durable state. Neither loses an entry, and neither writes one
    /// twice.
    async fn flush_history(&mut self, now: AgentTimestampMillis) -> AgentTaskResult<usize> {
        let pending = self.state()?.pending_history().to_vec();
        if pending.is_empty() {
            return Ok(0);
        }

        let mut flushed = Vec::with_capacity(pending.len());
        for entry in &pending {
            self.history.append(&self.scope, entry).await?;
            flushed.push(entry.sequence);
        }

        self.host
            .initiate(now, |state| {
                state.clear_flushed_history(&flushed);
                Ok(Vec::new())
            })
            .await?;
        Ok(flushed.len())
    }

    fn pending_assignment(&self) -> AgentTaskResult<Option<(AgentId, AgentTaskDefinition)>> {
        let Some(task) = self.state()?.task() else {
            return Ok(None);
        };
        if !task.awaits_assignment() {
            return Ok(None);
        }
        Ok(task
            .assignee
            .clone()
            .map(|assignee| (assignee, task.definition.clone())))
    }

    /// Runs one bounded command transition and records its resolved operation id
    /// in the same compare-and-set.
    ///
    /// A rejected transition never reaches the store, so it leaves no trace in
    /// the operation log and a corrected retry under the same operation id is
    /// still accepted. The domain rejection is carried out of the substrate's
    /// closure unchanged, so a caller sees the task's own stable code rather than
    /// a choreography error that happened to transport it.
    async fn transition<F>(
        &mut self,
        now: AgentTimestampMillis,
        readiness: Option<AgentAssignmentReadiness>,
        transition: F,
    ) -> AgentTaskResult<AgentTaskEntityReply>
    where
        F: FnOnce(
            &mut AgentTaskState,
        ) -> AgentTaskResult<(AgentOperationId, Option<AgentWakeOutcome>)>,
    {
        let mut outcome = None;
        let mut rejection = None;
        let committed = self
            .host
            .initiate(now, |state| {
                let assign =
                    |state: &mut AgentTaskState| -> AgentTaskResult<Vec<AgentExchangeEnvelope>> {
                        let (operation_id, wake) = transition(state)?;
                        // The command's own transition may have made the task
                        // eligible. Deciding here means the assignment, the run-creation
                        // command it owes, and the transition that caused it all commit
                        // together: the task can never be durably assigned and have
                        // forgotten to tell the run.
                        let owed = match &readiness {
                            Some(readiness) => decide_assignment(state, readiness, now)?
                                .into_iter()
                                .collect(),
                            None => Vec::new(),
                        };
                        let mut result = state.outcome();
                        result.wake = wake;
                        state
                            .applied_operations
                            .record(operation_id, result.clone());
                        state.updated_at = now;
                        outcome = Some(result);
                        Ok(owed)
                    };

                match assign(state) {
                    Ok(owed) => Ok(owed),
                    Err(error) => {
                        let carried = AgentChoreographyError::from(error.clone());
                        rejection = Some(error);
                        Err(carried)
                    }
                }
            })
            .await;

        if let Some(rejection) = rejection {
            return Err(rejection);
        }
        committed?;
        Ok(AgentTaskEntityReply::Applied {
            outcome: outcome.expect("an accepted transition produces an outcome"),
        })
    }

    async fn ensure_recovered(&mut self, now: AgentTimestampMillis) -> AgentTaskResult<()> {
        // Recovery is lazy and idempotent: the first message after activation
        // loads the authoritative state, which is exactly what an entity
        // re-materialized on a new shard owner must do before it transitions.
        if !self.recovered || self.host.state().is_err() {
            self.recover(now).await?;
        }
        Ok(())
    }
}

/// What one pass of the task entity's settlement did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTaskProgress {
    /// Whether the pass decided an assignment.
    pub assigned: bool,
    /// How many history entries it appended to the sink.
    pub history_flushed: usize,
    /// How many exchanges it settled.
    pub settled: usize,
    /// How many delivery attempts failed, leaving their exchange outstanding.
    pub failed: usize,
    /// How many exchanges the task still owes.
    pub outstanding: usize,
}

/// The serializable command protocol of the typed-task entity.
///
/// Large payloads are boxed so the enum stays small enough to move cheaply
/// through mailboxes and remote envelopes, and nothing in it is an `Arc` or an
/// in-process reply channel: the protocol is serializable from this first commit,
/// so no later slice has to retrofit remoting into an entity whose commands
/// cannot cross a node boundary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentTaskEntityCommand {
    /// Create the task. The operation id comes from the durable, deduplicated
    /// ingress that accepted the work
    /// ([specification 9.8](../../../docs/plans/rakka-agent/spec.md)); a
    /// delegating run creates a child task through the equivalent
    /// [`AgentExchangeKind::Creation`] exchange instead.
    Create {
        /// The stable operation id this command deduplicates on.
        operation_id: AgentOperationId,
        /// What to create.
        creation: Box<AgentTaskCreation>,
    },
    /// Declare one dependency edge after creation.
    DeclareDependency {
        /// The stable operation id this command deduplicates on.
        operation_id: AgentOperationId,
        /// The edge to declare.
        declaration: Box<AgentTaskDependencyDeclaration>,
    },
    /// Record how one dependency resolved, applying its failure policy.
    RecordDependencyOutcome {
        /// The stable operation id this command deduplicates on.
        operation_id: AgentOperationId,
        /// The dependency that resolved.
        dependency: AgentTaskId,
        /// How it resolved.
        outcome: AgentTaskDependencyOutcome,
    },
    /// Cancel the task.
    Cancel {
        /// The stable operation id this command deduplicates on.
        operation_id: AgentOperationId,
        /// A bounded, stable reason.
        reason: String,
    },
    /// Deliver one wake occurrence to the continuous goal's controller.
    ///
    /// Every trigger path — the shared scanner, an external event, an
    /// authenticated A2A command, a callback — injects this same command with
    /// the operation id the binding itself derives, so duplicate delivery is
    /// deduplicated by construction
    /// ([specification 8.2](../../../docs/plans/rakka-agent/spec.md)).
    AdmitWake {
        /// The stable operation id this command deduplicates on. It must be
        /// the binding's own derived admission operation id.
        operation_id: AgentOperationId,
        /// The wake to disposition.
        binding: Box<AgentWakeBinding>,
    },
    /// Release the active occurrence a completed execution owned.
    CompleteWakeOccurrence {
        /// The stable operation id this command deduplicates on.
        operation_id: AgentOperationId,
        /// The active wake to release.
        wake: AgentWakeId,
    },
    /// Take a schedule update into force, fencing parked occurrences the old
    /// schedule created.
    UpdateContinuousSchedule {
        /// The stable operation id this command deduplicates on.
        operation_id: AgentOperationId,
        /// The strictly newer schedule revision.
        schedule_revision: ScheduleRevision,
        /// A strictly newer wake-policy revision riding the same update, when
        /// the policy changes too.
        wake_policy: Option<Box<AgentWakePolicyRevision>>,
    },
    /// Read the task's bounded durable projection.
    Describe,
}

impl AgentTaskEntityCommand {
    /// The operation id this command deduplicates on, when it mutates state.
    #[must_use]
    pub const fn operation_id(&self) -> Option<&AgentOperationId> {
        match self {
            Self::Create { operation_id, .. }
            | Self::DeclareDependency { operation_id, .. }
            | Self::RecordDependencyOutcome { operation_id, .. }
            | Self::Cancel { operation_id, .. }
            | Self::AdmitWake { operation_id, .. }
            | Self::CompleteWakeOccurrence { operation_id, .. }
            | Self::UpdateContinuousSchedule { operation_id, .. } => Some(operation_id),
            Self::Describe => None,
        }
    }
}

/// The serializable reply protocol of the typed-task entity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentTaskEntityReply {
    /// The command transitioned the task.
    Applied {
        /// The outcome of the transition.
        outcome: AgentTaskOutcome,
    },
    /// The operation id was already applied; this is the original outcome, and
    /// no second transition happened.
    Duplicate {
        /// The outcome the original application produced.
        outcome: AgentTaskOutcome,
    },
    /// The task's bounded durable projection, absent if it was never created.
    Snapshot(Option<Box<AgentTaskSnapshot>>),
    /// What one settlement pass did.
    Progressed {
        /// The pass's report.
        progress: AgentTaskProgress,
    },
    /// The command was rejected.
    ///
    /// A rejection is not proof the command did not apply: a settlement
    /// failure after the transition committed — a history-sink outage, a
    /// delivery fault — reaches the caller as this reply too. Retrying with
    /// the same operation id is always safe; a command that committed answers
    /// [`Self::Duplicate`] with its original outcome.
    Rejected {
        /// Stable machine-readable error code.
        code: String,
        /// Human-readable detail.
        message: String,
    },
}

impl AgentTaskEntityReply {
    fn rejected(error: &AgentTaskError) -> Self {
        Self::Rejected {
            code: error.code().to_string(),
            message: error.to_string(),
        }
    }
}

/// The process-local message the typed-task entity accepts.
///
/// The reply channels never cross a node boundary:
/// [`init_agent_task_entity_remote_sharding`] reconstructs the exchange arm on
/// the owning node from the [`AgentExchangeEnvelope`] that arrived over
/// `rakka-remote`, which is the surface every cross-entity command travels.
pub enum AgentTaskEntityMessage {
    /// An ingress or administrative command.
    Command {
        /// The command to apply.
        command: Box<AgentTaskEntityCommand>,
        /// Where the reply goes.
        reply_to: ReplyTo<AgentTaskEntityReply>,
    },
    /// A cross-entity exchange: a delegating run's creation, or a run's result
    /// proposal.
    Exchange {
        /// The exchange to apply.
        envelope: Box<AgentExchangeEnvelope>,
        /// Where the reply goes.
        reply_to: ReplyTo<AgentExchangeReply>,
    },
    /// Decide a pending assignment, flush owed history, and drive owed
    /// exchanges.
    ///
    /// The entity does this itself after every transition. The command exists so
    /// that a recovery sweep or a test can drive a task that was lost between
    /// persisting what it owed and delivering it.
    Settle {
        /// Where the reply goes.
        reply_to: ReplyTo<AgentTaskEntityReply>,
    },
}

impl Debug for AgentTaskEntityMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Command { command, .. } => f
                .debug_struct("AgentTaskEntityMessage::Command")
                .field("command", command)
                .finish_non_exhaustive(),
            Self::Exchange { envelope, .. } => f
                .debug_struct("AgentTaskEntityMessage::Exchange")
                .field("envelope", envelope)
                .finish_non_exhaustive(),
            Self::Settle { .. } => f
                .debug_struct("AgentTaskEntityMessage::Settle")
                .finish_non_exhaustive(),
        }
    }
}

/// The actor-backed host of one sharded typed-task entity.
///
/// The actor is a routing and recovery shell: every decision lives in
/// [`AgentTaskEntityStore`] and every durable fact lives in the state store, so
/// the entity can passivate after any message and recover on another pod
/// ([specification 15](../../../docs/plans/rakka-agent/spec.md)).
///
/// An entity id that does not parse into an [`AgentTaskScope`] cannot address a
/// durable record, so such an entity rejects every message instead of guessing a
/// scope.
pub struct AgentTaskEntity<Store, Agents, History>
where
    Store: DurableStateStore<AgentTaskState>,
    Agents: DurableStateStore<AgentEntityState>,
    History: AgentTaskHistoryStore,
{
    entity: Result<AgentTaskEntityStore<Store, Agents, History>, AgentIdentityError>,
    router: AgentExchangeRouter,
    clock: AgentTaskClock,
}

impl<Store, Agents, History> AgentTaskEntity<Store, Agents, History>
where
    Store: DurableStateStore<AgentTaskState>,
    Agents: DurableStateStore<AgentEntityState>,
    History: AgentTaskHistoryStore,
{
    /// Creates an entity for one sharded entity id.
    #[must_use]
    pub fn new(
        entity_id: &EntityId,
        store: Store,
        agents: Agents,
        history: History,
        router: AgentExchangeRouter,
        clock: AgentTaskClock,
        policy: AgentSchemaPolicy,
    ) -> Self {
        let entity = AgentTaskScope::from_entity_id(entity_id).map(|scope| {
            AgentTaskEntityStore::new(scope, store, agents, history).with_schema_policy(policy)
        });
        Self {
            entity,
            router,
            clock,
        }
    }

    fn store(
        &mut self,
    ) -> Result<&mut AgentTaskEntityStore<Store, Agents, History>, AgentTaskError> {
        self.entity
            .as_mut()
            .map_err(|error| AgentTaskError::Identity(error.clone()))
    }
}

impl<Store, Agents, History> Actor for AgentTaskEntity<Store, Agents, History>
where
    Store: DurableStateStore<AgentTaskState>,
    Agents: DurableStateStore<AgentEntityState>,
    History: AgentTaskHistoryStore,
{
    type Msg = AgentTaskEntityMessage;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        actor_future(async move {
            // A transition is stamped where it commits, on the owner that wrote
            // it.
            let now = (self.clock)();
            let router = self.router.clone();

            match msg {
                AgentTaskEntityMessage::Command { command, reply_to } => {
                    let reply = match self.store() {
                        Err(error) => AgentTaskEntityReply::rejected(&error),
                        Ok(entity) => match entity.apply(*command, &router, now).await {
                            Ok(reply) => reply,
                            Err(error) => AgentTaskEntityReply::rejected(&error),
                        },
                    };
                    let _reply_dropped = reply_to.reply(reply);
                }
                AgentTaskEntityMessage::Exchange { envelope, reply_to } => {
                    let Ok(entity) = self.store() else {
                        // A misrouted entity cannot answer an exchange. Dropping
                        // the reply leaves the exchange outstanding on its
                        // initiator, which re-drives it — which is exactly what a
                        // lost delivery does, and what the substrate is built to
                        // converge from.
                        return Ok(ActorAction::Continue);
                    };
                    if let Ok(reply) = entity.accept(&envelope, &router, now).await {
                        let _reply_dropped = reply_to.reply(reply);
                    }
                }
                AgentTaskEntityMessage::Settle { reply_to } => {
                    let reply = match self.store() {
                        Err(error) => AgentTaskEntityReply::rejected(&error),
                        Ok(entity) => match entity.settle_side_effects(&router, now).await {
                            Ok(progress) => AgentTaskEntityReply::Progressed { progress },
                            Err(error) => AgentTaskEntityReply::rejected(&error),
                        },
                    };
                    let _reply_dropped = reply_to.reply(reply);
                }
            }
            Ok(ActorAction::Continue)
        })
    }
}

/// The entity type key of the typed-task entity.
pub type AgentTaskEntityTypeKey = EntityTypeKey<AgentTaskEntityMessage>;

/// The registration returned after initializing sharded task entities.
pub type AgentTaskEntityRegistration = EntityTypeRegistration<AgentTaskEntityMessage>;

/// A sharded reference to one typed-task entity.
pub type AgentTaskEntityRef = ShardedEntityRef<AgentTaskEntityMessage>;

/// The sharding settings of typed-task entities.
#[derive(Clone)]
pub struct AgentTaskEntityShardingSettings {
    key: AgentTaskEntityTypeKey,
    actor_options: ActorOptions,
    idle_passivation_timeout: Option<Duration>,
    buffer_config: Option<ShardBufferConfig>,
    passivation_buffer_duration: Duration,
    schema_policy: AgentSchemaPolicy,
    clock: AgentTaskClock,
}

impl Debug for AgentTaskEntityShardingSettings {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentTaskEntityShardingSettings")
            .field("entity_type", self.key.entity_type())
            .field("number_of_shards", &self.key.config().number_of_shards())
            .field("idle_passivation_timeout", &self.idle_passivation_timeout)
            .field("schema_policy", &self.schema_policy)
            .finish_non_exhaustive()
    }
}

impl AgentTaskEntityShardingSettings {
    /// Creates settings from an explicit entity type key.
    #[must_use]
    pub fn new(key: AgentTaskEntityTypeKey) -> Self {
        Self {
            key,
            actor_options: ActorOptions::default(),
            idle_passivation_timeout: None,
            buffer_config: Some(ShardBufferConfig::default()),
            passivation_buffer_duration: DEFAULT_AGENT_TASK_PASSIVATION_BUFFER_DURATION,
            schema_policy: AgentSchemaPolicy::default(),
            clock: system_task_clock(),
        }
    }

    /// Uses an explicit clock for the timestamps hosted entities persist.
    #[must_use]
    pub fn with_clock(mut self, clock: AgentTaskClock) -> Self {
        self.clock = clock;
        self
    }

    /// The entity type key used for task entities.
    #[must_use]
    pub const fn key(&self) -> &AgentTaskEntityTypeKey {
        &self.key
    }

    /// Sets the options used when each task entity actor is spawned.
    #[must_use]
    pub fn with_actor_options(mut self, actor_options: ActorOptions) -> Self {
        self.actor_options = actor_options;
        self
    }

    /// Enables idle passivation for quiescent task entities.
    #[must_use]
    pub const fn with_idle_passivation(mut self, timeout: Duration) -> Self {
        self.idle_passivation_timeout = Some(timeout);
        self
    }

    /// Disables idle passivation.
    #[must_use]
    pub const fn without_idle_passivation(mut self) -> Self {
        self.idle_passivation_timeout = None;
        self
    }

    /// Configures bounded buffering during shard handoff and passivation.
    #[must_use]
    pub fn with_buffering(mut self, config: ShardBufferConfig) -> Self {
        self.buffer_config = Some(config);
        self
    }

    /// Disables shard-level buffering.
    #[must_use]
    pub const fn without_buffering(mut self) -> Self {
        self.buffer_config = None;
        self
    }

    /// Sets how long explicit passivation buffers incoming messages.
    #[must_use]
    pub const fn with_passivation_buffer_duration(mut self, duration: Duration) -> Self {
        self.passivation_buffer_duration = duration;
        self
    }

    /// Uses an explicit schema-compatibility policy for hosted entities.
    #[must_use]
    pub const fn with_schema_policy(mut self, policy: AgentSchemaPolicy) -> Self {
        self.schema_policy = policy;
        self
    }
}

impl Default for AgentTaskEntityShardingSettings {
    fn default() -> Self {
        Self::new(agent_task_entity_type_key())
    }
}

/// Creates the default sharded entity type key for task entities.
#[must_use]
pub fn agent_task_entity_type_key() -> AgentTaskEntityTypeKey {
    EntityTypeKey::new(DEFAULT_AGENT_TASK_ENTITY_TYPE)
}

/// Maps a task scope to its sharded entity id.
#[must_use]
pub fn agent_task_entity_id(scope: &AgentTaskScope) -> EntityId {
    scope.entity_id()
}

/// The durable persistence id of one task entity's state.
#[must_use]
pub fn agent_task_entity_persistence_id(scope: &AgentTaskScope) -> PersistenceId {
    scope.persistence_id()
}

/// Initializes node-local sharded typed-task entities.
pub fn init_agent_task_entity_sharding<Store, Agents, History>(
    sharding: &ClusterSharding,
    store: Store,
    agents: Agents,
    history: History,
    router: AgentExchangeRouter,
    settings: AgentTaskEntityShardingSettings,
) -> ClusterShardingResult<AgentTaskEntityRegistration>
where
    Store: DurableStateStore<AgentTaskState>,
    Agents: DurableStateStore<AgentEntityState>,
    History: AgentTaskHistoryStore,
{
    sharding.init(agent_task_entity(store, agents, history, router, &settings))
}

/// Initializes sharded typed-task entities that a non-owning node can reach over
/// `rakka-remote`.
///
/// The remote ask surface is the [`AgentExchangeEnvelope`], because that is what
/// every cross-entity command is: a run's result proposal and a delegating run's
/// task creation both arrive as exchanges, and
/// [`crate::choreography::ShardedExchangeRoute`] delivers them to the owning node
/// unchanged. The application registers the exchange codecs with the node
/// runtime's serialization registry through
/// [`crate::choreography::register_agent_exchange_codecs`].
pub fn init_agent_task_entity_remote_sharding<Store, Agents, History>(
    sharding: &ClusterSharding,
    runtime: &mut ClusterNodeRuntime,
    store: Store,
    agents: Agents,
    history: History,
    router: AgentExchangeRouter,
    settings: AgentTaskEntityShardingSettings,
) -> ClusterNodeRuntimeResult<AgentTaskEntityRegistration>
where
    Store: DurableStateStore<AgentTaskState>,
    Agents: DurableStateStore<AgentEntityState>,
    History: AgentTaskHistoryStore,
{
    let entity = agent_task_entity(store, agents, history, router, &settings);
    sharding.init_remote_with_ask(
        runtime,
        entity,
        |envelope: AgentExchangeEnvelope, reply_to: ReplyTo<AgentExchangeReply>| {
            AgentTaskEntityMessage::Exchange {
                envelope: Box::new(envelope),
                reply_to,
            }
        },
    )
}

// The task entity is generic over its three stores — its own state, the agent
// state it reads admission facts from, and its history sink — so the entity type
// it builds is unavoidably wide.
#[allow(clippy::type_complexity)]
fn agent_task_entity<Store, Agents, History>(
    store: Store,
    agents: Agents,
    history: History,
    router: AgentExchangeRouter,
    settings: &AgentTaskEntityShardingSettings,
) -> Entity<
    AgentTaskEntityMessage,
    AgentTaskEntity<Store, Agents, History>,
    impl Fn(EntityContext<AgentTaskEntityMessage>) -> AgentTaskEntity<Store, Agents, History>
        + Send
        + Sync
        + 'static,
>
where
    Store: DurableStateStore<AgentTaskState>,
    Agents: DurableStateStore<AgentEntityState>,
    History: AgentTaskHistoryStore,
{
    let schema_policy = settings.schema_policy;
    let clock = settings.clock.clone();
    let mut entity = Entity::of(settings.key.clone(), move |context: EntityContext<_>| {
        AgentTaskEntity::new(
            context.entity_id(),
            store.clone(),
            agents.clone(),
            history.clone(),
            router.clone(),
            clock.clone(),
            schema_policy,
        )
    })
    .with_actor_options(settings.actor_options.clone())
    .with_passivation_buffer_duration(settings.passivation_buffer_duration);

    if let Some(timeout) = settings.idle_passivation_timeout {
        entity = entity.with_idle_passivation(timeout);
    }
    if let Some(buffer_config) = settings.buffer_config.clone() {
        entity = entity.with_buffering(buffer_config);
    } else {
        entity = entity.without_buffering();
    }
    entity
}

/// Returns a sharded reference to one typed-task entity.
pub fn agent_task_entity_ref(
    sharding: &ClusterSharding,
    key: &AgentTaskEntityTypeKey,
    scope: &AgentTaskScope,
) -> ClusterShardingResult<AgentTaskEntityRef> {
    sharding.entity_ref_for(key, scope.key())
}

/// Returns a sharded reference to one typed-task entity from a registration.
#[must_use]
pub fn registered_agent_task_entity_ref(
    registration: &AgentTaskEntityRegistration,
    scope: &AgentTaskScope,
) -> AgentTaskEntityRef {
    registration.entity_ref_for(scope.key())
}

/// Explicitly passivates one local typed-task entity.
pub fn passivate_agent_task_entity(
    sharding: &ClusterSharding,
    key: &AgentTaskEntityTypeKey,
    scope: &AgentTaskScope,
) -> ClusterShardingResult<bool> {
    sharding.passivate_entity_id(key, &scope.entity_id())
}

/// The rejection of a typed-task operation.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AgentTaskError {
    /// An identifier or scope key was malformed.
    Identity(AgentIdentityError),
    /// A persisted record carried an unsupported schema version.
    Schema(AgentSchemaError),
    /// The choreography substrate rejected an exchange.
    Choreography(Box<AgentChoreographyError>),
    /// The task's escrow ledger rejected an allocation, settlement, or return.
    Escrow(AgentEscrowError),
    /// The durable store rejected a load or write.
    Persistence(DurableError),
    /// A task definition could not be bounded.
    InvalidDefinition {
        /// What was out of bounds.
        detail: String,
    },
    /// The task does not exist.
    NotCreated {
        /// The scope of the task.
        scope: AgentTaskScope,
    },
    /// The task already exists.
    AlreadyCreated {
        /// The scope of the task.
        scope: AgentTaskScope,
    },
    /// The task is terminal and accepts no further transition.
    Terminal {
        /// Its terminal status.
        status: AgentTaskStatus,
    },
    /// An agent-owned task was created without an assignee.
    MissingAssignee,
    /// A continuous-mode task was created without a goal binding.
    ContinuousWithoutGoal,
    /// The wake contract itself refused the command.
    Wake(AgentWakeError),
    /// A wake command was delivered to a task that does not coordinate a
    /// continuous goal.
    WakeNotContinuous,
    /// A wake was delivered to a task that does not bind its goal.
    WakeGoalMismatch {
        /// The goal the wake was constructed for.
        offered: AgentGoalId,
    },
    /// A wake command's operation id disagrees with the one its own binding
    /// derives.
    WakeOperationMismatch,
    /// A schedule update whose revision does not move strictly forward.
    ScheduleNotMonotonic {
        /// The revision the update carried.
        offered: ScheduleRevision,
        /// The revision currently in force.
        current: ScheduleRevision,
    },
    /// A wake-policy update whose revision does not move strictly forward.
    WakePolicyNotNewer {
        /// The revision the update carried.
        offered: AgentRevisionNumber,
        /// The revision currently in force.
        current: AgentRevisionNumber,
    },
    /// An agent-owned task's id is too long to derive assignment run ids from.
    TaskIdTooLong {
        /// The task id's length, in bytes.
        length: usize,
        /// The longest id an agent-owned task may use, in bytes.
        maximum: usize,
    },
    /// The task's admission facts were not resolved before a decision.
    NotReady,
    /// A dependency would make the graph cyclic.
    DependencyCycle {
        /// The dependency that would close the cycle.
        dependency: AgentTaskId,
    },
    /// A dependency declaration exceeded the bounded ancestor depth.
    DependencyDepthExceeded {
        /// The depth the declaration carried.
        depth: usize,
        /// The maximum accepted depth.
        maximum: usize,
    },
    /// The task already declares as many dependencies as it may.
    DependencyLimitExceeded {
        /// The maximum number of dependencies.
        maximum: usize,
    },
    /// A dependency was redeclared or resolved with a conflicting value.
    DependencyConflict {
        /// The dependency in question.
        dependency: AgentTaskId,
    },
    /// An outcome was recorded for a dependency the task does not declare.
    UnknownDependency {
        /// The dependency in question.
        dependency: AgentTaskId,
    },
    /// Inline content exceeded the inline bound and belongs behind an artifact
    /// reference.
    ContentTooLarge {
        /// The size of the rejected content, in bytes.
        bytes: usize,
        /// The maximum accepted size, in bytes.
        maximum: usize,
    },
    /// The materialized task record would exceed its bound.
    MaterializedStateTooLarge {
        /// The size of the rejected record, in bytes.
        bytes: usize,
        /// The maximum accepted size, in bytes.
        maximum: usize,
    },
    /// A history sequence already holds a different entry.
    HistoryConflict {
        /// The contested sequence.
        sequence: AgentTaskHistorySequence,
    },
    /// The task owes its history sink more entries than it may hold, so it
    /// cannot transition again until the sink accepts them.
    HistoryBacklog {
        /// How many entries the task owes.
        pending: usize,
        /// The maximum it may owe.
        maximum: usize,
    },
    /// The agent's durable admission state could not be read.
    AgentRead {
        /// The agent whose state could not be read.
        agent: AgentId,
        /// The failure detail.
        message: String,
    },
    /// A result was decoded under a different task definition than it was
    /// accepted under.
    DefinitionMismatch {
        /// The definition and revision that were expected.
        expected: String,
        /// The definition and revision the record carried.
        actual: String,
    },
    /// A result was decoded under a different schema than it was accepted under.
    SchemaMismatch {
        /// The schema that was expected.
        expected: String,
        /// The schema the record carried.
        actual: String,
    },
    /// The accepted result is held behind an artifact reference and cannot be
    /// decoded without loading it.
    ResultBehindArtifact,
    /// A value could not be encoded.
    Encoding {
        /// The encoding failure detail.
        message: String,
    },
    /// A value could not be decoded.
    Decoding {
        /// The decoding failure detail.
        message: String,
    },
}

impl AgentTaskError {
    /// The stable machine-readable error code.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::Identity(error) => error.code(),
            Self::Schema(error) => error.code(),
            Self::Choreography(error) => error.code(),
            Self::Escrow(error) => error.code(),
            Self::Persistence(error) => error.code(),
            Self::InvalidDefinition { .. } => "invalid-task-definition",
            Self::NotCreated { .. } => "task-not-created",
            Self::AlreadyCreated { .. } => "task-already-created",
            Self::Terminal { .. } => "task-terminal",
            Self::MissingAssignee => "task-missing-assignee",
            Self::ContinuousWithoutGoal => "task-continuous-without-goal",
            Self::Wake(error) => error.code(),
            Self::WakeNotContinuous => "task-wake-not-continuous",
            Self::WakeGoalMismatch { .. } => "task-wake-goal-mismatch",
            Self::WakeOperationMismatch => "task-wake-operation-mismatch",
            Self::ScheduleNotMonotonic { .. } => "task-schedule-not-monotonic",
            Self::WakePolicyNotNewer { .. } => "task-wake-policy-not-newer",
            Self::TaskIdTooLong { .. } => "task-id-too-long",
            Self::NotReady => "task-admission-not-resolved",
            Self::DependencyCycle { .. } => "task-dependency-cycle",
            Self::DependencyDepthExceeded { .. } => "task-dependency-depth-exceeded",
            Self::DependencyLimitExceeded { .. } => "task-dependency-limit-exceeded",
            Self::DependencyConflict { .. } => "task-dependency-conflict",
            Self::UnknownDependency { .. } => "task-unknown-dependency",
            Self::ContentTooLarge { .. } => "task-content-too-large",
            Self::MaterializedStateTooLarge { .. } => "task-state-too-large",
            Self::HistoryConflict { .. } => "task-history-conflict",
            Self::HistoryBacklog { .. } => "task-history-backlog",
            Self::AgentRead { .. } => "task-agent-read-failed",
            Self::DefinitionMismatch { .. } => "task-definition-mismatch",
            Self::SchemaMismatch { .. } => "task-schema-mismatch",
            Self::ResultBehindArtifact => "task-result-behind-artifact",
            Self::Encoding { .. } => "task-encoding-failed",
            Self::Decoding { .. } => "task-decoding-failed",
        }
    }
}

impl Display for AgentTaskError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Identity(error) => Display::fmt(error, f),
            Self::Schema(error) => Display::fmt(error, f),
            Self::Choreography(error) => Display::fmt(error, f),
            Self::Escrow(error) => Display::fmt(error, f),
            Self::Persistence(error) => Display::fmt(error, f),
            Self::InvalidDefinition { detail } => {
                write!(f, "the task definition is not bounded: {detail}")
            }
            Self::NotCreated { scope } => write!(f, "task {scope} is not created"),
            Self::AlreadyCreated { scope } => write!(f, "task {scope} is already created"),
            Self::Terminal { status } => write!(
                f,
                "the task is {status} and accepts no further transition"
            ),
            Self::MissingAssignee => {
                write!(f, "an agent-owned task must name the agent it is created for")
            }
            Self::ContinuousWithoutGoal => {
                write!(f, "a continuous-mode task must bind the goal its controller drives")
            }
            Self::Wake(error) => Display::fmt(error, f),
            Self::WakeNotContinuous => write!(
                f,
                "a wake command addresses a task that does not coordinate a continuous goal"
            ),
            Self::WakeGoalMismatch { offered } => write!(
                f,
                "the wake was constructed for goal {offered}, which this task does not bind"
            ),
            Self::WakeOperationMismatch => write!(
                f,
                "the command's operation id is not the one its own wake binding derives"
            ),
            Self::ScheduleNotMonotonic { offered, current } => write!(
                f,
                "a schedule update must move strictly forward: revision {offered} does not follow {current}"
            ),
            Self::WakePolicyNotNewer { offered, current } => write!(
                f,
                "a wake-policy update must move strictly forward: revision {offered} does not follow {current}"
            ),
            Self::TaskIdTooLong { length, maximum } => write!(
                f,
                "the task id is {length} bytes, and an agent-owned task id may use at most {maximum} so the run ids derived from it stay valid"
            ),
            Self::NotReady => write!(
                f,
                "the agent's admission state was not resolved before the assignment decision"
            ),
            Self::DependencyCycle { dependency } => write!(
                f,
                "a dependency on {dependency} would make the task graph cyclic"
            ),
            Self::DependencyDepthExceeded { depth, maximum } => write!(
                f,
                "a dependency declared {depth} ancestors, which exceeds the maximum depth {maximum}"
            ),
            Self::DependencyLimitExceeded { maximum } => write!(
                f,
                "a task may declare at most {maximum} dependencies"
            ),
            Self::DependencyConflict { dependency } => write!(
                f,
                "dependency {dependency} was redeclared or resolved with a conflicting value"
            ),
            Self::UnknownDependency { dependency } => write!(
                f,
                "the task does not declare a dependency on {dependency}"
            ),
            Self::ContentTooLarge { bytes, maximum } => write!(
                f,
                "inline content is {bytes} bytes, which exceeds the {maximum} byte limit; it belongs behind an artifact reference"
            ),
            Self::MaterializedStateTooLarge { bytes, maximum } => write!(
                f,
                "the materialized task record is {bytes} bytes, which exceeds the {maximum} byte limit"
            ),
            Self::HistoryConflict { sequence } => write!(
                f,
                "history sequence {sequence} already holds a different entry"
            ),
            Self::HistoryBacklog { pending, maximum } => write!(
                f,
                "the task owes its history sink {pending} entries, the most it may hold is {maximum}, and it may not transition again until the sink accepts them"
            ),
            Self::AgentRead { agent, message } => write!(
                f,
                "the durable admission state of agent {agent} could not be read: {message}"
            ),
            Self::DefinitionMismatch { expected, actual } => write!(
                f,
                "the result was accepted under task definition {actual} but was decoded as {expected}"
            ),
            Self::SchemaMismatch { expected, actual } => write!(
                f,
                "the result was accepted under schema {actual} but was decoded as {expected}"
            ),
            Self::ResultBehindArtifact => write!(
                f,
                "the accepted result is held behind an artifact reference and must be loaded through a bounded adapter"
            ),
            Self::Encoding { message } => write!(f, "a task value could not be encoded: {message}"),
            Self::Decoding { message } => write!(f, "a task value could not be decoded: {message}"),
        }
    }
}

impl Error for AgentTaskError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Identity(error) => Some(error),
            Self::Schema(error) => Some(error),
            Self::Choreography(error) => Some(error),
            Self::Escrow(error) => Some(error),
            Self::Persistence(error) => Some(error),
            Self::Wake(error) => Some(error),
            _ => None,
        }
    }
}

impl From<AgentWakeError> for AgentTaskError {
    fn from(error: AgentWakeError) -> Self {
        Self::Wake(error)
    }
}

impl From<AgentIdentityError> for AgentTaskError {
    fn from(error: AgentIdentityError) -> Self {
        Self::Identity(error)
    }
}

impl From<AgentSchemaError> for AgentTaskError {
    fn from(error: AgentSchemaError) -> Self {
        Self::Schema(error)
    }
}

impl From<AgentChoreographyError> for AgentTaskError {
    fn from(error: AgentChoreographyError) -> Self {
        Self::Choreography(Box::new(error))
    }
}

impl From<AgentEscrowError> for AgentTaskError {
    fn from(error: AgentEscrowError) -> Self {
        Self::Escrow(error)
    }
}

impl From<DurableError> for AgentTaskError {
    fn from(error: DurableError) -> Self {
        Self::Persistence(error)
    }
}

impl From<AgentTaskError> for AgentChoreographyError {
    fn from(error: AgentTaskError) -> Self {
        match error {
            AgentTaskError::Identity(error) => Self::Identity(error),
            AgentTaskError::Schema(error) => Self::Schema(error),
            AgentTaskError::Choreography(error) => *error,
            AgentTaskError::Persistence(error) => Self::Persistence(error),
            other => Self::PayloadEncoding {
                message: other.to_string(),
            },
        }
    }
}

#[cfg(test)]
mod digest_tests {
    use super::{AgentContentDigest, AgentDigestAlgorithm};
    use serde_json::json;

    #[test]
    fn sha256_matches_the_standard_vectors() {
        // FIPS 180-4 / RFC 6234 known-answer vectors.
        assert_eq!(
            AgentContentDigest::sha256_of_bytes(b"").value,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            AgentContentDigest::sha256_of_bytes(b"abc").value,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            AgentContentDigest::sha256_of_bytes(
                b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"
            )
            .value,
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
        );
    }

    #[test]
    fn the_cryptographic_digest_is_canonical_and_labelled() {
        let a = AgentContentDigest::sha256_of_json(&json!({"a": 1, "b": 2}));
        let b = AgentContentDigest::sha256_of_json(&json!({"b": 2, "a": 1}));
        assert_eq!(a, b, "key order must not change the digest");
        assert_eq!(a.algorithm, AgentDigestAlgorithm::Sha256);
        assert!(a.algorithm.is_cryptographic());
        assert!(!AgentDigestAlgorithm::Fnv1a128.is_cryptographic());
        assert_eq!(a.to_string(), format!("sha2-256:{}", a.value));
    }

    #[test]
    fn a_changed_argument_changes_the_cryptographic_digest() {
        let before = AgentContentDigest::sha256_of_json(&json!({"amount": 100}));
        let after = AgentContentDigest::sha256_of_json(&json!({"amount": 101}));
        assert_ne!(before, after);
    }
}
