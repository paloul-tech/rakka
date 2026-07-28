//! Continuous-goal wake identity and the versioned wake policy.
//!
//! Owns the construction of [`AgentWakeId`] — goal plus [`ScheduleRevision`]
//! plus logical occurrence, so the same occurrence reached from any trigger
//! path yields one identity — the [`AgentWakeBinding`] record that ties one
//! wake to everything [specification 6.9](../../../docs/plans/rakka-agent/spec.md)
//! requires it to bind, and the versioned [`AgentWakePolicy`] with the full
//! field set of [specification 8.2](../../../docs/plans/rakka-agent/spec.md).
//!
//! # Why the wake identity is derived, not generated
//!
//! A wake may reach the controller from a durable timer scan, a duplicate scan
//! after scanner restart, an external event delivered twice, an authenticated
//! A2A command, or a callback — and however it arrives, it must admit at most
//! one epoch. That is only possible if the identity is a *pure function* of the
//! logical occurrence: [`wake_id_for_occurrence`] digests the tenant, goal,
//! schedule revision, and occurrence identity, and deliberately takes no
//! trigger source, delivery time, or lateness. Trigger metadata lives on the
//! [`AgentWakeBinding`], never in the identity.
//!
//! The value is a `wake-`-prefixed SHA-256 digest over a length-prefixed
//! canonical encoding, following the derived-claim-identity precedent: the
//! encoding is injective (no separator ambiguity, whatever the goal or event
//! identity contains), the output is fixed-length and always a valid identity
//! segment, and it leaves headroom for the epoch task and run identities later
//! derived from it. The derivation is a persisted compatibility surface; the
//! golden vectors in `tests/wake_identity.rs` pin it.
//!
//! # Policy defaults
//!
//! The defaults are the resolved continuous defaults of
//! [specification 21.1](../../../docs/plans/rakka-agent/spec.md) items 1-3:
//! overlap forbidden with durable coalescing, at most one coalesced occurrence
//! after downtime, and obsolete schedule revisions fenced. Parallel epochs and
//! bounded catch-up exist in the contract but must be declared explicitly,
//! with their concurrency and result policy; no default produces them.
//!
//! The controller's admission state machine also lives here:
//! [`AgentWakeControllerState`] dispositions every delivery deterministically
//! — fence, duplicate, admit, coalesce, or skip — while the task entity
//! records its transitions and the shared scanner injects its deliveries.
//! Epoch admission and the window-refill transition land with slice 3.3.
//! Scanner and pod uptime never create an occurrence; only durable logical
//! time does.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use rakka_agent_workflow::{
    AgentTimestampMillis, AgentTriggerSource, AgentTriggerSourceError, StateSchemaVersion,
};
use serde::de::Error as DeserializeError;
use serde::{Deserialize, Deserializer, Serialize};

use crate::budget::{AgentBudgetAllocation, AgentBudgetConsumption, AgentBudgetDimension};
use crate::definition::{AgentPolicyRef, AgentRevisionNumber, AgentRevisionProvenance};
use crate::identity::{
    validate_tenant, validated_id, AgentGoalId, AgentIdentityError, AgentOperationId,
    AgentOperationKind, AgentRunId, AgentTaskId, AgentWakeId, TenantId,
};
use crate::schema::{
    AgentRecordKind, VersionedAgentRecord, CURRENT_AGENT_WAKE_POLICY_SCHEMA_VERSION,
};
use crate::task::AgentContentDigest;

/// Prefix of every derived [`AgentWakeId`] value.
pub const AGENT_WAKE_ID_PREFIX: &str = "wake-";

/// Result type for wake identity, binding, and policy construction.
pub type AgentWakeResult<T> = Result<T, AgentWakeError>;

/// Monotonic revision of one continuous goal's schedule
/// ([specification 6.9](../../../docs/plans/rakka-agent/spec.md)).
///
/// A schedule update creates the next revision, and the controller fences
/// pending wakes carrying an obsolete one unless an explicit migration adopts
/// them. It is deliberately distinct from [`AgentRevisionNumber`]: the fencing
/// decision of slice 3.2 compares schedule revisions specifically, and the type
/// keeps a definition or policy revision from ever standing in for one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct ScheduleRevision(u64);

impl ScheduleRevision {
    /// First revision of any continuous goal's schedule.
    pub const INITIAL: Self = Self(1);

    /// Creates a schedule revision.
    ///
    /// Revisions begin at [`Self::INITIAL`], so zero is not a value any
    /// schedule ever carried; it is clamped to the initial revision — the
    /// constructor cannot produce a revision the fencing comparison of slice
    /// 3.2 would have to special-case.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        if value == 0 {
            Self::INITIAL
        } else {
            Self(value)
        }
    }

    /// Returns the raw revision number.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the next revision.
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

impl Display for ScheduleRevision {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<'de> Deserialize<'de> for ScheduleRevision {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u64::deserialize(deserializer)?;
        if value == 0 {
            return Err(DeserializeError::custom(
                "a schedule revision of zero was never issued",
            ));
        }
        Ok(Self(value))
    }
}

validated_id! {
    /// Identity of one external event that may wake a continuous goal
    /// ([specification 6.9](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// Application-owned event routing supplies it; it must be stable across
    /// redelivery, so a duplicate delivery reconstructs the same occurrence and
    /// therefore the same wake.
    pub AgentWakeEventId, "agent_wake_event_id"
}

validated_id! {
    /// Identity of one external callback that may wake a continuous goal
    /// ([specification 6.9](../../../docs/plans/rakka-agent/spec.md)).
    pub AgentWakeCallbackId, "agent_wake_callback_id"
}

/// The logical occurrence one wake stands for
/// ([specification 6.9](../../../docs/plans/rakka-agent/spec.md)).
///
/// This is the *identity* half of a wake: what happened in logical time, with
/// no trace of how its notification was delivered. A scheduled slot is its due
/// time under the schedule revision; an event, command, or callback is its
/// stable external identity. Two deliveries of the same occurrence — a
/// duplicate timer scan, a redelivered webhook, a replayed command — compare
/// equal here and therefore derive the same [`AgentWakeId`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentWakeOccurrence {
    /// One slot of the durable schedule, identified by its logical due time.
    Scheduled {
        /// When the schedule made this occurrence due.
        due_at: AgentTimestampMillis,
    },
    /// One external event accepted by application-owned event routing.
    ExternalEvent {
        /// Stable identity of the event, stable across redelivery.
        event: AgentWakeEventId,
    },
    /// One authenticated A2A wake command.
    Command {
        /// The command's stable operation id, which is also its deduplication
        /// identity — a replayed command reconstructs the same occurrence.
        operation: AgentOperationId,
    },
    /// One external callback normalized by application-owned routing.
    Callback {
        /// Stable identity of the callback.
        callback: AgentWakeCallbackId,
    },
    /// One controller-originated retry: the goal's own durable re-wake at a
    /// computed time, parked for the failure backoff elapsing or the goal
    /// window turning. Its delivery promotes whatever is admittable; it never
    /// admits an epoch of its own.
    Retry {
        /// When the retry becomes due — the backoff's end or the window's
        /// turn. Part of the identity, so each computed retry is one wake.
        due_at: AgentTimestampMillis,
        /// What the retry re-attempts.
        cause: AgentWakeRewakeCause,
        /// Which delivery generation of this re-wake this is. Part of the
        /// identity: a retry delivered before the controller's own clock
        /// reaches its due time is consumed without promoting anything, and
        /// its timer entry goes terminal — the slot re-arms under the next
        /// attempt so the re-park derives a fresh wake instead of being
        /// absorbed by the fired entry.
        #[serde(default)]
        attempt: u64,
    },
}

impl AgentWakeOccurrence {
    /// Stable kebab-case label of the occurrence kind.
    ///
    /// It is a segment of the wake-id digest input, so two occurrence kinds
    /// whose identity values coincide still derive distinct wakes.
    #[must_use]
    pub const fn kind_label(&self) -> &'static str {
        match self {
            Self::Scheduled { .. } => "scheduled",
            Self::ExternalEvent { .. } => "external-event",
            Self::Command { .. } => "a2a-command",
            Self::Callback { .. } => "callback",
            Self::Retry { .. } => "controller-retry",
        }
    }

    /// The occurrence's identity value: the second digest segment.
    #[must_use]
    pub fn identity_value(&self) -> String {
        match self {
            Self::Scheduled { due_at } => due_at.as_millis().to_string(),
            Self::ExternalEvent { event } => event.as_str().to_string(),
            Self::Command { operation } => operation.as_str().to_string(),
            Self::Callback { callback } => callback.as_str().to_string(),
            // Attempt zero keeps the original two-segment form, so every
            // binding persisted before the attempt generation existed still
            // re-derives its stored identity.
            Self::Retry {
                due_at,
                cause,
                attempt: 0,
            } => {
                format!("{}:{}", cause.as_label(), due_at.as_millis())
            }
            Self::Retry {
                due_at,
                cause,
                attempt,
            } => {
                format!("{}:{}:{attempt}", cause.as_label(), due_at.as_millis())
            }
        }
    }

    /// When the occurrence was due, for the kinds that have a logical due time.
    ///
    /// A retry exposes its computed due time — that is what makes its parked
    /// timer entry due at the backoff's end or the window's turn rather than
    /// immediately.
    #[must_use]
    pub const fn due_at(&self) -> Option<AgentTimestampMillis> {
        match self {
            Self::Scheduled { due_at } | Self::Retry { due_at, .. } => Some(*due_at),
            _ => None,
        }
    }
}

impl Display for AgentWakeOccurrence {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.kind_label(), self.identity_value())
    }
}

/// Derives the one wake identity of a logical occurrence
/// ([specification 6.9](../../../docs/plans/rakka-agent/spec.md)).
///
/// The identity is a pure function of `(tenant, goal, schedule revision,
/// occurrence)` — deliberately *not* of the trigger source, delivery time, or
/// lateness — so the same occurrence reached from any trigger path yields one
/// identity, which is what makes wake deduplication a construction property
/// rather than a runtime coordination problem.
///
/// The digest input length-prefixes every segment, so the encoding is
/// injective whatever the goal or event identity contains, and the output is a
/// fixed-length value that always satisfies the identity bounds. The format is
/// a persisted compatibility surface: the golden vectors in
/// `tests/wake_identity.rs` pin it, and changing it requires a migration.
pub fn wake_id_for_occurrence(
    tenant: &TenantId,
    goal: &AgentGoalId,
    schedule_revision: ScheduleRevision,
    occurrence: &AgentWakeOccurrence,
) -> AgentWakeResult<AgentWakeId> {
    validate_tenant(tenant)?;
    let mut canonical = Vec::new();
    for segment in [
        tenant.as_str(),
        goal.as_str(),
        &schedule_revision.to_string(),
        occurrence.kind_label(),
        &occurrence.identity_value(),
    ] {
        canonical.extend_from_slice(segment.len().to_string().as_bytes());
        canonical.push(b':');
        canonical.extend_from_slice(segment.as_bytes());
    }
    let digest = AgentContentDigest::sha256_of_bytes(&canonical);
    Ok(AgentWakeId::new(format!(
        "{AGENT_WAKE_ID_PREFIX}{}",
        digest.value
    ))?)
}

/// Derives the stable operation id of one wake's admission
/// ([specification 6.10](../../../docs/plans/rakka-agent/spec.md)).
///
/// This is the value the controller's durable inbox deduplicates on in slice
/// 3.2: because the wake id is itself derived, every trigger path reconstructs
/// the same admission operation, and a duplicate delivery replays instead of
/// admitting twice.
pub fn wake_admission_operation_id(
    tenant: &TenantId,
    goal: &AgentGoalId,
    wake: &AgentWakeId,
) -> AgentWakeResult<AgentOperationId> {
    Ok(AgentOperationId::new(
        AgentOperationKind::WakeAdmission,
        [tenant.as_str(), goal.as_str(), wake.as_str()],
    )?)
}

/// Derives the finite child task one admitted wake's epoch runs as
/// ([specification 6.5](../../../docs/plans/rakka-agent/spec.md)).
///
/// The identity is derived, never generated: `epoch-` plus the wake's own
/// digest, so replaying an admission resolves to the same child task, the
/// length is a constant 70 bytes independent of the root control task's id,
/// and two wakes can never share an epoch. A wake identity that does not
/// carry the canonical `wake-` prefix was not derived by
/// [`wake_id_for_occurrence`] and fails closed.
pub fn epoch_task_id_for_wake(wake: &AgentWakeId) -> AgentWakeResult<AgentTaskId> {
    let digest = wake
        .as_str()
        .strip_prefix(AGENT_WAKE_ID_PREFIX)
        .ok_or_else(|| AgentWakeError::ForeignWakeId { wake: wake.clone() })?;
    Ok(AgentTaskId::new(format!("epoch-{digest}"))?)
}

/// Derives the stable operation id of one epoch's admission
/// ([specification 6.10](../../../docs/plans/rakka-agent/spec.md)).
///
/// The epoch-creation exchange the controller owes deduplicates on it, so a
/// replayed admission resolves to the same child epoch rather than creating a
/// second one.
pub fn epoch_admission_operation_id(
    tenant: &TenantId,
    goal: &AgentGoalId,
    wake: &AgentWakeId,
) -> AgentWakeResult<AgentOperationId> {
    Ok(AgentOperationId::new(
        AgentOperationKind::EpochAdmission,
        [tenant.as_str(), goal.as_str(), wake.as_str()],
    )?)
}

/// Derives the stable operation id of one epoch's result exchange
/// ([specification 6.10](../../../docs/plans/rakka-agent/spec.md)).
///
/// The completed epoch task owes its result to the controller under it, so a
/// replayed completion resolves to the same release rather than a second one.
pub fn epoch_result_operation_id(
    tenant: &TenantId,
    goal: &AgentGoalId,
    wake: &AgentWakeId,
) -> AgentWakeResult<AgentOperationId> {
    Ok(AgentOperationId::new(
        AgentOperationKind::EpochResult,
        [tenant.as_str(), goal.as_str(), wake.as_str()],
    )?)
}

/// Trigger class through which a wake may reach a continuous goal
/// ([specification 8.2](../../../docs/plans/rakka-agent/spec.md)).
///
/// The wake policy declares the allowed set; a hybrid policy allows more than
/// one. The class describes *how* an occurrence's notification arrives, never
/// which occurrence it is — the identity of the wake is blind to it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentWakeTriggerKind {
    /// A durable one-shot timer recovered by the shared scanner.
    DurableTimer,
    /// An external event accepted by application-owned event routing.
    ExternalEvent,
    /// An authenticated A2A command.
    A2aCommand,
    /// An external callback normalized by application-owned routing.
    Callback,
    /// The controller's own durable retry. Never declared by a policy and
    /// never accepted for any occurrence but a retry: the policy's trigger
    /// set governs the outside world, and the outside world cannot speak as
    /// the controller.
    Controller,
}

impl AgentWakeTriggerKind {
    /// Every trigger class, in stable order.
    pub const ALL: [Self; 5] = [
        Self::DurableTimer,
        Self::ExternalEvent,
        Self::A2aCommand,
        Self::Callback,
        Self::Controller,
    ];

    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::DurableTimer => "durable-timer",
            Self::ExternalEvent => "external-event",
            Self::A2aCommand => "a2a-command",
            Self::Callback => "callback",
            Self::Controller => "controller",
        }
    }
}

impl Display for AgentWakeTriggerKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// Everything one wake binds
/// ([specification 6.9](../../../docs/plans/rakka-agent/spec.md)): the goal,
/// the schedule revision it was constructed under, the logical occurrence, the
/// trigger path that delivered it, its due and accepted times, and the policy
/// revision in force.
///
/// The binding is where delivery metadata lives — and where it stops. The wake
/// id is derived from the occurrence at construction, so two bindings for the
/// same occurrence carry the same identity however differently they were
/// triggered; the slice 3.2 controller persists the binding and admits on the
/// identity. Deserialization re-derives the id and fails closed on a record
/// whose stored identity does not match its own components.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AgentWakeBinding {
    tenant: TenantId,
    goal: AgentGoalId,
    schedule_revision: ScheduleRevision,
    occurrence: AgentWakeOccurrence,
    trigger: AgentWakeTriggerKind,
    source: Option<AgentTriggerSource>,
    accepted_at: AgentTimestampMillis,
    policy_revision: AgentRevisionNumber,
    wake: AgentWakeId,
}

impl AgentWakeBinding {
    /// Creates a wake binding, deriving its identity from the occurrence.
    pub fn new(
        tenant: TenantId,
        goal: AgentGoalId,
        schedule_revision: ScheduleRevision,
        occurrence: AgentWakeOccurrence,
        trigger: AgentWakeTriggerKind,
        accepted_at: AgentTimestampMillis,
        policy_revision: AgentRevisionNumber,
    ) -> AgentWakeResult<Self> {
        let wake = wake_id_for_occurrence(&tenant, &goal, schedule_revision, &occurrence)?;
        Ok(Self {
            tenant,
            goal,
            schedule_revision,
            occurrence,
            trigger,
            source: None,
            accepted_at,
            policy_revision,
            wake,
        })
    }

    /// Attaches bounded trigger-source metadata, validating its labels.
    ///
    /// The source describes delivery and never identity: two bindings that
    /// differ only in source still carry the same wake id.
    pub fn with_source(mut self, source: AgentTriggerSource) -> AgentWakeResult<Self> {
        source.validate()?;
        self.source = Some(source);
        Ok(self)
    }

    /// Tenant boundary of the wake.
    #[must_use]
    pub const fn tenant(&self) -> &TenantId {
        &self.tenant
    }

    /// Goal the wake may admit an epoch for.
    #[must_use]
    pub const fn goal(&self) -> &AgentGoalId {
        &self.goal
    }

    /// Schedule revision the wake was constructed under. Slice 3.2 fences a
    /// binding whose revision is obsolete.
    #[must_use]
    pub const fn schedule_revision(&self) -> ScheduleRevision {
        self.schedule_revision
    }

    /// The logical occurrence the wake stands for.
    #[must_use]
    pub const fn occurrence(&self) -> &AgentWakeOccurrence {
        &self.occurrence
    }

    /// Trigger class that delivered this occurrence.
    #[must_use]
    pub const fn trigger(&self) -> AgentWakeTriggerKind {
        self.trigger
    }

    /// Bounded trigger-source metadata, when the delivery carried any.
    #[must_use]
    pub const fn source(&self) -> Option<&AgentTriggerSource> {
        self.source.as_ref()
    }

    /// When the occurrence was due, for occurrence kinds with a due time.
    #[must_use]
    pub const fn due_at(&self) -> Option<AgentTimestampMillis> {
        self.occurrence.due_at()
    }

    /// When the trigger was durably accepted.
    #[must_use]
    pub const fn accepted_at(&self) -> AgentTimestampMillis {
        self.accepted_at
    }

    /// Wake-policy revision in force when the binding was constructed.
    #[must_use]
    pub const fn policy_revision(&self) -> AgentRevisionNumber {
        self.policy_revision
    }

    /// The derived wake identity: the deduplication identity of the wake.
    #[must_use]
    pub const fn wake_id(&self) -> &AgentWakeId {
        &self.wake
    }

    /// Whether `other` binds the same occurrence identity — the components
    /// the wake id is derived from — regardless of delivery metadata
    /// (trigger, source, accepted time, policy revision), which legitimately
    /// differs between two deliveries of one occurrence.
    #[must_use]
    pub fn same_identity(&self, other: &Self) -> bool {
        self.tenant == other.tenant
            && self.goal == other.goal
            && self.schedule_revision == other.schedule_revision
            && self.occurrence == other.occurrence
    }

    /// The stable operation id this wake's admission deduplicates on.
    pub fn admission_operation_id(&self) -> AgentWakeResult<AgentOperationId> {
        wake_admission_operation_id(&self.tenant, &self.goal, &self.wake)
    }
}

impl<'de> Deserialize<'de> for AgentWakeBinding {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Record {
            tenant: TenantId,
            goal: AgentGoalId,
            schedule_revision: ScheduleRevision,
            occurrence: AgentWakeOccurrence,
            trigger: AgentWakeTriggerKind,
            #[serde(default)]
            source: Option<AgentTriggerSource>,
            accepted_at: AgentTimestampMillis,
            policy_revision: AgentRevisionNumber,
            wake: AgentWakeId,
        }

        let record = Record::deserialize(deserializer)?;
        let mut binding = Self::new(
            record.tenant,
            record.goal,
            record.schedule_revision,
            record.occurrence,
            record.trigger,
            record.accepted_at,
            record.policy_revision,
        )
        .map_err(DeserializeError::custom)?;
        if let Some(source) = record.source {
            binding = binding
                .with_source(source)
                .map_err(DeserializeError::custom)?;
        }
        if *binding.wake_id() != record.wake {
            return Err(DeserializeError::custom(
                "wake binding carries an identity its components do not derive",
            ));
        }
        Ok(binding)
    }
}

/// What happens when a trigger arrives while an epoch is active
/// ([specification 8.2](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentWakeOverlapPolicy {
    /// The default: a second active epoch is forbidden, and triggers received
    /// while one is active are durably coalesced into at most one pending
    /// occurrence.
    ForbidAndCoalesce,
    /// Bounded parallel epochs. Never a default: the spec requires an explicit
    /// definition, which is why the concurrency bound and the result policy
    /// are constructor arguments with no fallback.
    Parallel {
        /// Most epochs that may be active at once. At least two — a bound of
        /// one is [`Self::ForbidAndCoalesce`] and must be spelled that way.
        max_concurrent_epochs: u32,
        /// Application-owned policy deciding how parallel epoch results
        /// combine.
        result_policy: AgentPolicyRef,
    },
}

/// What happens to occurrences that became due while no scanner could deliver
/// them ([specification 8.2](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentMissedOccurrencePolicy {
    /// The default after downtime: admit at most one coalesced occurrence,
    /// never unbounded catch-up.
    AdmitOneCoalesced,
    /// Skip missed occurrences entirely; the next due occurrence proceeds.
    Skip,
    /// Replay a bounded number of missed occurrences. Never a default: the
    /// spec requires an explicit definition.
    BoundedCatchUp {
        /// Most missed occurrences one recovery may replay. At least one.
        max_occurrences: u32,
    },
}

/// Calendar unit of a calendar-aligned budget window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentCalendarUnit {
    /// One calendar day.
    Day,
    /// One calendar week.
    Week,
    /// One calendar month.
    Month,
}

impl AgentCalendarUnit {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Day => "day",
            Self::Week => "week",
            Self::Month => "month",
        }
    }
}

impl Display for AgentCalendarUnit {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// The window a goal-level budget ceiling covers
/// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentBudgetWindow {
    /// A rolling window of fixed length.
    Rolling {
        /// Window length in milliseconds. Positive.
        length_millis: u64,
    },
    /// A calendar-aligned window.
    Calendar {
        /// The calendar unit the window aligns to.
        unit: AgentCalendarUnit,
    },
}

/// A goal-level budget ceiling over a rolling or calendar window
/// ([specification 8.2](../../../docs/plans/rakka-agent/spec.md),
/// [9.7](../../../docs/plans/rakka-agent/spec.md)).
///
/// The window's durable refill transition is slice 3.3; this is the policy
/// contract it enforces. Refill is a persisted logical-time transition and is
/// never inferred from process uptime, activation, pod restart, or shard
/// movement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentGoalWindowCeiling {
    /// The window the ceiling covers.
    pub window: AgentBudgetWindow,
    /// The ceiling in force for each window. At least one dimension must be
    /// bounded, or the window would restrict nothing.
    pub ceiling: AgentBudgetAllocation,
}

/// Backoff applied to wake admission after consecutive epoch failures
/// ([specification 8.2](../../../docs/plans/rakka-agent/spec.md)).
///
/// The multiplier is an integer percentage — 200 doubles the delay — so the
/// policy stays exactly representable, comparable, and serializable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentWakeBackoffPolicy {
    /// Delay before the first retry, in milliseconds. Positive.
    pub initial_millis: u64,
    /// Growth per consecutive failure, in percent. At least 100.
    pub multiplier_percent: u32,
    /// Ceiling the delay saturates at, in milliseconds. At least the initial
    /// delay.
    pub max_millis: u64,
    /// Consecutive failures after which the goal escalates instead of backing
    /// off further. Positive when set; unset means backoff alone.
    pub escalate_after_failures: Option<u32>,
}

impl AgentWakeBackoffPolicy {
    /// Default backoff: one second doubling to a one-hour ceiling, no
    /// escalation threshold.
    pub const DEFAULT: Self = Self {
        initial_millis: 1_000,
        multiplier_percent: 200,
        max_millis: 3_600_000,
        escalate_after_failures: None,
    };

    fn validate(&self) -> AgentWakeResult<()> {
        if self.initial_millis == 0 {
            return Err(AgentWakeError::BackoffInitialZero);
        }
        if self.multiplier_percent < 100 {
            return Err(AgentWakeError::BackoffMultiplierBelowUnit {
                multiplier_percent: self.multiplier_percent,
            });
        }
        if self.max_millis < self.initial_millis {
            return Err(AgentWakeError::BackoffMaximumBelowInitial {
                initial_millis: self.initial_millis,
                max_millis: self.max_millis,
            });
        }
        if self.escalate_after_failures == Some(0) {
            return Err(AgentWakeError::ZeroEscalationThreshold);
        }
        Ok(())
    }
}

impl Default for AgentWakeBackoffPolicy {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// What happens to triggers that arrive while the goal is suspended
/// ([specification 8.2](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentWakeSuspensionPolicy {
    /// The default: durably coalesce to at most one pending occurrence, which
    /// resume may admit.
    CoalesceLatest,
    /// Drop triggers received while suspended; only occurrences due after
    /// resume proceed.
    Drop,
}

/// Whether continued operation past the expiry horizon requires an explicit
/// renewal ([specification 8.2](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentWakeRenewalPolicy {
    /// The default: the goal runs to its expiry, if any, without renewal.
    NotRequired,
    /// An authorized renewal must arrive inside the window before expiry, or
    /// the goal expires. Requires an expiry to be set.
    RequiredBefore {
        /// Length of the renewal window before expiry, in milliseconds.
        /// Positive.
        window_millis: u64,
    },
}

/// When the goal retires
/// ([specification 8.2](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentWakeRetirementPolicy {
    /// The default: only an authorized command retires the goal.
    Manual,
    /// Retire after a bounded number of admitted occurrences.
    AfterOccurrences {
        /// Occurrences after which the goal retires. Positive.
        occurrences: u64,
    },
    /// Retire at a fixed logical time.
    At {
        /// The retirement time.
        at: AgentTimestampMillis,
    },
}

/// Suspension, renewal, expiry, and retirement policy of one continuous goal
/// ([specification 8.2](../../../docs/plans/rakka-agent/spec.md)).
///
/// The lifecycle transitions themselves land with slice 3.4; this is the
/// versioned contract they execute.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentWakeLifecyclePolicy {
    /// What happens to triggers received while suspended.
    pub while_suspended: AgentWakeSuspensionPolicy,
    /// Whether continued operation requires explicit renewal.
    pub renewal: AgentWakeRenewalPolicy,
    /// Logical time past which the goal expires, when it has one.
    pub expires_at: Option<AgentTimestampMillis>,
    /// When the goal retires.
    pub retirement: AgentWakeRetirementPolicy,
}

impl AgentWakeLifecyclePolicy {
    /// Default lifecycle: coalesce while suspended, no renewal requirement, no
    /// expiry, manual retirement.
    pub const DEFAULT: Self = Self {
        while_suspended: AgentWakeSuspensionPolicy::CoalesceLatest,
        renewal: AgentWakeRenewalPolicy::NotRequired,
        expires_at: None,
        retirement: AgentWakeRetirementPolicy::Manual,
    };

    fn validate(&self) -> AgentWakeResult<()> {
        if let AgentWakeRenewalPolicy::RequiredBefore { window_millis } = self.renewal {
            if window_millis == 0 {
                return Err(AgentWakeError::ZeroDuration {
                    field: "renewal window",
                });
            }
            if self.expires_at.is_none() {
                return Err(AgentWakeError::RenewalWithoutExpiry);
            }
        }
        if let AgentWakeRetirementPolicy::AfterOccurrences { occurrences } = self.retirement {
            if occurrences == 0 {
                return Err(AgentWakeError::ZeroRetirementOccurrences);
            }
        }
        Ok(())
    }
}

impl Default for AgentWakeLifecyclePolicy {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// The versioned wake policy of one continuous goal, with the full field set
/// of [specification 8.2](../../../docs/plans/rakka-agent/spec.md).
///
/// [`AgentWakePolicy::new`] produces the resolved defaults of
/// [specification 21.1](../../../docs/plans/rakka-agent/spec.md): overlap
/// forbidden with durable coalescing, at most one coalesced occurrence after
/// downtime, and no catch-up. Parallel epochs and bounded catch-up are
/// representable but never produced by a default — their constructors demand
/// the explicit concurrency and result policy the spec requires.
///
/// Fields are public so the policy composes as data, exactly like the task
/// definition; the bounded invariants are therefore re-checked by
/// [`AgentWakePolicy::validate`] wherever a policy crosses a durable boundary,
/// including deserialization, which fails closed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AgentWakePolicy {
    /// Trigger classes allowed to deliver occurrences. Non-empty; more than
    /// one makes the policy hybrid.
    pub triggers: BTreeSet<AgentWakeTriggerKind>,
    /// How long after its due time an occurrence may still be admitted as-is,
    /// in milliseconds. Positive when set; unset means no window bound.
    pub admission_window_millis: Option<u64>,
    /// Lateness past which an occurrence is treated as missed rather than
    /// admitted, in milliseconds. Positive when set, and at least the
    /// admission window when both are set — an occurrence still inside the
    /// window cannot simultaneously be missed. An occurrence between the two
    /// is past direct admission but not yet missed: it is what the overlap
    /// policy durably coalesces.
    pub maximum_lateness_millis: Option<u64>,
    /// What happens when a trigger arrives while an epoch is active.
    pub overlap: AgentWakeOverlapPolicy,
    /// What happens to occurrences missed during downtime.
    pub missed_occurrence: AgentMissedOccurrencePolicy,
    /// Budget escrowed to each admitted epoch.
    pub epoch_budget: AgentBudgetAllocation,
    /// Deadline of each admitted epoch, in milliseconds. Positive when set.
    pub epoch_deadline_millis: Option<u64>,
    /// Goal-level ceiling over a rolling or calendar window, when one is set.
    pub goal_window: Option<AgentGoalWindowCeiling>,
    /// Backoff after consecutive epoch failures.
    pub failure_backoff: AgentWakeBackoffPolicy,
    /// Suspension, renewal, expiry, and retirement policy.
    pub lifecycle: AgentWakeLifecyclePolicy,
}

impl AgentWakePolicy {
    /// Creates a policy with the resolved continuous defaults
    /// ([specification 21.1](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// An epoch must be bounded from the start: the budget must bound at least
    /// one dimension, or a deadline must be set, or construction fails —
    /// a continuous goal is bounded durable epochs, never an immortal loop.
    pub fn new(
        triggers: impl IntoIterator<Item = AgentWakeTriggerKind>,
        epoch_budget: AgentBudgetAllocation,
        epoch_deadline_millis: Option<u64>,
    ) -> AgentWakeResult<Self> {
        let policy = Self {
            triggers: triggers.into_iter().collect(),
            admission_window_millis: None,
            maximum_lateness_millis: None,
            overlap: AgentWakeOverlapPolicy::ForbidAndCoalesce,
            missed_occurrence: AgentMissedOccurrencePolicy::AdmitOneCoalesced,
            epoch_budget,
            epoch_deadline_millis,
            goal_window: None,
            failure_backoff: AgentWakeBackoffPolicy::DEFAULT,
            lifecycle: AgentWakeLifecyclePolicy::DEFAULT,
        };
        policy.validate()?;
        Ok(policy)
    }

    /// Declares bounded parallel epochs, which the spec never defaults to:
    /// the concurrency bound and the result policy are demanded here.
    pub fn with_parallel_epochs(
        mut self,
        max_concurrent_epochs: u32,
        result_policy: AgentPolicyRef,
    ) -> AgentWakeResult<Self> {
        self.overlap = AgentWakeOverlapPolicy::Parallel {
            max_concurrent_epochs,
            result_policy,
        };
        self.validate()?;
        Ok(self)
    }

    /// Declares bounded catch-up of missed occurrences, which the spec never
    /// defaults to.
    pub fn with_bounded_catch_up(mut self, max_occurrences: u32) -> AgentWakeResult<Self> {
        self.missed_occurrence = AgentMissedOccurrencePolicy::BoundedCatchUp { max_occurrences };
        self.validate()?;
        Ok(self)
    }

    /// Sets the admission window.
    pub fn with_admission_window(mut self, window_millis: u64) -> AgentWakeResult<Self> {
        self.admission_window_millis = Some(window_millis);
        self.validate()?;
        Ok(self)
    }

    /// Sets the maximum lateness.
    pub fn with_maximum_lateness(mut self, lateness_millis: u64) -> AgentWakeResult<Self> {
        self.maximum_lateness_millis = Some(lateness_millis);
        self.validate()?;
        Ok(self)
    }

    /// Sets the goal-level window ceiling.
    pub fn with_goal_window(
        mut self,
        goal_window: AgentGoalWindowCeiling,
    ) -> AgentWakeResult<Self> {
        self.goal_window = Some(goal_window);
        self.validate()?;
        Ok(self)
    }

    /// Sets the failure backoff.
    pub fn with_failure_backoff(
        mut self,
        backoff: AgentWakeBackoffPolicy,
    ) -> AgentWakeResult<Self> {
        self.failure_backoff = backoff;
        self.validate()?;
        Ok(self)
    }

    /// Sets the suspension, renewal, expiry, and retirement policy.
    pub fn with_lifecycle(mut self, lifecycle: AgentWakeLifecyclePolicy) -> AgentWakeResult<Self> {
        self.lifecycle = lifecycle;
        self.validate()?;
        Ok(self)
    }

    /// Whether the policy allows a trigger class.
    #[must_use]
    pub fn allows_trigger(&self, kind: AgentWakeTriggerKind) -> bool {
        self.triggers.contains(&kind)
    }

    /// Rejects a policy that violates its bounded invariants.
    ///
    /// The fields are public, so this runs wherever a policy crosses a durable
    /// boundary — construction, every `with_` helper, and deserialization.
    pub fn validate(&self) -> AgentWakeResult<()> {
        if self.triggers.is_empty() {
            return Err(AgentWakeError::EmptyTriggers);
        }
        for (field, value) in [
            ("admission window", self.admission_window_millis),
            ("maximum lateness", self.maximum_lateness_millis),
            ("epoch deadline", self.epoch_deadline_millis),
        ] {
            if value == Some(0) {
                return Err(AgentWakeError::ZeroDuration { field });
            }
        }
        if let (Some(window), Some(lateness)) =
            (self.admission_window_millis, self.maximum_lateness_millis)
        {
            if lateness < window {
                return Err(AgentWakeError::LatenessBelowAdmissionWindow {
                    admission_window_millis: window,
                    maximum_lateness_millis: lateness,
                });
            }
        }
        if let AgentWakeOverlapPolicy::Parallel {
            max_concurrent_epochs,
            ..
        } = &self.overlap
        {
            if *max_concurrent_epochs < 2 {
                return Err(AgentWakeError::ParallelEpochsNotParallel {
                    max_concurrent_epochs: *max_concurrent_epochs,
                });
            }
        }
        if let AgentMissedOccurrencePolicy::BoundedCatchUp { max_occurrences } =
            self.missed_occurrence
        {
            if max_occurrences == 0 {
                return Err(AgentWakeError::ZeroCatchUp);
            }
        }
        if self.epoch_deadline_millis.is_none() && self.epoch_budget.is_unbounded() {
            return Err(AgentWakeError::EpochUnbounded);
        }
        if let Some(goal_window) = &self.goal_window {
            if let AgentBudgetWindow::Rolling { length_millis } = goal_window.window {
                if length_millis == 0 {
                    return Err(AgentWakeError::ZeroDuration {
                        field: "rolling window length",
                    });
                }
            }
            if goal_window.ceiling.is_unbounded() {
                return Err(AgentWakeError::WindowCeilingUnbounded);
            }
            for dimension in AgentBudgetDimension::CONSERVED {
                let Some(limit) = goal_window.ceiling.get(dimension) else {
                    continue;
                };
                let Some(epoch_budget) = self.epoch_budget.get(dimension) else {
                    // An unbounded epoch dimension can never be charged
                    // against a bounded window ceiling: the very first
                    // admission would exhaust it.
                    return Err(AgentWakeError::WindowEpochUnbounded { dimension });
                };
                if epoch_budget > limit {
                    // A bounded epoch budget above the ceiling is just as
                    // unsatisfiable: even a freshly refilled window could
                    // never pay for one epoch, so every occurrence would
                    // defer forever.
                    return Err(AgentWakeError::WindowEpochExceedsCeiling {
                        dimension,
                        epoch_budget,
                        ceiling: limit,
                    });
                }
            }
        }
        self.failure_backoff.validate()?;
        self.lifecycle.validate()?;
        Ok(())
    }
}

impl<'de> Deserialize<'de> for AgentWakePolicy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Record {
            triggers: BTreeSet<AgentWakeTriggerKind>,
            admission_window_millis: Option<u64>,
            maximum_lateness_millis: Option<u64>,
            overlap: AgentWakeOverlapPolicy,
            missed_occurrence: AgentMissedOccurrencePolicy,
            epoch_budget: AgentBudgetAllocation,
            epoch_deadline_millis: Option<u64>,
            goal_window: Option<AgentGoalWindowCeiling>,
            failure_backoff: AgentWakeBackoffPolicy,
            lifecycle: AgentWakeLifecyclePolicy,
        }

        let record = Record::deserialize(deserializer)?;
        let policy = Self {
            triggers: record.triggers,
            admission_window_millis: record.admission_window_millis,
            maximum_lateness_millis: record.maximum_lateness_millis,
            overlap: record.overlap,
            missed_occurrence: record.missed_occurrence,
            epoch_budget: record.epoch_budget,
            epoch_deadline_millis: record.epoch_deadline_millis,
            goal_window: record.goal_window,
            failure_backoff: record.failure_backoff,
            lifecycle: record.lifecycle,
        };
        policy.validate().map_err(DeserializeError::custom)?;
        Ok(policy)
    }
}

/// One accepted revision of a continuous goal's wake policy
/// ([specification 8.2](../../../docs/plans/rakka-agent/spec.md)).
///
/// The policy is versioned exactly as settings are: every wake binds the
/// policy revision in force when it was constructed, so an operator can read
/// which contract admitted an epoch long after the policy moved on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentWakePolicyRevision {
    schema_version: StateSchemaVersion,
    revision: AgentRevisionNumber,
    policy: AgentWakePolicy,
    provenance: AgentRevisionProvenance,
}

impl AgentWakePolicyRevision {
    /// Creates the first wake-policy revision of a continuous goal.
    pub fn initial(
        policy: AgentWakePolicy,
        provenance: AgentRevisionProvenance,
    ) -> AgentWakeResult<Self> {
        policy.validate()?;
        Ok(Self {
            schema_version: CURRENT_AGENT_WAKE_POLICY_SCHEMA_VERSION,
            revision: AgentRevisionNumber::INITIAL,
            policy,
            provenance,
        })
    }

    /// Accepts an updated policy, producing the next revision.
    pub fn updated(
        &self,
        policy: AgentWakePolicy,
        provenance: AgentRevisionProvenance,
    ) -> AgentWakeResult<Self> {
        policy.validate()?;
        Ok(Self {
            schema_version: CURRENT_AGENT_WAKE_POLICY_SCHEMA_VERSION,
            revision: self.revision.next(),
            policy,
            provenance,
        })
    }

    /// Monotonic revision number.
    #[must_use]
    pub const fn revision(&self) -> AgentRevisionNumber {
        self.revision
    }

    /// The policy in force at this revision.
    #[must_use]
    pub const fn policy(&self) -> &AgentWakePolicy {
        &self.policy
    }

    /// Who accepted this revision, when, and under which audit reference.
    #[must_use]
    pub const fn provenance(&self) -> &AgentRevisionProvenance {
        &self.provenance
    }
}

impl VersionedAgentRecord for AgentWakePolicyRevision {
    const RECORD_KIND: AgentRecordKind = AgentRecordKind::WakePolicyRevision;

    fn schema_version(&self) -> StateSchemaVersion {
        self.schema_version
    }
}

/// Most occurrences the controller durably parks while they wait for an
/// active slot, whatever bound the policy declares.
///
/// The controller state lives inside the bounded task state, so the parked
/// queue is capped independently of
/// [`AgentMissedOccurrencePolicy::BoundedCatchUp`]'s own bound; an occurrence
/// past both is skipped, never silently kept.
pub const AGENT_WAKE_PENDING_CAPACITY: usize = 8;

/// Most concurrently active occurrences, whatever concurrency a parallel
/// overlap policy declares.
pub const AGENT_WAKE_ACTIVE_CAPACITY: usize = 8;

/// How many recently dispositioned wake identities the controller remembers
/// for deduplication beyond the operation-log window.
pub const AGENT_WAKE_RECENT_CAPACITY: usize = 16;

/// The finite child epoch one admitted occurrence executes as
/// ([specification 6.5](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentEpochRef {
    /// The epoch's derived child task.
    pub task: AgentTaskId,
    /// The run serving the epoch's first assignment generation.
    pub run: AgentRunId,
}

/// One admitted occurrence currently owning execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentActiveWake {
    binding: AgentWakeBinding,
    admitted_at: AgentTimestampMillis,
    /// Whether this occurrence was admitted as a downtime backlog's coalesced
    /// representative. While it runs, later missed occurrences of the same
    /// backlog are absorbed rather than parked, so one downtime yields one
    /// epoch. Records persisted before this field load as ordinary
    /// admissions.
    #[serde(default)]
    representative: bool,
    /// The finite child epoch this occurrence created, once the admitting
    /// transition attached it. Records persisted before this field load
    /// without one.
    #[serde(default)]
    epoch: Option<AgentEpochRef>,
}

impl AgentActiveWake {
    /// The admitted wake's binding.
    #[must_use]
    pub const fn binding(&self) -> &AgentWakeBinding {
        &self.binding
    }

    /// When the controller admitted the occurrence.
    #[must_use]
    pub const fn admitted_at(&self) -> AgentTimestampMillis {
        self.admitted_at
    }

    /// Whether the occurrence is a downtime backlog's coalesced
    /// representative.
    #[must_use]
    pub const fn is_representative(&self) -> bool {
        self.representative
    }

    /// The finite child epoch this occurrence executes as, once attached.
    #[must_use]
    pub const fn epoch(&self) -> Option<&AgentEpochRef> {
        self.epoch.as_ref()
    }
}

/// The durable ledger of one goal-window ceiling
/// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)): when the
/// current window began in logical time, and what admitted epochs have
/// consumed of it.
///
/// Refill is the ledger being replaced when an admission's logical time
/// crosses the window boundary — a persisted transition riding the recorded
/// admission, never an effect of a restart, activation, or shard movement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentWakeWindowLedger {
    window_start: AgentTimestampMillis,
    consumed: AgentBudgetConsumption,
}

impl AgentWakeWindowLedger {
    /// When the current window began, in logical time.
    #[must_use]
    pub const fn window_start(&self) -> AgentTimestampMillis {
        self.window_start
    }

    /// What admitted epochs have consumed within the current window.
    #[must_use]
    pub const fn consumed(&self) -> &AgentBudgetConsumption {
        &self.consumed
    }
}

const MILLIS_PER_DAY: u64 = 86_400_000;

/// Civil year and month of a day count since 1970-01-01 (UTC), by the
/// classic era-based algorithm.
const fn civil_year_month(days: u64) -> (u64, u64) {
    let z = days + 719_468;
    let era = z / 146_097;
    let doe = z % 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + if month <= 2 { 1 } else { 0 };
    (year, month)
}

/// Day count since 1970-01-01 (UTC) of the first day of a civil month.
const fn days_of_month_start(year: u64, month: u64) -> u64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year / 400;
    let yoe = year % 400;
    let mp = if month > 2 { month - 3 } else { month + 9 };
    let doy = (153 * mp + 2) / 5;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The UTC-aligned start of the calendar window containing `now`.
const fn calendar_window_start(unit: AgentCalendarUnit, now: AgentTimestampMillis) -> u64 {
    let days = now.as_millis() / MILLIS_PER_DAY;
    match unit {
        AgentCalendarUnit::Day => days * MILLIS_PER_DAY,
        AgentCalendarUnit::Week => {
            // 1970-01-01 was a Thursday; weeks start on Monday.
            let weekday = (days + 3) % 7;
            (days - weekday) * MILLIS_PER_DAY
        }
        AgentCalendarUnit::Month => {
            let (year, month) = civil_year_month(days);
            days_of_month_start(year, month) * MILLIS_PER_DAY
        }
    }
}

/// The start of the window containing `now`, given where the previous window
/// began.
fn advance_window_start(
    window: &AgentBudgetWindow,
    previous: AgentTimestampMillis,
    now: AgentTimestampMillis,
) -> AgentTimestampMillis {
    match window {
        AgentBudgetWindow::Rolling { length_millis } => {
            let elapsed = now.as_millis().saturating_sub(previous.as_millis());
            let advanced = previous.as_millis() + (elapsed / length_millis) * length_millis;
            AgentTimestampMillis::new(advanced)
        }
        AgentBudgetWindow::Calendar { unit } => {
            let boundary = calendar_window_start(*unit, now);
            // Logical time is monotone, but fail safe: a boundary can never
            // regress behind the window already in force.
            if boundary > previous.as_millis() {
                AgentTimestampMillis::new(boundary)
            } else {
                previous
            }
        }
    }
}

/// The start of the first window a goal-window ceiling opens.
fn initial_window_start(
    window: &AgentBudgetWindow,
    now: AgentTimestampMillis,
) -> AgentTimestampMillis {
    match window {
        // A rolling window is anchored at the first charge.
        AgentBudgetWindow::Rolling { .. } => now,
        AgentBudgetWindow::Calendar { unit } => {
            AgentTimestampMillis::new(calendar_window_start(*unit, now))
        }
    }
}

/// The UTC-aligned start of the calendar window after the one containing
/// `now`.
const fn next_calendar_boundary(unit: AgentCalendarUnit, now: AgentTimestampMillis) -> u64 {
    let days = now.as_millis() / MILLIS_PER_DAY;
    match unit {
        AgentCalendarUnit::Day => (days + 1) * MILLIS_PER_DAY,
        AgentCalendarUnit::Week => {
            let weekday = (days + 3) % 7;
            (days - weekday + 7) * MILLIS_PER_DAY
        }
        AgentCalendarUnit::Month => {
            let (year, month) = civil_year_month(days);
            let (year, month) = if month == 12 {
                (year + 1, 1)
            } else {
                (year, month + 1)
            };
            days_of_month_start(year, month) * MILLIS_PER_DAY
        }
    }
}

/// When the window containing `now` turns, given where it began.
fn next_window_boundary(
    window: &AgentBudgetWindow,
    start: AgentTimestampMillis,
    now: AgentTimestampMillis,
) -> AgentTimestampMillis {
    match window {
        AgentBudgetWindow::Rolling { length_millis } => {
            let elapsed = now.as_millis().saturating_sub(start.as_millis());
            let turns = elapsed / length_millis + 1;
            AgentTimestampMillis::new(
                start
                    .as_millis()
                    .saturating_add(turns.saturating_mul(*length_millis)),
            )
        }
        AgentBudgetWindow::Calendar { unit } => {
            AgentTimestampMillis::new(next_calendar_boundary(*unit, now))
        }
    }
}

/// Truncates a lifecycle reason to its bound on a character boundary.
fn bounded_reason(reason: impl Into<String>) -> String {
    let mut reason = reason.into();
    if reason.len() > AGENT_WAKE_REASON_MAX_LENGTH {
        let mut end = AGENT_WAKE_REASON_MAX_LENGTH;
        while !reason.is_char_boundary(end) {
            end -= 1;
        }
        reason.truncate(end);
    }
    reason
}

/// Merges one re-wake slot toward its desired due time, keeping the parked
/// mark only while the due time is unchanged.
fn merge_rewake_slot(slot: &mut Option<AgentWakeRewake>, desired: Option<AgentTimestampMillis>) {
    *slot = match (slot.take(), desired) {
        (_, None) => None,
        (Some(existing), Some(due_at)) if existing.due_at == due_at => Some(existing),
        (_, Some(due_at)) => Some(AgentWakeRewake {
            due_at,
            parked: false,
            attempt: 0,
        }),
    };
}

/// Longest suspension reason the controller stores.
pub const AGENT_WAKE_REASON_MAX_LENGTH: usize = 256;

/// Cause of one controller-originated re-wake.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentWakeRewakeCause {
    /// A failure backoff elapsing.
    Backoff,
    /// A goal-window turning while a deferred occurrence waits.
    WindowTurn,
}

impl AgentWakeRewakeCause {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Backoff => "backoff",
            Self::WindowTurn => "window-turn",
        }
    }
}

impl Display for AgentWakeRewakeCause {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// One owed or parked controller-originated re-wake
/// ([specification 8.2](../../../docs/plans/rakka-agent/spec.md)): a durable
/// retry the controller schedules for itself, so a parked occurrence is
/// re-attempted at a computed time instead of waiting for an external
/// delivery a quiet schedule never sends.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentWakeRewake {
    /// When the retry becomes due.
    pub due_at: AgentTimestampMillis,
    /// Whether a durable timer entry has been parked for it yet. The
    /// transition that owes the re-wake records it unparked; the settle pass
    /// parks it idempotently and marks it.
    pub parked: bool,
    /// The delivery generation, part of the parked retry's wake identity. A
    /// retry consumed while its cause still holds — delivered before the
    /// controller's own clock reached the due time, as a scanner host with a
    /// faster clock will do — burned its timer entry without doing its work;
    /// bumping the generation re-owes the slot under a fresh identity the
    /// fired entry cannot absorb, so the re-wake stays live under clock skew.
    #[serde(default)]
    pub attempt: u64,
}

/// The controller's two per-cause re-wake slots.
///
/// One slot per cause, because the causes coexist: a window-deferred
/// occurrence and an active backoff each need their own retry time, and one
/// overwriting the other loses the liveness this mechanism exists for.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AgentWakeRewakes {
    /// The failure-backoff retry, when one is owed.
    #[serde(default)]
    pub backoff: Option<AgentWakeRewake>,
    /// The window-turn retry, when one is owed.
    #[serde(default)]
    pub window_turn: Option<AgentWakeRewake>,
}

impl AgentWakeRewakes {
    /// Whether any slot is owed but not yet parked.
    #[must_use]
    pub fn owes_parking(&self) -> bool {
        self.backoff.is_some_and(|slot| !slot.parked)
            || self.window_turn.is_some_and(|slot| !slot.parked)
    }
}

/// Logical lifecycle status of one continuous goal
/// ([specification 8.2](../../../docs/plans/rakka-agent/spec.md)).
///
/// This is runtime status, deliberately separate from residency: a goal in
/// any of these states is fully passivatable.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentGoalLifecycleStatus {
    /// Admitting epochs under its policy.
    #[default]
    Active,
    /// Not admitting; triggers coalesce or drop per the suspension policy
    /// until an authorized resume.
    Suspended,
    /// Past its effective expiry without the renewal its policy required.
    /// Absorbing.
    Expired,
    /// Retired by command or by its retirement policy. Absorbing.
    Retired,
}

impl AgentGoalLifecycleStatus {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Suspended => "suspended",
            Self::Expired => "expired",
            Self::Retired => "retired",
        }
    }

    /// Whether the status admits new epochs.
    #[must_use]
    pub const fn permits_admission(self) -> bool {
        matches!(self, Self::Active)
    }

    /// Whether the status is absorbing.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Expired | Self::Retired)
    }
}

impl Display for AgentGoalLifecycleStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// The durable lifecycle state of one continuous goal's controller
/// ([specification 8.2](../../../docs/plans/rakka-agent/spec.md)).
///
/// The lifecycle revision is monotonic and fences operator commands exactly
/// as the agent entity's lifecycle revision does: statuses recur, so a stale
/// resume replayed after a later suspension is rejected rather than silently
/// lifting it, even after its operation id ages out of the deduplication
/// window.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentGoalLifecycleState {
    #[serde(default)]
    status: AgentGoalLifecycleStatus,
    #[serde(default = "initial_lifecycle_revision")]
    lifecycle_revision: AgentRevisionNumber,
    #[serde(default)]
    changed_by: Option<Box<AgentRevisionProvenance>>,
    #[serde(default)]
    suspended_reason: Option<String>,
    #[serde(default)]
    expires_at_override: Option<AgentTimestampMillis>,
    #[serde(default)]
    consecutive_failures: u32,
    #[serde(default)]
    backoff_until: Option<AgentTimestampMillis>,
    #[serde(default)]
    rewakes: AgentWakeRewakes,
}

fn initial_lifecycle_revision() -> AgentRevisionNumber {
    AgentRevisionNumber::INITIAL
}

impl Default for AgentGoalLifecycleState {
    fn default() -> Self {
        Self {
            status: AgentGoalLifecycleStatus::Active,
            lifecycle_revision: AgentRevisionNumber::INITIAL,
            changed_by: None,
            suspended_reason: None,
            expires_at_override: None,
            consecutive_failures: 0,
            backoff_until: None,
            rewakes: AgentWakeRewakes::default(),
        }
    }
}

impl AgentGoalLifecycleState {
    /// The goal's logical lifecycle status.
    #[must_use]
    pub const fn status(&self) -> AgentGoalLifecycleStatus {
        self.status
    }

    /// The monotonic lifecycle revision operator commands fence on.
    #[must_use]
    pub const fn lifecycle_revision(&self) -> AgentRevisionNumber {
        self.lifecycle_revision
    }

    /// Who accepted the most recent lifecycle transition, when one was
    /// commanded.
    #[must_use]
    pub const fn changed_by(&self) -> Option<&AgentRevisionProvenance> {
        match &self.changed_by {
            Some(provenance) => Some(provenance),
            None => None,
        }
    }

    /// The bounded suspension reason, while suspended.
    #[must_use]
    pub fn suspended_reason(&self) -> Option<&str> {
        self.suspended_reason.as_deref()
    }

    /// Consecutive failed epochs since the last completed one.
    #[must_use]
    pub const fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures
    }

    /// When the failure backoff currently in force elapses.
    #[must_use]
    pub const fn backoff_until(&self) -> Option<AgentTimestampMillis> {
        self.backoff_until
    }

    /// The effective expiry: a renewal's extension, or the policy's own.
    #[must_use]
    pub fn effective_expires_at(
        &self,
        policy: &AgentWakeLifecyclePolicy,
    ) -> Option<AgentTimestampMillis> {
        self.expires_at_override.or(policy.expires_at)
    }

    /// The controller-originated re-wake slots.
    #[must_use]
    pub const fn rewakes(&self) -> &AgentWakeRewakes {
        &self.rewakes
    }
}

/// The terminal class of one epoch's outcome, as the controller accounts it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AgentEpochOutcomeClass {
    /// The epoch produced an accepted result: the failure streak resets.
    Completed,
    /// The epoch failed: the streak grows and backoff engages.
    Failed,
    /// The epoch was cancelled: neither reset nor growth.
    Cancelled,
}

impl AgentEpochOutcomeClass {
    /// Stable kebab-case label of the class.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

/// Monotone wake counters of one continuous goal's controller
/// ([specification 8.2](../../../docs/plans/rakka-agent/spec.md)).
///
/// Every count moves inside the recorded transition that dispositioned the
/// wake, so a replayed delivery never moves one twice.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AgentWakeCounters {
    /// Occurrences admitted into an active slot, directly or after coalescing.
    #[serde(default)]
    pub admitted: u64,
    /// Occurrences durably parked behind an active occurrence.
    #[serde(default)]
    pub coalesced: u64,
    /// Parked occurrences replaced by a later one under the single coalescing
    /// slot.
    #[serde(default)]
    pub superseded: u64,
    /// Occurrences skipped as missed.
    #[serde(default)]
    pub missed: u64,
    /// Occurrences fenced for carrying an obsolete schedule revision.
    #[serde(default)]
    pub fenced: u64,
    /// Active occurrences released by a completed execution.
    #[serde(default)]
    pub released: u64,
    /// Occurrences parked because the goal-window ceiling was exhausted.
    #[serde(default)]
    pub deferred: u64,
    /// Occurrences parked because a failure backoff was in force.
    #[serde(default)]
    pub backed_off: u64,
    /// Occurrences parked while the goal was suspended.
    #[serde(default)]
    pub suspended: u64,
    /// Occurrences dropped while the goal was suspended under the drop
    /// policy.
    #[serde(default)]
    pub dropped: u64,
    /// Occurrences refused because the goal was expired or retired.
    #[serde(default)]
    pub barred: u64,
    /// Controller-originated retries consumed.
    #[serde(default)]
    pub retried: u64,
}

/// How the controller dispositioned one wake delivery
/// ([specification 8.2](../../../docs/plans/rakka-agent/spec.md)).
///
/// Every disposition except [`Self::Duplicate`] is a recorded state
/// transition under the wake's admission operation id; a duplicate makes no
/// transition and answers from what an earlier delivery already recorded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentWakeDisposition {
    /// The occurrence was admitted into an active slot within its admission
    /// window.
    Admitted {
        /// The admitted wake.
        wake: AgentWakeId,
    },
    /// The occurrence was admitted as the coalesced representative — it was
    /// past its direct-admission window or replayed after downtime, and the
    /// controller was free to run it.
    AdmittedCoalesced {
        /// The admitted wake.
        wake: AgentWakeId,
    },
    /// The occurrence was durably parked behind the active occurrence.
    Coalesced {
        /// The parked wake.
        wake: AgentWakeId,
        /// The previously parked wake this one replaced, when the single
        /// coalescing slot was already full.
        replaced: Option<AgentWakeId>,
    },
    /// The occurrence was skipped as missed under the policy in force.
    Skipped {
        /// The skipped wake.
        wake: AgentWakeId,
    },
    /// The occurrence was parked because the goal-window ceiling is
    /// exhausted. It is retried — oldest parked first — by the next delivery
    /// or release whose recorded transition observes a window able to pay,
    /// and the controller owes itself a durable window-turn re-wake so a
    /// quiet schedule cannot strand it.
    Deferred {
        /// The parked wake.
        wake: AgentWakeId,
        /// The dimension whose window ceiling refused the epoch.
        dimension: AgentBudgetDimension,
    },
    /// The occurrence was parked because a failure backoff is in force; the
    /// controller owes itself a durable backoff re-wake to retry it.
    BackedOff {
        /// The parked wake.
        wake: AgentWakeId,
        /// When the backoff elapses.
        until: AgentTimestampMillis,
    },
    /// The occurrence was parked while the goal is suspended; resume may
    /// admit it.
    SuspendedParked {
        /// The parked wake.
        wake: AgentWakeId,
    },
    /// The occurrence was dropped while the goal is suspended under the
    /// drop policy.
    Dropped {
        /// The dropped wake.
        wake: AgentWakeId,
    },
    /// The occurrence was refused because the goal is expired or retired.
    Barred {
        /// The refused wake.
        wake: AgentWakeId,
        /// The absorbing lifecycle status that refused it.
        status: AgentGoalLifecycleStatus,
    },
    /// A controller-originated retry was consumed; whatever it made
    /// admittable was promoted by this same transition.
    Retried {
        /// The retry wake.
        wake: AgentWakeId,
    },
    /// The occurrence carried an obsolete schedule revision and was fenced.
    Fenced {
        /// The fenced wake.
        wake: AgentWakeId,
        /// The obsolete revision the binding carried.
        offered: ScheduleRevision,
        /// The revision currently in force.
        current: ScheduleRevision,
    },
    /// The occurrence was already dispositioned; nothing changed.
    Duplicate {
        /// The already-dispositioned wake.
        wake: AgentWakeId,
    },
}

impl AgentWakeDisposition {
    /// Stable kebab-case label of the disposition.
    #[must_use]
    pub const fn as_label(&self) -> &'static str {
        match self {
            Self::Admitted { .. } => "admitted",
            Self::AdmittedCoalesced { .. } => "admitted-coalesced",
            Self::Coalesced { .. } => "coalesced",
            Self::Skipped { .. } => "skipped",
            Self::Deferred { .. } => "deferred",
            Self::BackedOff { .. } => "backed-off",
            Self::SuspendedParked { .. } => "suspended-parked",
            Self::Dropped { .. } => "dropped",
            Self::Barred { .. } => "barred",
            Self::Retried { .. } => "retried",
            Self::Fenced { .. } => "fenced",
            Self::Duplicate { .. } => "duplicate",
        }
    }

    /// The wake the disposition is about.
    #[must_use]
    pub const fn wake_id(&self) -> &AgentWakeId {
        match self {
            Self::Admitted { wake }
            | Self::AdmittedCoalesced { wake }
            | Self::Coalesced { wake, .. }
            | Self::Skipped { wake }
            | Self::Deferred { wake, .. }
            | Self::BackedOff { wake, .. }
            | Self::SuspendedParked { wake }
            | Self::Dropped { wake }
            | Self::Barred { wake, .. }
            | Self::Retried { wake }
            | Self::Fenced { wake, .. }
            | Self::Duplicate { wake } => wake,
        }
    }

    /// Whether the disposition admitted the occurrence into an active slot.
    #[must_use]
    pub const fn is_admission(&self) -> bool {
        matches!(self, Self::Admitted { .. } | Self::AdmittedCoalesced { .. })
    }
}

impl Display for AgentWakeDisposition {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// What releasing an active occurrence changed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentWakeRelease {
    /// The released wake.
    pub released: AgentWakeId,
    /// The parked wake promoted into the freed slot, when one was waiting.
    pub admitted_next: Option<AgentWakeId>,
    /// The released occurrence's epoch, when one was attached. Records
    /// persisted before this field load without one.
    #[serde(default)]
    pub epoch: Option<AgentEpochRef>,
}

/// Where a parked binding landed — the counter-neutral result of
/// [`AgentWakeControllerState::park_binding`].
enum AgentWakeParked {
    /// Stored in the pending queue, possibly replacing the previous occupant
    /// of the single coalescing slot.
    Stored {
        /// The wake the new occupant replaced, when the slot was full.
        replaced: Option<AgentWakeId>,
    },
    /// The bounded catch-up queue was full; the occurrence was not kept.
    Overflow,
}

/// What one wake transition of the controller recorded.
///
/// This rides on the task outcome the operation log remembers, so a replayed
/// wake command answers with exactly what its original application decided.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentWakeOutcome {
    /// How an admission command's delivery was dispositioned.
    Disposition(AgentWakeDisposition),
    /// What releasing an active occurrence changed.
    Release(AgentWakeRelease),
    /// A schedule update took force.
    ScheduleUpdated {
        /// The schedule revision now in force.
        schedule_revision: ScheduleRevision,
        /// The wake-policy revision now in force.
        policy_revision: AgentRevisionNumber,
        /// How many parked occurrences the update fenced.
        fenced: u64,
    },
    /// A lifecycle transition took force.
    Lifecycle {
        /// The lifecycle status now in force.
        status: AgentGoalLifecycleStatus,
        /// The lifecycle revision now in force.
        lifecycle_revision: AgentRevisionNumber,
    },
}

/// A bounded, credential-free view of one continuous goal's wake state,
/// exposed through the task snapshot
/// ([specification 17.18](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentWakeStatusView {
    /// The schedule revision in force.
    pub schedule_revision: ScheduleRevision,
    /// The wake-policy revision in force.
    pub policy_revision: AgentRevisionNumber,
    /// The wakes currently owning execution.
    pub active: Vec<AgentWakeId>,
    /// The wakes durably parked behind them, oldest first.
    pub pending: Vec<AgentWakeId>,
    /// The most recently admitted wake.
    pub last_admitted: Option<AgentWakeId>,
    /// When the most recent admission happened.
    pub last_admitted_at: Option<AgentTimestampMillis>,
    /// The monotone wake counters.
    pub counters: AgentWakeCounters,
    /// The goal-window ledger in force, once a windowed admission opened one.
    /// Views persisted before this field load without it.
    #[serde(default)]
    pub window: Option<AgentWakeWindowLedger>,
    /// The goal's lifecycle state: status, revision, failure streak, backoff,
    /// and the owed or parked controller-originated re-wakes. Views persisted
    /// before this field load without it.
    #[serde(default)]
    pub lifecycle: Option<AgentGoalLifecycleState>,
    /// The finite child epochs the active occurrences execute as, once
    /// attached. Views persisted before this field load with none.
    #[serde(default)]
    pub epochs: Vec<AgentEpochRef>,
}

/// Durable controller state of one continuous goal's wakes
/// ([specification 8.2](../../../docs/plans/rakka-agent/spec.md)).
///
/// This is the slice of the root control task's state the wake controller
/// owns: which occurrences are active, which are durably parked, and the
/// monotone counters. It is deliberately small — everything in it is bounded
/// by a constant, because it lives inside the bounded task state and survives
/// every passivation, restart, and shard movement.
///
/// Deduplication is layered. The operation log answers a replay of a recent
/// admission command; beneath it, this state fences on the active and parked
/// slots, a bounded ring of recently dispositioned wake identities, and — for
/// scheduled occurrences, which arrive in due order — a monotone due-time
/// watermark. A scheduled occurrence at or below the watermark that is no
/// longer in any slot was already dispositioned and answers as a duplicate.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct AgentWakeControllerState {
    active: Vec<AgentActiveWake>,
    pending: Vec<AgentWakeBinding>,
    recent: Vec<AgentWakeId>,
    scheduled_watermark: Option<AgentTimestampMillis>,
    last_admitted: Option<AgentWakeId>,
    last_admitted_at: Option<AgentTimestampMillis>,
    counters: AgentWakeCounters,
    window: Option<AgentWakeWindowLedger>,
    lifecycle: AgentGoalLifecycleState,
}

impl AgentWakeControllerState {
    /// Creates an empty controller state.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The occurrences currently owning execution.
    #[must_use]
    pub fn active(&self) -> &[AgentActiveWake] {
        &self.active
    }

    /// The occurrences durably parked behind the active ones, oldest first.
    #[must_use]
    pub fn pending(&self) -> &[AgentWakeBinding] {
        &self.pending
    }

    /// The most recently admitted wake, if any occurrence was ever admitted.
    #[must_use]
    pub const fn last_admitted(&self) -> Option<&AgentWakeId> {
        self.last_admitted.as_ref()
    }

    /// When the most recent admission happened.
    #[must_use]
    pub const fn last_admitted_at(&self) -> Option<AgentTimestampMillis> {
        self.last_admitted_at
    }

    /// The monotone wake counters.
    #[must_use]
    pub const fn counters(&self) -> &AgentWakeCounters {
        &self.counters
    }

    /// The goal-window ledger, once a windowed admission opened one.
    #[must_use]
    pub const fn window(&self) -> Option<&AgentWakeWindowLedger> {
        self.window.as_ref()
    }

    /// The goal's durable lifecycle state.
    #[must_use]
    pub const fn lifecycle(&self) -> &AgentGoalLifecycleState {
        &self.lifecycle
    }

    /// The deterministic backoff delay after `failures` consecutive failures.
    #[must_use]
    pub fn backoff_delay_millis(policy: &AgentWakeBackoffPolicy, failures: u32) -> u64 {
        let mut delay = policy.initial_millis;
        let mut step = 1;
        while step < failures {
            delay = delay.saturating_mul(u64::from(policy.multiplier_percent)) / 100;
            if delay >= policy.max_millis {
                return policy.max_millis;
            }
            step += 1;
        }
        delay.min(policy.max_millis)
    }

    /// Observes the lifecycle facts logical time has made true — expiry, a
    /// timed retirement, an occurrence-count retirement — returning the new
    /// status when one was crossed.
    ///
    /// This is the window-refill shape: whatever recorded transition first
    /// observes the crossing takes it durably; restarts, activations, and
    /// shard movement never do. Absorbing statuses stay absorbed.
    pub fn observe_lifecycle(
        &mut self,
        policy: &AgentWakePolicy,
        now: AgentTimestampMillis,
    ) -> Option<AgentGoalLifecycleStatus> {
        if self.lifecycle.status.is_terminal() {
            return None;
        }
        let retired = match policy.lifecycle.retirement {
            AgentWakeRetirementPolicy::Manual => false,
            AgentWakeRetirementPolicy::AfterOccurrences { occurrences } => {
                self.counters.admitted >= occurrences
            }
            AgentWakeRetirementPolicy::At { at } => now.as_millis() >= at.as_millis(),
        };
        if retired {
            self.lifecycle.status = AgentGoalLifecycleStatus::Retired;
            self.lifecycle.lifecycle_revision = self.lifecycle.lifecycle_revision.next();
            self.lifecycle.rewakes = AgentWakeRewakes::default();
            return Some(AgentGoalLifecycleStatus::Retired);
        }
        if let Some(expires_at) = self.lifecycle.effective_expires_at(&policy.lifecycle) {
            if now.as_millis() >= expires_at.as_millis() {
                self.lifecycle.status = AgentGoalLifecycleStatus::Expired;
                self.lifecycle.lifecycle_revision = self.lifecycle.lifecycle_revision.next();
                self.lifecycle.rewakes = AgentWakeRewakes::default();
                return Some(AgentGoalLifecycleStatus::Expired);
            }
        }
        None
    }

    fn fence_lifecycle_revision(&self, expected: AgentRevisionNumber) -> AgentWakeResult<()> {
        if self.lifecycle.status.is_terminal() {
            return Err(AgentWakeError::LifecycleTerminal {
                status: self.lifecycle.status,
            });
        }
        if expected != self.lifecycle.lifecycle_revision {
            return Err(AgentWakeError::StaleLifecycleRevision {
                expected,
                current: self.lifecycle.lifecycle_revision,
            });
        }
        Ok(())
    }

    /// Suspends the goal under an operator's authority.
    pub fn suspend(
        &mut self,
        expected: AgentRevisionNumber,
        reason: Option<String>,
        provenance: AgentRevisionProvenance,
    ) -> AgentWakeResult<AgentRevisionNumber> {
        self.fence_lifecycle_revision(expected)?;
        self.lifecycle.status = AgentGoalLifecycleStatus::Suspended;
        self.lifecycle.suspended_reason = reason.map(bounded_reason);
        self.lifecycle.changed_by = Some(Box::new(provenance));
        self.lifecycle.lifecycle_revision = self.lifecycle.lifecycle_revision.next();
        self.lifecycle.rewakes = AgentWakeRewakes::default();
        Ok(self.lifecycle.lifecycle_revision)
    }

    /// Resumes a suspended goal.
    ///
    /// Resume clears the failure backoff and its streak — the operator said
    /// "try again" — and the entity's resume transition promotes whatever the
    /// suspension parked, owing its epoch in the same compare-and-set.
    pub fn resume(
        &mut self,
        expected: AgentRevisionNumber,
        provenance: AgentRevisionProvenance,
    ) -> AgentWakeResult<AgentRevisionNumber> {
        self.fence_lifecycle_revision(expected)?;
        if self.lifecycle.status != AgentGoalLifecycleStatus::Suspended {
            return Err(AgentWakeError::NotSuspended {
                status: self.lifecycle.status,
            });
        }
        self.lifecycle.status = AgentGoalLifecycleStatus::Active;
        self.lifecycle.suspended_reason = None;
        self.lifecycle.consecutive_failures = 0;
        self.lifecycle.backoff_until = None;
        self.lifecycle.changed_by = Some(Box::new(provenance));
        self.lifecycle.lifecycle_revision = self.lifecycle.lifecycle_revision.next();
        Ok(self.lifecycle.lifecycle_revision)
    }

    /// Extends the goal's effective expiry.
    ///
    /// Under a required renewal, the extension must arrive inside the window
    /// before the effective expiry; under no requirement it is a plain
    /// extension accepted any time before expiry. The new expiry must
    /// strictly extend the effective one.
    pub fn renew(
        &mut self,
        expected: AgentRevisionNumber,
        policy: &AgentWakePolicy,
        new_expires_at: AgentTimestampMillis,
        provenance: AgentRevisionProvenance,
        now: AgentTimestampMillis,
    ) -> AgentWakeResult<AgentRevisionNumber> {
        self.fence_lifecycle_revision(expected)?;
        let Some(effective) = self.lifecycle.effective_expires_at(&policy.lifecycle) else {
            return Err(AgentWakeError::RenewalWithoutExpiry);
        };
        if new_expires_at.as_millis() <= effective.as_millis() {
            return Err(AgentWakeError::RenewalNotExtending {
                offered: new_expires_at,
                effective,
            });
        }
        if let AgentWakeRenewalPolicy::RequiredBefore { window_millis } = policy.lifecycle.renewal {
            let opens = effective.as_millis().saturating_sub(window_millis);
            if now.as_millis() < opens || now.as_millis() >= effective.as_millis() {
                return Err(AgentWakeError::RenewalOutsideWindow {
                    opens: AgentTimestampMillis::new(opens),
                    effective,
                });
            }
        }
        self.lifecycle.expires_at_override = Some(new_expires_at);
        self.lifecycle.changed_by = Some(Box::new(provenance));
        self.lifecycle.lifecycle_revision = self.lifecycle.lifecycle_revision.next();
        Ok(self.lifecycle.lifecycle_revision)
    }

    /// Retires the goal under an operator's authority. Absorbing.
    pub fn retire(
        &mut self,
        expected: AgentRevisionNumber,
        provenance: AgentRevisionProvenance,
    ) -> AgentWakeResult<AgentRevisionNumber> {
        self.fence_lifecycle_revision(expected)?;
        self.lifecycle.status = AgentGoalLifecycleStatus::Retired;
        self.lifecycle.changed_by = Some(Box::new(provenance));
        self.lifecycle.lifecycle_revision = self.lifecycle.lifecycle_revision.next();
        self.lifecycle.rewakes = AgentWakeRewakes::default();
        Ok(self.lifecycle.lifecycle_revision)
    }

    /// Accounts one epoch's terminal outcome: a completion resets the failure
    /// streak, a failure grows it and engages backoff, a cancellation does
    /// neither. Returns whether the failure escalated into an auto-suspend.
    pub fn record_epoch_outcome(
        &mut self,
        policy: &AgentWakePolicy,
        outcome: AgentEpochOutcomeClass,
        now: AgentTimestampMillis,
    ) -> bool {
        match outcome {
            AgentEpochOutcomeClass::Completed => {
                self.lifecycle.consecutive_failures = 0;
                self.lifecycle.backoff_until = None;
                false
            }
            AgentEpochOutcomeClass::Cancelled => false,
            AgentEpochOutcomeClass::Failed => {
                self.lifecycle.consecutive_failures =
                    self.lifecycle.consecutive_failures.saturating_add(1);
                let delay = Self::backoff_delay_millis(
                    &policy.failure_backoff,
                    self.lifecycle.consecutive_failures,
                );
                self.lifecycle.backoff_until = Some(AgentTimestampMillis::new(
                    now.as_millis().saturating_add(delay),
                ));
                let escalated = policy
                    .failure_backoff
                    .escalate_after_failures
                    .is_some_and(|threshold| self.lifecycle.consecutive_failures >= threshold)
                    && self.lifecycle.status == AgentGoalLifecycleStatus::Active;
                if escalated {
                    // Escalation is a durable suspension the operator must
                    // resume: backing off further would only defer the same
                    // failure again.
                    self.lifecycle.status = AgentGoalLifecycleStatus::Suspended;
                    self.lifecycle.suspended_reason = Some(bounded_reason(format!(
                        "escalated after {} consecutive epoch failures",
                        self.lifecycle.consecutive_failures
                    )));
                    self.lifecycle.lifecycle_revision = self.lifecycle.lifecycle_revision.next();
                    self.lifecycle.rewakes = AgentWakeRewakes::default();
                }
                escalated
            }
        }
    }

    /// Whether the goal window could pay for one epoch right now, without
    /// charging it — the read-only probe [`Self::ensure_rewakes`] plans by.
    fn window_can_pay(
        &self,
        ceiling: &AgentGoalWindowCeiling,
        epoch_budget: &AgentBudgetAllocation,
        now: AgentTimestampMillis,
    ) -> bool {
        let start = match self.window {
            Some(ledger) => advance_window_start(&ceiling.window, ledger.window_start(), now),
            None => initial_window_start(&ceiling.window, now),
        };
        let consumed = match self.window {
            Some(ledger) if ledger.window_start() == start => *ledger.consumed(),
            _ => AgentBudgetConsumption::zero(),
        };
        for dimension in AgentBudgetDimension::CONSERVED {
            if let Some(limit) = ceiling.ceiling.get(dimension) {
                let Some(requested) = epoch_budget.get(dimension) else {
                    return false;
                };
                if consumed.get(dimension).saturating_add(requested) > limit {
                    return false;
                }
            }
        }
        true
    }

    /// Whether the failure backoff is in force at this logical time.
    fn backoff_in_force(&self, now: AgentTimestampMillis) -> bool {
        self.lifecycle
            .backoff_until
            .is_some_and(|until| now.as_millis() < until.as_millis())
    }

    /// Recomputes the controller's owed re-wakes from what is true now.
    ///
    /// Run at the end of every mutating entry point, this is idempotent and
    /// self-healing: a slot is owed exactly when a parked occurrence needs a
    /// retry no external delivery is promised to provide — the backoff
    /// elapsing, or the goal window turning — and cleared whenever the goal
    /// cannot admit at all. A slot whose due time is unchanged keeps its
    /// parked mark; one recomputed to a new time is owed for parking again.
    pub fn ensure_rewakes(&mut self, policy: &AgentWakePolicy, now: AgentTimestampMillis) {
        if !self.lifecycle.status.permits_admission() {
            self.lifecycle.rewakes = AgentWakeRewakes::default();
            return;
        }
        let desired_backoff = (self.backoff_in_force(now) && !self.pending.is_empty())
            .then(|| self.lifecycle.backoff_until.expect("backoff is in force"));
        let desired_window_turn = policy.goal_window.as_ref().and_then(|ceiling| {
            let waiting = !self.pending.is_empty()
                && !self.window_can_pay(ceiling, &policy.epoch_budget, now);
            waiting.then(|| {
                let start = match self.window {
                    Some(ledger) => ledger.window_start(),
                    None => initial_window_start(&ceiling.window, now),
                };
                next_window_boundary(&ceiling.window, start, now)
            })
        });
        merge_rewake_slot(&mut self.lifecycle.rewakes.backoff, desired_backoff);
        merge_rewake_slot(&mut self.lifecycle.rewakes.window_turn, desired_window_turn);
    }

    /// Marks one re-wake slot parked, once the settle pass has durably parked
    /// its timer entry. A mark for a stale generation — the slot re-armed
    /// under a later attempt since the entry was parked — is a no-op, so the
    /// re-owed slot stays owed.
    pub fn mark_rewake_parked(
        &mut self,
        cause: AgentWakeRewakeCause,
        due_at: AgentTimestampMillis,
        attempt: u64,
    ) {
        let slot = match cause {
            AgentWakeRewakeCause::Backoff => &mut self.lifecycle.rewakes.backoff,
            AgentWakeRewakeCause::WindowTurn => &mut self.lifecycle.rewakes.window_turn,
        };
        if let Some(rewake) = slot {
            if rewake.due_at == due_at && rewake.attempt == attempt {
                rewake.parked = true;
            }
        }
    }

    /// Charges one epoch's allocation against the goal-window ceiling,
    /// refilling the window first when the admission's logical time crossed
    /// its boundary.
    ///
    /// The refill persists even when the charge is then refused: crossing the
    /// boundary is a logical-time fact, and recording it inside the same
    /// transition that observed it is exactly what keeps refill independent
    /// of restarts. An epoch dimension the policy leaves unbounded cannot be
    /// charged against a bounded ceiling and is refused closed — the policy
    /// constructor already rejects that combination.
    fn charge_goal_window(
        &mut self,
        ceiling: &AgentGoalWindowCeiling,
        epoch_budget: &AgentBudgetAllocation,
        now: AgentTimestampMillis,
    ) -> Result<(), AgentBudgetDimension> {
        let start = match self.window {
            Some(ledger) => advance_window_start(&ceiling.window, ledger.window_start(), now),
            None => initial_window_start(&ceiling.window, now),
        };
        if self
            .window
            .is_none_or(|ledger| ledger.window_start() != start)
        {
            self.window = Some(AgentWakeWindowLedger {
                window_start: start,
                consumed: AgentBudgetConsumption::zero(),
            });
        }
        let mut ledger = self.window.expect("the window was just ensured");
        for dimension in AgentBudgetDimension::CONSERVED {
            if let Some(limit) = ceiling.ceiling.get(dimension) {
                let Some(requested) = epoch_budget.get(dimension) else {
                    return Err(dimension);
                };
                if ledger.consumed.get(dimension).saturating_add(requested) > limit {
                    return Err(dimension);
                }
            }
        }
        for dimension in AgentBudgetDimension::CONSERVED {
            if ceiling.ceiling.get(dimension).is_some() {
                if let Some(requested) = epoch_budget.get(dimension) {
                    ledger.consumed.add(dimension, requested);
                }
            }
        }
        self.window = Some(ledger);
        Ok(())
    }

    /// Admits the binding when the goal window can pay for its epoch, and
    /// parks it as deferred otherwise.
    fn admit_or_defer(
        &mut self,
        policy: &AgentWakePolicy,
        binding: AgentWakeBinding,
        now: AgentTimestampMillis,
        coalesced: bool,
        representative: bool,
    ) -> AgentWakeDisposition {
        if let Some(ceiling) = &policy.goal_window {
            if let Err(dimension) = self.charge_goal_window(ceiling, &policy.epoch_budget, now) {
                let wake = binding.wake_id().clone();
                return match self.park_binding(policy, binding) {
                    AgentWakeParked::Stored { replaced } => {
                        // A deferral is one delivery and moves one primary
                        // counter; a replaced occupant is the secondary fact.
                        self.counters.deferred += 1;
                        if replaced.is_some() {
                            self.counters.superseded += 1;
                        }
                        AgentWakeDisposition::Deferred { wake, dimension }
                    }
                    // A full catch-up queue skips the overflow even when the
                    // proximate cause was the window.
                    AgentWakeParked::Overflow => {
                        self.counters.missed += 1;
                        AgentWakeDisposition::Skipped { wake }
                    }
                };
            }
        }
        self.admit_binding(binding, now, coalesced, representative)
    }

    /// Whether the controller already holds or recently dispositioned a wake.
    #[must_use]
    pub fn contains(&self, wake: &AgentWakeId) -> bool {
        self.active.iter().any(|a| a.binding.wake_id() == wake)
            || self.pending.iter().any(|b| b.wake_id() == wake)
            || self.recent.iter().any(|seen| seen == wake)
            || self.last_admitted.as_ref() == Some(wake)
    }

    fn active_capacity(policy: &AgentWakePolicy) -> usize {
        match &policy.overlap {
            AgentWakeOverlapPolicy::ForbidAndCoalesce => 1,
            AgentWakeOverlapPolicy::Parallel {
                max_concurrent_epochs,
                ..
            } => (*max_concurrent_epochs as usize).min(AGENT_WAKE_ACTIVE_CAPACITY),
        }
    }

    fn pending_capacity(policy: &AgentWakePolicy) -> usize {
        match policy.missed_occurrence {
            AgentMissedOccurrencePolicy::BoundedCatchUp { max_occurrences } => {
                (max_occurrences as usize).min(AGENT_WAKE_PENDING_CAPACITY)
            }
            _ => 1,
        }
    }

    /// Whether the occurrence is past the policy's maximum lateness at this
    /// logical time — the band the missed-occurrence policy owns.
    fn past_maximum_lateness(
        policy: &AgentWakePolicy,
        binding: &AgentWakeBinding,
        now: AgentTimestampMillis,
    ) -> bool {
        matches!(
            (
                binding
                    .due_at()
                    .map(|due_at| now.as_millis().saturating_sub(due_at.as_millis())),
                policy.maximum_lateness_millis,
            ),
            (Some(late), Some(maximum)) if late > maximum
        )
    }

    /// Dispositions one wake delivery under the policy and schedule revision
    /// in force.
    ///
    /// This is the deterministic admission decision of
    /// [specification 8.2](../../../docs/plans/rakka-agent/spec.md): fence an
    /// obsolete revision, answer a duplicate without a transition, and place
    /// everything else by its time band — within the admission window it is
    /// fresh; between the window and the maximum lateness it is what the
    /// overlap policy durably coalesces; past the maximum lateness the
    /// missed-occurrence policy decides. A binding whose revision is *ahead*
    /// of the controller fails closed: no schedule the controller accepted
    /// ever issued it.
    ///
    /// The entity's admission transition runs [`Self::promote_admittable`]
    /// before this, so an occurrence parked on an exhausted window takes a
    /// free slot ahead of the fresh delivery once the window refills.
    pub fn admit(
        &mut self,
        policy: &AgentWakePolicy,
        current_revision: ScheduleRevision,
        binding: AgentWakeBinding,
        now: AgentTimestampMillis,
    ) -> AgentWakeResult<AgentWakeDisposition> {
        let offered = binding.schedule_revision();
        if offered > current_revision {
            return Err(AgentWakeError::RevisionAhead {
                offered,
                current: current_revision,
            });
        }
        // A retry must arrive as the controller and only a retry may: the
        // policy's trigger set governs the outside world, and the outside
        // world cannot smuggle an occurrence in as the controller — nor a
        // retry in through a declared trigger.
        let retry = matches!(binding.occurrence(), AgentWakeOccurrence::Retry { .. });
        let controller_trigger = binding.trigger() == AgentWakeTriggerKind::Controller;
        if retry != controller_trigger {
            return Err(AgentWakeError::RetryTriggerMismatch {
                trigger: binding.trigger(),
            });
        }
        if !controller_trigger && !policy.allows_trigger(binding.trigger()) {
            return Err(AgentWakeError::TriggerNotAllowed {
                trigger: binding.trigger(),
            });
        }
        let wake = binding.wake_id().clone();
        if self.contains(&wake) {
            return Ok(AgentWakeDisposition::Duplicate { wake });
        }
        // The fence runs before the watermark: an obsolete occurrence is
        // fenced whatever its due time, and its due time never advances the
        // watermark — the watermark orders the *current* schedule's
        // occurrences only, so a future-dated obsolete straggler cannot
        // swallow the occurrences the new schedule legitimately issues.
        if offered < current_revision {
            self.note_seen(&binding);
            self.counters.fenced += 1;
            return Ok(AgentWakeDisposition::Fenced {
                wake,
                offered,
                current: current_revision,
            });
        }
        // A retry's whole work — promoting whatever is admittable — was
        // already done by the entity's pre-admission promotion pass, so its
        // own arm only consumes it: into the recent ring for duplicate-scan
        // dedup, counted, never near a time band or an active slot. A consume
        // that matches the current slot burned that generation's timer entry;
        // re-arming the slot under the next attempt keeps the re-wake live
        // when the cause still holds — a scanner clock ahead of this host's
        // delivers the retry before the backoff elapses or the window turns
        // here, and without the bump the parked occurrences would strand
        // behind a slot marked parked whose only entry is terminal. When the
        // pre-admit promotion did the work instead, `ensure_rewakes` clears
        // the slot and the bump is moot.
        if let AgentWakeOccurrence::Retry {
            due_at,
            cause,
            attempt,
        } = *binding.occurrence()
        {
            self.note_seen(&binding);
            self.counters.retried += 1;
            let slot = match cause {
                AgentWakeRewakeCause::Backoff => &mut self.lifecycle.rewakes.backoff,
                AgentWakeRewakeCause::WindowTurn => &mut self.lifecycle.rewakes.window_turn,
            };
            if let Some(rewake) = slot {
                if rewake.due_at == due_at && rewake.attempt == attempt {
                    rewake.attempt = rewake.attempt.saturating_add(1);
                    rewake.parked = false;
                }
            }
            return Ok(AgentWakeDisposition::Retried { wake });
        }
        // The watermark orders *scheduled* occurrences only: they arrive in
        // due order, so at-or-below-watermark is a redelivery. No other
        // occurrence kind carries that ordering contract, and none may be
        // swallowed by it.
        if let AgentWakeOccurrence::Scheduled { due_at } = binding.occurrence() {
            if self
                .scheduled_watermark
                .is_some_and(|watermark| due_at.as_millis() <= watermark.as_millis())
            {
                return Ok(AgentWakeDisposition::Duplicate { wake });
            }
        }
        // The lifecycle gate: an absorbing goal bars the delivery — recorded,
        // so the scanner marks the entry terminal — and a suspended one
        // dispositions it per the suspension policy.
        match self.lifecycle.status {
            AgentGoalLifecycleStatus::Active => {}
            AgentGoalLifecycleStatus::Suspended => {
                return Ok(match policy.lifecycle.while_suspended {
                    AgentWakeSuspensionPolicy::CoalesceLatest => {
                        match self.park_binding(policy, binding) {
                            AgentWakeParked::Stored { replaced } => {
                                self.counters.suspended += 1;
                                if replaced.is_some() {
                                    self.counters.superseded += 1;
                                }
                                AgentWakeDisposition::SuspendedParked { wake }
                            }
                            AgentWakeParked::Overflow => {
                                self.counters.missed += 1;
                                AgentWakeDisposition::Skipped { wake }
                            }
                        }
                    }
                    AgentWakeSuspensionPolicy::Drop => {
                        self.note_consumed(&binding);
                        self.counters.dropped += 1;
                        AgentWakeDisposition::Dropped { wake }
                    }
                });
            }
            status @ (AgentGoalLifecycleStatus::Expired | AgentGoalLifecycleStatus::Retired) => {
                self.note_seen(&binding);
                self.counters.barred += 1;
                return Ok(AgentWakeDisposition::Barred { wake, status });
            }
        }
        // The backoff gate: while a failure backoff is in force, every
        // delivery parks — a fresh occurrence starting an epoch mid-backoff
        // would make the backoff mean nothing.
        if self.backoff_in_force(now) {
            let until = self.lifecycle.backoff_until.expect("backoff is in force");
            return Ok(match self.park_binding(policy, binding) {
                AgentWakeParked::Stored { replaced } => {
                    self.counters.backed_off += 1;
                    if replaced.is_some() {
                        self.counters.superseded += 1;
                    }
                    AgentWakeDisposition::BackedOff { wake, until }
                }
                AgentWakeParked::Overflow => {
                    self.counters.missed += 1;
                    AgentWakeDisposition::Skipped { wake }
                }
            });
        }
        if Self::past_maximum_lateness(policy, &binding, now) {
            return Ok(self.dispose_missed(policy, binding, now));
        }
        let lateness = binding
            .due_at()
            .map(|due_at| now.as_millis().saturating_sub(due_at.as_millis()));
        let past_window = matches!(
            (lateness, policy.admission_window_millis),
            (Some(late), Some(window)) if late > window
        );
        if past_window {
            // The band between the admission window and the maximum lateness:
            // past direct admission but not yet missed, so it coalesces — into
            // a free active slot when one exists, behind the active occurrence
            // otherwise.
            return Ok(self.coalesce_or_admit(policy, binding, now));
        }
        if self.active.len() < Self::active_capacity(policy) {
            Ok(self.admit_or_defer(policy, binding, now, false, false))
        } else {
            Ok(self.coalesce(policy, binding))
        }
    }

    /// Releases an active occurrence, promoting the oldest parked occurrence
    /// into the freed slot when the goal window can pay for its epoch.
    ///
    /// The promotion happens inside this same transition: the coalesced
    /// occurrence's epoch follows the released one without any further
    /// trigger, which is what keeps the default overlap policy live. A parked
    /// occurrence the window cannot pay for stays parked; the next release or
    /// admission retries it after the refill its logical time earns.
    pub fn release(
        &mut self,
        policy: &AgentWakePolicy,
        wake: &AgentWakeId,
        now: AgentTimestampMillis,
    ) -> AgentWakeResult<AgentWakeRelease> {
        let Some(index) = self
            .active
            .iter()
            .position(|active| active.binding.wake_id() == wake)
        else {
            return Err(AgentWakeError::NotActive { wake: wake.clone() });
        };
        let released = self.active.remove(index);
        self.counters.released += 1;
        let admitted_next = self.promote_admittable(policy, now);
        Ok(AgentWakeRelease {
            released: wake.clone(),
            admitted_next,
            epoch: released.epoch,
        })
    }

    /// Promotes the oldest parked occurrence into a free active slot when the
    /// goal window can pay for its epoch, returning the promoted wake.
    ///
    /// A release runs this for the slot it freed, and the entity's admission
    /// transition runs it *before* dispositioning a fresh delivery — so an
    /// occurrence that deferred on an exhausted window is retried oldest
    /// first by the next transition that observes the refilled window,
    /// rather than being leapfrogged by every fresher occurrence. The caller
    /// owes the promoted occurrence's epoch creation in the same
    /// compare-and-set, exactly as it does for a direct admission.
    pub fn promote_admittable(
        &mut self,
        policy: &AgentWakePolicy,
        now: AgentTimestampMillis,
    ) -> Option<AgentWakeId> {
        // The same lifecycle and backoff gates every admission passes: a
        // suspended, expired, retired, or backing-off goal promotes nothing,
        // through every path that promotes — release, resume, retry, and the
        // pre-admission promotion alike.
        if !self.lifecycle.status.permits_admission() || self.backoff_in_force(now) {
            return None;
        }
        if self.pending.is_empty() || self.active.len() >= Self::active_capacity(policy) {
            return None;
        }
        if let Some(ceiling) = &policy.goal_window {
            if self
                .charge_goal_window(ceiling, &policy.epoch_budget, now)
                .is_err()
            {
                return None;
            }
        }
        let binding = self.pending.remove(0);
        let wake = binding.wake_id().clone();
        // A parked binding carries no representative mark, so the mark is
        // recomputed from what it means: under admit-one-coalesced, an
        // occurrence promoted past its maximum lateness stands for its
        // downtime backlog, and later missed occurrences of that backlog
        // must absorb into it rather than park an echo that would admit a
        // second epoch.
        let representative = matches!(
            policy.missed_occurrence,
            AgentMissedOccurrencePolicy::AdmitOneCoalesced
        ) && Self::past_maximum_lateness(policy, &binding, now);
        self.admit_binding(binding, now, true, representative);
        Some(wake)
    }

    /// Attaches the finite child epoch an admitting transition created to its
    /// active occurrence.
    ///
    /// The attachment happens inside the same durable transition as the
    /// admission and the owed epoch-creation exchange, so the controller can
    /// never durably hold an admitted occurrence while having forgotten which
    /// epoch it created.
    pub fn attach_epoch(
        &mut self,
        wake: &AgentWakeId,
        epoch: AgentEpochRef,
    ) -> AgentWakeResult<()> {
        let Some(active) = self
            .active
            .iter_mut()
            .find(|active| active.binding.wake_id() == wake)
        else {
            return Err(AgentWakeError::NotActive { wake: wake.clone() });
        };
        active.epoch = Some(epoch);
        Ok(())
    }

    /// Takes a schedule update into the controller: fences every parked
    /// occurrence constructed under an older revision and resets the
    /// scheduled-due-time watermark, returning how many were fenced.
    ///
    /// A schedule update calls this so an occurrence the old schedule parked
    /// can never admit an epoch the new schedule did not issue, and so the
    /// new schedule starts a fresh due-time sequence — it may legitimately
    /// issue occurrences due at or below whatever the old schedule reached.
    /// Active occurrences are untouched: they were already admitted.
    pub fn apply_schedule_update(&mut self, current_revision: ScheduleRevision) -> u64 {
        let before = self.pending.len();
        self.pending
            .retain(|binding| binding.schedule_revision() >= current_revision);
        let fenced = (before - self.pending.len()) as u64;
        self.counters.fenced += fenced;
        self.scheduled_watermark = None;
        fenced
    }

    fn dispose_missed(
        &mut self,
        policy: &AgentWakePolicy,
        binding: AgentWakeBinding,
        now: AgentTimestampMillis,
    ) -> AgentWakeDisposition {
        match policy.missed_occurrence {
            AgentMissedOccurrencePolicy::AdmitOneCoalesced => {
                if self.active.len() < Self::active_capacity(policy) {
                    self.admit_or_defer(policy, binding, now, true, true)
                } else if self.active.iter().any(AgentActiveWake::is_representative) {
                    // The active occurrence is already this downtime backlog's
                    // coalesced representative: later missed occurrences of
                    // the backlog are absorbed by it, so one downtime yields
                    // exactly one epoch rather than a representative plus an
                    // echo ([specification 21.1](../../../docs/plans/rakka-agent/spec.md)
                    // item 2).
                    let wake = binding.wake_id().clone();
                    self.note_consumed(&binding);
                    self.counters.missed += 1;
                    AgentWakeDisposition::Skipped { wake }
                } else {
                    // A normal epoch is running: the backlog parks exactly one
                    // representative behind it, admitted when it releases.
                    self.coalesce(policy, binding)
                }
            }
            AgentMissedOccurrencePolicy::Skip => {
                let wake = binding.wake_id().clone();
                self.note_consumed(&binding);
                self.counters.missed += 1;
                AgentWakeDisposition::Skipped { wake }
            }
            AgentMissedOccurrencePolicy::BoundedCatchUp { .. } => {
                if self.active.len() < Self::active_capacity(policy) {
                    self.admit_or_defer(policy, binding, now, true, false)
                } else {
                    self.coalesce(policy, binding)
                }
            }
        }
    }

    fn coalesce_or_admit(
        &mut self,
        policy: &AgentWakePolicy,
        binding: AgentWakeBinding,
        now: AgentTimestampMillis,
    ) -> AgentWakeDisposition {
        if self.active.len() < Self::active_capacity(policy) {
            self.admit_or_defer(policy, binding, now, true, false)
        } else {
            self.coalesce(policy, binding)
        }
    }

    /// Parks one consumed binding in the pending queue without touching any
    /// counter — the counter-neutral primitive every parking disposition
    /// shares, so each delivery moves exactly one primary counter.
    fn park_binding(
        &mut self,
        policy: &AgentWakePolicy,
        binding: AgentWakeBinding,
    ) -> AgentWakeParked {
        self.note_consumed(&binding);
        let capacity = Self::pending_capacity(policy);
        if self.pending.len() < capacity {
            self.pending.push(binding);
            AgentWakeParked::Stored { replaced: None }
        } else if capacity == 1 {
            // The default single coalescing slot: the latest occurrence wins,
            // which is the "at most one pending occurrence" the resolved
            // defaults promise.
            let replaced = self.pending[0].wake_id().clone();
            self.pending[0] = binding;
            AgentWakeParked::Stored {
                replaced: Some(replaced),
            }
        } else {
            // A full catch-up queue is the bound the policy declared: the
            // overflow is skipped, never silently kept.
            AgentWakeParked::Overflow
        }
    }

    fn coalesce(
        &mut self,
        policy: &AgentWakePolicy,
        binding: AgentWakeBinding,
    ) -> AgentWakeDisposition {
        let wake = binding.wake_id().clone();
        match self.park_binding(policy, binding) {
            AgentWakeParked::Stored { replaced } => {
                self.counters.coalesced += 1;
                if replaced.is_some() {
                    self.counters.superseded += 1;
                }
                AgentWakeDisposition::Coalesced { wake, replaced }
            }
            AgentWakeParked::Overflow => {
                self.counters.missed += 1;
                AgentWakeDisposition::Skipped { wake }
            }
        }
    }

    fn admit_binding(
        &mut self,
        binding: AgentWakeBinding,
        now: AgentTimestampMillis,
        coalesced: bool,
        representative: bool,
    ) -> AgentWakeDisposition {
        let wake = binding.wake_id().clone();
        self.note_consumed(&binding);
        self.active.push(AgentActiveWake {
            binding,
            admitted_at: now,
            representative,
            epoch: None,
        });
        self.last_admitted = Some(wake.clone());
        self.last_admitted_at = Some(now);
        self.counters.admitted += 1;
        if coalesced {
            AgentWakeDisposition::AdmittedCoalesced { wake }
        } else {
            AgentWakeDisposition::Admitted { wake }
        }
    }

    /// Remembers a dispositioned wake in the bounded recent ring, without
    /// touching the watermark — what a fence records: the occurrence was
    /// answered, but it belongs to an obsolete schedule whose due times must
    /// not order the current one's.
    fn note_seen(&mut self, binding: &AgentWakeBinding) {
        self.recent.push(binding.wake_id().clone());
        if self.recent.len() > AGENT_WAKE_RECENT_CAPACITY {
            self.recent.remove(0);
        }
    }

    /// Remembers a wake the controller consumed — admitted, coalesced, or
    /// skipped — advancing the scheduled-due-time watermark it deduplicates
    /// later redeliveries against.
    fn note_consumed(&mut self, binding: &AgentWakeBinding) {
        self.note_seen(binding);
        // Only a *scheduled* occurrence advances the watermark: the ordering
        // it deduplicates on belongs to the schedule's due sequence alone.
        if let AgentWakeOccurrence::Scheduled { due_at } = binding.occurrence() {
            let advanced = self
                .scheduled_watermark
                .is_none_or(|watermark| due_at.as_millis() > watermark.as_millis());
            if advanced {
                self.scheduled_watermark = Some(*due_at);
            }
        }
    }

    fn validate(&self) -> AgentWakeResult<()> {
        if self.active.len() > AGENT_WAKE_ACTIVE_CAPACITY {
            return Err(AgentWakeError::StateOutOfBounds {
                detail: "active occurrences",
            });
        }
        if self.pending.len() > AGENT_WAKE_PENDING_CAPACITY {
            return Err(AgentWakeError::StateOutOfBounds {
                detail: "parked occurrences",
            });
        }
        if self.recent.len() > AGENT_WAKE_RECENT_CAPACITY {
            return Err(AgentWakeError::StateOutOfBounds {
                detail: "recent wake identities",
            });
        }
        Ok(())
    }
}

impl<'de> Deserialize<'de> for AgentWakeControllerState {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Record {
            #[serde(default)]
            active: Vec<AgentActiveWake>,
            #[serde(default)]
            pending: Vec<AgentWakeBinding>,
            #[serde(default)]
            recent: Vec<AgentWakeId>,
            #[serde(default)]
            scheduled_watermark: Option<AgentTimestampMillis>,
            #[serde(default)]
            last_admitted: Option<AgentWakeId>,
            #[serde(default)]
            last_admitted_at: Option<AgentTimestampMillis>,
            #[serde(default)]
            counters: AgentWakeCounters,
            #[serde(default)]
            window: Option<AgentWakeWindowLedger>,
            #[serde(default)]
            lifecycle: AgentGoalLifecycleState,
        }

        let record = Record::deserialize(deserializer)?;
        let state = Self {
            active: record.active,
            pending: record.pending,
            recent: record.recent,
            scheduled_watermark: record.scheduled_watermark,
            last_admitted: record.last_admitted,
            last_admitted_at: record.last_admitted_at,
            counters: record.counters,
            window: record.window,
            lifecycle: record.lifecycle,
        };
        state.validate().map_err(DeserializeError::custom)?;
        Ok(state)
    }
}

/// Rejection of a wake identity, binding, or policy.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AgentWakeError {
    /// An identifier could not key a durable scope.
    Identity(AgentIdentityError),
    /// Trigger-source metadata exceeded its bounds.
    TriggerSource(AgentTriggerSourceError),
    /// The policy allowed no trigger class at all.
    EmptyTriggers,
    /// A parallel overlap policy declared a bound that is not parallel.
    ParallelEpochsNotParallel {
        /// The declared concurrency bound.
        max_concurrent_epochs: u32,
    },
    /// A bounded catch-up policy allowed zero occurrences.
    ZeroCatchUp,
    /// A duration field was set to zero.
    ZeroDuration {
        /// The field that was zero.
        field: &'static str,
    },
    /// A maximum lateness that undercuts the admission window.
    LatenessBelowAdmissionWindow {
        /// The admission window, in milliseconds.
        admission_window_millis: u64,
        /// The declared maximum lateness, in milliseconds.
        maximum_lateness_millis: u64,
    },
    /// The backoff's initial delay was zero.
    BackoffInitialZero,
    /// The backoff multiplier would shrink the delay.
    BackoffMultiplierBelowUnit {
        /// The declared multiplier, in percent.
        multiplier_percent: u32,
    },
    /// The backoff ceiling was below its initial delay.
    BackoffMaximumBelowInitial {
        /// The initial delay, in milliseconds.
        initial_millis: u64,
        /// The declared ceiling, in milliseconds.
        max_millis: u64,
    },
    /// An escalation threshold of zero failures.
    ZeroEscalationThreshold,
    /// A retirement policy after zero occurrences.
    ZeroRetirementOccurrences,
    /// A renewal requirement without an expiry to renew against.
    RenewalWithoutExpiry,
    /// A goal window whose ceiling bounds nothing.
    WindowCeilingUnbounded,
    /// A goal window bounding a dimension the per-epoch budget leaves
    /// unbounded.
    WindowEpochUnbounded {
        /// The dimension the ceiling bounds but the epoch budget does not.
        dimension: AgentBudgetDimension,
    },
    /// A goal window whose ceiling is below the per-epoch budget on a
    /// dimension, so no window could ever pay for a single epoch.
    WindowEpochExceedsCeiling {
        /// The dimension whose ceiling the epoch budget exceeds.
        dimension: AgentBudgetDimension,
        /// The per-epoch budget declared on that dimension.
        epoch_budget: u64,
        /// The window ceiling declared on that dimension.
        ceiling: u64,
    },
    /// An epoch with neither a bounded budget dimension nor a deadline.
    EpochUnbounded,
    /// A binding carrying a schedule revision ahead of the controller's: no
    /// schedule the controller accepted ever issued it.
    RevisionAhead {
        /// The revision the binding carried.
        offered: ScheduleRevision,
        /// The revision currently in force.
        current: ScheduleRevision,
    },
    /// A trigger class the policy does not allow.
    TriggerNotAllowed {
        /// The disallowed trigger class.
        trigger: AgentWakeTriggerKind,
    },
    /// A retry occurrence without the controller trigger, or the controller
    /// trigger on anything but a retry.
    RetryTriggerMismatch {
        /// The trigger the binding carried.
        trigger: AgentWakeTriggerKind,
    },
    /// A release of a wake that is not active.
    NotActive {
        /// The wake that was not active.
        wake: AgentWakeId,
    },
    /// A wake identity that was not derived by this crate's construction, so
    /// no epoch identity can be derived from it.
    ForeignWakeId {
        /// The underived wake identity.
        wake: AgentWakeId,
    },
    /// A lifecycle command carrying a revision the goal has moved past.
    StaleLifecycleRevision {
        /// The revision the command expected to advance.
        expected: AgentRevisionNumber,
        /// The revision currently in force.
        current: AgentRevisionNumber,
    },
    /// A resume of a goal that is not suspended.
    NotSuspended {
        /// The goal's current lifecycle status.
        status: AgentGoalLifecycleStatus,
    },
    /// A lifecycle command on an expired or retired goal. Absorbing statuses
    /// accept no lifecycle transition.
    LifecycleTerminal {
        /// The absorbing status.
        status: AgentGoalLifecycleStatus,
    },
    /// A renewal outside the window its policy requires it inside.
    RenewalOutsideWindow {
        /// When the renewal window opens.
        opens: AgentTimestampMillis,
        /// The effective expiry the window closes at.
        effective: AgentTimestampMillis,
    },
    /// A renewal that does not strictly extend the effective expiry.
    RenewalNotExtending {
        /// The expiry the renewal offered.
        offered: AgentTimestampMillis,
        /// The effective expiry already in force.
        effective: AgentTimestampMillis,
    },
    /// A persisted controller state exceeding its bounded capacities.
    StateOutOfBounds {
        /// Which bound the record exceeded.
        detail: &'static str,
    },
}

impl AgentWakeError {
    /// Stable machine-readable error code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Identity(_) => "wake-identity",
            Self::TriggerSource(_) => "wake-trigger-source",
            Self::EmptyTriggers => "wake-empty-triggers",
            Self::ParallelEpochsNotParallel { .. } => "wake-parallel-not-parallel",
            Self::ZeroCatchUp => "wake-zero-catch-up",
            Self::ZeroDuration { .. } => "wake-zero-duration",
            Self::LatenessBelowAdmissionWindow { .. } => "wake-lateness-below-admission-window",
            Self::BackoffInitialZero => "wake-backoff-initial-zero",
            Self::BackoffMultiplierBelowUnit { .. } => "wake-backoff-multiplier-below-unit",
            Self::BackoffMaximumBelowInitial { .. } => "wake-backoff-maximum-below-initial",
            Self::ZeroEscalationThreshold => "wake-zero-escalation-threshold",
            Self::ZeroRetirementOccurrences => "wake-zero-retirement-occurrences",
            Self::RenewalWithoutExpiry => "wake-renewal-without-expiry",
            Self::WindowCeilingUnbounded => "wake-window-ceiling-unbounded",
            Self::WindowEpochUnbounded { .. } => "wake-window-epoch-unbounded",
            Self::WindowEpochExceedsCeiling { .. } => "wake-window-epoch-exceeds-ceiling",
            Self::EpochUnbounded => "wake-epoch-unbounded",
            Self::RevisionAhead { .. } => "wake-revision-ahead",
            Self::TriggerNotAllowed { .. } => "wake-trigger-not-allowed",
            Self::RetryTriggerMismatch { .. } => "wake-retry-trigger-mismatch",
            Self::NotActive { .. } => "wake-not-active",
            Self::ForeignWakeId { .. } => "wake-foreign-id",
            Self::StaleLifecycleRevision { .. } => "wake-stale-lifecycle-revision",
            Self::NotSuspended { .. } => "wake-not-suspended",
            Self::LifecycleTerminal { .. } => "wake-lifecycle-terminal",
            Self::RenewalOutsideWindow { .. } => "wake-renewal-outside-window",
            Self::RenewalNotExtending { .. } => "wake-renewal-not-extending",
            Self::StateOutOfBounds { .. } => "wake-state-out-of-bounds",
        }
    }
}

impl Display for AgentWakeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Identity(error) => write!(f, "wake identity is invalid: {error}"),
            Self::TriggerSource(error) => {
                write!(f, "wake trigger source is out of bounds: {error}")
            }
            Self::EmptyTriggers => f.write_str("a wake policy must allow at least one trigger"),
            Self::ParallelEpochsNotParallel {
                max_concurrent_epochs,
            } => write!(
                f,
                "a parallel overlap policy needs at least two concurrent epochs, not {max_concurrent_epochs}; a bound of one is forbid-and-coalesce"
            ),
            Self::ZeroCatchUp => {
                f.write_str("a bounded catch-up policy must replay at least one occurrence")
            }
            Self::ZeroDuration { field } => write!(f, "the {field} must be positive"),
            Self::LatenessBelowAdmissionWindow {
                admission_window_millis,
                maximum_lateness_millis,
            } => write!(
                f,
                "the maximum lateness of {maximum_lateness_millis} ms is below the admission window of {admission_window_millis} ms; an occurrence between them would be both admittable and missed"
            ),
            Self::BackoffInitialZero => f.write_str("the backoff initial delay must be positive"),
            Self::BackoffMultiplierBelowUnit { multiplier_percent } => write!(
                f,
                "the backoff multiplier must be at least 100 percent, not {multiplier_percent}"
            ),
            Self::BackoffMaximumBelowInitial {
                initial_millis,
                max_millis,
            } => write!(
                f,
                "the backoff ceiling of {max_millis} ms is below the initial delay of {initial_millis} ms"
            ),
            Self::ZeroEscalationThreshold => {
                f.write_str("the escalation threshold must be positive when set")
            }
            Self::ZeroRetirementOccurrences => {
                f.write_str("retirement after occurrences requires a positive count")
            }
            Self::RenewalWithoutExpiry => {
                f.write_str("a renewal requirement needs an expiry to renew against")
            }
            Self::WindowCeilingUnbounded => {
                f.write_str("a goal window ceiling must bound at least one dimension")
            }
            Self::WindowEpochUnbounded { dimension } => write!(
                f,
                "the goal window bounds {dimension:?}, which the per-epoch budget leaves unbounded; the first admission would exhaust the window"
            ),
            Self::WindowEpochExceedsCeiling {
                dimension,
                epoch_budget,
                ceiling,
            } => write!(
                f,
                "the per-epoch budget of {epoch_budget} on {dimension:?} exceeds the goal window ceiling of {ceiling}; no window could ever pay for one epoch"
            ),
            Self::EpochUnbounded => f.write_str(
                "an epoch must be bounded: set a deadline or bound at least one budget dimension",
            ),
            Self::RevisionAhead { offered, current } => write!(
                f,
                "the binding carries schedule revision {offered}, ahead of the current revision {current}; no accepted schedule issued it"
            ),
            Self::TriggerNotAllowed { trigger } => {
                write!(f, "the wake policy does not allow the {trigger} trigger")
            }
            Self::RetryTriggerMismatch { trigger } => write!(
                f,
                "a controller retry and the {trigger} trigger cannot name each other: only the controller delivers retries, and it delivers nothing else"
            ),
            Self::NotActive { wake } => {
                write!(f, "the wake {wake} is not an active occurrence")
            }
            Self::ForeignWakeId { wake } => write!(
                f,
                "the wake {wake} was not derived by this crate's construction; no epoch identity derives from it"
            ),
            Self::StaleLifecycleRevision { expected, current } => write!(
                f,
                "the lifecycle command expected revision {expected}, but the goal has moved to {current}; re-read and decide again"
            ),
            Self::NotSuspended { status } => {
                write!(f, "the goal is {status}, not suspended; there is nothing to resume")
            }
            Self::LifecycleTerminal { status } => {
                write!(f, "the goal is {status}, which accepts no lifecycle transition")
            }
            Self::RenewalOutsideWindow { opens, effective } => write!(
                f,
                "the renewal must arrive inside [{}, {}); outside it the policy requires expiry",
                opens.as_millis(),
                effective.as_millis()
            ),
            Self::RenewalNotExtending { offered, effective } => write!(
                f,
                "a renewal must strictly extend the effective expiry: {} does not extend {}",
                offered.as_millis(),
                effective.as_millis()
            ),
            Self::StateOutOfBounds { detail } => write!(
                f,
                "the persisted wake controller state exceeds its bound on {detail}"
            ),
        }
    }
}

impl Error for AgentWakeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Identity(error) => Some(error),
            Self::TriggerSource(error) => Some(error),
            _ => None,
        }
    }
}

impl From<AgentIdentityError> for AgentWakeError {
    fn from(error: AgentIdentityError) -> Self {
        Self::Identity(error)
    }
}

impl From<AgentTriggerSourceError> for AgentWakeError {
    fn from(error: AgentTriggerSourceError) -> Self {
        Self::TriggerSource(error)
    }
}

#[cfg(test)]
mod tests {
    use rakka_agent_workflow::{AgentAuditEventId, AgentCausationId, PrincipalRef};

    use super::*;
    use crate::budget::AgentBudgetDimension;
    use crate::schema::AgentSchemaPolicy;

    fn tenant() -> TenantId {
        TenantId::new("acme")
    }

    fn goal() -> AgentGoalId {
        AgentGoalId::new("nightly-reconciliation").expect("goal id should be valid")
    }

    fn provenance() -> AgentRevisionProvenance {
        AgentRevisionProvenance {
            principal: PrincipalRef {
                principal_type: "service".to_string(),
                principal_id: "control-plane".to_string(),
                display_name: None,
            },
            accepted_at: AgentTimestampMillis::new(1),
            causation_id: AgentCausationId::new("cause-1"),
            audit_ref: AgentAuditEventId::new("audit-1"),
        }
    }

    fn bounded_budget() -> AgentBudgetAllocation {
        let mut budget = AgentBudgetAllocation::unbounded();
        budget.set(AgentBudgetDimension::ModelCalls, Some(16));
        budget
    }

    fn default_policy() -> AgentWakePolicy {
        AgentWakePolicy::new(
            [AgentWakeTriggerKind::DurableTimer],
            bounded_budget(),
            Some(60_000),
        )
        .expect("default policy should be valid")
    }

    #[test]
    fn the_defaults_are_the_resolved_continuous_defaults() {
        let policy = default_policy();
        assert_eq!(policy.overlap, AgentWakeOverlapPolicy::ForbidAndCoalesce);
        assert_eq!(
            policy.missed_occurrence,
            AgentMissedOccurrencePolicy::AdmitOneCoalesced
        );
        assert_eq!(
            policy.lifecycle.while_suspended,
            AgentWakeSuspensionPolicy::CoalesceLatest
        );
        assert_eq!(
            policy.lifecycle.renewal,
            AgentWakeRenewalPolicy::NotRequired
        );
        assert_eq!(
            policy.lifecycle.retirement,
            AgentWakeRetirementPolicy::Manual
        );
        assert!(policy.goal_window.is_none());
    }

    #[test]
    fn parallel_epochs_demand_a_parallel_bound() {
        let result_policy = AgentPolicyRef::new("merge-results").expect("ref should be valid");
        let error = default_policy()
            .with_parallel_epochs(1, result_policy.clone())
            .expect_err("a bound of one is not parallel");
        assert_eq!(error.code(), "wake-parallel-not-parallel");

        let policy = default_policy()
            .with_parallel_epochs(3, result_policy)
            .expect("an explicit parallel policy should be accepted");
        assert!(matches!(
            policy.overlap,
            AgentWakeOverlapPolicy::Parallel {
                max_concurrent_epochs: 3,
                ..
            }
        ));
    }

    #[test]
    fn catch_up_must_replay_at_least_one_occurrence() {
        let error = default_policy()
            .with_bounded_catch_up(0)
            .expect_err("zero catch-up should be refused");
        assert_eq!(error.code(), "wake-zero-catch-up");
    }

    #[test]
    fn an_unbounded_epoch_is_refused() {
        let error = AgentWakePolicy::new(
            [AgentWakeTriggerKind::DurableTimer],
            AgentBudgetAllocation::unbounded(),
            None,
        )
        .expect_err("an epoch with no bound at all should be refused");
        assert_eq!(error.code(), "wake-epoch-unbounded");

        AgentWakePolicy::new(
            [AgentWakeTriggerKind::DurableTimer],
            AgentBudgetAllocation::unbounded(),
            Some(60_000),
        )
        .expect("a deadline alone bounds the epoch");
    }

    #[test]
    fn an_empty_trigger_set_is_refused() {
        let error = AgentWakePolicy::new([], bounded_budget(), Some(60_000))
            .expect_err("no trigger class should be refused");
        assert_eq!(error.code(), "wake-empty-triggers");
    }

    #[test]
    fn a_window_ceiling_must_bound_something() {
        let error = default_policy()
            .with_goal_window(AgentGoalWindowCeiling {
                window: AgentBudgetWindow::Rolling {
                    length_millis: 86_400_000,
                },
                ceiling: AgentBudgetAllocation::unbounded(),
            })
            .expect_err("an unbounded ceiling should be refused");
        assert_eq!(error.code(), "wake-window-ceiling-unbounded");
    }

    #[test]
    fn an_epoch_budget_above_the_window_ceiling_is_refused() {
        // The default policy's epoch costs 16 model calls. A ceiling of 8
        // could never pay for one epoch even freshly refilled, so every
        // occurrence would defer forever; the contradiction is refused at
        // construction, exactly as its unbounded sibling is.
        let mut ceiling = AgentBudgetAllocation::unbounded();
        ceiling.set(AgentBudgetDimension::ModelCalls, Some(8));
        let error = default_policy()
            .with_goal_window(AgentGoalWindowCeiling {
                window: AgentBudgetWindow::Rolling {
                    length_millis: 3_600_000,
                },
                ceiling,
            })
            .expect_err("a ceiling below the epoch budget should be refused");
        assert_eq!(error.code(), "wake-window-epoch-exceeds-ceiling");

        // The bound is exact: a ceiling equal to the epoch budget pays for
        // exactly one epoch per window and is accepted.
        windowed_policy(3_600_000, 16);
    }

    #[test]
    fn a_renewal_requirement_needs_an_expiry() {
        let error = default_policy()
            .with_lifecycle(AgentWakeLifecyclePolicy {
                renewal: AgentWakeRenewalPolicy::RequiredBefore {
                    window_millis: 60_000,
                },
                ..AgentWakeLifecyclePolicy::DEFAULT
            })
            .expect_err("renewal without expiry should be refused");
        assert_eq!(error.code(), "wake-renewal-without-expiry");
    }

    #[test]
    fn the_maximum_lateness_cannot_undercut_the_admission_window() {
        // An occurrence inside the admission window but past the maximum
        // lateness would be both admittable and missed at once, so the
        // contradiction is refused at construction, whichever bound is
        // declared first.
        let error = default_policy()
            .with_admission_window(60_000)
            .expect("an admission window alone is accepted")
            .with_maximum_lateness(30_000)
            .expect_err("a lateness inside the window should be refused");
        assert_eq!(error.code(), "wake-lateness-below-admission-window");

        let error = default_policy()
            .with_maximum_lateness(30_000)
            .expect("a maximum lateness alone is accepted")
            .with_admission_window(60_000)
            .expect_err("the refusal cannot depend on declaration order");
        assert_eq!(error.code(), "wake-lateness-below-admission-window");

        default_policy()
            .with_admission_window(60_000)
            .expect("an admission window alone is accepted")
            .with_maximum_lateness(60_000)
            .expect("a lateness equal to the window is accepted");
    }

    #[test]
    fn a_zero_schedule_revision_is_unrepresentable() {
        assert_eq!(ScheduleRevision::new(0), ScheduleRevision::INITIAL);

        let error = serde_json::from_value::<ScheduleRevision>(serde_json::json!(0))
            .expect_err("a zero revision should fail closed on load");
        assert!(error.to_string().contains("never issued"));

        let loaded: ScheduleRevision =
            serde_json::from_value(serde_json::json!(7)).expect("a positive revision loads");
        assert_eq!(loaded, ScheduleRevision::new(7));
    }

    #[test]
    fn a_malformed_policy_fails_closed_on_deserialization() {
        let mut value = serde_json::to_value(default_policy()).expect("policy should serialize");
        value["triggers"] = serde_json::json!([]);
        let error = serde_json::from_value::<AgentWakePolicy>(value)
            .expect_err("an empty trigger set should fail closed on load");
        assert!(error.to_string().contains("at least one trigger"));
    }

    #[test]
    fn the_policy_revision_carries_the_current_schema_version() {
        let revision = AgentWakePolicyRevision::initial(default_policy(), provenance())
            .expect("initial revision should be accepted");
        assert_eq!(revision.revision(), AgentRevisionNumber::INITIAL);
        AgentSchemaPolicy::default()
            .check_record(&revision)
            .expect("the current schema version should be accepted");

        let updated = revision
            .updated(default_policy(), provenance())
            .expect("update should be accepted");
        assert_eq!(updated.revision(), AgentRevisionNumber::INITIAL.next());
    }

    #[test]
    fn the_same_occurrence_from_any_trigger_path_yields_one_identity() {
        let occurrence = AgentWakeOccurrence::Scheduled {
            due_at: AgentTimestampMillis::new(1_753_500_000_000),
        };
        let mut ids = BTreeSet::new();
        for (trigger, accepted_at) in [
            (AgentWakeTriggerKind::DurableTimer, 1_753_500_000_100),
            (AgentWakeTriggerKind::DurableTimer, 1_753_500_090_000),
            (AgentWakeTriggerKind::A2aCommand, 1_753_500_500_000),
        ] {
            let binding = AgentWakeBinding::new(
                tenant(),
                goal(),
                ScheduleRevision::INITIAL,
                occurrence.clone(),
                trigger,
                AgentTimestampMillis::new(accepted_at),
                AgentRevisionNumber::INITIAL,
            )
            .expect("binding should be valid");
            ids.insert(binding.wake_id().clone());
        }
        assert_eq!(ids.len(), 1, "every trigger path derives the same wake id");
    }

    #[test]
    fn a_binding_with_a_forged_identity_fails_closed_on_deserialization() {
        let binding = AgentWakeBinding::new(
            tenant(),
            goal(),
            ScheduleRevision::INITIAL,
            AgentWakeOccurrence::Scheduled {
                due_at: AgentTimestampMillis::new(1),
            },
            AgentWakeTriggerKind::DurableTimer,
            AgentTimestampMillis::new(2),
            AgentRevisionNumber::INITIAL,
        )
        .expect("binding should be valid");
        let mut value = serde_json::to_value(&binding).expect("binding should serialize");
        value["wake"] = serde_json::json!(format!("{AGENT_WAKE_ID_PREFIX}{}", "0".repeat(64)));
        let error = serde_json::from_value::<AgentWakeBinding>(value)
            .expect_err("a forged wake id should fail closed on load");
        assert!(error.to_string().contains("do not derive"));

        let roundtrip = serde_json::to_value(&binding).expect("binding should serialize");
        let loaded = serde_json::from_value::<AgentWakeBinding>(roundtrip)
            .expect("an untampered binding should load");
        assert_eq!(loaded, binding);
    }

    fn scheduled_binding(due_at: u64, revision: ScheduleRevision) -> AgentWakeBinding {
        AgentWakeBinding::new(
            tenant(),
            goal(),
            revision,
            AgentWakeOccurrence::Scheduled {
                due_at: AgentTimestampMillis::new(due_at),
            },
            AgentWakeTriggerKind::DurableTimer,
            AgentTimestampMillis::new(due_at),
            AgentRevisionNumber::INITIAL,
        )
        .expect("binding should be valid")
    }

    fn now(at: u64) -> AgentTimestampMillis {
        AgentTimestampMillis::new(at)
    }

    #[test]
    fn a_fresh_occurrence_admits_and_a_concurrent_one_coalesces_latest_wins() {
        let policy = default_policy();
        let mut controller = AgentWakeControllerState::new();

        let first = controller
            .admit(
                &policy,
                ScheduleRevision::INITIAL,
                scheduled_binding(1_000, ScheduleRevision::INITIAL),
                now(1_010),
            )
            .expect("a fresh occurrence should be dispositioned");
        assert!(matches!(first, AgentWakeDisposition::Admitted { .. }));

        let second = controller
            .admit(
                &policy,
                ScheduleRevision::INITIAL,
                scheduled_binding(2_000, ScheduleRevision::INITIAL),
                now(2_010),
            )
            .expect("a concurrent occurrence should be dispositioned");
        assert!(matches!(
            second,
            AgentWakeDisposition::Coalesced { replaced: None, .. }
        ));

        let third = controller
            .admit(
                &policy,
                ScheduleRevision::INITIAL,
                scheduled_binding(3_000, ScheduleRevision::INITIAL),
                now(3_010),
            )
            .expect("a later concurrent occurrence should be dispositioned");
        assert!(matches!(
            third,
            AgentWakeDisposition::Coalesced {
                replaced: Some(_),
                ..
            }
        ));

        assert_eq!(controller.active().len(), 1);
        assert_eq!(controller.pending().len(), 1);
        assert_eq!(
            controller.pending()[0].due_at(),
            Some(AgentTimestampMillis::new(3_000)),
            "the single coalescing slot keeps the latest occurrence"
        );
        assert_eq!(controller.counters().admitted, 1);
        assert_eq!(controller.counters().coalesced, 2);
        assert_eq!(controller.counters().superseded, 1);
    }

    #[test]
    fn release_promotes_the_parked_occurrence_in_the_same_transition() {
        let policy = default_policy();
        let mut controller = AgentWakeControllerState::new();
        let first = scheduled_binding(1_000, ScheduleRevision::INITIAL);
        let second = scheduled_binding(2_000, ScheduleRevision::INITIAL);
        let first_wake = first.wake_id().clone();
        let second_wake = second.wake_id().clone();

        controller
            .admit(&policy, ScheduleRevision::INITIAL, first, now(1_010))
            .expect("the first occurrence should admit");
        controller
            .admit(&policy, ScheduleRevision::INITIAL, second, now(2_010))
            .expect("the second occurrence should coalesce");

        let release = controller
            .release(&policy, &first_wake, now(5_000))
            .expect("the active occurrence should release");
        assert_eq!(release.released, first_wake);
        assert_eq!(release.admitted_next, Some(second_wake.clone()));
        assert_eq!(controller.active().len(), 1);
        assert_eq!(controller.active()[0].binding().wake_id(), &second_wake);
        assert!(controller.pending().is_empty());
        assert_eq!(controller.counters().admitted, 2);
        assert_eq!(controller.counters().released, 1);

        let error = controller
            .release(&policy, &first_wake, now(5_001))
            .expect_err("releasing a wake that is not active should be refused");
        assert_eq!(error.code(), "wake-not-active");
    }

    #[test]
    fn an_obsolete_revision_is_fenced_and_a_revision_ahead_fails_closed() {
        let policy = default_policy();
        let mut controller = AgentWakeControllerState::new();
        let current = ScheduleRevision::new(3);

        let fenced = controller
            .admit(
                &policy,
                current,
                scheduled_binding(1_000, ScheduleRevision::new(2)),
                now(1_010),
            )
            .expect("an obsolete revision should be dispositioned, not erred");
        assert!(matches!(
            fenced,
            AgentWakeDisposition::Fenced { offered, current: in_force, .. }
                if offered == ScheduleRevision::new(2) && in_force == current
        ));
        assert_eq!(controller.counters().fenced, 1);
        assert!(controller.active().is_empty());

        let error = controller
            .admit(
                &policy,
                current,
                scheduled_binding(1_000, ScheduleRevision::new(4)),
                now(1_010),
            )
            .expect_err("a revision ahead of the controller should fail closed");
        assert_eq!(error.code(), "wake-revision-ahead");
    }

    #[test]
    fn a_duplicate_delivery_answers_without_a_transition() {
        let policy = default_policy();
        let mut controller = AgentWakeControllerState::new();
        let binding = scheduled_binding(1_000, ScheduleRevision::INITIAL);
        let wake = binding.wake_id().clone();

        controller
            .admit(
                &policy,
                ScheduleRevision::INITIAL,
                binding.clone(),
                now(1_010),
            )
            .expect("the first delivery should admit");
        let before = controller.clone();

        let duplicate = controller
            .admit(&policy, ScheduleRevision::INITIAL, binding, now(9_999))
            .expect("a duplicate delivery should be dispositioned");
        assert_eq!(
            duplicate,
            AgentWakeDisposition::Duplicate { wake: wake.clone() }
        );
        assert_eq!(controller, before, "a duplicate must not change state");

        // Even after the occurrence releases and leaves every slot, the
        // scheduled watermark still answers a late redelivery as a duplicate.
        controller
            .release(&policy, &wake, now(10_000))
            .expect("the active occurrence should release");
        let late = controller
            .admit(
                &policy,
                ScheduleRevision::INITIAL,
                scheduled_binding(1_000, ScheduleRevision::INITIAL),
                now(20_000),
            )
            .expect("a late redelivery should be dispositioned");
        assert!(matches!(late, AgentWakeDisposition::Duplicate { .. }));
    }

    #[test]
    fn the_time_bands_place_an_occurrence() {
        let policy = default_policy()
            .with_admission_window(60_000)
            .expect("the window should be accepted")
            .with_maximum_lateness(120_000)
            .expect("the lateness should be accepted");
        let mut controller = AgentWakeControllerState::new();

        // Within the admission window: a direct admission.
        let fresh = controller
            .admit(
                &policy,
                ScheduleRevision::INITIAL,
                scheduled_binding(100_000, ScheduleRevision::INITIAL),
                now(130_000),
            )
            .expect("an in-window occurrence should be dispositioned");
        assert!(matches!(fresh, AgentWakeDisposition::Admitted { .. }));
        let active = controller.active()[0].binding().wake_id().clone();
        controller
            .release(&policy, &active, now(140_000))
            .expect("the active occurrence should release");

        // Between the window and the maximum lateness: coalesced, which in an
        // idle controller admits as the coalesced representative.
        let in_band = controller
            .admit(
                &policy,
                ScheduleRevision::INITIAL,
                scheduled_binding(200_000, ScheduleRevision::INITIAL),
                now(290_000),
            )
            .expect("an in-band occurrence should be dispositioned");
        assert!(matches!(
            in_band,
            AgentWakeDisposition::AdmittedCoalesced { .. }
        ));

        // Past the maximum lateness with the default policy: the one
        // coalesced representative — but an occurrence is already active, so
        // it parks.
        let missed = controller
            .admit(
                &policy,
                ScheduleRevision::INITIAL,
                scheduled_binding(300_000, ScheduleRevision::INITIAL),
                now(500_000),
            )
            .expect("a missed occurrence should be dispositioned");
        assert!(matches!(missed, AgentWakeDisposition::Coalesced { .. }));

        // The same lateness under a skip policy is skipped outright.
        let skip_policy = {
            let mut skip = policy.clone();
            skip.missed_occurrence = AgentMissedOccurrencePolicy::Skip;
            skip
        };
        let skipped = controller
            .admit(
                &skip_policy,
                ScheduleRevision::INITIAL,
                scheduled_binding(310_000, ScheduleRevision::INITIAL),
                now(500_000),
            )
            .expect("a skipped occurrence should be dispositioned");
        assert!(matches!(skipped, AgentWakeDisposition::Skipped { .. }));
        assert_eq!(controller.counters().missed, 1);
    }

    #[test]
    fn bounded_catch_up_queues_to_its_bound_and_skips_the_overflow() {
        let policy = default_policy()
            .with_maximum_lateness(1_000)
            .expect("the lateness should be accepted")
            .with_bounded_catch_up(2)
            .expect("the catch-up bound should be accepted");
        let mut controller = AgentWakeControllerState::new();

        // Four occurrences long past their lateness, as after downtime. The
        // first replays into the free slot; two queue; the fourth overflows
        // the declared bound and is skipped.
        let dispositions: Vec<_> = (1..=4)
            .map(|slot| {
                controller
                    .admit(
                        &policy,
                        ScheduleRevision::INITIAL,
                        scheduled_binding(slot * 1_000, ScheduleRevision::INITIAL),
                        now(1_000_000),
                    )
                    .expect("every occurrence should be dispositioned")
            })
            .collect();
        assert!(matches!(
            dispositions[0],
            AgentWakeDisposition::AdmittedCoalesced { .. }
        ));
        assert!(matches!(
            dispositions[1],
            AgentWakeDisposition::Coalesced { .. }
        ));
        assert!(matches!(
            dispositions[2],
            AgentWakeDisposition::Coalesced { .. }
        ));
        assert!(matches!(
            dispositions[3],
            AgentWakeDisposition::Skipped { .. }
        ));
        assert_eq!(controller.pending().len(), 2);
        assert_eq!(controller.counters().missed, 1);
    }

    #[test]
    fn a_disallowed_trigger_is_refused() {
        let policy = default_policy();
        let mut controller = AgentWakeControllerState::new();
        let binding = AgentWakeBinding::new(
            tenant(),
            goal(),
            ScheduleRevision::INITIAL,
            AgentWakeOccurrence::ExternalEvent {
                event: AgentWakeEventId::new("deploy-finished").expect("event id should be valid"),
            },
            AgentWakeTriggerKind::ExternalEvent,
            AgentTimestampMillis::new(1_000),
            AgentRevisionNumber::INITIAL,
        )
        .expect("binding should be valid");

        let error = controller
            .admit(&policy, ScheduleRevision::INITIAL, binding, now(1_000))
            .expect_err("a trigger class the policy does not allow should be refused");
        assert_eq!(error.code(), "wake-trigger-not-allowed");
    }

    #[test]
    fn a_schedule_update_fences_every_parked_occurrence() {
        let policy = default_policy();
        let mut controller = AgentWakeControllerState::new();
        controller
            .admit(
                &policy,
                ScheduleRevision::INITIAL,
                scheduled_binding(1_000, ScheduleRevision::INITIAL),
                now(1_010),
            )
            .expect("the first occurrence should admit");
        controller
            .admit(
                &policy,
                ScheduleRevision::INITIAL,
                scheduled_binding(2_000, ScheduleRevision::INITIAL),
                now(2_010),
            )
            .expect("the second occurrence should coalesce");

        let fenced = controller.apply_schedule_update(ScheduleRevision::INITIAL.next());
        assert_eq!(fenced, 1);
        assert!(controller.pending().is_empty());
        assert_eq!(
            controller.active().len(),
            1,
            "an already-admitted occurrence is not fenced by a schedule update"
        );
        assert_eq!(controller.counters().fenced, 1);
    }

    #[test]
    fn the_epoch_identity_is_derived_from_the_wake() {
        let occurrence = AgentWakeOccurrence::Scheduled {
            due_at: AgentTimestampMillis::new(1_753_500_000_000),
        };
        let wake =
            wake_id_for_occurrence(&tenant(), &goal(), ScheduleRevision::INITIAL, &occurrence)
                .expect("the wake derives");
        let task = epoch_task_id_for_wake(&wake).expect("the epoch task derives");

        // Pinned golden vector: the epoch id is the wake digest under the
        // `epoch-` prefix — a persisted compatibility surface exactly like
        // the wake derivation it extends.
        assert_eq!(
            task.as_str(),
            "epoch-73e57f72c96f774e5dd6f15cc0d3fb10f758ab6b1c59ebd7b0389e074cc8f392"
        );
        assert_eq!(task.as_str().len(), 70);
        assert_eq!(
            epoch_task_id_for_wake(&wake).expect("the derivation is stable"),
            task
        );

        let foreign = AgentWakeId::new("not-a-derived-wake").expect("the id is a valid segment");
        let error =
            epoch_task_id_for_wake(&foreign).expect_err("an underived wake identity fails closed");
        assert_eq!(error.code(), "wake-foreign-id");
    }

    #[test]
    fn a_fenced_occurrence_never_advances_the_watermark() {
        let policy = default_policy();
        let mut controller = AgentWakeControllerState::new();
        let current = ScheduleRevision::new(2);

        // A future-dated straggler from the obsolete schedule is fenced —
        // and its due time must not order the new schedule's occurrences.
        let fenced = controller
            .admit(
                &policy,
                current,
                scheduled_binding(500_000, ScheduleRevision::INITIAL),
                now(1_000),
            )
            .expect("the straggler is dispositioned");
        assert!(matches!(fenced, AgentWakeDisposition::Fenced { .. }));

        // The new schedule legitimately issues an earlier due time: it must
        // admit, not be swallowed as a duplicate of the fenced straggler.
        let admitted = controller
            .admit(
                &policy,
                current,
                scheduled_binding(1_000, current),
                now(1_010),
            )
            .expect("the current occurrence is dispositioned");
        assert!(matches!(admitted, AgentWakeDisposition::Admitted { .. }));
    }

    #[test]
    fn the_fence_runs_before_the_watermark() {
        let policy = default_policy();
        let mut controller = AgentWakeControllerState::new();
        let current = ScheduleRevision::new(2);

        // The current schedule has consumed up to due time 5_000.
        controller
            .admit(
                &policy,
                current,
                scheduled_binding(5_000, current),
                now(5_010),
            )
            .expect("the current occurrence admits");

        // An obsolete occurrence below that watermark is *fenced*, not
        // silently swallowed as a duplicate: the fence is the stronger fact,
        // and the fenced counter must record it.
        let stale = controller
            .admit(
                &policy,
                current,
                scheduled_binding(4_000, ScheduleRevision::INITIAL),
                now(5_020),
            )
            .expect("the stale occurrence is dispositioned");
        assert!(matches!(stale, AgentWakeDisposition::Fenced { .. }));
        assert_eq!(controller.counters().fenced, 1);
    }

    #[test]
    fn a_schedule_update_resets_the_watermark() {
        let policy = default_policy();
        let mut controller = AgentWakeControllerState::new();

        controller
            .admit(
                &policy,
                ScheduleRevision::INITIAL,
                scheduled_binding(5_000, ScheduleRevision::INITIAL),
                now(5_010),
            )
            .expect("the first occurrence admits");
        let wake = controller.active()[0].binding().wake_id().clone();
        controller
            .release(&policy, &wake, now(6_000))
            .expect("the occurrence releases");

        // The update fences nothing here, but it must reset the due-time
        // watermark: the new schedule may issue due times at or below what
        // the old schedule reached.
        let next = ScheduleRevision::INITIAL.next();
        assert_eq!(controller.apply_schedule_update(next), 0);
        let admitted = controller
            .admit(&policy, next, scheduled_binding(1_000, next), now(6_010))
            .expect("the new schedule's occurrence is dispositioned");
        assert!(
            matches!(admitted, AgentWakeDisposition::Admitted { .. }),
            "an earlier due time under the new revision admits, got {admitted:?}"
        );
    }

    #[test]
    fn a_downtime_backlog_yields_exactly_one_epoch() {
        let policy = default_policy()
            .with_maximum_lateness(1_000)
            .expect("the lateness is accepted");
        let mut controller = AgentWakeControllerState::new();

        // Three occurrences missed during one downtime. The first admits as
        // the backlog's coalesced representative; the rest are absorbed by
        // it — counted missed, never parked — so releasing the
        // representative finds nothing to promote.
        let dispositions: Vec<_> = (1..=3)
            .map(|slot| {
                controller
                    .admit(
                        &policy,
                        ScheduleRevision::INITIAL,
                        scheduled_binding(slot * 1_000, ScheduleRevision::INITIAL),
                        now(1_000_000),
                    )
                    .expect("every occurrence is dispositioned")
            })
            .collect();
        assert!(matches!(
            dispositions[0],
            AgentWakeDisposition::AdmittedCoalesced { .. }
        ));
        assert!(matches!(
            dispositions[1],
            AgentWakeDisposition::Skipped { .. }
        ));
        assert!(matches!(
            dispositions[2],
            AgentWakeDisposition::Skipped { .. }
        ));
        assert!(controller.active()[0].is_representative());
        assert!(controller.pending().is_empty());
        assert_eq!(controller.counters().admitted, 1);
        assert_eq!(controller.counters().missed, 2);

        let wake = controller.active()[0].binding().wake_id().clone();
        let release = controller
            .release(&policy, &wake, now(1_000_100))
            .expect("the representative releases");
        assert!(release.admitted_next.is_none());
        assert_eq!(controller.counters().admitted, 1);

        // A backlog behind a *normal* epoch still parks its one
        // representative, admitted at release.
        controller
            .admit(
                &policy,
                ScheduleRevision::INITIAL,
                scheduled_binding(2_000_000, ScheduleRevision::INITIAL),
                now(2_000_010),
            )
            .expect("a fresh occurrence admits normally");
        assert!(!controller.active()[0].is_representative());
        let parked = controller
            .admit(
                &policy,
                ScheduleRevision::INITIAL,
                scheduled_binding(2_100_000, ScheduleRevision::INITIAL),
                now(3_000_000),
            )
            .expect("the missed occurrence is dispositioned");
        assert!(matches!(parked, AgentWakeDisposition::Coalesced { .. }));
        assert_eq!(controller.pending().len(), 1);
    }

    #[test]
    fn a_promoted_representative_still_absorbs_its_backlog() {
        let policy = default_policy()
            .with_maximum_lateness(1_000)
            .expect("the lateness is accepted");
        let mut controller = AgentWakeControllerState::new();

        // A normal epoch is active, and a downtime backlog parks its one
        // representative behind it.
        controller
            .admit(
                &policy,
                ScheduleRevision::INITIAL,
                scheduled_binding(1_000, ScheduleRevision::INITIAL),
                now(1_010),
            )
            .expect("the fresh occurrence admits");
        assert!(!controller.active()[0].is_representative());
        let parked = controller
            .admit(
                &policy,
                ScheduleRevision::INITIAL,
                scheduled_binding(2_000, ScheduleRevision::INITIAL),
                now(500_000),
            )
            .expect("the missed occurrence is dispositioned");
        assert!(matches!(parked, AgentWakeDisposition::Coalesced { .. }));

        // The release promotes the parked occurrence, and it must come back
        // *as* the backlog's representative: the parked binding carries no
        // mark, so the mark is recomputed from its lateness at promotion.
        let wake = controller.active()[0].binding().wake_id().clone();
        let release = controller
            .release(&policy, &wake, now(500_100))
            .expect("the normal epoch releases");
        assert!(release.admitted_next.is_some());
        assert!(controller.active()[0].is_representative());

        // A later missed occurrence of the same backlog absorbs into the
        // promoted representative instead of parking an echo that would
        // admit a second epoch.
        let echo = controller
            .admit(
                &policy,
                ScheduleRevision::INITIAL,
                scheduled_binding(3_000, ScheduleRevision::INITIAL),
                now(500_200),
            )
            .expect("the later missed occurrence is dispositioned");
        assert!(matches!(echo, AgentWakeDisposition::Skipped { .. }));
        assert!(controller.pending().is_empty());

        let wake = controller.active()[0].binding().wake_id().clone();
        let release = controller
            .release(&policy, &wake, now(500_300))
            .expect("the representative releases");
        assert!(release.admitted_next.is_none());
        assert_eq!(controller.counters().admitted, 2);
    }

    fn windowed_policy(length_millis: u64, model_calls: u64) -> AgentWakePolicy {
        let mut ceiling = AgentBudgetAllocation::unbounded();
        ceiling.set(AgentBudgetDimension::ModelCalls, Some(model_calls));
        default_policy()
            .with_goal_window(AgentGoalWindowCeiling {
                window: AgentBudgetWindow::Rolling { length_millis },
                ceiling,
            })
            .expect("the windowed policy is valid")
    }

    #[test]
    fn the_window_ceiling_defers_an_epoch_it_cannot_pay_for() {
        // The default policy's epoch costs 16 model calls; a 24-call window
        // pays for one epoch, then defers the next until the window turns.
        let policy = windowed_policy(3_600_000, 24);
        let mut controller = AgentWakeControllerState::new();

        let first = controller
            .admit(
                &policy,
                ScheduleRevision::INITIAL,
                scheduled_binding(1_000, ScheduleRevision::INITIAL),
                now(1_000),
            )
            .expect("the first occurrence is dispositioned");
        assert!(matches!(first, AgentWakeDisposition::Admitted { .. }));
        let wake = controller.active()[0].binding().wake_id().clone();
        controller
            .release(&policy, &wake, now(2_000))
            .expect("the first epoch releases");

        let second = controller
            .admit(
                &policy,
                ScheduleRevision::INITIAL,
                scheduled_binding(3_000, ScheduleRevision::INITIAL),
                now(3_000),
            )
            .expect("the second occurrence is dispositioned");
        assert!(
            matches!(second, AgentWakeDisposition::Deferred { .. }),
            "the exhausted window defers, got {second:?}"
        );
        assert_eq!(controller.counters().deferred, 1);
        assert_eq!(controller.counters().admitted, 1);
        assert_eq!(controller.pending().len(), 1, "the deferred wake parks");

        // Releasing with nothing active cannot happen; instead the *next*
        // admission attempt after the window turns pays for the parked wake's
        // promotion. Advance past the rolling boundary and admit a fresh
        // occurrence: the refill is recorded by that same transition.
        let third = controller
            .admit(
                &policy,
                ScheduleRevision::INITIAL,
                scheduled_binding(4_000_000, ScheduleRevision::INITIAL),
                now(4_000_000),
            )
            .expect("the post-refill occurrence is dispositioned");
        assert!(
            matches!(third, AgentWakeDisposition::Admitted { .. }),
            "the refilled window admits, got {third:?}"
        );
        let ledger = controller.window().expect("the window ledger exists");
        assert_eq!(
            ledger.consumed().get(AgentBudgetDimension::ModelCalls),
            16,
            "the refilled window holds exactly the new epoch's charge"
        );
    }

    #[test]
    fn the_refilled_window_promotes_the_deferred_occurrence_first() {
        // One epoch per window: the canonical ceiling == epoch budget config,
        // where leapfrogging would starve a deferred occurrence forever.
        let policy = windowed_policy(3_600_000, 16);
        let mut controller = AgentWakeControllerState::new();

        // The first occurrence drains the window and releases; the second
        // defers on the drained window and parks.
        controller
            .admit(
                &policy,
                ScheduleRevision::INITIAL,
                scheduled_binding(1_000, ScheduleRevision::INITIAL),
                now(1_000),
            )
            .expect("the first occurrence admits");
        let wake = controller.active()[0].binding().wake_id().clone();
        controller
            .release(&policy, &wake, now(2_000))
            .expect("the first epoch releases");
        let deferred = controller
            .admit(
                &policy,
                ScheduleRevision::INITIAL,
                scheduled_binding(3_000, ScheduleRevision::INITIAL),
                now(3_000),
            )
            .expect("the second occurrence is dispositioned");
        assert!(matches!(deferred, AgentWakeDisposition::Deferred { .. }));

        // The entity's admission transition promotes before dispositioning:
        // after the window turns, the deferred occurrence takes the slot and
        // the fresh delivery parks behind it — oldest first.
        let promoted = controller
            .promote_admittable(&policy, now(4_000_000))
            .expect("the turned window pays for the deferred occurrence");
        let fresh = controller
            .admit(
                &policy,
                ScheduleRevision::INITIAL,
                scheduled_binding(4_000_000, ScheduleRevision::INITIAL),
                now(4_000_000),
            )
            .expect("the fresh occurrence is dispositioned");
        assert!(matches!(fresh, AgentWakeDisposition::Coalesced { .. }));
        assert_eq!(controller.active()[0].binding().wake_id(), &promoted);
        assert_eq!(
            controller.active()[0].binding().due_at(),
            Some(now(3_000)),
            "the older occurrence owns the slot"
        );
        assert_eq!(controller.pending().len(), 1);

        // The promotion charged the turned window in full, so neither the
        // release nor another promotion attempt can pay again until the next
        // turn — the parked fresh occurrence stays parked, not lost.
        let release = controller
            .release(&policy, &promoted, now(4_100_000))
            .expect("the promoted epoch releases");
        assert!(release.admitted_next.is_none());
        assert!(controller
            .promote_admittable(&policy, now(4_200_000))
            .is_none());
        assert_eq!(controller.pending().len(), 1);
        assert_eq!(controller.counters().admitted, 2);
    }

    #[test]
    fn the_window_refills_only_by_logical_time() {
        let policy = windowed_policy(3_600_000, 16);
        let mut controller = AgentWakeControllerState::new();
        controller
            .admit(
                &policy,
                ScheduleRevision::INITIAL,
                scheduled_binding(1_000, ScheduleRevision::INITIAL),
                now(1_000),
            )
            .expect("the first occurrence admits");
        let before = controller.window().copied().expect("the ledger exists");

        // A structural restart is a round-trip through the persisted record;
        // nothing about it may touch the ledger.
        let json = serde_json::to_value(&controller).expect("the state serializes");
        let recovered: AgentWakeControllerState =
            serde_json::from_value(json).expect("the state recovers");
        assert_eq!(
            recovered.window().copied().expect("the ledger survives"),
            before,
            "recovery neither refills nor consumes"
        );

        // Inside the same window, the charge is still refused after recovery.
        let mut controller = recovered;
        let wake = controller.active()[0].binding().wake_id().clone();
        controller
            .release(&policy, &wake, now(2_000))
            .expect("the epoch releases");
        let deferred = controller
            .admit(
                &policy,
                ScheduleRevision::INITIAL,
                scheduled_binding(3_000, ScheduleRevision::INITIAL),
                now(3_000),
            )
            .expect("the in-window occurrence is dispositioned");
        assert!(matches!(deferred, AgentWakeDisposition::Deferred { .. }));
    }

    #[test]
    fn a_release_promotes_only_what_the_window_can_pay_for() {
        let policy = windowed_policy(3_600_000, 24);
        let mut controller = AgentWakeControllerState::new();
        controller
            .admit(
                &policy,
                ScheduleRevision::INITIAL,
                scheduled_binding(1_000, ScheduleRevision::INITIAL),
                now(1_000),
            )
            .expect("the first occurrence admits");
        controller
            .admit(
                &policy,
                ScheduleRevision::INITIAL,
                scheduled_binding(2_000, ScheduleRevision::INITIAL),
                now(2_000),
            )
            .expect("the second occurrence coalesces");
        let wake = controller.active()[0].binding().wake_id().clone();

        // 16 of 24 calls are spent; the parked epoch would need 16 more, so
        // the release promotes nothing and the wake stays parked.
        let release = controller
            .release(&policy, &wake, now(3_000))
            .expect("the epoch releases");
        assert!(release.admitted_next.is_none());
        assert_eq!(controller.pending().len(), 1);

        // After the window turns, a release pays for the promotion.
        controller
            .admit(
                &policy,
                ScheduleRevision::INITIAL,
                scheduled_binding(4_000_000, ScheduleRevision::INITIAL),
                now(4_000_000),
            )
            .expect("the post-refill occurrence admits");
        let wake = controller.active()[0].binding().wake_id().clone();
        let release = controller
            .release(&policy, &wake, now(7_300_000))
            .expect("the epoch releases after the next turn");
        assert!(
            release.admitted_next.is_some(),
            "the turned window pays for the parked wake's promotion"
        );
    }

    #[test]
    fn calendar_windows_align_to_utc_boundaries() {
        // 2026-07-27 (epoch day 20661) is a Monday; 12:00 UTC that day.
        let monday_noon = 20_661 * MILLIS_PER_DAY + 12 * 3_600_000;
        assert_eq!(
            calendar_window_start(
                AgentCalendarUnit::Day,
                AgentTimestampMillis::new(monday_noon)
            ),
            20_661 * MILLIS_PER_DAY,
            "the day window starts at midnight UTC"
        );
        assert_eq!(
            calendar_window_start(
                AgentCalendarUnit::Week,
                AgentTimestampMillis::new(monday_noon)
            ),
            20_661 * MILLIS_PER_DAY,
            "a Monday noon is inside the week that began that midnight"
        );
        // The month began Wednesday 2026-07-01 (epoch day 20635).
        assert_eq!(
            calendar_window_start(
                AgentCalendarUnit::Month,
                AgentTimestampMillis::new(monday_noon)
            ),
            20_635 * MILLIS_PER_DAY,
            "the month window starts on 2026-07-01T00:00Z"
        );
        // A Sunday (2026-07-26, day 20660) belongs to the week that began the
        // previous Monday (2026-07-20, day 20654).
        assert_eq!(
            calendar_window_start(
                AgentCalendarUnit::Week,
                AgentTimestampMillis::new(20_660 * MILLIS_PER_DAY + 1)
            ),
            20_654 * MILLIS_PER_DAY,
        );
    }

    #[test]
    fn calendar_windows_handle_leap_years() {
        // 2024-02-29 — the leap day, a Thursday, epoch day 19782.
        let leap_noon = 19_782 * MILLIS_PER_DAY + 12 * 3_600_000;
        assert_eq!(
            calendar_window_start(AgentCalendarUnit::Day, AgentTimestampMillis::new(leap_noon)),
            19_782 * MILLIS_PER_DAY,
        );
        assert_eq!(
            calendar_window_start(
                AgentCalendarUnit::Week,
                AgentTimestampMillis::new(leap_noon)
            ),
            19_779 * MILLIS_PER_DAY,
            "the leap day belongs to the week of Monday 2024-02-26"
        );
        assert_eq!(
            calendar_window_start(
                AgentCalendarUnit::Month,
                AgentTimestampMillis::new(leap_noon)
            ),
            19_754 * MILLIS_PER_DAY,
            "the leap day belongs to the month window of 2024-02-01"
        );

        // Crossing a leap year's December: 2028-12-31 is epoch day 21549 in
        // the 366-day year 2028; its month window began 2028-12-01 (21519).
        assert_eq!(
            calendar_window_start(
                AgentCalendarUnit::Month,
                AgentTimestampMillis::new(21_549 * MILLIS_PER_DAY + 1)
            ),
            21_519 * MILLIS_PER_DAY,
        );
        // The very next day opens both a new month and a new year.
        assert_eq!(
            calendar_window_start(
                AgentCalendarUnit::Month,
                AgentTimestampMillis::new(21_550 * MILLIS_PER_DAY)
            ),
            21_550 * MILLIS_PER_DAY,
            "2029-01-01 starts its own month window"
        );
    }

    #[test]
    fn a_window_bounding_an_unbounded_epoch_dimension_is_refused() {
        let mut ceiling = AgentBudgetAllocation::unbounded();
        ceiling.set(AgentBudgetDimension::Tokens, Some(1_000_000));
        let error = default_policy()
            .with_goal_window(AgentGoalWindowCeiling {
                window: AgentBudgetWindow::Rolling {
                    length_millis: 3_600_000,
                },
                ceiling,
            })
            .expect_err("a ceiling on an unbounded epoch dimension is refused");
        assert_eq!(error.code(), "wake-window-epoch-unbounded");
    }

    #[test]
    fn a_retry_is_consumed_without_admitting_and_its_identity_is_pinned() {
        let policy = default_policy();
        let mut controller = AgentWakeControllerState::new();
        let retry = AgentWakeBinding::new(
            tenant(),
            goal(),
            ScheduleRevision::INITIAL,
            AgentWakeOccurrence::Retry {
                due_at: AgentTimestampMillis::new(1_753_500_000_000),
                cause: AgentWakeRewakeCause::Backoff,
                attempt: 0,
            },
            AgentWakeTriggerKind::Controller,
            AgentTimestampMillis::new(1_753_500_000_001),
            AgentRevisionNumber::INITIAL,
        )
        .expect("the retry binding is valid");

        // Pinned golden vector: the retry derivation is a persisted
        // compatibility surface like every other occurrence kind's.
        assert_eq!(
            retry.wake_id().as_str(),
            wake_id_for_occurrence(
                &tenant(),
                &goal(),
                ScheduleRevision::INITIAL,
                retry.occurrence()
            )
            .expect("the retry wake derives")
            .as_str(),
        );
        assert_eq!(
            retry.occurrence().identity_value(),
            "backoff:1753500000000",
            "the retry identity is its cause and computed due time"
        );

        // The controller trigger needs no policy declaration, and the retry
        // dispositions as consumed — no admission, no watermark movement.
        let disposition = controller
            .admit(
                &policy,
                ScheduleRevision::INITIAL,
                retry.clone(),
                now(1_753_500_000_002),
            )
            .expect("the retry is dispositioned");
        assert!(matches!(disposition, AgentWakeDisposition::Retried { .. }));
        assert_eq!(controller.counters().retried, 1);
        assert!(controller.active().is_empty());
        let fresh = controller
            .admit(
                &policy,
                ScheduleRevision::INITIAL,
                scheduled_binding(1_000, ScheduleRevision::INITIAL),
                now(1_753_500_000_003),
            )
            .expect("an earlier-due scheduled occurrence still admits");
        assert!(
            matches!(fresh, AgentWakeDisposition::Admitted { .. }),
            "the retry advanced no watermark, got {fresh:?}"
        );

        // A duplicate scan of the same retry answers from the ring.
        let duplicate = controller
            .admit(
                &policy,
                ScheduleRevision::INITIAL,
                retry,
                now(1_753_500_000_004),
            )
            .expect("the redelivered retry is answered");
        assert!(matches!(duplicate, AgentWakeDisposition::Duplicate { .. }));
    }

    #[test]
    fn an_early_retry_consume_re_arms_the_slot_under_the_next_attempt() {
        // A scanner host whose clock runs ahead delivers the backoff retry
        // while the backoff is still in force here: the consume burns the
        // parked entry, so the slot must re-owe itself under a fresh identity
        // or the parked occurrence strands behind a terminal timer entry.
        let policy = default_policy();
        let mut controller = AgentWakeControllerState::new();
        controller
            .admit(
                &policy,
                ScheduleRevision::INITIAL,
                scheduled_binding(1_000, ScheduleRevision::INITIAL),
                now(1_000),
            )
            .expect("the first occurrence admits");
        controller
            .admit(
                &policy,
                ScheduleRevision::INITIAL,
                scheduled_binding(2_000, ScheduleRevision::INITIAL),
                now(2_000),
            )
            .expect("the second occurrence coalesces");
        let wake = controller.active()[0].binding().wake_id().clone();
        controller.record_epoch_outcome(&policy, AgentEpochOutcomeClass::Failed, now(3_000));
        controller
            .release(&policy, &wake, now(3_001))
            .expect("the failed epoch releases");
        controller.ensure_rewakes(&policy, now(3_001));
        let until = controller
            .lifecycle()
            .backoff_until()
            .expect("the backoff is in force");
        let slot = controller
            .lifecycle()
            .rewakes()
            .backoff
            .expect("the backoff re-wake is owed");
        assert_eq!(slot.attempt, 0);
        controller.mark_rewake_parked(AgentWakeRewakeCause::Backoff, until, 0);

        // The early delivery: consumed, nothing promoted, and the slot
        // re-armed unparked under attempt 1 — a distinct wake identity.
        let early = AgentWakeBinding::new(
            tenant(),
            goal(),
            ScheduleRevision::INITIAL,
            AgentWakeOccurrence::Retry {
                due_at: until,
                cause: AgentWakeRewakeCause::Backoff,
                attempt: 0,
            },
            AgentWakeTriggerKind::Controller,
            now(3_002),
            AgentRevisionNumber::INITIAL,
        )
        .expect("the retry binding derives");
        let disposition = controller
            .admit(
                &policy,
                ScheduleRevision::INITIAL,
                early.clone(),
                now(3_002),
            )
            .expect("the early retry is consumed");
        assert!(matches!(disposition, AgentWakeDisposition::Retried { .. }));
        assert_eq!(controller.pending().len(), 1, "nothing promoted early");
        controller.ensure_rewakes(&policy, now(3_003));
        let slot = controller
            .lifecycle()
            .rewakes()
            .backoff
            .expect("the slot survives the early consume");
        assert_eq!(slot.due_at, until, "the due time is unchanged");
        assert_eq!(slot.attempt, 1, "the consume bumped the generation");
        assert!(!slot.parked, "the re-armed slot owes parking again");
        let re_armed = AgentWakeOccurrence::Retry {
            due_at: until,
            cause: AgentWakeRewakeCause::Backoff,
            attempt: 1,
        };
        assert_eq!(
            re_armed.identity_value(),
            format!("backoff:{}:1", until.as_millis()),
            "the attempt is part of the identity"
        );
        assert_ne!(
            wake_id_for_occurrence(&tenant(), &goal(), ScheduleRevision::INITIAL, &re_armed)
                .expect("the re-armed wake derives"),
            *early.wake_id(),
            "the re-park derives a wake the fired entry cannot absorb"
        );

        // A redelivery of the consumed generation answers from the ring and
        // leaves the re-armed slot alone; a stale-generation mark is a no-op,
        // while the current generation's mark parks it.
        let duplicate = controller
            .admit(&policy, ScheduleRevision::INITIAL, early, now(3_004))
            .expect("the redelivered retry is answered");
        assert!(matches!(duplicate, AgentWakeDisposition::Duplicate { .. }));
        controller.mark_rewake_parked(AgentWakeRewakeCause::Backoff, until, 0);
        let slot = controller
            .lifecycle()
            .rewakes()
            .backoff
            .expect("the slot survives");
        assert_eq!(slot.attempt, 1);
        assert!(!slot.parked, "a stale-generation mark is a no-op");
        controller.mark_rewake_parked(AgentWakeRewakeCause::Backoff, until, 1);
        assert!(
            controller
                .lifecycle()
                .rewakes()
                .backoff
                .expect("the slot survives")
                .parked
        );

        // Once the backoff elapses the retry's own transition promotes and
        // the slot clears.
        assert!(controller
            .promote_admittable(&policy, now(until.as_millis() + 1))
            .is_some());
        controller.ensure_rewakes(&policy, now(until.as_millis() + 1));
        assert!(controller.lifecycle().rewakes().backoff.is_none());
    }

    #[test]
    fn retry_and_controller_trigger_require_each_other() {
        let policy = default_policy();
        let mut controller = AgentWakeControllerState::new();

        // A retry smuggled through a declared trigger is refused.
        let smuggled_retry = AgentWakeBinding::new(
            tenant(),
            goal(),
            ScheduleRevision::INITIAL,
            AgentWakeOccurrence::Retry {
                due_at: AgentTimestampMillis::new(5_000),
                cause: AgentWakeRewakeCause::WindowTurn,
                attempt: 0,
            },
            AgentWakeTriggerKind::DurableTimer,
            AgentTimestampMillis::new(5_001),
            AgentRevisionNumber::INITIAL,
        )
        .expect("the binding is valid");
        let error = controller
            .admit(
                &policy,
                ScheduleRevision::INITIAL,
                smuggled_retry,
                now(5_002),
            )
            .expect_err("a retry without the controller trigger is refused");
        assert_eq!(error.code(), "wake-retry-trigger-mismatch");

        // A scheduled occurrence smuggled in as the controller is refused.
        let smuggled_schedule = AgentWakeBinding::new(
            tenant(),
            goal(),
            ScheduleRevision::INITIAL,
            AgentWakeOccurrence::Scheduled {
                due_at: AgentTimestampMillis::new(6_000),
            },
            AgentWakeTriggerKind::Controller,
            AgentTimestampMillis::new(6_001),
            AgentRevisionNumber::INITIAL,
        )
        .expect("the binding is valid");
        let error = controller
            .admit(
                &policy,
                ScheduleRevision::INITIAL,
                smuggled_schedule,
                now(6_002),
            )
            .expect_err("the controller trigger carries only retries");
        assert_eq!(error.code(), "wake-retry-trigger-mismatch");
    }

    #[test]
    fn the_backoff_delay_grows_geometrically_and_saturates() {
        let policy = AgentWakeBackoffPolicy::DEFAULT;
        let table = [
            (1, 1_000),
            (2, 2_000),
            (3, 4_000),
            (4, 8_000),
            (12, 2_048_000),
            (13, 3_600_000),
            (30, 3_600_000),
        ];
        for (failures, expected) in table {
            assert_eq!(
                AgentWakeControllerState::backoff_delay_millis(&policy, failures),
                expected,
                "delay after {failures} failures"
            );
        }
    }

    #[test]
    fn a_failed_epoch_engages_backoff_and_completion_resets_it() {
        let policy = default_policy();
        let mut controller = AgentWakeControllerState::new();

        let escalated =
            controller.record_epoch_outcome(&policy, AgentEpochOutcomeClass::Failed, now(10_000));
        assert!(!escalated);
        assert_eq!(controller.lifecycle().consecutive_failures(), 1);
        assert_eq!(
            controller.lifecycle().backoff_until(),
            Some(AgentTimestampMillis::new(11_000))
        );

        // While the backoff is in force, every delivery parks and nothing
        // promotes.
        let parked = controller
            .admit(
                &policy,
                ScheduleRevision::INITIAL,
                scheduled_binding(10_500, ScheduleRevision::INITIAL),
                now(10_500),
            )
            .expect("the delivery is dispositioned");
        assert!(matches!(parked, AgentWakeDisposition::BackedOff { .. }));
        assert_eq!(controller.counters().backed_off, 1);
        assert!(controller
            .promote_admittable(&policy, now(10_600))
            .is_none());

        // Once it elapses, promotion runs again.
        assert!(controller
            .promote_admittable(&policy, now(11_001))
            .is_some());

        // A cancellation neither grows nor resets; a completion resets.
        controller.record_epoch_outcome(&policy, AgentEpochOutcomeClass::Cancelled, now(12_000));
        assert_eq!(controller.lifecycle().consecutive_failures(), 1);
        controller.record_epoch_outcome(&policy, AgentEpochOutcomeClass::Completed, now(12_500));
        assert_eq!(controller.lifecycle().consecutive_failures(), 0);
        assert!(controller.lifecycle().backoff_until().is_none());
    }

    #[test]
    fn escalation_auto_suspends_after_the_threshold() {
        let policy = default_policy()
            .with_failure_backoff(AgentWakeBackoffPolicy {
                escalate_after_failures: Some(2),
                ..AgentWakeBackoffPolicy::DEFAULT
            })
            .expect("the backoff policy is valid");
        let mut controller = AgentWakeControllerState::new();
        let before = controller.lifecycle().lifecycle_revision();

        assert!(!controller.record_epoch_outcome(
            &policy,
            AgentEpochOutcomeClass::Failed,
            now(1_000)
        ));
        assert!(controller.record_epoch_outcome(
            &policy,
            AgentEpochOutcomeClass::Failed,
            now(2_000)
        ));
        assert_eq!(
            controller.lifecycle().status(),
            AgentGoalLifecycleStatus::Suspended
        );
        assert_eq!(controller.lifecycle().lifecycle_revision(), before.next());
        assert!(controller
            .lifecycle()
            .suspended_reason()
            .is_some_and(|reason| reason.contains("escalated")));
        // A racing operator command carrying the pre-escalation revision is
        // fenced.
        let error = controller
            .resume(before, provenance())
            .expect_err("the stale resume is fenced");
        assert_eq!(error.code(), "wake-stale-lifecycle-revision");
    }

    #[test]
    fn lifecycle_commands_fence_on_the_revision() {
        let mut controller = AgentWakeControllerState::new();
        let initial = controller.lifecycle().lifecycle_revision();

        let error = controller
            .suspend(initial.next(), None, provenance())
            .expect_err("a wrong expected revision is fenced");
        assert_eq!(error.code(), "wake-stale-lifecycle-revision");

        let suspended = controller
            .suspend(initial, Some("maintenance".to_string()), provenance())
            .expect("the suspend applies");
        assert_eq!(suspended, initial.next());
        assert_eq!(
            controller.lifecycle().status(),
            AgentGoalLifecycleStatus::Suspended
        );

        let error = controller
            .resume(initial, provenance())
            .expect_err("the pre-suspension revision is stale");
        assert_eq!(error.code(), "wake-stale-lifecycle-revision");
        let resumed = controller
            .resume(suspended, provenance())
            .expect("the resume applies");
        assert_eq!(
            controller.lifecycle().status(),
            AgentGoalLifecycleStatus::Active
        );

        let error = controller
            .resume(resumed, provenance())
            .expect_err("resuming an active goal is refused");
        assert_eq!(error.code(), "wake-not-suspended");

        let retired = controller
            .retire(resumed, provenance())
            .expect("the retire applies");
        assert_eq!(
            controller.lifecycle().status(),
            AgentGoalLifecycleStatus::Retired
        );
        let error = controller
            .suspend(retired, None, provenance())
            .expect_err("an absorbing status accepts no transition");
        assert_eq!(error.code(), "wake-lifecycle-terminal");
    }

    #[test]
    fn suspension_parks_or_drops_per_policy() {
        let coalesce_policy = default_policy();
        let mut controller = AgentWakeControllerState::new();
        controller
            .suspend(
                controller.lifecycle().lifecycle_revision(),
                None,
                provenance(),
            )
            .expect("the suspend applies");

        let parked = controller
            .admit(
                &coalesce_policy,
                ScheduleRevision::INITIAL,
                scheduled_binding(1_000, ScheduleRevision::INITIAL),
                now(1_000),
            )
            .expect("the delivery is dispositioned");
        assert!(matches!(
            parked,
            AgentWakeDisposition::SuspendedParked { .. }
        ));
        assert_eq!(controller.counters().suspended, 1);
        assert_eq!(controller.pending().len(), 1);

        let drop_policy = default_policy()
            .with_lifecycle(AgentWakeLifecyclePolicy {
                while_suspended: AgentWakeSuspensionPolicy::Drop,
                ..AgentWakeLifecyclePolicy::DEFAULT
            })
            .expect("the drop policy is valid");
        let dropped = controller
            .admit(
                &drop_policy,
                ScheduleRevision::INITIAL,
                scheduled_binding(2_000, ScheduleRevision::INITIAL),
                now(2_000),
            )
            .expect("the delivery is dispositioned");
        assert!(matches!(dropped, AgentWakeDisposition::Dropped { .. }));
        assert_eq!(controller.counters().dropped, 1);
        assert_eq!(controller.pending().len(), 1, "a drop parks nothing");

        // Resume promotes what the suspension parked (via the entity's
        // promotion; here the primitive itself).
        let revision = controller.lifecycle().lifecycle_revision();
        controller
            .resume(revision, provenance())
            .expect("the resume applies");
        assert!(controller
            .promote_admittable(&coalesce_policy, now(3_000))
            .is_some());
    }

    #[test]
    fn renewal_windows_are_enforced() {
        let policy = default_policy()
            .with_lifecycle(AgentWakeLifecyclePolicy {
                renewal: AgentWakeRenewalPolicy::RequiredBefore {
                    window_millis: 10_000,
                },
                expires_at: Some(AgentTimestampMillis::new(100_000)),
                ..AgentWakeLifecyclePolicy::DEFAULT
            })
            .expect("the renewal policy is valid");
        let mut controller = AgentWakeControllerState::new();
        let revision = controller.lifecycle().lifecycle_revision();

        // Too early: the window opens at 90_000.
        let error = controller
            .renew(
                revision,
                &policy,
                AgentTimestampMillis::new(200_000),
                provenance(),
                now(50_000),
            )
            .expect_err("a renewal before the window is refused");
        assert_eq!(error.code(), "wake-renewal-outside-window");

        // Inside the window, but not extending.
        let error = controller
            .renew(
                revision,
                &policy,
                AgentTimestampMillis::new(99_000),
                provenance(),
                now(95_000),
            )
            .expect_err("a non-extending renewal is refused");
        assert_eq!(error.code(), "wake-renewal-not-extending");

        // A proper renewal extends the effective expiry.
        controller
            .renew(
                revision,
                &policy,
                AgentTimestampMillis::new(200_000),
                provenance(),
                now(95_000),
            )
            .expect("the renewal applies");
        assert_eq!(
            controller
                .lifecycle()
                .effective_expires_at(&policy.lifecycle),
            Some(AgentTimestampMillis::new(200_000))
        );
        // The old expiry passing no longer expires the goal.
        assert!(controller
            .observe_lifecycle(&policy, now(150_000))
            .is_none());
        // The extended one does.
        assert_eq!(
            controller.observe_lifecycle(&policy, now(200_000)),
            Some(AgentGoalLifecycleStatus::Expired)
        );
        // Absorbing: nothing further transitions, and deliveries are barred.
        assert!(controller
            .observe_lifecycle(&policy, now(300_000))
            .is_none());
        let barred = controller
            .admit(
                &policy,
                ScheduleRevision::INITIAL,
                scheduled_binding(210_000, ScheduleRevision::INITIAL),
                now(300_000),
            )
            .expect("the delivery is dispositioned");
        assert!(matches!(barred, AgentWakeDisposition::Barred { .. }));
        assert_eq!(controller.counters().barred, 1);
    }

    #[test]
    fn retirement_is_observed_by_count_and_by_time() {
        let by_count = default_policy()
            .with_lifecycle(AgentWakeLifecyclePolicy {
                retirement: AgentWakeRetirementPolicy::AfterOccurrences { occurrences: 1 },
                ..AgentWakeLifecyclePolicy::DEFAULT
            })
            .expect("the retirement policy is valid");
        let mut controller = AgentWakeControllerState::new();
        assert!(controller.observe_lifecycle(&by_count, now(500)).is_none());
        controller
            .admit(
                &by_count,
                ScheduleRevision::INITIAL,
                scheduled_binding(1_000, ScheduleRevision::INITIAL),
                now(1_000),
            )
            .expect("the occurrence admits");
        assert_eq!(
            controller.observe_lifecycle(&by_count, now(1_100)),
            Some(AgentGoalLifecycleStatus::Retired)
        );

        let by_time = default_policy()
            .with_lifecycle(AgentWakeLifecyclePolicy {
                retirement: AgentWakeRetirementPolicy::At {
                    at: AgentTimestampMillis::new(50_000),
                },
                ..AgentWakeLifecyclePolicy::DEFAULT
            })
            .expect("the retirement policy is valid");
        let mut controller = AgentWakeControllerState::new();
        assert!(controller
            .observe_lifecycle(&by_time, now(49_999))
            .is_none());
        assert_eq!(
            controller.observe_lifecycle(&by_time, now(50_000)),
            Some(AgentGoalLifecycleStatus::Retired)
        );
    }

    #[test]
    fn ensure_rewakes_owes_and_heals_the_slots() {
        // Window-turn slot: a deferred occurrence on an exhausted window owes
        // a retry at the boundary. The ceiling pays for exactly one epoch per
        // window.
        let windowed = windowed_policy(3_600_000, 16);
        let mut controller = AgentWakeControllerState::new();
        controller
            .admit(
                &windowed,
                ScheduleRevision::INITIAL,
                scheduled_binding(1_000, ScheduleRevision::INITIAL),
                now(1_000),
            )
            .expect("the first occurrence admits");
        let wake = controller.active()[0].binding().wake_id().clone();
        controller
            .release(&windowed, &wake, now(2_000))
            .expect("the epoch releases");
        let deferred = controller
            .admit(
                &windowed,
                ScheduleRevision::INITIAL,
                scheduled_binding(3_000, ScheduleRevision::INITIAL),
                now(3_000),
            )
            .expect("the second occurrence is dispositioned");
        assert!(matches!(deferred, AgentWakeDisposition::Deferred { .. }));

        controller.ensure_rewakes(&windowed, now(3_000));
        let slot = controller
            .lifecycle()
            .rewakes()
            .window_turn
            .expect("the window-turn re-wake is owed");
        assert_eq!(slot.due_at, AgentTimestampMillis::new(1_000 + 3_600_000));
        assert!(!slot.parked);
        assert!(controller.lifecycle().rewakes().owes_parking());

        // Parking marks it; an unchanged recomputation keeps the mark.
        controller.mark_rewake_parked(AgentWakeRewakeCause::WindowTurn, slot.due_at, slot.attempt);
        controller.ensure_rewakes(&windowed, now(4_000));
        assert!(
            controller
                .lifecycle()
                .rewakes()
                .window_turn
                .expect("the slot survives")
                .parked
        );

        // Once the window can pay again — the retry's own transition promotes
        // and the queue drains — the slot clears.
        assert!(controller
            .promote_admittable(&windowed, now(1_000 + 3_600_000 + 1))
            .is_some());
        controller.ensure_rewakes(&windowed, now(1_000 + 3_600_000 + 1));
        assert!(controller.lifecycle().rewakes().window_turn.is_none());

        // Backoff slot: a failure with something parked owes the retry at
        // backoff_until; suspension clears every slot.
        let policy = default_policy();
        let mut controller = AgentWakeControllerState::new();
        controller
            .admit(
                &policy,
                ScheduleRevision::INITIAL,
                scheduled_binding(1_000, ScheduleRevision::INITIAL),
                now(1_000),
            )
            .expect("the first occurrence admits");
        controller
            .admit(
                &policy,
                ScheduleRevision::INITIAL,
                scheduled_binding(2_000, ScheduleRevision::INITIAL),
                now(2_000),
            )
            .expect("the second occurrence coalesces");
        let wake = controller.active()[0].binding().wake_id().clone();
        // The failure's own transition releases before promotion is gated by
        // the fresh backoff, leaving the parked occurrence waiting.
        controller.record_epoch_outcome(&policy, AgentEpochOutcomeClass::Failed, now(3_000));
        controller
            .release(&policy, &wake, now(3_001))
            .expect("the failed epoch releases");
        assert_eq!(controller.pending().len(), 1, "the backoff holds promotion");
        controller.ensure_rewakes(&policy, now(3_001));
        let slot = controller
            .lifecycle()
            .rewakes()
            .backoff
            .expect("the backoff re-wake is owed");
        assert_eq!(slot.due_at, AgentTimestampMillis::new(4_000));

        controller
            .suspend(
                controller.lifecycle().lifecycle_revision(),
                None,
                provenance(),
            )
            .expect("the suspend applies");
        controller.ensure_rewakes(&policy, now(3_500));
        assert!(controller.lifecycle().rewakes().backoff.is_none());
    }

    #[test]
    fn a_controller_state_beyond_its_bounds_fails_closed_on_load() {
        let mut value = serde_json::to_value(AgentWakeControllerState::new())
            .expect("controller state should serialize");
        let overflow: Vec<_> = (0..AGENT_WAKE_RECENT_CAPACITY + 1)
            .map(|index| {
                let binding = scheduled_binding(1_000 + index as u64, ScheduleRevision::INITIAL);
                serde_json::to_value(binding.wake_id()).expect("wake id should serialize")
            })
            .collect();
        value["recent"] = serde_json::Value::Array(overflow);
        let error = serde_json::from_value::<AgentWakeControllerState>(value)
            .expect_err("a state beyond its bounds should fail closed on load");
        assert!(error.to_string().contains("recent wake identities"));

        let empty: AgentWakeControllerState =
            serde_json::from_value(serde_json::json!({})).expect("an empty record loads");
        assert_eq!(empty, AgentWakeControllerState::new());
    }
}
