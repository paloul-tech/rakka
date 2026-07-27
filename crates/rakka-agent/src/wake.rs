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

use crate::budget::AgentBudgetAllocation;
use crate::definition::{AgentPolicyRef, AgentRevisionNumber, AgentRevisionProvenance};
use crate::identity::{
    validate_tenant, validated_id, AgentGoalId, AgentIdentityError, AgentOperationId,
    AgentOperationKind, AgentWakeId, TenantId,
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
        }
    }

    /// When the occurrence was due, for the kinds that have a logical due time.
    #[must_use]
    pub const fn due_at(&self) -> Option<AgentTimestampMillis> {
        match self {
            Self::Scheduled { due_at } => Some(*due_at),
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
}

impl AgentWakeTriggerKind {
    /// Every trigger class, in stable order.
    pub const ALL: [Self; 4] = [
        Self::DurableTimer,
        Self::ExternalEvent,
        Self::A2aCommand,
        Self::Callback,
    ];

    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::DurableTimer => "durable-timer",
            Self::ExternalEvent => "external-event",
            Self::A2aCommand => "a2a-command",
            Self::Callback => "callback",
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

/// One admitted occurrence currently owning execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentActiveWake {
    binding: AgentWakeBinding,
    admitted_at: AgentTimestampMillis,
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
        if !policy.allows_trigger(binding.trigger()) {
            return Err(AgentWakeError::TriggerNotAllowed {
                trigger: binding.trigger(),
            });
        }
        let wake = binding.wake_id().clone();
        if self.contains(&wake) {
            return Ok(AgentWakeDisposition::Duplicate { wake });
        }
        if let Some(due_at) = binding.due_at() {
            if self
                .scheduled_watermark
                .is_some_and(|watermark| due_at.as_millis() <= watermark.as_millis())
            {
                return Ok(AgentWakeDisposition::Duplicate { wake });
            }
        }
        if offered < current_revision {
            self.note_seen(&binding);
            self.counters.fenced += 1;
            return Ok(AgentWakeDisposition::Fenced {
                wake,
                offered,
                current: current_revision,
            });
        }
        let lateness = binding
            .due_at()
            .map(|due_at| now.as_millis().saturating_sub(due_at.as_millis()));
        let missed = matches!(
            (lateness, policy.maximum_lateness_millis),
            (Some(late), Some(maximum)) if late > maximum
        );
        if missed {
            return Ok(self.dispose_missed(policy, binding, now));
        }
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
            Ok(self.admit_binding(binding, now, false))
        } else {
            Ok(self.coalesce(policy, binding))
        }
    }

    /// Releases an active occurrence, promoting the oldest parked occurrence
    /// into the freed slot.
    ///
    /// The promotion happens inside this same transition: the coalesced
    /// occurrence's epoch follows the released one without any further
    /// trigger, which is what keeps the default overlap policy live.
    pub fn release(
        &mut self,
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
        self.active.remove(index);
        self.counters.released += 1;
        let admitted_next = if self.pending.is_empty() {
            None
        } else {
            let binding = self.pending.remove(0);
            let next = binding.wake_id().clone();
            self.admit_binding(binding, now, true);
            Some(next)
        };
        Ok(AgentWakeRelease {
            released: wake.clone(),
            admitted_next,
        })
    }

    /// Fences every parked occurrence constructed under a revision older than
    /// the one now in force, returning how many were fenced.
    ///
    /// A schedule update calls this so an occurrence the old schedule parked
    /// can never admit an epoch the new schedule did not issue. Active
    /// occurrences are untouched: they were already admitted.
    pub fn fence_obsolete_pending(&mut self, current_revision: ScheduleRevision) -> u64 {
        let before = self.pending.len();
        self.pending
            .retain(|binding| binding.schedule_revision() >= current_revision);
        let fenced = (before - self.pending.len()) as u64;
        self.counters.fenced += fenced;
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
                self.coalesce_or_admit(policy, binding, now)
            }
            AgentMissedOccurrencePolicy::Skip => {
                let wake = binding.wake_id().clone();
                self.note_seen(&binding);
                self.counters.missed += 1;
                AgentWakeDisposition::Skipped { wake }
            }
            AgentMissedOccurrencePolicy::BoundedCatchUp { .. } => {
                if self.active.len() < Self::active_capacity(policy) {
                    self.admit_binding(binding, now, true)
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
            self.admit_binding(binding, now, true)
        } else {
            self.coalesce(policy, binding)
        }
    }

    fn coalesce(
        &mut self,
        policy: &AgentWakePolicy,
        binding: AgentWakeBinding,
    ) -> AgentWakeDisposition {
        let wake = binding.wake_id().clone();
        self.note_seen(&binding);
        let capacity = Self::pending_capacity(policy);
        if self.pending.len() < capacity {
            self.pending.push(binding);
            self.counters.coalesced += 1;
            AgentWakeDisposition::Coalesced {
                wake,
                replaced: None,
            }
        } else if capacity == 1 {
            // The default single coalescing slot: the latest occurrence wins,
            // which is the "at most one pending occurrence" the resolved
            // defaults promise.
            let replaced = self.pending[0].wake_id().clone();
            self.pending[0] = binding;
            self.counters.coalesced += 1;
            self.counters.superseded += 1;
            AgentWakeDisposition::Coalesced {
                wake,
                replaced: Some(replaced),
            }
        } else {
            // A full catch-up queue is the bound the policy declared: the
            // overflow is skipped, never silently kept.
            self.counters.missed += 1;
            AgentWakeDisposition::Skipped { wake }
        }
    }

    fn admit_binding(
        &mut self,
        binding: AgentWakeBinding,
        now: AgentTimestampMillis,
        coalesced: bool,
    ) -> AgentWakeDisposition {
        let wake = binding.wake_id().clone();
        self.note_seen(&binding);
        self.active.push(AgentActiveWake {
            binding,
            admitted_at: now,
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

    fn note_seen(&mut self, binding: &AgentWakeBinding) {
        self.recent.push(binding.wake_id().clone());
        if self.recent.len() > AGENT_WAKE_RECENT_CAPACITY {
            self.recent.remove(0);
        }
        if let Some(due_at) = binding.due_at() {
            let advanced = self
                .scheduled_watermark
                .is_none_or(|watermark| due_at.as_millis() > watermark.as_millis());
            if advanced {
                self.scheduled_watermark = Some(due_at);
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
    /// A release of a wake that is not active.
    NotActive {
        /// The wake that was not active.
        wake: AgentWakeId,
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
            Self::EpochUnbounded => "wake-epoch-unbounded",
            Self::RevisionAhead { .. } => "wake-revision-ahead",
            Self::TriggerNotAllowed { .. } => "wake-trigger-not-allowed",
            Self::NotActive { .. } => "wake-not-active",
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
            Self::NotActive { wake } => {
                write!(f, "the wake {wake} is not an active occurrence")
            }
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
            .release(&first_wake, now(5_000))
            .expect("the active occurrence should release");
        assert_eq!(release.released, first_wake);
        assert_eq!(release.admitted_next, Some(second_wake.clone()));
        assert_eq!(controller.active().len(), 1);
        assert_eq!(controller.active()[0].binding().wake_id(), &second_wake);
        assert!(controller.pending().is_empty());
        assert_eq!(controller.counters().admitted, 2);
        assert_eq!(controller.counters().released, 1);

        let error = controller
            .release(&first_wake, now(5_001))
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
            .release(&wake, now(10_000))
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
            .release(&active, now(140_000))
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

        let fenced = controller.fence_obsolete_pending(ScheduleRevision::INITIAL.next());
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
