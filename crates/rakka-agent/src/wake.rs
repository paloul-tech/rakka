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
//! The durable wake controller, scanners, and coalescing runtime built over
//! this contract land with slice 3.2; epoch admission and the window-refill
//! transition with slice 3.3. Scanner and pod uptime never create an
//! occurrence; only durable logical time does.

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
}
