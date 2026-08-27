//! Discharging a terminal run's short-term memory retention.
//!
//! The stores have held `purge_run` since slice 2.1, and until this slice
//! nothing called it: retention was a capability with no caller, so the
//! *composition* — deciding a run is terminal, knowing when it went terminal,
//! purging both tiers in the right order, and staying idempotent under
//! re-drive — had never been exercised anywhere.
//!
//! Specification: sections 13.1 ("provide retention, tombstone, and deletion
//! semantics"), 13.2 (terminal-run retention is tenant policy), and 13.5
//! (snapshots embed copies of what they were assembled from); open decision 7.
//!
//! # What is discharged, and what is not
//!
//! Session memory and context snapshots. Both are scoped to one
//! [`AgentRunScope`] and both exist to serve one run's execution, so a
//! terminal run's retention window is the right clock for both.
//!
//! Agent-private long-term memory is deliberately untouched. It is scoped
//! `(TenantId, AgentId)`, it outlives every run that contributed to it, and
//! its retention is per-record — [`crate::memory::AgentPrivateMemoryStore`]'s
//! `purge_expired`, `tombstone`, and `delete`. Deleting an agent's memories
//! because one of its runs aged out would destroy exactly what promotion
//! exists to preserve.
//!
//! # Order: the derived tier first
//!
//! Snapshots are purged before session rows, and the order is load-bearing.
//! A snapshot embeds *copies* of the session entries and private content it
//! was assembled from, so a process killed between the two calls must leave
//! behind the copy something else can still sweep. Session-then-snapshots
//! would leave the content the policy said to delete alive in the
//! **immutable** tier, which no later session purge reaches. Both calls are
//! idempotent, so a re-drive completes an interrupted discharge.
//!
//! # An erasure request is not discharged by a private delete alone
//!
//! A private memory tombstoned or deleted after a snapshot embedded it keeps
//! that embedded copy until the run's snapshot purge. That is required, not
//! an oversight: [specification 13.5](../../../docs/plans/rakka-agent/spec.md)
//! makes a model-effect retry read the *same* snapshot, so a store that
//! scrubbed embedded content on withdrawal would make the immutable tier
//! mutable and break scenario 17's determinism.
//!
//! The operational consequence, stated so a deployment does not discover it:
//! **erasing a subject's content means purging the snapshots of every run
//! whose snapshot embedded it, as well as deleting the private record.** The
//! primitive is [`discharge_run_memory_retention`] per run; the exposure is
//! bounded by [`crate::memory::SessionRetentionPolicy::retain_for_millis`],
//! 30 days by default. Rakka does not maintain a reverse index from memory to
//! snapshot: it would have to be kept transactionally across two stores a
//! deployment may place in different backends, and it would reintroduce
//! mutability into the tier whose immutability the retry rule depends on.

use rakka_agent_workflow::AgentTimestampMillis;
use rakka_persistence::{DurableError, DurableStateStore};

use crate::choreography::AgentExchangeState;
use crate::identity::AgentRunScope;
use crate::memory::{AgentRunMemory, MemoryError, SessionPurgeOutcome, SessionRetentionPolicy};
use crate::run::{load_agent_run_state, AgentRunState, AgentRunStatus};
use crate::schema::AgentSchemaPolicy;

/// Reports a run-state store or schema failure as a memory backend failure,
/// so both passes in this module name the same backend for the same fault.
fn run_state_failure(error: &impl std::fmt::Display) -> MemoryError {
    MemoryError::Backend {
        backend: "run-state".to_string(),
        message: error.to_string(),
    }
}

/// What one run's retention discharge did.
///
/// Refusals are values, not errors, so a sweep over many runs reports what it
/// skipped instead of aborting on the first live one — the same argument
/// [`SessionPurgeOutcome::Held`] makes one level down.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AgentRunRetentionOutcome {
    /// Both short-term tiers were addressed; each reports its own outcome.
    Discharged {
        /// What the snapshot purge did.
        snapshots: SessionPurgeOutcome,
        /// What the session purge did.
        session: SessionPurgeOutcome,
    },
    /// The run has not reached a terminal status; nothing was deleted.
    NotTerminal {
        /// The status it is in.
        status: AgentRunStatus,
    },
    /// The run is terminal but carries no terminal timestamp — a record
    /// written before the stamp existed.
    ///
    /// The discharge refuses rather than guessing a due time from
    /// `updated_at`, which is the last *accepted transition* and keeps moving
    /// as settlement lands: a deadline measured from it could recede
    /// indefinitely, which is the opposite of what a retention policy is for.
    ///
    /// This refusal is **permanent** without an explicit repair. The stamp is
    /// written under an already-terminal guard, so a run that was already
    /// terminal when the field shipped never re-enters the transition that
    /// would give it one, and nothing else ever will — its short-term memory
    /// would be retained forever. [`backfill_run_terminal_stamp`] is the
    /// one-time repair, and [`AgentRunTerminalStampBackfill`] runs it over a
    /// batch of scopes; a non-zero
    /// [`AgentMemoryRetentionReport::terminal_time_unknown`] beside a healthy
    /// `discharged` is exactly the signal that a deployment still owes it.
    TerminalTimeUnknown,
    /// No durable record exists for the scope; there is nothing to discharge.
    RunAbsent,
}

impl AgentRunRetentionOutcome {
    /// Stable kebab-case label for metrics and logs.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Discharged { .. } => "discharged",
            Self::NotTerminal { .. } => "not-terminal",
            Self::TerminalTimeUnknown => "terminal-time-unknown",
            Self::RunAbsent => "run-absent",
        }
    }

    /// Whether either tier actually removed a record.
    ///
    /// A discharge that ran but deleted nothing is `false` — including the
    /// replay of an already discharged run, which both tiers answer
    /// `Purged { entries: 0 }`. That distinction is the whole point of the
    /// predicate: a caller that gates a deletion audit entry, an erasure
    /// notification, or a deleted-record count on it must not be told a
    /// deletion happened on every idempotent re-drive.
    #[must_use]
    pub const fn deleted_anything(self) -> bool {
        match self {
            Self::Discharged { snapshots, session } => {
                snapshots.entries_deleted() > 0 || session.entries_deleted() > 0
            }
            Self::NotTerminal { .. } | Self::TerminalTimeUnknown | Self::RunAbsent => false,
        }
    }
}

/// Discharges one terminal run's short-term retention across both tiers.
///
/// Reads the run's durable record to decide whether it is terminal and when it
/// became so, then purges snapshots and session rows in that order under the
/// caller's policy. See the module documentation for why the order matters and
/// why private memory is not touched.
///
/// # Errors
///
/// Propagates a store failure from either tier, and a run-state load failure
/// (including an unreadable schema version) as
/// [`MemoryError::Backend`]. A refusal — live run, missing stamp, absent
/// record — is an `Ok` value, not an error.
pub async fn discharge_run_memory_retention<Runs>(
    runs: &Runs,
    memory: &AgentRunMemory,
    scope: &AgentRunScope,
    policy: &SessionRetentionPolicy,
    schema: &AgentSchemaPolicy,
    now: AgentTimestampMillis,
) -> Result<AgentRunRetentionOutcome, MemoryError>
where
    Runs: DurableStateStore<crate::run::AgentRunState>,
{
    let state = load_agent_run_state(runs, scope, schema)
        .await
        .map_err(|error| run_state_failure(&error))?;
    let Some(state) = state else {
        return Ok(AgentRunRetentionOutcome::RunAbsent);
    };
    let Some(run) = state.run() else {
        return Ok(AgentRunRetentionOutcome::RunAbsent);
    };
    if !run.status.is_terminal() {
        return Ok(AgentRunRetentionOutcome::NotTerminal { status: run.status });
    }
    let Some(terminal_at) = run.terminal_at else {
        return Ok(AgentRunRetentionOutcome::TerminalTimeUnknown);
    };

    // The derived tier first: see the module doc.
    let snapshots = memory
        .snapshots()
        .purge_run(scope, policy, terminal_at, now)
        .await?;
    let session = memory
        .session()
        .purge_run(scope, policy, terminal_at, now)
        .await?;

    Ok(AgentRunRetentionOutcome::Discharged { snapshots, session })
}

/// What one sweep over many runs did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct AgentMemoryRetentionReport {
    /// Runs whose retention was discharged.
    pub discharged: usize,
    /// Runs still live.
    pub not_terminal: usize,
    /// Terminal runs carrying no terminal timestamp.
    pub terminal_time_unknown: usize,
    /// Scopes with no durable run record.
    pub absent: usize,
    /// Runs held by a legal hold on either tier, counted once per *run* —
    /// never once per tier, or a fleet of ten held runs would report twenty
    /// against ten discharged.
    pub held: usize,
    /// Runs whose retention window has not elapsed on either tier, counted
    /// once per run. This and [`Self::held`] may both count the same run: one
    /// tier can be under a hold while the other is simply not yet due.
    pub not_yet_due: usize,
    /// Records deleted across both tiers.
    pub records_deleted: u64,
}

/// A bounded, deployment-invoked retention pass over caller-supplied scopes.
///
/// Rakka keeps no index of runs by terminal time — enumeration belongs to the
/// application, exactly as tenant policy does — so the sweep takes the scopes
/// it is given. There is no resident sweeper: this is the same
/// deployment-invokes-it shape [`crate::wake_scanner::AgentWakeScanner`] and
/// `purge_expired` already follow, and for the same reason. A resident poller
/// would be a per-agent live task, which
/// [specification 6.11](../../../docs/plans/rakka-agent/spec.md) forbids.
#[derive(Debug, Clone)]
pub struct AgentMemoryRetentionSweep<Runs> {
    runs: Runs,
    memory: AgentRunMemory,
    schema: AgentSchemaPolicy,
    max_batch: usize,
}

impl<Runs> AgentMemoryRetentionSweep<Runs>
where
    Runs: DurableStateStore<crate::run::AgentRunState>,
{
    /// The most scopes one pass will visit.
    pub const DEFAULT_MAX_BATCH: usize = 128;

    /// A sweep over the given run store and memory bundle.
    #[must_use]
    pub fn new(runs: Runs, memory: AgentRunMemory) -> Self {
        Self {
            runs,
            memory,
            schema: AgentSchemaPolicy::default(),
            max_batch: Self::DEFAULT_MAX_BATCH,
        }
    }

    /// Uses an explicit schema-compatibility policy for the run states it
    /// reads.
    #[must_use]
    pub fn with_schema_policy(mut self, schema: AgentSchemaPolicy) -> Self {
        self.schema = schema;
        self
    }

    /// Bounds how many scopes one pass visits.
    #[must_use]
    pub const fn with_max_batch(mut self, max_batch: usize) -> Self {
        self.max_batch = if max_batch == 0 { 1 } else { max_batch };
        self
    }

    /// Discharges retention for each scope, reporting per-outcome counts.
    ///
    /// A refusal on one run never stops the pass; a *store failure* does,
    /// because a sweep that silently continued past a broken backend would
    /// report a discharge it did not perform.
    ///
    /// # Errors
    ///
    /// Propagates the first store failure.
    pub async fn discharge(
        &self,
        scopes: impl IntoIterator<Item = AgentRunScope>,
        policy: &SessionRetentionPolicy,
        now: AgentTimestampMillis,
    ) -> Result<AgentMemoryRetentionReport, MemoryError> {
        let mut report = AgentMemoryRetentionReport::default();
        for scope in scopes.into_iter().take(self.max_batch) {
            let outcome = discharge_run_memory_retention(
                &self.runs,
                &self.memory,
                &scope,
                policy,
                &self.schema,
                now,
            )
            .await?;
            match outcome {
                AgentRunRetentionOutcome::Discharged { snapshots, session } => {
                    report.discharged += 1;
                    // Records are a per-tier quantity and sum across both.
                    for tier in [snapshots, session] {
                        report.records_deleted = report
                            .records_deleted
                            .saturating_add(tier.entries_deleted());
                    }
                    // The two refusal counters are per-*run*, as their docs
                    // say, so they are tested once each rather than summed
                    // over the tiers. They are independent tests, not an
                    // else: one tier may be held while the other is merely
                    // not yet due, and both facts are true of that run.
                    if matches!(snapshots, SessionPurgeOutcome::Held)
                        || matches!(session, SessionPurgeOutcome::Held)
                    {
                        report.held += 1;
                    }
                    if matches!(snapshots, SessionPurgeOutcome::NotYetDue)
                        || matches!(session, SessionPurgeOutcome::NotYetDue)
                    {
                        report.not_yet_due += 1;
                    }
                }
                AgentRunRetentionOutcome::NotTerminal { .. } => report.not_terminal += 1,
                AgentRunRetentionOutcome::TerminalTimeUnknown => {
                    report.terminal_time_unknown += 1;
                }
                AgentRunRetentionOutcome::RunAbsent => report.absent += 1,
            }
        }
        Ok(report)
    }
}

// ---------------------------------------------------------------------------
// The one-time repair: stamping the backlog the once-only guard puts out of
// reach.
// ---------------------------------------------------------------------------

/// What one run's terminal-stamp backfill did.
///
/// Refusals are values for the same reason
/// [`AgentRunRetentionOutcome`]'s are: a migration over a fleet's worth of
/// scopes reports what it skipped rather than aborting on the first record
/// that did not need repairing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AgentRunTerminalStampOutcome {
    /// The record was terminal and unstamped; it now carries this stamp, and
    /// the schema version that can round-trip it.
    Stamped {
        /// The stamp written, derived from the record's `updated_at`.
        terminal_at: AgentTimestampMillis,
    },
    /// The record already carries a stamp; nothing was written. This is what
    /// every re-drive of a completed migration answers.
    AlreadyStamped,
    /// The run has not reached a terminal status, so it is not part of the
    /// backlog: it will stamp itself at its own terminal transition.
    NotTerminal {
        /// The status it is in.
        status: AgentRunStatus,
    },
    /// No durable record exists for the scope.
    RunAbsent,
    /// Another writer moved the record between the read and the write, so
    /// nothing was written. Re-drive the scope: this is a live entity
    /// accepting a settlement command, not a fault.
    Conflicted,
}

impl AgentRunTerminalStampOutcome {
    /// Stable kebab-case label for metrics and logs.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Stamped { .. } => "stamped",
            Self::AlreadyStamped => "already-stamped",
            Self::NotTerminal { .. } => "not-terminal",
            Self::RunAbsent => "run-absent",
            Self::Conflicted => "conflicted",
        }
    }

    /// Whether this scope should be visited again.
    ///
    /// Only a conflict should: every other outcome is a decision about a
    /// record this pass read successfully, and re-reading it would answer the
    /// same thing.
    #[must_use]
    pub const fn should_retry(self) -> bool {
        matches!(self, Self::Conflicted)
    }
}

/// Stamps one pre-upgrade terminal record so its retention can be discharged.
///
/// A run that reached a terminal status *before*
/// [`crate::run::AgentRun::terminal_at`] existed carries no stamp, and the
/// once-only guard on the terminal transition means nothing in normal
/// operation can ever give it one — so
/// [`discharge_run_memory_retention`] refuses it forever and its short-term
/// memory is never purged. This is the repair, and it is deliberately a
/// separate call rather than something the discharge does on its own: a
/// deployment decides to re-date a retention clock, the sweep does not decide
/// it silently. See [`crate::run::AgentRunState::backfill_terminal_at`] for
/// why `updated_at` is a sound fallback clock in one direction only.
///
/// The write is a compare-and-set against the revision this call read, so a
/// resident entity that wrote in between wins and this answers
/// [`AgentRunTerminalStampOutcome::Conflicted`] rather than clobbering it. In
/// the other direction the entity's own persist drops its cached record on a
/// revision conflict and recovers the authoritative one, so a backfill racing
/// a live terminal run costs that run one re-driven command, not a wedge.
///
/// # Errors
///
/// Propagates a store failure and an unreadable schema version as
/// [`MemoryError::Backend`]. Every refusal — already stamped, live, absent,
/// raced — is an `Ok` value.
pub async fn backfill_run_terminal_stamp<Runs>(
    runs: &Runs,
    scope: &AgentRunScope,
    schema: &AgentSchemaPolicy,
) -> Result<AgentRunTerminalStampOutcome, MemoryError>
where
    Runs: DurableStateStore<AgentRunState>,
{
    let persistence_id = scope.persistence_id();
    let Some(record) = runs
        .load(&persistence_id)
        .await
        .map_err(|error| run_state_failure(&error))?
    else {
        return Ok(AgentRunTerminalStampOutcome::RunAbsent);
    };
    // The same fail-closed check the discharge's load performs: a record this
    // binary cannot interpret must not be rewritten by it.
    record
        .state
        .check_schema(schema)
        .map_err(|error| run_state_failure(&error))?;

    let Some((status, stamped)) = record
        .state
        .run()
        .map(|run| (run.status, run.terminal_at.is_some()))
    else {
        return Ok(AgentRunTerminalStampOutcome::RunAbsent);
    };
    if !status.is_terminal() {
        return Ok(AgentRunTerminalStampOutcome::NotTerminal { status });
    }
    if stamped {
        return Ok(AgentRunTerminalStampOutcome::AlreadyStamped);
    }

    let expected_revision = record.revision;
    let mut state = record.state;
    let Some(terminal_at) = state.backfill_terminal_at() else {
        // Unreachable while the guard above and the one inside the mutator
        // agree. Answering "already stamped" rather than asserting keeps the
        // migration's failure mode "did nothing" instead of "panicked partway
        // through a fleet".
        return Ok(AgentRunTerminalStampOutcome::AlreadyStamped);
    };
    match runs
        .compare_and_set(&persistence_id, expected_revision, state)
        .await
    {
        Ok(_) => Ok(AgentRunTerminalStampOutcome::Stamped { terminal_at }),
        Err(DurableError::RevisionConflict { .. }) => Ok(AgentRunTerminalStampOutcome::Conflicted),
        Err(error) => Err(run_state_failure(&error)),
    }
}

/// What one backfill pass over many runs did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct AgentRunTerminalStampReport {
    /// Records repaired by this pass.
    pub stamped: usize,
    /// Records that already carried a stamp.
    pub already_stamped: usize,
    /// Runs still live, which will stamp themselves.
    pub not_terminal: usize,
    /// Scopes with no durable run record.
    pub absent: usize,
    /// Records another writer moved mid-pass. These are the only scopes worth
    /// re-driving, and a pass that reports any is not yet complete.
    pub conflicted: usize,
}

impl AgentRunTerminalStampReport {
    /// Whether every scope this pass visited reached a settled answer.
    ///
    /// A migration is finished when a pass over the same scopes reports this
    /// and [`Self::stamped`] is zero: the first says nothing was raced, the
    /// second that nothing was left to repair.
    #[must_use]
    pub const fn is_settled(&self) -> bool {
        self.conflicted == 0
    }
}

/// A bounded, deployment-invoked one-time repair over caller-supplied scopes.
///
/// Same shape and same reason as [`AgentMemoryRetentionSweep`]: Rakka keeps no
/// index of runs by terminal state, so enumeration belongs to the application,
/// and there is no resident migrator because a per-agent live task is what
/// [specification 6.11](../../../docs/plans/rakka-agent/spec.md) forbids.
///
/// **Run it once the fleet is fully upgraded, not during the rolling update.**
/// A repaired record carries schema version 2, which a binary from before the
/// bump fails closed on — correctly, since that binary would otherwise drop
/// the stamp again — so repairing early makes those records unreadable to
/// peers that are still running. Nothing is lost by waiting: an unstamped
/// record is refused by the discharge, not deleted.
#[derive(Debug, Clone)]
pub struct AgentRunTerminalStampBackfill<Runs> {
    runs: Runs,
    schema: AgentSchemaPolicy,
    max_batch: usize,
}

impl<Runs> AgentRunTerminalStampBackfill<Runs>
where
    Runs: DurableStateStore<AgentRunState>,
{
    /// The most scopes one pass will visit.
    pub const DEFAULT_MAX_BATCH: usize = 128;

    /// A backfill over the given run store.
    #[must_use]
    pub fn new(runs: Runs) -> Self {
        Self {
            runs,
            schema: AgentSchemaPolicy::default(),
            max_batch: Self::DEFAULT_MAX_BATCH,
        }
    }

    /// Uses an explicit schema-compatibility policy for the run states it
    /// reads.
    #[must_use]
    pub fn with_schema_policy(mut self, schema: AgentSchemaPolicy) -> Self {
        self.schema = schema;
        self
    }

    /// Bounds how many scopes one pass visits.
    #[must_use]
    pub const fn with_max_batch(mut self, max_batch: usize) -> Self {
        self.max_batch = if max_batch == 0 { 1 } else { max_batch };
        self
    }

    /// Stamps each scope that needs it, reporting per-outcome counts.
    ///
    /// A refusal on one run never stops the pass; a *store failure* does, for
    /// the same reason the retention sweep stops — a migration that continued
    /// past a broken backend would report a repair it did not perform.
    ///
    /// # Errors
    ///
    /// Propagates the first store failure.
    pub async fn stamp(
        &self,
        scopes: impl IntoIterator<Item = AgentRunScope>,
    ) -> Result<AgentRunTerminalStampReport, MemoryError> {
        let mut report = AgentRunTerminalStampReport::default();
        for scope in scopes.into_iter().take(self.max_batch) {
            match backfill_run_terminal_stamp(&self.runs, &scope, &self.schema).await? {
                AgentRunTerminalStampOutcome::Stamped { .. } => report.stamped += 1,
                AgentRunTerminalStampOutcome::AlreadyStamped => report.already_stamped += 1,
                AgentRunTerminalStampOutcome::NotTerminal { .. } => report.not_terminal += 1,
                AgentRunTerminalStampOutcome::RunAbsent => report.absent += 1,
                AgentRunTerminalStampOutcome::Conflicted => report.conflicted += 1,
            }
        }
        Ok(report)
    }
}
