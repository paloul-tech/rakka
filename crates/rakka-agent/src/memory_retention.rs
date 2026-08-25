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
use rakka_persistence::DurableStateStore;

use crate::identity::AgentRunScope;
use crate::memory::{AgentRunMemory, MemoryError, SessionPurgeOutcome, SessionRetentionPolicy};
use crate::run::{load_agent_run_state, AgentRunStatus};
use crate::schema::AgentSchemaPolicy;

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

    /// Whether anything was actually deleted.
    #[must_use]
    pub const fn deleted_anything(self) -> bool {
        matches!(
            self,
            Self::Discharged {
                snapshots: SessionPurgeOutcome::Purged { .. },
                ..
            } | Self::Discharged {
                session: SessionPurgeOutcome::Purged { .. },
                ..
            }
        )
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
        .map_err(|error| MemoryError::Backend {
            backend: "run-state".to_string(),
            message: error.to_string(),
        })?;
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
    /// Runs held by a legal hold on either tier.
    pub held: usize,
    /// Runs whose retention window has not elapsed.
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
                    for tier in [snapshots, session] {
                        match tier {
                            SessionPurgeOutcome::Purged { entries } => {
                                report.records_deleted =
                                    report.records_deleted.saturating_add(entries);
                            }
                            SessionPurgeOutcome::Held => report.held += 1,
                            SessionPurgeOutcome::NotYetDue => report.not_yet_due += 1,
                        }
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
