//! Bounded, authoritative operational queries.
//!
//! Owns [`AgentOperationalSnapshot`] and the session view assembled by
//! `AgentRunId`: point queries answered from durable state, bounded in the work
//! they do, and correct even when telemetry is entirely unavailable. An
//! operator asking what an agent is doing gets an answer from the same records
//! the runtime acts on, not from a metrics pipeline
//! ([specification 17.18](../../../docs/plans/rakka-agent/spec.md)).
//!
//! The split matters and is deliberate:
//!
//! - the **snapshot** is authoritative — derived from the durable run record
//!   alone, returning the durable state revision it read, useful while the
//!   entity is passivated and while every telemetry path is down (scenario
//!   56); and
//! - the **session view** is a projection — it joins the snapshot with the
//!   decision events a sink retained and the trace segments the durable
//!   records carry, exposes its own lag, and never becomes an alternate
//!   execution state machine (scenario 21).
//!
//! Identifiers appear here because an authorized query projection is exactly
//! where [specification 17.3](../../../docs/plans/rakka-agent/spec.md) permits
//! them; they still never label a metric. Continuous and multi-agent
//! projections extend this module in phases 3 and 4.

use rakka_agent_workflow::{
    AgentEffectId, AgentTelemetryContext, AgentTimestampMillis, HumanCheckpointId,
};
use rakka_persistence::{DurableStateStore, Revision};
use serde::{Deserialize, Serialize};

use crate::checkpoints::{AgentCheckpoint, AgentCheckpointKind, AgentCheckpointStatus};
use crate::choreography::AgentExchangeState;
use crate::definition::AgentEffectSafetyClass;
use crate::effect::{AgentEffectGeneration, AgentRunEffectKind, AgentRunEffectStatus};
use crate::identity::{AgentRunScope, AgentTaskId, AgentTaskScope, AgentWakeId, TenantId};
use crate::loop_runtime::AgentLoopState;
use crate::observability::{
    AgentDecisionEvent, AgentDecisionEventSink, AGENT_DECISION_EVENT_RETENTION,
};
use crate::run::{AgentRun, AgentRunResult, AgentRunSnapshot, AgentRunState, AgentRunStatus};
use crate::schema::AgentSchemaPolicy;
use crate::task::{AgentTaskResult, AgentTaskSnapshot, AgentTaskState};
use crate::wake_timers::{AgentWakeTimerStatus, AgentWakeTimerStoreState};

/// How far a requested cancellation has actually got
/// ([specification 17.18](../../../docs/plans/rakka-agent/spec.md), following
/// [8.7](../../../docs/plans/rakka-agent/spec.md)).
///
/// The vocabulary is complete from M1 even where a state is not yet reachable
/// (slice 1.13 resolution): [`Self::Propagating`] becomes derivable when
/// delegation lands in Phase 4, because a single M1 run has no descendants to
/// propagate to. No state is ever inferred from mere *acceptance* of a
/// cancellation request — the derivation reads what the durable record proves.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentCancellationProgress {
    /// No cancellation has been requested.
    NotRequested,
    /// Cancellation is requested and work the run started is still resolving.
    Requested,
    /// Cancellation is propagating to descendants. Not derivable before
    /// delegation exists (Phase 4).
    Propagating,
    /// New work is fenced and nothing the run started is still in flight, but
    /// the terminal transition has not yet committed.
    Quiesced,
    /// An ambiguous consequential effect blocks terminal cancellation until an
    /// operator establishes its outcome
    /// ([specification 11.5](../../../docs/plans/rakka-agent/spec.md);
    /// scenario 57).
    WaitingForReconciliation,
    /// The run reached its cancelled terminal state.
    Completed,
}

impl AgentCancellationProgress {
    /// Stable kebab-case label for views and logs.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::NotRequested => "not-requested",
            Self::Requested => "requested",
            Self::Propagating => "propagating",
            Self::Quiesced => "quiesced",
            Self::WaitingForReconciliation => "waiting-for-reconciliation",
            Self::Completed => "completed",
        }
    }

    /// Derives the progress from what the durable run record proves.
    #[must_use]
    pub fn derive(run: &AgentRun) -> Self {
        let cancellation_recorded = run
            .terminal_reason
            .as_ref()
            .is_some_and(|reason| reason.status() == AgentRunStatus::Cancelled)
            || run.status == AgentRunStatus::Cancelled;
        if !cancellation_recorded {
            return Self::NotRequested;
        }
        if run.status == AgentRunStatus::Cancelled {
            return Self::Completed;
        }
        let effects = run.loop_state.effects();
        if effects
            .iter()
            .any(|effect| matches!(effect.status, AgentRunEffectStatus::Indeterminate))
        {
            return Self::WaitingForReconciliation;
        }
        // "Still resolving" is the run's own settlement gate, not the effect set
        // alone: a cancelling run whose effects have all resolved can still owe
        // an outstanding result proposal to its task, which the task may yet
        // accept or reject ([`AgentLoopState::awaits_settlement`]). Reading
        // effects only would report `Quiesced` — "nothing the run started is
        // still in flight" — while that proposal is exactly such work.
        if run.loop_state.awaits_settlement() {
            return Self::Requested;
        }
        Self::Quiesced
    }
}

/// One pending or unresolved effect, as the authoritative snapshot reports it
/// ([specification 17.18](../../../docs/plans/rakka-agent/spec.md): pending
/// effects, attempts, safety classes, grants, and indeterminate work).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentPendingEffectView {
    /// The effect's derived identity.
    pub effect_id: AgentEffectId,
    /// Its current dispatch generation.
    pub generation: AgentEffectGeneration,
    /// What it calls.
    pub kind: AgentRunEffectKind,
    /// Where the current generation stands.
    pub status: AgentRunEffectStatus,
    /// Dispatch attempts the current generation has made.
    pub attempts: u32,
    /// Most attempts it may make.
    pub max_attempts: u32,
    /// Its safety class.
    pub safety_class: AgentEffectSafetyClass,
    /// Whether dispatch requires a checkpoint grant.
    pub checkpoint_required: bool,
    /// Whether dispatch requires a security-authorization grant.
    pub authorization_required: bool,
    /// Whether a digest-bound grant is currently held for it.
    pub granted: bool,
}

/// One open checkpoint, as the authoritative snapshot reports it
/// ([specification 17.18](../../../docs/plans/rakka-agent/spec.md):
/// checkpoint/authorization state and bounded resolver requirements).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentCheckpointView {
    /// The checkpoint's identity.
    pub checkpoint_id: HumanCheckpointId,
    /// What kind of decision resolves it.
    pub kind: AgentCheckpointKind,
    /// Where it stands.
    pub status: AgentCheckpointStatus,
    /// Roles a resolver must hold.
    pub required_roles: Vec<String>,
    /// How many capabilities a resolver must hold.
    pub required_capabilities: usize,
    /// When it becomes overdue and escalates, when set.
    pub due_at: Option<AgentTimestampMillis>,
    /// When it hard-expires, when set.
    pub expires_at: Option<AgentTimestampMillis>,
}

/// The authoritative operational point answer for one run
/// ([specification 17.18](../../../docs/plans/rakka-agent/spec.md)).
///
/// Everything here derives from the durable run record in one read — no
/// metric, trace, event sink, or Collector is consulted — so the answer stays
/// correct when telemetry is sampled, delayed, dropped, or unavailable
/// (scenario 56), and when the entity is passivated: the status is *logical*
/// lifecycle and never a residency claim
/// ([specification 6.11](../../../docs/plans/rakka-agent/spec.md)). It is a
/// bounded read model, never an alternate execution state machine.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentOperationalSnapshot {
    /// The durable state revision this answer was derived from.
    pub revision: Revision,
    /// When the answer was derived.
    pub observed_at: AgentTimestampMillis,
    /// The run's scope.
    pub scope: AgentRunScope,
    /// The run's bounded projection, once it has accepted an assignment.
    ///
    /// Content-redacted: the proposal, accepted result, and feedback the
    /// entity's own reply surface carries are stripped here, because the
    /// operational answer is an observability surface and content stays in
    /// durable state and protected artifacts
    /// ([specification 17.14](../../../docs/plans/rakka-agent/spec.md)).
    /// [`Self::has_pending_proposal`] carries the bounded fact an operator
    /// needs.
    pub run: Option<AgentRunSnapshot>,
    /// Whether a result proposal is awaiting the task's decision.
    pub has_pending_proposal: bool,
    /// The bounded label of the wait the run is in, when it is waiting.
    pub wait_reason: Option<String>,
    /// The earliest durable checkpoint deadline that will wake the run, when
    /// one is set. M1 waits park on checkpoints and effects; timer occurrences
    /// live in the substrate's timer store and join here when the wake
    /// controller lands (Phase 3).
    pub next_wake: Option<AgentTimestampMillis>,
    /// Every effect the run holds that is not yet resolved.
    pub pending_effects: Vec<AgentPendingEffectView>,
    /// Every checkpoint the run has open.
    pub open_checkpoints: Vec<AgentCheckpointView>,
    /// How far a requested cancellation has actually got.
    pub cancellation: AgentCancellationProgress,
    /// The durable decision-event cursor: how many decisions the run has
    /// sequenced ([specification 17.13](../../../docs/plans/rakka-agent/spec.md)).
    pub decision_cursor: u64,
    /// Decision events still owed to the sink.
    pub decisions_owed: usize,
    /// Decision events the bounded ring dropped before they were flushed.
    pub decision_drops: u64,
}

impl AgentOperationalSnapshot {
    /// Derives the snapshot from one durable run record.
    ///
    /// Pure over the record: deriving twice from the same revision yields the
    /// same answer, which is what makes the snapshot cacheable and
    /// comparison-friendly for operators.
    #[must_use]
    pub fn derive(
        state: &AgentRunState,
        revision: Revision,
        observed_at: AgentTimestampMillis,
    ) -> Self {
        let run = state.run();
        let loop_state = run.map(|run| &run.loop_state);
        let pending_effects = loop_state
            .map(|loop_state| {
                loop_state
                    .effects()
                    .iter()
                    .filter(|effect| effect.blocks_settlement())
                    .map(|effect| AgentPendingEffectView {
                        effect_id: effect.effect_id.clone(),
                        generation: effect.generation,
                        kind: effect.kind(),
                        status: effect.status,
                        attempts: effect.attempts,
                        max_attempts: effect.max_attempts,
                        safety_class: effect.safety.class(),
                        checkpoint_required: effect.checkpoint_required,
                        authorization_required: effect.authorization_required,
                        granted: loop_state.grant_for(effect).is_some(),
                    })
                    .collect()
            })
            .unwrap_or_default();
        let open_checkpoints: Vec<AgentCheckpointView> = loop_state
            .map(|loop_state| {
                loop_state
                    .open_checkpoints()
                    .iter()
                    // Only checkpoints the run is genuinely waiting on: a
                    // resolved-but-not-yet-dropped one is not a live wait, and
                    // its deadline must not surface as a `next_wake` — the same
                    // `is_waiting` gate `has_open_checkpoint` uses.
                    .filter(|checkpoint| checkpoint.status.is_waiting())
                    .map(|checkpoint: &AgentCheckpoint| AgentCheckpointView {
                        checkpoint_id: checkpoint.checkpoint_id.clone(),
                        kind: checkpoint.kind,
                        status: checkpoint.status,
                        required_roles: checkpoint.required_roles.clone(),
                        required_capabilities: checkpoint.required_capabilities.len(),
                        due_at: checkpoint.due_at,
                        expires_at: checkpoint.expires_at,
                    })
                    .collect()
            })
            .unwrap_or_default();
        let next_wake = open_checkpoints
            .iter()
            .flat_map(|checkpoint| [checkpoint.due_at, checkpoint.expires_at])
            .flatten()
            .min();
        let wait_reason = run.and_then(|run| {
            run.status
                .is_waiting()
                .then(|| run.status.as_label().to_string())
        });
        // One snapshot, read once: capture the bounded pending-proposal fact
        // before the content is redacted, rather than deriving the projection
        // twice.
        let (run_snapshot, has_pending_proposal) = match state.snapshot() {
            Some(mut snapshot) => {
                let has_pending_proposal = snapshot.proposal.is_some();
                snapshot.proposal = None;
                snapshot.accepted_result = None;
                snapshot.feedback = None;
                (Some(snapshot), has_pending_proposal)
            }
            None => (None, false),
        };
        Self {
            revision,
            observed_at,
            scope: state.scope().clone(),
            has_pending_proposal,
            run: run_snapshot,
            wait_reason,
            next_wake,
            pending_effects,
            open_checkpoints,
            cancellation: run.map_or(AgentCancellationProgress::NotRequested, |run| {
                AgentCancellationProgress::derive(run)
            }),
            decision_cursor: loop_state.map_or(0, AgentLoopState::decision_sequence),
            decisions_owed: loop_state.map_or(0, |loop_state| loop_state.decision_outbox().len()),
            decision_drops: loop_state.map_or(0, AgentLoopState::decision_drops),
        }
    }
}

/// Answers the authoritative point query for one run scope.
///
/// One durable read, one derivation. `Ok(None)` means no record exists for the
/// scope; an unsupported schema version fails closed exactly as the entity's
/// own recovery would.
pub async fn agent_operational_snapshot<Store>(
    store: &Store,
    scope: &AgentRunScope,
    policy: &AgentSchemaPolicy,
    observed_at: AgentTimestampMillis,
) -> AgentRunResult<Option<AgentOperationalSnapshot>>
where
    Store: DurableStateStore<AgentRunState>,
{
    let Some(record) = store.load(&scope.persistence_id()).await? else {
        return Ok(None);
    };
    record.state.check_schema(policy)?;
    Ok(Some(AgentOperationalSnapshot::derive(
        &record.state,
        record.revision,
        observed_at,
    )))
}

/// The authoritative operational answer for one task scope
/// ([specification 17.18](../../../docs/plans/rakka-agent/spec.md)'s
/// continuous-goal half).
///
/// For a continuous root, the embedded wake view answers the M3 checklist's
/// operational facts in one read: schedule and policy revisions in force,
/// lifecycle status and revision, the failure streak and any backoff, the
/// active epochs, the parked occurrences, the goal-window ledger, the monotone
/// counters, and the owed or parked controller-originated re-wakes. It is
/// derived from the durable record alone, so it answers identically while the
/// entity is passivated and while every telemetry path is down. "Next wake"
/// lives in the wake-timer store, not the task record — join it with
/// [`next_pending_wake_for_task`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AgentTaskOperationalSnapshot {
    /// The durable state revision this answer was derived from.
    pub revision: Revision,
    /// When the answer was derived.
    pub observed_at: AgentTimestampMillis,
    /// The task's scope.
    pub scope: AgentTaskScope,
    /// The task's bounded projection, absent if it was never created.
    ///
    /// Content-redacted: the accepted result's content is stripped, because
    /// the operational answer is an observability surface and content stays
    /// in durable state and protected artifacts
    /// ([specification 17.14](../../../docs/plans/rakka-agent/spec.md)).
    /// [`Self::has_accepted_result`] carries the bounded fact an operator
    /// needs.
    pub task: Option<AgentTaskSnapshot>,
    /// Whether the task holds an accepted typed result.
    pub has_accepted_result: bool,
    /// History entries recorded but not yet flushed to the history sink.
    pub owed_history: usize,
}

impl AgentTaskOperationalSnapshot {
    /// Derives the snapshot from one durable task record.
    ///
    /// Pure over the record: deriving twice from the same revision yields the
    /// same answer.
    #[must_use]
    pub fn derive(
        state: &AgentTaskState,
        revision: Revision,
        observed_at: AgentTimestampMillis,
    ) -> Self {
        let (task, has_accepted_result) = match state.snapshot() {
            Some(mut snapshot) => {
                let has_accepted_result = snapshot.accepted_result.is_some();
                snapshot.accepted_result = None;
                (Some(snapshot), has_accepted_result)
            }
            None => (None, false),
        };
        Self {
            revision,
            observed_at,
            scope: state.scope().clone(),
            task,
            has_accepted_result,
            owed_history: state.pending_history().len(),
        }
    }
}

/// Answers the authoritative point query for one task scope.
///
/// One durable read, one derivation, no entity activation. `Ok(None)` means no
/// record exists for the scope; an unsupported schema version fails closed
/// exactly as the entity's own recovery would.
pub async fn agent_task_operational_snapshot<Store>(
    store: &Store,
    scope: &AgentTaskScope,
    policy: &AgentSchemaPolicy,
    observed_at: AgentTimestampMillis,
) -> AgentTaskResult<Option<AgentTaskOperationalSnapshot>>
where
    Store: DurableStateStore<AgentTaskState>,
{
    let Some(record) = store.load(&scope.persistence_id()).await? else {
        return Ok(None);
    };
    record.state.check_schema(policy)?;
    Ok(Some(AgentTaskOperationalSnapshot::derive(
        &record.state,
        record.revision,
        observed_at,
    )))
}

/// The earliest pending wake-timer entry of one task: the "next wake" an
/// operator asks about, joined from the wake-timer store's durable state.
///
/// Pure over the recovered timer state, so any node — including one that owns
/// neither the task nor a scanner — computes the same answer from the same
/// record. `None` means no pending entry exists for the task; fired, fenced,
/// and cancelled entries never surface.
#[must_use]
pub fn next_pending_wake_for_task(
    timers: &AgentWakeTimerStoreState,
    tenant: &TenantId,
    task: &AgentTaskId,
) -> Option<(AgentWakeId, AgentTimestampMillis)> {
    timers
        .entries()
        .values()
        .filter(|entry| {
            entry.status() == AgentWakeTimerStatus::Pending
                && entry.binding().tenant() == tenant
                && entry.task() == task
        })
        .map(|entry| (entry.wake_id().clone(), entry.due_time()))
        .min_by_key(|(_, due)| *due)
}

/// Where a persisted trace segment was collected from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentSessionSegmentSource {
    /// The loop state's last committing segment.
    Loop,
    /// An unresolved effect's scheduling segment.
    EffectScheduling,
    /// An open checkpoint's parked segment.
    CheckpointOpen,
}

impl AgentSessionSegmentSource {
    /// Stable kebab-case label for views and logs.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Loop => "loop",
            Self::EffectScheduling => "effect-scheduling",
            Self::CheckpointOpen => "checkpoint-open",
        }
    }
}

/// One trace segment a durable record carries, referenced by the session view
/// ([specification 17.5](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentSessionTraceSegment {
    /// Which durable record carried it.
    pub source: AgentSessionSegmentSource,
    /// The persisted context.
    pub telemetry: AgentTelemetryContext,
}

/// The session observability view for one run, assembled by `AgentRunId`
/// ([specification 17.18](../../../docs/plans/rakka-agent/spec.md); scenario
/// 21).
///
/// The view is a projection over the authoritative snapshot: it adds what the
/// decision sink retained and the trace segments the durable records carry,
/// and it is explicit about its own freshness — [`Self::decisions_available`]
/// is `false` when the sink could not answer, and [`Self::decision_lag`]
/// counts the durably sequenced decisions the projection has not seen. A
/// telemetry outage degrades this view and never the snapshot inside it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentSessionView {
    /// The authoritative point answer.
    pub snapshot: AgentOperationalSnapshot,
    /// The retained decision events, in sequence order.
    pub decisions: Vec<AgentDecisionEvent>,
    /// Whether the decision sink answered at all.
    pub decisions_available: bool,
    /// Durably sequenced decisions the projection has not seen: the durable
    /// cursor minus the highest retained sequence.
    pub decision_lag: u64,
    /// The trace segments the durable records carry, for linking into the
    /// tracing backend.
    pub trace_segments: Vec<AgentSessionTraceSegment>,
}

/// Assembles the session view for one run scope.
///
/// One durable read plus one bounded sink read. A missing or failing sink
/// leaves [`AgentSessionView::decisions`] empty with
/// [`AgentSessionView::decisions_available`] `false` — the durable snapshot
/// half is never degraded by the telemetry half (scenario 56).
pub async fn assemble_agent_session_view<Store>(
    store: &Store,
    scope: &AgentRunScope,
    policy: &AgentSchemaPolicy,
    decisions: Option<&dyn AgentDecisionEventSink>,
    observed_at: AgentTimestampMillis,
) -> AgentRunResult<Option<AgentSessionView>>
where
    Store: DurableStateStore<AgentRunState>,
{
    let Some(record) = store.load(&scope.persistence_id()).await? else {
        return Ok(None);
    };
    record.state.check_schema(policy)?;
    let snapshot = AgentOperationalSnapshot::derive(&record.state, record.revision, observed_at);

    let mut trace_segments = Vec::new();
    if let Some(run) = record.state.run() {
        collect_segment(
            &mut trace_segments,
            AgentSessionSegmentSource::Loop,
            run.loop_state.telemetry(),
        );
        for effect in run.loop_state.effects() {
            if effect.blocks_settlement() {
                collect_segment(
                    &mut trace_segments,
                    AgentSessionSegmentSource::EffectScheduling,
                    &effect.telemetry,
                );
            }
        }
        for checkpoint in run.loop_state.open_checkpoints() {
            collect_segment(
                &mut trace_segments,
                AgentSessionSegmentSource::CheckpointOpen,
                &checkpoint.telemetry,
            );
        }
    }

    let (events, decisions_available) = match decisions {
        None => (Vec::new(), false),
        Some(sink) => match sink.read(scope, 0, AGENT_DECISION_EVENT_RETENTION).await {
            Ok(events) => (events, true),
            Err(_) => (Vec::new(), false),
        },
    };
    let projected = events.iter().map(|event| event.sequence).max().unwrap_or(0);
    let decision_lag = snapshot.decision_cursor.saturating_sub(projected);

    Ok(Some(AgentSessionView {
        snapshot,
        decisions: events,
        decisions_available,
        decision_lag,
        trace_segments,
    }))
}

fn collect_segment(
    segments: &mut Vec<AgentSessionTraceSegment>,
    source: AgentSessionSegmentSource,
    telemetry: &AgentTelemetryContext,
) {
    if telemetry.trace_parent.is_none() && telemetry.span_links.is_empty() {
        return;
    }
    segments.push(AgentSessionTraceSegment {
        source,
        telemetry: telemetry.clone(),
    });
}
