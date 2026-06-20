//! Retention windows and snapshot compaction for agent workflow state.
//!
//! The functions in this module are pure compaction helpers. They return a
//! compacted state plus archive records that an application can persist to an
//! event journal, object store, or compliance archive before deleting external
//! artifacts.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{
    AgentAuditEvent, AgentAuditEventId, AgentEffect, AgentEffectId, AgentEffectStatus, AgentRunId,
    AgentRunState, AgentRunStatus, AgentStatePayload, AgentTimestampMillis, ArtifactKind,
    ArtifactRef, HumanCheckpoint, HumanCheckpointId,
};

/// Retention policy for agent workflow state and archive handoff records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentRetentionPolicy {
    completed_checkpoint_retention_ms: Option<u64>,
    completed_effect_retention_ms: Option<u64>,
    audit_event_retention_ms: Option<u64>,
    artifact_reference_retention_ms: Option<u64>,
    prompt_artifact_retention_ms: Option<u64>,
    completion_artifact_retention_ms: Option<u64>,
    inline_state_retention_ms: Option<u64>,
    max_terminal_checkpoints: Option<usize>,
    max_terminal_effects: Option<usize>,
}

impl AgentRetentionPolicy {
    /// Creates a policy that keeps all agent workflow history.
    #[must_use]
    pub const fn disabled() -> Self {
        Self {
            completed_checkpoint_retention_ms: None,
            completed_effect_retention_ms: None,
            audit_event_retention_ms: None,
            artifact_reference_retention_ms: None,
            prompt_artifact_retention_ms: None,
            completion_artifact_retention_ms: None,
            inline_state_retention_ms: None,
            max_terminal_checkpoints: None,
            max_terminal_effects: None,
        }
    }

    /// Creates a policy that keeps all agent workflow history.
    #[must_use]
    pub const fn new() -> Self {
        Self::disabled()
    }

    /// Retains terminal human checkpoints for the given window in milliseconds.
    #[must_use]
    pub const fn completed_checkpoint_retention_ms(mut self, retention_ms: u64) -> Self {
        self.completed_checkpoint_retention_ms = Some(retention_ms);
        self
    }

    /// Retains terminal effects for the given window in milliseconds.
    #[must_use]
    pub const fn completed_effect_retention_ms(mut self, retention_ms: u64) -> Self {
        self.completed_effect_retention_ms = Some(retention_ms);
        self
    }

    /// Retains durable audit events and checkpoint audit ids for the given window.
    #[must_use]
    pub const fn audit_event_retention_ms(mut self, retention_ms: u64) -> Self {
        self.audit_event_retention_ms = Some(retention_ms);
        self
    }

    /// Retains general artifact references for the given window.
    #[must_use]
    pub const fn artifact_reference_retention_ms(mut self, retention_ms: u64) -> Self {
        self.artifact_reference_retention_ms = Some(retention_ms);
        self
    }

    /// Retains prompt artifact references for the given window.
    #[must_use]
    pub const fn prompt_artifact_retention_ms(mut self, retention_ms: u64) -> Self {
        self.prompt_artifact_retention_ms = Some(retention_ms);
        self
    }

    /// Retains completion artifact references for the given window.
    #[must_use]
    pub const fn completion_artifact_retention_ms(mut self, retention_ms: u64) -> Self {
        self.completion_artifact_retention_ms = Some(retention_ms);
        self
    }

    /// Retains inline run state payloads for the given window after run termination.
    #[must_use]
    pub const fn inline_state_retention_ms(mut self, retention_ms: u64) -> Self {
        self.inline_state_retention_ms = Some(retention_ms);
        self
    }

    /// Retains at most this many terminal checkpoints per run.
    #[must_use]
    pub const fn max_terminal_checkpoints(mut self, max_terminal_checkpoints: usize) -> Self {
        self.max_terminal_checkpoints = Some(max_terminal_checkpoints);
        self
    }

    /// Retains at most this many terminal effects per run.
    #[must_use]
    pub const fn max_terminal_effects(mut self, max_terminal_effects: usize) -> Self {
        self.max_terminal_effects = Some(max_terminal_effects);
        self
    }

    /// Terminal human checkpoint retention window, when enabled.
    #[must_use]
    pub const fn completed_checkpoint_retention_window_ms(self) -> Option<u64> {
        self.completed_checkpoint_retention_ms
    }

    /// Terminal effect retention window, when enabled.
    #[must_use]
    pub const fn completed_effect_retention_window_ms(self) -> Option<u64> {
        self.completed_effect_retention_ms
    }

    /// Audit event retention window, when enabled.
    #[must_use]
    pub const fn audit_event_retention_window_ms(self) -> Option<u64> {
        self.audit_event_retention_ms
    }

    /// General artifact reference retention window, when enabled.
    #[must_use]
    pub const fn artifact_reference_retention_window_ms(self) -> Option<u64> {
        self.artifact_reference_retention_ms
    }

    /// Prompt artifact reference retention window, when enabled.
    #[must_use]
    pub const fn prompt_artifact_retention_window_ms(self) -> Option<u64> {
        self.prompt_artifact_retention_ms
    }

    /// Completion artifact reference retention window, when enabled.
    #[must_use]
    pub const fn completion_artifact_retention_window_ms(self) -> Option<u64> {
        self.completion_artifact_retention_ms
    }

    /// Inline state retention window, when enabled.
    #[must_use]
    pub const fn inline_state_retention_window_ms(self) -> Option<u64> {
        self.inline_state_retention_ms
    }

    /// Maximum retained terminal checkpoints, when enabled.
    #[must_use]
    pub const fn max_terminal_checkpoint_count(self) -> Option<usize> {
        self.max_terminal_checkpoints
    }

    /// Maximum retained terminal effects, when enabled.
    #[must_use]
    pub const fn max_terminal_effect_count(self) -> Option<usize> {
        self.max_terminal_effects
    }
}

impl Default for AgentRetentionPolicy {
    fn default() -> Self {
        Self::disabled()
    }
}

/// Archive handoff category for data removed from hot state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentRetentionArchiveKind {
    /// Inline run state bytes were removed.
    InlineRunState,
    /// A run-level artifact reference was removed.
    RunArtifactRef,
    /// A terminal human checkpoint was removed.
    HumanCheckpoint,
    /// A terminal checkpoint's audit ids were removed.
    CheckpointAuditIds,
    /// A checkpoint artifact reference was removed.
    CheckpointArtifactRef,
    /// A terminal effect was removed.
    AgentEffect,
    /// An effect artifact reference was removed.
    EffectArtifactRef,
    /// A durable audit event was removed.
    AuditEvent,
}

/// Reason that data was removed from hot state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentRetentionArchiveReason {
    /// Data aged past its configured retention window.
    RetentionWindowExpired,
    /// Data exceeded the configured per-run history count.
    HistoryLimitExceeded,
    /// Artifact metadata aged past its configured retention window.
    ArtifactWindowExpired,
    /// Audit metadata aged past its configured retention window.
    AuditWindowExpired,
    /// Inline state aged past its configured retention window.
    InlineStateWindowExpired,
}

/// One archival handoff record produced by compaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRetentionArchiveRecord {
    /// Run id that owned the removed data.
    pub run_id: AgentRunId,
    /// Removed data category.
    pub kind: AgentRetentionArchiveKind,
    /// Stable id for the removed entity or reference.
    pub entity_id: Option<String>,
    /// Reason the record was removed from hot state.
    pub reason: AgentRetentionArchiveReason,
    /// Timestamp when compaction produced this handoff record.
    pub archived_at: AgentTimestampMillis,
    /// Window cutoff that made the record eligible, when applicable.
    pub retention_cutoff: Option<AgentTimestampMillis>,
    /// Artifact references that should be archived or deleted by application storage.
    pub artifact_refs: Vec<ArtifactRef>,
    /// Audit event ids that should remain discoverable in durable audit storage.
    pub audit_event_ids: Vec<AgentAuditEventId>,
    /// Inline bytes removed from hot state.
    pub inline_bytes: u64,
}

impl AgentRetentionArchiveRecord {
    fn new(
        run_id: AgentRunId,
        kind: AgentRetentionArchiveKind,
        reason: AgentRetentionArchiveReason,
        archived_at: AgentTimestampMillis,
    ) -> Self {
        Self {
            run_id,
            kind,
            entity_id: None,
            reason,
            archived_at,
            retention_cutoff: None,
            artifact_refs: Vec::new(),
            audit_event_ids: Vec::new(),
            inline_bytes: 0,
        }
    }
}

/// Counts and archive handoff records produced by agent workflow compaction.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRetentionCompactionReport {
    /// Archive handoff records for removed hot-state data.
    pub archive_records: Vec<AgentRetentionArchiveRecord>,
    /// Removed terminal checkpoints.
    pub removed_checkpoints: usize,
    /// Removed terminal effects.
    pub removed_effects: usize,
    /// Removed artifact references from retained entities.
    pub removed_artifact_refs: usize,
    /// Removed audit event ids from retained entities.
    pub removed_audit_event_ids: usize,
    /// Removed durable audit events.
    pub removed_audit_events: usize,
    /// Inline bytes removed from run state.
    pub cleared_inline_state_bytes: u64,
}

impl AgentRetentionCompactionReport {
    /// Returns true when compaction removed no hot-state data.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.archive_records.is_empty()
            && self.removed_checkpoints == 0
            && self.removed_effects == 0
            && self.removed_artifact_refs == 0
            && self.removed_audit_event_ids == 0
            && self.removed_audit_events == 0
            && self.cleared_inline_state_bytes == 0
    }
}

/// Compacted run state plus archive records for removed data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunStateCompaction {
    /// Compacted run state to persist as the new hot snapshot.
    pub run_state: AgentRunState,
    /// Counts and archive handoff records produced by compaction.
    pub report: AgentRetentionCompactionReport,
}

/// Compacted audit event set plus archive records for removed events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentAuditCompaction {
    /// Retained audit events.
    pub audit_events: Vec<AgentAuditEvent>,
    /// Counts and archive handoff records produced by compaction.
    pub report: AgentRetentionCompactionReport,
}

/// Compacts one agent run state according to the supplied policy.
#[must_use]
pub fn compact_agent_run_state(
    mut run_state: AgentRunState,
    policy: AgentRetentionPolicy,
    now: AgentTimestampMillis,
) -> AgentRunStateCompaction {
    let mut report = AgentRetentionCompactionReport::default();
    compact_run_payloads(&mut run_state, policy, now, &mut report);
    compact_checkpoints(&mut run_state, policy, now, &mut report);
    compact_effects(&mut run_state, policy, now, &mut report);
    AgentRunStateCompaction { run_state, report }
}

/// Compacts durable audit events according to the supplied policy.
#[must_use]
pub fn compact_agent_audit_events(
    audit_events: Vec<AgentAuditEvent>,
    policy: AgentRetentionPolicy,
    now: AgentTimestampMillis,
) -> AgentAuditCompaction {
    let mut retained = Vec::new();
    let mut report = AgentRetentionCompactionReport::default();
    for event in audit_events {
        if window_elapsed(event.occurred_at, policy.audit_event_retention_ms, now) {
            report.removed_audit_events += 1;
            let mut record = AgentRetentionArchiveRecord::new(
                event.run_id.clone(),
                AgentRetentionArchiveKind::AuditEvent,
                AgentRetentionArchiveReason::AuditWindowExpired,
                now,
            );
            record.entity_id = Some(event.audit_event_id.as_str().to_string());
            record.retention_cutoff = cutoff(policy.audit_event_retention_ms, now);
            record.artifact_refs = event.artifact_refs;
            record.audit_event_ids = vec![event.audit_event_id];
            report.archive_records.push(record);
        } else {
            retained.push(event);
        }
    }
    AgentAuditCompaction {
        audit_events: retained,
        report,
    }
}

fn compact_run_payloads(
    run_state: &mut AgentRunState,
    policy: AgentRetentionPolicy,
    now: AgentTimestampMillis,
    report: &mut AgentRetentionCompactionReport,
) {
    if !is_terminal_run_status(run_state.status) {
        return;
    }

    if let Some(reference) = run_state.inputs_ref.clone() {
        if should_remove_artifact_ref(&reference, policy, now) {
            report.removed_artifact_refs += 1;
            report.archive_records.push(artifact_archive_record(
                run_state.run_id.clone(),
                AgentRetentionArchiveKind::RunArtifactRef,
                AgentRetentionArchiveReason::ArtifactWindowExpired,
                now,
                policy,
                reference,
            ));
            run_state.inputs_ref = None;
        }
    }

    match run_state.state_payload.clone() {
        AgentStatePayload::Empty => {}
        AgentStatePayload::Artifact(reference) => {
            if should_remove_artifact_ref(&reference, policy, now) {
                report.removed_artifact_refs += 1;
                report.archive_records.push(artifact_archive_record(
                    run_state.run_id.clone(),
                    AgentRetentionArchiveKind::RunArtifactRef,
                    AgentRetentionArchiveReason::ArtifactWindowExpired,
                    now,
                    policy,
                    reference,
                ));
                run_state.state_payload = AgentStatePayload::Empty;
            }
        }
        AgentStatePayload::Inline(inline_state) => {
            if run_state.completed_at.is_some_and(|completed_at| {
                window_elapsed(completed_at, policy.inline_state_retention_ms, now)
            }) {
                let mut record = AgentRetentionArchiveRecord::new(
                    run_state.run_id.clone(),
                    AgentRetentionArchiveKind::InlineRunState,
                    AgentRetentionArchiveReason::InlineStateWindowExpired,
                    now,
                );
                record.retention_cutoff = cutoff(policy.inline_state_retention_ms, now);
                record.inline_bytes = inline_state.size_bytes;
                report.cleared_inline_state_bytes = report
                    .cleared_inline_state_bytes
                    .saturating_add(inline_state.size_bytes);
                report.archive_records.push(record);
                run_state.state_payload = AgentStatePayload::Empty;
            }
        }
    }
}

fn compact_checkpoints(
    run_state: &mut AgentRunState,
    policy: AgentRetentionPolicy,
    now: AgentTimestampMillis,
    report: &mut AgentRetentionCompactionReport,
) {
    let ranks = checkpoint_terminal_ranks(&run_state.checkpoints);
    let mut retained = Vec::with_capacity(run_state.checkpoints.len());
    for mut checkpoint in run_state.checkpoints.drain(..) {
        if !checkpoint.status.is_terminal() {
            retained.push(checkpoint);
            continue;
        }

        let rank = ranks
            .get(&checkpoint.checkpoint_id)
            .copied()
            .unwrap_or(usize::MAX);
        let terminal_at = checkpoint_terminal_at(&checkpoint);
        let expired = window_elapsed(terminal_at, policy.completed_checkpoint_retention_ms, now);
        let over_limit = policy
            .max_terminal_checkpoints
            .is_some_and(|limit| rank >= limit);
        if expired || over_limit {
            report.removed_checkpoints += 1;
            let mut record = AgentRetentionArchiveRecord::new(
                run_state.run_id.clone(),
                AgentRetentionArchiveKind::HumanCheckpoint,
                if expired {
                    AgentRetentionArchiveReason::RetentionWindowExpired
                } else {
                    AgentRetentionArchiveReason::HistoryLimitExceeded
                },
                now,
            );
            record.entity_id = Some(checkpoint.checkpoint_id.as_str().to_string());
            record.retention_cutoff = cutoff(policy.completed_checkpoint_retention_ms, now);
            record.artifact_refs = checkpoint.context_artifacts;
            record.audit_event_ids = checkpoint.audit_event_ids;
            report.removed_artifact_refs += record.artifact_refs.len();
            report.removed_audit_event_ids += record.audit_event_ids.len();
            report.archive_records.push(record);
            continue;
        }

        compact_checkpoint_artifacts(
            run_state.run_id.clone(),
            &mut checkpoint,
            policy,
            now,
            report,
        );
        compact_checkpoint_audit_ids(
            run_state.run_id.clone(),
            &mut checkpoint,
            policy,
            now,
            report,
        );
        retained.push(checkpoint);
    }
    run_state.checkpoints = retained;
}

fn compact_checkpoint_artifacts(
    run_id: AgentRunId,
    checkpoint: &mut HumanCheckpoint,
    policy: AgentRetentionPolicy,
    now: AgentTimestampMillis,
    report: &mut AgentRetentionCompactionReport,
) {
    let mut retained = Vec::with_capacity(checkpoint.context_artifacts.len());
    for reference in checkpoint.context_artifacts.drain(..) {
        if should_remove_artifact_ref(&reference, policy, now) {
            report.removed_artifact_refs += 1;
            let mut record = artifact_archive_record(
                run_id.clone(),
                AgentRetentionArchiveKind::CheckpointArtifactRef,
                AgentRetentionArchiveReason::ArtifactWindowExpired,
                now,
                policy,
                reference,
            );
            record.entity_id = Some(checkpoint.checkpoint_id.as_str().to_string());
            report.archive_records.push(record);
        } else {
            retained.push(reference);
        }
    }
    checkpoint.context_artifacts = retained;
}

fn compact_checkpoint_audit_ids(
    run_id: AgentRunId,
    checkpoint: &mut HumanCheckpoint,
    policy: AgentRetentionPolicy,
    now: AgentTimestampMillis,
    report: &mut AgentRetentionCompactionReport,
) {
    if checkpoint.audit_event_ids.is_empty()
        || !window_elapsed(
            checkpoint_terminal_at(checkpoint),
            policy.audit_event_retention_ms,
            now,
        )
    {
        return;
    }
    let audit_event_ids = std::mem::take(&mut checkpoint.audit_event_ids);
    report.removed_audit_event_ids += audit_event_ids.len();
    let mut record = AgentRetentionArchiveRecord::new(
        run_id,
        AgentRetentionArchiveKind::CheckpointAuditIds,
        AgentRetentionArchiveReason::AuditWindowExpired,
        now,
    );
    record.entity_id = Some(checkpoint.checkpoint_id.as_str().to_string());
    record.retention_cutoff = cutoff(policy.audit_event_retention_ms, now);
    record.audit_event_ids = audit_event_ids;
    report.archive_records.push(record);
}

fn compact_effects(
    run_state: &mut AgentRunState,
    policy: AgentRetentionPolicy,
    now: AgentTimestampMillis,
    report: &mut AgentRetentionCompactionReport,
) {
    let ranks = effect_terminal_ranks(&run_state.pending_effects);
    let mut retained = Vec::with_capacity(run_state.pending_effects.len());
    for mut effect in run_state.pending_effects.drain(..) {
        if !is_terminal_effect_status(effect.status) {
            retained.push(effect);
            continue;
        }

        let rank = ranks.get(&effect.effect_id).copied().unwrap_or(usize::MAX);
        let terminal_at = effect_terminal_at(&effect);
        let expired = window_elapsed(terminal_at, policy.completed_effect_retention_ms, now);
        let over_limit = policy
            .max_terminal_effects
            .is_some_and(|limit| rank >= limit);
        if expired || over_limit {
            report.removed_effects += 1;
            let mut record = AgentRetentionArchiveRecord::new(
                run_state.run_id.clone(),
                AgentRetentionArchiveKind::AgentEffect,
                if expired {
                    AgentRetentionArchiveReason::RetentionWindowExpired
                } else {
                    AgentRetentionArchiveReason::HistoryLimitExceeded
                },
                now,
            );
            record.entity_id = Some(effect.effect_id.as_str().to_string());
            record.retention_cutoff = cutoff(policy.completed_effect_retention_ms, now);
            if let Some(reference) = effect.payload_ref {
                record.artifact_refs.push(reference);
            }
            if let Some(reference) = effect.result_ref {
                record.artifact_refs.push(reference);
            }
            report.removed_artifact_refs += record.artifact_refs.len();
            report.archive_records.push(record);
            continue;
        }

        compact_effect_artifacts(run_state.run_id.clone(), &mut effect, policy, now, report);
        retained.push(effect);
    }
    run_state.pending_effects = retained;
}

fn compact_effect_artifacts(
    run_id: AgentRunId,
    effect: &mut AgentEffect,
    policy: AgentRetentionPolicy,
    now: AgentTimestampMillis,
    report: &mut AgentRetentionCompactionReport,
) {
    if let Some(reference) = effect.payload_ref.clone() {
        if should_remove_artifact_ref(&reference, policy, now) {
            report.removed_artifact_refs += 1;
            let mut record = artifact_archive_record(
                run_id.clone(),
                AgentRetentionArchiveKind::EffectArtifactRef,
                AgentRetentionArchiveReason::ArtifactWindowExpired,
                now,
                policy,
                reference,
            );
            record.entity_id = Some(effect.effect_id.as_str().to_string());
            report.archive_records.push(record);
            effect.payload_ref = None;
        }
    }
    if let Some(reference) = effect.result_ref.clone() {
        if should_remove_artifact_ref(&reference, policy, now) {
            report.removed_artifact_refs += 1;
            let mut record = artifact_archive_record(
                run_id,
                AgentRetentionArchiveKind::EffectArtifactRef,
                AgentRetentionArchiveReason::ArtifactWindowExpired,
                now,
                policy,
                reference,
            );
            record.entity_id = Some(effect.effect_id.as_str().to_string());
            report.archive_records.push(record);
            effect.result_ref = None;
        }
    }
}

fn artifact_archive_record(
    run_id: AgentRunId,
    kind: AgentRetentionArchiveKind,
    reason: AgentRetentionArchiveReason,
    now: AgentTimestampMillis,
    policy: AgentRetentionPolicy,
    reference: ArtifactRef,
) -> AgentRetentionArchiveRecord {
    let mut record = AgentRetentionArchiveRecord::new(run_id, kind, reason, now);
    record.entity_id = Some(reference.artifact_id.clone());
    record.retention_cutoff = cutoff(artifact_retention_ms(&reference, policy), now);
    record.artifact_refs.push(reference);
    record
}

fn checkpoint_terminal_ranks(
    checkpoints: &[HumanCheckpoint],
) -> BTreeMap<HumanCheckpointId, usize> {
    let mut terminal: Vec<_> = checkpoints
        .iter()
        .filter(|checkpoint| checkpoint.status.is_terminal())
        .map(|checkpoint| {
            (
                checkpoint.checkpoint_id.clone(),
                checkpoint_terminal_at(checkpoint),
            )
        })
        .collect();
    terminal.sort_by(|left, right| {
        right
            .1
            .cmp(&left.1)
            .then_with(|| left.0.as_str().cmp(right.0.as_str()))
    });
    terminal
        .into_iter()
        .enumerate()
        .map(|(rank, (checkpoint_id, _timestamp))| (checkpoint_id, rank))
        .collect()
}

fn effect_terminal_ranks(effects: &[AgentEffect]) -> BTreeMap<AgentEffectId, usize> {
    let mut terminal: Vec<_> = effects
        .iter()
        .filter(|effect| is_terminal_effect_status(effect.status))
        .map(|effect| (effect.effect_id.clone(), effect_terminal_at(effect)))
        .collect();
    terminal.sort_by(|left, right| {
        right
            .1
            .cmp(&left.1)
            .then_with(|| left.0.as_str().cmp(right.0.as_str()))
    });
    terminal
        .into_iter()
        .enumerate()
        .map(|(rank, (effect_id, _timestamp))| (effect_id, rank))
        .collect()
}

fn checkpoint_terminal_at(checkpoint: &HumanCheckpoint) -> AgentTimestampMillis {
    checkpoint.resolved_at.unwrap_or(checkpoint.created_at)
}

fn effect_terminal_at(effect: &AgentEffect) -> AgentTimestampMillis {
    effect.due_at.unwrap_or(effect.created_at)
}

fn is_terminal_run_status(status: AgentRunStatus) -> bool {
    matches!(
        status,
        AgentRunStatus::Completed | AgentRunStatus::Failed | AgentRunStatus::Cancelled
    )
}

fn is_terminal_effect_status(status: AgentEffectStatus) -> bool {
    matches!(
        status,
        AgentEffectStatus::Completed | AgentEffectStatus::Exhausted | AgentEffectStatus::Cancelled
    )
}

fn should_remove_artifact_ref(
    reference: &ArtifactRef,
    policy: AgentRetentionPolicy,
    now: AgentTimestampMillis,
) -> bool {
    window_elapsed(
        reference.created_at,
        artifact_retention_ms(reference, policy),
        now,
    )
}

fn artifact_retention_ms(reference: &ArtifactRef, policy: AgentRetentionPolicy) -> Option<u64> {
    match reference.kind {
        ArtifactKind::Prompt => policy
            .prompt_artifact_retention_ms
            .or(policy.artifact_reference_retention_ms),
        ArtifactKind::Completion => policy
            .completion_artifact_retention_ms
            .or(policy.artifact_reference_retention_ms),
        ArtifactKind::Input
        | ArtifactKind::File
        | ArtifactKind::Embedding
        | ArtifactKind::ToolOutput
        | ArtifactKind::Screenshot
        | ArtifactKind::Log
        | ArtifactKind::State
        | ArtifactKind::Other => policy.artifact_reference_retention_ms,
    }
}

fn cutoff(retention_ms: Option<u64>, now: AgentTimestampMillis) -> Option<AgentTimestampMillis> {
    retention_ms
        .map(|retention_ms| AgentTimestampMillis::new(now.as_millis().saturating_sub(retention_ms)))
}

fn window_elapsed(
    timestamp: AgentTimestampMillis,
    retention_ms: Option<u64>,
    now: AgentTimestampMillis,
) -> bool {
    retention_ms.is_some_and(|retention_ms| {
        now.as_millis().saturating_sub(timestamp.as_millis()) >= retention_ms
    })
}
