//! Replayable coordination events: one scoped cursor over every durable log an
//! agent deployment keeps
//! ([specification 17.13](../../docs/plans/rakka-agent/spec.md); scenario 45).
//!
//! # What this module is, and is not
//!
//! It is a **read** contract. Every coordination transition this crate makes
//! already writes an ordered, deduplicated record — the task, team, and
//! conversation history logs, and the run's decision-event sink — and each of
//! them is written *after* the durable compare-and-set that decided it, on a
//! sequence the transition itself consumed. Nothing here adds a second write
//! path, a second record, or a second source of truth: durable task/run state
//! stays the correctness source, and these events stay a projection of it.
//!
//! What was missing is the way out. Each log had its own cursor type, none
//! reported a retained floor, and the public A2A task-event cursor
//! (`<task-id>:<sequence>`, a compatibility commitment) cannot name a team or a
//! moderated conversation at all. This module supplies the three things
//! [specification 17.13](../../docs/plans/rakka-agent/spec.md) asks of a
//! coordination-event projection — a monotonic *scoped* sequence, a reconnect
//! cursor, and an explicit expired-window answer — over the logs that already
//! exist.
//!
//! # The scope is the entity address
//!
//! A cursor is [`AgentEntityAddress::key`] followed by `:` and the sequence, so
//! `task/acme/order-1:7` and `team/acme/support:3` are both legal and neither
//! collides with the substrate's public task cursor. The address carries its own
//! tenant, which is exactly why a cursor is never trusted on its face: a reader
//! must present the scope it is reading, and a cursor naming a *different* scope
//! is refused rather than followed
//! ([`AgentCoordinationReplayError::ScopeMismatch`]).
//!
//! # Two answers, never a short page
//!
//! A replay resolves to [`AgentCoordinationReplay::Page`] — contiguous from the
//! cursor — or to [`AgentCoordinationReplay::WindowExpired`], which names the
//! floor to resume from. A page that quietly skipped an evicted entry would be
//! indistinguishable from a complete one, which is the failure this module
//! exists to prevent. [`AgentCoordinationPage::complete_through`] closes the
//! matching ambiguity at the head: entries reach their log on the settle pass
//! *after* the transition, so an empty tail can mean "you are current" or "the
//! entity still owes its sink", and a reader is told which.

use std::fmt::{self, Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::choreography::{AgentEntityAddress, AgentEntityClass};
use crate::conversation::{
    AgentConversationError, AgentConversationHistoryCursor, AgentConversationHistoryEntry,
    AgentConversationHistoryKind, AgentConversationHistorySequence, AgentConversationHistoryStore,
};
use rakka_agent_workflow::AgentTimestampMillis;

use crate::identity::{AgentGoalId, AgentId, AgentOperationId, AgentRunId, AgentTaskId, TenantId};
use crate::observability::{
    AgentDecisionEvent, AgentDecisionEventSink, AgentDecisionKind, AgentObservabilityError,
};
use crate::schema::{AgentSchemaError, AgentSchemaPolicy};
use crate::task::{
    AgentContentDigest, AgentTaskError, AgentTaskHistoryCursor, AgentTaskHistoryEntry,
    AgentTaskHistoryKind, AgentTaskHistorySequence, AgentTaskHistoryStore, AgentTaskStatus,
};
use crate::team::{
    AgentTeamError, AgentTeamHistoryCursor, AgentTeamHistoryEntry, AgentTeamHistoryKind,
    AgentTeamHistorySequence, AgentTeamHistoryStore,
};

/// Separator between a coordination cursor's scope and its sequence.
///
/// Identity segments are validated free of the scope and persistence separators
/// but may contain a colon, so the sequence is taken from the *last* one — the
/// suffix this module always appends. The shape generalizes the substrate's
/// `<task-id>:<sequence>` public cursor without changing it.
pub const AGENT_COORDINATION_CURSOR_SEPARATOR: char = ':';

/// Largest page a coordination replay will return, whatever the caller asks.
pub const AGENT_COORDINATION_MAX_PAGE_SIZE: usize = 64;

/// Default page size of a coordination replay: what a limit of zero asks for.
pub const AGENT_COORDINATION_DEFAULT_PAGE_SIZE: usize = 16;

/// Result of a scoped coordination replay.
pub type AgentCoordinationReplayResult<T> = Result<T, AgentCoordinationReplayError>;

/// The rejection of a coordination replay.
///
/// Every variant is a *read* fault. None of them is a durable decision, so none
/// belongs in a caller's refusal accounting.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum AgentCoordinationReplayError {
    /// The cursor is not `<scope-key><separator><sequence>`, or its scope does
    /// not parse.
    MalformedCursor {
        /// The cursor as supplied, for the caller to correct.
        cursor: String,
    },
    /// The cursor parses, but names a different scope than the read addresses.
    ///
    /// A cursor carries its own tenant and entity, so following one blindly
    /// would let a caller page another scope's log — including another
    /// tenant's — through a scope it is authorized for.
    ScopeMismatch {
        /// The scope the read addresses.
        expected: Box<AgentEntityAddress>,
        /// The scope the cursor names.
        actual: Box<AgentEntityAddress>,
    },
    /// The scope names a tenant other than the caller's authenticated one.
    ///
    /// A scope key carries its own tenant, and reading one on the strength of
    /// authentication for a different tenant would disclose that tenant's
    /// coordination history. The fence is part of
    /// [`AgentCoordinationSources::replay`] itself, so a surface serving the
    /// replay cannot forget it. It reveals nothing: the caller supplied the
    /// scope it is being refused for.
    ForeignTenant {
        /// The tenant the caller is authenticated as.
        authenticated: TenantId,
    },
    /// The addressed entity has no durable record at all.
    ///
    /// Distinct from an empty log: a created entity that has not yet flushed
    /// its first entry answers an honest empty page, while a scope that was
    /// never created — a mistyped id, a cursor pasted where the scope belongs
    /// — must not be vouched for with a page that reads as "this entity
    /// recorded nothing".
    ScopeUnknown {
        /// The class of the addressed scope.
        class: AgentEntityClass,
    },
    /// The addressed entity class keeps no replayable log.
    ///
    /// The agent entity records its lifecycle in settings revisions and audit,
    /// not in a sequenced log, so a replay over it is refused rather than
    /// answered with an empty page that would read as "nothing happened".
    ScopeNotReplayable {
        /// The class that has no log.
        class: AgentEntityClass,
    },
    /// The run scope was addressed with no decision-event sink wired.
    ///
    /// Distinct from an empty log: a deployment that never wired a sink has no
    /// run events to serve, and saying so is not the same as saying the run
    /// decided nothing.
    RunEventsUnavailable,
    /// The task history log failed to answer.
    Task(AgentTaskError),
    /// The team history log failed to answer.
    Team(AgentTeamError),
    /// The conversation history log failed to answer.
    Conversation(AgentConversationError),
    /// The run's decision-event sink failed to answer.
    ///
    /// An expired window is not here — it is the
    /// [`AgentCoordinationReplay::WindowExpired`] answer — so this is a sink
    /// outage. The sink's own backend code stays in the message rather than in
    /// [`Self::code`], because a backend vocabulary is not this surface's
    /// stable one.
    RunEvents(AgentObservabilityError),
    /// A log entry carries a schema version this binary does not read.
    ///
    /// The replay is the load path of the four coordination logs, and a
    /// record from a newer or older generation is refused there rather than
    /// handed to a reader with guessed semantics — the same `schema-version-*`
    /// codes the entity load paths answer.
    Schema(AgentSchemaError),
}

impl AgentCoordinationReplayError {
    /// Stable machine-readable code.
    ///
    /// Compatibility commitment: these codes reach A2A responses and the typed
    /// client.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::MalformedCursor { .. } => "coordination-cursor-malformed",
            Self::ScopeMismatch { .. } => "coordination-cursor-scope-mismatch",
            Self::ForeignTenant { .. } => "coordination-scope-foreign-tenant",
            Self::ScopeUnknown { .. } => "coordination-scope-unknown",
            Self::ScopeNotReplayable { .. } => "coordination-scope-not-replayable",
            Self::RunEventsUnavailable => "coordination-run-events-unavailable",
            Self::Task(error) => error.code(),
            Self::Team(error) => error.code(),
            Self::Conversation(error) => error.code(),
            Self::Schema(error) => error.code(),
            Self::RunEvents(_) => "coordination-run-events-failed",
        }
    }
}

impl Display for AgentCoordinationReplayError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::MalformedCursor { cursor } => {
                write!(f, "the coordination cursor `{cursor}` does not parse")
            }
            Self::ScopeMismatch { expected, actual } => write!(
                f,
                "the coordination cursor names scope {actual}, but the read addresses {expected}"
            ),
            Self::ForeignTenant { authenticated } => write!(
                f,
                "the scope names a tenant other than the authenticated {authenticated}"
            ),
            Self::ScopeUnknown { class } => {
                write!(f, "the addressed {class} entity has no durable record")
            }
            Self::ScopeNotReplayable { class } => {
                write!(f, "the {class} entity keeps no replayable coordination log")
            }
            Self::RunEventsUnavailable => {
                f.write_str("no decision-event sink is wired, so run events cannot be replayed")
            }
            Self::Task(error) => Display::fmt(error, f),
            Self::Team(error) => Display::fmt(error, f),
            Self::Conversation(error) => Display::fmt(error, f),
            Self::RunEvents(error) => {
                write!(f, "the run's decision-event sink failed: {error}")
            }
            Self::Schema(error) => Display::fmt(error, f),
        }
    }
}

impl std::error::Error for AgentCoordinationReplayError {}

impl From<AgentTaskError> for AgentCoordinationReplayError {
    fn from(error: AgentTaskError) -> Self {
        Self::Task(error)
    }
}

impl From<AgentTeamError> for AgentCoordinationReplayError {
    fn from(error: AgentTeamError) -> Self {
        Self::Team(error)
    }
}

impl From<AgentConversationError> for AgentCoordinationReplayError {
    fn from(error: AgentConversationError) -> Self {
        Self::Conversation(error)
    }
}

impl From<AgentObservabilityError> for AgentCoordinationReplayError {
    fn from(error: AgentObservabilityError) -> Self {
        Self::RunEvents(error)
    }
}

impl From<AgentSchemaError> for AgentCoordinationReplayError {
    fn from(error: AgentSchemaError) -> Self {
        Self::Schema(error)
    }
}

/// A reconnect position in one scope's coordination log.
///
/// Opaque to callers by contract: the encoding is stable, but a caller echoes
/// what it was given rather than composing one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCoordinationCursor {
    scope: AgentEntityAddress,
    after: u64,
}

impl AgentCoordinationCursor {
    /// A cursor positioned after `sequence` in `scope`.
    #[must_use]
    pub const fn new(scope: AgentEntityAddress, after: u64) -> Self {
        Self { scope, after }
    }

    /// The scope the cursor reads.
    #[must_use]
    pub const fn scope(&self) -> &AgentEntityAddress {
        &self.scope
    }

    /// The sequence the reader has already seen.
    #[must_use]
    pub const fn after(&self) -> u64 {
        self.after
    }

    /// The stable wire encoding, `<scope-key>:<sequence>`.
    #[must_use]
    pub fn encode(&self) -> String {
        format!(
            "{}{AGENT_COORDINATION_CURSOR_SEPARATOR}{}",
            self.scope.key(),
            self.after
        )
    }

    /// Parses a cursor, failing closed on anything that is not one.
    ///
    /// # Errors
    ///
    /// [`AgentCoordinationReplayError::MalformedCursor`] when the separator is
    /// missing, the sequence is not a number, or the scope does not parse.
    pub fn parse(cursor: &str) -> AgentCoordinationReplayResult<Self> {
        let malformed = || AgentCoordinationReplayError::MalformedCursor {
            cursor: cursor.to_string(),
        };
        // The sequence is the suffix this module appends, so it is always past
        // the *last* separator — an identity segment containing one is safe.
        let (scope, after) = cursor
            .rsplit_once(AGENT_COORDINATION_CURSOR_SEPARATOR)
            .ok_or_else(malformed)?;
        let after: u64 = after.parse().map_err(|_| malformed())?;
        let scope = AgentEntityAddress::parse(scope).map_err(|_| malformed())?;
        Ok(Self { scope, after })
    }
}

impl Display for AgentCoordinationCursor {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.encode())
    }
}

/// A logical coordinate a coordination event happened at.
///
/// Kept typed rather than flattened to one number: a reader that could not tell
/// an assignment generation from a conversation round would be reading a
/// coincidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentCoordinationCoordinate {
    /// The assignment generation in force.
    Generation(u64),
    /// A moderated conversation's position.
    Round {
        /// The round.
        round: u64,
        /// The turn within it, when the entry names one.
        turn: Option<u32>,
    },
    /// The run turn that decided.
    Turn(u64),
}

/// What one coordination event records.
///
/// The wrapper keeps each source's own vocabulary rather than flattening them
/// into a single list: the labels are not disjoint — a team claim is recorded on
/// *both* the task and the team, under the same source label — so the scope
/// class is part of the identity of the kind, not context around it. That is why
/// [`Self::as_label`] is `<scope-class>/<source-label>` and not the source label
/// alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentCoordinationEventKind {
    /// Recorded on a task's durable history.
    Task(AgentTaskHistoryKind),
    /// Recorded on a team's durable history.
    Team(AgentTeamHistoryKind),
    /// Recorded on a moderated conversation's durable history.
    Conversation(AgentConversationHistoryKind),
    /// Recorded by a run's decision-event sink.
    Run(AgentDecisionKind),
}

impl AgentCoordinationEventKind {
    /// The entity class that recorded it.
    #[must_use]
    pub const fn class(self) -> AgentEntityClass {
        match self {
            Self::Task(_) => AgentEntityClass::Task,
            Self::Team(_) => AgentEntityClass::Team,
            Self::Conversation(_) => AgentEntityClass::Conversation,
            Self::Run(_) => AgentEntityClass::Run,
        }
    }

    /// The source vocabulary's own label, without the class qualifier.
    #[must_use]
    pub const fn source_label(self) -> &'static str {
        match self {
            Self::Task(kind) => kind.as_label(),
            Self::Team(kind) => kind.as_label(),
            Self::Conversation(kind) => kind.as_label(),
            Self::Run(kind) => kind.as_label(),
        }
    }

    /// Stable, injective label: `<scope-class>/<source-label>`.
    ///
    /// The qualifier is load-bearing. A task records `team-claim-recorded` when
    /// it takes a board claim and the team records `team-claim-recorded` when it
    /// makes one — the same fact from two sides, at two sequences, in two logs.
    /// A label that could not tell them apart would let a reader filtering by
    /// kind silently merge them.
    #[must_use]
    pub fn as_label(self) -> String {
        format!("{}/{}", self.class().as_label(), self.source_label())
    }
}

impl Display for AgentCoordinationEventKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.class().as_label(), self.source_label())
    }
}

/// One replayable coordination event.
///
/// A bounded projection of a durable record: identities, coordinates, stable
/// codes, and the digest a source already carried. It never carries messages,
/// turn bodies, prompts, tool payloads, memory records, or resolved credentials
/// — those live behind the artifact references the source records point at,
/// under the application's own retention policy
/// ([specification 17.14](../../docs/plans/rakka-agent/spec.md)).
///
/// # What a merged event cannot carry
///
/// The four sources do not record the same things, and the projection says so
/// rather than pretending otherwise. [`Self::status`] is a task fact and is
/// absent elsewhere; [`Self::goal`] is recorded only by the run's decision
/// source; a team claim id and a conversation reason arrive inside
/// [`Self::detail`], which is where the task side already puts a claim id. The
/// decision event's loop phase, decision source, revisions, selected tools,
/// safety class, and trace context do not survive the merge at all — the
/// three history logs carry no trace context, so a uniform correlation field
/// would be uniformly empty for three scopes out of four.
/// [`crate::query::assemble_agent_session_view`] is the run-scoped view that
/// keeps them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AgentCoordinationEvent {
    /// The entity whose log recorded it.
    pub scope: AgentEntityAddress,
    /// Monotonic sequence within that scope.
    pub sequence: u64,
    /// The cursor positioned immediately after this event.
    pub cursor: String,
    /// What was recorded.
    pub kind: AgentCoordinationEventKind,
    /// The operation that produced it. Replaying one durable transition always
    /// resolves to this same identity, which is what makes the log
    /// deduplication-safe on both sides.
    pub operation_id: AgentOperationId,
    /// The agent involved, when one was: the task's assignee, the team member,
    /// the conversation participant, or the run's own agent.
    pub agent: Option<AgentId>,
    /// The task involved, when one was.
    pub task: Option<AgentTaskId>,
    /// The run involved, when one was.
    pub run: Option<AgentRunId>,
    /// The goal the decision served, when its source recorded one. Only the
    /// run's decision events carry a goal binding; the three history logs do
    /// not record one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub goal: Option<AgentGoalId>,
    /// The task's status once the transition committed. A task fact; absent for
    /// every other scope.
    pub status: Option<AgentTaskStatus>,
    /// The fingerprint of the content the transition involved, when the source
    /// recorded one — a proposed result, a repeated result a stagnation trip
    /// detected. A digest, never the content.
    pub digest: Option<AgentContentDigest>,
    /// The logical coordinate the entry names, when it names one.
    pub coordinate: Option<AgentCoordinationCoordinate>,
    /// The authenticated principal involved, when one was, as `type:id`.
    pub principal: Option<String>,
    /// Bounded detail from the source: a refusal code, a terminal reason, a
    /// claim id, a count.
    pub detail: String,
    /// When the transition committed.
    pub occurred_at: AgentTimestampMillis,
}

impl AgentCoordinationEvent {
    fn cursor_for(scope: &AgentEntityAddress, sequence: u64) -> String {
        AgentCoordinationCursor::new(scope.clone(), sequence).encode()
    }

    /// Projects one task-history entry.
    #[must_use]
    pub fn from_task_history(scope: &AgentEntityAddress, entry: &AgentTaskHistoryEntry) -> Self {
        Self {
            scope: scope.clone(),
            sequence: entry.sequence.get(),
            cursor: Self::cursor_for(scope, entry.sequence.get()),
            kind: AgentCoordinationEventKind::Task(entry.kind),
            operation_id: entry.operation_id.clone(),
            agent: entry.agent.clone(),
            task: match scope {
                AgentEntityAddress::Task(task) => Some(task.task().clone()),
                _ => None,
            },
            run: entry.run.clone(),
            goal: None,
            status: Some(entry.status),
            digest: entry.digest.clone(),
            coordinate: entry
                .generation
                .map(|generation| AgentCoordinationCoordinate::Generation(generation.get())),
            principal: entry.principal.clone(),
            detail: entry.detail.clone(),
            occurred_at: entry.at,
        }
    }

    /// Projects one team-history entry.
    #[must_use]
    pub fn from_team_history(scope: &AgentEntityAddress, entry: &AgentTeamHistoryEntry) -> Self {
        // The claim id is the team log's own correlation key and has no field on
        // the merged shape; the task side already records it in `detail`, so the
        // two sides of a claim read the same way.
        let detail = match (&entry.claim, entry.detail.is_empty()) {
            (Some(claim), true) => claim.to_string(),
            (Some(claim), false) => format!("{claim} {}", entry.detail),
            (None, _) => entry.detail.clone(),
        };
        Self {
            scope: scope.clone(),
            sequence: entry.sequence.get(),
            cursor: Self::cursor_for(scope, entry.sequence.get()),
            kind: AgentCoordinationEventKind::Team(entry.kind),
            operation_id: entry.operation_id.clone(),
            agent: entry.member.clone(),
            task: entry.task.clone(),
            run: None,
            goal: None,
            status: None,
            digest: None,
            coordinate: None,
            principal: None,
            detail,
            occurred_at: entry.at,
        }
    }

    /// Projects one conversation-history entry.
    #[must_use]
    pub fn from_conversation_history(
        scope: &AgentEntityAddress,
        entry: &AgentConversationHistoryEntry,
    ) -> Self {
        let detail = match (&entry.reason, entry.detail.is_empty()) {
            (Some(reason), true) => reason.clone(),
            (Some(reason), false) => format!("{} {reason}", entry.detail),
            (None, _) => entry.detail.clone(),
        };
        Self {
            scope: scope.clone(),
            sequence: entry.sequence.get(),
            cursor: Self::cursor_for(scope, entry.sequence.get()),
            kind: AgentCoordinationEventKind::Conversation(entry.kind),
            operation_id: entry.operation_id.clone(),
            agent: entry.participant.clone(),
            task: None,
            run: None,
            goal: None,
            status: None,
            digest: None,
            coordinate: entry.round.map(|round| AgentCoordinationCoordinate::Round {
                round,
                turn: entry.turn,
            }),
            principal: entry.principal.clone(),
            detail,
            occurred_at: entry.at,
        }
    }

    /// Projects one run decision event.
    #[must_use]
    pub fn from_decision(scope: &AgentEntityAddress, event: &AgentDecisionEvent) -> Self {
        Self {
            scope: scope.clone(),
            sequence: event.sequence,
            cursor: Self::cursor_for(scope, event.sequence),
            kind: AgentCoordinationEventKind::Run(event.kind),
            operation_id: event.operation_id.clone(),
            agent: match scope {
                AgentEntityAddress::Run(run) => Some(run.agent().clone()),
                _ => None,
            },
            task: event.task.clone(),
            run: match scope {
                AgentEntityAddress::Run(run) => Some(run.run().clone()),
                _ => None,
            },
            goal: event.goal.clone(),
            status: None,
            digest: None,
            coordinate: Some(AgentCoordinationCoordinate::Turn(event.turn)),
            principal: None,
            detail: event.reason_code.clone().unwrap_or_default(),
            occurred_at: event.occurred_at,
        }
    }
}

/// One contiguous page of a scope's coordination log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AgentCoordinationPage {
    /// The scope read.
    pub scope: AgentEntityAddress,
    /// The events, oldest first, contiguous from the cursor.
    pub events: Vec<AgentCoordinationEvent>,
    /// The cursor resuming after this page, when the log holds more.
    pub next_cursor: Option<String>,
    /// The highest sequence this page proves the reader has now seen. A reader
    /// that keeps this and passes it back never repeats and never skips.
    pub complete_through: u64,
    /// Whether the log itself holds more past this page.
    ///
    /// Distinct from "the entity is current": an entity flushes what a
    /// transition owed on the settle pass *after* it committed, so `false` here
    /// means only that the log has nothing further right now.
    pub has_more: bool,
    /// Events the source recorded and durably lost before any reader could see
    /// them, when the source counts such losses.
    ///
    /// Only the run's decision outbox can lose an event — it is a ring that
    /// drops its oldest rather than fail a transition over telemetry — and the
    /// count is the difference between "resynchronize and you will have
    /// everything" and "these are gone". The history logs never lose an entry,
    /// so this is zero for them.
    pub unrecoverable_losses: u64,
}

/// The answer to one scoped replay.
///
/// Two arms, deliberately: a caller must handle the retention gap explicitly
/// rather than receive a short page it could mistake for a complete one
/// ([specification 17.13](../../docs/plans/rakka-agent/spec.md); scenario 45).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "outcome")]
#[non_exhaustive]
pub enum AgentCoordinationReplay {
    /// The cursor resumed.
    Page(AgentCoordinationPage),
    /// The cursor precedes what the log retains; resynchronize from
    /// authoritative state and resume at the floor.
    WindowExpired {
        /// The scope read.
        scope: AgentEntityAddress,
        /// The floor to resume from: the oldest sequence still retained at or
        /// past the reader's position, when anything is retained there.
        oldest_retained: Option<u64>,
        /// The cursor to resume from once the reader has resynchronized.
        resume_from: String,
    },
}

impl AgentCoordinationReplay {
    fn expired(scope: &AgentEntityAddress, oldest_retained: Option<u64>) -> Self {
        // Resuming *at* the floor means positioning after the one before it, so
        // the floor itself is the next entry delivered.
        let resume_after = oldest_retained.map_or(0, |oldest| oldest.saturating_sub(1));
        Self::WindowExpired {
            scope: scope.clone(),
            oldest_retained,
            resume_from: AgentCoordinationCursor::new(scope.clone(), resume_after).encode(),
        }
    }

    /// The page, when the cursor resumed.
    #[must_use]
    pub const fn page(&self) -> Option<&AgentCoordinationPage> {
        match self {
            Self::Page(page) => Some(page),
            Self::WindowExpired { .. } => None,
        }
    }

    /// Whether the reader must resynchronize before resuming.
    #[must_use]
    pub const fn requires_resync(&self) -> bool {
        matches!(self, Self::WindowExpired { .. })
    }
}

/// Resolves the sequence a read starts after, fencing the cursor's scope.
fn resume_after(
    scope: &AgentEntityAddress,
    cursor: Option<&str>,
) -> AgentCoordinationReplayResult<u64> {
    let Some(cursor) = cursor else {
        return Ok(0);
    };
    let parsed = AgentCoordinationCursor::parse(cursor)?;
    // A cursor carries its own tenant and entity. Following one that names a
    // different scope would page a log the caller did not address — and was not
    // authorized for — so the mismatch is refused, never reconciled.
    if parsed.scope() != scope {
        return Err(AgentCoordinationReplayError::ScopeMismatch {
            expected: Box::new(scope.clone()),
            actual: Box::new(parsed.scope().clone()),
        });
    }
    Ok(parsed.after())
}

fn clamp_limit(limit: usize) -> usize {
    if limit == 0 {
        AGENT_COORDINATION_DEFAULT_PAGE_SIZE
    } else {
        limit.min(AGENT_COORDINATION_MAX_PAGE_SIZE)
    }
}

fn page_from(
    scope: &AgentEntityAddress,
    after: u64,
    events: Vec<AgentCoordinationEvent>,
    has_more: bool,
    unrecoverable_losses: u64,
) -> AgentCoordinationPage {
    let complete_through = events.last().map_or(after, |event| event.sequence);
    let next_cursor =
        has_more.then(|| AgentCoordinationCursor::new(scope.clone(), complete_through).encode());
    AgentCoordinationPage {
        scope: scope.clone(),
        events,
        next_cursor,
        complete_through,
        has_more,
        unrecoverable_losses,
    }
}

/// Replays one task's coordination log.
///
/// # Errors
///
/// Fails on a malformed cursor, a cursor naming another scope, a history
/// backend fault, or an entry whose schema version `policy` refuses. A cursor
/// preceding the retained window is not an error: it resolves to
/// [`AgentCoordinationReplay::WindowExpired`].
pub async fn replay_task_coordination_events<History>(
    history: &History,
    scope: &AgentEntityAddress,
    cursor: Option<&str>,
    limit: usize,
    policy: &AgentSchemaPolicy,
) -> AgentCoordinationReplayResult<AgentCoordinationReplay>
where
    History: AgentTaskHistoryStore,
{
    let AgentEntityAddress::Task(task_scope) = scope else {
        return Err(AgentCoordinationReplayError::ScopeNotReplayable {
            class: scope.class(),
        });
    };
    let after = resume_after(scope, cursor)?;
    let read = AgentTaskHistoryCursor::start()
        .resuming_at(AgentTaskHistorySequence::new(after.saturating_add(1)))
        .with_limit(clamp_limit(limit));
    match history.read(task_scope, read).await {
        Err(AgentTaskError::HistoryWindowExpired { oldest_retained }) => {
            Ok(AgentCoordinationReplay::expired(
                scope,
                oldest_retained.map(AgentTaskHistorySequence::get),
            ))
        }
        Err(error) => Err(error.into()),
        Ok(page) => {
            for entry in &page.entries {
                policy.check_record(entry)?;
            }
            let has_more = page.next.is_some();
            let events = page
                .entries
                .iter()
                .map(|entry| AgentCoordinationEvent::from_task_history(scope, entry))
                .collect();
            Ok(AgentCoordinationReplay::Page(page_from(
                scope, after, events, has_more, 0,
            )))
        }
    }
}

/// Replays one team's coordination log.
///
/// # Errors
///
/// As [`replay_task_coordination_events`].
pub async fn replay_team_coordination_events<History>(
    history: &History,
    scope: &AgentEntityAddress,
    cursor: Option<&str>,
    limit: usize,
    policy: &AgentSchemaPolicy,
) -> AgentCoordinationReplayResult<AgentCoordinationReplay>
where
    History: AgentTeamHistoryStore,
{
    let AgentEntityAddress::Team(team_scope) = scope else {
        return Err(AgentCoordinationReplayError::ScopeNotReplayable {
            class: scope.class(),
        });
    };
    let after = resume_after(scope, cursor)?;
    let read = AgentTeamHistoryCursor::start()
        .resuming_at(AgentTeamHistorySequence::new(after.saturating_add(1)))
        .with_limit(clamp_limit(limit));
    match history.read(team_scope, read).await {
        Err(AgentTeamError::HistoryWindowExpired { oldest_retained }) => {
            Ok(AgentCoordinationReplay::expired(
                scope,
                oldest_retained.map(AgentTeamHistorySequence::get),
            ))
        }
        Err(error) => Err(error.into()),
        Ok(page) => {
            for entry in &page.entries {
                policy.check_record(entry)?;
            }
            let has_more = page.next.is_some();
            let events = page
                .entries
                .iter()
                .map(|entry| AgentCoordinationEvent::from_team_history(scope, entry))
                .collect();
            Ok(AgentCoordinationReplay::Page(page_from(
                scope, after, events, has_more, 0,
            )))
        }
    }
}

/// Replays one moderated conversation's coordination log.
///
/// # Errors
///
/// As [`replay_task_coordination_events`].
pub async fn replay_conversation_coordination_events<History>(
    history: &History,
    scope: &AgentEntityAddress,
    cursor: Option<&str>,
    limit: usize,
    policy: &AgentSchemaPolicy,
) -> AgentCoordinationReplayResult<AgentCoordinationReplay>
where
    History: AgentConversationHistoryStore,
{
    let AgentEntityAddress::Conversation(conversation_scope) = scope else {
        return Err(AgentCoordinationReplayError::ScopeNotReplayable {
            class: scope.class(),
        });
    };
    let after = resume_after(scope, cursor)?;
    let read = AgentConversationHistoryCursor::start()
        .resuming_at(AgentConversationHistorySequence::new(
            after.saturating_add(1),
        ))
        .with_limit(clamp_limit(limit));
    match history.read(conversation_scope, read).await {
        Err(AgentConversationError::HistoryWindowExpired { oldest_retained }) => {
            Ok(AgentCoordinationReplay::expired(
                scope,
                oldest_retained.map(AgentConversationHistorySequence::get),
            ))
        }
        Err(error) => Err(error.into()),
        Ok(page) => {
            for entry in &page.entries {
                policy.check_record(entry)?;
            }
            let has_more = page.next.is_some();
            let events = page
                .entries
                .iter()
                .map(|entry| AgentCoordinationEvent::from_conversation_history(scope, entry))
                .collect();
            Ok(AgentCoordinationReplay::Page(page_from(
                scope, after, events, has_more, 0,
            )))
        }
    }
}

/// Replays one run's decision log.
///
/// `losses` is the run's durable decision-drop count, which the caller reads
/// from authoritative state — the sink cannot know it. Pass zero when it is not
/// available; the page then reports no loss, which is the honest answer for a
/// reader that has nothing better.
///
/// # Errors
///
/// As [`replay_task_coordination_events`].
pub async fn replay_run_coordination_events(
    sink: Option<&dyn AgentDecisionEventSink>,
    scope: &AgentEntityAddress,
    cursor: Option<&str>,
    limit: usize,
    losses: u64,
    policy: &AgentSchemaPolicy,
) -> AgentCoordinationReplayResult<AgentCoordinationReplay> {
    let AgentEntityAddress::Run(run_scope) = scope else {
        return Err(AgentCoordinationReplayError::ScopeNotReplayable {
            class: scope.class(),
        });
    };
    let Some(sink) = sink else {
        return Err(AgentCoordinationReplayError::RunEventsUnavailable);
    };
    let after = resume_after(scope, cursor)?;
    let limit = clamp_limit(limit);
    // `has_more` is the sink's own explicit answer: the read contract only
    // promises up to `limit` events, so inferring it from the page length
    // would let a compliant short-paging sink strand the retained tail behind
    // a "you are current".
    match sink.read(run_scope, after, limit).await {
        Err(AgentObservabilityError::ReplayWindowExpired { oldest_retained }) => {
            Ok(AgentCoordinationReplay::expired(scope, oldest_retained))
        }
        Err(error) => Err(error.into()),
        Ok(page) => {
            for event in &page.events {
                policy.check_record(event)?;
            }
            let events = page
                .events
                .iter()
                .map(|event| AgentCoordinationEvent::from_decision(scope, event))
                .collect();
            Ok(AgentCoordinationReplay::Page(page_from(
                scope,
                after,
                events,
                page.has_more,
                losses,
            )))
        }
    }
}

/// Every durable log a scoped replay may reach.
///
/// The fan-out is one place so a caller does not repeat the class match — and so
/// a class with no log is refused in exactly one place rather than answered with
/// an empty page in several.
pub struct AgentCoordinationSources<'a, Tasks, Teams, Conversations> {
    tasks: &'a Tasks,
    teams: &'a Teams,
    conversations: &'a Conversations,
    runs: Option<&'a dyn AgentDecisionEventSink>,
    run_losses: u64,
    policy: AgentSchemaPolicy,
}

impl<'a, Tasks, Teams, Conversations> AgentCoordinationSources<'a, Tasks, Teams, Conversations>
where
    Tasks: AgentTaskHistoryStore,
    Teams: AgentTeamHistoryStore,
    Conversations: AgentConversationHistoryStore,
{
    /// Binds the three history logs. The run scope stays unavailable until a
    /// decision sink is wired.
    #[must_use]
    pub const fn new(tasks: &'a Tasks, teams: &'a Teams, conversations: &'a Conversations) -> Self {
        Self {
            tasks,
            teams,
            conversations,
            runs: None,
            run_losses: 0,
            policy: AgentSchemaPolicy::n_plus_one(),
        }
    }

    /// Replaces the default N/N+1 schema policy every log entry is checked
    /// against before it is handed to a reader.
    #[must_use]
    pub const fn with_schema_policy(mut self, policy: AgentSchemaPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Wires the run scope's decision-event sink.
    #[must_use]
    pub const fn with_run_events(mut self, sink: &'a dyn AgentDecisionEventSink) -> Self {
        self.runs = Some(sink);
        self
    }

    /// Declares the addressed run's durable decision-drop count, which only
    /// authoritative run state knows.
    #[must_use]
    pub const fn with_run_losses(mut self, losses: u64) -> Self {
        self.run_losses = losses;
        self
    }

    /// Replays whichever log the scope addresses, on behalf of the
    /// authenticated tenant.
    ///
    /// The tenant fence lives here, in the one entry point every surface
    /// shares, rather than in each surface's own handler: a scope key carries
    /// its own tenant, and a surface that forgot to compare it against the
    /// caller's authenticated one would disclose another tenant's
    /// coordination history. A surface that wants to refuse earlier — before
    /// consulting its authorizer — may still pre-check; this fence is what
    /// makes forgetting impossible.
    ///
    /// # Errors
    ///
    /// As [`replay_task_coordination_events`], plus
    /// [`AgentCoordinationReplayError::ForeignTenant`] for a scope naming a tenant
    /// other than `authenticated`,
    /// [`AgentCoordinationReplayError::ScopeNotReplayable`] for a class that keeps no
    /// log, and [`AgentCoordinationReplayError::RunEventsUnavailable`] for a run scope
    /// with no sink wired.
    pub async fn replay(
        &self,
        authenticated: &TenantId,
        scope: &AgentEntityAddress,
        cursor: Option<&str>,
        limit: usize,
    ) -> AgentCoordinationReplayResult<AgentCoordinationReplay> {
        if scope.tenant() != authenticated {
            return Err(AgentCoordinationReplayError::ForeignTenant {
                authenticated: authenticated.clone(),
            });
        }
        match scope {
            AgentEntityAddress::Task(_) => {
                replay_task_coordination_events(self.tasks, scope, cursor, limit, &self.policy)
                    .await
            }
            AgentEntityAddress::Team(_) => {
                replay_team_coordination_events(self.teams, scope, cursor, limit, &self.policy)
                    .await
            }
            AgentEntityAddress::Conversation(_) => {
                replay_conversation_coordination_events(
                    self.conversations,
                    scope,
                    cursor,
                    limit,
                    &self.policy,
                )
                .await
            }
            AgentEntityAddress::Run(_) => {
                replay_run_coordination_events(
                    self.runs,
                    scope,
                    cursor,
                    limit,
                    self.run_losses,
                    &self.policy,
                )
                .await
            }
            // The agent entity records its lifecycle in settings revisions and
            // audit, not in a sequenced log. An empty page would read as "this
            // agent has done nothing", which is a different claim entirely.
            AgentEntityAddress::Agent(_) => Err(AgentCoordinationReplayError::ScopeNotReplayable {
                class: scope.class(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{AgentTaskScope, TenantId};

    fn task_scope(tenant: &str, task: &str) -> AgentEntityAddress {
        AgentEntityAddress::Task(
            AgentTaskScope::new(
                TenantId::new(tenant),
                AgentTaskId::new(task).expect("a legal task id"),
            )
            .expect("a legal task scope"),
        )
    }

    #[test]
    fn a_cursor_round_trips_through_its_encoding() {
        let scope = task_scope("acme", "order-1");
        let cursor = AgentCoordinationCursor::new(scope.clone(), 7);
        assert_eq!(cursor.encode(), "task/acme/order-1:7");
        let parsed = AgentCoordinationCursor::parse(&cursor.encode()).expect("it parses");
        assert_eq!(parsed, cursor);
        assert_eq!(parsed.scope(), &scope);
        assert_eq!(parsed.after(), 7);
    }

    #[test]
    fn an_identifier_holding_a_separator_still_round_trips() {
        // Identity segments are validated free of the scope and persistence
        // separators, not of the colon — so the sequence must be taken from the
        // last one, never the first.
        let scope = task_scope("acme", "order:5");
        let cursor = AgentCoordinationCursor::new(scope.clone(), 9);
        assert_eq!(cursor.encode(), "task/acme/order:5:9");
        let parsed = AgentCoordinationCursor::parse(&cursor.encode()).expect("it parses");
        assert_eq!(parsed.scope(), &scope);
        assert_eq!(parsed.after(), 9);
    }

    #[test]
    fn a_malformed_or_foreign_cursor_fails_closed() {
        for malformed in [
            "order-1:5",              // no class segment: the substrate's shape
            "task/acme/order-1",      // no sequence
            "task/acme/order-1:none", // sequence is not a number
            "widget/acme/thing:1",    // unknown class
            "",
        ] {
            let error = AgentCoordinationCursor::parse(malformed)
                .expect_err("a cursor that is not one is refused");
            assert_eq!(error.code(), "coordination-cursor-malformed", "{malformed}");
        }

        // A bare address whose last segment ends in digits parses — as a
        // *different* scope. The scope fence is what stops it, not the parser.
        let addressed = task_scope("acme", "order-1");
        let error = resume_after(&addressed, Some("task/acme/order-2:3"))
            .expect_err("a cursor naming another scope is refused");
        assert_eq!(error.code(), "coordination-cursor-scope-mismatch");

        let cross_tenant = resume_after(&addressed, Some("task/other/order-1:3"))
            .expect_err("a cursor naming another tenant is refused");
        assert_eq!(cross_tenant.code(), "coordination-cursor-scope-mismatch");
    }

    #[test]
    fn the_kind_label_is_injective_across_scopes() {
        // The same fact recorded on both sides of a claim shares one source
        // label; only the class qualifier tells the two logs apart.
        let task_side = AgentCoordinationEventKind::Task(AgentTaskHistoryKind::TeamClaimRecorded);
        let team_side = AgentCoordinationEventKind::Team(AgentTeamHistoryKind::ClaimRecorded);
        assert_eq!(task_side.source_label(), team_side.source_label());
        assert_ne!(task_side.as_label(), team_side.as_label());
        assert_eq!(task_side.as_label(), "task/team-claim-recorded");
        assert_eq!(team_side.as_label(), "team/team-claim-recorded");
    }

    #[test]
    fn an_expired_window_resumes_at_the_floor_it_reports() {
        let scope = task_scope("acme", "order-1");
        let expired = AgentCoordinationReplay::expired(&scope, Some(40));
        assert!(expired.requires_resync());
        let AgentCoordinationReplay::WindowExpired { resume_from, .. } = &expired else {
            panic!("an expired window is not a page");
        };
        let resumed = AgentCoordinationCursor::parse(resume_from).expect("the floor cursor parses");
        assert_eq!(
            resumed.after(),
            39,
            "resuming *at* 40 means positioning after 39"
        );
    }

    #[test]
    fn an_empty_floor_resumes_from_the_beginning() {
        let scope = task_scope("acme", "order-1");
        let expired = AgentCoordinationReplay::expired(&scope, None);
        let AgentCoordinationReplay::WindowExpired { resume_from, .. } = &expired else {
            panic!("an expired window is not a page");
        };
        assert_eq!(
            AgentCoordinationCursor::parse(resume_from)
                .expect("it parses")
                .after(),
            0
        );
    }
}
