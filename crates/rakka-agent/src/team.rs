//! The sharded team entity and its durable shared task board
//! ([specification 8.10](../../docs/plans/rakka-agent/spec.md)).
//!
//! A team is trusted application data: a stable
//! [`crate::identity::AgentTeamId`], a leader, a
//! root goal, bounded member types/instances with capability scopes, a
//! creation/expiry policy, and the durable shared task board — all of it the
//! entity's own state, written in single compare-and-sets. Claim, release,
//! and transfer are atomic board decisions under revision and lease fencing
//! with stable derived operation ids; a stale command fails closed on the
//! entry's claim epoch, and a replayed one is answered from the operation
//! log rather than deciding twice.
//!
//! **The board never holds ownership.** A recorded claim drives the task
//! entity's own `decide_assignment` through the
//! [`AgentExchangeKind::TeamClaim`] exchange, and the task's
//! assignment-generation fence stays the one-normal-owner guarantee
//! (scenario 42). What the board stores about an accepted assignment —
//! generation and run — is an observational echo delivered by the
//! [`AgentExchangeKind::TeamClaimResult`] exchange, never a second copy of
//! ownership. The claim lease bounds the claim-*pending* window only: an
//! activated claim is never stealable by lease expiry, because run budgets
//! bound execution.
//!
//! The entity follows the acyclic choreography rule: accepting a delivered
//! exchange makes local progress only, and owed exchanges are committed to
//! the journal inside the deciding transition and drained by the courier.
//! Idle teams passivate — the board is data, not a resident coordinator —
//! and expiry is observed lazily by the next command or settle pass, never
//! by a timer.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rakka_agent_workflow::{AgentCorrelationId, AgentTimestampMillis, StateSchemaVersion};
use rakka_core::{
    actor_future, Actor, ActorAction, ActorContext, ActorFuture, ActorOptions, MetricsRecorder,
    NoopMetricsRecorder, ReplyTo,
};
use rakka_persistence::{DurableStateStore, PersistenceId};
use rakka_sharding::{
    ClusterNodeRuntime, ClusterNodeRuntimeResult, ClusterSharding, ClusterShardingResult, Entity,
    EntityContext, EntityId, EntityTypeKey, EntityTypeRegistration, ShardBufferConfig,
    ShardedEntityRef,
};
use serde::{Deserialize, Serialize};

use crate::choreography::{
    drive_pending_exchanges, AgentChoreographyError, AgentEntityAddress, AgentExchangeEnvelope,
    AgentExchangeHost, AgentExchangeJournal, AgentExchangeKind, AgentExchangeParticipant,
    AgentExchangePayload, AgentExchangeReply, AgentExchangeResult, AgentExchangeRouter,
    AgentExchangeState, AgentExchangeTransition,
};
use crate::coordination::{
    team_claim_id_for, team_claim_operation_id, team_claim_release_operation_id,
    AgentCoordinationError, AgentTeamClaimAction, AgentTeamClaimCommand, AgentTeamClaimOutcome,
    AgentTeamClaimResultNotice, AgentTeamPolicy, AGENT_TEAM_CLAIM_PAYLOAD_TYPE,
    AGENT_TEAM_CLAIM_RESULT_PAYLOAD_TYPE,
};
use crate::definition::{AgentCapabilityId, AgentRevisionNumber, AgentRevisionProvenance};
use crate::identity::{
    AgentGoalId, AgentId, AgentIdentityError, AgentOperationId, AgentTaskId, AgentTaskScope,
    AgentTeamClaimId, AgentTeamScope,
};
use crate::observability::{
    record_agent_domain_counter, record_unsettleable_exchanges, METRIC_AGENT_TEAM_OPERATIONS,
};
use crate::schema::{
    AgentRecordKind, AgentSchemaError, AgentSchemaPolicy, VersionedAgentRecord,
    CURRENT_AGENT_TEAM_HISTORY_SCHEMA_VERSION, CURRENT_AGENT_TEAM_STATE_SCHEMA_VERSION,
};

/// Result type for team entity operations.
pub type AgentTeamResult<T> = Result<T, AgentTeamError>;

/// Clock supplying the timestamps team transitions persist.
pub type AgentTeamClock = Arc<dyn Fn() -> AgentTimestampMillis + Send + Sync>;

/// The system clock, stamping transitions where they commit.
#[must_use]
pub fn system_team_clock() -> AgentTeamClock {
    Arc::new(|| {
        let millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| {
                u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
            });
        AgentTimestampMillis::new(millis)
    })
}

/// The default sharded entity type of team entities.
pub const DEFAULT_AGENT_TEAM_ENTITY_TYPE: &str = "RakkaAgentTeam";

/// Maximum serialized bytes of one team's materialized state.
pub const AGENT_TEAM_MATERIALIZED_MAX_BYTES: usize = 32 * 1024;

/// Bytes held back from the materialized bound so a settle transition never
/// finds the record too large to write.
pub const AGENT_TEAM_STATE_GROWTH_RESERVE_BYTES: usize = 4 * 1024;

/// Bounded window of resolved operations a team remembers for deduplication.
pub const AGENT_TEAM_OPERATION_LOG_CAPACITY: usize = 64;

/// Bounded outbox of history entries a team may owe its sink.
pub const AGENT_TEAM_PENDING_HISTORY_CAPACITY: usize = 32;

/// The most history entries one team transition records.
pub const AGENT_TEAM_MAX_HISTORY_PER_TRANSITION: usize = 4;

/// Maximum bytes of one bounded history or refusal detail.
pub const AGENT_TEAM_DETAIL_MAX_LENGTH: usize = 512;

/// Default page size of a team history read.
pub const AGENT_TEAM_HISTORY_DEFAULT_PAGE_SIZE: usize = 16;

/// Maximum page size a team history cursor may request.
pub const AGENT_TEAM_HISTORY_MAX_PAGE_SIZE: usize = 64;

/// Payload type of the bounded receipt a team returns for a claim result.
pub const AGENT_TEAM_CLAIM_RESULT_RECEIPT_PAYLOAD_TYPE: &str = "rakka.agent.TeamClaimResultReceipt";

/// Payload type of the bounded receipt a team returns for a terminal notice.
pub const AGENT_TEAM_TERMINAL_RECEIPT_PAYLOAD_TYPE: &str = "rakka.agent.TeamTerminalReceipt";

const DEFAULT_AGENT_TEAM_PASSIVATION_BUFFER_DURATION: Duration = Duration::from_millis(25);

fn bounded_detail(detail: impl Into<String>) -> String {
    let mut detail = detail.into();
    if detail.len() > AGENT_TEAM_DETAIL_MAX_LENGTH {
        detail.truncate(
            (0..=AGENT_TEAM_DETAIL_MAX_LENGTH)
                .rev()
                .find(|index| detail.is_char_boundary(*index))
                .unwrap_or(0),
        );
    }
    detail
}

/// Lifecycle of one team.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentTeamStatus {
    /// The team accepts board and membership operations.
    Active,
    /// The team was disbanded by an authorized command; the board is
    /// read-only history.
    Disbanded,
    /// The team's creation policy expired it; observed lazily, never by a
    /// timer.
    Expired,
}

impl AgentTeamStatus {
    /// Whether no further board or membership operation is accepted.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Disbanded | Self::Expired)
    }

    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Disbanded => "disbanded",
            Self::Expired => "expired",
        }
    }
}

impl Display for AgentTeamStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// One bounded team member record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTeamMember {
    /// The capability scopes this member was admitted under. Trusted setup
    /// data — never model output — and never an authority source of its own:
    /// task-side authorization still decides what a member's runs may do.
    pub capability_scopes: BTreeSet<AgentCapabilityId>,
    /// When the member joined.
    pub joined_at: AgentTimestampMillis,
    /// The lifecycle revision the membership change committed under.
    pub revision: AgentRevisionNumber,
}

/// Status of one shared task-board entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentTeamBoardEntryStatus {
    /// Posted and claimable.
    Open,
    /// A claim is recorded and its assignment outcome is pending.
    Pending,
    /// A release was requested and its task-side outcome is pending.
    Releasing,
    /// The claimant's assignment durably accepted; the entry mirrors the
    /// owner the task's own fence guarantees.
    Active,
    /// The task reported a state that closes the entry — terminal, unknown,
    /// or foreign.
    Done,
}

impl AgentTeamBoardEntryStatus {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Pending => "pending",
            Self::Releasing => "releasing",
            Self::Active => "active",
            Self::Done => "done",
        }
    }
}

impl Display for AgentTeamBoardEntryStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// The claim one board entry currently records.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTeamBoardClaim {
    /// The derived claim identity.
    pub claim: AgentTeamClaimId,
    /// The claiming member.
    pub member: AgentId,
    /// When the claim's pending window lapses and the entry becomes
    /// stealable. Meaningless once the claim activates.
    pub lease_expires_at: AgentTimestampMillis,
    /// When the claim was recorded.
    pub claimed_at: AgentTimestampMillis,
    /// The accepted assignment generation, echoed by the task after
    /// activation. Observational only — never ownership.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation_echo: Option<crate::task::AgentAssignmentGeneration>,
    /// The run serving the accepted generation, echoed by the task.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_echo: Option<crate::identity::AgentRunId>,
}

/// One shared task-board entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTeamBoardEntry {
    /// The posted task.
    pub task: AgentTaskId,
    /// The member that posted it.
    pub posted_by: AgentId,
    /// When it was posted.
    pub posted_at: AgentTimestampMillis,
    /// The entry's claim epoch: every board decision over the entry bumps
    /// it, and a command carrying an older expectation fails closed.
    pub claim_epoch: u64,
    /// Where the entry stands.
    pub status: AgentTeamBoardEntryStatus,
    /// The recorded claim, while one stands.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claim: Option<AgentTeamBoardClaim>,
    /// The bounded code of the last refusal or terminal echo that touched
    /// the entry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_code: Option<String>,
}

/// One bounded mediated peer message on the team's durable ring.
///
/// Recipients read the ring through the team query surface; nothing is
/// pushed. The ring drops its oldest entry when full, and the drop is
/// visible in [`AgentTeam::messages_dropped`] — bounded loss, never silent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTeamMessage {
    /// Monotonic sequence within the team.
    pub sequence: u64,
    /// The sending member.
    pub from: AgentId,
    /// The addressed member; `None` is a broadcast to the team.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to: Option<AgentId>,
    /// The bounded message body.
    pub body: String,
    /// When it was appended.
    pub at: AgentTimestampMillis,
}

/// The materialized team: membership, board, and message ring.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTeam {
    /// Lifecycle of the team.
    pub status: AgentTeamStatus,
    /// The team leader. A member; may release or transfer any pending claim.
    pub leader: AgentId,
    /// The root goal this team works toward.
    pub root_goal: AgentGoalId,
    /// The trusted policy in force, embedded at creation.
    pub policy: AgentTeamPolicy,
    /// Fences membership and disband commands.
    pub lifecycle_revision: AgentRevisionNumber,
    members: BTreeMap<AgentId, AgentTeamMember>,
    board: BTreeMap<AgentTaskId, AgentTeamBoardEntry>,
    messages: VecDeque<AgentTeamMessage>,
    /// Messages the bounded ring has dropped, oldest first.
    pub messages_dropped: u64,
    next_message_sequence: u64,
    /// When the team was created.
    pub created_at: AgentTimestampMillis,
    /// When the team expires, when its policy sets a horizon. Observed
    /// lazily.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<AgentTimestampMillis>,
}

impl AgentTeam {
    /// The bounded membership.
    #[must_use]
    pub const fn members(&self) -> &BTreeMap<AgentId, AgentTeamMember> {
        &self.members
    }

    /// The shared task board.
    #[must_use]
    pub const fn board(&self) -> &BTreeMap<AgentTaskId, AgentTeamBoardEntry> {
        &self.board
    }

    /// The bounded message ring, oldest first.
    #[must_use]
    pub const fn messages(&self) -> &VecDeque<AgentTeamMessage> {
        &self.messages
    }

    /// Whether one agent is a member.
    #[must_use]
    pub fn is_member(&self, agent: &AgentId) -> bool {
        self.members.contains_key(agent)
    }

    /// Whether the team's expiry horizon has passed.
    #[must_use]
    pub fn is_expired_at(&self, now: AgentTimestampMillis) -> bool {
        self.expires_at
            .is_some_and(|expires| now.as_millis() >= expires.as_millis())
    }

    fn check_bounds(&self) -> AgentTeamResult<()> {
        let bytes = serde_json::to_vec(self)
            .map(|bytes| bytes.len())
            .unwrap_or(usize::MAX);
        let maximum =
            AGENT_TEAM_MATERIALIZED_MAX_BYTES.saturating_sub(AGENT_TEAM_STATE_GROWTH_RESERVE_BYTES);
        if bytes > maximum {
            return Err(AgentTeamError::StateBounds { bytes, maximum });
        }
        Ok(())
    }
}

/// The trusted creation record of one team.
///
/// Construction data from the application wiring — leader, policy, root
/// goal, and initial members can never come from model output or a wire
/// peer; the A2A surface carries no create operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTeamCreation {
    /// The team leader; always a member.
    pub leader: AgentId,
    /// The root goal the team works toward.
    pub root_goal: AgentGoalId,
    /// The policy in force for the team's lifetime.
    pub policy: AgentTeamPolicy,
    /// Initial members and their capability scopes. The leader joins
    /// whether or not it is listed.
    #[serde(default)]
    pub members: BTreeMap<AgentId, BTreeSet<AgentCapabilityId>>,
}

/// Monotonic sequence of one team history entry.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct AgentTeamHistorySequence(u64);

impl AgentTeamHistorySequence {
    /// The first sequence a team's history uses.
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

impl Display for AgentTeamHistorySequence {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, f)
    }
}

/// What one team history entry records
/// ([specification 17.13](../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentTeamHistoryKind {
    /// The team was created.
    Created,
    /// A member joined.
    MemberJoined,
    /// A member left.
    MemberLeft,
    /// A task was posted to the board.
    TaskPosted,
    /// A claim, steal, or transfer-in was recorded on an entry.
    ClaimRecorded,
    /// A release of a pending claim was requested.
    ClaimReleaseRequested,
    /// A claim resolved — activated, refused, released, superseded, or
    /// closed by a terminal echo.
    ClaimSettled,
    /// A board entry closed eagerly by its task's terminal notice
    /// ([specification 8.10](../../../docs/plans/rakka-agent/spec.md));
    /// the detail carries the task's terminal-reason code.
    TaskClosed,
    /// A pending claim transferred to another member.
    TransferRecorded,
    /// A mediated message was appended to the ring.
    MessageAppended,
    /// The team was disbanded.
    Disbanded,
    /// The team's expiry horizon was durably observed.
    Expired,
}

impl AgentTeamHistoryKind {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Created => "team-created",
            Self::MemberJoined => "team-member-joined",
            Self::MemberLeft => "team-member-left",
            Self::TaskPosted => "team-task-posted",
            Self::ClaimRecorded => "team-claim-recorded",
            Self::ClaimReleaseRequested => "team-claim-release-requested",
            Self::ClaimSettled => "team-claim-settled",
            Self::TaskClosed => "team-task-closed",
            Self::TransferRecorded => "team-transfer-recorded",
            Self::MessageAppended => "team-message-appended",
            Self::Disbanded => "team-disbanded",
            Self::Expired => "team-expired",
        }
    }
}

impl Display for AgentTeamHistoryKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// One append-only entry in a team's durable history
/// ([specification 17.13](../../docs/plans/rakka-agent/spec.md)).
///
/// A bounded record of *what happened*: identities and stable codes only.
/// It never carries message bodies, prompts, memory records, or resolved
/// credentials.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTeamHistoryEntry {
    schema_version: StateSchemaVersion,
    /// Monotonic sequence within the team, and the append's idempotency key.
    pub sequence: AgentTeamHistorySequence,
    /// What the entry records.
    pub kind: AgentTeamHistoryKind,
    /// The operation that produced it.
    pub operation_id: AgentOperationId,
    /// The member involved, when one was.
    pub member: Option<AgentId>,
    /// The board task involved, when one was.
    pub task: Option<AgentTaskId>,
    /// The claim involved, when one was.
    pub claim: Option<AgentTeamClaimId>,
    /// Bounded detail: the refusal code, the settle code, the count.
    pub detail: String,
    /// When the transition committed.
    pub at: AgentTimestampMillis,
}

impl AgentTeamHistoryEntry {
    pub(crate) fn new(
        sequence: AgentTeamHistorySequence,
        kind: AgentTeamHistoryKind,
        operation_id: AgentOperationId,
        at: AgentTimestampMillis,
    ) -> Self {
        Self {
            schema_version: CURRENT_AGENT_TEAM_HISTORY_SCHEMA_VERSION,
            sequence,
            kind,
            operation_id,
            member: None,
            task: None,
            claim: None,
            detail: String::new(),
            at,
        }
    }

    fn with_member(mut self, member: AgentId) -> Self {
        self.member = Some(member);
        self
    }

    fn with_task(mut self, task: AgentTaskId) -> Self {
        self.task = Some(task);
        self
    }

    fn with_claim(mut self, claim: AgentTeamClaimId) -> Self {
        self.claim = Some(claim);
        self
    }

    fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = bounded_detail(detail);
        self
    }
}

impl VersionedAgentRecord for AgentTeamHistoryEntry {
    const RECORD_KIND: AgentRecordKind = AgentRecordKind::TeamHistoryEntry;

    fn schema_version(&self) -> StateSchemaVersion {
        self.schema_version
    }
}

/// A bounded, authorized read over a team's history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentTeamHistoryCursor {
    after: Option<AgentTeamHistorySequence>,
    limit: usize,
}

impl AgentTeamHistoryCursor {
    /// A cursor over the whole history, from the beginning.
    #[must_use]
    pub const fn start() -> Self {
        Self {
            after: None,
            limit: AGENT_TEAM_HISTORY_DEFAULT_PAGE_SIZE,
        }
    }

    /// A cursor resuming after one sequence.
    #[must_use]
    pub const fn after(sequence: AgentTeamHistorySequence) -> Self {
        Self {
            after: Some(sequence),
            limit: AGENT_TEAM_HISTORY_DEFAULT_PAGE_SIZE,
        }
    }

    /// Sets the page size, clamped to [`AGENT_TEAM_HISTORY_MAX_PAGE_SIZE`].
    #[must_use]
    pub const fn with_limit(mut self, limit: usize) -> Self {
        self.limit = if limit == 0 {
            1
        } else if limit > AGENT_TEAM_HISTORY_MAX_PAGE_SIZE {
            AGENT_TEAM_HISTORY_MAX_PAGE_SIZE
        } else {
            limit
        };
        self
    }

    /// Repositions this cursor so `sequence` is the next entry it expects.
    ///
    /// The companion of [`AgentTeamError::HistoryWindowExpired`]: a reader
    /// handed a retained floor resumes *at* it, keeping its page size. Sequences
    /// start at [`AgentTeamHistorySequence::FIRST`], so a zero resumes from the
    /// beginning.
    #[must_use]
    pub const fn resuming_at(mut self, sequence: AgentTeamHistorySequence) -> Self {
        self.after = match sequence.get() {
            0 => None,
            value => Some(AgentTeamHistorySequence::new(value - 1)),
        };
        self
    }

    /// The sequence this page resumes after.
    #[must_use]
    pub const fn position(&self) -> Option<AgentTeamHistorySequence> {
        self.after
    }

    /// The clamped page size.
    #[must_use]
    pub const fn limit(&self) -> usize {
        self.limit
    }
}

impl Default for AgentTeamHistoryCursor {
    fn default() -> Self {
        Self::start()
    }
}

/// One bounded page of team history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentTeamHistoryPage {
    /// The entries, oldest first.
    pub entries: Vec<AgentTeamHistoryEntry>,
    /// The cursor that resumes after this page, when more history exists.
    pub next: Option<AgentTeamHistoryCursor>,
}

impl AgentTeamHistoryPage {
    /// Whether more history follows this page.
    #[must_use]
    pub const fn has_more(&self) -> bool {
        self.next.is_some()
    }
}

/// Boxed future returned by an [`AgentTeamHistoryStore`].
pub type AgentTeamHistoryFuture<'a, T> =
    Pin<Box<dyn Future<Output = AgentTeamResult<T>> + Send + 'a>>;

/// The append-only durable history of every team, separate from the bounded
/// materialized state that drives transitions
/// ([specification 17.13](../../docs/plans/rakka-agent/spec.md)).
///
/// An append is idempotent on `(scope, sequence)`: the entity assigns the
/// sequence inside the transition that produced the entry, so re-driving an
/// interrupted flush writes the same entry to the same slot. A store that
/// finds a different entry already at a sequence must fail closed rather
/// than overwrite it.
pub trait AgentTeamHistoryStore: Clone + Send + Sync + 'static {
    /// Stable backend name, used in telemetry.
    fn backend_name(&self) -> &'static str;

    /// Appends one entry, idempotently.
    fn append<'a>(
        &'a self,
        scope: &'a AgentTeamScope,
        entry: &'a AgentTeamHistoryEntry,
    ) -> AgentTeamHistoryFuture<'a, ()>;

    /// Reads one bounded page, contiguous from the cursor.
    ///
    /// A backend MUST fail a read with
    /// [`AgentTeamError::HistoryWindowExpired`] — naming the oldest entry the
    /// reader can actually resume from — whenever answering would otherwise
    /// vouch for entries the reader has not seen: a cursor preceding the
    /// retained window, a discontinuity at the read head, or a cursor past
    /// the newest retained entry, which this log never issued. A
    /// discontinuity *inside* the page truncates it before the hole with a
    /// `next` cursor instead, so the retained prefix is delivered whole and
    /// the next read is refused at the hole. See
    /// [`crate::task::AgentTaskHistoryStore::read`] for the full contract;
    /// [`crate::testkit::assert_team_history_store_contract`] is the harness
    /// that proves it.
    fn read<'a>(
        &'a self,
        scope: &'a AgentTeamScope,
        cursor: AgentTeamHistoryCursor,
    ) -> AgentTeamHistoryFuture<'a, AgentTeamHistoryPage>;
}

/// An in-memory team history, for tests and single-process deployments.
///
/// The PostgreSQL backend is a recorded follow-up of slice 5.2.
#[derive(Debug, Clone, Default)]
pub struct InMemoryAgentTeamHistoryStore {
    inner: Arc<Mutex<InMemoryTeamHistoryInner>>,
}

/// The shared state behind every clone of one in-memory team history.
///
/// Retention lives *inside* the shared state, beside the log it bounds: the
/// store is `Clone` by contract, and a bound that lived per-handle would let a
/// clone taken before `with_retention` keep appending to the same shared log
/// unbounded — the retention contract silently failing to hold.
#[derive(Debug, Default)]
struct InMemoryTeamHistoryInner {
    entries: BTreeMap<String, BTreeMap<u64, AgentTeamHistoryEntry>>,
    retention: Option<usize>,
}

impl InMemoryAgentTeamHistoryStore {
    /// Creates an empty history that retains everything appended to it.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Bounds the history retained per team, evicting the oldest entries.
    ///
    /// Retention is off by default because this log is also the audit record
    /// [specification 17.13](../../docs/plans/rakka-agent/spec.md) requires, and
    /// the entity refuses a transition rather than lose an entry. Enabling it
    /// forfeits that audit obligation for whatever the window drops, in exchange
    /// for a bounded log and the explicit expired-window answer a replay cursor
    /// needs. A limit of zero is treated as one. The bound is shared state:
    /// clones share the log, so they share its retention, whichever handle set
    /// it.
    #[must_use]
    pub fn with_retention(self, entries: usize) -> Self {
        self.inner
            .lock()
            .expect("the team history should not be poisoned")
            .retention = Some(entries.max(1));
        self
    }

    /// How many entries one team has.
    #[must_use]
    pub fn len(&self, scope: &AgentTeamScope) -> usize {
        self.inner
            .lock()
            .expect("the team history should not be poisoned")
            .entries
            .get(&scope.key())
            .map_or(0, BTreeMap::len)
    }

    /// Whether one team has no history.
    #[must_use]
    pub fn is_empty(&self, scope: &AgentTeamScope) -> bool {
        self.len(scope) == 0
    }
}

impl AgentTeamHistoryStore for InMemoryAgentTeamHistoryStore {
    fn backend_name(&self) -> &'static str {
        "in-memory"
    }

    fn append<'a>(
        &'a self,
        scope: &'a AgentTeamScope,
        entry: &'a AgentTeamHistoryEntry,
    ) -> AgentTeamHistoryFuture<'a, ()> {
        Box::pin(async move {
            let mut inner = self
                .inner
                .lock()
                .expect("the team history should not be poisoned");
            let retention = inner.retention;
            let team = inner.entries.entry(scope.key()).or_default();
            match team.get(&entry.sequence.get()) {
                Some(existing) if existing == entry => Ok(()),
                Some(_) => Err(AgentTeamError::HistoryConflict {
                    sequence: entry.sequence,
                }),
                None => {
                    team.insert(entry.sequence.get(), entry.clone());
                    if let Some(retention) = retention {
                        while team.len() > retention {
                            let oldest = *team
                                .keys()
                                .next()
                                .expect("a history longer than its retention holds an entry");
                            team.remove(&oldest);
                        }
                    }
                    Ok(())
                }
            }
        })
    }

    fn read<'a>(
        &'a self,
        scope: &'a AgentTeamScope,
        cursor: AgentTeamHistoryCursor,
    ) -> AgentTeamHistoryFuture<'a, AgentTeamHistoryPage> {
        Box::pin(async move {
            let inner = self
                .inner
                .lock()
                .expect("the team history should not be poisoned");
            let Some(team) = inner.entries.get(&scope.key()) else {
                // A positioned cursor into a scope with no log at all is a
                // cursor this store never issued; an empty page would vouch
                // for entries the reader believes it has seen. Only the
                // start-of-log read is honestly empty here.
                if cursor.position().is_some_and(|after| after.get() > 0) {
                    return Err(AgentTeamError::HistoryWindowExpired {
                        oldest_retained: None,
                    });
                }
                return Ok(AgentTeamHistoryPage {
                    entries: Vec::new(),
                    next: None,
                });
            };

            // The sequence the reader expects next: one past its cursor, or the
            // very first entry when it is starting from the beginning.
            let start = cursor
                .position()
                .map_or(AgentTeamHistorySequence::FIRST.get(), |after| {
                    after.get().saturating_add(1)
                });
            // A cursor past the newest retained entry was never issued by this
            // log: an empty page would stamp "you are current" over sequences
            // the reader has not seen, and once the log grows past the cursor
            // the reader would resume across them silently.
            let newest = team.keys().next_back().copied().unwrap_or_default();
            if start > newest.saturating_add(1) {
                return Err(AgentTeamError::HistoryWindowExpired {
                    oldest_retained: team
                        .keys()
                        .next()
                        .copied()
                        .map(AgentTeamHistorySequence::new),
                });
            }
            // History sequences are dense — the transition that consumes one
            // pushes its entry in the same step — so a missing entry means the
            // window moved (or a durable backend lost it). A hole at the read
            // head is refused with the floor past it; a hole further in
            // truncates the page instead, so the retained prefix is delivered
            // whole whatever the reader's page size, and the *next* read
            // starts at the hole and gets the refusal.
            if let Some((&first, _)) = team.range(start..).next() {
                if first != start {
                    return Err(AgentTeamError::HistoryWindowExpired {
                        oldest_retained: Some(AgentTeamHistorySequence::new(first)),
                    });
                }
            }
            let mut page: Vec<AgentTeamHistoryEntry> = Vec::new();
            let mut expected = start;
            for (&sequence, entry) in team.range(start..) {
                if sequence != expected || page.len() == cursor.limit() {
                    break;
                }
                page.push(entry.clone());
                expected = sequence.saturating_add(1);
            }
            let next = team
                .range(expected..)
                .next()
                .is_some()
                .then(|| {
                    page.last().map(|entry| {
                        AgentTeamHistoryCursor::after(entry.sequence).with_limit(cursor.limit())
                    })
                })
                .flatten();

            Ok(AgentTeamHistoryPage {
                entries: page,
                next,
            })
        })
    }
}

/// The compact result of one accepted team transition.
///
/// A replayed operation returns this again rather than transitioning twice.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTeamOutcome {
    /// Lifecycle after the transition.
    pub status: AgentTeamStatus,
    /// The lifecycle revision after the transition.
    pub lifecycle_revision: AgentRevisionNumber,
    /// Membership size after the transition.
    pub members: usize,
    /// Board size after the transition.
    pub board_entries: usize,
    /// Message-ring size after the transition.
    pub messages: usize,
    /// The board entry the transition touched, when one was.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry: Option<Box<AgentTeamBoardEntry>>,
}

/// A bounded, credential-free projection of one team.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AgentTeamSnapshot {
    /// The team's scope.
    pub scope: AgentTeamScope,
    /// Lifecycle.
    pub status: AgentTeamStatus,
    /// The leader.
    pub leader: AgentId,
    /// The root goal.
    pub root_goal: AgentGoalId,
    /// The policy revision in force.
    pub policy_revision: AgentRevisionNumber,
    /// The lifecycle revision.
    pub lifecycle_revision: AgentRevisionNumber,
    /// The bounded membership.
    pub members: BTreeMap<AgentId, AgentTeamMember>,
    /// The board entries, in task order.
    pub board: Vec<AgentTeamBoardEntry>,
    /// The message ring, oldest first.
    pub messages: Vec<AgentTeamMessage>,
    /// Messages the bounded ring dropped.
    pub messages_dropped: u64,
    /// When the team was created.
    pub created_at: AgentTimestampMillis,
    /// When the team expires, when its policy sets a horizon.
    pub expires_at: Option<AgentTimestampMillis>,
    /// How many history entries the team has recorded.
    pub history_entries: u64,
    /// History entries recorded but not yet flushed to the history sink.
    ///
    /// A replay reader at [`Self::history_entries`] is current only when this
    /// is zero: an entity flushes what a transition owed on the settle pass
    /// *after* it committed, so a log that answers "no more" may still be
    /// waiting for a tail.
    pub owed_history: usize,
    /// The time of the last accepted transition.
    pub updated_at: AgentTimestampMillis,
}

/// The bounded log of resolved team operations.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTeamOperationLog {
    entries: VecDeque<AgentTeamOperationLogEntry>,
}

impl AgentTeamOperationLog {
    /// The outcome a previously applied operation produced, if it is still
    /// in the window.
    #[must_use]
    pub fn outcome(&self, operation_id: &AgentOperationId) -> Option<&AgentTeamOutcome> {
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

    fn record(&mut self, operation_id: AgentOperationId, outcome: AgentTeamOutcome) {
        self.entries.push_back(AgentTeamOperationLogEntry {
            operation_id,
            outcome,
        });
        while self.entries.len() > AGENT_TEAM_OPERATION_LOG_CAPACITY {
            self.entries.pop_front();
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AgentTeamOperationLogEntry {
    operation_id: AgentOperationId,
    outcome: AgentTeamOutcome,
}

/// The durable state of one team entity.
///
/// The materialized team, the history it owes its sink, the operations it
/// has resolved, and the exchange journal — all in one compare-and-set.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentTeamState {
    schema_version: StateSchemaVersion,
    scope: AgentTeamScope,
    team: Option<AgentTeam>,
    applied_operations: AgentTeamOperationLog,
    pending_history: Vec<AgentTeamHistoryEntry>,
    next_history_sequence: AgentTeamHistorySequence,
    journal: AgentExchangeJournal,
    updated_at: AgentTimestampMillis,
}

impl AgentTeamState {
    /// The state of a team that has never been created.
    #[must_use]
    pub fn uncreated(scope: AgentTeamScope, now: AgentTimestampMillis) -> Self {
        Self {
            schema_version: CURRENT_AGENT_TEAM_STATE_SCHEMA_VERSION,
            scope,
            team: None,
            applied_operations: AgentTeamOperationLog::default(),
            pending_history: Vec::new(),
            next_history_sequence: AgentTeamHistorySequence::FIRST,
            journal: AgentExchangeJournal::new(),
            updated_at: now,
        }
    }

    /// The scope this state belongs to.
    #[must_use]
    pub const fn scope(&self) -> &AgentTeamScope {
        &self.scope
    }

    /// The materialized team, once it has been created.
    #[must_use]
    pub const fn team(&self) -> Option<&AgentTeam> {
        self.team.as_ref()
    }

    /// Whether the team has been created.
    #[must_use]
    pub const fn is_created(&self) -> bool {
        self.team.is_some()
    }

    /// The bounded log of resolved operations.
    #[must_use]
    pub const fn applied_operations(&self) -> &AgentTeamOperationLog {
        &self.applied_operations
    }

    /// The history entries the team owes its sink.
    #[must_use]
    pub fn pending_history(&self) -> &[AgentTeamHistoryEntry] {
        &self.pending_history
    }

    /// The time of the last accepted transition.
    #[must_use]
    pub const fn updated_at(&self) -> AgentTimestampMillis {
        self.updated_at
    }

    /// How many further history entries the team may record before its
    /// outbox is full.
    #[must_use]
    pub fn history_headroom(&self) -> usize {
        AGENT_TEAM_PENDING_HISTORY_CAPACITY.saturating_sub(self.pending_history.len())
    }

    /// The compact outcome describing the current state.
    #[must_use]
    pub fn outcome(&self) -> AgentTeamOutcome {
        self.outcome_for(None)
    }

    fn outcome_for(&self, task: Option<&AgentTaskId>) -> AgentTeamOutcome {
        let Some(team) = &self.team else {
            return AgentTeamOutcome {
                status: AgentTeamStatus::Active,
                lifecycle_revision: AgentRevisionNumber::INITIAL,
                members: 0,
                board_entries: 0,
                messages: 0,
                entry: None,
            };
        };
        AgentTeamOutcome {
            status: team.status,
            lifecycle_revision: team.lifecycle_revision,
            members: team.members.len(),
            board_entries: team.board.len(),
            messages: team.messages.len(),
            entry: task
                .and_then(|task| team.board.get(task))
                .cloned()
                .map(Box::new),
        }
    }

    /// A bounded, credential-free projection of this state.
    #[must_use]
    pub fn snapshot(&self) -> Option<AgentTeamSnapshot> {
        let team = self.team.as_ref()?;
        Some(AgentTeamSnapshot {
            scope: self.scope.clone(),
            status: team.status,
            leader: team.leader.clone(),
            root_goal: team.root_goal.clone(),
            policy_revision: team.policy.revision,
            lifecycle_revision: team.lifecycle_revision,
            members: team.members.clone(),
            board: team.board.values().cloned().collect(),
            messages: team.messages.iter().cloned().collect(),
            messages_dropped: team.messages_dropped,
            created_at: team.created_at,
            expires_at: team.expires_at,
            history_entries: self.next_history_sequence.get().saturating_sub(1),
            owed_history: self.pending_history.len(),
            updated_at: self.updated_at,
        })
    }

    fn record_history(
        &mut self,
        build: impl FnOnce(AgentTeamHistorySequence) -> AgentTeamHistoryEntry,
    ) {
        let sequence = self.next_history_sequence;
        self.next_history_sequence = sequence.next();
        self.pending_history.push(build(sequence));
    }

    fn clear_flushed_history(&mut self, flushed: &[AgentTeamHistorySequence]) {
        self.pending_history
            .retain(|entry| !flushed.contains(&entry.sequence));
    }

    fn team_mut(&mut self) -> AgentTeamResult<&mut AgentTeam> {
        self.team
            .as_mut()
            .ok_or_else(|| AgentTeamError::NotCreated {
                scope: self.scope.clone(),
            })
    }

    /// Refuses every mutating command a non-active team cannot take.
    fn require_active(&self, now: AgentTimestampMillis) -> AgentTeamResult<&AgentTeam> {
        let Some(team) = self.team.as_ref() else {
            return Err(AgentTeamError::NotCreated {
                scope: self.scope.clone(),
            });
        };
        match team.status {
            AgentTeamStatus::Disbanded => Err(AgentTeamError::Disbanded),
            AgentTeamStatus::Expired => Err(AgentTeamError::Expired),
            // The horizon refuses before the durable flip: the settle pass
            // owns the flip, and a command's own refusal must not depend on
            // whether that pass has run yet.
            AgentTeamStatus::Active if team.is_expired_at(now) => Err(AgentTeamError::Expired),
            AgentTeamStatus::Active => Ok(team),
        }
    }
}

impl AgentExchangeState for AgentTeamState {
    fn exchange_journal(&self) -> &AgentExchangeJournal {
        &self.journal
    }

    fn exchange_journal_mut(&mut self) -> &mut AgentExchangeJournal {
        &mut self.journal
    }

    fn check_schema(&self, policy: &AgentSchemaPolicy) -> Result<(), AgentSchemaError> {
        policy.check_record(self)?;
        for entry in &self.pending_history {
            policy.check_record(entry)?;
        }
        Ok(())
    }
}

impl VersionedAgentRecord for AgentTeamState {
    const RECORD_KIND: AgentRecordKind = AgentRecordKind::TeamState;

    fn schema_version(&self) -> StateSchemaVersion {
        self.schema_version
    }
}

/// The domain half of the team entity.
///
/// It supplies bounded, pure transitions and nothing else; the choreography
/// substrate owns durability, deduplication, re-drive, and routing.
#[derive(Debug, Clone, Copy, Default)]
pub struct AgentTeamParticipant;

impl AgentExchangeParticipant for AgentTeamParticipant {
    type State = AgentTeamState;

    fn initialize(&self, address: &AgentEntityAddress, now: AgentTimestampMillis) -> Self::State {
        let scope = match address {
            AgentEntityAddress::Team(scope) => scope.clone(),
            // The host builds a participant for the address it serves, and
            // the entity refuses an id that does not parse into a team
            // scope, so this is unreachable in practice. An uncreated team
            // under an address that can never receive a creation is inert.
            other => AgentTeamScope::new(other.tenant().clone(), unroutable_team_id())
                .expect("the unroutable team scope is well formed"),
        };
        AgentTeamState::uncreated(scope, now)
    }

    fn apply(
        &self,
        state: &mut Self::State,
        envelope: &AgentExchangeEnvelope,
        now: AgentTimestampMillis,
    ) -> AgentExchangeTransition {
        let result = match envelope.kind() {
            AgentExchangeKind::TeamClaimResult => apply_claim_result(state, envelope, now),
            AgentExchangeKind::TeamTerminalNotice => apply_team_terminal(state, envelope, now),
            kind => refuse(
                "unsupported-exchange",
                format!("a team entity does not receive a {kind} exchange"),
            ),
        };
        AgentExchangeTransition::new(result)
    }

    fn check_settle(
        &self,
        envelope: &AgentExchangeEnvelope,
        result: &AgentExchangeResult,
    ) -> Result<(), AgentChoreographyError> {
        match envelope.kind() {
            AgentExchangeKind::TeamClaim if !result.is_accepted() => {
                // A refused claim action settles only under the task's
                // definitive arbitration answers. Every other refusal — an
                // `unsupported-exchange` from an owner that predates the
                // kind, a payload it could not decode — leaves the exchange
                // outstanding for re-drive until an owner that can answer it
                // does (the rolling-upgrade rule).
                match result.status().rejection_code() {
                    Some(
                        "team-claim-stale-epoch"
                        | "team-claim-already-owned"
                        | "team-claim-assignment-inflight"
                        | "team-claim-task-terminal"
                        | "team-claim-task-unknown"
                        | "team-claim-wrong-team"
                        | "team-claim-task-cancelling"
                        | "team-claim-handoff-pending"
                        | "team-claim-limit-exceeded"
                        | "task-state-too-large"
                        | "team-release-assignment-inflight"
                        | "team-release-unknown",
                    ) => Ok(()),
                    code => Err(AgentChoreographyError::UnsettleableRefusal {
                        kind: AgentExchangeKind::TeamClaim,
                        code: code.unwrap_or_default().to_string(),
                    }),
                }
            }
            AgentExchangeKind::TeamTerminalNotice if !result.is_accepted() => {
                // The receiver half of the shared classifier: the host
                // memoizes only the refusals classified definitive here, so
                // an undecodable payload stays unmemoized and re-runs the
                // arm once the binary can decode it (the rolling-upgrade
                // rule).
                match result.status().rejection_code() {
                    Some(code)
                        if crate::coordination::team_terminal_notice_refusal_settles(code) =>
                    {
                        Ok(())
                    }
                    code => Err(AgentChoreographyError::UnsettleableRefusal {
                        kind: AgentExchangeKind::TeamTerminalNotice,
                        code: code.unwrap_or_default().to_string(),
                    }),
                }
            }
            _ => Ok(()),
        }
    }

    fn settle(
        &self,
        state: &mut Self::State,
        envelope: &AgentExchangeEnvelope,
        result: &AgentExchangeResult,
        now: AgentTimestampMillis,
    ) -> Vec<AgentExchangeEnvelope> {
        if envelope.kind() == AgentExchangeKind::TeamClaim {
            settle_claim_action(state, envelope, result, now);
        }
        Vec::new()
    }
}

/// The unroutable placeholder a misaddressed host initializes under.
fn unroutable_team_id() -> crate::identity::AgentTeamId {
    crate::identity::AgentTeamId::new("unroutable").expect("the unroutable team id is well formed")
}

fn refuse(code: &str, message: String) -> AgentExchangeResult {
    AgentExchangeResult::rejected(
        code,
        message,
        AgentExchangePayload::empty(AGENT_TEAM_CLAIM_RESULT_RECEIPT_PAYLOAD_TYPE),
    )
}

fn accepted() -> AgentExchangeResult {
    AgentExchangeResult::accepted(AgentExchangePayload::empty(
        AGENT_TEAM_CLAIM_RESULT_RECEIPT_PAYLOAD_TYPE,
    ))
}

/// Applies one delivered claim-result notice: the board settlement of a
/// claim the task resolved.
fn apply_claim_result(
    state: &mut AgentTeamState,
    envelope: &AgentExchangeEnvelope,
    now: AgentTimestampMillis,
) -> AgentExchangeResult {
    let notice: AgentTeamClaimResultNotice = match envelope
        .payload()
        .decode(AGENT_TEAM_CLAIM_RESULT_PAYLOAD_TYPE)
    {
        Ok(notice) => notice,
        Err(error) => return refuse(error.code(), error.to_string()),
    };
    // The initiator must be the task the notice names: a notice about task T
    // sent by anything but T's entity is forged, however well formed.
    match envelope.initiator() {
        AgentEntityAddress::Task(scope) if scope == &notice.task => {}
        _ => {
            return refuse(
                "team-claim-forged",
                "a claim result must be initiated by the task it resolves".to_string(),
            )
        }
    }
    if state.team.is_none() {
        return refuse(
            "team-not-found",
            "no team exists under this scope".to_string(),
        );
    }
    let operation_id = envelope.operation_id().clone();
    let task_id = notice.task.task().clone();
    let team = state.team.as_mut().expect("checked above");
    let Some(entry) = team.board.get_mut(&task_id) else {
        return refuse(
            "team-claim-unknown",
            "the task is not on this team's board".to_string(),
        );
    };

    let current_claim = entry.claim.as_ref().map(|claim| claim.claim.clone());
    if current_claim.as_ref() == Some(&notice.claim) {
        match &notice.outcome {
            AgentTeamClaimOutcome::Activated {
                generation,
                run,
                member,
            } => {
                entry.status = AgentTeamBoardEntryStatus::Active;
                entry.last_code = None;
                if let Some(claim) = entry.claim.as_mut() {
                    claim.generation_echo = Some(*generation);
                    claim.run_echo = Some(run.clone());
                }
                let claim_id = notice.claim.clone();
                let member = member.clone();
                state.record_history(|sequence| {
                    AgentTeamHistoryEntry::new(
                        sequence,
                        AgentTeamHistoryKind::ClaimSettled,
                        operation_id,
                        now,
                    )
                    .with_task(task_id)
                    .with_claim(claim_id)
                    .with_member(member)
                    .with_detail("activated")
                });
            }
            AgentTeamClaimOutcome::Refused { code } => {
                if matches!(
                    entry.status,
                    AgentTeamBoardEntryStatus::Pending | AgentTeamBoardEntryStatus::Releasing
                ) {
                    let member = entry.claim.as_ref().map(|claim| claim.member.clone());
                    entry.status = AgentTeamBoardEntryStatus::Open;
                    entry.claim = None;
                    entry.last_code = Some(bounded_detail(code.clone()));
                    let claim_id = notice.claim.clone();
                    let code = code.clone();
                    state.record_history(|sequence| {
                        let mut entry = AgentTeamHistoryEntry::new(
                            sequence,
                            AgentTeamHistoryKind::ClaimSettled,
                            operation_id,
                            now,
                        )
                        .with_task(task_id)
                        .with_claim(claim_id)
                        .with_detail(code);
                        entry.member = member;
                        entry
                    });
                }
                // A refusal for a claim the board already moved past —
                // an Active entry, a Done entry — changes nothing: the
                // durable settlement is absorbing.
            }
        }
    } else if let AgentTeamClaimOutcome::Activated {
        generation,
        run,
        member,
    } = &notice.outcome
    {
        // The activation of a claim the board superseded — a steal or a
        // release that raced the original claim's acceptance. The task's
        // assignment fence durably accepted *this* claim, so every board
        // decision still in flight over the entry is doomed to a definitive
        // refusal whose settle leaves a filled echo alone — the owner's echo
        // therefore fills the entry whatever interim shape the board holds,
        // however the deliveries interleave. Only an entry the board already
        // closed, or one whose current claim itself carries an activation
        // echo, absorbs the notice.
        let absorbed = entry.status == AgentTeamBoardEntryStatus::Done
            || entry
                .claim
                .as_ref()
                .is_some_and(|claim| claim.generation_echo.is_some());
        if !absorbed {
            entry.status = AgentTeamBoardEntryStatus::Active;
            entry.claim = Some(AgentTeamBoardClaim {
                claim: notice.claim.clone(),
                member: member.clone(),
                lease_expires_at: now,
                claimed_at: now,
                generation_echo: Some(*generation),
                run_echo: Some(run.clone()),
            });
            entry.last_code = None;
            let claim_id = notice.claim.clone();
            let member = member.clone();
            state.record_history(|sequence| {
                AgentTeamHistoryEntry::new(
                    sequence,
                    AgentTeamHistoryKind::ClaimSettled,
                    operation_id,
                    now,
                )
                .with_task(task_id)
                .with_claim(claim_id)
                .with_member(member)
                .with_detail("activated")
            });
        }
        // Any other shape — the result of a superseded claim over an entry
        // that has moved on — is absorbed: its supersession was already
        // recorded when the newer decision committed.
    }
    // A refused superseded claim is absorbed without touching the entry.
    accepted()
}

/// Applies one delivered terminal notice: the task ended, and its board
/// entry closes eagerly instead of lingering until a member's claim attempt
/// is refused ([specification 8.10](../../../docs/plans/rakka-agent/spec.md)).
///
/// The close bumps the entry's claim epoch — it is a board decision — and
/// that bump is load-bearing: every stale in-flight board reply settles
/// into the epoch guard as a no-op afterwards, including the release arm
/// that would otherwise rewrite a `Done` entry `Active`. Deliberately no
/// active-team gate, the [`apply_claim_result`] posture: the board is data,
/// and an expired team's entry still deserves closing. A missing or
/// already-`Done` entry accepts idempotently with no board write — the
/// `Done` entry is the durable echo past the journal's bounded window, and
/// a task never posted here has nothing to close.
fn apply_team_terminal(
    state: &mut AgentTeamState,
    envelope: &AgentExchangeEnvelope,
    now: AgentTimestampMillis,
) -> AgentExchangeResult {
    let refuse_terminal = |code: &str, message: String| {
        AgentExchangeResult::rejected(
            code,
            message,
            AgentExchangePayload::empty(AGENT_TEAM_TERMINAL_RECEIPT_PAYLOAD_TYPE),
        )
    };
    let accepted_terminal = || {
        AgentExchangeResult::accepted(AgentExchangePayload::empty(
            AGENT_TEAM_TERMINAL_RECEIPT_PAYLOAD_TYPE,
        ))
    };
    let notice: crate::coordination::AgentTeamTerminalNotice = match envelope
        .payload()
        .decode(crate::coordination::AGENT_TEAM_TERMINAL_NOTICE_PAYLOAD_TYPE)
    {
        // Non-settling by the shared classifier, so a newer binary's payload
        // converges after a rolling upgrade instead of being refused for
        // good.
        Ok(notice) => notice,
        Err(error) => {
            return refuse_terminal("team-terminal-notice-undecodable", error.to_string())
        }
    };
    // The initiator must be the task the notice names: a notice about task T
    // sent by anything but T's entity is forged, however well formed.
    match envelope.initiator() {
        AgentEntityAddress::Task(scope) if scope == &notice.task => {}
        _ => {
            return refuse_terminal(
                "team-terminal-notice-forged",
                "a terminal notice must be initiated by the task it reports".to_string(),
            )
        }
    }
    if state.team.is_none() {
        return refuse_terminal(
            "team-not-found",
            "no team exists under this scope".to_string(),
        );
    }
    let operation_id = envelope.operation_id().clone();
    let task_id = notice.task.task().clone();
    let team = state.team.as_mut().expect("checked above");
    let Some(entry) = team.board.get_mut(&task_id) else {
        // Never posted here, or already evicted under ceiling pressure:
        // nothing to close, and the answer must be idempotent — a replay
        // after eviction converges on the same acceptance.
        return accepted_terminal();
    };
    if entry.status == AgentTeamBoardEntryStatus::Done {
        // The durable echo past the journal window — closed by an earlier
        // delivery of this notice or by a lazy claim refusal.
        return accepted_terminal();
    }
    let detail = bounded_detail(notice.terminal_reason);
    let member = entry.claim.as_ref().map(|claim| claim.member.clone());
    entry.status = AgentTeamBoardEntryStatus::Done;
    entry.claim_epoch = entry.claim_epoch.saturating_add(1);
    entry.claim = None;
    entry.last_code = Some(detail.clone());
    state.record_history(|sequence| {
        let mut history = AgentTeamHistoryEntry::new(
            sequence,
            AgentTeamHistoryKind::TaskClosed,
            operation_id,
            now,
        )
        .with_task(task_id)
        .with_detail(detail);
        history.member = member;
        history
    });
    accepted_terminal()
}

/// Settles the reply of one claim action the team initiated.
fn settle_claim_action(
    state: &mut AgentTeamState,
    envelope: &AgentExchangeEnvelope,
    result: &AgentExchangeResult,
    now: AgentTimestampMillis,
) {
    let Ok(command) = envelope
        .payload()
        .decode::<AgentTeamClaimCommand>(AGENT_TEAM_CLAIM_PAYLOAD_TYPE)
    else {
        // This binary encoded the payload it is now settling; a decode
        // failure is a construction bug surfaced loudly in tests.
        debug_assert!(false, "a team-claim payload failed to decode on settle");
        return;
    };
    let operation_id = envelope.operation_id().clone();
    let Some(team) = state.team.as_mut() else {
        return;
    };
    let Some(entry) = team.board.get_mut(&command.task) else {
        return;
    };
    // Only the reply of the entry's *current* decision settles it; a reply
    // to a superseded epoch is absorbed — the board already moved on.
    if entry.claim_epoch != command.epoch {
        return;
    }

    if result.is_accepted() {
        match command.action {
            // "Claim recorded" — the assignment outcome arrives later by the
            // claim-result exchange; the entry stays pending.
            AgentTeamClaimAction::Claim { .. } => {}
            AgentTeamClaimAction::Release => {
                if entry.status == AgentTeamBoardEntryStatus::Releasing {
                    let member = entry.claim.as_ref().map(|claim| claim.member.clone());
                    entry.status = AgentTeamBoardEntryStatus::Open;
                    entry.claim = None;
                    entry.last_code = Some("released".to_string());
                    let task = command.task.clone();
                    let claim = command.claim.clone();
                    state.record_history(|sequence| {
                        let mut history = AgentTeamHistoryEntry::new(
                            sequence,
                            AgentTeamHistoryKind::ClaimSettled,
                            operation_id,
                            now,
                        )
                        .with_task(task)
                        .with_claim(claim)
                        .with_detail("released");
                        history.member = member;
                        history
                    });
                }
            }
        }
        return;
    }

    let Some(code) = result.status().rejection_code() else {
        return;
    };
    // A restore or reopen speaks for the claim this decision minted. When the
    // entry's claim has already moved past it — the owner echo of a superseded
    // activation replaced it before this reply settled — the refusal's board
    // consequence was superseded too, and the settle changes nothing.
    let claim_is_current = entry
        .claim
        .as_ref()
        .is_some_and(|claim| claim.claim == command.claim);
    let (status, clear_claim) = match (&command.action, code) {
        // The steal raced an acceptance: the entry is owned, but by the
        // holder the task's fence protected, not this claimant. The owner
        // echo arrives with the superseded claim's activation — and when it
        // already has, there is nothing left to clear.
        (AgentTeamClaimAction::Claim { .. }, "team-claim-already-owned") => {
            if !claim_is_current {
                return;
            }
            (AgentTeamBoardEntryStatus::Active, true)
        }
        // Our own claimant's assignment accepted while the release was in
        // flight; the claim stands and the entry is owned.
        (AgentTeamClaimAction::Release, "team-claim-already-owned") => {
            (AgentTeamBoardEntryStatus::Active, false)
        }
        // The generation is offered and undecided: the release restores to
        // pending and may be retried after the offer resolves.
        (AgentTeamClaimAction::Release, "team-release-assignment-inflight") => {
            if !claim_is_current {
                return;
            }
            (AgentTeamBoardEntryStatus::Pending, false)
        }
        // The release outran its own claim exchange: the task has not seen
        // the claim yet, because the two ride independent operations the
        // courier does not order. The entry restores to pending — the claim
        // may still record, the release may be retried once it has, and an
        // expired lease keeps the steal escape hatch open. Releasing must
        // never be a shape the board cannot leave.
        (AgentTeamClaimAction::Release, "team-release-unknown") => {
            if !claim_is_current {
                return;
            }
            (AgentTeamBoardEntryStatus::Pending, false)
        }
        // The superseding claim found the superseded offer still undecided:
        // the entry reopens for a retry after the offer resolves — and if
        // the offer accepts instead, the superseded activation's owner echo
        // fills this reopened entry.
        (AgentTeamClaimAction::Claim { .. }, "team-claim-assignment-inflight") => {
            if !claim_is_current {
                return;
            }
            (AgentTeamBoardEntryStatus::Open, true)
        }
        (_, "team-claim-task-terminal" | "team-claim-task-unknown" | "team-claim-wrong-team") => {
            (AgentTeamBoardEntryStatus::Done, true)
        }
        (
            _,
            "team-claim-task-cancelling"
            | "team-claim-handoff-pending"
            | "team-claim-limit-exceeded"
            | "task-state-too-large",
        ) => {
            if !claim_is_current {
                return;
            }
            (AgentTeamBoardEntryStatus::Open, true)
        }
        // A stale epoch names a decision the board itself superseded; the
        // epoch guard above already absorbs most of these, and the rest
        // change nothing.
        _ => return,
    };
    let member = entry.claim.as_ref().map(|claim| claim.member.clone());
    entry.status = status;
    if clear_claim {
        entry.claim = None;
    }
    entry.last_code = Some(bounded_detail(code));
    let task = command.task.clone();
    let claim = command.claim.clone();
    let code = code.to_string();
    state.record_history(|sequence| {
        let mut history = AgentTeamHistoryEntry::new(
            sequence,
            AgentTeamHistoryKind::ClaimSettled,
            operation_id,
            now,
        )
        .with_task(task)
        .with_claim(claim)
        .with_detail(code);
        history.member = member;
        history
    });
}

/// One durable, deduplicated command over a team entity.
///
/// Every mutating variant carries the caller's stable operation id; a replay
/// answers [`AgentTeamEntityReply::Duplicate`] with the original outcome.
/// The `Create` and `Disband` commands are trusted application wiring and
/// have no A2A carrier.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum AgentTeamEntityCommand {
    /// Reads the bounded projection.
    Describe,
    /// Creates the team from trusted application data.
    Create {
        /// Stable dedup identity of this creation.
        operation_id: AgentOperationId,
        /// The trusted creation record.
        creation: Box<AgentTeamCreation>,
    },
    /// Adds a member, fenced on the lifecycle revision.
    AddMember {
        /// Stable dedup identity of this membership change.
        operation_id: AgentOperationId,
        /// The joining member.
        member: AgentId,
        /// The member's admitted capability scopes.
        capability_scopes: BTreeSet<AgentCapabilityId>,
        /// The lifecycle revision this change expects to succeed.
        expected_lifecycle_revision: AgentRevisionNumber,
        /// Who accepted the change, and when.
        provenance: Box<AgentRevisionProvenance>,
    },
    /// Removes a member, fenced on the lifecycle revision.
    RemoveMember {
        /// Stable dedup identity of this membership change.
        operation_id: AgentOperationId,
        /// The leaving member.
        member: AgentId,
        /// The lifecycle revision this change expects to succeed.
        expected_lifecycle_revision: AgentRevisionNumber,
        /// Who accepted the change, and when.
        provenance: Box<AgentRevisionProvenance>,
    },
    /// Posts an existing task to the shared board.
    PostTask {
        /// Stable dedup identity of this post.
        operation_id: AgentOperationId,
        /// The task to post. Its existence is validated by the claim
        /// exchange's task-side arbitration, never by the board.
        task: AgentTaskId,
        /// The posting member.
        posted_by: AgentId,
    },
    /// Claims a board entry for one member.
    Claim {
        /// Stable dedup identity of this claim command.
        operation_id: AgentOperationId,
        /// The board task.
        task: AgentTaskId,
        /// The claiming member.
        member: AgentId,
        /// The entry's claim epoch this command observed. A stale
        /// expectation fails closed.
        expected_epoch: u64,
    },
    /// Releases a pending claim before its assignment accepted.
    Release {
        /// Stable dedup identity of this release command.
        operation_id: AgentOperationId,
        /// The board task.
        task: AgentTaskId,
        /// The requesting member — the holder, or the leader.
        member: AgentId,
        /// The entry's claim epoch this command observed.
        expected_epoch: u64,
    },
    /// Transfers a pending claim to another member in one board decision.
    Transfer {
        /// Stable dedup identity of this transfer command.
        operation_id: AgentOperationId,
        /// The board task.
        task: AgentTaskId,
        /// The requesting member — the holder, or the leader.
        member: AgentId,
        /// The member receiving the claim.
        target: AgentId,
        /// The entry's claim epoch this command observed.
        expected_epoch: u64,
    },
    /// Appends a mediated peer message to the durable ring.
    AppendMessage {
        /// Stable dedup identity of this append.
        operation_id: AgentOperationId,
        /// The sending member.
        from: AgentId,
        /// The addressed member; `None` broadcasts.
        to: Option<AgentId>,
        /// The bounded body.
        body: String,
    },
    /// Disbands the team, fenced on the lifecycle revision.
    Disband {
        /// Stable dedup identity of this disband.
        operation_id: AgentOperationId,
        /// The lifecycle revision this change expects to succeed.
        expected_lifecycle_revision: AgentRevisionNumber,
        /// Who accepted the disband, and when.
        provenance: Box<AgentRevisionProvenance>,
        /// Bounded reason recorded in history.
        reason: String,
    },
}

impl AgentTeamEntityCommand {
    /// The stable operation id of a mutating command.
    #[must_use]
    pub const fn operation_id(&self) -> Option<&AgentOperationId> {
        match self {
            Self::Describe => None,
            Self::Create { operation_id, .. }
            | Self::AddMember { operation_id, .. }
            | Self::RemoveMember { operation_id, .. }
            | Self::PostTask { operation_id, .. }
            | Self::Claim { operation_id, .. }
            | Self::Release { operation_id, .. }
            | Self::Transfer { operation_id, .. }
            | Self::AppendMessage { operation_id, .. }
            | Self::Disband { operation_id, .. } => Some(operation_id),
        }
    }

    /// The bounded operation label the team-operations counter records.
    #[must_use]
    pub const fn operation_label(&self) -> &'static str {
        match self {
            Self::Describe => "describe",
            Self::Create { .. } => "create",
            Self::AddMember { .. } => "join",
            Self::RemoveMember { .. } => "leave",
            Self::PostTask { .. } => "post",
            Self::Claim { .. } => "claim",
            Self::Release { .. } => "release",
            Self::Transfer { .. } => "transfer",
            Self::AppendMessage { .. } => "message",
            Self::Disband { .. } => "disband",
        }
    }
}

/// The reply of one team entity operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum AgentTeamEntityReply {
    /// The command applied.
    Applied {
        /// The transition's compact outcome.
        outcome: AgentTeamOutcome,
    },
    /// The command already ran; this is its original outcome.
    Duplicate {
        /// The recorded outcome.
        outcome: AgentTeamOutcome,
    },
    /// The bounded projection, `None` while the team is uncreated.
    Snapshot(Option<Box<AgentTeamSnapshot>>),
    /// What a settle pass accomplished.
    Progressed {
        /// The settle pass report.
        progress: AgentTeamProgress,
    },
    /// The command was refused under a stable code.
    Rejected {
        /// Stable machine-readable reason code.
        code: String,
        /// Human-readable detail.
        message: String,
    },
}

impl AgentTeamEntityReply {
    fn rejected(error: &AgentTeamError) -> Self {
        Self::Rejected {
            code: error.code().to_string(),
            message: error.to_string(),
        }
    }
}

/// What one team settle pass accomplished.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTeamProgress {
    /// History entries durably flushed to the sink.
    pub history_flushed: usize,
    /// Whether a passed expiry horizon was durably observed.
    pub expiry_observed: bool,
    /// Exchanges settled by the drive.
    pub settled: usize,
    /// Deliveries that failed and stay outstanding.
    pub failed: usize,
    /// How many of those were refusals no re-drive can settle.
    pub unsettleable: usize,
    /// Exchanges still outstanding after the drive.
    pub outstanding: usize,
}

/// The actor message surface of the team entity.
#[derive(Debug)]
pub enum AgentTeamEntityMessage {
    /// One durable, deduplicated command.
    Command {
        /// The command.
        command: Box<AgentTeamEntityCommand>,
        /// Where the reply goes.
        reply_to: ReplyTo<AgentTeamEntityReply>,
    },
    /// One delivered cross-entity exchange.
    Exchange {
        /// The envelope.
        envelope: Box<AgentExchangeEnvelope>,
        /// Where the reply goes.
        reply_to: ReplyTo<AgentExchangeReply>,
    },
    /// Drives the settle pass: flush, expiry observation, courier.
    Settle {
        /// Where the report goes.
        reply_to: ReplyTo<AgentTeamEntityReply>,
    },
}

/// The durable facade of one team scope.
///
/// Every decision lives here; the actor is a routing and recovery shell over
/// it, so the entity can passivate after any message.
pub struct AgentTeamEntityStore<Store, History>
where
    Store: DurableStateStore<AgentTeamState>,
    History: AgentTeamHistoryStore,
{
    scope: AgentTeamScope,
    host: AgentExchangeHost<AgentTeamParticipant, Store>,
    history: History,
    policy: AgentSchemaPolicy,
    metrics: Arc<dyn MetricsRecorder>,
    recovered: bool,
}

impl<Store, History> Debug for AgentTeamEntityStore<Store, History>
where
    Store: DurableStateStore<AgentTeamState>,
    History: AgentTeamHistoryStore,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentTeamEntityStore")
            .field("scope", &self.scope)
            .field("history", &self.history.backend_name())
            .field("recovered", &self.recovered)
            .finish_non_exhaustive()
    }
}

impl<Store, History> AgentTeamEntityStore<Store, History>
where
    Store: DurableStateStore<AgentTeamState>,
    History: AgentTeamHistoryStore,
{
    /// Creates a durable facade for one team scope.
    #[must_use]
    pub fn new(scope: AgentTeamScope, store: Store, history: History) -> Self {
        let host = AgentExchangeHost::new(
            AgentEntityAddress::Team(scope.clone()),
            AgentTeamParticipant,
            store,
        );
        Self {
            scope,
            host,
            history,
            policy: AgentSchemaPolicy::default(),
            metrics: Arc::new(NoopMetricsRecorder),
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

    /// Wires a metrics recorder for the bounded team-operations counter this
    /// entity emits after its durable transitions commit.
    #[must_use]
    pub fn with_metrics(mut self, metrics: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics = metrics;
        self
    }

    /// The scope this facade addresses.
    #[must_use]
    pub const fn scope(&self) -> &AgentTeamScope {
        &self.scope
    }

    /// The durable persistence id of this team's state.
    #[must_use]
    pub fn persistence_id(&self) -> PersistenceId {
        self.scope.persistence_id()
    }

    /// Loads the team's durable state, failing closed on an unsupported
    /// schema version.
    pub async fn recover(&mut self, now: AgentTimestampMillis) -> AgentTeamResult<&AgentTeamState> {
        let state = self.host.recover(now).await?;
        self.recovered = true;
        Ok(state)
    }

    /// The currently recovered state.
    pub fn state(&self) -> AgentTeamResult<&AgentTeamState> {
        Ok(self.host.state()?)
    }

    /// The bounded projection of the team, once it has been created.
    pub fn snapshot(&self) -> AgentTeamResult<Option<AgentTeamSnapshot>> {
        Ok(self.state()?.snapshot())
    }

    /// Applies one command, then settles what it made possible locally: the
    /// history the transition owes.
    ///
    /// # Errors
    ///
    /// An error does not prove the command was not applied. Retrying with
    /// the same operation id is always safe: a command that committed
    /// answers [`AgentTeamEntityReply::Duplicate`] with its original
    /// outcome rather than transitioning twice.
    pub async fn apply(
        &mut self,
        command: AgentTeamEntityCommand,
        router: &AgentExchangeRouter,
        now: AgentTimestampMillis,
    ) -> AgentTeamResult<AgentTeamEntityReply> {
        self.ensure_recovered(now).await?;

        if let Some(operation_id) = command.operation_id() {
            if let Some(outcome) = self
                .state()?
                .applied_operations
                .outcome(operation_id)
                .cloned()
            {
                // The command already ran. Its original outcome is
                // returned, and no second transition happens — which is
                // what makes a replayed claim converge on one board
                // decision.
                return Ok(AgentTeamEntityReply::Duplicate { outcome });
            }
        }

        if matches!(command, AgentTeamEntityCommand::Describe) {
            return Ok(AgentTeamEntityReply::Snapshot(
                self.snapshot()?.map(Box::new),
            ));
        }
        self.require_history_headroom(now).await?;

        let operation = command.operation_label();
        let reply = self.apply_transition(command, now).await;
        match &reply {
            Ok(_) => self.count_operation(operation, "applied"),
            Err(error) if error.is_domain_refusal() => {
                self.count_operation(operation, "refused");
            }
            Err(_) => {}
        }
        // Owed exchanges are drained by the courier — the settle pass — and
        // never synchronously from a command; the router rides along only so
        // callers hold the same surface everywhere.
        let _ = router;
        // A rejected transition flushed nothing and, after a persistence
        // fence, holds no recovered cache — flushing here would mask the
        // rejection with a recovery error.
        if reply.is_ok() {
            self.flush_history(now).await?;
        }
        reply
    }

    async fn apply_transition(
        &mut self,
        command: AgentTeamEntityCommand,
        now: AgentTimestampMillis,
    ) -> AgentTeamResult<AgentTeamEntityReply> {
        match command {
            AgentTeamEntityCommand::Describe => unreachable!("handled by the caller"),
            AgentTeamEntityCommand::Create {
                operation_id,
                creation,
            } => {
                self.transition(now, move |state| {
                    let touched = create_team(state, &operation_id, *creation, now)?;
                    Ok((operation_id, touched, Vec::new()))
                })
                .await
            }
            AgentTeamEntityCommand::AddMember {
                operation_id,
                member,
                capability_scopes,
                expected_lifecycle_revision,
                provenance,
            } => {
                self.transition(now, move |state| {
                    add_member(
                        state,
                        &operation_id,
                        member,
                        capability_scopes,
                        expected_lifecycle_revision,
                        &provenance,
                        now,
                    )?;
                    Ok((operation_id, None, Vec::new()))
                })
                .await
            }
            AgentTeamEntityCommand::RemoveMember {
                operation_id,
                member,
                expected_lifecycle_revision,
                provenance,
            } => {
                self.transition(now, move |state| {
                    remove_member(
                        state,
                        &operation_id,
                        &member,
                        expected_lifecycle_revision,
                        &provenance,
                        now,
                    )?;
                    Ok((operation_id, None, Vec::new()))
                })
                .await
            }
            AgentTeamEntityCommand::PostTask {
                operation_id,
                task,
                posted_by,
            } => {
                self.transition(now, move |state| {
                    post_task(state, &operation_id, task.clone(), posted_by, now)?;
                    Ok((operation_id, Some(task), Vec::new()))
                })
                .await
            }
            AgentTeamEntityCommand::Claim {
                operation_id,
                task,
                member,
                expected_epoch,
            } => {
                self.transition(now, move |state| {
                    let owed = claim_entry(
                        state,
                        &operation_id,
                        task.clone(),
                        member,
                        expected_epoch,
                        now,
                    )?;
                    Ok((operation_id, Some(task), owed))
                })
                .await
            }
            AgentTeamEntityCommand::Release {
                operation_id,
                task,
                member,
                expected_epoch,
            } => {
                self.transition(now, move |state| {
                    let owed = release_entry(
                        state,
                        &operation_id,
                        task.clone(),
                        &member,
                        expected_epoch,
                        now,
                    )?;
                    Ok((operation_id, Some(task), owed))
                })
                .await
            }
            AgentTeamEntityCommand::Transfer {
                operation_id,
                task,
                member,
                target,
                expected_epoch,
            } => {
                self.transition(now, move |state| {
                    let owed = transfer_entry(
                        state,
                        &operation_id,
                        task.clone(),
                        &member,
                        target,
                        expected_epoch,
                        now,
                    )?;
                    Ok((operation_id, Some(task), owed))
                })
                .await
            }
            AgentTeamEntityCommand::AppendMessage {
                operation_id,
                from,
                to,
                body,
            } => {
                self.transition(now, move |state| {
                    append_message(state, &operation_id, from, to, body, now)?;
                    Ok((operation_id, None, Vec::new()))
                })
                .await
            }
            AgentTeamEntityCommand::Disband {
                operation_id,
                expected_lifecycle_revision,
                provenance,
                reason,
            } => {
                self.transition(now, move |state| {
                    disband_team(
                        state,
                        &operation_id,
                        expected_lifecycle_revision,
                        &provenance,
                        reason,
                        now,
                    )?;
                    Ok((operation_id, None, Vec::new()))
                })
                .await
            }
        }
    }

    /// Accepts one delivered exchange and makes local progress only.
    ///
    /// The courier — a settle pass, a recovery sweep — drains whatever the
    /// acceptance committed; driving it from here would re-enter the
    /// mid-delivery initiator (see [`crate::run`]'s `accept`).
    pub async fn accept(
        &mut self,
        envelope: &AgentExchangeEnvelope,
        router: &AgentExchangeRouter,
        now: AgentTimestampMillis,
    ) -> AgentTeamResult<AgentExchangeReply> {
        self.ensure_recovered(now).await?;
        self.require_history_headroom(now).await?;
        let reply = self.host.accept(envelope, now).await?;
        if envelope.kind() == AgentExchangeKind::TeamClaimResult
            && !reply.is_replayed()
            && reply.result().is_accepted()
        {
            let outcome = envelope
                .payload()
                .decode::<AgentTeamClaimResultNotice>(AGENT_TEAM_CLAIM_RESULT_PAYLOAD_TYPE)
                .ok()
                .map(|notice| match notice.outcome {
                    AgentTeamClaimOutcome::Activated { .. } => "activated",
                    AgentTeamClaimOutcome::Refused { .. } => "reopened",
                });
            if let Some(outcome) = outcome {
                self.count_operation("claim", outcome);
            }
        }
        if envelope.kind() == AgentExchangeKind::TeamTerminalNotice
            && !reply.is_replayed()
            && reply.result().is_accepted()
        {
            // Counted once per fresh application, replays never; an
            // idempotent no-entry acceptance counts too — the operation is
            // the notice's application, not the entry mutation.
            self.count_operation("close", "applied");
        }
        let _ = router;
        self.flush_history(now).await?;
        Ok(reply)
    }

    /// Observes a passed expiry horizon, flushes owed history, and drives
    /// the exchanges the team owes.
    ///
    /// Safe to call at any time and from any node: every step reads what it
    /// needs from durable state.
    pub async fn settle_side_effects(
        &mut self,
        router: &AgentExchangeRouter,
        now: AgentTimestampMillis,
    ) -> AgentTeamResult<AgentTeamProgress> {
        self.ensure_recovered(now).await?;
        self.require_history_headroom(now).await?;
        let expiry_observed = self.observe_expiry(now).await?;
        let flushed = self.flush_history(now).await?;
        let report = drive_pending_exchanges(&mut self.host, router, now).await?;
        // A drive settlement may have recorded history of its own.
        let flushed = flushed + self.flush_history(now).await?;
        record_unsettleable_exchanges(self.metrics.as_ref(), &report.unsettleable);
        Ok(AgentTeamProgress {
            history_flushed: flushed,
            expiry_observed,
            settled: report.settled,
            failed: report.failed,
            unsettleable: report.unsettleable.len(),
            outstanding: self.host.outstanding()?.len(),
        })
    }

    /// Durably flips an Active team whose expiry horizon has passed.
    ///
    /// The write is skipped entirely while nothing would flip, so a sweep
    /// over a healthy team burns no revision.
    async fn observe_expiry(&mut self, now: AgentTimestampMillis) -> AgentTeamResult<bool> {
        let would_expire = self
            .state()?
            .team()
            .is_some_and(|team| team.status == AgentTeamStatus::Active && team.is_expired_at(now));
        if !would_expire {
            return Ok(false);
        }
        let operation_id = AgentOperationId::new(
            crate::identity::AgentOperationKind::TeamOperation,
            [
                self.scope.tenant().as_str(),
                self.scope.team().as_str(),
                "expiry",
            ],
        )?;
        self.host
            .initiate(now, |state| {
                let Some(team) = state.team.as_mut() else {
                    return Ok(Vec::new());
                };
                if team.status != AgentTeamStatus::Active || !team.is_expired_at(now) {
                    return Ok(Vec::new());
                }
                team.status = AgentTeamStatus::Expired;
                state.record_history(|sequence| {
                    AgentTeamHistoryEntry::new(
                        sequence,
                        AgentTeamHistoryKind::Expired,
                        operation_id,
                        now,
                    )
                    .with_detail("expired")
                });
                state.updated_at = now;
                Ok(Vec::new())
            })
            .await?;
        self.count_operation("expire", "applied");
        Ok(true)
    }

    async fn require_history_headroom(&mut self, now: AgentTimestampMillis) -> AgentTeamResult<()> {
        if self.state()?.history_headroom() >= AGENT_TEAM_MAX_HISTORY_PER_TRANSITION {
            return Ok(());
        }
        self.flush_history(now).await?;
        let state = self.state()?;
        if state.history_headroom() >= AGENT_TEAM_MAX_HISTORY_PER_TRANSITION {
            return Ok(());
        }
        Err(AgentTeamError::HistoryBacklog {
            pending: state.pending_history().len(),
            maximum: AGENT_TEAM_PENDING_HISTORY_CAPACITY,
        })
    }

    async fn flush_history(&mut self, now: AgentTimestampMillis) -> AgentTeamResult<usize> {
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

    /// Runs one bounded command transition and records its resolved
    /// operation id in the same compare-and-set.
    ///
    /// A rejected transition never reaches the store, so it leaves no trace
    /// in the operation log and a corrected retry under the same operation
    /// id is still accepted.
    async fn transition<F>(
        &mut self,
        now: AgentTimestampMillis,
        transition: F,
    ) -> AgentTeamResult<AgentTeamEntityReply>
    where
        F: FnOnce(
            &mut AgentTeamState,
        ) -> AgentTeamResult<(
            AgentOperationId,
            Option<AgentTaskId>,
            Vec<AgentExchangeEnvelope>,
        )>,
    {
        let mut outcome = None;
        let mut rejection = None;
        let committed = self
            .host
            .initiate(now, |state| {
                let step =
                    |state: &mut AgentTeamState| -> AgentTeamResult<Vec<AgentExchangeEnvelope>> {
                        let (operation_id, touched, owed) = transition(state)?;
                        let result = state.outcome_for(touched.as_ref());
                        state
                            .applied_operations
                            .record(operation_id, result.clone());
                        state.updated_at = now;
                        outcome = Some(result);
                        Ok(owed)
                    };
                match step(state) {
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
        let outcome = outcome.expect("a committed transition recorded its outcome");
        Ok(AgentTeamEntityReply::Applied { outcome })
    }

    async fn ensure_recovered(&mut self, now: AgentTimestampMillis) -> AgentTeamResult<()> {
        if !self.recovered {
            self.recover(now).await?;
        }
        Ok(())
    }

    fn count_operation(&self, operation: &str, outcome: &str) {
        let _ = record_agent_domain_counter(
            self.metrics.as_ref(),
            METRIC_AGENT_TEAM_OPERATIONS,
            1,
            &[("operation", operation), ("outcome", outcome)],
        );
    }
}

fn create_team(
    state: &mut AgentTeamState,
    operation_id: &AgentOperationId,
    creation: AgentTeamCreation,
    now: AgentTimestampMillis,
) -> AgentTeamResult<Option<AgentTaskId>> {
    if state.team.is_some() {
        return Err(AgentTeamError::AlreadyCreated {
            scope: state.scope.clone(),
        });
    }
    let mut members: BTreeMap<AgentId, AgentTeamMember> = creation
        .members
        .into_iter()
        .map(|(agent, capability_scopes)| {
            (
                agent,
                AgentTeamMember {
                    capability_scopes,
                    joined_at: now,
                    revision: AgentRevisionNumber::INITIAL,
                },
            )
        })
        .collect();
    members
        .entry(creation.leader.clone())
        .or_insert_with(|| AgentTeamMember {
            capability_scopes: BTreeSet::new(),
            joined_at: now,
            revision: AgentRevisionNumber::INITIAL,
        });
    let maximum = creation.policy.effective_max_members() as usize;
    if members.len() > maximum {
        return Err(AgentTeamError::MembersExhausted { maximum });
    }
    let expires_at = creation
        .policy
        .expires_after_ms
        .map(|ms| AgentTimestampMillis::new(now.as_millis().saturating_add(ms)));
    let member_count = members.len();
    let team = AgentTeam {
        status: AgentTeamStatus::Active,
        leader: creation.leader.clone(),
        root_goal: creation.root_goal,
        policy: creation.policy,
        lifecycle_revision: AgentRevisionNumber::INITIAL,
        members,
        board: BTreeMap::new(),
        messages: VecDeque::new(),
        messages_dropped: 0,
        next_message_sequence: 1,
        created_at: now,
        expires_at,
    };
    team.check_bounds()?;
    state.team = Some(team);
    let leader = creation.leader;
    let operation = operation_id.clone();
    state.record_history(|sequence| {
        AgentTeamHistoryEntry::new(sequence, AgentTeamHistoryKind::Created, operation, now)
            .with_member(leader)
            .with_detail(format!("members={member_count}"))
    });
    Ok(None)
}

fn add_member(
    state: &mut AgentTeamState,
    operation_id: &AgentOperationId,
    member: AgentId,
    capability_scopes: BTreeSet<AgentCapabilityId>,
    expected_lifecycle_revision: AgentRevisionNumber,
    provenance: &AgentRevisionProvenance,
    now: AgentTimestampMillis,
) -> AgentTeamResult<()> {
    state.require_active(now)?;
    let team = state.team_mut()?;
    if team.lifecycle_revision != expected_lifecycle_revision {
        return Err(AgentTeamError::StaleLifecycleRevision {
            expected: expected_lifecycle_revision,
            actual: team.lifecycle_revision,
        });
    }
    if team.members.contains_key(&member) {
        return Err(AgentTeamError::AlreadyMember { member });
    }
    let maximum = team.policy.effective_max_members() as usize;
    if team.members.len() >= maximum {
        return Err(AgentTeamError::MembersExhausted { maximum });
    }
    team.lifecycle_revision = team.lifecycle_revision.next();
    let revision = team.lifecycle_revision;
    team.members.insert(
        member.clone(),
        AgentTeamMember {
            capability_scopes,
            joined_at: now,
            revision,
        },
    );
    team.check_bounds()?;
    let operation = operation_id.clone();
    let principal = provenance.principal.principal_id.clone();
    state.record_history(|sequence| {
        AgentTeamHistoryEntry::new(sequence, AgentTeamHistoryKind::MemberJoined, operation, now)
            .with_member(member)
            .with_detail(principal)
    });
    Ok(())
}

fn remove_member(
    state: &mut AgentTeamState,
    operation_id: &AgentOperationId,
    member: &AgentId,
    expected_lifecycle_revision: AgentRevisionNumber,
    provenance: &AgentRevisionProvenance,
    now: AgentTimestampMillis,
) -> AgentTeamResult<()> {
    state.require_active(now)?;
    let team = state.team_mut()?;
    if team.lifecycle_revision != expected_lifecycle_revision {
        return Err(AgentTeamError::StaleLifecycleRevision {
            expected: expected_lifecycle_revision,
            actual: team.lifecycle_revision,
        });
    }
    if !team.members.contains_key(member) {
        return Err(AgentTeamError::NotMember {
            member: member.clone(),
        });
    }
    if member == &team.leader {
        return Err(AgentTeamError::LeaderImmovable);
    }
    // A member holding an unresolved claim cannot leave: the claim's
    // arbitration is in flight and its board settlement must land against a
    // member the team still knows. An *activated* claim is no obstacle —
    // the task's own machinery owns that work now.
    let holds_pending = team.board.values().any(|entry| {
        matches!(
            entry.status,
            AgentTeamBoardEntryStatus::Pending | AgentTeamBoardEntryStatus::Releasing
        ) && entry
            .claim
            .as_ref()
            .is_some_and(|claim| &claim.member == member)
    });
    if holds_pending {
        return Err(AgentTeamError::MemberClaimPending {
            member: member.clone(),
        });
    }
    team.lifecycle_revision = team.lifecycle_revision.next();
    team.members.remove(member);
    let operation = operation_id.clone();
    let member = member.clone();
    let principal = provenance.principal.principal_id.clone();
    state.record_history(|sequence| {
        AgentTeamHistoryEntry::new(sequence, AgentTeamHistoryKind::MemberLeft, operation, now)
            .with_member(member)
            .with_detail(principal)
    });
    Ok(())
}

fn post_task(
    state: &mut AgentTeamState,
    operation_id: &AgentOperationId,
    task: AgentTaskId,
    posted_by: AgentId,
    now: AgentTimestampMillis,
) -> AgentTeamResult<()> {
    state.require_active(now)?;
    let team = state.team_mut()?;
    if !team.members.contains_key(&posted_by) {
        return Err(AgentTeamError::NotMember { member: posted_by });
    }
    if team.board.contains_key(&task) {
        return Err(AgentTeamError::TaskAlreadyPosted { task });
    }
    let maximum = team.policy.effective_max_board_entries() as usize;
    if team.board.len() >= maximum {
        // Done entries are settled facts kept only while space allows: under
        // ceiling pressure they are evicted, lazily like every board expiry,
        // so a long-lived board can never be exhausted by its own finished
        // work. A re-post after eviction simply re-arbitrates at the task,
        // which refuses terminal work and closes the fresh entry again.
        team.board
            .retain(|_, entry| entry.status != AgentTeamBoardEntryStatus::Done);
    }
    if team.board.len() >= maximum {
        return Err(AgentTeamError::BoardExhausted { maximum });
    }
    team.board.insert(
        task.clone(),
        AgentTeamBoardEntry {
            task: task.clone(),
            posted_by: posted_by.clone(),
            posted_at: now,
            claim_epoch: 0,
            status: AgentTeamBoardEntryStatus::Open,
            claim: None,
            last_code: None,
        },
    );
    team.check_bounds()?;
    let operation = operation_id.clone();
    state.record_history(|sequence| {
        AgentTeamHistoryEntry::new(sequence, AgentTeamHistoryKind::TaskPosted, operation, now)
            .with_member(posted_by)
            .with_task(task)
    });
    Ok(())
}

/// Builds the claim exchange one board decision owes its task.
fn owed_claim_exchange(
    state: &AgentTeamState,
    operation_id: AgentOperationId,
    command: &AgentTeamClaimCommand,
    now: AgentTimestampMillis,
) -> AgentTeamResult<AgentExchangeEnvelope> {
    let task_scope = AgentTaskScope::new(state.scope.tenant().clone(), command.task.clone())?;
    let payload = AgentExchangePayload::encode(AGENT_TEAM_CLAIM_PAYLOAD_TYPE, command)?;
    Ok(AgentExchangeEnvelope::new(
        operation_id.clone(),
        AgentExchangeKind::TeamClaim,
        AgentEntityAddress::Team(state.scope.clone()),
        AgentEntityAddress::Task(task_scope),
        payload,
        AgentCorrelationId::new(operation_id.as_str()),
        now,
    )?)
}

/// Mints one superseding claim decision over a board entry: bumps the
/// claim epoch, rewrites the entry to its pending shape toward the
/// claimant, and builds the claim command the task is owed. The caller has
/// already arbitrated who may mint — a fresh claim's steal window, a
/// transfer's holder-or-leader rule — and this shared half is what keeps a
/// transfer's minted claim structurally identical to a fresh one.
fn mint_entry_claim(
    scope: &AgentTeamScope,
    entry: &mut AgentTeamBoardEntry,
    task: &AgentTaskId,
    claimant: &AgentId,
    policy_revision: AgentRevisionNumber,
    lease_ms: u64,
    now: AgentTimestampMillis,
) -> AgentTeamResult<AgentTeamClaimCommand> {
    let epoch = entry.claim_epoch + 1;
    let claim = team_claim_id_for(scope, task, claimant, epoch)?;
    let lease_expires_at = AgentTimestampMillis::new(now.as_millis().saturating_add(lease_ms));
    entry.claim_epoch = epoch;
    entry.status = AgentTeamBoardEntryStatus::Pending;
    entry.claim = Some(AgentTeamBoardClaim {
        claim: claim.clone(),
        member: claimant.clone(),
        lease_expires_at,
        claimed_at: now,
        generation_echo: None,
        run_echo: None,
    });
    entry.last_code = None;
    Ok(AgentTeamClaimCommand {
        team: scope.clone(),
        claim,
        task: task.clone(),
        epoch,
        action: AgentTeamClaimAction::Claim {
            member: claimant.clone(),
        },
        policy_revision,
        lease_expires_at,
    })
}

fn claim_entry(
    state: &mut AgentTeamState,
    operation_id: &AgentOperationId,
    task: AgentTaskId,
    member: AgentId,
    expected_epoch: u64,
    now: AgentTimestampMillis,
) -> AgentTeamResult<Vec<AgentExchangeEnvelope>> {
    state.require_active(now)?;
    let scope = state.scope.clone();
    let team = state.team_mut()?;
    if !team.members.contains_key(&member) {
        return Err(AgentTeamError::NotMember { member });
    }
    let policy_revision = team.policy.revision;
    let lease_ms = team.policy.claim_lease_ms;
    let Some(entry) = team.board.get_mut(&task) else {
        return Err(AgentTeamError::TaskNotPosted { task });
    };
    if entry.claim_epoch != expected_epoch {
        return Err(AgentTeamError::StaleClaimEpoch {
            expected: expected_epoch,
            actual: entry.claim_epoch,
        });
    }
    let stealing = match entry.status {
        AgentTeamBoardEntryStatus::Open => false,
        AgentTeamBoardEntryStatus::Pending => {
            let expired = entry
                .claim
                .as_ref()
                .is_some_and(|claim| now.as_millis() >= claim.lease_expires_at.as_millis());
            if !expired {
                return Err(AgentTeamError::ClaimNotStealable);
            }
            true
        }
        AgentTeamBoardEntryStatus::Releasing => {
            return Err(AgentTeamError::EntryBusy {
                status: entry.status,
            })
        }
        AgentTeamBoardEntryStatus::Active => return Err(AgentTeamError::EntryOwned),
        AgentTeamBoardEntryStatus::Done => return Err(AgentTeamError::EntryDone),
    };

    let command = mint_entry_claim(
        &scope,
        entry,
        &task,
        &member,
        policy_revision,
        lease_ms,
        now,
    )?;
    let claim = command.claim.clone();
    let exchange_operation = team_claim_operation_id(state.scope.tenant(), &claim)?;
    let envelope = owed_claim_exchange(state, exchange_operation, &command, now)?;
    let operation = operation_id.clone();
    state.record_history(|sequence| {
        AgentTeamHistoryEntry::new(
            sequence,
            AgentTeamHistoryKind::ClaimRecorded,
            operation,
            now,
        )
        .with_member(member)
        .with_task(task)
        .with_claim(claim)
        .with_detail(if stealing { "steal" } else { "claim" })
    });
    Ok(vec![envelope])
}

fn release_entry(
    state: &mut AgentTeamState,
    operation_id: &AgentOperationId,
    task: AgentTaskId,
    member: &AgentId,
    expected_epoch: u64,
    now: AgentTimestampMillis,
) -> AgentTeamResult<Vec<AgentExchangeEnvelope>> {
    state.require_active(now)?;
    let scope = state.scope.clone();
    let team = state.team_mut()?;
    let leader = team.leader.clone();
    let policy_revision = team.policy.revision;
    let Some(entry) = team.board.get_mut(&task) else {
        return Err(AgentTeamError::TaskNotPosted { task });
    };
    if entry.claim_epoch != expected_epoch {
        return Err(AgentTeamError::StaleClaimEpoch {
            expected: expected_epoch,
            actual: entry.claim_epoch,
        });
    }
    if entry.status == AgentTeamBoardEntryStatus::Active {
        // An accepted assignment leaves the board only through task-side
        // outcomes; a bare board release would contradict the assignment
        // fence.
        return Err(AgentTeamError::EntryOwned);
    }
    if entry.status != AgentTeamBoardEntryStatus::Pending {
        return Err(AgentTeamError::EntryBusy {
            status: entry.status,
        });
    }
    let Some(claim) = entry.claim.clone() else {
        return Err(AgentTeamError::EntryBusy {
            status: entry.status,
        });
    };
    if &claim.member != member && member != &leader {
        return Err(AgentTeamError::NotClaimHolder {
            member: member.clone(),
        });
    }

    let epoch = entry.claim_epoch + 1;
    entry.claim_epoch = epoch;
    entry.status = AgentTeamBoardEntryStatus::Releasing;

    let command = AgentTeamClaimCommand {
        team: scope,
        claim: claim.claim.clone(),
        task: task.clone(),
        epoch,
        action: AgentTeamClaimAction::Release,
        policy_revision,
        lease_expires_at: claim.lease_expires_at,
    };
    let exchange_operation =
        team_claim_release_operation_id(state.scope.tenant(), &claim.claim, epoch)?;
    let envelope = owed_claim_exchange(state, exchange_operation, &command, now)?;
    let operation = operation_id.clone();
    let requester = member.clone();
    state.record_history(|sequence| {
        AgentTeamHistoryEntry::new(
            sequence,
            AgentTeamHistoryKind::ClaimReleaseRequested,
            operation,
            now,
        )
        .with_member(requester)
        .with_task(task)
        .with_claim(claim.claim)
    });
    Ok(vec![envelope])
}

fn transfer_entry(
    state: &mut AgentTeamState,
    operation_id: &AgentOperationId,
    task: AgentTaskId,
    member: &AgentId,
    target: AgentId,
    expected_epoch: u64,
    now: AgentTimestampMillis,
) -> AgentTeamResult<Vec<AgentExchangeEnvelope>> {
    state.require_active(now)?;
    let scope = state.scope.clone();
    let team = state.team_mut()?;
    let leader = team.leader.clone();
    let policy_revision = team.policy.revision;
    let lease_ms = team.policy.claim_lease_ms;
    if !team.members.contains_key(&target) {
        return Err(AgentTeamError::NotMember { member: target });
    }
    let Some(entry) = team.board.get_mut(&task) else {
        return Err(AgentTeamError::TaskNotPosted { task });
    };
    if entry.claim_epoch != expected_epoch {
        return Err(AgentTeamError::StaleClaimEpoch {
            expected: expected_epoch,
            actual: entry.claim_epoch,
        });
    }
    if entry.status == AgentTeamBoardEntryStatus::Active {
        // Post-acceptance transfer is the handoff machinery's job
        // ([specification 8.9](../../docs/plans/rakka-agent/spec.md)), out
        // of board scope by design.
        return Err(AgentTeamError::EntryOwned);
    }
    if entry.status != AgentTeamBoardEntryStatus::Pending {
        return Err(AgentTeamError::EntryBusy {
            status: entry.status,
        });
    }
    let Some(prior) = entry.claim.clone() else {
        return Err(AgentTeamError::EntryBusy {
            status: entry.status,
        });
    };
    if &prior.member != member && member != &leader {
        return Err(AgentTeamError::NotClaimHolder {
            member: member.clone(),
        });
    }

    // At the task a transfer IS a superseding claim: the arbitration that
    // supersedes the prior claimant pre-mint and refuses in-flight or
    // accepted work is reused whole.
    let command = mint_entry_claim(
        &scope,
        entry,
        &task,
        &target,
        policy_revision,
        lease_ms,
        now,
    )?;
    let claim = command.claim.clone();
    let exchange_operation = team_claim_operation_id(state.scope.tenant(), &claim)?;
    let envelope = owed_claim_exchange(state, exchange_operation, &command, now)?;
    let operation = operation_id.clone();
    state.record_history(|sequence| {
        AgentTeamHistoryEntry::new(
            sequence,
            AgentTeamHistoryKind::TransferRecorded,
            operation,
            now,
        )
        .with_member(target)
        .with_task(task)
        .with_claim(claim)
        .with_detail(format!("from={}", prior.member))
    });
    Ok(vec![envelope])
}

fn append_message(
    state: &mut AgentTeamState,
    operation_id: &AgentOperationId,
    from: AgentId,
    to: Option<AgentId>,
    body: String,
    now: AgentTimestampMillis,
) -> AgentTeamResult<()> {
    state.require_active(now)?;
    let team = state.team_mut()?;
    if !team.members.contains_key(&from) {
        return Err(AgentTeamError::NotMember { member: from });
    }
    if let Some(to) = &to {
        if !team.members.contains_key(to) {
            return Err(AgentTeamError::NotMember { member: to.clone() });
        }
    }
    let maximum = team.policy.effective_max_message_bytes();
    if body.len() > maximum {
        return Err(AgentTeamError::MessageTooLarge {
            bytes: body.len(),
            maximum,
        });
    }
    let sequence = team.next_message_sequence;
    team.next_message_sequence += 1;
    team.messages.push_back(AgentTeamMessage {
        sequence,
        from: from.clone(),
        to: to.clone(),
        body,
        at: now,
    });
    let ring = team.policy.effective_max_messages() as usize;
    while team.messages.len() > ring {
        team.messages.pop_front();
        team.messages_dropped += 1;
    }
    team.check_bounds()?;
    let operation = operation_id.clone();
    state.record_history(|sequence_number| {
        let mut entry = AgentTeamHistoryEntry::new(
            sequence_number,
            AgentTeamHistoryKind::MessageAppended,
            operation,
            now,
        )
        .with_member(from)
        .with_detail(format!("sequence={sequence}"));
        entry.task = None;
        // The addressed member rides the bounded detail-free field set; the
        // body never joins history ([specification 17.13](../../docs/plans/rakka-agent/spec.md)).
        if let Some(to) = to {
            entry.detail = bounded_detail(format!("sequence={sequence} to={to}"));
        }
        entry
    });
    Ok(())
}

fn disband_team(
    state: &mut AgentTeamState,
    operation_id: &AgentOperationId,
    expected_lifecycle_revision: AgentRevisionNumber,
    provenance: &AgentRevisionProvenance,
    reason: String,
    now: AgentTimestampMillis,
) -> AgentTeamResult<()> {
    state.require_active(now)?;
    let team = state.team_mut()?;
    if team.lifecycle_revision != expected_lifecycle_revision {
        return Err(AgentTeamError::StaleLifecycleRevision {
            expected: expected_lifecycle_revision,
            actual: team.lifecycle_revision,
        });
    }
    if let Some(entry) = team.board.values().find(|entry| {
        matches!(
            entry.status,
            AgentTeamBoardEntryStatus::Pending | AgentTeamBoardEntryStatus::Releasing
        )
    }) {
        // Disband must not strand an in-flight arbitration; an Active claim
        // is no obstacle — the board is data, and disband cancels nothing.
        return Err(AgentTeamError::DisbandClaimPending {
            task: entry.task.clone(),
        });
    }
    team.lifecycle_revision = team.lifecycle_revision.next();
    team.status = AgentTeamStatus::Disbanded;
    let operation = operation_id.clone();
    let principal = provenance.principal.principal_id.clone();
    state.record_history(|sequence| {
        AgentTeamHistoryEntry::new(sequence, AgentTeamHistoryKind::Disbanded, operation, now)
            .with_detail(if reason.is_empty() { principal } else { reason })
    });
    Ok(())
}

/// The sharded team entity actor: a routing and recovery shell over
/// [`AgentTeamEntityStore`].
pub struct AgentTeamEntity<Store, History>
where
    Store: DurableStateStore<AgentTeamState>,
    History: AgentTeamHistoryStore,
{
    entity: Result<AgentTeamEntityStore<Store, History>, AgentIdentityError>,
    router: AgentExchangeRouter,
    clock: AgentTeamClock,
}

impl<Store, History> AgentTeamEntity<Store, History>
where
    Store: DurableStateStore<AgentTeamState>,
    History: AgentTeamHistoryStore,
{
    /// Creates an entity for one sharded entity id.
    #[must_use]
    pub fn new(
        entity_id: &EntityId,
        store: Store,
        history: History,
        router: AgentExchangeRouter,
        clock: AgentTeamClock,
        policy: AgentSchemaPolicy,
    ) -> Self {
        let entity = AgentTeamScope::from_entity_id(entity_id).map(|scope| {
            AgentTeamEntityStore::new(scope, store, history).with_schema_policy(policy)
        });
        Self {
            entity,
            router,
            clock,
        }
    }

    /// Wires a metrics recorder for the hosted entity's bounded
    /// team-operations counter.
    #[must_use]
    pub fn with_metrics(mut self, metrics: Arc<dyn MetricsRecorder>) -> Self {
        self.entity = self.entity.map(|store| store.with_metrics(metrics));
        self
    }

    fn store(&mut self) -> Result<&mut AgentTeamEntityStore<Store, History>, AgentTeamError> {
        self.entity
            .as_mut()
            .map_err(|error| AgentTeamError::Identity(error.clone()))
    }
}

impl<Store, History> Actor for AgentTeamEntity<Store, History>
where
    Store: DurableStateStore<AgentTeamState>,
    History: AgentTeamHistoryStore,
{
    type Msg = AgentTeamEntityMessage;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        actor_future(async move {
            // A transition is stamped where it commits, on the owner that
            // wrote it.
            let now = (self.clock)();
            let router = self.router.clone();

            match msg {
                AgentTeamEntityMessage::Command { command, reply_to } => {
                    let reply = match self.store() {
                        Err(error) => AgentTeamEntityReply::rejected(&error),
                        Ok(entity) => match entity.apply(*command, &router, now).await {
                            Ok(reply) => reply,
                            Err(error) => AgentTeamEntityReply::rejected(&error),
                        },
                    };
                    let _reply_dropped = reply_to.reply(reply);
                }
                AgentTeamEntityMessage::Exchange { envelope, reply_to } => {
                    let Ok(entity) = self.store() else {
                        // A misrouted entity cannot answer an exchange.
                        // Dropping the reply leaves the exchange outstanding
                        // on its initiator, which re-drives it.
                        return Ok(ActorAction::Continue);
                    };
                    if let Ok(reply) = entity.accept(&envelope, &router, now).await {
                        let _reply_dropped = reply_to.reply(reply);
                    }
                }
                AgentTeamEntityMessage::Settle { reply_to } => {
                    let reply = match self.store() {
                        Err(error) => AgentTeamEntityReply::rejected(&error),
                        Ok(entity) => match entity.settle_side_effects(&router, now).await {
                            Ok(progress) => AgentTeamEntityReply::Progressed { progress },
                            Err(error) => AgentTeamEntityReply::rejected(&error),
                        },
                    };
                    let _reply_dropped = reply_to.reply(reply);
                }
            }
            Ok(ActorAction::Continue)
        })
    }
}

/// The entity type key of the team entity.
pub type AgentTeamEntityTypeKey = EntityTypeKey<AgentTeamEntityMessage>;

/// The registration returned after initializing sharded team entities.
pub type AgentTeamEntityRegistration = EntityTypeRegistration<AgentTeamEntityMessage>;

/// A sharded reference to one team entity.
pub type AgentTeamEntityRef = ShardedEntityRef<AgentTeamEntityMessage>;

/// The sharding settings of team entities.
#[derive(Clone)]
pub struct AgentTeamEntityShardingSettings {
    key: AgentTeamEntityTypeKey,
    actor_options: ActorOptions,
    idle_passivation_timeout: Option<Duration>,
    buffer_config: Option<ShardBufferConfig>,
    passivation_buffer_duration: Duration,
    schema_policy: AgentSchemaPolicy,
    clock: AgentTeamClock,
    metrics: Arc<dyn MetricsRecorder>,
}

impl Debug for AgentTeamEntityShardingSettings {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentTeamEntityShardingSettings")
            .field("entity_type", self.key.entity_type())
            .field("number_of_shards", &self.key.config().number_of_shards())
            .field("idle_passivation_timeout", &self.idle_passivation_timeout)
            .field("schema_policy", &self.schema_policy)
            .finish_non_exhaustive()
    }
}

impl AgentTeamEntityShardingSettings {
    /// Creates settings from an explicit entity type key.
    #[must_use]
    pub fn new(key: AgentTeamEntityTypeKey) -> Self {
        Self {
            key,
            actor_options: ActorOptions::default(),
            idle_passivation_timeout: None,
            buffer_config: Some(ShardBufferConfig::default()),
            passivation_buffer_duration: DEFAULT_AGENT_TEAM_PASSIVATION_BUFFER_DURATION,
            schema_policy: AgentSchemaPolicy::default(),
            clock: system_team_clock(),
            metrics: Arc::new(NoopMetricsRecorder),
        }
    }

    /// The entity type key used for team entities.
    #[must_use]
    pub const fn key(&self) -> &AgentTeamEntityTypeKey {
        &self.key
    }

    /// Wires a metrics recorder for every hosted entity's bounded
    /// team-operations counter.
    #[must_use]
    pub fn with_metrics(mut self, metrics: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics = metrics;
        self
    }

    /// Uses an explicit clock for the timestamps hosted entities persist.
    #[must_use]
    pub fn with_clock(mut self, clock: AgentTeamClock) -> Self {
        self.clock = clock;
        self
    }

    /// Sets the options used when each team entity actor is spawned.
    #[must_use]
    pub fn with_actor_options(mut self, actor_options: ActorOptions) -> Self {
        self.actor_options = actor_options;
        self
    }

    /// Enables idle passivation for quiescent team entities.
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

impl Default for AgentTeamEntityShardingSettings {
    fn default() -> Self {
        Self::new(agent_team_entity_type_key())
    }
}

/// Creates the default sharded entity type key for team entities.
#[must_use]
pub fn agent_team_entity_type_key() -> AgentTeamEntityTypeKey {
    EntityTypeKey::new(DEFAULT_AGENT_TEAM_ENTITY_TYPE)
}

/// Maps a team scope to its sharded entity id.
#[must_use]
pub fn agent_team_entity_id(scope: &AgentTeamScope) -> EntityId {
    scope.entity_id()
}

/// The durable persistence id of one team entity's state.
#[must_use]
pub fn agent_team_entity_persistence_id(scope: &AgentTeamScope) -> PersistenceId {
    scope.persistence_id()
}

/// Initializes node-local sharded team entities.
pub fn init_agent_team_entity_sharding<Store, History>(
    sharding: &ClusterSharding,
    store: Store,
    history: History,
    router: AgentExchangeRouter,
    settings: AgentTeamEntityShardingSettings,
) -> ClusterShardingResult<AgentTeamEntityRegistration>
where
    Store: DurableStateStore<AgentTeamState>,
    History: AgentTeamHistoryStore,
{
    sharding.init(agent_team_entity(store, history, router, &settings))
}

/// Initializes sharded team entities that a non-owning node can reach over
/// `rakka-remote`.
///
/// The remote ask surface is the [`AgentExchangeEnvelope`], exactly as for
/// the task and run entities; the application registers the exchange codecs
/// through [`crate::choreography::register_agent_exchange_codecs`].
pub fn init_agent_team_entity_remote_sharding<Store, History>(
    sharding: &ClusterSharding,
    runtime: &mut ClusterNodeRuntime,
    store: Store,
    history: History,
    router: AgentExchangeRouter,
    settings: AgentTeamEntityShardingSettings,
) -> ClusterNodeRuntimeResult<AgentTeamEntityRegistration>
where
    Store: DurableStateStore<AgentTeamState>,
    History: AgentTeamHistoryStore,
{
    let entity = agent_team_entity(store, history, router, &settings);
    sharding.init_remote_with_ask(
        runtime,
        entity,
        |envelope: AgentExchangeEnvelope, reply_to: ReplyTo<AgentExchangeReply>| {
            AgentTeamEntityMessage::Exchange {
                envelope: Box::new(envelope),
                reply_to,
            }
        },
    )
}

// The team entity is generic over its two stores, so the entity type it
// builds is unavoidably wide.
#[allow(clippy::type_complexity)]
fn agent_team_entity<Store, History>(
    store: Store,
    history: History,
    router: AgentExchangeRouter,
    settings: &AgentTeamEntityShardingSettings,
) -> Entity<
    AgentTeamEntityMessage,
    AgentTeamEntity<Store, History>,
    impl Fn(EntityContext<AgentTeamEntityMessage>) -> AgentTeamEntity<Store, History>
        + Send
        + Sync
        + 'static,
>
where
    Store: DurableStateStore<AgentTeamState>,
    History: AgentTeamHistoryStore,
{
    let schema_policy = settings.schema_policy;
    let clock = settings.clock.clone();
    let metrics = settings.metrics.clone();
    let mut entity = Entity::of(settings.key.clone(), move |context: EntityContext<_>| {
        AgentTeamEntity::new(
            context.entity_id(),
            store.clone(),
            history.clone(),
            router.clone(),
            clock.clone(),
            schema_policy,
        )
        .with_metrics(metrics.clone())
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

/// Returns a sharded reference to one team entity.
pub fn agent_team_entity_ref(
    sharding: &ClusterSharding,
    key: &AgentTeamEntityTypeKey,
    scope: &AgentTeamScope,
) -> ClusterShardingResult<AgentTeamEntityRef> {
    sharding.entity_ref_for(key, scope.key())
}

/// Returns a sharded reference to one team entity from a registration.
#[must_use]
pub fn registered_agent_team_entity_ref(
    registration: &AgentTeamEntityRegistration,
    scope: &AgentTeamScope,
) -> AgentTeamEntityRef {
    registration.entity_ref_for(scope.key())
}

/// Explicitly passivates one local team entity.
pub fn passivate_agent_team_entity(
    sharding: &ClusterSharding,
    key: &AgentTeamEntityTypeKey,
    scope: &AgentTeamScope,
) -> ClusterShardingResult<bool> {
    sharding.passivate_entity_id(key, &scope.entity_id())
}

/// The rejection of a team entity operation.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum AgentTeamError {
    /// An identifier or scope key was malformed.
    Identity(AgentIdentityError),
    /// A persisted record carried an unsupported schema version.
    Schema(AgentSchemaError),
    /// The choreography substrate rejected an exchange.
    Choreography(Box<AgentChoreographyError>),
    /// A coordination derivation failed.
    Coordination(Box<AgentCoordinationError>),
    /// No team exists under this scope.
    NotCreated {
        /// The addressed scope.
        scope: AgentTeamScope,
    },
    /// A team already exists under this scope.
    AlreadyCreated {
        /// The addressed scope.
        scope: AgentTeamScope,
    },
    /// The team was disbanded.
    Disbanded,
    /// The team's expiry horizon has passed.
    Expired,
    /// A lifecycle command expected a revision the team has moved past.
    StaleLifecycleRevision {
        /// The revision the command expected.
        expected: AgentRevisionNumber,
        /// The revision in force.
        actual: AgentRevisionNumber,
    },
    /// The named agent is not a member.
    NotMember {
        /// The agent.
        member: AgentId,
    },
    /// The named agent is already a member.
    AlreadyMember {
        /// The agent.
        member: AgentId,
    },
    /// The leader cannot be removed.
    LeaderImmovable,
    /// The member holds an unresolved claim and cannot leave.
    MemberClaimPending {
        /// The member.
        member: AgentId,
    },
    /// The membership ceiling is reached.
    MembersExhausted {
        /// The effective ceiling.
        maximum: usize,
    },
    /// The board ceiling is reached.
    BoardExhausted {
        /// The effective ceiling.
        maximum: usize,
    },
    /// The task is already on the board.
    TaskAlreadyPosted {
        /// The task.
        task: AgentTaskId,
    },
    /// The task is not on the board.
    TaskNotPosted {
        /// The task.
        task: AgentTaskId,
    },
    /// The command observed a claim epoch the entry has moved past.
    StaleClaimEpoch {
        /// The epoch the command expected.
        expected: u64,
        /// The epoch in force.
        actual: u64,
    },
    /// The entry is mid-decision and cannot take this command.
    EntryBusy {
        /// The entry's status.
        status: AgentTeamBoardEntryStatus,
    },
    /// The entry's claim activated; ownership left the board.
    EntryOwned,
    /// The entry is closed.
    EntryDone,
    /// The recorded claim's lease has not lapsed.
    ClaimNotStealable,
    /// The requester neither holds the claim nor leads the team.
    NotClaimHolder {
        /// The requester.
        member: AgentId,
    },
    /// The message body exceeds the bounded ceiling.
    MessageTooLarge {
        /// The body size.
        bytes: usize,
        /// The effective ceiling.
        maximum: usize,
    },
    /// A claim arbitration is in flight; disband would strand it.
    DisbandClaimPending {
        /// The entry whose claim is unresolved.
        task: AgentTaskId,
    },
    /// A history append found a different entry at the sequence.
    HistoryConflict {
        /// The conflicting sequence.
        sequence: AgentTeamHistorySequence,
    },
    /// The read cursor precedes the history the backend still retains, so
    /// resuming from it would silently skip entries
    /// ([specification 17.13](../../docs/plans/rakka-agent/spec.md)). The reader
    /// resynchronizes from authoritative state and resumes at the floor.
    HistoryWindowExpired {
        /// The floor to resume from: the oldest sequence still retained at or
        /// past the reader's position, when anything is retained there.
        oldest_retained: Option<AgentTeamHistorySequence>,
    },
    /// The history outbox cannot hold what the next transition may record.
    HistoryBacklog {
        /// Entries pending flush.
        pending: usize,
        /// The outbox capacity.
        maximum: usize,
    },
    /// The materialized state would exceed its bound.
    StateBounds {
        /// The serialized size.
        bytes: usize,
        /// The effective maximum.
        maximum: usize,
    },
}

impl AgentTeamError {
    /// Stable machine-readable code.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::Identity(_) => "team-identity",
            Self::Schema(error) => error.code(),
            Self::Choreography(error) => error.code(),
            Self::Coordination(error) => error.code(),
            Self::NotCreated { .. } => "team-not-found",
            Self::AlreadyCreated { .. } => "team-already-created",
            Self::Disbanded => "team-disbanded",
            Self::Expired => "team-expired",
            Self::StaleLifecycleRevision { .. } => "team-stale-lifecycle-revision",
            Self::NotMember { .. } => "team-not-member",
            Self::AlreadyMember { .. } => "team-already-member",
            Self::LeaderImmovable => "team-leader-immovable",
            Self::MemberClaimPending { .. } => "team-member-claim-pending",
            Self::MembersExhausted { .. } => "team-members-exhausted",
            Self::BoardExhausted { .. } => "team-board-exhausted",
            Self::TaskAlreadyPosted { .. } => "team-task-already-posted",
            Self::TaskNotPosted { .. } => "team-task-not-posted",
            Self::StaleClaimEpoch { .. } => "team-claim-stale-epoch",
            Self::EntryBusy { .. } => "team-board-entry-busy",
            Self::EntryOwned => "team-claim-already-owned",
            Self::EntryDone => "team-board-entry-done",
            Self::ClaimNotStealable => "team-claim-not-stealable",
            Self::NotClaimHolder { .. } => "team-release-not-holder",
            Self::MessageTooLarge { .. } => "team-message-too-large",
            Self::DisbandClaimPending { .. } => "team-disband-claim-pending",
            Self::HistoryConflict { .. } => "team-history-conflict",
            Self::HistoryWindowExpired { .. } => "team-history-window-expired",
            Self::HistoryBacklog { .. } => "team-history-backlog",
            Self::StateBounds { .. } => "team-state-too-large",
        }
    }

    /// Whether this rejection is a domain refusal — a durable decision the
    /// caller rebases on (and a bounded metric records) — rather than a
    /// transport, schema, identity, or read-path fault.
    ///
    /// The list is exclusionary, so a variant added without a thought here
    /// becomes a "refusal" by default: it would answer a caller as a rejected
    /// *command* and count against the entity's refusal metric. Only decisions a
    /// command reached belong on the true side —
    /// [`Self::HistoryWindowExpired`] is a read answer, never a decision.
    #[must_use]
    pub const fn is_domain_refusal(&self) -> bool {
        !matches!(
            self,
            Self::Identity(_)
                | Self::Schema(_)
                | Self::Choreography(_)
                | Self::Coordination(_)
                | Self::HistoryWindowExpired { .. }
        )
    }
}

impl Display for AgentTeamError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Identity(error) => Display::fmt(error, f),
            Self::Schema(error) => Display::fmt(error, f),
            Self::Choreography(error) => Display::fmt(error, f),
            Self::Coordination(error) => Display::fmt(error, f),
            Self::NotCreated { scope } => write!(f, "no team exists under scope {scope}"),
            Self::AlreadyCreated { scope } => {
                write!(f, "a team already exists under scope {scope}")
            }
            Self::Disbanded => f.write_str("the team was disbanded"),
            Self::Expired => f.write_str("the team's expiry horizon has passed"),
            Self::StaleLifecycleRevision { expected, actual } => write!(
                f,
                "the command expected lifecycle revision {expected} but {actual} is in force"
            ),
            Self::NotMember { member } => write!(f, "{member} is not a member of this team"),
            Self::AlreadyMember { member } => {
                write!(f, "{member} is already a member of this team")
            }
            Self::LeaderImmovable => f.write_str("the team leader cannot be removed"),
            Self::MemberClaimPending { member } => write!(
                f,
                "{member} holds an unresolved board claim and cannot leave"
            ),
            Self::MembersExhausted { maximum } => {
                write!(f, "the team already has its maximum of {maximum} members")
            }
            Self::BoardExhausted { maximum } => {
                write!(
                    f,
                    "the board already holds its maximum of {maximum} entries"
                )
            }
            Self::TaskAlreadyPosted { task } => {
                write!(f, "task {task} is already on the board")
            }
            Self::TaskNotPosted { task } => write!(f, "task {task} is not on the board"),
            Self::StaleClaimEpoch { expected, actual } => write!(
                f,
                "the command expected claim epoch {expected} but {actual} is in force"
            ),
            Self::EntryBusy { status } => {
                write!(
                    f,
                    "the board entry is {status} and cannot take this command"
                )
            }
            Self::EntryOwned => {
                f.write_str("the entry's claim activated; ownership left the board")
            }
            Self::EntryDone => f.write_str("the board entry is closed"),
            Self::ClaimNotStealable => f.write_str("the recorded claim's lease has not lapsed"),
            Self::NotClaimHolder { member } => {
                write!(f, "{member} neither holds the claim nor leads the team")
            }
            Self::MessageTooLarge { bytes, maximum } => write!(
                f,
                "the message body is {bytes} bytes; the ceiling is {maximum}"
            ),
            Self::DisbandClaimPending { task } => write!(
                f,
                "task {task} has an unresolved claim; disband would strand it"
            ),
            Self::HistoryConflict { sequence } => write!(
                f,
                "a different history entry already occupies sequence {sequence}"
            ),
            Self::HistoryWindowExpired { oldest_retained } => match oldest_retained {
                Some(oldest) => write!(
                    f,
                    "the history cursor precedes the retained window, which starts at sequence {oldest}; resynchronize from authoritative state"
                ),
                None => f.write_str(
                    "the history cursor precedes the retained window, which holds nothing; resynchronize from authoritative state",
                ),
            },
            Self::HistoryBacklog { pending, maximum } => write!(
                f,
                "the history outbox holds {pending} of {maximum} entries and cannot accept more"
            ),
            Self::StateBounds { bytes, maximum } => write!(
                f,
                "the materialized team is {bytes} bytes; the bound is {maximum}"
            ),
        }
    }
}

impl Error for AgentTeamError {}

impl From<AgentIdentityError> for AgentTeamError {
    fn from(error: AgentIdentityError) -> Self {
        Self::Identity(error)
    }
}

impl From<AgentSchemaError> for AgentTeamError {
    fn from(error: AgentSchemaError) -> Self {
        Self::Schema(error)
    }
}

impl From<AgentChoreographyError> for AgentTeamError {
    fn from(error: AgentChoreographyError) -> Self {
        Self::Choreography(Box::new(error))
    }
}

impl From<AgentCoordinationError> for AgentTeamError {
    fn from(error: AgentCoordinationError) -> Self {
        Self::Coordination(Box::new(error))
    }
}

impl From<AgentTeamError> for AgentChoreographyError {
    fn from(error: AgentTeamError) -> Self {
        match error {
            AgentTeamError::Identity(error) => Self::Identity(error),
            AgentTeamError::Schema(error) => Self::Schema(error),
            AgentTeamError::Choreography(error) => *error,
            other => Self::PayloadEncoding {
                message: other.to_string(),
            },
        }
    }
}
