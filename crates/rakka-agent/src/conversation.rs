//! The sharded moderated-conversation entity and its durable ordered turn
//! protocol ([specification 8.11](../../docs/plans/rakka-agent/spec.md)).
//!
//! A conversation is trusted application data: a stable
//! [`crate::identity::AgentConversationId`], a moderator, an ordered
//! authorized participant roster, a mode, a completion rule, a governing
//! task binding, and creation-fixed budgets — all of it the entity's own
//! state, written in single compare-and-sets. Only the current authorized
//! participant may submit the next turn; a duplicate or out-of-order turn
//! is deduplicated or rejected under a stable code (scenario 43).
//!
//! **Turn deduplication is layered.** Inside the bounded operation-log
//! window a replayed submit is answered with its original outcome. Past the
//! window, the dense turn ledger is the durable echo: a redelivered turn
//! whose speaker and body digest match the recorded coordinate converges
//! idempotently — checked before every other guard, including the terminal
//! one, so it converges even after the conversation ended — while a
//! regenerated submission with different content refuses loudly rather
//! than being silently absorbed. The transcript ring is never a
//! deduplication surface: it drops oldest under its ceiling, and the drop
//! is visible, never silent.
//!
//! Budgets ride the existing budget vocabulary with no parallel machinery:
//! a token grant gates each next turn (exhaustion refuses, never parks),
//! the wall-clock deadline is fixed at creation and observed lazily — a
//! command refuses before the durable flip, the settle pass owns the flip,
//! never a timer — and an accepted turn's reported usage is recorded even
//! when it overshoots, because the spend already happened in the speaker's
//! run. The moderator's early end terminalizes the conversation only; its
//! *result* rides the governing task's existing typed-result and
//! goal-evaluation doors on the run side. The end is the moderator's alone:
//! the ending agent is a claim the transition fences against the durable
//! moderator, exactly as a turn's speaker is fenced against the cursor's
//! derived owner, so a roster participant may speak but never terminalize.
//!
//! The entity embeds the exchange host for uniform routing and recovery,
//! and initiates exactly one exchange: the terminal notice to the governing
//! task ([`crate::choreography::AgentExchangeKind::ConversationTerminalNotice`]),
//! owed in the same compare-and-set as each terminal flip and re-derived by
//! the settle pass until it settles. The conversation has no autonomous
//! timer, so delivery rides whatever drives its settle pass — the A2A
//! surface after every conversation operation, the application's settle
//! sweep, or recovery followed by either. Idle conversations passivate —
//! the protocol is data, not a resident coordinator.

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

use crate::budget::{AgentBudgetConsumption, AgentBudgetDimension, AgentBudgetExhaustion};
use crate::choreography::{
    drive_pending_exchanges, AgentChoreographyError, AgentEntityAddress, AgentExchangeEnvelope,
    AgentExchangeHost, AgentExchangeJournal, AgentExchangeKind, AgentExchangeParticipant,
    AgentExchangePayload, AgentExchangeReply, AgentExchangeResult, AgentExchangeRouter,
    AgentExchangeState, AgentExchangeTransition,
};
use crate::coordination::{
    conversation_expiry_operation_id, conversation_terminal_notice_operation_id,
    conversation_terminal_notice_refusal_settles, conversation_turn_content_digest,
    AgentConversationTerminalNotice, AgentCoordinationError, AgentModerationPolicy,
    AGENT_CONVERSATION_TERMINAL_NOTICE_PAYLOAD_TYPE,
};
use crate::definition::{AgentRevisionNumber, AgentRevisionProvenance};
use crate::identity::{
    AgentConversationId, AgentConversationScope, AgentId, AgentIdentityError, AgentOperationId,
    AgentTaskId, AgentTaskScope,
};
use crate::observability::{
    record_agent_domain_counter, record_unsettleable_exchanges, METRIC_AGENT_MODERATION_TURNS,
};
use crate::schema::{
    AgentRecordKind, AgentSchemaError, AgentSchemaPolicy, VersionedAgentRecord,
    CURRENT_AGENT_CONVERSATION_HISTORY_SCHEMA_VERSION,
    CURRENT_AGENT_CONVERSATION_STATE_SCHEMA_VERSION,
};

/// Result type for conversation entity operations.
pub type AgentConversationResult<T> = Result<T, AgentConversationError>;

/// Clock supplying the timestamps conversation transitions persist.
pub type AgentConversationClock = Arc<dyn Fn() -> AgentTimestampMillis + Send + Sync>;

/// The system clock, stamping transitions where they commit.
#[must_use]
pub fn system_conversation_clock() -> AgentConversationClock {
    Arc::new(|| {
        let millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| {
                u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
            });
        AgentTimestampMillis::new(millis)
    })
}

/// The default sharded entity type of conversation entities.
pub const DEFAULT_AGENT_CONVERSATION_ENTITY_TYPE: &str = "RakkaAgentConversation";

/// Maximum serialized bytes of one conversation's *persisted state* — the
/// whole [`AgentConversationState`], not just the materialized conversation
/// inside it.
///
/// The bound covers what the compare-and-set actually writes: the
/// conversation, the bounded operation log, the pending history outbox, and
/// the exchange journal. An earlier 32 KiB figure measured only the
/// conversation, and could never have held a full operation log and outbox
/// beside even a *default* ledger and ring — so the creation-time arithmetic
/// it was paired with was not an upper bound on anything.
///
/// Sized so every policy the hard caps admit provably fits: the wedge the
/// creation-time guard exists to prevent is then impossible by construction
/// rather than merely refused at the door.
pub const AGENT_CONVERSATION_MATERIALIZED_MAX_BYTES: usize = 128 * 1024;

/// Bytes held back from the materialized bound so a settle transition never
/// finds the record too large to write.
pub const AGENT_CONVERSATION_STATE_GROWTH_RESERVE_BYTES: usize = 4 * 1024;

/// Bounded window of resolved operations a conversation remembers for
/// deduplication.
pub const AGENT_CONVERSATION_OPERATION_LOG_CAPACITY: usize = 64;

/// Bounded outbox of history entries a conversation may owe its sink.
pub const AGENT_CONVERSATION_PENDING_HISTORY_CAPACITY: usize = 32;

/// The most history entries one conversation transition records.
pub const AGENT_CONVERSATION_MAX_HISTORY_PER_TRANSITION: usize = 4;

/// Maximum bytes of one bounded history or refusal detail.
pub const AGENT_CONVERSATION_DETAIL_MAX_LENGTH: usize = 512;

/// Default page size of a conversation history read.
pub const AGENT_CONVERSATION_HISTORY_DEFAULT_PAGE_SIZE: usize = 16;

/// Maximum page size a conversation history cursor may request.
pub const AGENT_CONVERSATION_HISTORY_MAX_PAGE_SIZE: usize = 64;

/// Maximum bytes of the identity-only transcript artifact reference.
pub const AGENT_CONVERSATION_TRANSCRIPT_REF_MAX_BYTES: usize = 256;

/// Maximum bytes of the moderator's bounded early-end reason.
pub const AGENT_CONVERSATION_REASON_MAX_BYTES: usize = 256;

/// Bytes the creation-time worst-case arithmetic reserves per turn-ledger
/// record.
pub const AGENT_CONVERSATION_TURN_RECORD_RESERVE_BYTES: usize = 128;

/// Bytes the creation-time worst-case arithmetic reserves per transcript
/// message *beside* its body — the coordinate, speaker id, timestamp, and
/// JSON envelope around it.
pub const AGENT_CONVERSATION_MESSAGE_RECORD_RESERVE_BYTES: usize = 128;

/// Bytes the creation-time worst-case arithmetic reserves per resolved
/// operation the deduplication log remembers: its id and the full outcome it
/// echoes.
pub const AGENT_CONVERSATION_OPERATION_LOG_ENTRY_RESERVE_BYTES: usize = 320;

/// Bytes the creation-time worst-case arithmetic reserves per history entry
/// waiting in the outbox — including a detail at
/// [`AGENT_CONVERSATION_DETAIL_MAX_LENGTH`].
pub const AGENT_CONVERSATION_HISTORY_ENTRY_RESERVE_BYTES: usize = 1024;

/// Bytes the creation-time worst-case arithmetic reserves for everything
/// outside the turn ledger, the transcript ring, the operation log, and the
/// history outbox: the scope, the roster, the policy, the cursor, the
/// budgets, the transcript reference, and the exchange journal.
pub const AGENT_CONVERSATION_FIXED_OVERHEAD_BYTES: usize = 8 * 1024;

/// Hex characters of the turn body digest the ledger records.
pub const AGENT_CONVERSATION_DIGEST_PREFIX_LENGTH: usize = 16;

/// Payload type of the bounded receipt a conversation returns for a refused
/// exchange.
pub const AGENT_CONVERSATION_RECEIPT_PAYLOAD_TYPE: &str = "rakka.agent.ConversationReceipt";

const DEFAULT_AGENT_CONVERSATION_PASSIVATION_BUFFER_DURATION: Duration = Duration::from_millis(25);

fn bounded_detail(detail: impl Into<String>) -> String {
    let mut detail = detail.into();
    if detail.len() > AGENT_CONVERSATION_DETAIL_MAX_LENGTH {
        detail.truncate(
            (0..=AGENT_CONVERSATION_DETAIL_MAX_LENGTH)
                .rev()
                .find(|index| detail.is_char_boundary(*index))
                .unwrap_or(0),
        );
    }
    detail
}

/// The `skip_serializing_if` predicate of the terminal-notice marker: an
/// unsettled record serializes byte-identically to one persisted before the
/// field existed.
#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_false(value: &bool) -> bool {
    !*value
}

/// Lifecycle of one conversation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentConversationStatus {
    /// The conversation accepts turns.
    Active,
    /// The conversation ended — its completion rule was met, or the
    /// moderator ended it early under policy.
    Ended,
    /// The conversation's creation-fixed deadline passed; observed lazily,
    /// never by a timer.
    Expired,
}

impl AgentConversationStatus {
    /// Whether no further turn is accepted.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Ended | Self::Expired)
    }

    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Ended => "ended",
            Self::Expired => "expired",
        }
    }
}

impl Display for AgentConversationStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// Why one conversation reached its terminal status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentConversationTerminalReason {
    /// Every round the completion rule required completed.
    RoundsComplete,
    /// The moderator ended the conversation early under policy.
    ModeratorEnded,
    /// The creation-fixed deadline passed.
    Expired,
}

impl AgentConversationTerminalReason {
    /// Stable kebab-case code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::RoundsComplete => "rounds-complete",
            Self::ModeratorEnded => "moderator-ended",
            Self::Expired => "expired",
        }
    }
}

impl Display for AgentConversationTerminalReason {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.code())
    }
}

/// How turn ownership advances
/// ([specification 8.11](../../docs/plans/rakka-agent/spec.md)).
///
/// The two modes cover the two recovery shapes scenario 43 must prove: a
/// round-robin owner is a pure function of the immutable roster and the
/// durable cursor, while a moderator-directed owner is a durable moderator
/// decision. A broadcast or free-for-all mode would contradict "only the
/// current authorized participant may submit" and is deliberately absent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentConversationMode {
    /// Each round visits the roster in order; the moderator holds no
    /// rotation turn. An application that wants the moderator's voice in
    /// rotation lists it in the roster.
    RoundRobin,
    /// The moderator owns every even turn and each of its turns directs
    /// what follows: designating the next speaker, or closing the round.
    ModeratorDirected,
}

impl AgentConversationMode {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::RoundRobin => "round-robin",
            Self::ModeratorDirected => "moderator-directed",
        }
    }
}

impl Display for AgentConversationMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// When one conversation completes on its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentConversationCompletionRule {
    /// Completing the final permitted round ends the conversation in the
    /// same compare-and-set — completion beats exhaustion, so no turn ever
    /// refuses rounds-exhausted under this rule.
    AllRounds,
    /// Only the moderator's early end completes the conversation; at the
    /// round ceiling the cursor parks and further turns refuse under a
    /// stable code while the status stays active — never a silent park.
    ModeratorDecides,
}

impl AgentConversationCompletionRule {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::AllRounds => "all-rounds",
            Self::ModeratorDecides => "moderator-decides",
        }
    }
}

impl Display for AgentConversationCompletionRule {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// The speaker one ledger record attributes, as a roster coordinate.
///
/// Stored positionally rather than by id so a ledger of hundreds of records
/// stays within its per-record byte reserve whatever the roster's ids look
/// like; the roster is immutable, so the coordinate resolves stably forever.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentConversationSpeaker {
    /// The moderator.
    Moderator,
    /// The roster participant at this index.
    Participant(u8),
}

/// What a moderator-directed moderator turn directs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentConversationDirection {
    /// The named roster participant owns the next turn.
    Designate(AgentId),
    /// The round closes; the completion arithmetic decides whether the next
    /// round opens or the conversation ends.
    CloseRound,
}

/// One recorded turn: the dense ledger entry that is the durable
/// deduplication echo past the bounded operation-log window.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentConversationTurnRecord {
    /// The round the turn belongs to.
    pub round: u64,
    /// The turn index within the round.
    pub turn: u32,
    /// Who spoke, as a stable roster coordinate.
    pub speaker: AgentConversationSpeaker,
    /// The leading hex characters of the body digest — enough to answer a
    /// replay's identity question without carrying the body.
    pub digest_prefix: String,
    /// When the turn committed.
    pub at: AgentTimestampMillis,
}

/// One bounded transcript message on the conversation's durable ring.
///
/// The ring is the bounded in-state transcript; the identity-only artifact
/// reference points at whatever fuller transcript the application keeps.
/// The ring drops its oldest entry when full, and the drop is visible in
/// [`AgentConversation::messages_dropped`] — bounded loss, never silent,
/// and never a deduplication surface.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentConversationMessage {
    /// The round the message's turn belongs to.
    pub round: u64,
    /// The turn index within the round.
    pub turn: u32,
    /// The speaking participant or moderator.
    pub speaker: AgentId,
    /// The bounded message body.
    pub body: String,
    /// When it was appended.
    pub at: AgentTimestampMillis,
}

/// The conversation's creation-fixed budgets, in the existing budget
/// vocabulary ([specification 9.7](../../docs/plans/rakka-agent/spec.md)).
///
/// No parallel budget machinery: the token ceiling is the existing
/// [`AgentBudgetDimension::Tokens`] dimension, the deadline is fixed at
/// creation the way a run's deadline is fixed at assignment acceptance, and
/// consumption is the existing [`AgentBudgetConsumption`] record. An
/// accepted turn's usage is recorded even when it overshoots — the spend
/// already happened in the speaker's run — and exhaustion refuses the
/// *next* turn rather than parking anything.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentConversationBudgets {
    /// The token ceiling; `None` is unbounded.
    pub tokens: Option<u64>,
    /// The creation-fixed deadline; `None` never expires on its own.
    pub deadline: Option<AgentTimestampMillis>,
    /// What the conversation's turns have reported consuming.
    pub consumed: AgentBudgetConsumption,
}

/// The materialized conversation: roster, cursor, ledger, and transcript.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentConversation {
    /// Lifecycle of the conversation.
    pub status: AgentConversationStatus,
    /// Why the terminal status was reached, once one was.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_reason: Option<AgentConversationTerminalReason>,
    /// The moderator.
    pub moderator: AgentId,
    /// How turn ownership advances.
    pub mode: AgentConversationMode,
    /// When the conversation completes on its own.
    pub completion: AgentConversationCompletionRule,
    /// The trusted policy in force, embedded at creation.
    pub policy: AgentModerationPolicy,
    /// The governing task this conversation serves. Trusted wiring: the
    /// moderator is that task's assignee, and the early-end result rides
    /// the task's existing result-validation and evaluation doors.
    pub task: AgentTaskId,
    /// The round the next expected turn belongs to.
    pub round: u64,
    /// The turn index the next expected turn holds within its round.
    pub turn_in_round: u32,
    /// The stored owner of the next moderator-directed participant turn —
    /// the one owner fact that is a durable decision rather than a
    /// derivation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub designated: Option<AgentId>,
    participants: Vec<AgentId>,
    turns: Vec<AgentConversationTurnRecord>,
    messages: VecDeque<AgentConversationMessage>,
    /// Messages the bounded ring has dropped, oldest first.
    pub messages_dropped: u64,
    /// The identity-only transcript artifact reference, when the
    /// application keeps one. Never dereferenced by the entity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transcript_ref: Option<String>,
    /// The creation-fixed budgets and what turns have consumed.
    pub budgets: AgentConversationBudgets,
    /// When the conversation was created.
    pub created_at: AgentTimestampMillis,
    /// When the conversation reached its terminal status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ended_at: Option<AgentTimestampMillis>,
    /// Whether the terminal notice to the governing task has settled: the
    /// durable once-guard past the journal's bounded deduplication window.
    /// Records persisted before this field load with it unset — so a
    /// pre-slice conversation that is already terminal owes the notice once
    /// on its next settle pass, deliberately: that is the back-fill making
    /// it observable from its task.
    #[serde(default, skip_serializing_if = "is_false")]
    pub terminal_notice_settled: bool,
}

impl AgentConversation {
    /// The ordered, immutable participant roster.
    #[must_use]
    pub fn participants(&self) -> &[AgentId] {
        &self.participants
    }

    /// The dense turn ledger, in submission order.
    #[must_use]
    pub fn turns(&self) -> &[AgentConversationTurnRecord] {
        &self.turns
    }

    /// The bounded transcript ring, oldest first.
    #[must_use]
    pub const fn messages(&self) -> &VecDeque<AgentConversationMessage> {
        &self.messages
    }

    /// Whether one agent is the moderator or a roster participant.
    #[must_use]
    pub fn is_authorized(&self, agent: &AgentId) -> bool {
        agent == &self.moderator || self.participants.contains(agent)
    }

    /// Whether the creation-fixed deadline has passed.
    #[must_use]
    pub fn is_expired_at(&self, now: AgentTimestampMillis) -> bool {
        self.budgets
            .deadline
            .is_some_and(|deadline| now.as_millis() >= deadline.as_millis())
    }

    /// Who owns the next expected turn, derived from the durable cursor —
    /// never from anything delivery- or residency-shaped, which is what
    /// makes the owner recoverable by construction after passivation or
    /// shard movement.
    #[must_use]
    pub fn turn_owner(&self) -> Option<&AgentId> {
        if self.status != AgentConversationStatus::Active {
            return None;
        }
        // A parked cursor owns nothing. Under the moderator-decides rule the
        // round ceiling leaves the conversation active with the cursor at
        // the rim, and every turn from there refuses
        // `conversation-rounds-exhausted` — so naming a next speaker would
        // send that speaker into a refusal forever, and a driver polling
        // `current_speaker` would retry instead of routing the moderator to
        // its early end. The projection has to say what is true: nobody's
        // turn.
        if self.round >= u64::from(self.policy.effective_max_rounds()) {
            return None;
        }
        if self.turn_in_round >= self.policy.effective_max_turns_per_round() {
            return None;
        }
        match self.mode {
            AgentConversationMode::RoundRobin => self.participants.get(self.turn_in_round as usize),
            AgentConversationMode::ModeratorDirected => {
                if self.turn_in_round % 2 == 0 {
                    Some(&self.moderator)
                } else {
                    self.designated.as_ref()
                }
            }
        }
    }

    /// Resolves one ledger speaker coordinate back to its id.
    #[must_use]
    pub fn speaker_id(&self, speaker: AgentConversationSpeaker) -> Option<&AgentId> {
        match speaker {
            AgentConversationSpeaker::Moderator => Some(&self.moderator),
            AgentConversationSpeaker::Participant(index) => self.participants.get(index as usize),
        }
    }
}

/// The trusted creation record of one conversation.
///
/// Construction data from the application wiring — moderator, roster, mode,
/// policy, budgets, and the governing task binding can never come from
/// model output or a wire peer; the A2A surface carries no create
/// operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentConversationCreation {
    /// The moderator.
    pub moderator: AgentId,
    /// The ordered participant roster. Immutable after creation.
    pub participants: Vec<AgentId>,
    /// How turn ownership advances.
    pub mode: AgentConversationMode,
    /// When the conversation completes on its own.
    pub completion: AgentConversationCompletionRule,
    /// The policy in force for the conversation's lifetime.
    pub policy: AgentModerationPolicy,
    /// The governing task this conversation serves.
    pub task: AgentTaskId,
    /// The token ceiling; `None` is unbounded.
    #[serde(default)]
    pub tokens: Option<u64>,
    /// Milliseconds after creation at which the conversation expires,
    /// fixing the deadline forever. `None` never expires on its own.
    #[serde(default)]
    pub max_wall_clock_millis: Option<u64>,
    /// The identity-only transcript artifact reference.
    #[serde(default)]
    pub transcript_ref: Option<String>,
}

/// One participant's turn submission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentConversationTurnSubmit {
    /// The round this turn claims.
    pub round: u64,
    /// The turn index this turn claims within its round.
    pub turn: u32,
    /// The claimed speaker, validated against the roster and the cursor's
    /// derived owner.
    pub participant: AgentId,
    /// The bounded turn body.
    pub body: String,
    /// The moderator's direction; required on a moderator-directed
    /// moderator turn, forbidden everywhere else.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub direction: Option<AgentConversationDirection>,
    /// What the speaker's run reports having consumed producing this turn.
    /// Recorded, never refused: the spend already happened.
    #[serde(default)]
    pub usage: AgentBudgetConsumption,
}

/// Monotonic sequence of one conversation history entry.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct AgentConversationHistorySequence(u64);

impl AgentConversationHistorySequence {
    /// The first sequence a conversation's history uses.
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

impl Display for AgentConversationHistorySequence {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, f)
    }
}

/// What one conversation history entry records
/// ([specification 17.13](../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentConversationHistoryKind {
    /// The conversation was created.
    Created,
    /// A turn was recorded.
    TurnRecorded,
    /// A round closed and the next one opened.
    RoundAdvanced,
    /// The conversation ended — completion, or the moderator's early end.
    Ended,
    /// The creation-fixed deadline was durably observed.
    Expired,
}

impl AgentConversationHistoryKind {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Created => "conversation-created",
            Self::TurnRecorded => "conversation-turn-recorded",
            Self::RoundAdvanced => "conversation-round-advanced",
            Self::Ended => "conversation-ended",
            Self::Expired => "conversation-expired",
        }
    }
}

impl Display for AgentConversationHistoryKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// One append-only entry in a conversation's durable history
/// ([specification 17.13](../../docs/plans/rakka-agent/spec.md)).
///
/// A bounded record of *what happened*: identities, coordinates, and stable
/// codes only. It never carries turn bodies, prompts, memory records, or
/// resolved credentials.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentConversationHistoryEntry {
    schema_version: StateSchemaVersion,
    /// Monotonic sequence within the conversation, and the append's
    /// idempotency key.
    pub sequence: AgentConversationHistorySequence,
    /// What the entry records.
    pub kind: AgentConversationHistoryKind,
    /// The operation that produced it.
    pub operation_id: AgentOperationId,
    /// The participant involved, when one was.
    pub participant: Option<AgentId>,
    /// The round involved, when one was.
    pub round: Option<u64>,
    /// The turn index involved, when one was.
    pub turn: Option<u32>,
    /// Bounded detail: a stable code or count, never free text. The one
    /// terminalizing operation a caller can reach records its free-text
    /// reason in [`Self::reason`] instead, so a reader never has to guess
    /// which of the two this field holds.
    pub detail: String,
    /// The authenticated principal that accepted the operation, when one
    /// was required — the durable answer to *who did this*.
    ///
    /// Recorded as `type:id`. Added after the initial slice, so a history
    /// entry written before it decodes with `None`.
    #[serde(default)]
    pub principal: Option<String>,
    /// The caller's bounded free-text reason, when the operation carried
    /// one. Bounded by [`AGENT_CONVERSATION_REASON_MAX_BYTES`].
    ///
    /// Added after the initial slice, so a history entry written before it
    /// decodes with `None`.
    #[serde(default)]
    pub reason: Option<String>,
    /// When the transition committed.
    pub at: AgentTimestampMillis,
}

impl AgentConversationHistoryEntry {
    pub(crate) fn new(
        sequence: AgentConversationHistorySequence,
        kind: AgentConversationHistoryKind,
        operation_id: AgentOperationId,
        at: AgentTimestampMillis,
    ) -> Self {
        Self {
            schema_version: CURRENT_AGENT_CONVERSATION_HISTORY_SCHEMA_VERSION,
            sequence,
            kind,
            operation_id,
            participant: None,
            round: None,
            turn: None,
            detail: String::new(),
            principal: None,
            reason: None,
            at,
        }
    }

    fn with_participant(mut self, participant: AgentId) -> Self {
        self.participant = Some(participant);
        self
    }

    /// Records who accepted the operation and why they said they did.
    ///
    /// The reason is truncated to [`AGENT_CONVERSATION_REASON_MAX_BYTES`] on
    /// a character boundary; `end_early` refuses an over-long one before
    /// reaching here, so truncation is the defensive half of that bound.
    fn with_provenance(mut self, provenance: &AgentRevisionProvenance, reason: &str) -> Self {
        self.principal = Some(format!(
            "{}:{}",
            provenance.principal.principal_type, provenance.principal.principal_id
        ));
        if !reason.is_empty() {
            let mut bounded = reason.to_string();
            if bounded.len() > AGENT_CONVERSATION_REASON_MAX_BYTES {
                let cut = (0..=AGENT_CONVERSATION_REASON_MAX_BYTES)
                    .rev()
                    .find(|index| bounded.is_char_boundary(*index))
                    .unwrap_or(0);
                bounded.truncate(cut);
            }
            self.reason = Some(bounded);
        }
        self
    }

    fn with_round(mut self, round: u64) -> Self {
        self.round = Some(round);
        self
    }

    fn with_coordinate(mut self, round: u64, turn: u32) -> Self {
        self.round = Some(round);
        self.turn = Some(turn);
        self
    }

    fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = bounded_detail(detail);
        self
    }
}

impl VersionedAgentRecord for AgentConversationHistoryEntry {
    const RECORD_KIND: AgentRecordKind = AgentRecordKind::ConversationHistoryEntry;

    fn schema_version(&self) -> StateSchemaVersion {
        self.schema_version
    }
}

/// A bounded, authorized read over a conversation's history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentConversationHistoryCursor {
    after: Option<AgentConversationHistorySequence>,
    limit: usize,
}

impl AgentConversationHistoryCursor {
    /// A cursor over the whole history, from the beginning.
    #[must_use]
    pub const fn start() -> Self {
        Self {
            after: None,
            limit: AGENT_CONVERSATION_HISTORY_DEFAULT_PAGE_SIZE,
        }
    }

    /// A cursor resuming after one sequence.
    #[must_use]
    pub const fn after(sequence: AgentConversationHistorySequence) -> Self {
        Self {
            after: Some(sequence),
            limit: AGENT_CONVERSATION_HISTORY_DEFAULT_PAGE_SIZE,
        }
    }

    /// Sets the page size, clamped to
    /// [`AGENT_CONVERSATION_HISTORY_MAX_PAGE_SIZE`].
    #[must_use]
    pub const fn with_limit(mut self, limit: usize) -> Self {
        self.limit = if limit == 0 {
            1
        } else if limit > AGENT_CONVERSATION_HISTORY_MAX_PAGE_SIZE {
            AGENT_CONVERSATION_HISTORY_MAX_PAGE_SIZE
        } else {
            limit
        };
        self
    }

    /// Repositions this cursor so `sequence` is the next entry it expects.
    ///
    /// The companion of [`AgentConversationError::HistoryWindowExpired`]: a
    /// reader handed a retained floor resumes *at* it, keeping its page size.
    /// Sequences start at [`AgentConversationHistorySequence::FIRST`], so a zero
    /// resumes from the beginning.
    #[must_use]
    pub const fn resuming_at(mut self, sequence: AgentConversationHistorySequence) -> Self {
        self.after = match sequence.get() {
            0 => None,
            value => Some(AgentConversationHistorySequence::new(value - 1)),
        };
        self
    }

    /// The sequence this page resumes after.
    #[must_use]
    pub const fn position(&self) -> Option<AgentConversationHistorySequence> {
        self.after
    }

    /// The clamped page size.
    #[must_use]
    pub const fn limit(&self) -> usize {
        self.limit
    }
}

impl Default for AgentConversationHistoryCursor {
    fn default() -> Self {
        Self::start()
    }
}

/// One bounded page of conversation history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentConversationHistoryPage {
    /// The entries, oldest first.
    pub entries: Vec<AgentConversationHistoryEntry>,
    /// The cursor that resumes after this page, when more history exists.
    pub next: Option<AgentConversationHistoryCursor>,
}

impl AgentConversationHistoryPage {
    /// Whether more history follows this page.
    #[must_use]
    pub const fn has_more(&self) -> bool {
        self.next.is_some()
    }
}

/// Boxed future returned by an [`AgentConversationHistoryStore`].
pub type AgentConversationHistoryFuture<'a, T> =
    Pin<Box<dyn Future<Output = AgentConversationResult<T>> + Send + 'a>>;

/// The append-only durable history of every conversation, separate from the
/// bounded materialized state that drives transitions
/// ([specification 17.13](../../docs/plans/rakka-agent/spec.md)).
///
/// An append is idempotent on `(scope, sequence)`: the entity assigns the
/// sequence inside the transition that produced the entry, so re-driving an
/// interrupted flush writes the same entry to the same slot. A store that
/// finds a different entry already at a sequence must fail closed rather
/// than overwrite it.
pub trait AgentConversationHistoryStore: Clone + Send + Sync + 'static {
    /// Stable backend name, used in telemetry.
    fn backend_name(&self) -> &'static str;

    /// Appends one entry, idempotently.
    fn append<'a>(
        &'a self,
        scope: &'a AgentConversationScope,
        entry: &'a AgentConversationHistoryEntry,
    ) -> AgentConversationHistoryFuture<'a, ()>;

    /// Reads one bounded page, contiguous from the cursor.
    ///
    /// A backend MUST fail a read with
    /// [`AgentConversationError::HistoryWindowExpired`] — naming the oldest
    /// entry the reader can actually resume from — whenever answering would
    /// otherwise vouch for entries the reader has not seen: a cursor
    /// preceding the retained window, a discontinuity at the read head, or a
    /// cursor past the newest retained entry, which this log never issued. A
    /// discontinuity *inside* the page truncates it before the hole with a
    /// `next` cursor instead, so the retained prefix is delivered whole and
    /// the next read is refused at the hole. See
    /// [`crate::task::AgentTaskHistoryStore::read`] for the full contract;
    /// [`crate::testkit::assert_conversation_history_store_contract`] is the
    /// harness that proves it.
    fn read<'a>(
        &'a self,
        scope: &'a AgentConversationScope,
        cursor: AgentConversationHistoryCursor,
    ) -> AgentConversationHistoryFuture<'a, AgentConversationHistoryPage>;
}

/// An in-memory conversation history, for tests and single-process
/// deployments.
///
/// The PostgreSQL backend is a recorded follow-up of slice 5.3, the team
/// history's precedent.
#[derive(Debug, Clone, Default)]
pub struct InMemoryAgentConversationHistoryStore {
    inner: Arc<Mutex<InMemoryConversationHistoryInner>>,
}

/// The shared state behind every clone of one in-memory conversation history.
///
/// Retention lives *inside* the shared state, beside the log it bounds: the
/// store is `Clone` by contract, and a bound that lived per-handle would let a
/// clone taken before `with_retention` keep appending to the same shared log
/// unbounded — the retention contract silently failing to hold.
#[derive(Debug, Default)]
struct InMemoryConversationHistoryInner {
    entries: BTreeMap<String, BTreeMap<u64, AgentConversationHistoryEntry>>,
    retention: Option<usize>,
}

impl InMemoryAgentConversationHistoryStore {
    /// Creates an empty history that retains everything appended to it.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Bounds the history retained per conversation, evicting the oldest
    /// entries.
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
            .expect("the conversation history should not be poisoned")
            .retention = Some(entries.max(1));
        self
    }

    /// How many entries one conversation has.
    #[must_use]
    pub fn len(&self, scope: &AgentConversationScope) -> usize {
        self.inner
            .lock()
            .expect("the conversation history should not be poisoned")
            .entries
            .get(&scope.key())
            .map_or(0, BTreeMap::len)
    }

    /// Whether one conversation has no history.
    #[must_use]
    pub fn is_empty(&self, scope: &AgentConversationScope) -> bool {
        self.len(scope) == 0
    }
}

impl AgentConversationHistoryStore for InMemoryAgentConversationHistoryStore {
    fn backend_name(&self) -> &'static str {
        "in-memory"
    }

    fn append<'a>(
        &'a self,
        scope: &'a AgentConversationScope,
        entry: &'a AgentConversationHistoryEntry,
    ) -> AgentConversationHistoryFuture<'a, ()> {
        Box::pin(async move {
            let mut inner = self
                .inner
                .lock()
                .expect("the conversation history should not be poisoned");
            let retention = inner.retention;
            let conversation = inner.entries.entry(scope.key()).or_default();
            match conversation.get(&entry.sequence.get()) {
                Some(existing) if existing == entry => Ok(()),
                Some(_) => Err(AgentConversationError::HistoryConflict {
                    sequence: entry.sequence,
                }),
                None => {
                    conversation.insert(entry.sequence.get(), entry.clone());
                    if let Some(retention) = retention {
                        while conversation.len() > retention {
                            let oldest = *conversation
                                .keys()
                                .next()
                                .expect("a history longer than its retention holds an entry");
                            conversation.remove(&oldest);
                        }
                    }
                    Ok(())
                }
            }
        })
    }

    fn read<'a>(
        &'a self,
        scope: &'a AgentConversationScope,
        cursor: AgentConversationHistoryCursor,
    ) -> AgentConversationHistoryFuture<'a, AgentConversationHistoryPage> {
        Box::pin(async move {
            let inner = self
                .inner
                .lock()
                .expect("the conversation history should not be poisoned");
            let Some(conversation) = inner.entries.get(&scope.key()) else {
                // A positioned cursor into a scope with no log at all is a
                // cursor this store never issued; an empty page would vouch
                // for entries the reader believes it has seen. Only the
                // start-of-log read is honestly empty here.
                if cursor.position().is_some_and(|after| after.get() > 0) {
                    return Err(AgentConversationError::HistoryWindowExpired {
                        oldest_retained: None,
                    });
                }
                return Ok(AgentConversationHistoryPage {
                    entries: Vec::new(),
                    next: None,
                });
            };

            // The sequence the reader expects next: one past its cursor, or the
            // very first entry when it is starting from the beginning.
            let start = cursor
                .position()
                .map_or(AgentConversationHistorySequence::FIRST.get(), |after| {
                    after.get().saturating_add(1)
                });
            // A cursor past the newest retained entry was never issued by this
            // log: an empty page would stamp "you are current" over sequences
            // the reader has not seen, and once the log grows past the cursor
            // the reader would resume across them silently.
            let newest = conversation.keys().next_back().copied().unwrap_or_default();
            if start > newest.saturating_add(1) {
                return Err(AgentConversationError::HistoryWindowExpired {
                    oldest_retained: conversation
                        .keys()
                        .next()
                        .copied()
                        .map(AgentConversationHistorySequence::new),
                });
            }
            // History sequences are dense — the transition that consumes one
            // pushes its entry in the same step — so a missing entry means the
            // window moved (or a durable backend lost it). A hole at the read
            // head is refused with the floor past it; a hole further in
            // truncates the page instead, so the retained prefix is delivered
            // whole whatever the reader's page size, and the *next* read
            // starts at the hole and gets the refusal.
            if let Some((&first, _)) = conversation.range(start..).next() {
                if first != start {
                    return Err(AgentConversationError::HistoryWindowExpired {
                        oldest_retained: Some(AgentConversationHistorySequence::new(first)),
                    });
                }
            }
            let mut page: Vec<AgentConversationHistoryEntry> = Vec::new();
            let mut expected = start;
            for (&sequence, entry) in conversation.range(start..) {
                if sequence != expected || page.len() == cursor.limit() {
                    break;
                }
                page.push(entry.clone());
                expected = sequence.saturating_add(1);
            }
            let next = conversation
                .range(expected..)
                .next()
                .is_some()
                .then(|| {
                    page.last().map(|entry| {
                        AgentConversationHistoryCursor::after(entry.sequence)
                            .with_limit(cursor.limit())
                    })
                })
                .flatten();

            Ok(AgentConversationHistoryPage {
                entries: page,
                next,
            })
        })
    }
}

/// The compact result of one accepted conversation transition.
///
/// A replayed operation returns this again rather than transitioning twice.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentConversationOutcome {
    /// Lifecycle after the transition.
    pub status: AgentConversationStatus,
    /// The terminal reason, once one was reached.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_reason: Option<AgentConversationTerminalReason>,
    /// The round the next expected turn belongs to.
    pub round: u64,
    /// The turn index the next expected turn holds.
    pub turn_in_round: u32,
    /// Who owns the next expected turn, when the conversation is active.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_speaker: Option<AgentId>,
    /// How many turns the ledger has recorded.
    pub turns_recorded: u64,
    /// Transcript-ring size after the transition.
    pub messages: usize,
}

/// A bounded, credential-free projection of one conversation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AgentConversationSnapshot {
    /// The conversation's scope.
    pub scope: AgentConversationScope,
    /// Lifecycle.
    pub status: AgentConversationStatus,
    /// The terminal reason, once one was reached.
    pub terminal_reason: Option<AgentConversationTerminalReason>,
    /// The moderator.
    pub moderator: AgentId,
    /// The ordered participant roster.
    pub participants: Vec<AgentId>,
    /// How turn ownership advances.
    pub mode: AgentConversationMode,
    /// When the conversation completes on its own.
    pub completion: AgentConversationCompletionRule,
    /// The policy revision in force.
    pub policy_revision: AgentRevisionNumber,
    /// The governing task.
    pub task: AgentTaskId,
    /// The round the next expected turn belongs to.
    pub round: u64,
    /// The turn index the next expected turn holds.
    pub turn_in_round: u32,
    /// The stored moderator-directed designation, while one stands.
    pub designated: Option<AgentId>,
    /// Who owns the next expected turn, derived from the cursor.
    pub current_speaker: Option<AgentId>,
    /// The dense turn ledger, in submission order.
    pub turns: Vec<AgentConversationTurnRecord>,
    /// The transcript ring, oldest first.
    pub messages: Vec<AgentConversationMessage>,
    /// Messages the bounded ring dropped.
    pub messages_dropped: u64,
    /// The identity-only transcript artifact reference.
    pub transcript_ref: Option<String>,
    /// The creation-fixed budgets and consumption.
    pub budgets: AgentConversationBudgets,
    /// When the conversation was created.
    pub created_at: AgentTimestampMillis,
    /// When the conversation reached its terminal status.
    pub ended_at: Option<AgentTimestampMillis>,
    /// Whether the terminal notice to the governing task has settled.
    /// Snapshots persisted before this field load with it unset.
    #[serde(default, skip_serializing_if = "is_false")]
    pub terminal_notice_settled: bool,
    /// How many history entries the conversation has recorded.
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

/// The bounded log of resolved conversation operations.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentConversationOperationLog {
    entries: VecDeque<AgentConversationOperationLogEntry>,
}

impl AgentConversationOperationLog {
    /// The outcome a previously applied operation produced, if it is still
    /// in the window.
    #[must_use]
    pub fn outcome(&self, operation_id: &AgentOperationId) -> Option<&AgentConversationOutcome> {
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

    fn record(&mut self, operation_id: AgentOperationId, outcome: AgentConversationOutcome) {
        self.entries.push_back(AgentConversationOperationLogEntry {
            operation_id,
            outcome,
        });
        while self.entries.len() > AGENT_CONVERSATION_OPERATION_LOG_CAPACITY {
            self.entries.pop_front();
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AgentConversationOperationLogEntry {
    operation_id: AgentOperationId,
    outcome: AgentConversationOutcome,
}

/// The durable state of one conversation entity.
///
/// The materialized conversation, the history it owes its sink, the
/// operations it has resolved, and the exchange journal — all in one
/// compare-and-set. The journal is empty this slice — the conversation
/// initiates no exchange — but pre-wired so the terminal-notification
/// exchange of the replayable-events slice is a code change, not a schema
/// migration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentConversationState {
    schema_version: StateSchemaVersion,
    scope: AgentConversationScope,
    conversation: Option<AgentConversation>,
    applied_operations: AgentConversationOperationLog,
    pending_history: Vec<AgentConversationHistoryEntry>,
    next_history_sequence: AgentConversationHistorySequence,
    journal: AgentExchangeJournal,
    updated_at: AgentTimestampMillis,
}

impl AgentConversationState {
    /// The state of a conversation that has never been created.
    #[must_use]
    pub fn uncreated(scope: AgentConversationScope, now: AgentTimestampMillis) -> Self {
        Self {
            schema_version: CURRENT_AGENT_CONVERSATION_STATE_SCHEMA_VERSION,
            scope,
            conversation: None,
            applied_operations: AgentConversationOperationLog::default(),
            pending_history: Vec::new(),
            next_history_sequence: AgentConversationHistorySequence::FIRST,
            journal: AgentExchangeJournal::new(),
            updated_at: now,
        }
    }

    /// The scope this state belongs to.
    #[must_use]
    pub const fn scope(&self) -> &AgentConversationScope {
        &self.scope
    }

    /// The materialized conversation, once it has been created.
    #[must_use]
    pub const fn conversation(&self) -> Option<&AgentConversation> {
        self.conversation.as_ref()
    }

    /// Whether the conversation has been created.
    #[must_use]
    pub const fn is_created(&self) -> bool {
        self.conversation.is_some()
    }

    /// The bounded log of resolved operations.
    #[must_use]
    pub const fn applied_operations(&self) -> &AgentConversationOperationLog {
        &self.applied_operations
    }

    /// The history entries the conversation owes its sink.
    #[must_use]
    pub fn pending_history(&self) -> &[AgentConversationHistoryEntry] {
        &self.pending_history
    }

    /// The time of the last accepted transition.
    #[must_use]
    pub const fn updated_at(&self) -> AgentTimestampMillis {
        self.updated_at
    }

    /// How many further history entries the conversation may record before
    /// its outbox is full.
    #[must_use]
    pub fn history_headroom(&self) -> usize {
        AGENT_CONVERSATION_PENDING_HISTORY_CAPACITY.saturating_sub(self.pending_history.len())
    }

    /// Refuses a state that would exceed its persisted bound.
    ///
    /// Measured over the whole record the compare-and-set writes — the
    /// conversation *and* the operation log, the pending history outbox, and
    /// the exchange journal beside it. Measuring only the conversation would
    /// leave the three bounded collections growing under no bound at all,
    /// and it is the whole record a durable store's per-value limit applies
    /// to.
    ///
    /// This stays a defensive guard: [`create_conversation`] refuses at the
    /// door any policy whose worst case could reach here, because a
    /// mid-round refusal would wedge the protocol.
    fn check_bounds(&self) -> AgentConversationResult<()> {
        let bytes = serde_json::to_vec(self)
            .map(|bytes| bytes.len())
            .unwrap_or(usize::MAX);
        let maximum = AGENT_CONVERSATION_MATERIALIZED_MAX_BYTES
            .saturating_sub(AGENT_CONVERSATION_STATE_GROWTH_RESERVE_BYTES);
        if bytes > maximum {
            return Err(AgentConversationError::StateBounds { bytes, maximum });
        }
        Ok(())
    }

    /// The compact outcome describing the current state.
    #[must_use]
    pub fn outcome(&self) -> AgentConversationOutcome {
        let Some(conversation) = &self.conversation else {
            return AgentConversationOutcome {
                status: AgentConversationStatus::Active,
                terminal_reason: None,
                round: 0,
                turn_in_round: 0,
                current_speaker: None,
                turns_recorded: 0,
                messages: 0,
            };
        };
        AgentConversationOutcome {
            status: conversation.status,
            terminal_reason: conversation.terminal_reason,
            round: conversation.round,
            turn_in_round: conversation.turn_in_round,
            current_speaker: conversation.turn_owner().cloned(),
            turns_recorded: conversation.turns.len() as u64,
            messages: conversation.messages.len(),
        }
    }

    /// A bounded, credential-free projection of this state.
    #[must_use]
    pub fn snapshot(&self) -> Option<AgentConversationSnapshot> {
        let conversation = self.conversation.as_ref()?;
        Some(AgentConversationSnapshot {
            scope: self.scope.clone(),
            status: conversation.status,
            terminal_reason: conversation.terminal_reason,
            moderator: conversation.moderator.clone(),
            participants: conversation.participants.clone(),
            mode: conversation.mode,
            completion: conversation.completion,
            policy_revision: conversation.policy.revision,
            task: conversation.task.clone(),
            round: conversation.round,
            turn_in_round: conversation.turn_in_round,
            designated: conversation.designated.clone(),
            current_speaker: conversation.turn_owner().cloned(),
            turns: conversation.turns.clone(),
            messages: conversation.messages.iter().cloned().collect(),
            messages_dropped: conversation.messages_dropped,
            transcript_ref: conversation.transcript_ref.clone(),
            budgets: conversation.budgets.clone(),
            created_at: conversation.created_at,
            ended_at: conversation.ended_at,
            terminal_notice_settled: conversation.terminal_notice_settled,
            history_entries: self.next_history_sequence.get().saturating_sub(1),
            owed_history: self.pending_history.len(),
            updated_at: self.updated_at,
        })
    }

    fn record_history(
        &mut self,
        build: impl FnOnce(AgentConversationHistorySequence) -> AgentConversationHistoryEntry,
    ) {
        let sequence = self.next_history_sequence;
        self.next_history_sequence = sequence.next();
        self.pending_history.push(build(sequence));
    }

    fn clear_flushed_history(&mut self, flushed: &[AgentConversationHistorySequence]) {
        self.pending_history
            .retain(|entry| !flushed.contains(&entry.sequence));
    }

    fn conversation_mut(&mut self) -> AgentConversationResult<&mut AgentConversation> {
        self.conversation
            .as_mut()
            .ok_or_else(|| AgentConversationError::NotCreated {
                scope: self.scope.clone(),
            })
    }

    /// Refuses every mutating command a non-active conversation cannot
    /// take.
    fn require_active(
        &self,
        now: AgentTimestampMillis,
    ) -> AgentConversationResult<&AgentConversation> {
        let Some(conversation) = self.conversation.as_ref() else {
            return Err(AgentConversationError::NotCreated {
                scope: self.scope.clone(),
            });
        };
        match conversation.status {
            AgentConversationStatus::Ended => Err(AgentConversationError::Ended),
            AgentConversationStatus::Expired => Err(AgentConversationError::Expired),
            // The deadline refuses before the durable flip: the settle pass
            // owns the flip, and a command's own refusal must not depend on
            // whether that pass has run yet.
            AgentConversationStatus::Active if conversation.is_expired_at(now) => {
                Err(AgentConversationError::Expired)
            }
            AgentConversationStatus::Active => Ok(conversation),
        }
    }
}

impl AgentExchangeState for AgentConversationState {
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

impl VersionedAgentRecord for AgentConversationState {
    const RECORD_KIND: AgentRecordKind = AgentRecordKind::ConversationState;

    fn schema_version(&self) -> StateSchemaVersion {
        self.schema_version
    }
}

/// The domain half of the conversation entity.
///
/// It supplies bounded, pure transitions and nothing else; the choreography
/// substrate owns durability, deduplication, re-drive, and routing. The
/// conversation receives and initiates no exchange this slice, so every
/// delivered kind is refused and the settle hooks are inert.
#[derive(Debug, Clone, Copy, Default)]
pub struct AgentConversationParticipant;

impl AgentExchangeParticipant for AgentConversationParticipant {
    type State = AgentConversationState;

    fn initialize(&self, address: &AgentEntityAddress, now: AgentTimestampMillis) -> Self::State {
        let scope = match address {
            AgentEntityAddress::Conversation(scope) => scope.clone(),
            // The host builds a participant for the address it serves, and
            // the entity refuses an id that does not parse into a
            // conversation scope, so this is unreachable in practice. An
            // uncreated conversation under an address that can never
            // receive a creation is inert.
            other => {
                AgentConversationScope::new(other.tenant().clone(), unroutable_conversation_id())
                    .expect("the unroutable conversation scope is well formed")
            }
        };
        AgentConversationState::uncreated(scope, now)
    }

    fn apply(
        &self,
        _state: &mut Self::State,
        envelope: &AgentExchangeEnvelope,
        _now: AgentTimestampMillis,
    ) -> AgentExchangeTransition {
        let kind = envelope.kind();
        AgentExchangeTransition::new(refuse(
            "unsupported-exchange",
            format!("a conversation entity does not receive a {kind} exchange"),
        ))
    }

    fn check_settle(
        &self,
        envelope: &AgentExchangeEnvelope,
        result: &AgentExchangeResult,
    ) -> Result<(), AgentChoreographyError> {
        match envelope.kind() {
            AgentExchangeKind::ConversationTerminalNotice if !result.is_accepted() => {
                // A refused terminal notice settles only under the task's
                // definitive answers — a forged verdict, or a record too
                // full to ever grow the provenance cell — through the one
                // classifier both ends of the exchange share.
                // `task-not-created` stays outstanding, the
                // dependency-registration posture, so a notice racing its
                // task's creation converges on a later re-drive.
                match result.status().rejection_code() {
                    Some(code) if conversation_terminal_notice_refusal_settles(code) => Ok(()),
                    code => Err(AgentChoreographyError::UnsettleableRefusal {
                        kind: AgentExchangeKind::ConversationTerminalNotice,
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
        _result: &AgentExchangeResult,
        now: AgentTimestampMillis,
    ) -> Vec<AgentExchangeEnvelope> {
        if envelope.kind() == AgentExchangeKind::ConversationTerminalNotice {
            // The task answered — the provenance cell recorded, echoed, or
            // refused under a definitive code. The marker settles either
            // way: the durable once-guard that quiesces the owed derivation
            // past the journal's bounded window.
            settle_terminal_notice_exchange(state, envelope, now);
        }
        Vec::new()
    }
}

/// The unroutable placeholder a misaddressed host initializes under.
fn unroutable_conversation_id() -> AgentConversationId {
    AgentConversationId::new("unroutable").expect("the unroutable conversation id is well formed")
}

fn refuse(code: &str, message: String) -> AgentExchangeResult {
    AgentExchangeResult::rejected(
        code,
        message,
        AgentExchangePayload::empty(AGENT_CONVERSATION_RECEIPT_PAYLOAD_TYPE),
    )
}

/// The terminal notice the conversation owes its governing task, when it
/// owes one now ([specification 8.11](../../../docs/plans/rakka-agent/spec.md)).
///
/// Owed exactly once per conversation — the terminal flip is absorbing —
/// and re-derived by every settle pass until the exchange settles: the
/// journal's initiation record is the once-guard inside its bounded window,
/// and the conversation's `terminal_notice_settled` marker is the durable
/// once-guard past it. This is what makes a terminated conversation
/// observable from its governing task.
fn owed_terminal_notice(
    state: &AgentConversationState,
    now: AgentTimestampMillis,
) -> AgentConversationResult<Option<AgentExchangeEnvelope>> {
    let Some(conversation) = state.conversation.as_ref() else {
        return Ok(None);
    };
    if !conversation.status.is_terminal() || conversation.terminal_notice_settled {
        return Ok(None);
    }
    let operation_id = conversation_terminal_notice_operation_id(
        state.scope.tenant(),
        state.scope.conversation(),
    )?;
    if state.journal.has_initiated(&operation_id) {
        return Ok(None);
    }
    let Some(terminal_reason) = conversation.terminal_reason else {
        // A terminal status always recorded its reason; absent one there is
        // nothing coherent to report.
        return Ok(None);
    };
    let notice = AgentConversationTerminalNotice {
        conversation: state.scope.clone(),
        task: conversation.task.clone(),
        status: conversation.status,
        terminal_reason,
        round: conversation.round,
        turns_recorded: conversation.turns().len() as u64,
        ended_at: conversation.ended_at.unwrap_or(now),
    };
    let payload =
        AgentExchangePayload::encode(AGENT_CONVERSATION_TERMINAL_NOTICE_PAYLOAD_TYPE, &notice)?;
    let target = AgentTaskScope::new(state.scope.tenant().clone(), notice.task.clone())?;
    Ok(Some(AgentExchangeEnvelope::new(
        operation_id.clone(),
        AgentExchangeKind::ConversationTerminalNotice,
        AgentEntityAddress::Conversation(state.scope.clone()),
        AgentEntityAddress::Task(target),
        payload,
        AgentCorrelationId::new(operation_id.as_str()),
        now,
    )?))
}

/// Marks the terminal notice settled on the conversation: the durable
/// once-guard past the journal's bounded deduplication window.
fn settle_terminal_notice_exchange(
    state: &mut AgentConversationState,
    envelope: &AgentExchangeEnvelope,
    now: AgentTimestampMillis,
) {
    let owed =
        conversation_terminal_notice_operation_id(state.scope.tenant(), state.scope.conversation())
            .ok();
    if owed.as_ref() != Some(envelope.operation_id()) {
        return;
    }
    let Some(conversation) = state.conversation.as_mut() else {
        return;
    };
    if conversation.status.is_terminal() && !conversation.terminal_notice_settled {
        conversation.terminal_notice_settled = true;
        state.updated_at = now;
    }
}

/// One durable, deduplicated command over a conversation entity.
///
/// Every mutating variant carries the caller's stable operation id; a
/// replay answers [`AgentConversationEntityReply::Duplicate`] with the
/// original outcome — from the bounded operation log inside its window, and
/// from the dense turn ledger past it. The `Create` command is trusted
/// application wiring and has no A2A carrier.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum AgentConversationEntityCommand {
    /// Reads the bounded projection.
    Describe,
    /// Creates the conversation from trusted application data.
    Create {
        /// Stable dedup identity of this creation.
        operation_id: AgentOperationId,
        /// The trusted creation record.
        creation: Box<AgentConversationCreation>,
    },
    /// Submits the next turn, fenced on the claimed `(round, turn)`
    /// coordinate and the cursor's derived owner.
    SubmitTurn {
        /// Stable dedup identity of this turn, derived over the coordinate
        /// and the body digest by
        /// [`crate::coordination::conversation_turn_operation_id`].
        operation_id: AgentOperationId,
        /// The submission.
        submit: Box<AgentConversationTurnSubmit>,
    },
    /// Ends the conversation early under policy, fenced on the claimed
    /// moderator and the round.
    EndEarly {
        /// Stable dedup identity of this end decision, round-qualified by
        /// [`crate::coordination::conversation_end_operation_id`].
        operation_id: AgentOperationId,
        /// The agent claiming the end. A claim, like a turn's speaker: the
        /// transition fences it against the durable moderator, so only the
        /// moderator's end terminalizes the conversation.
        moderator: AgentId,
        /// The round this end decision was made against. An end decided
        /// against a round the conversation moved past fails closed — the
        /// round is the end decision's epoch.
        expected_round: u64,
        /// Bounded reason recorded in history.
        reason: String,
        /// Who accepted the end, and when.
        provenance: Box<AgentRevisionProvenance>,
    },
}

impl AgentConversationEntityCommand {
    /// The stable operation id of a mutating command.
    #[must_use]
    pub const fn operation_id(&self) -> Option<&AgentOperationId> {
        match self {
            Self::Describe => None,
            Self::Create { operation_id, .. }
            | Self::SubmitTurn { operation_id, .. }
            | Self::EndEarly { operation_id, .. } => Some(operation_id),
        }
    }

    /// The bounded operation label the moderation-turns counter records.
    #[must_use]
    pub const fn operation_label(&self) -> &'static str {
        match self {
            Self::Describe => "describe",
            Self::Create { .. } => "create",
            Self::SubmitTurn { .. } => "turn",
            Self::EndEarly { .. } => "end",
        }
    }
}

/// The reply of one conversation entity operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum AgentConversationEntityReply {
    /// The command applied.
    Applied {
        /// The transition's compact outcome.
        outcome: AgentConversationOutcome,
    },
    /// The command already ran; this is its original outcome — answered
    /// from the operation log inside the window, and from the turn ledger's
    /// echo past it.
    Duplicate {
        /// The recorded outcome.
        outcome: AgentConversationOutcome,
    },
    /// The bounded projection, `None` while the conversation is uncreated.
    Snapshot(Option<Box<AgentConversationSnapshot>>),
    /// What a settle pass accomplished.
    Progressed {
        /// The settle pass report.
        progress: AgentConversationProgress,
    },
    /// The command was refused under a stable code.
    Rejected {
        /// Stable machine-readable reason code.
        code: String,
        /// Human-readable detail.
        message: String,
    },
}

impl AgentConversationEntityReply {
    fn rejected(error: &AgentConversationError) -> Self {
        Self::Rejected {
            code: error.code().to_string(),
            message: error.to_string(),
        }
    }
}

/// What one conversation settle pass accomplished.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentConversationProgress {
    /// History entries durably flushed to the sink.
    pub history_flushed: usize,
    /// Whether a passed deadline was durably observed.
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

/// The actor message surface of the conversation entity.
#[derive(Debug)]
pub enum AgentConversationEntityMessage {
    /// One durable, deduplicated command.
    Command {
        /// The command.
        command: Box<AgentConversationEntityCommand>,
        /// Where the reply goes.
        reply_to: ReplyTo<AgentConversationEntityReply>,
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
        reply_to: ReplyTo<AgentConversationEntityReply>,
    },
}

/// The durable facade of one conversation scope.
///
/// Every decision lives here; the actor is a routing and recovery shell
/// over it, so the entity can passivate after any message.
pub struct AgentConversationEntityStore<Store, History>
where
    Store: DurableStateStore<AgentConversationState>,
    History: AgentConversationHistoryStore,
{
    scope: AgentConversationScope,
    host: AgentExchangeHost<AgentConversationParticipant, Store>,
    history: History,
    policy: AgentSchemaPolicy,
    metrics: Arc<dyn MetricsRecorder>,
    recovered: bool,
}

impl<Store, History> Debug for AgentConversationEntityStore<Store, History>
where
    Store: DurableStateStore<AgentConversationState>,
    History: AgentConversationHistoryStore,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentConversationEntityStore")
            .field("scope", &self.scope)
            .field("history", &self.history.backend_name())
            .field("recovered", &self.recovered)
            .finish_non_exhaustive()
    }
}

impl<Store, History> AgentConversationEntityStore<Store, History>
where
    Store: DurableStateStore<AgentConversationState>,
    History: AgentConversationHistoryStore,
{
    /// Creates a durable facade for one conversation scope.
    #[must_use]
    pub fn new(scope: AgentConversationScope, store: Store, history: History) -> Self {
        let host = AgentExchangeHost::new(
            AgentEntityAddress::Conversation(scope.clone()),
            AgentConversationParticipant,
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

    /// Wires a metrics recorder for the bounded moderation-turns counter
    /// this entity emits after its durable transitions commit.
    #[must_use]
    pub fn with_metrics(mut self, metrics: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics = metrics;
        self
    }

    /// The scope this facade addresses.
    #[must_use]
    pub const fn scope(&self) -> &AgentConversationScope {
        &self.scope
    }

    /// The durable persistence id of this conversation's state.
    #[must_use]
    pub fn persistence_id(&self) -> PersistenceId {
        self.scope.persistence_id()
    }

    /// Loads the conversation's durable state, failing closed on an
    /// unsupported schema version.
    pub async fn recover(
        &mut self,
        now: AgentTimestampMillis,
    ) -> AgentConversationResult<&AgentConversationState> {
        let state = self.host.recover(now).await?;
        self.recovered = true;
        Ok(state)
    }

    /// The currently recovered state.
    pub fn state(&self) -> AgentConversationResult<&AgentConversationState> {
        Ok(self.host.state()?)
    }

    /// The bounded projection of the conversation, once it has been
    /// created.
    pub fn snapshot(&self) -> AgentConversationResult<Option<AgentConversationSnapshot>> {
        Ok(self.state()?.snapshot())
    }

    /// Applies one command, then settles what it made possible locally: the
    /// history the transition owes.
    ///
    /// # Errors
    ///
    /// An error does not prove the command was not applied. Retrying with
    /// the same operation id is always safe: a command that committed
    /// answers [`AgentConversationEntityReply::Duplicate`] with its
    /// original outcome rather than transitioning twice.
    pub async fn apply(
        &mut self,
        command: AgentConversationEntityCommand,
        router: &AgentExchangeRouter,
        now: AgentTimestampMillis,
    ) -> AgentConversationResult<AgentConversationEntityReply> {
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
                // what makes a replayed turn converge on one ledger record.
                return Ok(AgentConversationEntityReply::Duplicate { outcome });
            }
        }

        if matches!(command, AgentConversationEntityCommand::Describe) {
            return Ok(AgentConversationEntityReply::Snapshot(
                self.snapshot()?.map(Box::new),
            ));
        }

        let operation = command.operation_label();
        // The past-window ledger echo is answered before every other guard,
        // including the history headroom one: it writes nothing at all, so a
        // redelivered turn must converge on the recorded turn even while the
        // history sink — pure observability — is unavailable. Refusing an
        // idempotent convergence with `conversation-history-backlog` would
        // leave the retrying caller unable to learn its turn had landed.
        if let AgentConversationEntityCommand::SubmitTurn { submit, .. } = &command {
            if let Some(echo) = probe_turn_echo(self.state()?, submit) {
                return match echo {
                    Ok(()) => Ok(AgentConversationEntityReply::Duplicate {
                        outcome: self.state()?.outcome(),
                    }),
                    Err(error) => {
                        if error.is_domain_refusal() {
                            self.count_operation(operation, "refused");
                        }
                        Err(error)
                    }
                };
            }
        }

        self.require_history_headroom(now).await?;

        let reply = self.apply_transition(command, now).await;
        match &reply {
            // A duplicate — the past-window ledger echo — made no durable
            // decision and counts nothing, so a replay never double-counts.
            Ok(AgentConversationEntityReply::Applied { .. }) => {
                self.count_operation(operation, "applied");
            }
            Ok(_) => {}
            Err(error) if error.is_domain_refusal() => {
                self.count_operation(operation, "refused");
            }
            Err(_) => {}
        }
        // The conversation owes no exchanges this slice; the router rides
        // along only so callers hold the same surface everywhere.
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
        command: AgentConversationEntityCommand,
        now: AgentTimestampMillis,
    ) -> AgentConversationResult<AgentConversationEntityReply> {
        match command {
            AgentConversationEntityCommand::Describe => unreachable!("handled by the caller"),
            AgentConversationEntityCommand::Create {
                operation_id,
                creation,
            } => {
                self.transition(now, move |state| {
                    create_conversation(state, &operation_id, *creation, now)?;
                    Ok(operation_id)
                })
                .await
            }
            AgentConversationEntityCommand::SubmitTurn {
                operation_id,
                submit,
            } => {
                // The ledger echo already answered in `apply`, ahead of
                // every other guard; `record_turn` re-probes so the pure
                // transition stays self-contained for a direct caller.
                self.transition(now, move |state| {
                    record_turn(state, &operation_id, &submit, now)?;
                    Ok(operation_id)
                })
                .await
            }
            AgentConversationEntityCommand::EndEarly {
                operation_id,
                moderator,
                expected_round,
                reason,
                provenance,
            } => {
                self.transition(now, move |state| {
                    end_early(
                        state,
                        &operation_id,
                        &moderator,
                        expected_round,
                        &provenance,
                        reason,
                        now,
                    )?;
                    Ok(operation_id)
                })
                .await
            }
        }
    }

    /// Accepts one delivered exchange and makes local progress only.
    ///
    /// The conversation refuses every exchange kind this slice; accepting
    /// still records the refusal through the host so a replayed delivery
    /// answers identically.
    pub async fn accept(
        &mut self,
        envelope: &AgentExchangeEnvelope,
        router: &AgentExchangeRouter,
        now: AgentTimestampMillis,
    ) -> AgentConversationResult<AgentExchangeReply> {
        self.ensure_recovered(now).await?;
        self.require_history_headroom(now).await?;
        let reply = self.host.accept(envelope, now).await?;
        let _ = router;
        self.flush_history(now).await?;
        Ok(reply)
    }

    /// Observes a passed deadline, re-owes an unsettled terminal notice,
    /// flushes owed history, and drives the exchanges the conversation
    /// owes.
    ///
    /// Safe to call at any time and from any node: every step reads what it
    /// needs from durable state.
    pub async fn settle_side_effects(
        &mut self,
        router: &AgentExchangeRouter,
        now: AgentTimestampMillis,
    ) -> AgentConversationResult<AgentConversationProgress> {
        self.ensure_recovered(now).await?;
        self.require_history_headroom(now).await?;
        let expiry_observed = self.observe_expiry(now).await?;
        self.settle_terminal_notice(now).await?;
        let flushed = self.flush_history(now).await?;
        let report = drive_pending_exchanges(&mut self.host, router, now).await?;
        // A drive settlement may have recorded history of its own.
        let flushed = flushed + self.flush_history(now).await?;
        record_unsettleable_exchanges(self.metrics.as_ref(), &report.unsettleable);
        Ok(AgentConversationProgress {
            history_flushed: flushed,
            expiry_observed,
            settled: report.settled,
            failed: report.failed,
            unsettleable: report.unsettleable.len(),
            outstanding: self.host.outstanding()?.len(),
        })
    }

    /// Re-owes the terminal notice a terminal conversation still owes its
    /// governing task ([specification 8.11](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// The courier half of the notice: the terminal transition owed it in
    /// its own compare-and-set, but a crash between that commit and the
    /// initiation — or a conversation that terminalized before the exchange
    /// existed — must not leave the task blind. The derivation is pure over
    /// durable state, the journal's initiation record guards the bounded
    /// window, and the `terminal_notice_settled` marker quiesces it past
    /// that window; a healthy sweep burns no revision.
    async fn settle_terminal_notice(
        &mut self,
        now: AgentTimestampMillis,
    ) -> AgentConversationResult<()> {
        let would_advance = {
            let state = self.state()?;
            state.conversation().is_some_and(|conversation| {
                conversation.status.is_terminal()
                    && !conversation.terminal_notice_settled
                    && conversation_terminal_notice_operation_id(
                        state.scope.tenant(),
                        state.scope.conversation(),
                    )
                    .is_ok_and(|operation| !state.journal.has_initiated(&operation))
            })
        };
        if !would_advance {
            return Ok(());
        }
        let mut rejection = None;
        let committed = self
            .host
            .initiate(now, |state| match owed_terminal_notice(state, now) {
                Ok(owed) => Ok(owed.into_iter().collect()),
                Err(error) => {
                    let carried = AgentChoreographyError::from(error.clone());
                    rejection = Some(error);
                    Err(carried)
                }
            })
            .await;
        if let Some(rejection) = rejection {
            return Err(rejection);
        }
        committed?;
        Ok(())
    }

    /// Durably flips an active conversation whose deadline has passed.
    ///
    /// The write is skipped entirely while nothing would flip, so a sweep
    /// over a healthy conversation burns no revision.
    async fn observe_expiry(&mut self, now: AgentTimestampMillis) -> AgentConversationResult<bool> {
        let would_expire = self.state()?.conversation().is_some_and(|conversation| {
            conversation.status == AgentConversationStatus::Active
                && conversation.is_expired_at(now)
        });
        if !would_expire {
            return Ok(false);
        }
        let operation_id =
            conversation_expiry_operation_id(self.scope.tenant(), self.scope.conversation())?;
        self.host
            .initiate(now, |state| {
                let Some(conversation) = state.conversation.as_mut() else {
                    return Ok(Vec::new());
                };
                if conversation.status != AgentConversationStatus::Active
                    || !conversation.is_expired_at(now)
                {
                    return Ok(Vec::new());
                }
                conversation.status = AgentConversationStatus::Expired;
                conversation.terminal_reason = Some(AgentConversationTerminalReason::Expired);
                conversation.ended_at = Some(now);
                state.record_history(|sequence| {
                    AgentConversationHistoryEntry::new(
                        sequence,
                        AgentConversationHistoryKind::Expired,
                        operation_id,
                        now,
                    )
                    .with_detail("expired")
                });
                state.updated_at = now;
                // The expiry flip is a terminal transition like any other:
                // the notice to the governing task commits with it.
                owed_terminal_notice(state, now)
                    .map(|owed| owed.into_iter().collect())
                    .map_err(AgentChoreographyError::from)
            })
            .await?;
        self.count_operation("expire", "applied");
        Ok(true)
    }

    async fn require_history_headroom(
        &mut self,
        now: AgentTimestampMillis,
    ) -> AgentConversationResult<()> {
        if self.state()?.history_headroom() >= AGENT_CONVERSATION_MAX_HISTORY_PER_TRANSITION {
            return Ok(());
        }
        self.flush_history(now).await?;
        let state = self.state()?;
        if state.history_headroom() >= AGENT_CONVERSATION_MAX_HISTORY_PER_TRANSITION {
            return Ok(());
        }
        Err(AgentConversationError::HistoryBacklog {
            pending: state.pending_history().len(),
            maximum: AGENT_CONVERSATION_PENDING_HISTORY_CAPACITY,
        })
    }

    async fn flush_history(&mut self, now: AgentTimestampMillis) -> AgentConversationResult<usize> {
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
    ) -> AgentConversationResult<AgentConversationEntityReply>
    where
        F: FnOnce(&mut AgentConversationState) -> AgentConversationResult<AgentOperationId>,
    {
        let mut outcome = None;
        let mut rejection = None;
        let committed = self
            .host
            .initiate(now, |state| {
                let step = |state: &mut AgentConversationState| -> AgentConversationResult<()> {
                    let operation_id = transition(state)?;
                    let result = state.outcome();
                    state
                        .applied_operations
                        .record(operation_id, result.clone());
                    state.updated_at = now;
                    outcome = Some(result);
                    Ok(())
                };
                match step(state) {
                    // A transition that terminalized the conversation owes
                    // the governing task its notice in this same
                    // compare-and-set; a non-terminal transition derives
                    // nothing. Strict propagation: a construction failure
                    // rejects the command whole, and the retry converges
                    // under the same operation id.
                    Ok(()) => match owed_terminal_notice(state, now) {
                        Ok(owed) => Ok(owed.into_iter().collect()),
                        Err(error) => {
                            let carried = AgentChoreographyError::from(error.clone());
                            rejection = Some(error);
                            Err(carried)
                        }
                    },
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
        Ok(AgentConversationEntityReply::Applied { outcome })
    }

    async fn ensure_recovered(&mut self, now: AgentTimestampMillis) -> AgentConversationResult<()> {
        // The host drops its cached record when a compare-and-set loses, and
        // a conversation has two writers by construction — the resident
        // sharded entity and the A2A service's own store. Asking the host
        // rather than trusting this facade's flag is what keeps a lost race
        // from wedging the entity for the rest of its residency (the task and
        // run stores hold the same rule).
        if !self.recovered || self.host.state().is_err() {
            self.recover(now).await?;
        }
        Ok(())
    }

    fn count_operation(&self, operation: &str, outcome: &str) {
        let _ = record_agent_domain_counter(
            self.metrics.as_ref(),
            METRIC_AGENT_MODERATION_TURNS,
            1,
            &[("operation", operation), ("outcome", outcome)],
        );
    }
}

/// What one turn body costs the serialized state: its JSON-escaped length,
/// without the surrounding quotes.
///
/// Plain text costs exactly what it measures; a body full of quotes or
/// control characters costs more, which is the number the state bound will
/// see and therefore the number `max_message_bytes` has to govern.
fn stored_body_bytes(body: &str) -> usize {
    serde_json::to_string(body)
        .map(|escaped| escaped.len().saturating_sub(2))
        .unwrap_or(usize::MAX)
}

/// The stored prefix of one turn's content digest — body *and* direction, so
/// the ledger echo fences a regenerated direction exactly as it fences
/// regenerated words.
fn digest_prefix(body: &str, direction: Option<&AgentConversationDirection>) -> String {
    conversation_turn_content_digest(body, direction)
        .value
        .chars()
        .take(AGENT_CONVERSATION_DIGEST_PREFIX_LENGTH)
        .collect()
}

/// Answers a submission naming a coordinate the cursor moved past, from the
/// dense ledger — the durable deduplication echo past the bounded
/// operation-log window.
///
/// `None` means the coordinate is not in the past and the full transition
/// decides. `Some(Ok(()))` is the idempotent echo of the recorded turn;
/// checked before every other guard, including the terminal one, because a
/// redelivered turn replaying after the conversation ended must converge on
/// the recorded turn, not be refused as if none was recorded (the
/// echo-before-every-guard rule).
fn probe_turn_echo(
    state: &AgentConversationState,
    submit: &AgentConversationTurnSubmit,
) -> Option<AgentConversationResult<()>> {
    let conversation = state.conversation.as_ref()?;
    if (submit.round, submit.turn) >= (conversation.round, conversation.turn_in_round) {
        return None;
    }
    let Some(record) = conversation
        .turns
        .iter()
        .find(|record| record.round == submit.round && record.turn == submit.turn)
    else {
        // The cursor moved past this coordinate without recording it — the
        // round closed before this turn index existed. The window this
        // submission speaks for is gone.
        return Some(Err(AgentConversationError::TurnSuperseded));
    };
    let recorded_speaker = conversation.speaker_id(record.speaker);
    if recorded_speaker != Some(&submit.participant) {
        return Some(Err(AgentConversationError::TurnSuperseded));
    }
    if record.digest_prefix != digest_prefix(&submit.body, submit.direction.as_ref()) {
        // The coordinate is occupied by this speaker's *other* content: a
        // regenerated submission after a crash, not a redelivery. Echoing
        // the recorded turn would silently persuade the speaker its new
        // content was recorded; refusing loudly is the only honest answer.
        // The direction is content too — a turn regenerated to designate
        // where the recorded one closed the round decides something else
        // entirely, however identical its words.
        return Some(Err(AgentConversationError::TurnContentMismatch));
    }
    Some(Ok(()))
}

fn create_conversation(
    state: &mut AgentConversationState,
    operation_id: &AgentOperationId,
    creation: AgentConversationCreation,
    now: AgentTimestampMillis,
) -> AgentConversationResult<()> {
    if state.conversation.is_some() {
        return Err(AgentConversationError::AlreadyCreated {
            scope: state.scope.clone(),
        });
    }
    if creation.participants.is_empty() {
        return Err(AgentConversationError::ParticipantsInvalid {
            reason: "the roster is empty".to_string(),
        });
    }
    let distinct: BTreeSet<&AgentId> = creation.participants.iter().collect();
    if distinct.len() != creation.participants.len() {
        return Err(AgentConversationError::ParticipantsInvalid {
            reason: "the roster repeats a participant".to_string(),
        });
    }
    let hard_cap = crate::coordination::AGENT_CONVERSATION_MAX_PARTICIPANTS as usize;
    if creation.participants.len() > hard_cap {
        return Err(AgentConversationError::ParticipantsInvalid {
            reason: format!("the roster exceeds the hard cap of {hard_cap} participants"),
        });
    }
    let turns_per_round = creation.policy.effective_max_turns_per_round() as usize;
    if creation.mode == AgentConversationMode::RoundRobin
        && creation.participants.len() > turns_per_round
    {
        // A round-robin round is one turn per roster member, so a roster
        // longer than the turn ceiling could never complete a round.
        return Err(AgentConversationError::ParticipantsInvalid {
            reason: format!(
                "a round-robin roster of {} exceeds the {turns_per_round}-turn round ceiling",
                creation.participants.len()
            ),
        });
    }
    // A conversation must have some road to a terminal state, or the
    // governing task waits on a signal that can never come. Under the
    // moderator-decides rule the round ceiling only *parks* the cursor, so
    // the early end is the sole exit — and forbidding it without a
    // wall-clock deadline leaves none at all. Refused at the door, beside
    // the other wedge guards, because there is no later moment at which the
    // configuration could become satisfiable.
    if creation.completion == AgentConversationCompletionRule::ModeratorDecides
        && !creation.policy.moderator_may_end_early
        && creation.max_wall_clock_millis.is_none()
    {
        return Err(AgentConversationError::CompletionUnreachable);
    }
    if let Some(reference) = &creation.transcript_ref {
        if reference.len() > AGENT_CONVERSATION_TRANSCRIPT_REF_MAX_BYTES {
            return Err(AgentConversationError::TranscriptRefInvalid {
                bytes: reference.len(),
                maximum: AGENT_CONVERSATION_TRANSCRIPT_REF_MAX_BYTES,
            });
        }
    }
    // A policy whose worst case cannot fit the state bound refuses at
    // creation: a mid-round state-bounds refusal would wedge the protocol
    // with the early end as its only exit, so the door is where the
    // arithmetic must hold. The in-flight bounds check stays a defensive
    // guard only — which is true only while this sum is a genuine upper
    // bound on everything `check_bounds` measures, so every bounded
    // collection in the persisted record is a term here.
    //
    // The ring is charged its *stored* cost: `max_message_bytes` bounds a
    // body's escaped length (`record_turn`), so the same number bounds what
    // the ring contributes to the serialized state.
    let ledger = creation.policy.effective_max_rounds() as usize
        * creation.policy.effective_max_turns_per_round() as usize
        * AGENT_CONVERSATION_TURN_RECORD_RESERVE_BYTES;
    let ring = creation.policy.effective_max_messages() as usize
        * (creation
            .policy
            .effective_max_message_bytes()
            .saturating_add(AGENT_CONVERSATION_MESSAGE_RECORD_RESERVE_BYTES));
    let operations = AGENT_CONVERSATION_OPERATION_LOG_CAPACITY
        * AGENT_CONVERSATION_OPERATION_LOG_ENTRY_RESERVE_BYTES;
    let outbox = AGENT_CONVERSATION_PENDING_HISTORY_CAPACITY
        * AGENT_CONVERSATION_HISTORY_ENTRY_RESERVE_BYTES;
    let worst_case = ledger + ring + operations + outbox + AGENT_CONVERSATION_FIXED_OVERHEAD_BYTES;
    let maximum = AGENT_CONVERSATION_MATERIALIZED_MAX_BYTES
        .saturating_sub(AGENT_CONVERSATION_STATE_GROWTH_RESERVE_BYTES);
    if worst_case > maximum {
        return Err(AgentConversationError::PolicyTooLarge {
            bytes: worst_case,
            maximum,
        });
    }
    let deadline = creation
        .max_wall_clock_millis
        .map(|ms| AgentTimestampMillis::new(now.as_millis().saturating_add(ms)));
    let participant_count = creation.participants.len();
    let conversation = AgentConversation {
        status: AgentConversationStatus::Active,
        terminal_reason: None,
        moderator: creation.moderator.clone(),
        mode: creation.mode,
        completion: creation.completion,
        policy: creation.policy,
        task: creation.task,
        round: 0,
        turn_in_round: 0,
        designated: None,
        participants: creation.participants,
        turns: Vec::new(),
        messages: VecDeque::new(),
        messages_dropped: 0,
        transcript_ref: creation.transcript_ref,
        budgets: AgentConversationBudgets {
            tokens: creation.tokens,
            deadline,
            consumed: AgentBudgetConsumption::zero(),
        },
        created_at: now,
        ended_at: None,
        terminal_notice_settled: false,
    };
    state.conversation = Some(conversation);
    state.check_bounds()?;
    let moderator = creation.moderator;
    let operation = operation_id.clone();
    state.record_history(|sequence| {
        AgentConversationHistoryEntry::new(
            sequence,
            AgentConversationHistoryKind::Created,
            operation,
            now,
        )
        .with_participant(moderator)
        .with_detail(format!("participants={participant_count}"))
    });
    Ok(())
}

/// Closes the current round: opens the next, or — when the final permitted
/// round completed under [`AgentConversationCompletionRule::AllRounds`] —
/// ends the conversation in the same compare-and-set. Completion beats
/// exhaustion.
///
/// Returns whether the conversation ended.
fn close_round(conversation: &mut AgentConversation, now: AgentTimestampMillis) -> bool {
    conversation.round += 1;
    conversation.turn_in_round = 0;
    conversation.designated = None;
    if conversation.round >= u64::from(conversation.policy.effective_max_rounds())
        && conversation.completion == AgentConversationCompletionRule::AllRounds
    {
        conversation.status = AgentConversationStatus::Ended;
        conversation.terminal_reason = Some(AgentConversationTerminalReason::RoundsComplete);
        conversation.ended_at = Some(now);
        return true;
    }
    false
}

fn record_turn(
    state: &mut AgentConversationState,
    operation_id: &AgentOperationId,
    submit: &AgentConversationTurnSubmit,
    now: AgentTimestampMillis,
) -> AgentConversationResult<()> {
    // The facade answers the ledger echo before initiating this transition;
    // re-probing keeps the pure transition self-contained for any caller
    // that reaches it directly.
    if let Some(echo) = probe_turn_echo(state, submit) {
        return echo;
    }
    state.require_active(now)?;
    let conversation = state.conversation_mut()?;
    if !conversation.is_authorized(&submit.participant) {
        return Err(AgentConversationError::NotParticipant {
            participant: submit.participant.clone(),
        });
    }
    if (submit.round, submit.turn) > (conversation.round, conversation.turn_in_round) {
        // Nothing is recorded for a future coordinate, so the eventual
        // in-order submission arrives untainted.
        return Err(AgentConversationError::TurnOutOfOrder {
            expected_round: conversation.round,
            expected_turn: conversation.turn_in_round,
        });
    }
    // The coordinate now equals the cursor. The protocol may still be over:
    // under the moderator-decides rule the cursor parks at the round
    // ceiling, and the refusal must not depend on who asks — so the ceiling
    // refuses before the owner fence.
    let max_rounds = u64::from(conversation.policy.effective_max_rounds());
    if conversation.round >= max_rounds {
        return Err(AgentConversationError::RoundsExhausted {
            maximum: max_rounds,
        });
    }
    // The turns-per-round ceiling is a ceiling on *records*, so it is
    // checked for every turn, not only the moderator's designating one —
    // enforcing it on one branch let a round record one more turn than the
    // policy declared, billing a turn the operator did not admit and
    // eroding the ledger reserve the creation arithmetic holds. Like the
    // round ceiling above, it refuses before the owner fence: the answer
    // does not depend on who asks.
    // The turns-per-round ceiling stated directly: it bounds *records*, so
    // it is checked for every turn rather than only the moderator's
    // designating one, which is what let a round record one turn more than
    // the policy declared — billing a turn the operator never admitted and
    // eroding the per-round ledger reserve the creation arithmetic holds.
    // The designation look-ahead below is what normally keeps a round
    // inside this bound; this is the bound itself, and like the round
    // ceiling above it refuses before the owner fence, because the answer
    // does not depend on who asks.
    let max_turns = conversation.policy.effective_max_turns_per_round();
    if conversation.turn_in_round >= max_turns {
        return Err(AgentConversationError::TurnsExhausted { maximum: max_turns });
    }
    let owner = conversation.turn_owner().cloned();
    if owner.as_ref() != Some(&submit.participant) {
        return Err(AgentConversationError::TurnNotOwner {
            participant: submit.participant.clone(),
        });
    }
    // Direction rules: a moderator-directed moderator turn must direct
    // what follows; every other turn must not.
    let moderator_turn = conversation.mode == AgentConversationMode::ModeratorDirected
        && conversation.turn_in_round % 2 == 0;
    let speaker = match conversation.mode {
        AgentConversationMode::RoundRobin => AgentConversationSpeaker::Participant(
            u8::try_from(conversation.turn_in_round)
                .expect("the turn ceiling keeps round-robin indexes within u8"),
        ),
        AgentConversationMode::ModeratorDirected if moderator_turn => {
            AgentConversationSpeaker::Moderator
        }
        AgentConversationMode::ModeratorDirected => {
            let index = conversation
                .participants
                .iter()
                .position(|participant| participant == &submit.participant)
                .expect("the owner fence admitted a roster participant");
            AgentConversationSpeaker::Participant(
                u8::try_from(index).expect("the roster cap keeps indexes within u8"),
            )
        }
    };
    match (&submit.direction, moderator_turn) {
        (None, true) => return Err(AgentConversationError::DirectionRequired),
        (Some(_), false) => return Err(AgentConversationError::DirectionForbidden),
        (Some(AgentConversationDirection::Designate(designated)), true) => {
            if !conversation.participants.contains(designated) {
                return Err(AgentConversationError::DesignateUnknown {
                    participant: designated.clone(),
                });
            }
            // A designation commits three slots: this turn, the designated
            // participant's, and the moderator's turn after it — which is
            // the only way a moderator-directed round ever closes. All three
            // must fit inside the ceiling, or the round would reach the rim
            // with no move left that the ceiling admits and no way to close.
            if conversation.turn_in_round.saturating_add(3) > max_turns {
                // The designated exchange would land past the rim; only
                // closing the round is accepted here.
                return Err(AgentConversationError::TurnsExhausted { maximum: max_turns });
            }
        }
        (Some(AgentConversationDirection::CloseRound), true) | (None, false) => {}
    }
    // Measured as *stored*, not as typed: the ring lives inside the
    // serialized state, where a quote doubles and a control character
    // expands sixfold. Charging the raw length here while the state bound
    // measures the escaped one is what let a policy pass the door and still
    // blow the bound mid-round. Plain text is unaffected — its escaped
    // length is its raw length.
    let max_bytes = conversation.policy.effective_max_message_bytes();
    let stored_bytes = stored_body_bytes(&submit.body);
    if stored_bytes > max_bytes {
        return Err(AgentConversationError::MessageTooLarge {
            bytes: stored_bytes,
            maximum: max_bytes,
        });
    }
    // The reported usage is the speaker's own claim about its own run, and
    // the grant it spends is shared — so it is bounded like every other wire
    // claim, before it can be added to anything. Overshooting what remains
    // stays legal below the ceiling; an implausible report is refused whole.
    if let Some(maximum) = conversation.policy.max_turn_tokens {
        if submit.usage.tokens > maximum {
            return Err(AgentConversationError::TurnUsageImplausible {
                reported: submit.usage.tokens,
                maximum,
            });
        }
    }
    if let Some(limit) = conversation.budgets.tokens {
        let consumed = conversation.budgets.consumed.tokens;
        if consumed >= limit {
            // Refuse, never park: the conversation stays active and the
            // moderator's early end — whose result rides the run-side
            // doors — is the application's move.
            return Err(AgentConversationError::BudgetExhausted(
                AgentBudgetExhaustion::new(AgentBudgetDimension::Tokens, limit, consumed),
            ));
        }
    }

    // Every guard passed: the ledger append, the ring append, the usage
    // record, and the cursor advance commit as one.
    let round = conversation.round;
    let turn = conversation.turn_in_round;
    conversation.turns.push(AgentConversationTurnRecord {
        round,
        turn,
        speaker,
        digest_prefix: digest_prefix(&submit.body, submit.direction.as_ref()),
        at: now,
    });
    conversation.messages.push_back(AgentConversationMessage {
        round,
        turn,
        speaker: submit.participant.clone(),
        body: submit.body.clone(),
        at: now,
    });
    let ring = conversation.policy.effective_max_messages() as usize;
    while conversation.messages.len() > ring {
        conversation.messages.pop_front();
        conversation.messages_dropped += 1;
    }
    // Recorded even when it overshoots — the spend already happened in the
    // speaker's run; the next turn's gate is where exhaustion bites.
    conversation.budgets.consumed = conversation.budgets.consumed.saturating_add(&submit.usage);

    let mut round_closed = false;
    let mut ended = false;
    match conversation.mode {
        AgentConversationMode::RoundRobin => {
            let last_turn =
                conversation.turn_in_round as usize + 1 >= conversation.participants.len();
            if last_turn {
                round_closed = true;
                ended = close_round(conversation, now);
            } else {
                conversation.turn_in_round += 1;
            }
        }
        AgentConversationMode::ModeratorDirected => match &submit.direction {
            Some(AgentConversationDirection::Designate(designated)) => {
                conversation.designated = Some(designated.clone());
                conversation.turn_in_round += 1;
            }
            Some(AgentConversationDirection::CloseRound) => {
                round_closed = true;
                ended = close_round(conversation, now);
            }
            None => {
                // A participant turn returns ownership to the moderator.
                conversation.designated = None;
                conversation.turn_in_round += 1;
            }
        },
    }
    let participant = submit.participant.clone();
    let operation = operation_id.clone();
    // The spend is attributed: the shared grant is drawn down by a named
    // speaker's own claim, so the audit trail records who reported what
    // rather than leaving an exhausted conversation with an anonymous
    // total. Counts and coordinates only — never the body.
    let reported = submit.usage.tokens;
    state.record_history(|sequence| {
        AgentConversationHistoryEntry::new(
            sequence,
            AgentConversationHistoryKind::TurnRecorded,
            operation,
            now,
        )
        .with_participant(participant)
        .with_coordinate(round, turn)
        .with_detail(format!("tokens={reported}"))
    });
    if round_closed {
        let operation = operation_id.clone();
        state.record_history(|sequence| {
            AgentConversationHistoryEntry::new(
                sequence,
                AgentConversationHistoryKind::RoundAdvanced,
                operation,
                now,
            )
            .with_detail(format!("round={}", round + 1))
        });
    }
    if ended {
        let operation = operation_id.clone();
        state.record_history(|sequence| {
            AgentConversationHistoryEntry::new(
                sequence,
                AgentConversationHistoryKind::Ended,
                operation,
                now,
            )
            .with_detail(AgentConversationTerminalReason::RoundsComplete.code())
        });
    }
    // Measured last, over everything this transition wrote — the ledger
    // record and ring append above *and* the history entries it just owed
    // the outbox, all of which the same compare-and-set persists.
    state.check_bounds()
}

fn end_early(
    state: &mut AgentConversationState,
    operation_id: &AgentOperationId,
    moderator: &AgentId,
    expected_round: u64,
    provenance: &AgentRevisionProvenance,
    reason: String,
    now: AgentTimestampMillis,
) -> AgentConversationResult<()> {
    state.require_active(now)?;
    let conversation = state.conversation_mut()?;
    // The reason is caller-supplied free text riding a durable append, so
    // it is bounded like every other caller-supplied field — at the ceiling
    // the constant has always advertised, rather than being silently
    // truncated at the generic detail bound to twice it.
    if reason.len() > AGENT_CONVERSATION_REASON_MAX_BYTES {
        return Err(AgentConversationError::ReasonTooLarge {
            bytes: reason.len(),
            maximum: AGENT_CONVERSATION_REASON_MAX_BYTES,
        });
    }
    // The policy refusal comes first because it does not depend on who asks
    // — the same discipline that puts the round ceiling ahead of the turn
    // owner fence. Then the identity fence: the spec grants the early end to
    // the moderator alone, so a roster participant's end refuses here rather
    // than terminalizing a conversation it only speaks in.
    if !conversation.policy.moderator_may_end_early {
        return Err(AgentConversationError::EndNotPermitted);
    }
    if &conversation.moderator != moderator {
        return Err(AgentConversationError::EndNotModerator {
            participant: moderator.clone(),
        });
    }
    if conversation.round != expected_round {
        // An end decided against a round the conversation moved past must
        // not clip a conversation that moved on — the round is the end
        // decision's epoch.
        return Err(AgentConversationError::EndStaleRound {
            expected: expected_round,
            actual: conversation.round,
        });
    }
    conversation.status = AgentConversationStatus::Ended;
    conversation.terminal_reason = Some(AgentConversationTerminalReason::ModeratorEnded);
    conversation.ended_at = Some(now);
    // The one terminalizing operation a caller can reach records who
    // accepted it: the principal in its own field, the moderator as the
    // participant, the round it was decided against, and the caller's
    // reason separately from the stable terminal code. Nothing about the
    // decision has to be inferred from an overloaded detail string.
    let operation = operation_id.clone();
    let round = conversation.round;
    let moderator = moderator.clone();
    state.record_history(|sequence| {
        AgentConversationHistoryEntry::new(
            sequence,
            AgentConversationHistoryKind::Ended,
            operation,
            now,
        )
        .with_participant(moderator)
        .with_round(round)
        .with_detail(AgentConversationTerminalReason::ModeratorEnded.code())
        .with_provenance(provenance, &reason)
    });
    Ok(())
}

/// The sharded conversation entity actor: a routing and recovery shell over
/// [`AgentConversationEntityStore`].
pub struct AgentConversationEntity<Store, History>
where
    Store: DurableStateStore<AgentConversationState>,
    History: AgentConversationHistoryStore,
{
    entity: Result<AgentConversationEntityStore<Store, History>, AgentIdentityError>,
    router: AgentExchangeRouter,
    clock: AgentConversationClock,
}

impl<Store, History> AgentConversationEntity<Store, History>
where
    Store: DurableStateStore<AgentConversationState>,
    History: AgentConversationHistoryStore,
{
    /// Creates an entity for one sharded entity id.
    #[must_use]
    pub fn new(
        entity_id: &EntityId,
        store: Store,
        history: History,
        router: AgentExchangeRouter,
        clock: AgentConversationClock,
        policy: AgentSchemaPolicy,
    ) -> Self {
        let entity = AgentConversationScope::from_entity_id(entity_id).map(|scope| {
            AgentConversationEntityStore::new(scope, store, history).with_schema_policy(policy)
        });
        Self {
            entity,
            router,
            clock,
        }
    }

    /// Wires a metrics recorder for the hosted entity's bounded
    /// moderation-turns counter.
    #[must_use]
    pub fn with_metrics(mut self, metrics: Arc<dyn MetricsRecorder>) -> Self {
        self.entity = self.entity.map(|store| store.with_metrics(metrics));
        self
    }

    fn store(
        &mut self,
    ) -> Result<&mut AgentConversationEntityStore<Store, History>, AgentConversationError> {
        self.entity
            .as_mut()
            .map_err(|error| AgentConversationError::Identity(error.clone()))
    }
}

impl<Store, History> Actor for AgentConversationEntity<Store, History>
where
    Store: DurableStateStore<AgentConversationState>,
    History: AgentConversationHistoryStore,
{
    type Msg = AgentConversationEntityMessage;

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
                AgentConversationEntityMessage::Command { command, reply_to } => {
                    let reply = match self.store() {
                        Err(error) => AgentConversationEntityReply::rejected(&error),
                        Ok(entity) => match entity.apply(*command, &router, now).await {
                            Ok(reply) => reply,
                            Err(error) => AgentConversationEntityReply::rejected(&error),
                        },
                    };
                    let _reply_dropped = reply_to.reply(reply);
                }
                AgentConversationEntityMessage::Exchange { envelope, reply_to } => {
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
                AgentConversationEntityMessage::Settle { reply_to } => {
                    let reply = match self.store() {
                        Err(error) => AgentConversationEntityReply::rejected(&error),
                        Ok(entity) => match entity.settle_side_effects(&router, now).await {
                            Ok(progress) => AgentConversationEntityReply::Progressed { progress },
                            Err(error) => AgentConversationEntityReply::rejected(&error),
                        },
                    };
                    let _reply_dropped = reply_to.reply(reply);
                }
            }
            Ok(ActorAction::Continue)
        })
    }
}

/// The entity type key of the conversation entity.
pub type AgentConversationEntityTypeKey = EntityTypeKey<AgentConversationEntityMessage>;

/// The registration returned after initializing sharded conversation
/// entities.
pub type AgentConversationEntityRegistration =
    EntityTypeRegistration<AgentConversationEntityMessage>;

/// A sharded reference to one conversation entity.
pub type AgentConversationEntityRef = ShardedEntityRef<AgentConversationEntityMessage>;

/// The sharding settings of conversation entities.
#[derive(Clone)]
pub struct AgentConversationEntityShardingSettings {
    key: AgentConversationEntityTypeKey,
    actor_options: ActorOptions,
    idle_passivation_timeout: Option<Duration>,
    buffer_config: Option<ShardBufferConfig>,
    passivation_buffer_duration: Duration,
    schema_policy: AgentSchemaPolicy,
    clock: AgentConversationClock,
    metrics: Arc<dyn MetricsRecorder>,
}

impl Debug for AgentConversationEntityShardingSettings {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentConversationEntityShardingSettings")
            .field("entity_type", self.key.entity_type())
            .field("number_of_shards", &self.key.config().number_of_shards())
            .field("idle_passivation_timeout", &self.idle_passivation_timeout)
            .field("schema_policy", &self.schema_policy)
            .finish_non_exhaustive()
    }
}

impl AgentConversationEntityShardingSettings {
    /// Creates settings from an explicit entity type key.
    #[must_use]
    pub fn new(key: AgentConversationEntityTypeKey) -> Self {
        Self {
            key,
            actor_options: ActorOptions::default(),
            idle_passivation_timeout: None,
            buffer_config: Some(ShardBufferConfig::default()),
            passivation_buffer_duration: DEFAULT_AGENT_CONVERSATION_PASSIVATION_BUFFER_DURATION,
            schema_policy: AgentSchemaPolicy::default(),
            clock: system_conversation_clock(),
            metrics: Arc::new(NoopMetricsRecorder),
        }
    }

    /// The entity type key used for conversation entities.
    #[must_use]
    pub const fn key(&self) -> &AgentConversationEntityTypeKey {
        &self.key
    }

    /// Wires a metrics recorder for every hosted entity's bounded
    /// moderation-turns counter.
    #[must_use]
    pub fn with_metrics(mut self, metrics: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics = metrics;
        self
    }

    /// Uses an explicit clock for the timestamps hosted entities persist.
    #[must_use]
    pub fn with_clock(mut self, clock: AgentConversationClock) -> Self {
        self.clock = clock;
        self
    }

    /// Sets the options used when each conversation entity actor is
    /// spawned.
    #[must_use]
    pub fn with_actor_options(mut self, actor_options: ActorOptions) -> Self {
        self.actor_options = actor_options;
        self
    }

    /// Enables idle passivation for quiescent conversation entities.
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

impl Default for AgentConversationEntityShardingSettings {
    fn default() -> Self {
        Self::new(agent_conversation_entity_type_key())
    }
}

/// Creates the default sharded entity type key for conversation entities.
#[must_use]
pub fn agent_conversation_entity_type_key() -> AgentConversationEntityTypeKey {
    EntityTypeKey::new(DEFAULT_AGENT_CONVERSATION_ENTITY_TYPE)
}

/// Maps a conversation scope to its sharded entity id.
#[must_use]
pub fn agent_conversation_entity_id(scope: &AgentConversationScope) -> EntityId {
    scope.entity_id()
}

/// The durable persistence id of one conversation entity's state.
#[must_use]
pub fn agent_conversation_entity_persistence_id(scope: &AgentConversationScope) -> PersistenceId {
    scope.persistence_id()
}

/// Initializes node-local sharded conversation entities.
pub fn init_agent_conversation_entity_sharding<Store, History>(
    sharding: &ClusterSharding,
    store: Store,
    history: History,
    router: AgentExchangeRouter,
    settings: AgentConversationEntityShardingSettings,
) -> ClusterShardingResult<AgentConversationEntityRegistration>
where
    Store: DurableStateStore<AgentConversationState>,
    History: AgentConversationHistoryStore,
{
    sharding.init(agent_conversation_entity(store, history, router, &settings))
}

/// Initializes sharded conversation entities that a non-owning node can
/// reach over `rakka-remote`.
///
/// The remote ask surface is the [`AgentExchangeEnvelope`], exactly as for
/// the task, run, and team entities; the application registers the exchange
/// codecs through [`crate::choreography::register_agent_exchange_codecs`].
pub fn init_agent_conversation_entity_remote_sharding<Store, History>(
    sharding: &ClusterSharding,
    runtime: &mut ClusterNodeRuntime,
    store: Store,
    history: History,
    router: AgentExchangeRouter,
    settings: AgentConversationEntityShardingSettings,
) -> ClusterNodeRuntimeResult<AgentConversationEntityRegistration>
where
    Store: DurableStateStore<AgentConversationState>,
    History: AgentConversationHistoryStore,
{
    let entity = agent_conversation_entity(store, history, router, &settings);
    sharding.init_remote_with_ask(
        runtime,
        entity,
        |envelope: AgentExchangeEnvelope, reply_to: ReplyTo<AgentExchangeReply>| {
            AgentConversationEntityMessage::Exchange {
                envelope: Box::new(envelope),
                reply_to,
            }
        },
    )
}

// The conversation entity is generic over its two stores, so the entity
// type it builds is unavoidably wide.
#[allow(clippy::type_complexity)]
fn agent_conversation_entity<Store, History>(
    store: Store,
    history: History,
    router: AgentExchangeRouter,
    settings: &AgentConversationEntityShardingSettings,
) -> Entity<
    AgentConversationEntityMessage,
    AgentConversationEntity<Store, History>,
    impl Fn(EntityContext<AgentConversationEntityMessage>) -> AgentConversationEntity<Store, History>
        + Send
        + Sync
        + 'static,
>
where
    Store: DurableStateStore<AgentConversationState>,
    History: AgentConversationHistoryStore,
{
    let schema_policy = settings.schema_policy;
    let clock = settings.clock.clone();
    let metrics = settings.metrics.clone();
    let mut entity = Entity::of(settings.key.clone(), move |context: EntityContext<_>| {
        AgentConversationEntity::new(
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

/// Returns a sharded reference to one conversation entity.
pub fn agent_conversation_entity_ref(
    sharding: &ClusterSharding,
    key: &AgentConversationEntityTypeKey,
    scope: &AgentConversationScope,
) -> ClusterShardingResult<AgentConversationEntityRef> {
    sharding.entity_ref_for(key, scope.key())
}

/// Returns a sharded reference to one conversation entity from a
/// registration.
#[must_use]
pub fn registered_agent_conversation_entity_ref(
    registration: &AgentConversationEntityRegistration,
    scope: &AgentConversationScope,
) -> AgentConversationEntityRef {
    registration.entity_ref_for(scope.key())
}

/// Explicitly passivates one local conversation entity.
pub fn passivate_agent_conversation_entity(
    sharding: &ClusterSharding,
    key: &AgentConversationEntityTypeKey,
    scope: &AgentConversationScope,
) -> ClusterShardingResult<bool> {
    sharding.passivate_entity_id(key, &scope.entity_id())
}

/// The rejection of a conversation entity operation.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum AgentConversationError {
    /// An identifier or scope key was malformed.
    Identity(AgentIdentityError),
    /// A persisted record carried an unsupported schema version.
    Schema(AgentSchemaError),
    /// The choreography substrate rejected an exchange.
    Choreography(Box<AgentChoreographyError>),
    /// A coordination derivation failed.
    Coordination(Box<AgentCoordinationError>),
    /// No conversation exists under this scope.
    NotCreated {
        /// The addressed scope.
        scope: AgentConversationScope,
    },
    /// A conversation already exists under this scope.
    AlreadyCreated {
        /// The addressed scope.
        scope: AgentConversationScope,
    },
    /// The conversation ended.
    Ended,
    /// The conversation's creation-fixed deadline has passed.
    Expired,
    /// The creation roster is empty, repeats a participant, or exceeds its
    /// ceiling.
    ParticipantsInvalid {
        /// What the roster violated.
        reason: String,
    },
    /// The transcript artifact reference exceeds its bounded ceiling.
    TranscriptRefInvalid {
        /// The reference size.
        bytes: usize,
        /// The ceiling.
        maximum: usize,
    },
    /// The policy's maxed-out ledger and ring could not fit the state
    /// bound; a conversation under it could wedge mid-round.
    PolicyTooLarge {
        /// The worst-case size the arithmetic reached.
        bytes: usize,
        /// The effective maximum.
        maximum: usize,
    },
    /// The claimed speaker is neither the moderator nor on the roster.
    NotParticipant {
        /// The claimed speaker.
        participant: AgentId,
    },
    /// The claimed coordinate is ahead of the cursor.
    TurnOutOfOrder {
        /// The round the cursor expects.
        expected_round: u64,
        /// The turn index the cursor expects.
        expected_turn: u32,
    },
    /// The claimed speaker does not own the cursor's turn.
    TurnNotOwner {
        /// The claimed speaker.
        participant: AgentId,
    },
    /// The recorded turn at this coordinate carries this speaker's *other*
    /// content: a regenerated submission, not a redelivery.
    TurnContentMismatch,
    /// The cursor moved past this coordinate under a different decision.
    TurnSuperseded,
    /// A moderator-directed moderator turn carried no direction.
    DirectionRequired,
    /// A direction rode a turn that may not carry one.
    DirectionForbidden,
    /// The designated next speaker is not on the roster.
    DesignateUnknown {
        /// The designated agent.
        participant: AgentId,
    },
    /// The round ceiling is reached; under the moderator-decides rule the
    /// early end is the moderator's move.
    RoundsExhausted {
        /// The effective ceiling.
        maximum: u64,
    },
    /// The turns-per-round ceiling is reached; only closing the round is
    /// accepted.
    TurnsExhausted {
        /// The effective ceiling.
        maximum: u32,
    },
    /// The turn body exceeds the bounded ceiling.
    MessageTooLarge {
        /// The body size.
        bytes: usize,
        /// The effective ceiling.
        maximum: usize,
    },
    /// The turn reports more token usage than the policy admits for one
    /// turn.
    TurnUsageImplausible {
        /// The reported usage.
        reported: u64,
        /// The per-turn ceiling.
        maximum: u64,
    },
    /// A creation-fixed budget is exhausted; the turn is refused, nothing
    /// parks.
    BudgetExhausted(AgentBudgetExhaustion),
    /// The early-end reason exceeds its bounded ceiling.
    ReasonTooLarge {
        /// The reason's size.
        bytes: usize,
        /// The effective ceiling.
        maximum: usize,
    },
    /// The completion rule and the early-end policy leave no reachable
    /// terminal state.
    CompletionUnreachable,
    /// The policy forbids the moderator from ending early.
    EndNotPermitted,
    /// The agent claiming the early end is not the conversation's moderator.
    EndNotModerator {
        /// The claimed moderator.
        participant: AgentId,
    },
    /// An end decision was made against a round the conversation moved
    /// past.
    EndStaleRound {
        /// The round the end decision was made against.
        expected: u64,
        /// The round in force.
        actual: u64,
    },
    /// A history append found a different entry at the sequence.
    HistoryConflict {
        /// The conflicting sequence.
        sequence: AgentConversationHistorySequence,
    },
    /// The read cursor precedes the history the backend still retains, so
    /// resuming from it would silently skip entries
    /// ([specification 17.13](../../docs/plans/rakka-agent/spec.md)). The reader
    /// resynchronizes from authoritative state and resumes at the floor.
    HistoryWindowExpired {
        /// The floor to resume from: the oldest sequence still retained at or
        /// past the reader's position, when anything is retained there.
        oldest_retained: Option<AgentConversationHistorySequence>,
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

impl AgentConversationError {
    /// Stable machine-readable code.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::Identity(_) => "conversation-identity",
            Self::Schema(error) => error.code(),
            Self::Choreography(error) => error.code(),
            Self::Coordination(error) => error.code(),
            Self::NotCreated { .. } => "conversation-not-found",
            Self::AlreadyCreated { .. } => "conversation-already-created",
            Self::Ended => "conversation-ended",
            Self::Expired => "conversation-expired",
            Self::ParticipantsInvalid { .. } => "conversation-participants-invalid",
            Self::TranscriptRefInvalid { .. } => "conversation-transcript-ref-invalid",
            Self::PolicyTooLarge { .. } => "conversation-policy-too-large",
            Self::NotParticipant { .. } => "conversation-not-participant",
            Self::TurnOutOfOrder { .. } => "conversation-turn-out-of-order",
            Self::TurnNotOwner { .. } => "conversation-not-your-turn",
            Self::TurnContentMismatch => "conversation-turn-content-mismatch",
            Self::TurnSuperseded => "conversation-turn-superseded",
            Self::DirectionRequired => "conversation-direction-required",
            Self::DirectionForbidden => "conversation-direction-forbidden",
            Self::DesignateUnknown { .. } => "conversation-designate-unknown",
            Self::RoundsExhausted { .. } => "conversation-rounds-exhausted",
            Self::TurnsExhausted { .. } => "conversation-turns-exhausted",
            Self::MessageTooLarge { .. } => "conversation-message-too-large",
            Self::TurnUsageImplausible { .. } => "conversation-turn-usage-too-large",
            Self::BudgetExhausted(exhaustion) => exhaustion.code(),
            Self::ReasonTooLarge { .. } => "conversation-reason-too-large",
            Self::CompletionUnreachable => "conversation-completion-unreachable",
            Self::EndNotPermitted => "conversation-end-not-permitted",
            Self::EndNotModerator { .. } => "conversation-end-not-moderator",
            Self::EndStaleRound { .. } => "conversation-end-stale-round",
            Self::HistoryConflict { .. } => "conversation-history-conflict",
            Self::HistoryWindowExpired { .. } => "conversation-history-window-expired",
            Self::HistoryBacklog { .. } => "conversation-history-backlog",
            Self::StateBounds { .. } => "conversation-state-too-large",
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

impl Display for AgentConversationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Identity(error) => Display::fmt(error, f),
            Self::Schema(error) => Display::fmt(error, f),
            Self::Choreography(error) => Display::fmt(error, f),
            Self::Coordination(error) => Display::fmt(error, f),
            Self::NotCreated { scope } => {
                write!(f, "no conversation exists under scope {scope}")
            }
            Self::AlreadyCreated { scope } => {
                write!(f, "a conversation already exists under scope {scope}")
            }
            Self::Ended => f.write_str("the conversation ended"),
            Self::Expired => f.write_str("the conversation's creation-fixed deadline has passed"),
            Self::ParticipantsInvalid { reason } => {
                write!(f, "the participant roster is invalid: {reason}")
            }
            Self::TranscriptRefInvalid { bytes, maximum } => write!(
                f,
                "the transcript reference is {bytes} bytes; the ceiling is {maximum}"
            ),
            Self::PolicyTooLarge { bytes, maximum } => write!(
                f,
                "the policy's worst case is {bytes} bytes against a {maximum}-byte state bound"
            ),
            Self::NotParticipant { participant } => write!(
                f,
                "{participant} is neither the moderator nor a roster participant"
            ),
            Self::TurnOutOfOrder {
                expected_round,
                expected_turn,
            } => write!(
                f,
                "the next expected turn is round {expected_round} turn {expected_turn}"
            ),
            Self::TurnNotOwner { participant } => {
                write!(f, "{participant} does not own the current turn")
            }
            Self::TurnContentMismatch => f.write_str(
                "a different body is recorded at this coordinate; the submission was regenerated, \
                 not redelivered",
            ),
            Self::TurnSuperseded => f.write_str(
                "the conversation moved past this coordinate under a different decision",
            ),
            Self::DirectionRequired => {
                f.write_str("a moderator-directed moderator turn must carry a direction")
            }
            Self::DirectionForbidden => f.write_str("this turn may not carry a direction"),
            Self::DesignateUnknown { participant } => {
                write!(f, "{participant} is not on the roster")
            }
            Self::RoundsExhausted { maximum } => {
                write!(
                    f,
                    "the conversation completed its maximum of {maximum} rounds"
                )
            }
            Self::TurnsExhausted { maximum } => write!(
                f,
                "the round holds its maximum of {maximum} turns; only closing the round is accepted"
            ),
            Self::MessageTooLarge { bytes, maximum } => write!(
                f,
                "the turn body is {bytes} bytes; the ceiling is {maximum}"
            ),
            Self::TurnUsageImplausible { reported, maximum } => write!(
                f,
                "the turn reports {reported} tokens; one turn may report at most {maximum}"
            ),
            Self::BudgetExhausted(exhaustion) => Display::fmt(exhaustion, f),
            Self::ReasonTooLarge { bytes, maximum } => write!(
                f,
                "the early-end reason is {bytes} bytes; the ceiling is {maximum}"
            ),
            Self::CompletionUnreachable => f.write_str(
                "a moderator-decides conversation that forbids the early end and sets no \
                 wall-clock deadline can never reach a terminal state",
            ),
            Self::EndNotPermitted => {
                f.write_str("the policy forbids the moderator from ending early")
            }
            Self::EndNotModerator { participant } => write!(
                f,
                "{participant} is not the conversation's moderator and cannot end it early"
            ),
            Self::EndStaleRound { expected, actual } => write!(
                f,
                "the end decision was made against round {expected} but round {actual} is in force"
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
                "the materialized conversation is {bytes} bytes; the bound is {maximum}"
            ),
        }
    }
}

impl Error for AgentConversationError {}

impl From<AgentIdentityError> for AgentConversationError {
    fn from(error: AgentIdentityError) -> Self {
        Self::Identity(error)
    }
}

impl From<AgentSchemaError> for AgentConversationError {
    fn from(error: AgentSchemaError) -> Self {
        Self::Schema(error)
    }
}

impl From<AgentChoreographyError> for AgentConversationError {
    fn from(error: AgentChoreographyError) -> Self {
        Self::Choreography(Box::new(error))
    }
}

impl From<AgentCoordinationError> for AgentConversationError {
    fn from(error: AgentCoordinationError) -> Self {
        Self::Coordination(Box::new(error))
    }
}

impl From<AgentConversationError> for AgentChoreographyError {
    fn from(error: AgentConversationError) -> Self {
        match error {
            AgentConversationError::Identity(error) => Self::Identity(error),
            AgentConversationError::Schema(error) => Self::Schema(error),
            AgentConversationError::Choreography(error) => *error,
            other => Self::PayloadEncoding {
                message: other.to_string(),
            },
        }
    }
}
