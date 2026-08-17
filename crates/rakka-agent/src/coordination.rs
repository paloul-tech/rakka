//! Coordination capabilities: handoff, teams, and moderation.
//!
//! Owns `AgentCoordinationCapability` descriptors, which are trusted definition
//! and setup data. The runtime may expose a capability to the model as a tool,
//! but model output can never create the capability, its target, its budget, or
//! its scope.
//!
//! Handoff keeps the same `AgentTaskId`: the source run is fenced, a target run
//! is created, context and artifacts are projected explicitly rather than
//! inherited, and `HandedOff` is recorded only after the target durably
//! accepts. The handoff reuses the delegation idioms wholesale: the
//! [`AgentHandoffRecord`] persists in the same compare-and-set that commits
//! the outbound send effect, strictly before any dispatch; its identity is a
//! pure derivation of the source run's `(turn, slot)` coordinate that doubles
//! as the A2A message id and deduplication key; and the target resolves once,
//! inside that compare-and-set, through the application-owned
//! [`crate::delegation::AgentDelegationCatalog`] — replays reuse the recorded
//! resolution verbatim (open decision 6's disposition). Unlike delegation, a
//! handoff never creates a child task: ingress drives a new assignment
//! generation on the *same* task, so the transfer debits no descendant and
//! opens no fan-in membership.
//!
//! Team coordination owns [`crate::identity::AgentTeamId`], bounded
//! membership, and a durable shared task board whose claims, releases, and
//! transfers are atomic under revision and lease fencing. The
//! [`AgentTeamPolicy`] payload carries the board's bounded ceilings and the
//! claim-lease duration; the board itself is the team entity's durable state
//! ([`crate::team`]). Moderation owns
//! [`crate::identity::AgentConversationId`], the authorized participant set,
//! durable turn and round state, and the bounded transcript, where only the
//! current participant may submit and duplicates are rejected. The
//! [`AgentModerationPolicy`] payload carries the turn protocol's bounded
//! ceilings; the protocol itself is the conversation entity's durable state
//! ([`crate::conversation`]).
//!
//! Every one of these exchanges travels the outbox, inbox, and `rakka-a2a` path
//! even when the participants are colocated, and idle teams, boards, and
//! participants passivate — the board is data, not a resident process.
//!
//! Specification: sections 8.8 through 8.11.

use std::error::Error;
use std::fmt::{self, Display, Formatter};

use rakka_agent_workflow::{AgentEffectId, AgentTelemetryContext, AgentTimestampMillis};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::definition::{
    AgentCapabilityId, AgentCoordinationCapabilityKind, AgentRevisionNumber, AgentToolId,
};
use crate::delegation::AgentDelegationTarget;
use crate::identity::{
    validate_tenant, AgentConversationId, AgentGoalId, AgentHandoffId, AgentId, AgentIdentityError,
    AgentOperationId, AgentOperationKind, AgentRunId, AgentRunScope, AgentTaskId, AgentTaskScope,
    AgentTeamClaimId, AgentTeamScope, TenantId,
};
use crate::model::AgentToolCallId;
use crate::task::{AgentAssignmentGeneration, AgentContentDigest, AgentTaskStatus};

/// Result type for coordination construction and validation.
pub type AgentCoordinationResult<T> = Result<T, AgentCoordinationError>;

/// Prefix of every derived [`AgentHandoffId`].
///
/// The suffix is a fixed-length digest, so the id always satisfies the
/// identity bounds whatever the source scope contains, and an id without this
/// prefix was not derived by [`handoff_id_for`].
pub const AGENT_HANDOFF_ID_PREFIX: &str = "handoff-";

/// Maximum serialized bytes of one [`AgentHandoffRecord`].
///
/// The record rides the source run's bounded durable state, so context that
/// does not fit as references belongs behind an artifact — the same rule the
/// delegation record applies to its inline input.
pub const AGENT_HANDOFF_RECORD_MAX_BYTES: usize = 8 * 1024;

/// Maximum explicit context references one handoff projects.
pub const AGENT_HANDOFF_MAX_CONTEXT_REFS: usize = 8;

/// Maximum bytes of one projected context reference.
pub const AGENT_HANDOFF_CONTEXT_REF_MAX_BYTES: usize = 256;

/// Maximum bytes of the model-supplied handoff reason.
pub const AGENT_HANDOFF_REASON_MAX_BYTES: usize = 256;

/// Derives the identity of the handoff one run's turn commits in one slot.
///
/// The derivation is pure — the same length-prefixed digest construction as
/// [`crate::delegation::delegation_id_for`], with a leading domain segment so
/// a handoff and a delegation committed at the same `(turn, slot)` coordinate
/// can never collide — and it doubles verbatim as the A2A message id and
/// deduplication key of the send, so replaying the transition that decided
/// the handoff resolves to the same recorded transfer.
pub fn handoff_id_for(
    scope: &AgentRunScope,
    turn: u64,
    slot: usize,
) -> AgentCoordinationResult<AgentHandoffId> {
    validate_tenant(scope.tenant())?;
    let digest = AgentContentDigest::sha256_of_segments([
        "handoff",
        scope.tenant().as_str(),
        scope.agent().as_str(),
        scope.run().as_str(),
        &turn.to_string(),
        &slot.to_string(),
    ]);
    Ok(AgentHandoffId::new(format!(
        "{AGENT_HANDOFF_ID_PREFIX}{}",
        digest.value
    ))?)
}

/// Derives the stable operation id of the one handoff-result exchange a task
/// ever owes one handoff's source run.
///
/// Pure over `(tenant, handoff)`: the handoff's resolution is absorbing —
/// accepted or refused, first writer wins — so one logical result exists per
/// handoff, ever, and every re-drive after any loss owes the identical
/// operation ([specification 9.8](../../../docs/plans/rakka-agent/spec.md)).
pub fn handoff_result_operation_id(
    tenant: &TenantId,
    handoff: &AgentHandoffId,
) -> Result<AgentOperationId, AgentIdentityError> {
    AgentOperationId::new(
        AgentOperationKind::Handoff,
        [tenant.as_str(), handoff.as_str(), "result"],
    )
}

/// Prefix of every derived [`AgentTeamClaimId`].
///
/// The suffix is a fixed-length digest, so the id always satisfies the
/// identity bounds whatever the board coordinate contains, and an id without
/// this prefix was not derived by [`team_claim_id_for`].
pub const AGENT_TEAM_CLAIM_ID_PREFIX: &str = "team-claim-";

/// Derives the identity of the claim one team's board decision records for
/// one `(task, member, epoch)` coordinate.
///
/// The derivation is pure over durable board state at the claiming
/// transition — the same length-prefixed digest construction as
/// [`handoff_id_for`], with a leading domain segment so a claim can never
/// collide with any other derived identity family — so replaying the command
/// that decided the claim re-derives the identical id and converges on the
/// same recorded arbitration rather than a second owner.
pub fn team_claim_id_for(
    scope: &AgentTeamScope,
    task: &AgentTaskId,
    member: &AgentId,
    epoch: u64,
) -> AgentCoordinationResult<AgentTeamClaimId> {
    validate_tenant(scope.tenant())?;
    let digest = AgentContentDigest::sha256_of_segments([
        "team-claim",
        scope.tenant().as_str(),
        scope.team().as_str(),
        task.as_str(),
        member.as_str(),
        &epoch.to_string(),
    ]);
    Ok(AgentTeamClaimId::new(format!(
        "{AGENT_TEAM_CLAIM_ID_PREFIX}{}",
        digest.value
    ))?)
}

/// Derives the stable operation id of the one claim-apply exchange a team
/// ever owes one claim.
///
/// Pure over `(tenant, claim)`: the claim id already binds the board's
/// `(task, member, epoch)` coordinate, and a board decision owes exactly one
/// apply exchange, so every re-drive after any loss owes the identical
/// operation ([specification 9.8](../../../docs/plans/rakka-agent/spec.md)).
pub fn team_claim_operation_id(
    tenant: &TenantId,
    claim: &AgentTeamClaimId,
) -> Result<AgentOperationId, AgentIdentityError> {
    AgentOperationId::new(
        AgentOperationKind::TeamClaim,
        [tenant.as_str(), claim.as_str(), "apply"],
    )
}

/// Derives the stable operation id of one release exchange over one
/// recorded claim.
///
/// The board epoch qualifies the id: a release the task refused as
/// in-flight restores the entry, and a *retried* release is a new board
/// decision at a new epoch — it must not collide in the journal with the
/// attempt it retries.
pub fn team_claim_release_operation_id(
    tenant: &TenantId,
    claim: &AgentTeamClaimId,
    epoch: u64,
) -> Result<AgentOperationId, AgentIdentityError> {
    let epoch = epoch.to_string();
    AgentOperationId::new(
        AgentOperationKind::TeamClaim,
        [tenant.as_str(), claim.as_str(), "release", epoch.as_str()],
    )
}

/// Derives the stable operation id of the one claim-result exchange a task
/// ever owes one claim's team.
///
/// Pure over `(tenant, claim)`: the claim's resolution is absorbing —
/// activated or refused, first writer wins — so one logical result exists per
/// claim, ever, and every re-drive after any loss owes the identical
/// operation.
pub fn team_claim_result_operation_id(
    tenant: &TenantId,
    claim: &AgentTeamClaimId,
) -> Result<AgentOperationId, AgentIdentityError> {
    AgentOperationId::new(
        AgentOperationKind::TeamClaim,
        [tenant.as_str(), claim.as_str(), "result"],
    )
}

/// Derives the stable operation id of one submitted conversation turn.
///
/// Every input is a logical coordinate of the decision itself — the
/// [`wake identity`](crate::wake::wake_id_for_occurrence) discipline — and the
/// content digest is deliberately one of them: a durable redelivery carries
/// the same content and re-derives the same operation, so it converges on the
/// recorded turn; a *regenerated* submission with different content at the
/// same `(round, turn)` coordinate is a new, illegal decision, and deriving a
/// different id keeps it from being silently answered with the recorded
/// turn's echo — it falls through to the ledger guard and refuses loudly.
///
/// "Content" is the whole decision, not just the words:
/// [`conversation_turn_content_digest`] covers the moderator's direction too,
/// because designating a speaker and closing the round are different
/// decisions however identical their bodies.
pub fn conversation_turn_operation_id(
    tenant: &TenantId,
    conversation: &AgentConversationId,
    round: u64,
    turn: u32,
    participant: &AgentId,
    content_digest: &AgentContentDigest,
) -> Result<AgentOperationId, AgentIdentityError> {
    let round = round.to_string();
    let turn = turn.to_string();
    AgentOperationId::new(
        AgentOperationKind::ConversationTurn,
        [
            tenant.as_str(),
            conversation.as_str(),
            round.as_str(),
            turn.as_str(),
            participant.as_str(),
            content_digest.value.as_str(),
        ],
    )
}

/// Digests one conversation turn's content — its body *and* the moderator's
/// direction — for identity derivation.
///
/// The direction belongs to the digest because it is the half of a moderator
/// turn that steers the protocol: a turn regenerated with the same words but
/// `Designate` where the recorded one closed the round is a different
/// decision, and one identity for both would let it be absorbed as a
/// duplicate instead of refusing loudly. Segments are length-prefixed, so the
/// encoding stays injective across the three direction shapes.
///
/// The leading domain segment keeps the digest family injective against every
/// other derived identity, the [`handoff_id_for`] discipline.
#[must_use]
pub fn conversation_turn_content_digest(
    body: &str,
    direction: Option<&crate::conversation::AgentConversationDirection>,
) -> AgentContentDigest {
    use crate::conversation::AgentConversationDirection;

    let (kind, target) = match direction {
        None => ("none", ""),
        Some(AgentConversationDirection::CloseRound) => ("close-round", ""),
        Some(AgentConversationDirection::Designate(designated)) => {
            ("designate", designated.as_str())
        }
    };
    AgentContentDigest::sha256_of_segments(["conversation-turn-content", body, kind, target])
}

/// Derives the stable operation id of one conversation's creation.
///
/// Pure over `(tenant, conversation)`: a conversation is created exactly once
/// by trusted wiring, so every replay of the creating command names the
/// identical operation.
///
/// Deliberately blind to the creation's content, unlike
/// [`conversation_create_content_operation_id`] — kept for callers that only
/// need the identity of "the creation of this conversation".
pub fn conversation_create_operation_id(
    tenant: &TenantId,
    conversation: &AgentConversationId,
) -> Result<AgentOperationId, AgentIdentityError> {
    AgentOperationId::new(
        AgentOperationKind::ConversationOperation,
        [tenant.as_str(), conversation.as_str(), "create"],
    )
}

/// Derives the stable operation id of one conversation's creation, qualified
/// by the creation record itself.
///
/// The turn identity's discipline applied to creation: a replay carrying the
/// *same* record re-derives the same operation and converges, while a second
/// creation with a different roster, mode, policy, or budget derives a
/// different one — so it falls through to the entity's own guard and refuses
/// `conversation-already-created` instead of being absorbed as a duplicate of
/// a conversation it does not describe.
pub fn conversation_create_content_operation_id(
    tenant: &TenantId,
    conversation: &AgentConversationId,
    creation: &crate::conversation::AgentConversationCreation,
) -> Result<AgentOperationId, AgentIdentityError> {
    let digest = serde_json::to_value(creation)
        .map(|value| AgentContentDigest::sha256_of_json(&value))
        .unwrap_or_else(|_| AgentContentDigest::sha256_of_segments(["conversation-create-unset"]));
    AgentOperationId::new(
        AgentOperationKind::ConversationOperation,
        [
            tenant.as_str(),
            conversation.as_str(),
            "create",
            digest.value.as_str(),
        ],
    )
}

/// Derives the stable operation id of one early-end decision over one
/// conversation.
///
/// The round qualifies the id: an end decided against an old round is fenced
/// as stale, and a *retried* end at a later round is a new decision — it must
/// not collide in the operation log with the attempt it retries (the
/// [`team_claim_release_operation_id`] hazard).
///
/// The reason qualifies it too, for the same reason a turn's body digest
/// qualifies the turn's: a redelivery carries the same reason and converges
/// on the recorded end, while an end *regenerated* with different reasoning
/// is a different decision. Sharing one identity would answer it `Duplicate`
/// while the audited reason in the append-only history stayed the first
/// attempt's — success-shaped, and wrong.
pub fn conversation_end_operation_id(
    tenant: &TenantId,
    conversation: &AgentConversationId,
    round: u64,
    reason: &str,
) -> Result<AgentOperationId, AgentIdentityError> {
    let round = round.to_string();
    let digest = conversation_end_reason_digest(reason);
    AgentOperationId::new(
        AgentOperationKind::ConversationOperation,
        [
            tenant.as_str(),
            conversation.as_str(),
            "end",
            round.as_str(),
            digest.value.as_str(),
        ],
    )
}

/// Digests one early-end reason for identity derivation.
///
/// The leading domain segment keeps the digest family injective against every
/// other derived identity, the [`handoff_id_for`] discipline.
#[must_use]
pub fn conversation_end_reason_digest(reason: &str) -> AgentContentDigest {
    AgentContentDigest::sha256_of_segments(["conversation-end-reason", reason])
}

/// Derives the stable operation id of the lazy deadline-expiry observation
/// over one conversation.
///
/// Pure over `(tenant, conversation)`: the flip is absorbing, so one logical
/// expiry observation exists per conversation, ever.
pub fn conversation_expiry_operation_id(
    tenant: &TenantId,
    conversation: &AgentConversationId,
) -> Result<AgentOperationId, AgentIdentityError> {
    AgentOperationId::new(
        AgentOperationKind::ConversationOperation,
        [tenant.as_str(), conversation.as_str(), "expiry"],
    )
}

/// Payload type of the team-claim exchange a team drives onto a task.
pub const AGENT_TEAM_CLAIM_PAYLOAD_TYPE: &str = "rakka.agent.TeamClaim";

/// Payload type of the claim-result exchange a task owes a claim's team.
pub const AGENT_TEAM_CLAIM_RESULT_PAYLOAD_TYPE: &str = "rakka.agent.TeamClaimResult";

/// The board decision one team-claim exchange carries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentTeamClaimAction {
    /// Claim the entry for one member — a fresh claim, an expired-lease
    /// steal, and a transfer's superseding claim are all this action; the
    /// task's arbitration cannot and need not tell them apart.
    Claim {
        /// The member the board recorded as the claimant.
        member: AgentId,
    },
    /// Release the recorded claim before its assignment accepted.
    Release,
}

/// The command payload of one [`crate::choreography::AgentExchangeKind::TeamClaim`]
/// exchange (team → task).
///
/// Every field is durable board state re-validated by the task's own
/// arbitration; nothing here is model output. The task fences the `epoch`
/// against its own recorded claim fence, so a courier-reordered stale action
/// refuses rather than reviving a superseded decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTeamClaimCommand {
    /// The team whose board decision this is.
    pub team: AgentTeamScope,
    /// The derived claim this action applies or releases.
    pub claim: AgentTeamClaimId,
    /// The claimed task.
    pub task: AgentTaskId,
    /// The board entry's claim epoch after the deciding transition.
    pub epoch: u64,
    /// The decision.
    pub action: AgentTeamClaimAction,
    /// The team policy revision in force at the decision.
    pub policy_revision: AgentRevisionNumber,
    /// When the recorded claim's pending window lapses. Advisory to the
    /// task — the board observes expiry itself, lazily.
    pub lease_expires_at: AgentTimestampMillis,
}

/// How one recorded claim resolved at its task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentTeamClaimOutcome {
    /// The claimant's assignment was durably accepted; the echoes let the
    /// board mirror the owner without ever holding ownership.
    Activated {
        /// The accepted assignment generation.
        generation: AgentAssignmentGeneration,
        /// The run serving that generation.
        run: AgentRunId,
        /// The member the assignment accepted under.
        member: AgentId,
    },
    /// The claim refused — arbitration, readiness, exhaustion, budget, or
    /// cancellation — under a stable code; the board entry reopens.
    Refused {
        /// The stable refusal code.
        code: String,
    },
}

/// The notice payload of one
/// [`crate::choreography::AgentExchangeKind::TeamClaimResult`] exchange
/// (task → source team).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTeamClaimResultNotice {
    /// The task reporting the resolution.
    pub task: AgentTaskScope,
    /// The claim that resolved.
    pub claim: AgentTeamClaimId,
    /// The board epoch the claim was recorded under.
    pub epoch: u64,
    /// How it resolved.
    pub outcome: AgentTeamClaimOutcome,
}

/// Derives the stable operation id of the one terminal notice a task ever
/// owes its governing team's board.
///
/// Pure over `(tenant, team, task)`: terminality is absorbing — a task
/// terminalizes at most once, under one immutable governing team — so one
/// logical notice exists per task, ever, and every re-derivation after any
/// loss owes the identical operation
/// ([specification 9.8](../../../docs/plans/rakka-agent/spec.md)).
pub fn team_terminal_notice_operation_id(
    tenant: &TenantId,
    team: &crate::identity::AgentTeamId,
    task: &AgentTaskId,
) -> Result<AgentOperationId, AgentIdentityError> {
    AgentOperationId::new(
        AgentOperationKind::TeamOperation,
        [
            tenant.as_str(),
            team.as_str(),
            task.as_str(),
            "terminal-notice",
        ],
    )
}

/// Derives the stable operation id of the one terminal notice a conversation
/// ever owes its governing task.
///
/// Pure over `(tenant, conversation)`: the terminal flip is absorbing —
/// rounds-complete, moderator-ended, or expired, first writer wins — so one
/// logical notice exists per conversation, ever, and every re-derivation
/// after any loss owes the identical operation.
pub fn conversation_terminal_notice_operation_id(
    tenant: &TenantId,
    conversation: &AgentConversationId,
) -> Result<AgentOperationId, AgentIdentityError> {
    AgentOperationId::new(
        AgentOperationKind::ConversationOperation,
        [tenant.as_str(), conversation.as_str(), "terminal-notice"],
    )
}

/// Payload type of the terminal-notice exchange a task drives onto its
/// governing team's board.
pub const AGENT_TEAM_TERMINAL_NOTICE_PAYLOAD_TYPE: &str = "rakka.agent.TeamTerminalNotice";

/// Payload type of the terminal-notice exchange a conversation drives onto
/// its governing task.
pub const AGENT_CONVERSATION_TERMINAL_NOTICE_PAYLOAD_TYPE: &str =
    "rakka.agent.ConversationTerminalNotice";

/// The notice payload of one
/// [`crate::choreography::AgentExchangeKind::TeamTerminalNotice`] exchange
/// (task → governing team).
///
/// Identities and stable codes only — the board closes the entry and echoes
/// the reason; it never mirrors result content.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTeamTerminalNotice {
    /// The terminal task closing its board entry.
    pub task: AgentTaskScope,
    /// The task's terminal status.
    pub status: AgentTaskStatus,
    /// The terminal reason's stable code
    /// ([`crate::task::AgentTaskTerminalReason::code`]).
    pub terminal_reason: String,
}

/// The notice payload of one
/// [`crate::choreography::AgentExchangeKind::ConversationTerminalNotice`]
/// exchange (conversation → governing task).
///
/// Identity and coordinates only — never transcript bodies; the transcript
/// stays behind the conversation's own authorized surfaces.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentConversationTerminalNotice {
    /// The terminated conversation reporting itself.
    pub conversation: crate::identity::AgentConversationScope,
    /// The governing task the conversation was created against.
    pub task: AgentTaskId,
    /// The conversation's terminal status.
    pub status: crate::conversation::AgentConversationStatus,
    /// Why it terminated.
    pub terminal_reason: crate::conversation::AgentConversationTerminalReason,
    /// How many rounds *completed* before it ended.
    ///
    /// [`crate::conversation::AgentConversation::round`] is a next-expected
    /// cursor on a live conversation, and it advances exactly once per
    /// closed round, so on a terminated one the same number is the count of
    /// rounds that finished. Named for the reading that holds here: as an
    /// index it would be wrong under `RoundsComplete`, which closes the
    /// final round before flipping and so leaves the cursor one past the
    /// last round that ran.
    pub rounds_completed: u64,
    /// How many turns were recorded over the conversation's life.
    pub turns_recorded: u64,
    /// When the terminal flip committed.
    pub ended_at: AgentTimestampMillis,
}

/// Whether one refusal code definitively settles a team terminal notice at
/// its initiating task.
///
/// One classifier for both ends of the exchange — the task's settle rule and
/// the team's memoization gate — so the two sides agree by construction:
/// a code in this list settles the notice *and* memoizes at the receiver; a
/// code outside it stays outstanding *and* re-runs the receiving arm on the
/// next drive. A closed team is frozen as history, and a verdict the payload
/// itself fails — forged, or reporting a task that has not ended — never
/// changes on replay: the courier re-delivers the *stored* envelope rather
/// than re-deriving it, so the same bytes answer the same way for as long as
/// they exist.
///
/// `team-not-found` settles here, and the deliberate divergence from
/// [`conversation_terminal_notice_refusal_settles`] — which leaves the
/// analogous `task-not-created` outstanding — is the part worth writing
/// down, because the codes look symmetric and the entities are not.
///
/// A conversation is created *against* an existing task, so a notice
/// arriving before that task exists is an ordering race inside one flow and
/// waiting it out converges. A team is trusted application wiring, created
/// ahead of the tasks that name it and never by a peer or a model, so a task
/// naming a team that does not exist is a wiring mistake rather than a race
/// — the wrong id, or a team never stood up. Waiting that out would trade a
/// bounded, already-surfaced mistake (`max_unclaimed_millis` expires such a
/// task through the cancellation machinery whole) for an exchange owed
/// forever, whose every re-drive costs the receiver a durable write.
///
/// What settling costs, stated so the trade is visible: if that team is
/// created *later* and the terminal task is posted to its board, the board
/// holds an `Open` entry for work that already ended, and it closes the old
/// lazy way — through a member's claim attempt refused
/// `team-claim-task-terminal`. That is the pre-slice behavior, not a new
/// hazard, and it needs a board post that no longer has any reason to happen.
pub(crate) fn team_terminal_notice_refusal_settles(code: &str) -> bool {
    matches!(
        code,
        "team-not-found"
            | "team-terminal-notice-forged"
            | "team-terminal-notice-not-terminal"
            | "team-expired"
            | "team-disbanded"
    )
}

/// Whether one refusal code definitively settles a conversation terminal
/// notice at its initiating conversation.
///
/// The same two-ended classifier discipline as
/// [`team_terminal_notice_refusal_settles`], and only a forged verdict
/// qualifies — the one answer that cannot change on a later re-drive.
///
/// Two refusals are deliberately absent, both for the same reason: the task
/// is not saying *never*, it is saying *not yet*. `task-not-created` is the
/// dependency-registration posture — a notice racing its task's creation
/// converges on a re-drive instead of memoizing the miss. A record too full
/// to grow the provenance cell is the sharper case, because the receiver's
/// own bound is what moves: it charges a pre-terminal task the
/// [`crate::task::AGENT_TASK_STATE_GROWTH_RESERVE_BYTES`] headroom that
/// task's remaining lifecycle still needs, and a terminal task nothing at
/// all, so the very cell that will not fit today fits once the task
/// terminalizes. Settling on it would quiesce both ends over a refusal the
/// receiver is about to stop making, and the provenance would be lost for
/// good.
///
/// `task-dependency-limit-exceeded` is the *other* exit of that same bound
/// check, and it classifies the opposite way. The task's dependency map only
/// ever grows, and recording a conversation cell does not touch it, so a
/// record already over its ceiling refuses this notice identically forever —
/// leaving it outstanding would re-run the receiving arm, and its durable
/// write, on every settle pass for the life of both entities. A bound this
/// exchange can never satisfy is a definitive answer even though a bound it
/// merely has to wait out is not.
pub(crate) fn conversation_terminal_notice_refusal_settles(code: &str) -> bool {
    matches!(
        code,
        "conversation-terminal-notice-forged" | "task-dependency-limit-exceeded"
    )
}

/// One coordination capability descriptor: the policy payload behind one
/// [`AgentCoordinationCapabilityKind`]
/// ([specification 8.8](../../../docs/plans/rakka-agent/spec.md)).
///
/// Descriptors are trusted definition/setup data. They never join the
/// serialized agent definition: admission enforces the *kind* set as the
/// [`crate::definition::AgentEnvelopeDimension::CoordinationCapability`]
/// envelope dimension, and each runtime wiring validates its descriptor
/// against that set at construction — a deployment cannot wire a
/// coordination tool while forgetting the capability that authorizes it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentCoordinationCapability {
    /// Same-task transfer of responsibility to another agent
    /// ([specification 8.9](../../../docs/plans/rakka-agent/spec.md)).
    Handoff(AgentHandoffPolicy),
    /// Child task/run creation with bounded fan-out and result fan-in
    /// ([specification 8.4](../../../docs/plans/rakka-agent/spec.md)).
    Delegation(AgentDelegationPolicy),
    /// Team leadership over a durable shared task board
    /// ([specification 8.10](../../../docs/plans/rakka-agent/spec.md)).
    TeamLeadership(AgentTeamPolicy),
    /// Moderation of a turn-taking conversation
    /// ([specification 8.11](../../../docs/plans/rakka-agent/spec.md)).
    Moderation(AgentModerationPolicy),
}

impl AgentCoordinationCapability {
    /// The capability kind this descriptor realizes.
    #[must_use]
    pub const fn kind(&self) -> AgentCoordinationCapabilityKind {
        match self {
            Self::Handoff(_) => AgentCoordinationCapabilityKind::Handoff,
            Self::Delegation(_) => AgentCoordinationCapabilityKind::Delegation,
            Self::TeamLeadership(_) => AgentCoordinationCapabilityKind::Team,
            Self::Moderation(_) => AgentCoordinationCapabilityKind::Moderation,
        }
    }

    /// Stable kebab-case label: the kind's label.
    #[must_use]
    pub const fn as_label(&self) -> &'static str {
        self.kind().as_label()
    }

    /// The policy revision this descriptor carries.
    #[must_use]
    pub const fn revision(&self) -> AgentRevisionNumber {
        match self {
            Self::Handoff(policy) => policy.revision,
            Self::Delegation(policy) => policy.revision,
            Self::TeamLeadership(policy) => policy.revision,
            Self::Moderation(policy) => policy.revision,
        }
    }
}

/// The handoff capability's policy payload
/// ([specification 8.9](../../../docs/plans/rakka-agent/spec.md)).
///
/// The tool id is the one declared coordination tool the loop intercepts
/// into a handoff; the revision is persisted on every handoff record this
/// policy authorizes. The capability revision of specification 8.9 is the
/// definition revision under which the `Handoff` kind was admitted — the
/// record carries both.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AgentHandoffPolicy {
    /// The declared coordination tool the loop intercepts.
    pub tool: AgentToolId,
    /// The policy revision handoff records persist.
    pub revision: AgentRevisionNumber,
}

impl AgentHandoffPolicy {
    /// Creates the policy.
    #[must_use]
    pub const fn new(tool: AgentToolId, revision: AgentRevisionNumber) -> Self {
        Self { tool, revision }
    }
}

/// The delegation capability's policy payload
/// ([specification 8.4](../../../docs/plans/rakka-agent/spec.md)).
///
/// A descriptor mirror of the declared surface of
/// [`crate::delegation::AgentRunDelegationConfig`], which realizes it — the
/// config's catalog stays runtime wiring and is never descriptor data.
/// Derive one with [`crate::delegation::AgentRunDelegationConfig::descriptor`]
/// rather than constructing a parallel copy of trusted config.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AgentDelegationPolicy {
    /// The declared coordination tool the loop intercepts.
    pub tool: AgentToolId,
    /// The declared await verb the loop intercepts into a fan-in close,
    /// when the deployment wires one.
    #[serde(default)]
    pub fan_in_tool: Option<AgentToolId>,
    /// The policy revision.
    pub revision: AgentRevisionNumber,
}

impl AgentDelegationPolicy {
    /// Creates the policy.
    #[must_use]
    pub const fn new(tool: AgentToolId, revision: AgentRevisionNumber) -> Self {
        Self {
            tool,
            fan_in_tool: None,
            revision,
        }
    }

    /// Declares the await verb.
    #[must_use]
    pub fn with_fan_in_tool(mut self, tool: AgentToolId) -> Self {
        self.fan_in_tool = Some(tool);
        self
    }
}

/// Hard ceiling on a team's bounded membership; a policy value above it
/// clamps.
pub const AGENT_TEAM_MAX_MEMBERS: u32 = 16;

/// Hard ceiling on a team board's entries; a policy value above it clamps.
pub const AGENT_TEAM_MAX_BOARD_ENTRIES: u32 = 32;

/// Hard ceiling on a team's mediated-message ring; a policy value above it
/// clamps.
pub const AGENT_TEAM_MAX_MESSAGES: u32 = 16;

/// Hard ceiling on one mediated message's body bytes; a policy value above
/// it clamps.
pub const AGENT_TEAM_MESSAGE_MAX_BYTES: usize = 1024;

const fn default_team_max_members() -> u32 {
    8
}

const fn default_team_max_board_entries() -> u32 {
    16
}

const fn default_team_max_messages() -> u32 {
    8
}

const fn default_team_max_message_bytes() -> usize {
    AGENT_TEAM_MESSAGE_MAX_BYTES
}

const fn default_team_claim_lease_ms() -> u64 {
    300_000
}

/// The team-leadership capability's policy payload
/// ([specification 8.10](../../../docs/plans/rakka-agent/spec.md)).
///
/// Trusted definition/setup data: the board's bounded ceilings, the
/// claim-lease duration, and the creation/expiry policy. The lease bounds the
/// claim-*pending* window only — an accepted assignment is never stealable by
/// lease expiry; run budgets bound execution. Every field is serde-defaulted,
/// so a pre-slice revision-only payload still decodes.
///
/// The `tool` field is the shaped-but-dormant hook for a later model-visible
/// team coordination tool: this slice wires no run-loop interception, no team
/// effect kind, and no executor port, so a declared tool id is carried but
/// never dispatched.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AgentTeamPolicy {
    /// The policy revision.
    pub revision: AgentRevisionNumber,
    /// Bounded membership ceiling, clamped to [`AGENT_TEAM_MAX_MEMBERS`].
    #[serde(default = "default_team_max_members")]
    pub max_members: u32,
    /// Bounded board ceiling, clamped to [`AGENT_TEAM_MAX_BOARD_ENTRIES`].
    #[serde(default = "default_team_max_board_entries")]
    pub max_board_entries: u32,
    /// Bounded message-ring ceiling, clamped to [`AGENT_TEAM_MAX_MESSAGES`].
    #[serde(default = "default_team_max_messages")]
    pub max_messages: u32,
    /// Bounded message-body ceiling, clamped to
    /// [`AGENT_TEAM_MESSAGE_MAX_BYTES`].
    #[serde(default = "default_team_max_message_bytes")]
    pub max_message_bytes: usize,
    /// Milliseconds a recorded claim may stay pending before another member
    /// may steal the entry. Observed lazily at the next board command — no
    /// timer ever fires to expire a lease.
    #[serde(default = "default_team_claim_lease_ms")]
    pub claim_lease_ms: u64,
    /// Milliseconds after creation at which the team expires, observed
    /// lazily. `None` means the team never expires on its own.
    #[serde(default)]
    pub expires_after_ms: Option<u64>,
    /// The declared coordination tool a later slice's loop interception will
    /// realize. Dormant in this slice: carried, validated, never dispatched.
    #[serde(default)]
    pub tool: Option<AgentToolId>,
}

impl AgentTeamPolicy {
    /// Creates the policy with the default ceilings and lease.
    #[must_use]
    pub const fn new(revision: AgentRevisionNumber) -> Self {
        Self {
            revision,
            max_members: default_team_max_members(),
            max_board_entries: default_team_max_board_entries(),
            max_messages: default_team_max_messages(),
            max_message_bytes: default_team_max_message_bytes(),
            claim_lease_ms: default_team_claim_lease_ms(),
            expires_after_ms: None,
            tool: None,
        }
    }

    /// Sets the membership ceiling, clamped to the hard cap.
    #[must_use]
    pub fn with_max_members(mut self, max_members: u32) -> Self {
        self.max_members = max_members.min(AGENT_TEAM_MAX_MEMBERS);
        self
    }

    /// Sets the board ceiling, clamped to the hard cap.
    #[must_use]
    pub fn with_max_board_entries(mut self, max_board_entries: u32) -> Self {
        self.max_board_entries = max_board_entries.min(AGENT_TEAM_MAX_BOARD_ENTRIES);
        self
    }

    /// Sets the message-ring ceiling, clamped to the hard cap.
    #[must_use]
    pub fn with_max_messages(mut self, max_messages: u32) -> Self {
        self.max_messages = max_messages.min(AGENT_TEAM_MAX_MESSAGES);
        self
    }

    /// Sets the message-body ceiling, clamped to the hard cap.
    #[must_use]
    pub fn with_max_message_bytes(mut self, max_message_bytes: usize) -> Self {
        self.max_message_bytes = max_message_bytes.min(AGENT_TEAM_MESSAGE_MAX_BYTES);
        self
    }

    /// Sets the claim-lease duration.
    #[must_use]
    pub const fn with_claim_lease_ms(mut self, claim_lease_ms: u64) -> Self {
        self.claim_lease_ms = claim_lease_ms;
        self
    }

    /// Sets the lazy expiry horizon.
    #[must_use]
    pub const fn with_expiry_after_ms(mut self, expires_after_ms: u64) -> Self {
        self.expires_after_ms = Some(expires_after_ms);
        self
    }

    /// Declares the dormant coordination tool.
    #[must_use]
    pub fn with_tool(mut self, tool: AgentToolId) -> Self {
        self.tool = Some(tool);
        self
    }

    /// Membership ceiling with the hard cap applied, whatever a stored or
    /// wire-carried payload claims.
    #[must_use]
    pub fn effective_max_members(&self) -> u32 {
        self.max_members.clamp(1, AGENT_TEAM_MAX_MEMBERS)
    }

    /// Board ceiling with the hard cap applied.
    #[must_use]
    pub fn effective_max_board_entries(&self) -> u32 {
        self.max_board_entries
            .clamp(1, AGENT_TEAM_MAX_BOARD_ENTRIES)
    }

    /// Message-ring ceiling with the hard cap applied.
    #[must_use]
    pub fn effective_max_messages(&self) -> u32 {
        self.max_messages.clamp(1, AGENT_TEAM_MAX_MESSAGES)
    }

    /// Message-body ceiling with the hard cap applied.
    #[must_use]
    pub fn effective_max_message_bytes(&self) -> usize {
        self.max_message_bytes
            .clamp(1, AGENT_TEAM_MESSAGE_MAX_BYTES)
    }
}

/// Hard cap on a conversation's authorized participant roster.
pub const AGENT_CONVERSATION_MAX_PARTICIPANTS: u32 = 8;

/// Hard cap on the round ceiling any moderation policy may declare.
pub const AGENT_CONVERSATION_MAX_ROUNDS: u32 = 16;

/// Hard cap on the turns-per-round ceiling any moderation policy may declare.
pub const AGENT_CONVERSATION_MAX_TURNS_PER_ROUND: u32 = 16;

/// Hard cap on the transcript message-ring ceiling.
pub const AGENT_CONVERSATION_MAX_MESSAGES: u32 = 16;

/// Hard cap on one transcript message body's bytes.
pub const AGENT_CONVERSATION_MESSAGE_MAX_BYTES: usize = 1024;

/// Default ceiling on the token usage one conversation turn may report.
///
/// Sized well above any single model call's plausible total so an honest
/// report is never refused, and far below the point where one self-report
/// can silently exhaust a shared grant on behalf of everyone else. A
/// deployment whose turns legitimately run larger raises it — or opts out
/// entirely — through
/// [`AgentModerationPolicy::with_max_turn_tokens`].
pub const AGENT_CONVERSATION_DEFAULT_MAX_TURN_TOKENS: u64 = 1_000_000;

const fn default_conversation_max_rounds() -> u32 {
    4
}

const fn default_conversation_max_turns_per_round() -> u32 {
    8
}

const fn default_conversation_max_messages() -> u32 {
    8
}

const fn default_conversation_max_message_bytes() -> usize {
    AGENT_CONVERSATION_MESSAGE_MAX_BYTES
}

const fn default_conversation_max_turn_tokens() -> Option<u64> {
    Some(AGENT_CONVERSATION_DEFAULT_MAX_TURN_TOKENS)
}

const fn default_moderator_may_end_early() -> bool {
    true
}

/// The moderation capability's policy payload
/// ([specification 8.11](../../../docs/plans/rakka-agent/spec.md)).
///
/// Trusted definition/setup data: the turn protocol's bounded ceilings and
/// the moderator's early-end permission. Every field is serde-defaulted, so a
/// pre-slice revision-only payload still decodes. The wall-clock and token
/// budgets are not policy — they are per-conversation creation data, fixed at
/// creation the way a run's deadline is fixed at assignment acceptance.
///
/// The `tool` field is the shaped-but-dormant hook for a later model-visible
/// moderation tool: this slice wires no run-loop interception, no
/// conversation effect kind, and no executor port, so a declared tool id is
/// carried but never dispatched.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AgentModerationPolicy {
    /// The policy revision.
    pub revision: AgentRevisionNumber,
    /// Bounded round ceiling, clamped to [`AGENT_CONVERSATION_MAX_ROUNDS`].
    #[serde(default = "default_conversation_max_rounds")]
    pub max_rounds: u32,
    /// Bounded turns-per-round ceiling, clamped to
    /// [`AGENT_CONVERSATION_MAX_TURNS_PER_ROUND`].
    #[serde(default = "default_conversation_max_turns_per_round")]
    pub max_turns_per_round: u32,
    /// Bounded transcript-ring ceiling, clamped to
    /// [`AGENT_CONVERSATION_MAX_MESSAGES`].
    #[serde(default = "default_conversation_max_messages")]
    pub max_messages: u32,
    /// Bounded message-body ceiling, clamped to
    /// [`AGENT_CONVERSATION_MESSAGE_MAX_BYTES`].
    #[serde(default = "default_conversation_max_message_bytes")]
    pub max_message_bytes: usize,
    /// Ceiling on the token usage one turn may *report*, or `None` to accept
    /// any self-report.
    ///
    /// The reported spend is the speaker's own claim, and the conversation's
    /// token grant is shared: without a ceiling one turn's implausible
    /// report exhausts the grant for every other participant, and because
    /// exhaustion refuses rather than parks, the conversation has no
    /// reachable progress left. The ceiling bounds the claim the way
    /// `max_message_bytes` bounds the body; overshooting the *remaining*
    /// grant stays legal, because that spend already happened.
    #[serde(default = "default_conversation_max_turn_tokens")]
    pub max_turn_tokens: Option<u64>,
    /// Whether the moderator may end the conversation before its completion
    /// rule is met. The early-end *result* still passes the task's typed
    /// result validation and the goal's evaluation door on the run side.
    #[serde(default = "default_moderator_may_end_early")]
    pub moderator_may_end_early: bool,
    /// The declared coordination tool a later slice's loop interception will
    /// realize. Dormant in this slice: carried, validated, never dispatched.
    #[serde(default)]
    pub tool: Option<AgentToolId>,
}

impl AgentModerationPolicy {
    /// Creates the policy with the default ceilings.
    #[must_use]
    pub const fn new(revision: AgentRevisionNumber) -> Self {
        Self {
            revision,
            max_rounds: default_conversation_max_rounds(),
            max_turns_per_round: default_conversation_max_turns_per_round(),
            max_messages: default_conversation_max_messages(),
            max_message_bytes: default_conversation_max_message_bytes(),
            max_turn_tokens: default_conversation_max_turn_tokens(),
            moderator_may_end_early: default_moderator_may_end_early(),
            tool: None,
        }
    }

    /// Sets the round ceiling, clamped to the hard cap.
    #[must_use]
    pub fn with_max_rounds(mut self, max_rounds: u32) -> Self {
        self.max_rounds = max_rounds.min(AGENT_CONVERSATION_MAX_ROUNDS);
        self
    }

    /// Sets the turns-per-round ceiling, clamped to the hard cap.
    #[must_use]
    pub fn with_max_turns_per_round(mut self, max_turns_per_round: u32) -> Self {
        self.max_turns_per_round = max_turns_per_round.min(AGENT_CONVERSATION_MAX_TURNS_PER_ROUND);
        self
    }

    /// Sets the transcript-ring ceiling, clamped to the hard cap.
    #[must_use]
    pub fn with_max_messages(mut self, max_messages: u32) -> Self {
        self.max_messages = max_messages.min(AGENT_CONVERSATION_MAX_MESSAGES);
        self
    }

    /// Sets the message-body ceiling, clamped to the hard cap.
    #[must_use]
    pub fn with_max_message_bytes(mut self, max_message_bytes: usize) -> Self {
        self.max_message_bytes = max_message_bytes.min(AGENT_CONVERSATION_MESSAGE_MAX_BYTES);
        self
    }

    /// Sets the ceiling on the token usage one turn may report.
    #[must_use]
    pub const fn with_max_turn_tokens(mut self, max_turn_tokens: u64) -> Self {
        self.max_turn_tokens = Some(max_turn_tokens);
        self
    }

    /// Accepts any reported per-turn usage, however large.
    ///
    /// Only for a deployment whose speakers are as trusted as the moderation
    /// policy itself: one implausible report exhausts the shared grant for
    /// every participant.
    #[must_use]
    pub const fn without_turn_token_ceiling(mut self) -> Self {
        self.max_turn_tokens = None;
        self
    }

    /// Forbids the moderator from ending before the completion rule is met.
    #[must_use]
    pub const fn without_early_end(mut self) -> Self {
        self.moderator_may_end_early = false;
        self
    }

    /// Declares the dormant coordination tool.
    #[must_use]
    pub fn with_tool(mut self, tool: AgentToolId) -> Self {
        self.tool = Some(tool);
        self
    }

    /// Round ceiling with the hard cap applied, whatever a stored or
    /// wire-carried payload claims.
    #[must_use]
    pub fn effective_max_rounds(&self) -> u32 {
        self.max_rounds.clamp(1, AGENT_CONVERSATION_MAX_ROUNDS)
    }

    /// Turns-per-round ceiling with the hard cap applied.
    #[must_use]
    pub fn effective_max_turns_per_round(&self) -> u32 {
        self.max_turns_per_round
            .clamp(1, AGENT_CONVERSATION_MAX_TURNS_PER_ROUND)
    }

    /// Transcript-ring ceiling with the hard cap applied.
    #[must_use]
    pub fn effective_max_messages(&self) -> u32 {
        self.max_messages.clamp(1, AGENT_CONVERSATION_MAX_MESSAGES)
    }

    /// Message-body ceiling with the hard cap applied.
    #[must_use]
    pub fn effective_max_message_bytes(&self) -> usize {
        self.max_message_bytes
            .clamp(1, AGENT_CONVERSATION_MESSAGE_MAX_BYTES)
    }
}

/// The bounded request the model may make through the declared handoff tool.
///
/// This is the *entire* vocabulary model output has over handoff: a skill, a
/// bounded reason, and explicit context references. Unknown fields fail the
/// parse, so an agent id, endpoint, budget, or scope in model output is
/// refused rather than ignored — the catalog and the task's own durable state
/// decide those. Context entries are artifact or content *references* only,
/// never inline payloads: source session and private memory are structurally
/// out of reach, and this vocabulary keeps them out of the projection too.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHandoffToolCall {
    /// The skill the model requests a transfer to.
    pub skill: AgentCapabilityId,
    /// The bounded reason for the transfer.
    pub reason: String,
    /// Explicit context/artifact references projected to the target.
    #[serde(default)]
    pub context: Vec<String>,
}

impl AgentHandoffToolCall {
    /// Parses the handoff tool's arguments, failing closed on anything
    /// beyond the declared vocabulary.
    pub fn parse(arguments: &Value) -> AgentCoordinationResult<Self> {
        serde_json::from_value(arguments.clone()).map_err(|error| {
            AgentCoordinationError::InvalidArguments {
                message: error.to_string(),
            }
        })
    }
}

/// Rejects a context projection that exceeds its structural bounds.
///
/// Crate-visible because the bound is enforced twice: by the sender's record
/// validation, and by the receiving task transition re-validating the wire's
/// claim.
pub(crate) fn check_context_refs(context: &[String]) -> AgentCoordinationResult<()> {
    if context.len() > AGENT_HANDOFF_MAX_CONTEXT_REFS {
        return Err(AgentCoordinationError::ContextRefsTooMany {
            count: context.len(),
            maximum: AGENT_HANDOFF_MAX_CONTEXT_REFS,
        });
    }
    for reference in context {
        if reference.is_empty() || reference.len() > AGENT_HANDOFF_CONTEXT_REF_MAX_BYTES {
            return Err(AgentCoordinationError::ContextRefInvalid {
                bytes: reference.len(),
                maximum: AGENT_HANDOFF_CONTEXT_REF_MAX_BYTES,
            });
        }
    }
    Ok(())
}

/// One durable transfer of a task's responsibility from a source run to a
/// target agent ([specification 8.9](../../../docs/plans/rakka-agent/spec.md)).
///
/// Persisted in the same compare-and-set that commits the send effect,
/// strictly before any dispatch. Every identity below is either a pure
/// derivation of the source's `(turn, slot)` coordinate or the recorded
/// output of the one catalog resolution this handoff ever performs. The
/// specification's capability revision is [`Self::definition_revision`] —
/// the revision under which the `Handoff` kind was admitted — and its policy
/// revision is [`Self::policy_revision`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentHandoffRecord {
    /// The handoff identity, derived by [`handoff_id_for`].
    pub handoff: AgentHandoffId,
    /// The collaborative goal the source serves, when it serves one.
    #[serde(default)]
    pub goal: Option<AgentGoalId>,
    /// The task whose responsibility transfers. Preserved verbatim: a
    /// handoff never mints a task identity.
    pub task: AgentTaskId,
    /// The source run initiating the transfer.
    pub source_run: AgentRunScope,
    /// The assignment generation the source run serves.
    pub source_generation: AgentAssignmentGeneration,
    /// The skill the model requested.
    pub requested_skill: AgentCapabilityId,
    /// The target the catalog resolved, recorded so a replay never
    /// re-resolves.
    pub resolved: AgentDelegationTarget,
    /// The bounded reason the model supplied.
    pub reason: String,
    /// The handoff policy revision that authorized the transfer.
    pub policy_revision: AgentRevisionNumber,
    /// The agent definition revision the source decided under: the revision
    /// that admitted the handoff capability.
    pub definition_revision: AgentRevisionNumber,
    /// The agent settings revision the source decided under.
    pub settings_revision: AgentRevisionNumber,
    /// Explicit context/artifact references projected to the target — the
    /// only context that crosses; never session or private memory.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub context: Vec<String>,
    /// The A2A message id of the send: the handoff id verbatim.
    pub a2a_message_id: String,
    /// The deduplication key of the send: the handoff id verbatim.
    pub deduplication_key: String,
    /// The source turn that committed the handoff.
    pub turn: u64,
    /// The effect slot within the turn.
    pub slot: usize,
    /// The send effect's derived identity.
    pub effect: AgentEffectId,
    /// The model tool call this handoff answers — its causation. The
    /// settlement transition records the resolution as this call's tool
    /// result, which is how the turn completes.
    pub call_id: AgentToolCallId,
    /// Trace propagation for the send.
    #[serde(default)]
    pub telemetry: AgentTelemetryContext,
    /// When the record was committed.
    pub created_at: AgentTimestampMillis,
}

impl AgentHandoffRecord {
    /// Rejects a record that exceeds its structural bounds.
    ///
    /// The whole record refuses rather than truncating: a reason or context
    /// projection that does not fit belongs behind an artifact reference.
    pub fn validate(&self) -> AgentCoordinationResult<()> {
        self.resolved
            .validate()
            .map_err(|error| AgentCoordinationError::TargetInvalid {
                message: error.to_string(),
            })?;
        if self.reason.len() > AGENT_HANDOFF_REASON_MAX_BYTES {
            return Err(AgentCoordinationError::ReasonTooLong {
                bytes: self.reason.len(),
                maximum: AGENT_HANDOFF_REASON_MAX_BYTES,
            });
        }
        check_context_refs(&self.context)?;
        let bytes = serde_json::to_vec(self)
            .map_err(|error| AgentCoordinationError::Encoding {
                message: error.to_string(),
            })?
            .len();
        if bytes > AGENT_HANDOFF_RECORD_MAX_BYTES {
            return Err(AgentCoordinationError::RecordTooLarge {
                bytes,
                maximum: AGENT_HANDOFF_RECORD_MAX_BYTES,
            });
        }
        Ok(())
    }
}

/// Where one handoff stands.
///
/// `Pending` and `Sent` are the unsettled states: the record is durable and
/// the transfer is committed or recorded at the task, but the target's
/// assignment has not resolved. Every other variant is absorbing for this
/// handoff identity — recovery after ambiguity resolves through the task's
/// recorded provenance, never a resurrected send.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentHandoffStatus {
    /// The record is persisted and the send effect is committed or in
    /// flight.
    Pending,
    /// The task durably recorded the transfer and offered the target its
    /// assignment generation.
    Sent {
        /// The assignment generation the task minted toward the target,
        /// when the receipt reported one.
        #[serde(default)]
        target_generation: Option<AgentAssignmentGeneration>,
    },
    /// The target durably accepted its assignment: responsibility has
    /// transferred, and the source terminates `HandedOff`.
    Accepted {
        /// The target run now serving the task.
        target_run: AgentRunId,
        /// The accepted assignment generation.
        generation: AgentAssignmentGeneration,
    },
    /// The task or target refused definitively; the source assignment was
    /// restored and the source run resumes.
    Refused {
        /// Stable machine-readable refusal code.
        code: String,
    },
    /// The send failed definitively without the task recording a transfer.
    Failed {
        /// Stable machine-readable failure code.
        code: String,
    },
}

impl AgentHandoffStatus {
    /// Whether the handoff reached a resolution.
    #[must_use]
    pub const fn is_settled(&self) -> bool {
        !matches!(self, Self::Pending | Self::Sent { .. })
    }

    /// Whether the source run is still fenced by this handoff.
    ///
    /// The fence is derived from this status, never from a separate marker:
    /// an unresolved transfer and an accepted one both fence — acceptance
    /// terminates the source in the same compare-and-set that records it —
    /// while a refusal or failure releases the fence and the run resumes.
    #[must_use]
    pub const fn holds_fence(&self) -> bool {
        !matches!(self, Self::Refused { .. } | Self::Failed { .. })
    }

    /// Whether the transfer still awaits the target's durable resolution.
    #[must_use]
    pub const fn awaits_target(&self) -> bool {
        matches!(self, Self::Pending | Self::Sent { .. })
    }

    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Sent { .. } => "sent",
            Self::Accepted { .. } => "accepted",
            Self::Refused { .. } => "refused",
            Self::Failed { .. } => "failed",
        }
    }
}

/// One handoff's durable home on the source run's loop state.
///
/// The cell commits with the send effect and settles in the same
/// compare-and-set that applies the effect's outcome or the task's
/// handoff-result exchange, so the record, the effect, and the status can
/// never disagree about what happened. A run holds at most one handoff cell,
/// ever: a settled refusal or failure may be replaced by a later attempt
/// under a new `(turn, slot)` identity, and an accepted transfer terminates
/// the run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentHandoffCell {
    /// The durable record, persisted before the send.
    pub record: Box<AgentHandoffRecord>,
    /// Where the handoff stands.
    pub status: AgentHandoffStatus,
    /// When the status settled, when it has.
    #[serde(default)]
    pub settled_at: Option<AgentTimestampMillis>,
}

impl AgentHandoffCell {
    /// Creates the pending cell committed alongside the send effect.
    #[must_use]
    pub fn pending(record: Box<AgentHandoffRecord>) -> Self {
        Self {
            record,
            status: AgentHandoffStatus::Pending,
            settled_at: None,
        }
    }

    /// Records the task's durable acknowledgement of the transfer.
    ///
    /// Only a pending cell moves: a settled cell keeps its resolution, and
    /// a duplicate receipt cannot rewrite it.
    pub fn mark_sent(&mut self, target_generation: Option<AgentAssignmentGeneration>) {
        if matches!(self.status, AgentHandoffStatus::Pending) {
            self.status = AgentHandoffStatus::Sent { target_generation };
        }
    }

    /// Settles the cell with the target's durably accepted assignment,
    /// first-writer-wins.
    pub fn settle_accepted(
        &mut self,
        target_run: AgentRunId,
        generation: AgentAssignmentGeneration,
        now: AgentTimestampMillis,
    ) {
        if self.status.is_settled() {
            return;
        }
        self.status = AgentHandoffStatus::Accepted {
            target_run,
            generation,
        };
        self.settled_at = Some(now);
    }

    /// Corrects the cell to the target's durably accepted assignment — the
    /// one transition allowed to rewrite a settled resolution.
    ///
    /// First-writer-wins holds for every ordinary settle, but a locally
    /// settled *failure* — a fenced wind-down, a reconciliation decision an
    /// ambiguously failed write later contradicted — is this run's belief,
    /// while the task's accepted resolution is the durable record of where
    /// responsibility went, and the record wins. An already-accepted cell
    /// keeps its resolution: the correction is idempotent.
    pub fn correct_accepted(
        &mut self,
        target_run: AgentRunId,
        generation: AgentAssignmentGeneration,
        now: AgentTimestampMillis,
    ) {
        if matches!(self.status, AgentHandoffStatus::Accepted { .. }) {
            return;
        }
        self.status = AgentHandoffStatus::Accepted {
            target_run,
            generation,
        };
        self.settled_at = Some(now);
    }

    /// Settles the cell with a definitive refusal, first-writer-wins.
    pub fn settle_refused(&mut self, code: impl Into<String>, now: AgentTimestampMillis) {
        if self.status.is_settled() {
            return;
        }
        self.status = AgentHandoffStatus::Refused { code: code.into() };
        self.settled_at = Some(now);
    }

    /// Settles the cell with a definitive failure, first-writer-wins.
    pub fn settle_failed(&mut self, code: impl Into<String>, now: AgentTimestampMillis) {
        if self.status.is_settled() {
            return;
        }
        self.status = AgentHandoffStatus::Failed { code: code.into() };
        self.settled_at = Some(now);
    }
}

/// The bounded receipt one completed handoff send returns
/// ([specification 14.4](../../../docs/plans/rakka-agent/spec.md)).
///
/// Identities and a status label only. A receipt proves the task durably
/// recorded the transfer — never that the target accepted: acceptance
/// returns later through the handoff-result exchange.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentA2aHandoffReceipt {
    /// The handoff the send carried.
    pub handoff: AgentHandoffId,
    /// The assignment generation the task minted toward the target, when
    /// the receiving surface reported one.
    #[serde(default)]
    pub target_generation: Option<AgentAssignmentGeneration>,
    /// The peer's bounded task-state label at the time of the send.
    pub peer_status: String,
}

impl AgentA2aHandoffReceipt {
    /// Rejects a receipt whose status label exceeds its bound.
    pub fn validate(&self) -> AgentCoordinationResult<()> {
        if self.peer_status.len() > crate::delegation::AGENT_A2A_SEND_STATUS_MAX_BYTES {
            return Err(AgentCoordinationError::ReceiptInvalid {
                message: format!(
                    "the peer status label is {} bytes, which exceeds the {} byte bound",
                    self.peer_status.len(),
                    crate::delegation::AGENT_A2A_SEND_STATUS_MAX_BYTES
                ),
            });
        }
        Ok(())
    }
}

/// Errors of coordination construction and validation.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AgentCoordinationError {
    /// An identity failed validation.
    Identity(AgentIdentityError),
    /// The tool call's arguments do not parse as the declared vocabulary.
    InvalidArguments {
        /// The parse failure detail.
        message: String,
    },
    /// The reason exceeds its byte bound.
    ReasonTooLong {
        /// Serialized size.
        bytes: usize,
        /// The bound.
        maximum: usize,
    },
    /// The context projection carries too many references.
    ContextRefsTooMany {
        /// The reference count.
        count: usize,
        /// The bound.
        maximum: usize,
    },
    /// A context reference is empty or exceeds its byte bound.
    ContextRefInvalid {
        /// The reference's size.
        bytes: usize,
        /// The bound.
        maximum: usize,
    },
    /// The record exceeds its serialized byte bound.
    RecordTooLarge {
        /// Serialized size.
        bytes: usize,
        /// The bound.
        maximum: usize,
    },
    /// The resolved target failed validation.
    TargetInvalid {
        /// The validation failure detail.
        message: String,
    },
    /// A receipt failed validation.
    ReceiptInvalid {
        /// The validation failure detail.
        message: String,
    },
    /// The record or a related value failed to encode.
    Encoding {
        /// The encoding failure detail.
        message: String,
    },
}

impl AgentCoordinationError {
    /// Stable machine-readable code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Identity(_) => "handoff-identity-invalid",
            Self::InvalidArguments { .. } => "handoff-invalid-arguments",
            Self::ReasonTooLong { .. } => "handoff-reason-too-long",
            Self::ContextRefsTooMany { .. } | Self::ContextRefInvalid { .. } => {
                "handoff-context-invalid"
            }
            Self::RecordTooLarge { .. } => "handoff-record-too-large",
            Self::TargetInvalid { .. } => "handoff-target-invalid",
            Self::ReceiptInvalid { .. } => "handoff-receipt-invalid",
            Self::Encoding { .. } => "handoff-record-unencodable",
        }
    }
}

impl Display for AgentCoordinationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Identity(error) => Display::fmt(error, f),
            Self::InvalidArguments { message } => {
                write!(f, "the handoff arguments do not parse: {message}")
            }
            Self::ReasonTooLong { bytes, maximum } => write!(
                f,
                "the handoff reason is {bytes} bytes, which exceeds the {maximum} byte bound"
            ),
            Self::ContextRefsTooMany { count, maximum } => write!(
                f,
                "the handoff projects {count} context references, which exceeds the bound of \
                 {maximum}"
            ),
            Self::ContextRefInvalid { bytes, maximum } => write!(
                f,
                "a handoff context reference is {bytes} bytes; it must be non-empty and at most \
                 {maximum} bytes"
            ),
            Self::RecordTooLarge { bytes, maximum } => write!(
                f,
                "the handoff record is {bytes} bytes, which exceeds the {maximum} byte bound"
            ),
            Self::TargetInvalid { message } => {
                write!(f, "the resolved handoff target is invalid: {message}")
            }
            Self::ReceiptInvalid { message } => {
                write!(f, "the handoff send receipt is invalid: {message}")
            }
            Self::Encoding { message } => {
                write!(f, "the handoff record failed to encode: {message}")
            }
        }
    }
}

impl Error for AgentCoordinationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Identity(error) => Some(error),
            _ => None,
        }
    }
}

impl From<AgentIdentityError> for AgentCoordinationError {
    fn from(error: AgentIdentityError) -> Self {
        Self::Identity(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{AgentId, AgentRunId, TenantId};

    fn scope() -> AgentRunScope {
        AgentRunScope::new(
            TenantId::new("tenant-a"),
            AgentId::new("agent-source").expect("agent"),
            AgentRunId::new("run-1").expect("run"),
        )
        .expect("scope")
    }

    #[test]
    fn handoff_ids_are_pure_and_domain_separated() {
        let first = handoff_id_for(&scope(), 3, 1).expect("id");
        let replay = handoff_id_for(&scope(), 3, 1).expect("id");
        assert_eq!(first, replay);
        assert!(first.as_str().starts_with(AGENT_HANDOFF_ID_PREFIX));
        let other_slot = handoff_id_for(&scope(), 3, 2).expect("id");
        assert_ne!(first, other_slot);
        let delegation =
            crate::delegation::delegation_id_for(&scope(), 3, 1).expect("delegation id");
        assert_ne!(
            first.as_str().trim_start_matches(AGENT_HANDOFF_ID_PREFIX),
            delegation
                .as_str()
                .trim_start_matches(crate::delegation::AGENT_DELEGATION_ID_PREFIX),
            "a handoff and a delegation at the same coordinate must never share a digest"
        );
    }

    #[test]
    fn handoff_result_operation_ids_are_pure() {
        let tenant = TenantId::new("tenant-a");
        let handoff = handoff_id_for(&scope(), 1, 0).expect("id");
        let first = handoff_result_operation_id(&tenant, &handoff).expect("operation");
        let replay = handoff_result_operation_id(&tenant, &handoff).expect("operation");
        assert_eq!(first, replay);
    }

    #[test]
    fn descriptor_kinds_align() {
        let handoff = AgentCoordinationCapability::Handoff(AgentHandoffPolicy::new(
            AgentToolId::new("transfer").expect("tool"),
            AgentRevisionNumber::new(1),
        ));
        assert_eq!(handoff.kind(), AgentCoordinationCapabilityKind::Handoff);
        assert_eq!(handoff.as_label(), "handoff");
        assert_eq!(handoff.revision(), AgentRevisionNumber::new(1));
    }

    #[test]
    fn tool_call_parse_fails_closed_on_unknown_fields() {
        let arguments = serde_json::json!({
            "skill": "billing-transfer",
            "reason": "needs billing authority",
            "target_agent": "agent-b",
        });
        let error = AgentHandoffToolCall::parse(&arguments).expect_err("must refuse");
        assert!(matches!(
            error,
            AgentCoordinationError::InvalidArguments { .. }
        ));
    }

    #[test]
    fn settlement_is_first_writer_wins() {
        let record = AgentHandoffRecord {
            handoff: handoff_id_for(&scope(), 1, 0).expect("id"),
            goal: None,
            task: AgentTaskId::new("task-1").expect("task"),
            source_run: scope(),
            source_generation: AgentAssignmentGeneration::new(1),
            requested_skill: AgentCapabilityId::new("billing").expect("skill"),
            resolved: AgentDelegationTarget::new(
                AgentId::new("agent-target").expect("agent"),
                crate::definition::AgentTaskDefinitionId::new("refund").expect("definition"),
            ),
            reason: "needs billing authority".into(),
            policy_revision: AgentRevisionNumber::new(1),
            definition_revision: AgentRevisionNumber::new(1),
            settings_revision: AgentRevisionNumber::new(1),
            context: Vec::new(),
            a2a_message_id: "handoff-x".into(),
            deduplication_key: "handoff-x".into(),
            turn: 1,
            slot: 0,
            effect: AgentEffectId::new("effect-1"),
            call_id: AgentToolCallId::new("call-1").expect("call"),
            telemetry: AgentTelemetryContext::default(),
            created_at: AgentTimestampMillis::new(1),
        };
        record.validate().expect("valid");
        let mut cell = AgentHandoffCell::pending(Box::new(record));
        assert!(cell.status.holds_fence());
        assert!(cell.status.awaits_target());
        cell.mark_sent(Some(AgentAssignmentGeneration::new(2)));
        assert!(matches!(cell.status, AgentHandoffStatus::Sent { .. }));
        cell.settle_refused("handoff-target-refused", AgentTimestampMillis::new(2));
        assert!(!cell.status.holds_fence());
        cell.settle_accepted(
            AgentRunId::new("task-1-gen-2").expect("run"),
            AgentAssignmentGeneration::new(2),
            AgentTimestampMillis::new(3),
        );
        assert!(
            matches!(cell.status, AgentHandoffStatus::Refused { .. }),
            "a settled cell must keep its resolution"
        );
    }
}
