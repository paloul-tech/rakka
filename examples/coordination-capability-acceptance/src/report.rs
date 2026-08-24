//! The acceptance report: the transcript the binary prints and the typed
//! facts the integration test asserts.

/// What one full acceptance walk produced.
#[derive(Debug)]
pub struct AcceptanceReport {
    /// One stable line per milestone bullet, in the walk's order. These are
    /// the binary's stdout and the README's documented transcript.
    pub lines: Vec<String>,
    /// The one `AgentTaskId` the whole walk carries, from board post to
    /// completion — the identity a handoff must preserve.
    pub task_id: String,
    /// Assignment generations the task ever minted.
    pub generations: u32,
    /// The stable code the uncapable member's claim was refused with.
    pub claim_refusal_code: String,
    /// The stable code the uncapable speaker's turn was refused with.
    pub turn_refusal_code: String,
    /// Resident entities while the board waited for a claim.
    pub resident_at_wait: usize,
    /// The source run's terminal status label after the transfer.
    pub source_status: String,
    /// The agent that owns the task after the handoff.
    pub owner_after_handoff: String,
    /// How many transfers the handoff send executor was ever asked for,
    /// counted at the executor itself: once into the injected owner loss,
    /// once by the re-drive that completed.
    pub transfers_attempted: usize,
    /// How many times the human-owned upstream accepted a result.
    pub human_results_accepted: usize,
    /// Whether the human result resolved the dependent's edge *and* flipped
    /// its decision graph back to satisfied.
    pub dependent_unblocked: bool,
    /// Whether the consequential effect parked on a real bound checkpoint
    /// that the human result left open — the registry declares the gate and
    /// the run's durable state holds it.
    pub checkpoint_gated_effect: bool,
    /// How many times the non-idempotent effect actually ran.
    pub effect_invocations: usize,
    /// Turns the moderated conversation recorded.
    pub turns_recorded: usize,
    /// Turns the conversation recorded after its pod loss and replay.
    pub turns_after_recovery: usize,
    /// The conversation's terminal reason code.
    pub conversation_terminal: String,
    /// The board entry's status once the task ended.
    pub board_entry_status: String,
    /// Coordination events replayed from a cursor across every scope.
    pub replayed_events: usize,
    /// Whether a truncated window answered `WindowExpired` with a floor that
    /// resumed for real.
    pub window_expired_resumed: bool,
    /// Every serialized surface swept for planted content sentinels.
    pub surfaces: Vec<String>,
}

/// The exact transcript one acceptance walk prints, one line per milestone
/// bullet. The walk assigns each line from this array once its asserts have
/// proven the line's facts, and `tests/acceptance.rs` pins the README's
/// quoted block to it — the transcript has exactly one source.
pub const EXPECTED_TRANSCRIPT: &[&str] = &[
    "ok  1/16 five sharded entity types over real ClusterSharding: four agents, one team, and two board tasks posted deliberately unassigned",
    "ok  2/16 two members claimed concurrently through rakka-a2a: one owner admitted at generation 1, the loser's stale-epoch command failed closed",
    "ok  3/16 TEAM ENVELOPE: a member whose definition never granted Team was refused team-coordination-unauthorized and its board entry reopened",
    "ok  4/16 the board waited with nothing resident: 0 resident entities, and the claim activated across the passivation",
    "ok  5/16 the owner's model turn transferred the SAME AgentTaskId: the handoff record and its A2aSendCall committed in one compare-and-set, fencing the source",
    "ok  6/16 HandedOff recorded strictly after the target's durable acceptance: one task id, one new generation, and no session or private memory travelled",
    "ok  7/16 HANDOFF POD LOSS: the task store died mid-transfer; recovery re-derived the HandoffResult and converged on one transfer",
    "ok  8/16 a human-owned approval was declared upstream of the ticket: the dependency edge registered with the upstream in the declaring transition, and the dependent's decision graph read unsatisfied",
    "ok  9/16 an authenticated human result completed the upstream through rakka-a2a and unblocked the dependent; a replayed submission echoed the original",
    "ok 10/16 CHECKPOINT BOUNDARY: the consequential effect still parked on a bound AgentCheckpoint — the human result resolved no checkpoint and invoked nothing",
    "ok 11/16 the moderated conversation ran its bounded rounds in order; the dense turn ledger absorbed a replayed turn without recording a second",
    "ok 12/16 MODERATION ENVELOPE: a rostered speaker whose definition never granted Moderation was refused conversation-moderation-unauthorized",
    "ok 13/16 CONVERSATION POD LOSS: the conversation store died mid-round; participant, round, turn owner, transcript, and budgets recovered without duplicating a turn",
    "ok 14/16 the conversation's terminal reached the governing task, and the terminal task closed its board entry with the claim epoch bumped",
    "ok 15/16 the coordination log replayed from a cursor across task, team, and conversation with no gap or repeat; a truncated window answered WindowExpired with a floor that resumed",
    "ok 16/16 no content crossed onto a coordination surface: planted sentinels appear in no board, no replay page, and no metric",
];
