//! The acceptance report: the transcript the binary prints and the typed
//! facts the integration test asserts.

/// What one full M3 acceptance walk produced.
#[derive(Debug)]
pub struct AcceptanceReport {
    /// One stable line per milestone fact, in the walk's order. These are
    /// the binary's stdout and the README's documented transcript.
    pub lines: Vec<String>,
    /// Epoch task records durably created, one per admitted occurrence.
    pub epoch_tasks: usize,
    /// The monotone admitted count the controller settled on.
    pub admitted: u64,
    /// Occurrences durably coalesced behind an active epoch.
    pub coalesced: u64,
    /// Backlog occurrences absorbed as missed behind the representative.
    pub missed: u64,
    /// Deliveries fenced for carrying an obsolete schedule revision.
    pub fenced: u64,
    /// Occurrences parked by an exhausted goal window.
    pub deferred: u64,
    /// Occurrences parked by a failure backoff.
    pub backed_off: u64,
    /// Controller-originated re-wakes consumed.
    pub retried: u64,
    /// Deliveries refused by an absorbing lifecycle status.
    pub barred: u64,
    /// The stable error code the stale former owner's write was fenced with.
    pub stale_owner_code: String,
    /// The renewed effective expiry the re-recovered owner observed.
    pub renewed_expiry: u64,
    /// Escrow children still outstanding on the root at the end — every
    /// epoch's budget settled and returned.
    pub escrow_outstanding: usize,
    /// Whether any wake-timer entry was still pending at the end.
    pub pending_wake: bool,
}

/// The exact transcript one acceptance walk prints. `tests/acceptance.rs`
/// asserts the produced lines equal this, and the README quotes it verbatim
/// — a single source for all three.
pub const EXPECTED_TRANSCRIPT: &[&str] = &[
    "ok  1/16 the continuous root is durable and passivatable: controller state persisted, no resident actor, loop, or timer",
    "ok  2/16 a scheduled occurrence admitted one derived epoch task and run; the epoch ran to completion and its result released the occurrence",
    "ok  3/16 the replayed admission answered Duplicate from the durable record: one admission, one epoch",
    "ok  4/16 overlap is forbidden: the next occurrence admitted and the one behind it coalesced durably",
    "ok  5/16 the owner died mid-settlement; the rebuilt owner replayed the same exchange and converged: one release, one promotion",
    "ok  6/16 a downtime backlog of 3 missed occurrences admitted one coalesced representative and absorbed 2 as missed",
    "ok  7/16 the failed epoch backed off; the durable backoff re-wake retried and admitted the occurrence parked behind it",
    "ok  8/16 a second consecutive failure escalated: the goal auto-suspended durably",
    "ok  9/16 the stale-revision resume was fenced with wake-stale-lifecycle-revision; the current-revision resume reactivated the goal and cleared the backoff",
    "ok 10/16 the schedule update to revision 2 took the windowed policy into force and fenced a stale revision-1 delivery terminally",
    "ok 11/16 the exhausted goal window deferred the next occurrence and parked a durable window-turn re-wake at the boundary",
    "ok 12/16 the window turned: the parked re-wake's scan promoted the deferred occurrence; the ledger survived every rebuild",
    "ok 13/16 the renewal extended expiry to 50000000; a stale former owner's write was fenced with revision-conflict and re-recovery converged",
    "ok 14/16 each epoch is its own derived finite task and run with a bounded occurrence input; continuity lives only in the root's durable controller",
    "ok 15/16 the goal retired after its 9th admitted occurrence; a later delivery was barred and its entry marked terminal",
    "ok 16/16 the operational query answered from durable state alone: schedule revision 2, lifecycle retired, 9 admitted, 2 missed, 1 coalesced, 1 fenced, no pending wake",
];
