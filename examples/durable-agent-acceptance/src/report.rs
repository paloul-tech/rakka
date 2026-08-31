//! The acceptance report: the transcript the binary prints and the typed
//! facts the integration test asserts.

/// What one full acceptance walk produced.
#[derive(Debug)]
pub struct AcceptanceReport {
    /// One stable line per spec 22 bullet, in the statement's order. These
    /// are the binary's stdout and the README's documented transcript.
    pub lines: Vec<String>,
    /// The single durable task identity the deduplicated A2A sends mapped to.
    pub task_id: String,
    /// How many times the external tool was actually invoked.
    pub tool_invocations: usize,
    /// The distinct external idempotency keys the tool saw.
    pub tool_idempotency_keys: usize,
    /// Session-memory entries the completed run persisted.
    pub session_entries: usize,
    /// Immutable context snapshots the completed run persisted.
    pub context_snapshots: usize,
    /// The bounded `rakka.agent.*` metric names observed, sorted.
    pub metric_names: Vec<String>,
    /// Decision events still owed because the sink was unavailable.
    pub decisions_owed: usize,
    /// Trace segments assembled into the session view.
    pub trace_segments: usize,
    /// Every serialized telemetry surface, for the no-content assertion.
    pub telemetry_surfaces: Vec<String>,
}

/// The exact transcript one acceptance walk prints, one line per spec 22
/// bullet. `tests/acceptance.rs` asserts the produced lines equal this, and
/// the README quotes it verbatim — a single source for all three.
pub const EXPECTED_TRANSCRIPT: &[&str] = &[
    "ok  1/18 instantiated with versioned settings: revision 1",
    "ok  2/18 duplicate A2A sends mapped to one task task-be0e74d4fdbd9e6f and its initial run",
    "ok  3/18 the typed result passed rule answer-present before the task completed",
    "ok  4/18 admission fails closed: not-admitted before the decision, not-admitted again after a widening definition",
    "ok  5/18 budgets settled durably: 2 loop iterations, 2 model calls, 3 effect attempts, escrow returned",
    "ok  6/18 fully passivated (0 resident entities) and still addressable: the describe ask re-materialized the owner",
    "ok  7/18 both model turns executed through dispatcher worker-1",
    "ok  8/18 the session view assembled 1 correlated trace segments by run id",
    "ok  9/18 bounded metrics observed: rakka.agent.recovery.duration, rakka.agent.recovery.events, rakka.agent.run.transitions, rakka.agent.telemetry.flush.failures, rakka.agent.turn.duration",
    "ok 10/18 short-term session context persisted: 3 entries, 2 immutable snapshots",
    "ok 11/18 each effectful call is its own durable effect: model and tool ticketed separately",
    "ok 12/18 the checkpoint-required tool parked the run WaitingForApproval, passivated",
    "ok 13/18 recovered after dispatcher loss (worker died mid-attempt) and owner loss (decision write killed, redelivered)",
    "ok 14/18 the ambiguous non-idempotent tool parked one Indeterminate outcome; invoked exactly once, never re-invoked",
    "ok 15/18 resumed only after the deduplicated reconciliation decision; its replay answered Duplicate",
    "ok 16/18 the authoritative snapshot answered from durable state with no telemetry wired into the query",
    "ok 17/18 default telemetry carries no prompt, tool payload, memory content, or credential material",
    "ok 18/18 the unavailable decision sink blocked nothing: the run completed, flush failures are a bounded metric, owed events are visible",
];
