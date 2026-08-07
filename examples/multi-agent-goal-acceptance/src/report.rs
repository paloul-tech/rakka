//! The acceptance report: the transcript the binary prints and the typed
//! facts the integration test asserts.

/// What one full acceptance walk produced.
#[derive(Debug)]
pub struct AcceptanceReport {
    /// One stable line per milestone bullet, in the walk's order. These are
    /// the binary's stdout and the README's documented transcript.
    pub lines: Vec<String>,
    /// The two distinct specialist child task ids the delegations created.
    pub child_tasks: Vec<String>,
    /// The workflow invocation id — verbatim the child workflow run id and
    /// the `StartRun` deduplication key.
    pub invocation_id: String,
    /// Durable `StartRun` entries the child workflow inbox ever accepted.
    pub inbox_start_entries: usize,
    /// How many times the compiled refund step executed.
    pub refund_step_executions: usize,
    /// How many times the summarizer's non-idempotent tool was invoked.
    pub tool_invocations: usize,
    /// The distinct external idempotency keys that tool saw.
    pub tool_idempotency_keys: usize,
    /// Resident entities while the fan-out waited.
    pub resident_at_wait: usize,
    /// The refusal code the direct criteria decision was answered with.
    pub unattested_code: String,
    /// The goal's terminal status label.
    pub goal_status: String,
    /// Whether the recorded communal claim's provenance names the delegation.
    pub claim_provenance_has_delegation: bool,
    /// Task nodes the authorized goal view assembled.
    pub view_tasks: usize,
    /// Run nodes the authorized goal view assembled.
    pub view_runs: usize,
    /// Delegation edges the authorized goal view assembled.
    pub view_delegations: usize,
    /// Workflow links the authorized goal view assembled.
    pub view_workflow_links: usize,
    /// Evaluation views the authorized goal view assembled.
    pub view_evaluations: usize,
    /// Evidence items on the terminal decision's evaluation reference.
    pub view_evidence: usize,
    /// Joined communal-claim references on the authorized goal view.
    pub view_claims: usize,
    /// Escrow children the root still held open at the end.
    pub escrow_outstanding: usize,
    /// Every serialized surface swept for planted content sentinels.
    pub surfaces: Vec<String>,
}

/// The exact transcript one acceptance walk prints, one line per milestone
/// bullet. `tests/acceptance.rs` asserts the produced lines equal this, and
/// the README quotes it verbatim — a single source for all three.
pub const EXPECTED_TRANSCRIPT: &[&str] = &[
    "ok  1/18 three agents instantiated; the goal-bearing root created: goal Active with an evaluator, two specialist skills, one versioned workflow tool, one shared knowledge space",
    "ok  2/18 one turn committed the fan-out atomically: 2 delegation records, 1 workflow invocation, a closed 3-member fan-in group",
    "ok  3/18 both sends created real children through rakka-a2a: two distinct child tasks, each with its own generation-1 run and verified provenance",
    "ok  4/18 a replayed delegation send converged on the same child: the deduplication key answered, no second child",
    "ok  5/18 the workflow start accepted the derived StartRun into the compiled refund v1 child's durable inbox: invocation id = child run id = deduplication key",
    "ok  6/18 the fan-out persisted and the root waits with nothing resident: AwaitingChildren, 0 outstanding effects, 0 resident entities",
    "ok  7/18 ROOT pod loss: the passivated root re-materialized on the next ask; a killed result write was redelivered by the child's re-driven settle and converged on one recorded result",
    "ok  8/18 the translator's terminal task owed its delegation result; recorded on the root without resolving the 3-member group",
    "ok  9/18 the translator's claim appended into the shared space with attributable provenance (goal, task, run, delegation stamped); its replay answered the original claim",
    "ok 10/18 CHILD pod loss: the summarizer's worker died after invoking the non-idempotent tool; recovery parked one Indeterminate outcome — invoked exactly once, never re-invoked",
    "ok 11/18 the deduplicated reconciliation decision resolved the ambiguity; the summarizer completed and its result recorded on the root",
    "ok 12/18 a workflow start replayed after both losses adopted the SAME child run — one durable StartRun ever — and the compiled step executed exactly once",
    "ok 13/18 a direct criteria decision was refused task-goal-decision-unattested: with an evaluator configured, Satisfied has one door",
    "ok 14/18 the EvaluateGoal effect ran the configured evaluator over durable evidence; the goal-evaluation exchange recorded Satisfied fenced to the assigned root run",
    "ok 15/18 the application relayed the deduplicated workflow result; the group resolved all-settled and fan-in resumed the model deterministically",
    "ok 16/18 the root proposed its own validated result: run and task Completed, escrow returned across root and children",
    "ok 17/18 the authorized goal view reconstructed the goal, 3 tasks, 3 runs, 2 delegations, 1 workflow link, 1 evaluation, and its evidence from durable state alone",
    "ok 18/18 no private content leaked: planted sentinels appear in no claim, no goal view, no snapshot, no metric",
];
