//! The external system, outside every pod.
//!
//! [Specification 18](../../../docs/plans/rakka-agent/spec.md) closes its
//! recovery scenarios with the fault-injection directive this harness exists
//! to satisfy: kill the dispatcher or owner pod at every durable effect
//! boundary, *including after a test external system commits but before it
//! returns the receipt*. The in-process
//! [`rakka_agent::testkit::RecordingToolExecutor`] records invocations in a
//! `Mutex` — which dies with the pod that made them, so a pod killed after the
//! external commit leaves no evidence the commit happened.
//!
//! This ledger is an append-only file in the shared directory. Its whole
//! purpose is to outlive the pod that wrote to it.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use rakka_agent::{
    AgentModelAdapter, AgentModelFuture, AgentModelRequest, AgentModelRetryPolicy, AgentModelTurn,
    AgentRevisionNumber, AgentTaskContent, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};

/// The ledger file's name inside the shared directory.
const LEDGER: &str = "external.log";

/// Appends one line to the shared ledger, creating it if needed.
///
/// One `write_all` of the line *and* its newline, never `writeln!`. `writeln!`
/// on an unbuffered file issues one write per format piece — the payload, then
/// the newline — and this harness aborts pods from arbitrary threads: an abort
/// landing between the two leaves a line with no terminator that the recovering
/// pod's own append then merges with, so two external calls read back as one
/// entry and the oracle below reports a clean single turn. Two pods appending
/// concurrently can interleave the same way. Under `O_APPEND` a single write is
/// atomic, which is what makes a line a line.
fn append(root: &Path, line: &str) {
    use std::io::Write as _;

    let _ = std::fs::create_dir_all(root);
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(root.join(LEDGER))
    {
        let _ = file.write_all(format!("{line}\n").as_bytes());
        let _ = file.sync_all();
    }
}

/// Every line the ledger holds, oldest first.
#[must_use]
pub fn ledger_entries(root: &Path) -> Vec<String> {
    std::fs::read_to_string(root.join(LEDGER))
        .unwrap_or_default()
        .lines()
        .map(ToString::to_string)
        .collect()
}

/// A model adapter that commits to the shared ledger before it answers.
///
/// The order is the point: the line is on disk, `sync_all`'d, *before* the turn
/// is returned to the run — so a pod that dies between them has committed
/// externally and recorded nothing durable of its own. That is the ambiguous
/// window, and the recovery a later pod performs must not turn it into two
/// commits under the same identity.
#[derive(Debug, Clone)]
pub struct LedgerModelAdapter {
    root: Arc<PathBuf>,
    answer: String,
}

impl LedgerModelAdapter {
    /// An adapter whose turns propose `answer` and whose calls are ledgered.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>, answer: impl Into<String>) -> Self {
        Self {
            root: Arc::new(root.into()),
            answer: answer.into(),
        }
    }
}

impl AgentModelAdapter for LedgerModelAdapter {
    fn adapter_version(&self) -> AgentRevisionNumber {
        CURRENT_AGENT_LOOP_ADAPTER_VERSION
    }

    fn retry_policy(&self) -> AgentModelRetryPolicy {
        AgentModelRetryPolicy::DEFAULT
    }

    fn call<'a>(&'a self, request: &'a AgentModelRequest) -> AgentModelFuture<'a> {
        Box::pin(async move {
            // The external commit, before the receipt exists anywhere.
            //
            // The snapshot id is what makes this line an *identity* rather than
            // a turn number. It is derived from the run's full scope and the
            // turn, so a retry of this turn by a recovering pod appends a
            // byte-identical line, while a second run under a different
            // assignment generation — the thing recovery must never produce —
            // derives a different one and shows up as a distinct entry.
            append(
                &self.root,
                &format!(
                    "model-call turn={} snapshot={}",
                    request.turn,
                    request.context.snapshot_id.as_str()
                ),
            );
            Ok(AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
                .with_text("Resolved.")
                .with_proposal(
                    AgentTaskContent::inline(serde_json::json!({ "answer": self.answer }))
                        .expect("the proposal is inline-bounded"),
                ))
        })
    }
}
