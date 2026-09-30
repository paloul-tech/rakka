//! The M5 acceptance walk: one durable coordination story demonstrating
//! every bullet of the coordination-capability milestone
//! (`docs/plans/rakka-agent/spec.md`, section 22) with deterministic model
//! adapters.
//!
//! One `AgentTaskId` is posted to a team board, atomically claimed, handed
//! off to a specialist, blocked on a human-owned approval, reviewed in a
//! moderated conversation, and finally closed — over all five real sharded
//! entity types, with every coordination command travelling the in-process
//! `rakka-a2a` service core. The binary prints one stable line per bullet;
//! `tests/acceptance.rs` runs the same flow and asserts the transcript
//! verbatim plus the typed facts behind it, so the README's documented
//! stdout cannot rot.
//!
//! Delegation and workflow tools are deliberately not walk beats: the M4
//! milestone proves them in `examples/multi-agent-goal-acceptance`, and this
//! walk is the coordination capabilities that milestone did not cover.

#![forbid(unsafe_code)]

pub mod flow;
pub mod report;
pub mod wiring;

pub use flow::{run_acceptance, CONTENT_SENTINELS};
pub use report::AcceptanceReport;
