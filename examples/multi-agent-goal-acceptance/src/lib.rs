//! The M4 acceptance walk: one durable collaborative goal demonstrating
//! every bullet of the multi-agent goal milestone
//! (`docs/plans/rakka-agent/spec.md`, section 22) with deterministic model
//! adapters.
//!
//! One root goal delegates bounded work to two specialist Rakka Agents
//! through the real in-process `rakka-a2a` service core, invokes one
//! compiled workflow through its versioned tool descriptor, survives root
//! and child pod loss, records an attributable communal claim, and reaches
//! `Satisfied` only through the configured evaluator — reconstructing the
//! whole tree afterwards through the authorized goal view. The binary prints
//! one stable line per bullet; `tests/acceptance.rs` runs the same flow and
//! asserts the transcript verbatim plus the typed facts behind it, so the
//! README's documented stdout cannot rot.
//!
//! Cancellation propagation is deliberately not a walk beat: `rakka-agent`'s
//! own suite proves it (specification 18, items 29, 31, and 33), and the goal
//! view's cancellation surface is pinned by `tests/goal_view.rs` there.

#![forbid(unsafe_code)]

pub mod flow;
pub mod report;
pub mod wiring;

pub use flow::{run_acceptance, CONTENT_SENTINELS};
pub use report::AcceptanceReport;
