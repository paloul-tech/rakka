//! The M3 acceptance walk: one continuous goal demonstrating every bullet
//! of the continuous-goal milestone checklist
//! (`docs/plans/rakka-agent/spec.md`, "Continuous Goal Milestone (M3)")
//! with fault injection across pod restart and shard movement.
//!
//! The binary prints one stable line per milestone fact; `tests/acceptance.rs`
//! runs the same flow and asserts the transcript verbatim, plus the typed
//! facts behind it, so the README's documented stdout cannot rot. Everything
//! is in-process and deterministic: durable in-memory stores, the real wake
//! scanner over the real durable wake-timer index, the real entity stores
//! rebuilt from durable state at every step, and the deterministic model
//! adapter for the one epoch that runs end to end.

#![forbid(unsafe_code)]

pub mod flow;
pub mod report;
pub mod wiring;

pub use flow::run_acceptance;
pub use report::AcceptanceReport;
