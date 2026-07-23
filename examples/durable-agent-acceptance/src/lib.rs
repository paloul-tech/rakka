//! The M1 acceptance walk: one sharded Rakka Agent demonstrating every
//! bullet of the initial acceptance statement
//! (`docs/plans/rakka-agent/spec.md`, section 22) with the deterministic
//! model adapter.
//!
//! The binary prints one stable line per bullet; `tests/acceptance.rs` runs
//! the same flow and asserts the transcript verbatim, plus the typed facts
//! behind it, so the README's documented stdout cannot rot. Everything is
//! in-process and deterministic: real `ClusterSharding` over all three
//! entity types, the in-process A2A service core (no HTTP), the production
//! effect dispatcher, and in-memory durable stores.

#![forbid(unsafe_code)]

pub mod flow;
pub mod report;
pub mod wiring;

pub use flow::{run_acceptance, CONTENT_SENTINELS};
pub use report::AcceptanceReport;
