//! Multi-pod fault and soak validation for the durable agent domain.
//!
//! See the crate README for what this harness proves and how to run it. It
//! re-proves specification 18 scenarios 1, 2, and 60 at multi-pod fidelity
//! (for 1, the creation-deduplication half): a real pod loss, a shared
//! durable store outside the dying process, and a survivor that downs the
//! departed pod, takes over its shards, and finishes from the record. Which
//! scenarios need that fidelity, and why, is recorded in
//! `docs/rakka-agent-fault-injection-matrix.md`; the roster in
//! `docs/rakka-agent-recovery-scenarios.md` cites this crate for those rows.

pub mod codec;
pub mod external;
pub mod flow;
pub mod stores;
pub mod wiring;
