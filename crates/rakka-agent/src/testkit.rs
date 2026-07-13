//! Deterministic test support.
//!
//! Owns the deterministic model adapter — scripted text and results, structured
//! task-result proposals, tool and delegation requests, and responses
//! conditional on prior messages or tool results — plus the fake tools, peers,
//! and crash points the recovery scenarios drive. The adapter implements the
//! trait in [`crate::model`] and is available without the `rig` feature, and it
//! exercises the same durable effect path as a production adapter rather than a
//! shortcut around it.
//!
//! Specification: sections 10.4 and 18. Filled by slice 1.6 and extended with
//! the fault-injection crash points in slice 1.14.
