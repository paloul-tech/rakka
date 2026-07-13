//! Bounded, authoritative operational queries.
//!
//! Owns `AgentOperationalSnapshot` and the session view assembled by
//! `AgentRunId`: point queries answered from durable state, bounded in the work
//! they do, and correct even when telemetry is entirely unavailable. An
//! operator asking what an agent is doing gets an answer from the same records
//! the runtime acts on, not from a metrics pipeline.
//!
//! Specification: section 17.18. Filled by slice 1.13; continuous and
//! multi-agent projections extend it in phases 3 and 4.
