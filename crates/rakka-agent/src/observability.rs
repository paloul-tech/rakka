//! Structured decision events, spans, and metrics.
//!
//! Owns the bounded trace segments with persisted W3C context and links across
//! every durable boundary — no span stays open across a wait — the structured
//! decision events that explain why the runtime did what it did, and the
//! bounded metric set, which never carries an identifier in a label.
//!
//! Content capture is disabled by default. Runtime events are observability and
//! never the correctness source: the durable run, inbox, and outbox state is.
//! An operational answer must therefore stay correct with telemetry entirely
//! unavailable, which is what [`crate::query`] guarantees.
//!
//! Specification: section 17. Filled by slice 1.13, reusing the existing
//! `rakka-agent-workflow` trace-context and OTLP substrate.
