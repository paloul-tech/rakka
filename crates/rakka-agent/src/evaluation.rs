//! Progress, evidence, and completion.
//!
//! Owns the evaluator contract — deterministic assertions, authoritative
//! queries, verification workflows, an evaluator model or agent under a
//! distinct policy, and human review — all executed as durable effects with a
//! persisted outcome, evidence references, and the criteria revision they were
//! judged against. A goal becomes `Satisfied` only through an evaluation of the
//! current criteria revision against durable evidence, never because a model
//! said so.
//!
//! Also owns stagnation detection — repetition fingerprints and no-progress
//! epochs — feeding a deterministic continue, replan, wait, escalate, or
//! terminate policy.
//!
//! Specification: section 8.3. Filled by slice 4.2.
