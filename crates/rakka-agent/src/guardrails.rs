//! The guardrail chain.
//!
//! Owns the versioned, ordered guardrail stages that run at the model, tool,
//! A2A, and memory boundaries, their bounded outcome set, and the rule that a
//! stage may transform deterministically or block, but never introduce
//! nondeterministic I/O into a durable transition. Stages a deployment marks
//! mandatory cannot be removed by an agent definition or a run setup.
//!
//! Specification: section 16. Filled by slice 1.8.
