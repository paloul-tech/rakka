//! The durable agent loop.
//!
//! Owns the loop phase enum and the versioned durable loop-state record, plus
//! the execution rule that governs every handler: a transition is bounded, and
//! it persists the next effect or the next wait before it returns. Nothing that
//! matters to the loop lives only in memory, so a crash at any point resumes
//! from the last persisted transition rather than replaying model or tool work.
//!
//! Specification: sections 9.4 and 9.5. Filled by slice 1.5, and retrofitted
//! onto the full effect state machine of [`crate::effect`] in slice 1.7.
