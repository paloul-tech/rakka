//! The hierarchical escrow budget ledger.
//!
//! Owns the escrow model: a parent debits an allocation inside the transition
//! that creates a child and carries it on the creation command, so a child can
//! never oversubscribe its parent. Dispatch-time reservation touches only the
//! run's own ledger and is a single-entity transition. Settlement and return
//! travel back up as deduplicated exchanges, and exhaustion parks the scope
//! with a structured top-up request rather than failing it silently.
//!
//! Both `Started` and `Indeterminate` attempts consume budget: work whose
//! outcome is unknown has still been paid for.
//!
//! Specification: section 9.7, with the continuous budget windows of 8.2.
//! Filled by slice 1.9; goal-scope windows by slice 3.3.
