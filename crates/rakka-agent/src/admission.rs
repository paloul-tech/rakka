//! Autonomy admission.
//!
//! Owns `AutonomyAdmissionDecision`: the fail-closed check that decides whether
//! an unattended class of work may run at all. Admission is rechecked when an
//! update widens what a run may do, and the immediate-safety dimensions are
//! rechecked again at dispatch, so a settings change during a wait cannot let
//! an already-parked attempt through on stale terms.
//!
//! Specification: section 7.4. Filled by slice 1.9, extending the existing
//! `rakka-agent-workflow` autonomy counters into the ledger dimensions of
//! [`crate::budget`].
