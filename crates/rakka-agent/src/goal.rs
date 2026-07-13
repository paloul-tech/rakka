//! The goal contract and its lifecycle.
//!
//! Owns `AgentGoalSpec` and `AgentGoalStatus`, including the distinction
//! between a goal that is `Unsatisfied` and one that has `Failed`, and the
//! budget-exhaustion parking and escalation policy at goal scope. The root
//! `AgentTaskEntity` coordinates the goal; `AgentGoalId` defaults to the root
//! `AgentTaskId` value while the two types stay distinct. A goal stays
//! addressable while fully passivated.
//!
//! Specification: sections 8.1 and 6.3, with the continuous clauses used by
//! [`crate::wake`]. Filled by slice 4.1; the continuous fields land earlier, in
//! slice 3.1.
