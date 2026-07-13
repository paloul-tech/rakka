//! The continuous wake controller.
//!
//! Owns `AgentWakeId` — goal plus `ScheduleRevision` plus logical occurrence,
//! so the same occurrence reached from any trigger path yields one identity —
//! the versioned `AgentWakePolicy`, and the durable controller built over the
//! `rakka-agent-workflow` timer and trigger substrate: overlap forbidden by
//! default, durable coalescing, at most one occurrence after downtime, and
//! revision fencing. An admitted wake produces exactly one finite child task
//! and run epoch.
//!
//! Scanner and pod uptime never create an occurrence; only durable logical time
//! does. Continuity across epochs comes from controller state, private memory,
//! and artifacts, never from a resident process.
//!
//! Specification: sections 8.2 and 6.9. Filled by phase 3.
