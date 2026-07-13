//! The typed agent client.
//!
//! Owns `RakkaAgentClient`, the typed facade applications use to create tasks,
//! submit settings and administrative commands, and subscribe to replayable
//! task and run events. Every call travels the same durable command path as an
//! external request: there is no local actor shortcut, so a client call and an
//! A2A call converge on the same durable inbox and the same deduplication.
//!
//! Specification: section 14.5. Filled by slice 1.12; coordination events extend
//! the replay in phase 5.
