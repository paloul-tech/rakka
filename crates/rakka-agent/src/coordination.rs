//! Coordination capabilities: handoff, teams, and moderation.
//!
//! Owns `AgentCoordinationCapability` descriptors, which are trusted definition
//! and setup data. The runtime may expose a capability to the model as a tool,
//! but model output can never create the capability, its target, its budget, or
//! its scope.
//!
//! Handoff keeps the same `AgentTaskId`: the source run is fenced, a target run
//! is created, context and artifacts are projected explicitly rather than
//! inherited, and `HandedOff` is recorded only after the target durably
//! accepts. Team coordination owns `AgentTeamId`, bounded membership, and a
//! durable shared task board whose claims, releases, and transfers are atomic
//! under revision and lease fencing. Moderation owns `AgentConversationId`,
//! the participant set, durable turn and round state, and transcript artifacts,
//! where only the current participant may submit and duplicates are rejected.
//!
//! Every one of these exchanges travels the outbox, inbox, and `rakka-a2a` path
//! even when the participants are colocated, and idle teams, boards, and
//! participants passivate — the board is data, not a resident process.
//!
//! Specification: sections 8.8 through 8.11. Filled by phase 5.
