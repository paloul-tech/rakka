//! Agent definition, settings revisions, and run setup envelopes.
//!
//! Owns `AgentDefinitionRevision`, `SettingsRevision` with its three timing
//! classes — turn-bound, immediate safety, and run-pinned — and
//! `AgentSetupRevision`, whose envelope may only narrow what the definition
//! already permits. A setup that widens authority is rejected at validation and
//! again at dispatch.
//!
//! Specification: sections 7.1 through 7.3. Filled by slice 1.2; dispatch-time
//! envelope enforcement lands with the tool authority layers in slice 1.8.
