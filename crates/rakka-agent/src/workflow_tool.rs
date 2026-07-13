//! Workflows as tools.
//!
//! Owns `WorkflowToolDescriptor` and the invocation path that creates or adopts
//! an independently durable child workflow run keyed by a stable identity,
//! while the parent waits durably. A replayed invocation adopts the one child
//! run rather than starting a second.
//!
//! The child's internal effects keep their own durable boundaries. A workflow
//! is never collapsed into a single opaque retryable effect, because retrying
//! it would replay every external call it already made.
//!
//! Specification: sections 8.6 and 11.7. Filled by slice 4.5.
