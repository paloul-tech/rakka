//! The tool registry and the tool authority layers.
//!
//! Owns the tool registry — kinds, descriptor schema and version, safety class,
//! capabilities, and credential class — and the four authority layers that
//! stand between a model suggestion and an external call: `ToolDescriptor`,
//! `ToolBinding`, `EffectIntent`, and `DispatchGrant`. Each layer may only
//! narrow the one above it, grants bind to an exact intent, and every grant is
//! revalidated before the attempt. Model output can request a call; it can
//! never widen authority, target, capability, or credential class.
//!
//! Also owns the `ExecutionPolicyRef` persistence and the trust-class routing
//! hooks that let an application place a tool executor in the isolation it
//! requires, and the `AgentEnvironmentRef` contract and concurrency rules for
//! tool adapters sharing an environment.
//!
//! Specification: sections 11.7 and 11.8, with the shared environment of 8.5.
//! Filled by slice 1.8; the shared-environment rules by slice 4.6.
