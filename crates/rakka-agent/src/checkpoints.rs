//! Durable checkpoints and human-in-the-loop gates.
//!
//! Owns the three checkpoint kinds — `Approval`, `SecurityAuthorization`, and
//! `IndeterminateEffectReconciliation` — and the checkpoint record itself. A
//! grant binds to an exact effect intent and argument digest; if the binding
//! changes, the grant is invalid, and every grant is revalidated before
//! dispatch. The reconciliation decision set has no generic `Retry`: a
//! `ConfirmedNotExecuted` decision creates a new effect generation instead of
//! replaying the old one.
//!
//! Waits are fully passivated and driven by durable timers for SLA and
//! escalation. A timeout never auto-approves sensitive or non-idempotent work.
//!
//! Specification: section 12. Filled by slice 1.10.
