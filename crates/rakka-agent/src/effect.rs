//! The effect model: intents, safety classes, and the dispatch state machine.
//!
//! Owns the `EffectIntent` record, the safety classes that decide what recovery
//! is permitted, and the generation-carrying effect status machine. Dispatch is
//! durable first: `Started` is persisted with a lease and fence before the
//! external invocation, credentials are resolved only at dispatch and never
//! persisted, and a result from a stale generation is rejected.
//!
//! Crash and timeout handling follows the safety class. An effect whose outcome
//! cannot be established becomes `Indeterminate` and moves to
//! `WaitingForReconciliation` rather than being retried; there is no generic
//! retry for an ambiguous non-idempotent effect. Cancellation fences new
//! dispatch immediately, but an ambiguous effect stays in reconciliation until
//! its outcome is resolved, so cancellation is never terminal before then.
//!
//! Specification: sections 11.1 through 11.6, with the cancellation clauses of
//! 8.7. Filled by slice 1.7 over the `rakka-agent-workflow` dispatcher and
//! effect bridge.
