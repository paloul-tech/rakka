//! The provider-neutral model adapter trait.
//!
//! Owns the Rakka model contract (working name `AgentModelAdapter`): it turns
//! an immutable context snapshot and a settings revision into a bounded model
//! request, and turns the provider response into a bounded Rakka result or
//! artifact. The durable loop, the effect model, and the testkit depend on this
//! trait and on nothing else — no provider client, stream, open request, or
//! credential value is ever durable state.
//!
//! Model calls are effects with an explicit retry policy, so a provider that
//! stalls or fails is handled by the effect machine rather than by the adapter.
//!
//! Specification: sections 10.1 and 10.3. Filled by slice 1.6. The Rig-backed
//! implementation lives behind the `rig` feature in [`crate::rig`]; the
//! deterministic implementation lives in [`crate::testkit`].
