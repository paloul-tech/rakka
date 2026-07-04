//! Future A2A/Rakka codec boundary.
//!
//! Phase 1 validates public A2A requests into local command drafts only, so no
//! example-local inter-node payload codec is registered yet. Phase 2 should add
//! codecs for the remote-safe A2A run request/response messages.

use rakka::remote::SerializationRegistry;

/// Builds the serialization registry used by the Phase 1 node runtime.
#[must_use]
pub fn serialization_registry() -> SerializationRegistry {
    SerializationRegistry::new()
}
