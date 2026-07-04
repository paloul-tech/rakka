//! Future A2A/Rakka codec boundary.
//!
//! Phase 0 does not route public A2A requests to sharded entities, so no
//! example-local inter-node payload codec is registered yet. Phase 2 should add
//! codecs for the remote-safe A2A run request/response messages.

use rakka::remote::SerializationRegistry;

/// Builds the serialization registry used by the Phase 0 node runtime.
#[must_use]
pub fn serialization_registry() -> SerializationRegistry {
    SerializationRegistry::new()
}
