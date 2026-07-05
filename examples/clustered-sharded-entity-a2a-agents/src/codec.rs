//! Future A2A/Rakka codec boundary.
//!
//! Phase 2 accepts public A2A requests locally, so no example-local inter-node
//! payload codec is registered yet. The clustered routing phase should add
//! codecs for the remote-safe A2A run request/response messages.

use rakka::remote::SerializationRegistry;

/// Builds the serialization registry used by the Phase 2 node runtime.
#[must_use]
pub fn serialization_registry() -> SerializationRegistry {
    SerializationRegistry::new()
}
