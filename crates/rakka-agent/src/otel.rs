//! OpenTelemetry GenAI convention mapping (`otel` feature).
//!
//! Owns the mapping from the stable Rakka telemetry domain of
//! [`crate::observability`] to a pinned, reviewed OpenTelemetry GenAI
//! semantic-convention revision, composed additively over the existing
//! `rakka-agent-workflow` OTLP bridge. It does not own application exporter
//! credentials and does not install a global SDK into the core runtime.
//!
//! A convention upgrade requires an adapter compatibility review, but must not
//! by itself require a durable agent-state migration.
//!
//! Specification: sections 17.17 and 19. Filled by slice 1.13.
