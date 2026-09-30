//! Agent telemetry export acceptance: a real OpenTelemetry SDK at a real
//! application binary.
//!
//! Slice 6.3b of `docs/plans/rakka-agent/implementation-plan.md`, and the
//! deployment half of [specification
//! 17.17](../../../docs/plans/rakka-agent/spec.md): the application binary
//! owns the OpenTelemetry SDK, the `tracing` subscriber and layer, the OTLP
//! exporter, exporter credentials, and shutdown/flush, while the Rakka crates
//! stay SDK- and version-neutral.
//!
//! Slice 6.3a made the agent domain *emit* — a segment vocabulary closed on
//! the path a run takes, a GenAI convention mapping behind the `otel` feature,
//! a documented metric catalogue — and stopped at a serializable bridge record
//! nothing in the workspace ever sent. This example is what sends it, and what
//! proves the claim end to end: a real sharded agent run, mapped through
//! `AgentOtlpBridgeExport`, exported over OTLP to a Collector, with the
//! catalogue's units and bucket boundaries and an exemplar intact.

#![forbid(unsafe_code)]

pub mod collector;
pub mod flow;
pub mod report;
pub mod sdk;
pub mod wiring;

pub use flow::{run_acceptance, CONTENT_SENTINELS};
pub use report::{AcceptanceReport, EXPECTED_TRANSCRIPT};
