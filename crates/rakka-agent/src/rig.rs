//! The Rig-backed model adapter (`rig` feature).
//!
//! Owns the Rig implementation of [`crate::model`]'s adapter trait and the
//! pinned Rig version together with its upgrade review. Rig types never escape
//! this module: they do not appear in the crate's non-`rig` public API, in
//! persisted state, or in A2A metadata, and a raw Rig run is never the durable
//! compatibility format — Rakka persists its own versioned loop representation.
//!
//! Rig memory policies may window, compact, or summarize history, but only
//! behind the Rakka-owned write path, so scoped memory stores and their stable
//! operation identifiers stay authoritative.
//!
//! Specification: sections 10.1 through 10.3. Filled by slice 1.6, which brings
//! the pinned Rig dependency with it.
