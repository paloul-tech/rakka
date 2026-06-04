#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! External child-process actor foundation.

use std::ffi::OsStr;

use rakka_core::Subsystem;

/// Crate name used in diagnostics.
pub const CRATE_NAME: &str = "rakka-process";

/// Subsystem associated with child-process actors.
#[must_use]
pub const fn subsystem() -> Subsystem {
    Subsystem::Process
}

/// Creates a Tokio process command for future process actor ownership.
#[must_use]
pub fn command(program: impl AsRef<OsStr>) -> tokio::process::Command {
    tokio::process::Command::new(program)
}
