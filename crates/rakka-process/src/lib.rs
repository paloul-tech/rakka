#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! External child-process actor foundation.

use std::ffi::OsStr;

use rakka_core::Subsystem;

mod actor;
mod error;
mod managed;
mod spec;
mod stdio;

pub use actor::{
    spawn_process_actor, ProcessActor, ProcessActorCommand, ProcessActorConfig,
    ProcessActorStartMode, ProcessActorState, ProcessActorStatus, ProcessCheck, ProcessHealth,
    ProcessRestartJitter, ProcessRestartPolicy, ProcessSupervisionEvent,
};
pub use error::{ProcessError, ProcessResult};
pub use managed::{
    ManagedProcess, ProcessExit, ProcessShutdown, ProcessShutdownOutcome, ProcessStart,
};
pub use spec::{ExecutableAllowlist, GracefulShutdown, ProcessSpec, ProcessStdio, ResourceHints};
pub use stdio::{
    spawn_stdio_actor, LineJsonCodec, LineJsonFrame, RawLineStdioCodec, StdioActor, StdioCodec,
    StdioCommand, StdioProtocolConfig, StdioReply, StdioStatus,
};

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
