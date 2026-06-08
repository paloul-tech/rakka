#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! External child-process actor foundation.

use std::ffi::OsStr;

use rakka_core::Subsystem;

mod actor;
mod capture;
mod endpoint;
mod error;
mod file_watch;
mod managed;
mod oneshot;
mod sharded;
mod spec;
mod stdio;

pub use actor::{
    spawn_process_actor, ProcessActor, ProcessActorCommand, ProcessActorConfig,
    ProcessActorStartMode, ProcessActorState, ProcessActorStatus, ProcessCheck, ProcessHealth,
    ProcessRestartJitter, ProcessRestartPolicy, ProcessSupervisionEvent,
};
pub use endpoint::{
    start_local_grpc_process, start_socket_process, wait_for_local_endpoint,
    EndpointReadinessConfig, EndpointReady, LocalEndpoint, LocalGrpcEndpoint, LocalGrpcProcess,
    LocalGrpcProcessConfig, SocketProcess, SocketProcessConfig,
};
pub use error::{ProcessError, ProcessResult};
pub use file_watch::{
    run_file_watch, FileWatchCleanup, FileWatchCompleted, FileWatchCompletion, FileWatchConfig,
    FileWatchInput, FileWatchOutcome, FileWatchOutput, FileWatchOutputPolicy,
    FileWatchProcessExited, FileWatchTimedOut,
};
pub use managed::{
    ManagedProcess, ProcessExit, ProcessShutdown, ProcessShutdownOutcome, ProcessStart,
};
pub use oneshot::{run_one_shot, OneShotConfig, OneShotOutcome, OneShotOutput};
pub use sharded::{
    process_backed_entity_route, ProcessBackedEntity, ProcessBackedEntityAction,
    ProcessBackedEntityBehavior, ProcessBackedEntityContext, ProcessBackedEntityFuture,
    ProcessBackedEntityProcess, ProcessBackedEntityState, ProcessBackedEntityStatus,
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
