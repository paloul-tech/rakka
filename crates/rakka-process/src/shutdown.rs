//! Coordinated shutdown helpers for child processes.

use std::sync::Arc;
use std::time::Duration;

use rakka_core::{
    ActorRef, AskError, CoordinatedShutdown, RakkaError, RakkaResult, ShutdownPhase, ShutdownTask,
    ShutdownTaskOptions, Subsystem,
};
use tokio::sync::Mutex;

use crate::{
    ManagedProcess, ProcessActorCommand, ProcessActorConfig, ProcessActorState, ProcessError,
    ProcessShutdown,
};

/// Extra actor ask allowance added to a configured child-process shutdown timeout.
pub const PROCESS_ACTOR_STOP_TASK_GRACE: Duration = Duration::from_secs(1);

/// Registers a task that sends [`ProcessActorCommand::Stop`] to a process actor.
///
/// The provided timeout is used for both the actor ask and the coordinated
/// shutdown task. A process actor that has already stopped, or whose child is
/// already absent, is treated as a successful idempotent shutdown.
pub fn register_process_actor_stop_task(
    shutdown: &CoordinatedShutdown,
    task_name: impl Into<String>,
    actor: ActorRef<ProcessActorCommand>,
    timeout: Duration,
) -> RakkaResult<ShutdownTask> {
    shutdown.add_task_with_options(
        ShutdownPhase::stop_process_actors(),
        task_name,
        process_shutdown_options("process-actor-stop", timeout)?,
        move |_context| {
            let actor = actor.clone();
            async move { stop_process_actor(actor, timeout).await }
        },
    )
}

/// Registers a process actor stop task using the actor's configured process timeout.
///
/// The task timeout is the child-process graceful shutdown timeout plus a small
/// ask round-trip allowance so the actor can report the result after its owned
/// [`ManagedProcess`] has completed shutdown.
pub fn register_configured_process_actor_stop_task(
    shutdown: &CoordinatedShutdown,
    task_name: impl Into<String>,
    actor: ActorRef<ProcessActorCommand>,
    config: &ProcessActorConfig,
) -> RakkaResult<ShutdownTask> {
    register_process_actor_stop_task(
        shutdown,
        task_name,
        actor,
        configured_process_actor_stop_timeout(config),
    )
}

/// Registers a task that shuts down a directly owned [`ManagedProcess`].
///
/// This is for applications that own a process handle outside a process actor.
/// The process's configured graceful shutdown timeout controls when
/// [`ManagedProcess::shutdown`] escalates to killing the child.
pub fn register_managed_process_shutdown_task(
    shutdown: &CoordinatedShutdown,
    task_name: impl Into<String>,
    process: ManagedProcess,
) -> RakkaResult<ShutdownTask> {
    let timeout = process
        .spec()
        .shutdown_timeout_duration()
        .saturating_add(PROCESS_ACTOR_STOP_TASK_GRACE);
    let process = Arc::new(Mutex::new(Some(process)));
    shutdown.add_task_with_options(
        ShutdownPhase::stop_process_actors(),
        task_name,
        process_shutdown_options("managed-process-shutdown", timeout)?,
        move |_context| {
            let process = process.clone();
            async move { shutdown_managed_process(process).await.map(|_shutdown| ()) }
        },
    )
}

/// Returns the configured process actor task timeout for a config.
#[must_use]
pub fn configured_process_actor_stop_timeout(config: &ProcessActorConfig) -> Duration {
    config
        .spec()
        .shutdown_timeout_duration()
        .saturating_add(PROCESS_ACTOR_STOP_TASK_GRACE)
}

async fn stop_process_actor(
    actor: ActorRef<ProcessActorCommand>,
    timeout: Duration,
) -> RakkaResult<()> {
    match actor
        .ask(|reply_to| ProcessActorCommand::Stop { reply_to }, timeout)
        .await
    {
        Ok(Ok(status)) if status.state() == ProcessActorState::Stopped => Ok(()),
        Ok(Ok(_status)) => Ok(()),
        Ok(Err(ProcessError::NotRunning)) => Ok(()),
        Ok(Err(error)) => Err(error.into_rakka_error()),
        Err(AskError::MailboxClosed) if actor.is_terminated() => Ok(()),
        Err(error) => Err(RakkaError::new(
            Subsystem::Process,
            "process-actor-stop-ask",
            error.to_string(),
        )),
    }
}

async fn shutdown_managed_process(
    process: Arc<Mutex<Option<ManagedProcess>>>,
) -> RakkaResult<Option<ProcessShutdown>> {
    let mut process = process.lock().await;
    let Some(mut owned) = process.take() else {
        return Ok(None);
    };

    match owned.shutdown().await {
        Ok(shutdown) => Ok(Some(shutdown)),
        Err(ProcessError::AlreadyReaped { .. }) => Ok(None),
        Err(error) => Err(error),
    }
    .map_err(ProcessError::into_rakka_error)
}

fn process_shutdown_options(
    operation: &'static str,
    timeout: Duration,
) -> RakkaResult<ShutdownTaskOptions> {
    ShutdownTaskOptions::default()
        .with_timeout(timeout)
        .with_attribute("operation", operation)?
        .with_attribute("timeout-ms", timeout.as_millis().to_string())
}
