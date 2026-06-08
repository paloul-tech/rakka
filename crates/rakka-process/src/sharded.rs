//! Process-backed sharded entity foundation.

use std::future::Future;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use rakka_cluster::NodeId;
use rakka_core::{
    actor_future, Actor, ActorAction, ActorContext, ActorFuture, ActorSystem, Message, RakkaError,
    Subsystem,
};
use rakka_sharding::{LocalEntityContext, LocalEntityRoute};

use crate::{
    ExecutableAllowlist, ManagedProcess, ProcessError, ProcessExit, ProcessResult, ProcessSpec,
};

/// Boxed future returned by process-backed entity behavior hooks.
pub type ProcessBackedEntityFuture<'a, T> =
    Pin<Box<dyn Future<Output = ProcessResult<T>> + Send + 'a>>;

/// Process specification plus allowlist used for one process-backed entity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessBackedEntityProcess {
    spec: ProcessSpec,
    allowlist: ExecutableAllowlist,
}

impl ProcessBackedEntityProcess {
    /// Creates process configuration for one process-backed entity.
    #[must_use]
    pub const fn new(spec: ProcessSpec, allowlist: ExecutableAllowlist) -> Self {
        Self { spec, allowlist }
    }

    /// Process specification.
    #[must_use]
    pub const fn spec(&self) -> &ProcessSpec {
        &self.spec
    }

    /// Executable allowlist.
    #[must_use]
    pub const fn allowlist(&self) -> &ExecutableAllowlist {
        &self.allowlist
    }

    fn into_parts(self) -> (ProcessSpec, ExecutableAllowlist) {
        (self.spec, self.allowlist)
    }
}

/// Local sharding identity and derived labels for a process-backed entity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessBackedEntityContext {
    local: LocalEntityContext,
    log_label: String,
    fencing_key: String,
}

impl ProcessBackedEntityContext {
    /// Creates process-backed entity context from local sharding context.
    #[must_use]
    pub fn new(local: LocalEntityContext) -> Self {
        let log_label = format!(
            "{}:{} shard={} node={}",
            local.entity_type(),
            local.entity_id(),
            local.shard_id(),
            local.local_node_id()
        );
        let fencing_key = format!("{}:{}", local.entity_type(), local.entity_id());
        Self {
            local,
            log_label,
            fencing_key,
        }
    }

    /// Underlying local sharded entity context.
    #[must_use]
    pub const fn local(&self) -> &LocalEntityContext {
        &self.local
    }

    /// Local cluster node id.
    #[must_use]
    pub fn local_node_id(&self) -> &NodeId {
        self.local.local_node_id()
    }

    /// Entity type name.
    #[must_use]
    pub fn entity_type(&self) -> &rakka_sharding::EntityType {
        self.local.entity_type()
    }

    /// Entity id.
    #[must_use]
    pub fn entity_id(&self) -> &rakka_sharding::EntityId {
        self.local.entity_id()
    }

    /// Shard id.
    #[must_use]
    pub const fn shard_id(&self) -> rakka_sharding::ShardId {
        self.local.shard_id()
    }

    /// Stable actor name used by the local route.
    #[must_use]
    pub fn actor_name(&self) -> &str {
        self.local.actor_name()
    }

    /// Human-readable log label for process telemetry.
    #[must_use]
    pub fn log_label(&self) -> &str {
        &self.log_label
    }

    /// Stable logical key applications should use for durable fencing.
    ///
    /// The key intentionally excludes the owning node incarnation. During shard
    /// handoff, new owners should acquire or compare-and-set durable state for
    /// this key before starting an external child process. That keeps two
    /// nodes from intentionally owning the same logical service identity.
    #[must_use]
    pub fn fencing_key(&self) -> &str {
        &self.fencing_key
    }

    /// Recommended sandbox directory for this entity under a caller-owned root.
    #[must_use]
    pub fn working_dir_under(&self, root: impl AsRef<Path>) -> PathBuf {
        root.as_ref()
            .join(sanitize_path_segment(self.entity_type().as_str()))
            .join(format!("shard-{}", self.shard_id().as_u32()))
            .join(sanitize_path_segment(self.entity_id().as_str()))
    }
}

/// Local process-backed entity runtime state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessBackedEntityState {
    /// Entity actor has not started its child process.
    Idle,
    /// Entity actor is running its recovery hook before process startup.
    Recovering,
    /// Entity actor is spawning its child process.
    Starting,
    /// Child process is running.
    Running,
    /// Entity actor has stopped.
    Stopped,
    /// Entity actor observed a terminal process-backed failure.
    Failed,
}

/// Snapshot of process-backed entity runtime status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessBackedEntityStatus {
    state: ProcessBackedEntityState,
    recovered: bool,
    pid: Option<u32>,
    last_exit: Option<ProcessExit>,
    last_error: Option<ProcessError>,
}

impl ProcessBackedEntityStatus {
    /// Creates process-backed entity status.
    #[must_use]
    pub const fn new(
        state: ProcessBackedEntityState,
        recovered: bool,
        pid: Option<u32>,
        last_exit: Option<ProcessExit>,
        last_error: Option<ProcessError>,
    ) -> Self {
        Self {
            state,
            recovered,
            pid,
            last_exit,
            last_error,
        }
    }

    /// Current runtime state.
    #[must_use]
    pub const fn state(&self) -> ProcessBackedEntityState {
        self.state
    }

    /// Returns true after the recovery hook has completed for this actor instance.
    #[must_use]
    pub const fn recovered(&self) -> bool {
        self.recovered
    }

    /// Child process id, when running.
    #[must_use]
    pub const fn pid(&self) -> Option<u32> {
        self.pid
    }

    /// Last child process exit observed by this entity.
    #[must_use]
    pub const fn last_exit(&self) -> Option<&ProcessExit> {
        self.last_exit.as_ref()
    }

    /// Last process-backed entity error.
    #[must_use]
    pub const fn last_error(&self) -> Option<&ProcessError> {
        self.last_error.as_ref()
    }
}

/// Action returned by a process-backed entity message handler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessBackedEntityAction {
    /// Continue processing messages.
    Continue,
    /// Stop the entity after the current message and shut down its child process.
    Passivate,
}

/// Behavior supplied by applications for a process-backed sharded entity.
pub trait ProcessBackedEntityBehavior<M>: Send + 'static
where
    M: Message,
{
    /// Builds the process specification for this entity instance.
    fn process(&self, context: &ProcessBackedEntityContext) -> ProcessBackedEntityProcess;

    /// Recovers durable application state before the child process starts.
    ///
    /// Implementations should acquire any durable fencing token or revision for
    /// `context.fencing_key()` here, before process startup.
    fn recover<'a>(
        &'a mut self,
        _context: &'a ProcessBackedEntityContext,
    ) -> ProcessBackedEntityFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    /// Handles one message with access to the running child process.
    fn handle<'a>(
        &'a mut self,
        context: &'a ProcessBackedEntityContext,
        process: &'a mut ManagedProcess,
        message: M,
    ) -> ProcessBackedEntityFuture<'a, ProcessBackedEntityAction>;

    /// Called after the entity actor is stopped and its child has been shut down.
    fn stopped<'a>(
        &'a mut self,
        _context: &'a ProcessBackedEntityContext,
    ) -> ProcessBackedEntityFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

/// Sharded entity actor that owns one child process.
pub struct ProcessBackedEntity<M, B>
where
    M: Message,
    B: ProcessBackedEntityBehavior<M>,
{
    context: ProcessBackedEntityContext,
    behavior: B,
    process: Option<ManagedProcess>,
    recovered: bool,
    state: ProcessBackedEntityState,
    last_exit: Option<ProcessExit>,
    last_error: Option<ProcessError>,
    _message: PhantomData<fn(M)>,
}

impl<M, B> ProcessBackedEntity<M, B>
where
    M: Message,
    B: ProcessBackedEntityBehavior<M>,
{
    /// Creates a process-backed entity actor.
    #[must_use]
    pub const fn new(context: ProcessBackedEntityContext, behavior: B) -> Self {
        Self {
            context,
            behavior,
            process: None,
            recovered: false,
            state: ProcessBackedEntityState::Idle,
            last_exit: None,
            last_error: None,
            _message: PhantomData,
        }
    }

    /// Process-backed entity context.
    #[must_use]
    pub const fn context(&self) -> &ProcessBackedEntityContext {
        &self.context
    }

    /// Current status snapshot.
    #[must_use]
    pub fn status(&self) -> ProcessBackedEntityStatus {
        ProcessBackedEntityStatus::new(
            self.state,
            self.recovered,
            self.process.as_ref().and_then(ManagedProcess::pid),
            self.last_exit.clone(),
            self.last_error.clone(),
        )
    }

    async fn ensure_process_started(&mut self) -> ProcessResult<()> {
        if let Some(process) = &mut self.process {
            match process.try_wait()? {
                Some(exit) => {
                    self.last_exit = Some(exit);
                    self.process = None;
                    self.state = ProcessBackedEntityState::Idle;
                }
                None => return Ok(()),
            }
        }

        if !self.recovered {
            self.state = ProcessBackedEntityState::Recovering;
            if let Err(error) = self.behavior.recover(&self.context).await {
                self.state = ProcessBackedEntityState::Failed;
                self.last_error = Some(error.clone());
                return Err(error);
            }
            self.recovered = true;
        }

        self.state = ProcessBackedEntityState::Starting;
        let process = self.behavior.process(&self.context);
        let (spec, allowlist) = process.into_parts();
        match ManagedProcess::spawn(spec, &allowlist) {
            Ok(process) => {
                self.state = ProcessBackedEntityState::Running;
                self.process = Some(process);
                Ok(())
            }
            Err(error) => {
                self.state = ProcessBackedEntityState::Failed;
                self.last_error = Some(error.clone());
                Err(error)
            }
        }
    }

    async fn shutdown_process(&mut self) {
        self.state = ProcessBackedEntityState::Stopped;
        if let Some(mut process) = self.process.take() {
            match process.shutdown().await {
                Ok(shutdown) => self.last_exit = Some(shutdown.exit().clone()),
                Err(error) => self.last_error = Some(error),
            }
        }
    }
}

impl<M, B> Actor for ProcessBackedEntity<M, B>
where
    M: Message,
    B: ProcessBackedEntityBehavior<M>,
{
    type Msg = M;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        actor_future(async move {
            self.ensure_process_started()
                .await
                .map_err(process_backed_rakka_error)?;
            let process = self
                .process
                .as_mut()
                .expect("process should be running after successful startup");
            let action = self
                .behavior
                .handle(&self.context, process, msg)
                .await
                .map_err(process_backed_rakka_error)?;

            match action {
                ProcessBackedEntityAction::Continue => Ok(ActorAction::Continue),
                ProcessBackedEntityAction::Passivate => Ok(ActorAction::Stop),
            }
        })
    }

    fn stopped<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        _reason: &'a rakka_core::TerminationReason,
    ) -> ActorFuture<'a> {
        actor_future(async move {
            self.shutdown_process().await;
            if let Err(error) = self.behavior.stopped(&self.context).await {
                self.last_error = Some(error);
            }
            Ok(ActorAction::Continue)
        })
    }
}

/// Creates a local sharding route for process-backed entity actors.
pub fn process_backed_entity_route<M, B, F>(
    local_node_id: NodeId,
    system: ActorSystem,
    factory: F,
) -> LocalEntityRoute<
    M,
    ProcessBackedEntity<M, B>,
    impl Fn(LocalEntityContext) -> ProcessBackedEntity<M, B> + Send + Sync + 'static,
>
where
    M: Message,
    B: ProcessBackedEntityBehavior<M>,
    F: Fn(ProcessBackedEntityContext) -> B + Send + Sync + 'static,
{
    let factory = Arc::new(factory);
    LocalEntityRoute::new(local_node_id, system, move |local_context| {
        let context = ProcessBackedEntityContext::new(local_context);
        let behavior = factory(context.clone());
        ProcessBackedEntity::new(context, behavior)
    })
}

fn process_backed_rakka_error(error: ProcessError) -> RakkaError {
    RakkaError::new(
        Subsystem::Process,
        "process-backed-entity",
        error.to_string(),
    )
}

fn sanitize_path_segment(value: &str) -> String {
    let sanitized = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.') {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();

    if sanitized.is_empty() {
        "_".to_string()
    } else {
        sanitized
    }
}
