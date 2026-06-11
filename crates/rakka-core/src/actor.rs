//! Local typed actor runtime.

use std::any::{type_name, Any};
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::FutureExt;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::dead_letter::{DeadLetter, DeadLetterReason};
use crate::path::{ActorPath, ActorUid};
use crate::supervision::{ActorOptions, SupervisionStrategy};
use crate::system::ActorSystem;
use crate::{RakkaError, RakkaResult};

/// Default local actor mailbox capacity.
pub const DEFAULT_MAILBOX_CAPACITY: usize = 1024;

/// Marker trait for values that can be sent to local actors.
pub trait Message: Send + 'static {}

impl<T> Message for T where T: Send + 'static {}

/// Boxed future returned by actor lifecycle and message handlers.
pub type ActorFuture<'a> = Pin<Box<dyn Future<Output = ActorResult> + Send + 'a>>;

/// Result returned by actor lifecycle and message handlers.
pub type ActorResult = RakkaResult<ActorAction>;

/// Wraps an async block as an actor future.
pub fn actor_future<'a>(future: impl Future<Output = ActorResult> + Send + 'a) -> ActorFuture<'a> {
    Box::pin(future)
}

/// Action requested by an actor handler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActorAction {
    /// Continue processing the next mailbox message.
    Continue,
    /// Stop the actor after the current handler completes.
    Stop,
}

/// Local actor behavior.
pub trait Actor: Send + 'static {
    /// Typed message protocol accepted by this actor.
    type Msg: Message;

    /// Called after the actor task starts.
    fn started<'a>(&'a mut self, _ctx: &'a mut ActorContext<Self::Msg>) -> ActorFuture<'a> {
        actor_future(async { Ok(ActorAction::Continue) })
    }

    /// Handles one message from the actor mailbox.
    fn handle<'a>(
        &'a mut self,
        ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a>;

    /// Called after a restart creates a fresh actor instance.
    fn restarted<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        _failure: &'a ActorFailure,
    ) -> ActorFuture<'a> {
        actor_future(async { Ok(ActorAction::Continue) })
    }

    /// Called immediately before the actor terminates.
    fn stopped<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        _reason: &'a TerminationReason,
    ) -> ActorFuture<'a> {
        actor_future(async { Ok(ActorAction::Continue) })
    }
}

/// Failure observed while executing an actor handler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActorFailure {
    /// Actor handler returned an error.
    Error(RakkaError),
    /// Actor handler panicked.
    Panic,
}

impl Display for ActorFailure {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Error(error) => Display::fmt(error, f),
            Self::Panic => f.write_str("actor panicked"),
        }
    }
}

impl Error for ActorFailure {}

/// Reason an actor terminated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TerminationReason {
    /// Actor stopped normally.
    Normal,
    /// Actor received an explicit stop signal.
    Stopped,
    /// Actor stopped because supervision escalated a failure.
    Escalated(ActorFailure),
    /// Actor stopped because supervision chose stop.
    Failed(ActorFailure),
}

/// DeathWatch notification sent when a watched actor terminates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActorTerminated {
    /// Path of the actor that terminated.
    pub path: ActorPath,
    /// Incarnation uid of the actor that terminated.
    pub uid: ActorUid,
    /// Termination reason.
    pub reason: TerminationReason,
}

/// Serializable descriptor for an actor reference.
///
/// The descriptor captures both the logical path and incarnation uid. Resolving
/// it only succeeds while the same live actor cell is still registered in the
/// target actor system.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SerializedActorRef {
    system_name: String,
    path: ActorPath,
    uid: ActorUid,
    message_type: String,
}

impl SerializedActorRef {
    /// Creates a serialized actor reference descriptor.
    #[must_use]
    pub fn new(
        system_name: impl Into<String>,
        path: ActorPath,
        uid: ActorUid,
        message_type: impl Into<String>,
    ) -> Self {
        Self {
            system_name: system_name.into(),
            path,
            uid,
            message_type: message_type.into(),
        }
    }

    /// Actor system name that produced the descriptor.
    #[must_use]
    pub fn system_name(&self) -> &str {
        &self.system_name
    }

    /// Logical actor path.
    #[must_use]
    pub const fn path(&self) -> &ActorPath {
        &self.path
    }

    /// Incarnation uid.
    #[must_use]
    pub const fn uid(&self) -> ActorUid {
        self.uid
    }

    /// Rust message type name recorded for typed local resolution.
    #[must_use]
    pub fn message_type(&self) -> &str {
        &self.message_type
    }
}

/// Serializable actor runtime snapshot used by operational diagnostics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorRuntimeSnapshot {
    path: ActorPath,
    uid: ActorUid,
    mailbox_capacity: usize,
    mailbox_depth: usize,
    terminated: bool,
    termination_reason: Option<String>,
}

impl ActorRuntimeSnapshot {
    /// Creates an actor runtime snapshot.
    #[must_use]
    pub fn new(
        path: ActorPath,
        uid: ActorUid,
        mailbox_capacity: usize,
        mailbox_depth: usize,
        terminated: bool,
        termination_reason: Option<String>,
    ) -> Self {
        Self {
            path,
            uid,
            mailbox_capacity,
            mailbox_depth,
            terminated,
            termination_reason,
        }
    }

    /// Actor path.
    #[must_use]
    pub fn path(&self) -> &ActorPath {
        &self.path
    }

    /// Incarnation uid.
    #[must_use]
    pub const fn uid(&self) -> ActorUid {
        self.uid
    }

    /// Configured mailbox capacity.
    #[must_use]
    pub const fn mailbox_capacity(&self) -> usize {
        self.mailbox_capacity
    }

    /// Current queued mailbox envelopes.
    #[must_use]
    pub const fn mailbox_depth(&self) -> usize {
        self.mailbox_depth
    }

    /// Returns true after the actor terminates.
    #[must_use]
    pub const fn terminated(&self) -> bool {
        self.terminated
    }

    /// Stable termination reason label, when terminated.
    #[must_use]
    pub fn termination_reason(&self) -> Option<&str> {
        self.termination_reason.as_deref()
    }
}

/// Typed reference to a local actor.
pub struct ActorRef<M>
where
    M: Message,
{
    sender: mpsc::Sender<Envelope<M>>,
    cell: Arc<ActorCell>,
}

impl<M> ActorRef<M>
where
    M: Message,
{
    /// Sends a message without waiting for a reply.
    pub fn tell(&self, msg: M) -> Result<(), TellError<M>> {
        if self.is_terminated() {
            self.publish_dead_letter::<M>(DeadLetterReason::MailboxClosed);
            return Err(TellError::Closed(msg));
        }

        self.cell.mark_enqueued();
        match self.sender.try_send(Envelope::User(msg)) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(Envelope::User(msg))) => {
                self.cell.mark_dequeued();
                self.publish_dead_letter::<M>(DeadLetterReason::MailboxFull);
                Err(TellError::Full(msg))
            }
            Err(TrySendError::Closed(Envelope::User(msg))) => {
                self.cell.mark_dequeued();
                self.publish_dead_letter::<M>(DeadLetterReason::MailboxClosed);
                Err(TellError::Closed(msg))
            }
            Err(TrySendError::Full(Envelope::Stop)) => {
                unreachable!("stop envelope not sent by tell")
            }
            Err(TrySendError::Closed(Envelope::Stop)) => {
                unreachable!("stop envelope not sent by tell")
            }
        }
    }

    /// Sends a request message and waits for its reply.
    pub async fn ask<R>(
        &self,
        build: impl FnOnce(ReplyTo<R>) -> M,
        timeout: Duration,
    ) -> Result<R, AskError>
    where
        R: Send + 'static,
    {
        let (sender, receiver) = oneshot::channel();
        let msg = build(ReplyTo::new(sender));

        self.tell(msg).map_err(|error| match error {
            TellError::Full(_) => AskError::MailboxFull,
            TellError::Closed(_) => AskError::MailboxClosed,
        })?;

        match tokio::time::timeout(timeout, receiver).await {
            Ok(Ok(reply)) => Ok(reply),
            Ok(Err(_closed)) => Err(AskError::ReplyDropped),
            Err(_elapsed) => Err(AskError::Timeout),
        }
    }

    /// Requests actor termination.
    pub fn stop(&self) -> Result<(), StopError> {
        if self.is_terminated() {
            return Ok(());
        }

        self.cell.mark_enqueued();
        match self.sender.try_send(Envelope::Stop) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => {
                self.cell.mark_dequeued();
                Err(StopError::MailboxFull)
            }
            Err(TrySendError::Closed(_)) => {
                self.cell.mark_dequeued();
                Err(StopError::MailboxClosed)
            }
        }
    }

    /// Returns this actor's logical path.
    #[must_use]
    pub fn path(&self) -> &ActorPath {
        &self.cell.path
    }

    /// Returns this actor's incarnation uid.
    #[must_use]
    pub fn uid(&self) -> ActorUid {
        self.cell.uid
    }

    /// Returns a serializable descriptor for this actor reference.
    #[must_use]
    pub fn to_serialized_ref(&self) -> SerializedActorRef {
        self.cell.to_serialized_ref()
    }

    /// Waits until this actor incarnation terminates.
    pub async fn when_terminated(&self) -> ActorTerminated {
        let (sender, receiver) = oneshot::channel();
        self.watch(DeathRecipient::new(move |terminated| {
            let _ = sender.send(terminated);
        }));
        receiver
            .await
            .expect("actor termination watcher should always send")
    }

    /// Returns true after the actor has terminated.
    #[must_use]
    pub fn is_terminated(&self) -> bool {
        self.cell.terminated.load(Ordering::Acquire)
    }

    /// Configured mailbox capacity.
    #[must_use]
    pub fn mailbox_capacity(&self) -> usize {
        self.cell.mailbox_capacity()
    }

    /// Current queued mailbox envelopes.
    #[must_use]
    pub fn mailbox_depth(&self) -> usize {
        self.cell.mailbox_depth()
    }

    /// Returns a serializable runtime snapshot for this actor.
    #[must_use]
    pub fn snapshot(&self) -> ActorRuntimeSnapshot {
        self.cell.snapshot()
    }

    pub(crate) fn stop_handle(&self) -> ActorStopHandle {
        let cloned = self.clone();
        ActorStopHandle {
            cell: self.cell.clone(),
            stop: Arc::new(move || {
                let _ = cloned.stop();
            }),
        }
    }

    fn watch(&self, recipient: DeathRecipient) {
        self.cell.watch(recipient);
    }

    fn publish_dead_letter<T>(&self, reason: DeadLetterReason)
    where
        T: Message,
    {
        let _ = self.cell.dead_letters.send(DeadLetter {
            recipient: self.path().clone(),
            message_type: type_name::<T>().to_string(),
            reason,
        });
    }
}

impl<M> Clone for ActorRef<M>
where
    M: Message,
{
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            cell: self.cell.clone(),
        }
    }
}

impl<M> Debug for ActorRef<M>
where
    M: Message,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ActorRef")
            .field("path", self.path())
            .field("uid", &self.uid())
            .field("terminated", &self.is_terminated())
            .finish()
    }
}

/// Error returned by `ActorRef::tell`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TellError<M> {
    /// Destination mailbox was full.
    Full(M),
    /// Destination actor was stopped or mailbox was closed.
    Closed(M),
}

impl<M> TellError<M> {
    /// Returns the message that could not be delivered.
    #[must_use]
    pub fn into_message(self) -> M {
        match self {
            Self::Full(msg) | Self::Closed(msg) => msg,
        }
    }
}

/// Error returned by `ActorRef::ask`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AskError {
    /// Destination mailbox was full.
    MailboxFull,
    /// Destination actor was stopped or mailbox was closed.
    MailboxClosed,
    /// Timed out waiting for a reply.
    Timeout,
    /// Actor dropped the reply channel before sending.
    ReplyDropped,
}

impl Display for AskError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::MailboxFull => f.write_str("actor mailbox was full"),
            Self::MailboxClosed => f.write_str("actor mailbox was closed"),
            Self::Timeout => f.write_str("ask timed out"),
            Self::ReplyDropped => f.write_str("ask reply channel was dropped"),
        }
    }
}

impl Error for AskError {}

/// Error returned by `ActorRef::stop`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopError {
    /// Destination mailbox was full.
    MailboxFull,
    /// Destination actor was already closed.
    MailboxClosed,
}

impl Display for StopError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::MailboxFull => f.write_str("actor mailbox was full"),
            Self::MailboxClosed => f.write_str("actor mailbox was closed"),
        }
    }
}

impl Error for StopError {}

/// One-shot reply capability used by `ActorRef::ask`.
pub struct ReplyTo<R>
where
    R: Send + 'static,
{
    sender: oneshot::Sender<R>,
}

impl<R> ReplyTo<R>
where
    R: Send + 'static,
{
    /// Creates a reply capability from a Tokio one-shot sender.
    #[must_use]
    pub fn new(sender: oneshot::Sender<R>) -> Self {
        Self { sender }
    }

    /// Sends a reply to the waiting caller.
    pub fn reply(self, reply: R) -> Result<(), R> {
        self.sender.send(reply)
    }
}

impl<R> Debug for ReplyTo<R>
where
    R: Send + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplyTo").finish_non_exhaustive()
    }
}

/// Actor execution context passed to handlers.
pub struct ActorContext<M>
where
    M: Message,
{
    system: ActorSystem,
    myself: ActorRef<M>,
    children: Vec<ActorStopHandle>,
}

impl<M> ActorContext<M>
where
    M: Message,
{
    fn new(system: ActorSystem, myself: ActorRef<M>) -> Self {
        Self {
            system,
            myself,
            children: Vec::new(),
        }
    }

    /// Returns the owning actor system.
    #[must_use]
    pub fn system(&self) -> &ActorSystem {
        &self.system
    }

    /// Returns this actor's typed self reference.
    #[must_use]
    pub fn myself(&self) -> &ActorRef<M> {
        &self.myself
    }

    /// Returns this actor's logical path.
    #[must_use]
    pub fn path(&self) -> &ActorPath {
        self.myself.path()
    }

    /// Spawns a child actor with default options.
    pub fn spawn_child<A>(
        &mut self,
        name: impl AsRef<str>,
        actor: A,
    ) -> RakkaResult<ActorRef<A::Msg>>
    where
        A: Actor,
    {
        let actor = Mutex::new(Some(actor));
        self.spawn_child_with_options(
            name,
            move || {
                actor
                    .lock()
                    .expect("actor factory mutex poisoned")
                    .take()
                    .expect("single-use actor factory cannot restart")
            },
            ActorOptions::default(),
        )
    }

    /// Spawns a restartable child actor with default options.
    pub fn spawn_child_factory<A, F>(
        &mut self,
        name: impl AsRef<str>,
        factory: F,
    ) -> RakkaResult<ActorRef<A::Msg>>
    where
        A: Actor,
        F: Fn() -> A + Send + Sync + 'static,
    {
        self.spawn_child_with_options(name, factory, ActorOptions::default())
    }

    /// Spawns a restartable child actor with explicit options.
    pub fn spawn_child_with_options<A, F>(
        &mut self,
        name: impl AsRef<str>,
        factory: F,
        options: ActorOptions,
    ) -> RakkaResult<ActorRef<A::Msg>>
    where
        A: Actor,
        F: Fn() -> A + Send + Sync + 'static,
    {
        if options.mailbox_capacity == 0 {
            return Err(RakkaError::core(
                "invalid-mailbox-capacity",
                "actor mailbox capacity must be greater than zero",
            ));
        }

        let (path, uid) = self.system.child_identity(self.path(), name.as_ref())?;
        let actor_ref = spawn_actor_task(self.system.clone(), path, uid, factory, options)?;
        let stop_handle = actor_ref.stop_handle();
        self.children.push(stop_handle);
        Ok(actor_ref)
    }

    /// Watches a target actor and sends `msg` to this actor when it terminates.
    pub fn watch_with<T>(&self, target: &ActorRef<T>, msg: M)
    where
        T: Message,
    {
        let myself = self.myself.clone();
        target.watch(DeathRecipient::new(move |_terminated| {
            let _ = myself.tell(msg);
        }));
    }

    /// Schedules a message to be sent to this actor once after `delay`.
    pub fn schedule_once(&self, delay: Duration, msg: M) -> TimerHandle<M> {
        let myself = self.myself.clone();
        let handle = tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let _ = myself.tell(msg);
        });
        TimerHandle {
            handle,
            _message: std::marker::PhantomData,
        }
    }

    /// Requests a child actor to stop.
    pub fn stop_child<T>(&self, child: &ActorRef<T>) -> Result<(), StopError>
    where
        T: Message,
    {
        child.stop()
    }

    fn stop_children(&self) {
        for child in &self.children {
            child.stop();
        }
    }
}

/// Handle for a scheduled actor timer.
pub struct TimerHandle<M>
where
    M: Message,
{
    handle: JoinHandle<()>,
    _message: std::marker::PhantomData<M>,
}

impl<M> TimerHandle<M>
where
    M: Message,
{
    /// Cancels the timer if it has not fired yet.
    pub fn abort(&self) {
        self.handle.abort();
    }

    /// Returns true if the timer task has finished.
    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }
}

impl<M> Debug for TimerHandle<M>
where
    M: Message,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("TimerHandle")
            .field("finished", &self.is_finished())
            .finish()
    }
}

#[derive(Clone)]
pub(crate) struct ActorStopHandle {
    cell: Arc<ActorCell>,
    stop: Arc<dyn Fn() + Send + Sync>,
}

impl ActorStopHandle {
    pub(crate) fn stop(&self) {
        (self.stop)();
    }

    pub(crate) fn snapshot(&self) -> ActorRuntimeSnapshot {
        self.cell.snapshot()
    }
}

pub(crate) fn spawn_actor_task<A, F>(
    system: ActorSystem,
    path: ActorPath,
    uid: ActorUid,
    factory: F,
    options: ActorOptions,
) -> RakkaResult<ActorRef<A::Msg>>
where
    A: Actor,
    F: Fn() -> A + Send + Sync + 'static,
{
    let (sender, receiver) = mpsc::channel(options.mailbox_capacity);
    let cell = Arc::new(ActorCell::new(
        system.name().to_string(),
        path,
        uid,
        type_name::<A::Msg>(),
        Arc::new(sender.clone()),
        system.dead_letters(),
        options.mailbox_capacity,
    ));
    system.register_actor_cell(cell.clone())?;
    let actor_ref = ActorRef {
        sender,
        cell: cell.clone(),
    };
    system.register_actor(actor_ref.stop_handle());
    let factory = Arc::new(factory);
    let task_ref = actor_ref.clone();
    tokio::spawn(run_actor_task(
        system, task_ref, receiver, cell, factory, options,
    ));
    Ok(actor_ref)
}

async fn run_actor_task<A, F>(
    system: ActorSystem,
    actor_ref: ActorRef<A::Msg>,
    mut receiver: mpsc::Receiver<Envelope<A::Msg>>,
    cell: Arc<ActorCell>,
    factory: Arc<F>,
    options: ActorOptions,
) where
    A: Actor,
    F: Fn() -> A + Send + Sync + 'static,
{
    let mut actor = factory();
    let mut ctx = ActorContext::new(system.clone(), actor_ref);
    let mut restart_count = 0usize;
    let mut termination_reason = TerminationReason::Normal;

    if let Some(reason) = lifecycle_failure(catch_actor_result(actor.started(&mut ctx)).await) {
        termination_reason = reason;
    } else {
        while let Some(envelope) = receiver.recv().await {
            cell.mark_dequeued();
            match envelope {
                Envelope::Stop => {
                    termination_reason = TerminationReason::Stopped;
                    break;
                }
                Envelope::User(msg) => {
                    let outcome = catch_actor_result(actor.handle(&mut ctx, msg)).await;
                    match outcome {
                        Ok(Ok(ActorAction::Continue)) => {}
                        Ok(Ok(ActorAction::Stop)) => {
                            termination_reason = TerminationReason::Stopped;
                            break;
                        }
                        Ok(Err(error)) => {
                            let failure = ActorFailure::Error(error);
                            if let Some(reason) = supervise_failure(
                                &mut actor,
                                &mut ctx,
                                &factory,
                                &options.supervision,
                                &failure,
                                &mut restart_count,
                            )
                            .await
                            {
                                termination_reason = reason;
                                break;
                            }
                        }
                        Err(failure) => {
                            if let Some(reason) = supervise_failure(
                                &mut actor,
                                &mut ctx,
                                &factory,
                                &options.supervision,
                                &failure,
                                &mut restart_count,
                            )
                            .await
                            {
                                termination_reason = reason;
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    let _ = catch_actor_result(actor.stopped(&mut ctx, &termination_reason)).await;
    ctx.stop_children();
    cell.mark_terminated(termination_reason);
    system.unregister_actor_cell(&cell);
}

fn lifecycle_failure(outcome: Result<ActorResult, ActorFailure>) -> Option<TerminationReason> {
    match outcome {
        Ok(Ok(ActorAction::Continue)) => None,
        Ok(Ok(ActorAction::Stop)) => Some(TerminationReason::Stopped),
        Ok(Err(error)) => Some(TerminationReason::Failed(ActorFailure::Error(error))),
        Err(failure) => Some(TerminationReason::Failed(failure)),
    }
}

async fn supervise_failure<A, F>(
    actor: &mut A,
    ctx: &mut ActorContext<A::Msg>,
    factory: &Arc<F>,
    strategy: &SupervisionStrategy,
    failure: &ActorFailure,
    restart_count: &mut usize,
) -> Option<TerminationReason>
where
    A: Actor,
    F: Fn() -> A + Send + Sync + 'static,
{
    match strategy {
        SupervisionStrategy::Resume => None,
        SupervisionStrategy::Restart => {
            *actor = factory.as_ref()();
            lifecycle_failure(catch_actor_result(actor.restarted(ctx, failure)).await)
        }
        SupervisionStrategy::Stop => Some(TerminationReason::Failed(failure.clone())),
        SupervisionStrategy::Escalate => Some(TerminationReason::Escalated(failure.clone())),
        SupervisionStrategy::RestartWithBackoff {
            min_backoff,
            max_backoff,
            max_restarts,
        } => {
            if *restart_count >= *max_restarts {
                return Some(TerminationReason::Failed(failure.clone()));
            }

            let delay = backoff_delay(*min_backoff, *max_backoff, *restart_count);
            *restart_count += 1;
            tokio::time::sleep(delay).await;
            *actor = factory.as_ref()();
            lifecycle_failure(catch_actor_result(actor.restarted(ctx, failure)).await)
        }
    }
}

fn backoff_delay(min_backoff: Duration, max_backoff: Duration, restart_count: usize) -> Duration {
    let factor = 1u32.checked_shl(restart_count.min(16) as u32).unwrap_or(1);
    min_backoff.saturating_mul(factor).min(max_backoff)
}

async fn catch_actor_result(future: ActorFuture<'_>) -> Result<ActorResult, ActorFailure> {
    AssertUnwindSafe(future)
        .catch_unwind()
        .await
        .map_err(|_panic| ActorFailure::Panic)
}

enum Envelope<M>
where
    M: Message,
{
    User(M),
    Stop,
}

pub(crate) struct ActorCell {
    system_name: String,
    path: ActorPath,
    uid: ActorUid,
    message_type: &'static str,
    sender: Arc<dyn Any + Send + Sync>,
    dead_letters: tokio::sync::broadcast::Sender<DeadLetter>,
    mailbox_capacity: usize,
    mailbox_depth: AtomicUsize,
    terminated: AtomicBool,
    termination_reason: Mutex<Option<TerminationReason>>,
    watchers: Mutex<Vec<DeathRecipient>>,
}

impl ActorCell {
    fn new(
        system_name: String,
        path: ActorPath,
        uid: ActorUid,
        message_type: &'static str,
        sender: Arc<dyn Any + Send + Sync>,
        dead_letters: tokio::sync::broadcast::Sender<DeadLetter>,
        mailbox_capacity: usize,
    ) -> Self {
        Self {
            system_name,
            path,
            uid,
            message_type,
            sender,
            dead_letters,
            mailbox_capacity,
            mailbox_depth: AtomicUsize::new(0),
            terminated: AtomicBool::new(false),
            termination_reason: Mutex::new(None),
            watchers: Mutex::new(Vec::new()),
        }
    }

    fn mailbox_capacity(&self) -> usize {
        self.mailbox_capacity
    }

    fn mailbox_depth(&self) -> usize {
        self.mailbox_depth.load(Ordering::Acquire)
    }

    pub(crate) fn path(&self) -> &ActorPath {
        &self.path
    }

    pub(crate) const fn uid(&self) -> ActorUid {
        self.uid
    }

    pub(crate) fn message_type(&self) -> &'static str {
        self.message_type
    }

    pub(crate) fn is_terminated(&self) -> bool {
        self.terminated.load(Ordering::Acquire)
    }

    pub(crate) fn to_serialized_ref(&self) -> SerializedActorRef {
        SerializedActorRef::new(
            self.system_name.clone(),
            self.path.clone(),
            self.uid,
            self.message_type,
        )
    }

    pub(crate) fn typed_ref<M>(cell: &Arc<Self>) -> Option<ActorRef<M>>
    where
        M: Message,
    {
        let sender = cell.sender.downcast_ref::<mpsc::Sender<Envelope<M>>>()?;
        Some(ActorRef {
            sender: sender.clone(),
            cell: cell.clone(),
        })
    }

    fn mark_enqueued(&self) {
        self.mailbox_depth.fetch_add(1, Ordering::AcqRel);
    }

    fn mark_dequeued(&self) {
        let _previous =
            self.mailbox_depth
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |depth| {
                    Some(depth.saturating_sub(1))
                });
    }

    fn snapshot(&self) -> ActorRuntimeSnapshot {
        let termination_reason = self
            .termination_reason
            .lock()
            .expect("termination reason mutex poisoned")
            .as_ref()
            .map(termination_reason_label)
            .map(str::to_string);

        ActorRuntimeSnapshot::new(
            self.path.clone(),
            self.uid,
            self.mailbox_capacity,
            self.mailbox_depth(),
            self.terminated.load(Ordering::Acquire),
            termination_reason,
        )
    }

    fn watch(&self, recipient: DeathRecipient) {
        let maybe_terminated = {
            let mut watchers = self.watchers.lock().expect("watchers mutex poisoned");
            if self.terminated.load(Ordering::Acquire) {
                self.termination_reason
                    .lock()
                    .expect("termination reason mutex poisoned")
                    .clone()
                    .map(|reason| ActorTerminated {
                        path: self.path.clone(),
                        uid: self.uid,
                        reason,
                    })
            } else {
                watchers.push(recipient);
                return;
            }
        };

        if let Some(terminated) = maybe_terminated {
            recipient.notify(terminated);
        }
    }

    fn mark_terminated(&self, reason: TerminationReason) {
        let watchers = {
            let mut watchers = self.watchers.lock().expect("watchers mutex poisoned");
            *self
                .termination_reason
                .lock()
                .expect("termination reason mutex poisoned") = Some(reason.clone());
            self.mailbox_depth.store(0, Ordering::Release);
            self.terminated.store(true, Ordering::Release);
            watchers.drain(..).collect::<Vec<_>>()
        };

        let terminated = ActorTerminated {
            path: self.path.clone(),
            uid: self.uid,
            reason,
        };

        for watcher in watchers {
            watcher.notify(terminated.clone());
        }
    }
}

fn termination_reason_label(reason: &TerminationReason) -> &'static str {
    match reason {
        TerminationReason::Normal => "normal",
        TerminationReason::Stopped => "stopped",
        TerminationReason::Escalated(_) => "escalated",
        TerminationReason::Failed(_) => "failed",
    }
}

struct DeathRecipient {
    notify: Box<dyn FnOnce(ActorTerminated) + Send + 'static>,
}

impl DeathRecipient {
    fn new(notify: impl FnOnce(ActorTerminated) + Send + 'static) -> Self {
        Self {
            notify: Box::new(notify),
        }
    }

    fn notify(self, terminated: ActorTerminated) {
        (self.notify)(terminated);
    }
}
