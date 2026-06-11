//! Local typed actor runtime.

use std::any::{type_name, Any};
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::future::Future;
use std::marker::PhantomData;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
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

/// Function-style behavior used by the Phase 2 actor facade.
pub trait Behavior<M>: Send + 'static
where
    M: Message,
{
    /// Handles one message.
    fn on_message<'a>(&'a mut self, ctx: &'a mut ActorContext<M>, msg: M) -> ActorFuture<'a>;
}

impl<M, F> Behavior<M> for F
where
    M: Message,
    F: for<'a> FnMut(&'a mut ActorContext<M>, M) -> ActorFuture<'a> + Send + 'static,
{
    fn on_message<'a>(&'a mut self, ctx: &'a mut ActorContext<M>, msg: M) -> ActorFuture<'a> {
        self(ctx, msg)
    }
}

/// Actor implementation backed by a function-style behavior.
pub struct BehaviorActor<M, B>
where
    M: Message,
    B: Behavior<M>,
{
    behavior: B,
    _message: PhantomData<fn(M)>,
}

impl<M, B> BehaviorActor<M, B>
where
    M: Message,
    B: Behavior<M>,
{
    /// Creates a behavior actor.
    #[must_use]
    pub fn new(behavior: B) -> Self {
        Self {
            behavior,
            _message: PhantomData,
        }
    }
}

impl<M, B> Actor for BehaviorActor<M, B>
where
    M: Message,
    B: Behavior<M>,
{
    type Msg = M;

    fn handle<'a>(
        &'a mut self,
        ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        self.behavior.on_message(ctx, msg)
    }
}

/// Actor implementation backed by a synchronous handler function.
pub struct ActorFn<M, F>
where
    M: Message,
    F: FnMut(&mut ActorContext<M>, M) -> ActorResult + Send + 'static,
{
    handler: F,
    _message: PhantomData<fn(M)>,
}

impl<M, F> Actor for ActorFn<M, F>
where
    M: Message,
    F: FnMut(&mut ActorContext<M>, M) -> ActorResult + Send + 'static,
{
    type Msg = M;

    fn handle<'a>(
        &'a mut self,
        ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        let result = (self.handler)(ctx, msg);
        actor_future(async move { result })
    }
}

/// Creates an actor from a synchronous handler function.
#[must_use]
pub fn actor_fn<M, F>(handler: F) -> ActorFn<M, F>
where
    M: Message,
    F: FnMut(&mut ActorContext<M>, M) -> ActorResult + Send + 'static,
{
    ActorFn {
        handler,
        _message: PhantomData,
    }
}

/// Actor that initializes its behavior with access to [`ActorContext`].
pub struct SetupActor<M, F, B>
where
    M: Message,
    F: FnOnce(&mut ActorContext<M>) -> RakkaResult<B> + Send + 'static,
    B: Behavior<M>,
{
    setup: Option<F>,
    behavior: Option<B>,
    _message: PhantomData<fn(M)>,
}

impl<M, F, B> SetupActor<M, F, B>
where
    M: Message,
    F: FnOnce(&mut ActorContext<M>) -> RakkaResult<B> + Send + 'static,
    B: Behavior<M>,
{
    /// Creates a setup actor.
    #[must_use]
    pub fn new(setup: F) -> Self {
        Self {
            setup: Some(setup),
            behavior: None,
            _message: PhantomData,
        }
    }
}

impl<M, F, B> Actor for SetupActor<M, F, B>
where
    M: Message,
    F: FnOnce(&mut ActorContext<M>) -> RakkaResult<B> + Send + 'static,
    B: Behavior<M>,
{
    type Msg = M;

    fn started<'a>(&'a mut self, ctx: &'a mut ActorContext<Self::Msg>) -> ActorFuture<'a> {
        actor_future(async move {
            let setup = self.setup.take().ok_or_else(|| {
                RakkaError::core("setup-already-consumed", "setup actor initialized twice")
            })?;
            self.behavior = Some(setup(ctx)?);
            Ok(ActorAction::Continue)
        })
    }

    fn handle<'a>(
        &'a mut self,
        ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        match self.behavior.as_mut() {
            Some(behavior) => behavior.on_message(ctx, msg),
            None => actor_future(async {
                Err(RakkaError::core(
                    "setup-not-complete",
                    "setup actor received a message before initialization completed",
                ))
            }),
        }
    }
}

/// Creates an actor whose behavior is initialized with its actor context.
#[must_use]
pub fn setup<M, F, B>(setup: F) -> SetupActor<M, F, B>
where
    M: Message,
    F: FnOnce(&mut ActorContext<M>) -> RakkaResult<B> + Send + 'static,
    B: Behavior<M>,
{
    SetupActor::new(setup)
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

/// Actor identity fields useful for tracing and structured logs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorTraceContext {
    system_name: String,
    path: ActorPath,
    uid: ActorUid,
}

impl ActorTraceContext {
    /// Creates actor trace context.
    #[must_use]
    pub fn new(system_name: impl Into<String>, path: ActorPath, uid: ActorUid) -> Self {
        Self {
            system_name: system_name.into(),
            path,
            uid,
        }
    }

    /// Actor system name.
    #[must_use]
    pub fn system_name(&self) -> &str {
        &self.system_name
    }

    /// Logical actor path.
    #[must_use]
    pub const fn path(&self) -> &ActorPath {
        &self.path
    }

    /// Actor incarnation uid.
    #[must_use]
    pub const fn uid(&self) -> ActorUid {
        self.uid
    }
}

/// Handle returned by DeathWatch registration.
#[derive(Debug, Clone)]
pub struct WatchHandle {
    id: u64,
    target: Weak<ActorCell>,
}

impl WatchHandle {
    /// DeathWatch registration id.
    #[must_use]
    pub const fn id(&self) -> u64 {
        self.id
    }

    /// Cancels the watch registration if the target is still alive.
    pub fn cancel(&self) -> bool {
        self.target
            .upgrade()
            .is_some_and(|target| target.unwatch(self.id))
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

    /// Returns actor identity fields for tracing and structured logs.
    #[must_use]
    pub fn trace_context(&self) -> ActorTraceContext {
        self.cell.trace_context()
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

    fn watch(&self, recipient: DeathRecipient) -> WatchHandle {
        ActorCell::watch(&self.cell, recipient)
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
    children: HashMap<String, ActorStopHandle>,
    timers: HashMap<String, TimerHandle<M>>,
    receive_timeout: Option<ReceiveTimeout<M>>,
}

impl<M> ActorContext<M>
where
    M: Message,
{
    fn new(system: ActorSystem, myself: ActorRef<M>) -> Self {
        Self {
            system,
            myself,
            children: HashMap::new(),
            timers: HashMap::new(),
            receive_timeout: None,
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

    /// Returns actor identity fields for tracing and structured logs.
    #[must_use]
    pub fn trace_context(&self) -> ActorTraceContext {
        self.myself.trace_context()
    }

    /// Spawns a child actor with default options.
    pub fn spawn<A>(&mut self, name: impl AsRef<str>, actor: A) -> RakkaResult<ActorRef<A::Msg>>
    where
        A: Actor,
    {
        self.spawn_child(name, actor)
    }

    /// Spawns a restartable child actor with default options.
    pub fn spawn_factory<A, F>(
        &mut self,
        name: impl AsRef<str>,
        factory: F,
    ) -> RakkaResult<ActorRef<A::Msg>>
    where
        A: Actor,
        F: Fn() -> A + Send + Sync + 'static,
    {
        self.spawn_child_factory(name, factory)
    }

    /// Spawns a restartable child actor with explicit options.
    pub fn spawn_with_options<A, F>(
        &mut self,
        name: impl AsRef<str>,
        factory: F,
        options: ActorOptions,
    ) -> RakkaResult<ActorRef<A::Msg>>
    where
        A: Actor,
        F: Fn() -> A + Send + Sync + 'static,
    {
        self.spawn_child_with_options(name, factory, options)
    }

    /// Spawns an anonymous child actor with default options.
    pub fn spawn_anonymous<A>(&mut self, actor: A) -> RakkaResult<ActorRef<A::Msg>>
    where
        A: Actor,
    {
        let actor = Mutex::new(Some(actor));
        self.spawn_anonymous_with_options(
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

    /// Spawns an anonymous restartable child actor with default options.
    pub fn spawn_anonymous_factory<A, F>(&mut self, factory: F) -> RakkaResult<ActorRef<A::Msg>>
    where
        A: Actor,
        F: Fn() -> A + Send + Sync + 'static,
    {
        self.spawn_anonymous_with_options(factory, ActorOptions::default())
    }

    /// Spawns an anonymous restartable child actor with explicit options.
    pub fn spawn_anonymous_with_options<A, F>(
        &mut self,
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

        let (name, path, uid) = self.system.anonymous_child_identity(self.path());
        let actor_ref = spawn_actor_task(self.system.clone(), path, uid, factory, options)?;
        self.children.insert(name, actor_ref.stop_handle());
        Ok(actor_ref)
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
        self.children.insert(name.as_ref().to_string(), stop_handle);
        Ok(actor_ref)
    }

    /// Returns logical paths for live children known to this context.
    #[must_use]
    pub fn children(&self) -> Vec<ActorPath> {
        self.children
            .values()
            .filter(|child| !child.is_terminated())
            .map(ActorStopHandle::path)
            .cloned()
            .collect()
    }

    /// Returns a live child's logical path by name.
    #[must_use]
    pub fn child(&self, name: &str) -> Option<ActorPath> {
        self.children
            .get(name)
            .filter(|child| !child.is_terminated())
            .map(ActorStopHandle::path)
            .cloned()
    }

    /// Watches a target actor and sends `msg` to this actor when it terminates.
    pub fn watch_with<T>(&self, target: &ActorRef<T>, msg: M) -> WatchHandle
    where
        T: Message,
    {
        let myself = self.myself.clone();
        target.watch(DeathRecipient::new(move |_terminated| {
            let _ = myself.tell(msg);
        }))
    }

    /// Watches a target actor and converts termination into this actor's message type.
    pub fn watch<T>(&self, target: &ActorRef<T>) -> WatchHandle
    where
        T: Message,
        M: From<ActorTerminated>,
    {
        let myself = self.myself.clone();
        target.watch(DeathRecipient::new(move |terminated| {
            let _ = myself.tell(M::from(terminated));
        }))
    }

    /// Cancels a DeathWatch registration.
    pub fn unwatch(&self, handle: &WatchHandle) -> bool {
        handle.cancel()
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
            _message: PhantomData,
        }
    }

    /// Starts or replaces a keyed one-shot timer.
    pub fn start_timer_once(&mut self, key: impl Into<String>, delay: Duration, msg: M) {
        let key = key.into();
        if let Some(existing) = self.timers.remove(&key) {
            existing.abort();
        }
        let timer = self.schedule_once(delay, msg);
        self.timers.insert(key, timer);
    }

    /// Cancels a keyed timer.
    pub fn cancel_timer(&mut self, key: &str) -> bool {
        self.timers.remove(key).is_some_and(|timer| {
            timer.abort();
            true
        })
    }

    /// Returns true when a keyed timer is known and has not finished.
    #[must_use]
    pub fn is_timer_active(&self, key: &str) -> bool {
        self.timers
            .get(key)
            .is_some_and(|timer| !timer.is_finished())
    }

    /// Configures a receive timeout using a message factory.
    pub fn set_receive_timeout_factory(
        &mut self,
        delay: Duration,
        build: impl Fn() -> M + Send + Sync + 'static,
    ) {
        self.receive_timeout = Some(ReceiveTimeout {
            delay,
            build: Arc::new(build),
            timer: None,
        });
        self.arm_receive_timeout();
    }

    /// Configures a receive timeout using a cloneable message.
    pub fn set_receive_timeout(&mut self, delay: Duration, msg: M)
    where
        M: Clone + Sync,
    {
        self.set_receive_timeout_factory(delay, move || msg.clone());
    }

    /// Cancels the current receive timeout, if one is configured.
    pub fn cancel_receive_timeout(&mut self) -> bool {
        self.receive_timeout.take().is_some_and(|mut timeout| {
            if let Some(timer) = timeout.timer.take() {
                timer.abort();
            }
            true
        })
    }

    /// Pipes a future result back to this actor.
    pub fn pipe_to_self<T, E, Fut, Map>(&self, future: Fut, map: Map) -> JoinHandle<()>
    where
        T: Send + 'static,
        E: Send + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
        Map: FnOnce(Result<T, E>) -> M + Send + 'static,
    {
        let myself = self.myself.clone();
        tokio::spawn(async move {
            let msg = map(future.await);
            let _ = myself.tell(msg);
        })
    }

    /// Performs an ask and pipes the result back to this actor.
    pub fn ask<T, R, Build, Map>(
        &self,
        target: &ActorRef<T>,
        build: Build,
        timeout: Duration,
        map: Map,
    ) -> JoinHandle<()>
    where
        T: Message,
        R: Send + 'static,
        Build: FnOnce(ReplyTo<R>) -> T + Send + 'static,
        Map: FnOnce(Result<R, AskError>) -> M + Send + 'static,
    {
        let target = target.clone();
        self.pipe_to_self(async move { target.ask(build, timeout).await }, map)
    }

    /// Performs an ask whose successful reply is itself a status result.
    pub fn ask_with_status<T, R, E, Build, Map>(
        &self,
        target: &ActorRef<T>,
        build: Build,
        timeout: Duration,
        map: Map,
    ) -> JoinHandle<()>
    where
        T: Message,
        R: Send + 'static,
        E: Send + 'static,
        Build: FnOnce(ReplyTo<Result<R, E>>) -> T + Send + 'static,
        Map: FnOnce(Result<Result<R, E>, AskError>) -> M + Send + 'static,
    {
        let target = target.clone();
        self.pipe_to_self(async move { target.ask(build, timeout).await }, map)
    }

    /// Spawns a message adapter that converts another protocol into this actor's protocol.
    pub fn message_adapter<N, F>(&mut self, adapt: F) -> RakkaResult<ActorRef<N>>
    where
        N: Message,
        F: FnMut(N) -> M + Send + 'static,
    {
        self.spawn_anonymous(MessageAdapterActor {
            target: self.myself.clone(),
            adapt,
            _input: PhantomData,
        })
    }

    /// Requests a child actor to stop.
    pub fn stop_child<T>(&self, child: &ActorRef<T>) -> Result<(), StopError>
    where
        T: Message,
    {
        child.stop()
    }

    /// Requests an actor to stop.
    pub fn stop<T>(&self, actor: &ActorRef<T>) -> Result<(), StopError>
    where
        T: Message,
    {
        actor.stop()
    }

    /// Requests a named child to stop.
    pub fn stop_child_named(&self, name: &str) -> bool {
        if let Some(child) = self.children.get(name) {
            child.stop();
            true
        } else {
            false
        }
    }

    fn arm_receive_timeout(&mut self) {
        if let Some(timeout) = self.receive_timeout.as_mut() {
            if let Some(timer) = timeout.timer.take() {
                timer.abort();
            }
            let myself = self.myself.clone();
            let delay = timeout.delay;
            let build = timeout.build.clone();
            let handle = tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                let _ = myself.tell(build());
            });
            timeout.timer = Some(TimerHandle {
                handle,
                _message: PhantomData,
            });
        }
    }

    fn stop_runtime_owned_tasks(&mut self) {
        for child in self.children.values() {
            child.stop();
        }
        for (_key, timer) in self.timers.drain() {
            timer.abort();
        }
        if let Some(mut timeout) = self.receive_timeout.take() {
            if let Some(timer) = timeout.timer.take() {
                timer.abort();
            }
        }
    }
}

struct ReceiveTimeout<M>
where
    M: Message,
{
    delay: Duration,
    build: Arc<dyn Fn() -> M + Send + Sync>,
    timer: Option<TimerHandle<M>>,
}

struct MessageAdapterActor<M, N, F>
where
    M: Message,
    N: Message,
    F: FnMut(N) -> M + Send + 'static,
{
    target: ActorRef<M>,
    adapt: F,
    _input: PhantomData<fn(N)>,
}

impl<M, N, F> Actor for MessageAdapterActor<M, N, F>
where
    M: Message,
    N: Message,
    F: FnMut(N) -> M + Send + 'static,
{
    type Msg = N;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        let target = self.target.clone();
        let adapted = (self.adapt)(msg);
        actor_future(async move {
            target.tell(adapted).map_err(|error| match error {
                TellError::Full(_msg) => {
                    RakkaError::core("message-adapter-mailbox-full", "target mailbox was full")
                }
                TellError::Closed(_msg) => RakkaError::core(
                    "message-adapter-mailbox-closed",
                    "target mailbox was closed",
                ),
            })?;
            Ok(ActorAction::Continue)
        })
    }
}

/// Handle for a scheduled actor timer.
pub struct TimerHandle<M>
where
    M: Message,
{
    handle: JoinHandle<()>,
    _message: PhantomData<fn(M)>,
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

    pub(crate) fn path(&self) -> &ActorPath {
        self.cell.path()
    }

    pub(crate) fn is_terminated(&self) -> bool {
        self.cell.is_terminated()
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
        ctx.arm_receive_timeout();
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
                        Ok(Ok(ActorAction::Continue)) => {
                            ctx.arm_receive_timeout();
                        }
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
                            ctx.arm_receive_timeout();
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
                            ctx.arm_receive_timeout();
                        }
                    }
                }
            }
        }
    }

    let _ = catch_actor_result(actor.stopped(&mut ctx, &termination_reason)).await;
    ctx.stop_runtime_owned_tasks();
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
    next_watch_id: AtomicU64,
    watchers: Mutex<Vec<WatchRegistration>>,
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
            next_watch_id: AtomicU64::new(1),
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

    pub(crate) fn trace_context(&self) -> ActorTraceContext {
        ActorTraceContext::new(self.system_name.clone(), self.path.clone(), self.uid)
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

    fn watch(cell: &Arc<Self>, recipient: DeathRecipient) -> WatchHandle {
        let id = cell.next_watch_id.fetch_add(1, Ordering::Relaxed);
        let mut recipient = Some(recipient);
        let maybe_terminated = {
            let mut watchers = cell.watchers.lock().expect("watchers mutex poisoned");
            if cell.terminated.load(Ordering::Acquire) {
                cell.termination_reason
                    .lock()
                    .expect("termination reason mutex poisoned")
                    .clone()
                    .map(|reason| ActorTerminated {
                        path: cell.path.clone(),
                        uid: cell.uid,
                        reason,
                    })
            } else {
                watchers.push(WatchRegistration {
                    id,
                    recipient: recipient
                        .take()
                        .expect("watch recipient should be available"),
                });
                None
            }
        };

        if let Some(terminated) = maybe_terminated {
            recipient
                .take()
                .expect("watch recipient should be available")
                .notify(terminated);
        }

        WatchHandle {
            id,
            target: Arc::downgrade(cell),
        }
    }

    fn unwatch(&self, id: u64) -> bool {
        let mut watchers = self.watchers.lock().expect("watchers mutex poisoned");
        let previous = watchers.len();
        watchers.retain(|watcher| watcher.id != id);
        watchers.len() != previous
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
            watchers
                .drain(..)
                .map(|registration| registration.recipient)
                .collect::<Vec<_>>()
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

struct WatchRegistration {
    id: u64,
    recipient: DeathRecipient,
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
