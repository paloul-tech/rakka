//! Local actor runtime integration tests.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rakka_core::{
    actor_fn, actor_future, setup, Actor, ActorAction, ActorContext, ActorFuture, ActorOptions,
    ActorSystem, ActorSystemRuntimeSettings, ActorSystemShutdownConfig, Behavior, DeadLetterReason,
    RakkaError, ReplyTo, SerializedActorRef, SupervisionStrategy, TellError,
};
use tokio::sync::{mpsc, Notify};

#[derive(Debug)]
enum EchoMessage {
    Ping(ReplyTo<&'static str>),
}

struct EchoActor;

impl Actor for EchoActor {
    type Msg = EchoMessage;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        actor_future(async move {
            match msg {
                EchoMessage::Ping(reply_to) => {
                    let _ = reply_to.reply("pong");
                }
            }
            Ok(ActorAction::Continue)
        })
    }
}

#[tokio::test]
async fn ask_returns_reply() {
    let system = ActorSystem::new("ask");
    let echo = system.spawn_actor("echo", EchoActor).unwrap();

    let reply = echo
        .ask(EchoMessage::Ping, Duration::from_secs(1))
        .await
        .unwrap();

    assert_eq!(reply, "pong");
    system.shutdown();
}

#[derive(Debug)]
enum BlockingMessage {
    Block,
    Queued,
    Extra,
}

struct BlockingActor {
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

impl Actor for BlockingActor {
    type Msg = BlockingMessage;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        let entered = self.entered.clone();
        let release = self.release.clone();
        actor_future(async move {
            if matches!(msg, BlockingMessage::Block) {
                entered.notify_one();
                release.notified().await;
            }
            Ok(ActorAction::Continue)
        })
    }
}

#[tokio::test]
async fn bounded_mailbox_reports_full() {
    let system = ActorSystem::new("mailbox");
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let entered_wait = entered.notified();

    let actor = system
        .spawn_actor_with_options(
            "blocking",
            {
                let entered = entered.clone();
                let release = release.clone();
                move || BlockingActor {
                    entered: entered.clone(),
                    release: release.clone(),
                }
            },
            ActorOptions::default().with_mailbox_capacity(1),
        )
        .unwrap();

    actor.tell(BlockingMessage::Block).unwrap();
    entered_wait.await;
    actor.tell(BlockingMessage::Queued).unwrap();

    assert!(matches!(
        actor.tell(BlockingMessage::Extra),
        Err(TellError::Full(BlockingMessage::Extra))
    ));

    release.notify_waiters();
    system.shutdown();
}

#[derive(Debug)]
enum TimerMessage {
    Start,
    Tick,
}

struct TimerActor {
    sender: mpsc::Sender<&'static str>,
}

impl Actor for TimerActor {
    type Msg = TimerMessage;

    fn handle<'a>(
        &'a mut self,
        ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        let sender = self.sender.clone();
        if matches!(msg, TimerMessage::Start) {
            let _timer = ctx.schedule_once(Duration::from_millis(10), TimerMessage::Tick);
        }

        actor_future(async move {
            if matches!(msg, TimerMessage::Tick) {
                sender.send("tick").await.unwrap();
            }
            Ok(ActorAction::Continue)
        })
    }
}

#[tokio::test]
async fn scheduled_timer_delivers_message() {
    let system = ActorSystem::new("timer");
    let (sender, mut receiver) = mpsc::channel(1);
    let actor = system.spawn_actor("timer", TimerActor { sender }).unwrap();

    actor.tell(TimerMessage::Start).unwrap();

    let observed = tokio::time::timeout(Duration::from_secs(1), receiver.recv())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(observed, "tick");
    system.shutdown();
}

#[derive(Debug)]
enum ChildMessage {
    Stop,
}

struct ChildActor;

impl Actor for ChildActor {
    type Msg = ChildMessage;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        _msg: Self::Msg,
    ) -> ActorFuture<'a> {
        actor_future(async { Ok(ActorAction::Stop) })
    }
}

#[derive(Debug)]
enum ParentMessage {
    Start,
    ChildGone,
}

struct ParentActor {
    sender: mpsc::Sender<&'static str>,
}

impl Actor for ParentActor {
    type Msg = ParentMessage;

    fn handle<'a>(
        &'a mut self,
        ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        let sender = self.sender.clone();
        if matches!(msg, ParentMessage::Start) {
            let child = ctx.spawn_child("child", ChildActor).unwrap();
            ctx.watch_with(&child, ParentMessage::ChildGone);
            child.tell(ChildMessage::Stop).unwrap();
        }

        actor_future(async move {
            if matches!(msg, ParentMessage::ChildGone) {
                sender.send("gone").await.unwrap();
            }
            Ok(ActorAction::Continue)
        })
    }
}

#[tokio::test]
async fn child_spawn_and_watch_notification_work() {
    let system = ActorSystem::new("watch");
    let (sender, mut receiver) = mpsc::channel(1);
    let parent = system
        .spawn_actor("parent", ParentActor { sender })
        .unwrap();

    parent.tell(ParentMessage::Start).unwrap();

    let observed = tokio::time::timeout(Duration::from_secs(1), receiver.recv())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(observed, "gone");
    system.shutdown();
}

#[derive(Debug)]
enum StopMessage {
    Stop,
}

struct StopActor;

impl Actor for StopActor {
    type Msg = StopMessage;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        _msg: Self::Msg,
    ) -> ActorFuture<'a> {
        actor_future(async { Ok(ActorAction::Stop) })
    }
}

#[tokio::test]
async fn sending_to_stopped_actor_publishes_dead_letter() {
    let system = ActorSystem::new("dead-letter");
    let mut dead_letters = system.subscribe_dead_letters();
    let actor = system.spawn_actor("stop", StopActor).unwrap();

    actor.tell(StopMessage::Stop).unwrap();
    wait_until_terminated(&actor).await;

    assert!(matches!(
        actor.tell(StopMessage::Stop),
        Err(TellError::Closed(StopMessage::Stop))
    ));

    let dead_letter = tokio::time::timeout(Duration::from_secs(1), dead_letters.recv())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(dead_letter.reason, DeadLetterReason::MailboxClosed);
    assert_eq!(dead_letter.recipient, actor.path().clone());
    system.shutdown();
}

#[derive(Debug)]
enum SupervisedMessage {
    Fail,
    Get(ReplyTo<usize>),
}

struct SupervisedActor {
    generation: usize,
}

impl Actor for SupervisedActor {
    type Msg = SupervisedMessage;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        let generation = self.generation;
        actor_future(async move {
            match msg {
                SupervisedMessage::Fail => Err(RakkaError::core("boom", "actor failed")),
                SupervisedMessage::Get(reply_to) => {
                    let _ = reply_to.reply(generation);
                    Ok(ActorAction::Continue)
                }
            }
        })
    }
}

#[tokio::test]
async fn restart_supervision_replaces_actor_instance() {
    let system = ActorSystem::new("restart");
    let next_generation = Arc::new(AtomicUsize::new(1));
    let actor = system
        .spawn_actor_with_options(
            "supervised",
            {
                let next_generation = next_generation.clone();
                move || SupervisedActor {
                    generation: next_generation.fetch_add(1, Ordering::SeqCst),
                }
            },
            ActorOptions::default().with_supervision(SupervisionStrategy::Restart),
        )
        .unwrap();

    let before = actor
        .ask(SupervisedMessage::Get, Duration::from_secs(1))
        .await
        .unwrap();
    actor.tell(SupervisedMessage::Fail).unwrap();
    let after = actor
        .ask(SupervisedMessage::Get, Duration::from_secs(1))
        .await
        .unwrap();

    assert_eq!(before, 1);
    assert_eq!(after, 2);
    system.shutdown();
}

struct StatefulActor {
    value: usize,
}

impl Actor for StatefulActor {
    type Msg = SupervisedMessage;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        match msg {
            SupervisedMessage::Fail => {
                self.value += 1;
                actor_future(async { Err(RakkaError::core("resume", "actor failed")) })
            }
            SupervisedMessage::Get(reply_to) => {
                let value = self.value;
                actor_future(async move {
                    let _ = reply_to.reply(value);
                    Ok(ActorAction::Continue)
                })
            }
        }
    }
}

#[tokio::test]
async fn resume_supervision_keeps_actor_instance() {
    let system = ActorSystem::new("resume");
    let actor = system
        .spawn_actor_with_options(
            "stateful",
            || StatefulActor { value: 0 },
            ActorOptions::default().with_supervision(SupervisionStrategy::Resume),
        )
        .unwrap();

    actor.tell(SupervisedMessage::Fail).unwrap();
    let value = actor
        .ask(SupervisedMessage::Get, Duration::from_secs(1))
        .await
        .unwrap();

    assert_eq!(value, 1);
    system.shutdown();
}

#[tokio::test]
async fn duplicate_root_names_are_rejected_until_reincarnation() {
    let system = ActorSystem::new("identity");
    let first = system.spawn_actor("same", StopActor).unwrap();
    let first_path = first.path().clone();
    let first_uid = first.uid();

    let duplicate = system.spawn_actor("same", StopActor).unwrap_err();
    assert_eq!(duplicate.code(), "actor-path-in-use");

    let termination = first.when_terminated();
    first.tell(StopMessage::Stop).unwrap();
    let terminated = termination.await;
    assert_eq!(terminated.path, first_path);
    assert_eq!(terminated.uid, first_uid);

    let second = system.spawn_actor("same", StopActor).unwrap();
    assert_eq!(second.path(), &first_path);
    assert_ne!(second.uid(), first_uid);
    system.terminate().await.unwrap();
}

#[derive(Debug)]
enum DuplicateChildMessage {
    Start,
}

struct DuplicateChildParent {
    sender: mpsc::Sender<String>,
}

impl Actor for DuplicateChildParent {
    type Msg = DuplicateChildMessage;

    fn handle<'a>(
        &'a mut self,
        ctx: &'a mut ActorContext<Self::Msg>,
        _msg: Self::Msg,
    ) -> ActorFuture<'a> {
        let sender = self.sender.clone();
        let duplicate = {
            let _first = ctx.spawn_child("child", ChildActor).unwrap();
            ctx.spawn_child("child", ChildActor).unwrap_err()
        };

        actor_future(async move {
            sender.send(duplicate.code().to_string()).await.unwrap();
            Ok(ActorAction::Continue)
        })
    }
}

#[tokio::test]
async fn duplicate_live_child_names_are_rejected() {
    let system = ActorSystem::new("children");
    let (sender, mut receiver) = mpsc::channel(1);
    let parent = system
        .spawn_actor("parent", DuplicateChildParent { sender })
        .unwrap();

    parent.tell(DuplicateChildMessage::Start).unwrap();

    let error_code = tokio::time::timeout(Duration::from_secs(1), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(error_code, "actor-path-in-use");
    system.terminate().await.unwrap();
}

#[tokio::test]
async fn actor_ref_resolver_round_trips_and_rejects_stale_refs() {
    let system = ActorSystem::new("resolver");
    let resolver = system.actor_ref_resolver();
    let actor = system.spawn_actor("echo", EchoActor).unwrap();
    let serialized = resolver.to_serialized_ref(&actor);

    let resolved = resolver.resolve::<EchoMessage>(&serialized).unwrap();
    let reply = resolved
        .ask(EchoMessage::Ping, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(reply, "pong");

    let wrong_system = SerializedActorRef::new(
        "other-system",
        serialized.path().clone(),
        serialized.uid(),
        serialized.message_type().to_string(),
    );
    let error = resolver.resolve::<EchoMessage>(&wrong_system).unwrap_err();
    assert_eq!(error.code(), "actor-ref-system-mismatch");

    let wrong_message_type = resolver.resolve::<StopMessage>(&serialized).unwrap_err();
    assert_eq!(wrong_message_type.code(), "actor-ref-message-type-mismatch");

    actor.stop().unwrap();
    actor.when_terminated().await;
    let replacement = system.spawn_actor("echo", EchoActor).unwrap();
    assert_eq!(replacement.path(), serialized.path());
    assert_ne!(replacement.uid(), serialized.uid());

    let stale = resolver.resolve::<EchoMessage>(&serialized).unwrap_err();
    assert_eq!(stale.code(), "actor-ref-incarnation-mismatch");
    system.terminate().await.unwrap();
}

#[tokio::test]
async fn deathwatch_notification_keeps_old_uid_after_reincarnation() {
    let system = ActorSystem::new("deathwatch-uid");
    let first = system.spawn_actor("watched", StopActor).unwrap();
    let first_path = first.path().clone();
    let first_uid = first.uid();

    let termination = first.when_terminated();
    first.tell(StopMessage::Stop).unwrap();
    let terminated = termination.await;

    let second = system.spawn_actor("watched", StopActor).unwrap();
    assert_eq!(terminated.path, first_path);
    assert_eq!(terminated.uid, first_uid);
    assert_eq!(second.path(), &first_path);
    assert_ne!(second.uid(), first_uid);
    system.terminate().await.unwrap();
}

#[tokio::test]
async fn actor_system_builder_and_terminate_complete_lifecycle() {
    let system = ActorSystem::builder("lifecycle")
        .with_serialization_registry(String::from("registry"))
        .with_runtime_settings(ActorSystemRuntimeSettings::new(8))
        .with_shutdown_config(ActorSystemShutdownConfig::new(Duration::from_secs(1)))
        .build()
        .await
        .unwrap();
    assert_eq!(system.runtime_settings().default_mailbox_capacity(), 8);
    assert!(system
        .serialization_registry()
        .expect("registry should be configured")
        .is::<String>());

    let actor = system.spawn_actor("echo", EchoActor).unwrap();
    assert!(!actor.is_terminated());

    let waiter = {
        let system = system.clone();
        tokio::spawn(async move {
            system.when_terminated().await;
        })
    };
    system.terminate().await.unwrap();
    waiter.await.unwrap();
    assert!(system.is_terminated());
    assert!(actor.is_terminated());

    let late_spawn = system.spawn_actor("late", StopActor).unwrap_err();
    assert_eq!(late_spawn.code(), "system-terminating");
}

#[derive(Debug)]
enum FunctionActorMessage {
    Increment(ReplyTo<usize>),
}

#[tokio::test]
async fn function_actor_facade_handles_messages() {
    let system = ActorSystem::new("function-actor");
    let mut count = 0usize;
    let counter = system
        .spawn(
            "counter",
            actor_fn(
                move |_ctx: &mut ActorContext<FunctionActorMessage>, msg: FunctionActorMessage| {
                    match msg {
                        FunctionActorMessage::Increment(reply_to) => {
                            count += 1;
                            let value = count;
                            let _ = reply_to.reply(value);
                            Ok(ActorAction::Continue)
                        }
                    }
                },
            ),
        )
        .unwrap();

    assert_eq!(
        counter
            .ask(FunctionActorMessage::Increment, Duration::from_secs(1))
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        counter
            .ask(FunctionActorMessage::Increment, Duration::from_secs(1))
            .await
            .unwrap(),
        2
    );
    system.terminate().await.unwrap();
}

#[derive(Debug)]
enum SetupMessage {
    GetPath(ReplyTo<String>),
}

struct SetupBehavior {
    path: String,
}

impl Behavior<SetupMessage> for SetupBehavior {
    fn on_message<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<SetupMessage>,
        msg: SetupMessage,
    ) -> ActorFuture<'a> {
        let path = self.path.clone();
        actor_future(async move {
            match msg {
                SetupMessage::GetPath(reply_to) => {
                    let _ = reply_to.reply(path);
                }
            }
            Ok(ActorAction::Continue)
        })
    }
}

#[tokio::test]
async fn setup_actor_facade_initializes_with_context() {
    let system = ActorSystem::new("setup");
    let actor = system
        .spawn(
            "configured",
            setup(|ctx: &mut ActorContext<SetupMessage>| {
                Ok(SetupBehavior {
                    path: ctx.path().to_string(),
                })
            }),
        )
        .unwrap();

    let path = actor
        .ask(SetupMessage::GetPath, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(path, actor.path().to_string());
    system.terminate().await.unwrap();
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ContextFacadeMessage {
    Start,
    Adapted(&'static str),
    TimerFired,
    Piped(u32),
    Asked(Result<&'static str, rakka_core::AskError>),
    WatchedGone,
    UnwatchedGone,
    Timeout,
}

struct ContextFacadeActor {
    echo: rakka_core::ActorRef<EchoMessage>,
    events: mpsc::Sender<&'static str>,
}

impl Actor for ContextFacadeActor {
    type Msg = ContextFacadeMessage;

    fn handle<'a>(
        &'a mut self,
        ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        let events = self.events.clone();

        match msg {
            ContextFacadeMessage::Start => {
                let child = ctx.spawn("child", ChildActor).unwrap();
                let unwatch_child = ctx.spawn_anonymous(ChildActor).unwrap();
                assert!(ctx.child("child").is_some());
                assert!(ctx.children().len() >= 2);
                assert!(ctx.trace_context().path().as_str().contains("/user/parent"));

                ctx.watch_with(&child, ContextFacadeMessage::WatchedGone);
                let handle = ctx.watch_with(&unwatch_child, ContextFacadeMessage::UnwatchedGone);
                assert!(ctx.unwatch(&handle));
                assert!(ctx.stop_child_named("child"));
                ctx.stop(&unwatch_child).unwrap();

                let adapter = ctx.message_adapter(ContextFacadeMessage::Adapted).unwrap();
                adapter.tell("adapted").unwrap();

                ctx.start_timer_once(
                    "tick",
                    Duration::from_millis(10),
                    ContextFacadeMessage::TimerFired,
                );
                assert!(ctx.is_timer_active("tick"));

                ctx.pipe_to_self(async { Ok::<u32, ()>(7) }, |result| {
                    ContextFacadeMessage::Piped(result.unwrap())
                });
                ctx.ask(
                    &self.echo,
                    EchoMessage::Ping,
                    Duration::from_secs(1),
                    ContextFacadeMessage::Asked,
                );
                ctx.set_receive_timeout(Duration::from_millis(25), ContextFacadeMessage::Timeout);

                actor_future(async move {
                    events.send("started").await.unwrap();
                    Ok(ActorAction::Continue)
                })
            }
            ContextFacadeMessage::Adapted(value) => actor_future(async move {
                events.send(value).await.unwrap();
                Ok(ActorAction::Continue)
            }),
            ContextFacadeMessage::TimerFired => actor_future(async move {
                events.send("timer").await.unwrap();
                Ok(ActorAction::Continue)
            }),
            ContextFacadeMessage::Piped(value) => actor_future(async move {
                assert_eq!(value, 7);
                events.send("piped").await.unwrap();
                Ok(ActorAction::Continue)
            }),
            ContextFacadeMessage::Asked(result) => actor_future(async move {
                assert_eq!(result.unwrap(), "pong");
                events.send("asked").await.unwrap();
                Ok(ActorAction::Continue)
            }),
            ContextFacadeMessage::WatchedGone => actor_future(async move {
                events.send("watched").await.unwrap();
                Ok(ActorAction::Continue)
            }),
            ContextFacadeMessage::UnwatchedGone => actor_future(async move {
                events.send("unwatched").await.unwrap();
                Ok(ActorAction::Continue)
            }),
            ContextFacadeMessage::Timeout => actor_future(async move {
                events.send("timeout").await.unwrap();
                Ok(ActorAction::Stop)
            }),
        }
    }
}

#[tokio::test]
async fn context_facade_covers_spawn_watch_timers_ask_and_pipe_to_self() {
    let system = ActorSystem::new("context-facade");
    let echo = system.spawn("echo", EchoActor).unwrap();
    let (events, mut observed) = mpsc::channel(16);
    let parent = system
        .spawn("parent", ContextFacadeActor { echo, events })
        .unwrap();

    parent.tell(ContextFacadeMessage::Start).unwrap();

    let mut seen = Vec::new();
    while seen.len() < 7 {
        let event = tokio::time::timeout(Duration::from_secs(1), observed.recv())
            .await
            .unwrap()
            .unwrap();
        seen.push(event);
    }

    assert!(seen.contains(&"started"));
    assert!(seen.contains(&"adapted"));
    assert!(seen.contains(&"timer"));
    assert!(seen.contains(&"piped"));
    assert!(seen.contains(&"asked"));
    assert!(seen.contains(&"watched"));
    assert!(seen.contains(&"timeout"));
    assert!(!seen.contains(&"unwatched"));
    system.terminate().await.unwrap();
}

async fn wait_until_terminated<M>(actor: &rakka_core::ActorRef<M>)
where
    M: rakka_core::Message,
{
    for _ in 0..100 {
        if actor.is_terminated() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("actor did not terminate");
}
