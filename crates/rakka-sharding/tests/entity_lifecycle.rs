//! Local entity lifecycle: messages held across a stop, one idle timer per entity,
//! self-stopping entities, and the stop-window hazards of issue #112.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rakka_cluster::{
    ClusterMembership, ClusterNode, DiscoverySnapshot, MembershipConfig, NodeAddress, NodeId,
};
use rakka_core::{
    actor_future, Actor, ActorAction, ActorContext, ActorFuture, ActorOptions, ActorSystem,
    DeadLetterReason, ReplyTo, TerminationReason,
};
use rakka_sharding::{
    ClusterSharding, Entity, EntityContext, EntityDeliveryFailure, EntityId, EntityRef,
    EntityRoute, EntityTellError, EntityType, EntityTypeKey, LocalEntityContext, LocalEntityRoute,
    ShardCoordinator, ShardHandoffState, ShardRegion, ShardingConfig,
};
use tokio::sync::{mpsc, oneshot, Notify, Semaphore};

#[derive(Debug)]
enum LifecycleCommand {
    Record(String),
    Get(ReplyTo<Vec<String>>),
    Passivate,
}

#[derive(Clone)]
struct StopGate {
    entered: mpsc::UnboundedSender<()>,
    release: Arc<Notify>,
}

struct LifecycleEntity {
    incarnation: usize,
    items: Vec<String>,
    log: Arc<Mutex<Vec<(usize, String)>>>,
    stop_gate: Option<StopGate>,
    handle_gate: Option<Arc<Semaphore>>,
}

impl Actor for LifecycleEntity {
    type Msg = LifecycleCommand;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        actor_future(async move {
            match msg {
                LifecycleCommand::Record(value) => {
                    if let Some(gate) = &self.handle_gate {
                        gate.acquire().await.expect("handle gate open").forget();
                    }
                    self.log
                        .lock()
                        .unwrap()
                        .push((self.incarnation, value.clone()));
                    self.items.push(value);
                }
                LifecycleCommand::Get(reply_to) => {
                    let _ = reply_to.reply(self.items.clone());
                }
                LifecycleCommand::Passivate => {
                    self.log
                        .lock()
                        .unwrap()
                        .push((self.incarnation, "passivate".to_string()));
                    return Ok(ActorAction::Stop);
                }
            }
            Ok(ActorAction::Continue)
        })
    }

    fn stopped<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        _reason: &'a TerminationReason,
    ) -> ActorFuture<'a> {
        actor_future(async move {
            if let Some(gate) = &self.stop_gate {
                let _ = gate.entered.send(());
                gate.release.notified().await;
            }
            Ok(ActorAction::Continue)
        })
    }
}

type LifecycleFactory = Box<dyn Fn(LocalEntityContext) -> LifecycleEntity + Send + Sync>;
type LifecycleRoute = LocalEntityRoute<LifecycleCommand, LifecycleEntity, LifecycleFactory>;

#[derive(Default)]
struct FixtureConfig {
    idle: Option<Duration>,
    mailbox_capacity: Option<usize>,
    stop_gate: bool,
    handle_gate: bool,
}

struct Fixture {
    system: ActorSystem,
    entity_type: EntityType,
    sharding_config: ShardingConfig,
    route: LifecycleRoute,
    region: ShardRegion<LifecycleCommand>,
    log: Arc<Mutex<Vec<(usize, String)>>>,
    incarnations: Arc<AtomicUsize>,
    entered: mpsc::UnboundedReceiver<()>,
    release: Arc<Notify>,
    handle_gate: Option<Arc<Semaphore>>,
}

fn node(logical_id: &str, incarnation: &str) -> ClusterNode {
    ClusterNode::new(
        NodeId::new(logical_id, incarnation),
        NodeAddress::new(format!("{logical_id}.rakka.default.svc"), 2552),
    )
}

fn membership_with_up_nodes(nodes: Vec<ClusterNode>) -> ClusterMembership {
    let local = nodes[0].clone();
    let config = MembershipConfig::new(1, Duration::from_millis(50), Duration::from_millis(100));
    let mut membership = ClusterMembership::new(local, config);
    membership
        .record_discovery(DiscoverySnapshot::new("test", 1, nodes))
        .unwrap();
    for member in membership
        .snapshot()
        .members()
        .iter()
        .map(|member| member.node().id().clone())
        .collect::<Vec<_>>()
    {
        membership.mark_up(&member, 2).unwrap();
    }
    membership
}

fn fixture(name: &str, config: FixtureConfig) -> Fixture {
    let membership = membership_with_up_nodes(vec![node("rakka-0", "uid-a")]);
    let local_node_id = membership.local_node_id().clone();
    let entity_type = EntityType::new("Lifecycle");
    let sharding_config = ShardingConfig::new(4).unwrap();
    let mut coordinator = ShardCoordinator::new(entity_type.clone(), sharding_config.clone());
    coordinator.reconcile(&membership);

    let system = ActorSystem::new(name);
    let log = Arc::new(Mutex::new(Vec::new()));
    let incarnations = Arc::new(AtomicUsize::new(0));
    let (entered_tx, entered) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let handle_gate = config.handle_gate.then(|| Arc::new(Semaphore::new(0)));
    let stop_gate = config.stop_gate.then(|| StopGate {
        entered: entered_tx,
        release: release.clone(),
    });
    let factory: LifecycleFactory = Box::new({
        let log = log.clone();
        let incarnations = incarnations.clone();
        let handle_gate = handle_gate.clone();
        move |_context: LocalEntityContext| LifecycleEntity {
            incarnation: incarnations.fetch_add(1, Ordering::SeqCst) + 1,
            items: Vec::new(),
            log: log.clone(),
            stop_gate: stop_gate.clone(),
            handle_gate: handle_gate.clone(),
        }
    });
    let mut route = LocalEntityRoute::new(local_node_id, system.clone(), factory);
    if let Some(idle) = config.idle {
        route = route.with_idle_passivation(idle);
    }
    if let Some(capacity) = config.mailbox_capacity {
        route = route.with_actor_options(ActorOptions::default().with_mailbox_capacity(capacity));
    }
    let region = ShardRegion::from_snapshot(
        entity_type.clone(),
        sharding_config.clone(),
        &coordinator.snapshot(),
        route.clone(),
    )
    .unwrap();

    Fixture {
        system,
        entity_type,
        sharding_config,
        route,
        region,
        log,
        incarnations,
        entered,
        release,
        handle_gate,
    }
}

impl Fixture {
    fn entity(&self, id: &str) -> EntityRef<LifecycleCommand> {
        self.region.entity_ref(id)
    }

    fn record(
        &self,
        entity: &EntityRef<LifecycleCommand>,
        value: &str,
    ) -> Result<(), EntityTellError<LifecycleCommand>> {
        entity.tell(&self.region, LifecycleCommand::Record(value.to_string()))
    }

    /// Sends a `Get` synchronously (so it is queued at the call site, not on first poll)
    /// and returns the reply receiver.
    fn send_get(&self, entity: &EntityRef<LifecycleCommand>) -> oneshot::Receiver<Vec<String>> {
        let (sender, receiver) = oneshot::channel();
        entity
            .tell(&self.region, LifecycleCommand::Get(ReplyTo::new(sender)))
            .unwrap();
        receiver
    }

    async fn get(&self, entity: &EntityRef<LifecycleCommand>) -> Vec<String> {
        entity
            .ask(&self.region, LifecycleCommand::Get, Duration::from_secs(1))
            .await
            .unwrap()
    }

    async fn await_entered(&mut self) {
        tokio::time::timeout(Duration::from_secs(1), self.entered.recv())
            .await
            .expect("entity should enter its stopped hook")
            .expect("entered channel open");
    }

    fn log(&self) -> Vec<(usize, String)> {
        self.log.lock().unwrap().clone()
    }

    /// Waits until the entity has handled `value` in incarnation `incarnation`, so a
    /// small mailbox has room for the `Get` that follows.
    async fn wait_for_handled(&self, incarnation: usize, value: &str) {
        let wanted = (incarnation, value.to_string());
        for _attempt in 0..200 {
            if self.log.lock().unwrap().contains(&wanted) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("entity never handled {wanted:?}; log = {:?}", self.log());
    }

    fn incarnations(&self) -> usize {
        self.incarnations.load(Ordering::SeqCst)
    }
}

fn log_entries(entries: &[(usize, &str)]) -> Vec<(usize, String)> {
    entries
        .iter()
        .map(|(incarnation, value)| (*incarnation, (*value).to_string()))
        .collect()
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_string()).collect()
}

async fn wait_for_entity_count(route: &LifecycleRoute, expected: usize, within: Duration) {
    let started = tokio::time::Instant::now();
    while started.elapsed() < within {
        if route.entity_count() == expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(route.entity_count(), expected);
}

async fn wait_for_active(route: &LifecycleRoute, entity_id: &EntityId) {
    for _attempt in 0..200 {
        if route.entity_actor(entity_id).is_some() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("entity {entity_id} did not become active");
}

async fn reply(receiver: oneshot::Receiver<Vec<String>>) -> Vec<String> {
    tokio::time::timeout(Duration::from_secs(1), receiver)
        .await
        .expect("reply within a second")
        .expect("reply not dropped")
}

#[tokio::test]
async fn message_held_during_idle_stop_is_handled_by_next_incarnation() {
    let mut fixture = fixture(
        "held-idle-stop",
        FixtureConfig {
            idle: Some(Duration::from_millis(20)),
            stop_gate: true,
            ..FixtureConfig::default()
        },
    );
    let entity = fixture.entity("entity-1");

    fixture.record(&entity, "first").unwrap();
    fixture.await_entered().await;

    fixture.record(&entity, "held").unwrap();
    let pending = fixture.send_get(&entity);
    fixture.release.notify_one();

    assert_eq!(reply(pending).await, strings(&["held"]));
    assert_eq!(fixture.log(), log_entries(&[(1, "first"), (2, "held")]));
    assert_eq!(fixture.route.entity_count(), 1);
}

#[tokio::test]
async fn held_messages_keep_arrival_order_ahead_of_later_ones() {
    let mut fixture = fixture(
        "held-order",
        FixtureConfig {
            stop_gate: true,
            ..FixtureConfig::default()
        },
    );
    let entity = fixture.entity("entity-1");

    fixture.record(&entity, "zero").unwrap();
    assert!(fixture.route.passivate_entity(entity.entity_id()));
    fixture.await_entered().await;

    fixture.record(&entity, "a").unwrap();
    fixture.record(&entity, "b").unwrap();
    fixture.record(&entity, "c").unwrap();
    fixture.release.notify_one();
    wait_for_active(&fixture.route, entity.entity_id()).await;

    fixture.record(&entity, "d").unwrap();
    assert_eq!(fixture.get(&entity).await, strings(&["a", "b", "c", "d"]));
}

#[tokio::test]
async fn held_messages_are_bounded_by_mailbox_capacity() {
    let mut fixture = fixture(
        "held-bound",
        FixtureConfig {
            mailbox_capacity: Some(2),
            stop_gate: true,
            ..FixtureConfig::default()
        },
    );
    let entity = fixture.entity("entity-1");

    fixture.record(&entity, "zero").unwrap();
    assert!(fixture.route.passivate_entity(entity.entity_id()));
    fixture.await_entered().await;

    fixture.record(&entity, "h1").unwrap();
    fixture.record(&entity, "h2").unwrap();
    assert!(matches!(
        fixture.record(&entity, "third"),
        Err(EntityTellError::Delivery {
            failure: EntityDeliveryFailure::MailboxFull,
            message: LifecycleCommand::Record(value),
        }) if value == "third"
    ));

    fixture.release.notify_one();
    fixture.wait_for_handled(2, "h2").await;
    assert_eq!(fixture.get(&entity).await, strings(&["h1", "h2"]));
}

#[tokio::test]
async fn explicit_passivation_queues_stop_message_and_stop_together() {
    let mut fixture = fixture(
        "explicit-passivation",
        FixtureConfig {
            stop_gate: true,
            ..FixtureConfig::default()
        },
    );
    let entity = fixture.entity("entity-1");

    fixture.record(&entity, "a").unwrap();
    assert!(fixture
        .route
        .passivate_entity_with(entity.entity_id(), Some(LifecycleCommand::Passivate)));
    fixture.await_entered().await;
    assert_eq!(fixture.log(), log_entries(&[(1, "a"), (1, "passivate")]));

    assert!(fixture
        .route
        .passivate_entity_with(entity.entity_id(), Some(LifecycleCommand::Passivate)));
    fixture.record(&entity, "b").unwrap();
    fixture.release.notify_one();

    assert_eq!(fixture.get(&entity).await, strings(&["b"]));
    assert_eq!(fixture.incarnations(), 2);
    assert_eq!(
        fixture.log(),
        log_entries(&[(1, "a"), (1, "passivate"), (2, "b")])
    );
}

#[tokio::test]
async fn self_passivating_entity_answers_two_consecutive_asks_without_buffering() {
    let system = ActorSystem::new("facade-no-buffering");
    let sharding = ClusterSharding::get(&system);
    let key = EntityTypeKey::<LifecycleCommand>::new("FacadeLifecycle")
        .with_number_of_shards(4)
        .unwrap();
    let (entered_tx, mut entered) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let log = Arc::new(Mutex::new(Vec::new()));
    let stop_gate = StopGate {
        entered: entered_tx,
        release: release.clone(),
    };
    let registration = sharding
        .init(
            Entity::of(key.clone(), {
                let log = log.clone();
                move |_context: EntityContext<LifecycleCommand>| LifecycleEntity {
                    incarnation: 0,
                    items: Vec::new(),
                    log: log.clone(),
                    stop_gate: Some(stop_gate.clone()),
                    handle_gate: None,
                }
            })
            .without_buffering(),
        )
        .unwrap();
    let entity = registration.entity_ref_for("entity-1");

    let first = entity
        .ask(LifecycleCommand::Get, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(first, Vec::<String>::new());
    assert!(sharding
        .passivate_entity_id(&key, &EntityId::new("entity-1"))
        .unwrap());
    tokio::time::timeout(Duration::from_secs(1), entered.recv())
        .await
        .expect("entity should enter its stopped hook")
        .unwrap();

    let (sender, receiver) = oneshot::channel();
    entity
        .tell(LifecycleCommand::Get(ReplyTo::new(sender)))
        .unwrap();
    release.notify_one();

    assert_eq!(reply(receiver).await, Vec::<String>::new());
    system.shutdown();
}

#[tokio::test]
async fn stop_that_cannot_be_queued_leaves_entity_active() {
    let fixture = fixture(
        "abandoned-stop",
        FixtureConfig {
            idle: Some(Duration::from_millis(20)),
            mailbox_capacity: Some(1),
            handle_gate: true,
            ..FixtureConfig::default()
        },
    );
    let entity = fixture.entity("entity-1");

    fixture.record(&entity, "blocked").unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    fixture.record(&entity, "queued").unwrap();
    tokio::time::sleep(Duration::from_millis(60)).await;

    assert_eq!(fixture.route.entity_count(), 1);
    assert!(fixture.route.entity_actor(entity.entity_id()).is_some());

    fixture.handle_gate.as_ref().unwrap().add_permits(2);
    wait_for_entity_count(&fixture.route, 0, Duration::from_millis(200)).await;

    fixture.handle_gate.as_ref().unwrap().add_permits(1);
    fixture.record(&entity, "after").unwrap();
    fixture.wait_for_handled(2, "after").await;
    assert_eq!(fixture.get(&entity).await, strings(&["after"]));
    assert_eq!(fixture.route.entity_count(), 1);
    assert_eq!(
        fixture.log(),
        log_entries(&[(1, "blocked"), (1, "queued"), (2, "after")])
    );
}

#[tokio::test]
async fn self_stopping_entity_drops_queued_messages_as_dead_letters_and_holds_later_ones() {
    let mut fixture = fixture(
        "self-stop",
        FixtureConfig {
            stop_gate: true,
            ..FixtureConfig::default()
        },
    );
    let mut dead_letters = fixture.system.subscribe_dead_letters();
    let entity = fixture.entity("entity-1");

    entity
        .tell(&fixture.region, LifecycleCommand::Passivate)
        .unwrap();
    fixture.record(&entity, "behind-1").unwrap();
    fixture.record(&entity, "behind-2").unwrap();
    fixture.await_entered().await;

    fixture.record(&entity, "after").unwrap();
    fixture.release.notify_one();
    assert_eq!(fixture.get(&entity).await, strings(&["after"]));

    let mut seen = Vec::new();
    while let Ok(Ok(dead_letter)) =
        tokio::time::timeout(Duration::from_millis(200), dead_letters.recv()).await
    {
        seen.push(dead_letter);
    }
    assert_eq!(seen.len(), 2, "dead letters: {seen:?}");
    assert!(seen
        .iter()
        .all(|dead_letter| dead_letter.reason == DeadLetterReason::MailboxClosed));
}

#[tokio::test]
async fn shard_transfer_refuses_delivery_after_state_flip_and_holds_after_reacquire() {
    let mut fixture = fixture(
        "shard-transfer",
        FixtureConfig {
            stop_gate: true,
            ..FixtureConfig::default()
        },
    );
    let entity = fixture.entity("entity-1");
    let shard = entity.shard_id(&fixture.sharding_config);

    fixture.record(&entity, "a").unwrap();
    assert_eq!(fixture.route.mark_shard_transferring(shard), 1);
    fixture.await_entered().await;

    assert!(matches!(
        fixture.record(&entity, "b"),
        Err(EntityTellError::Delivery {
            failure: EntityDeliveryFailure::ShardHandoff {
                state: ShardHandoffState::Transferring,
                ..
            },
            ..
        })
    ));

    fixture.route.mark_shard_acquired(shard);
    fixture.record(&entity, "c").unwrap();
    fixture.release.notify_one();
    assert_eq!(fixture.get(&entity).await, strings(&["c"]));
}

#[tokio::test]
async fn remembered_replay_during_stop_reactivates_entity() {
    let mut fixture = fixture(
        "remembered-replay",
        FixtureConfig {
            stop_gate: true,
            ..FixtureConfig::default()
        },
    );
    let entity = fixture.entity("entity-1");
    let shard = entity.shard_id(&fixture.sharding_config);

    fixture.record(&entity, "a").unwrap();
    assert!(fixture.route.passivate_entity(entity.entity_id()));
    fixture.await_entered().await;

    let activated = <LifecycleRoute as EntityRoute<LifecycleCommand>>::activate_entity(
        &fixture.route,
        &fixture.entity_type,
        entity.entity_id(),
        shard,
    )
    .unwrap();
    assert!(activated);

    fixture.release.notify_one();
    wait_for_active(&fixture.route, entity.entity_id()).await;
    assert_eq!(fixture.get(&entity).await, Vec::<String>::new());
    assert_eq!(fixture.incarnations(), 2);
}

#[tokio::test]
async fn delivery_racing_the_watcher_lands_after_held_messages() {
    let mut fixture = fixture(
        "racing-watcher",
        FixtureConfig {
            stop_gate: true,
            ..FixtureConfig::default()
        },
    );
    let entity = fixture.entity("entity-1");

    fixture.record(&entity, "zero").unwrap();
    assert!(fixture.route.passivate_entity(entity.entity_id()));
    fixture.await_entered().await;
    fixture.record(&entity, "h1").unwrap();
    fixture.record(&entity, "h2").unwrap();

    let go = Arc::new(Notify::new());
    let racer = tokio::spawn({
        let go = go.clone();
        let region = fixture.region.clone();
        let entity = entity.clone();
        async move {
            go.notified().await;
            entity
                .tell(&region, LifecycleCommand::Record("late".to_string()))
                .unwrap();
        }
    });
    go.notify_one();
    fixture.release.notify_one();
    racer.await.unwrap();

    assert_eq!(fixture.get(&entity).await, strings(&["h1", "h2", "late"]));
}

#[tokio::test]
async fn held_messages_are_dropped_when_the_system_is_terminating() {
    let mut fixture = fixture(
        "terminating-system",
        FixtureConfig {
            stop_gate: true,
            ..FixtureConfig::default()
        },
    );
    let entity = fixture.entity("entity-1");

    fixture.record(&entity, "zero").unwrap();
    assert!(fixture.route.passivate_entity(entity.entity_id()));
    fixture.await_entered().await;
    fixture.record(&entity, "h1").unwrap();

    let terminate = tokio::spawn({
        let system = fixture.system.clone();
        async move { system.terminate().await }
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    fixture.release.notify_one();

    wait_for_entity_count(&fixture.route, 0, Duration::from_secs(2)).await;
    terminate.await.unwrap().unwrap();
    assert!(matches!(
        fixture.record(&entity, "later"),
        Err(EntityTellError::Delivery {
            failure: EntityDeliveryFailure::SpawnFailed(_),
            ..
        })
    ));
}

#[tokio::test]
async fn idle_timer_is_one_task_per_entity() {
    let fixture = fixture(
        "one-idle-timer",
        FixtureConfig {
            idle: Some(Duration::from_millis(200)),
            ..FixtureConfig::default()
        },
    );
    let entity = fixture.entity("entity-1");

    for index in 0..1_000 {
        fixture.record(&entity, &format!("m{index}")).unwrap();
    }
    assert_eq!(fixture.route.armed_idle_timers(), 1);

    wait_for_entity_count(&fixture.route, 0, Duration::from_secs(2)).await;
    assert_eq!(fixture.route.armed_idle_timers(), 0);
}

#[tokio::test]
async fn idle_deadline_extends_on_each_delivery() {
    let fixture = fixture(
        "idle-deadline-extends",
        FixtureConfig {
            idle: Some(Duration::from_millis(50)),
            ..FixtureConfig::default()
        },
    );
    let entity = fixture.entity("entity-1");
    let started = tokio::time::Instant::now();

    fixture.record(&entity, "t0").unwrap();
    tokio::time::sleep_until(started + Duration::from_millis(30)).await;
    fixture.record(&entity, "t30").unwrap();
    tokio::time::sleep_until(started + Duration::from_millis(60)).await;
    fixture.record(&entity, "t60").unwrap();
    tokio::time::sleep_until(started + Duration::from_millis(90)).await;
    assert_eq!(fixture.route.entity_count(), 1);

    wait_for_entity_count(&fixture.route, 0, Duration::from_millis(110)).await;
    assert!(started.elapsed() < Duration::from_millis(200));
}

#[tokio::test]
async fn idle_timer_from_a_previous_incarnation_does_not_stop_the_next() {
    let mut fixture = fixture(
        "idle-timer-old-incarnation",
        FixtureConfig {
            idle: Some(Duration::from_millis(100)),
            stop_gate: true,
            ..FixtureConfig::default()
        },
    );
    let entity = fixture.entity("entity-1");
    let started = tokio::time::Instant::now();

    fixture.record(&entity, "a").unwrap();
    tokio::time::sleep_until(started + Duration::from_millis(20)).await;
    assert!(fixture
        .route
        .passivate_entity_with(entity.entity_id(), None));
    fixture.await_entered().await;
    fixture.record(&entity, "b").unwrap();
    fixture.release.notify_one();
    fixture.wait_for_handled(2, "b").await;

    tokio::time::sleep_until(started + Duration::from_millis(110)).await;
    assert_eq!(fixture.route.entity_count(), 1);
    assert!(fixture.route.entity_actor(entity.entity_id()).is_some());

    wait_for_entity_count(&fixture.route, 0, Duration::from_millis(190)).await;
    assert!(started.elapsed() < Duration::from_millis(300));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_message_is_lost_across_repeated_idle_stops() {
    const ENTITIES: usize = 50;
    const SENDERS: usize = 4;
    const PER_SENDER: usize = 2_500;
    const TOTAL: usize = SENDERS * PER_SENDER;

    let fixture = fixture(
        "stress-idle-stops",
        FixtureConfig {
            idle: Some(Duration::from_millis(1)),
            ..FixtureConfig::default()
        },
    );
    let entities = (0..ENTITIES)
        .map(|index| fixture.entity(&format!("entity-{index}")))
        .collect::<Vec<_>>();

    let mut senders = Vec::new();
    for sender in 0..SENDERS {
        let region = fixture.region.clone();
        let entities = entities.clone();
        senders.push(tokio::spawn(async move {
            let mut refused = 0usize;
            for index in 0..PER_SENDER {
                let sequence = sender * PER_SENDER + index;
                let entity = &entities[sequence % ENTITIES];
                match entity.tell(&region, LifecycleCommand::Record(format!("m{sequence}"))) {
                    Ok(()) => {}
                    Err(EntityTellError::Delivery {
                        failure: EntityDeliveryFailure::MailboxFull,
                        ..
                    }) => refused += 1,
                    Err(other) => panic!("unexpected delivery failure: {other:?}"),
                }
                if index % 97 == 0 {
                    tokio::task::yield_now().await;
                }
            }
            refused
        }));
    }
    let mut refused = 0usize;
    for sender in senders {
        refused += sender.await.unwrap();
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline
        && fixture.log.lock().unwrap().len() + refused < TOTAL
    {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let handled = fixture.log.lock().unwrap().len();
    assert_eq!(
        handled + refused,
        TOTAL,
        "handled {handled} + refused {refused} != sent {TOTAL}"
    );
    wait_for_entity_count(&fixture.route, 0, Duration::from_secs(2)).await;
}
