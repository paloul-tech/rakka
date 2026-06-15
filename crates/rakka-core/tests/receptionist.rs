//! Local receptionist integration tests.

use std::time::Duration;

use rakka_core::{
    actor_future, Actor, ActorAction, ActorContext, ActorFuture, ActorSystem, Receptionist,
    ReceptionistError, ServiceKey,
};

#[derive(Debug)]
enum WorkerCommand {
    Stop,
}

#[derive(Debug)]
struct OtherCommand;

struct WorkerActor;

impl Actor for WorkerActor {
    type Msg = WorkerCommand;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        actor_future(async move {
            match msg {
                WorkerCommand::Stop => Ok(ActorAction::Stop),
            }
        })
    }
}

#[tokio::test]
async fn register_find_and_deregister_local_service() {
    let system = ActorSystem::new("receptionist-register");
    let receptionist = Receptionist::get(&system);
    let key = ServiceKey::<WorkerCommand>::new("workers");
    let worker = system.spawn_actor("worker", WorkerActor).unwrap();
    let _registration = receptionist
        .register(&key, worker.clone())
        .expect("worker should register");

    let listing = receptionist.find(&key).expect("listing should resolve");
    assert_eq!(listing.len(), 1);
    assert!(listing.contains(&worker));
    assert_eq!(listing.key().id(), "workers");

    assert!(receptionist
        .deregister(&key, &worker)
        .expect("worker should deregister"));
    assert!(receptionist
        .find(&key)
        .expect("listing should resolve")
        .is_empty());

    system.shutdown();
}

#[tokio::test]
async fn duplicate_registration_is_one_listing_entry_until_all_leases_drop() {
    let system = ActorSystem::new("receptionist-duplicate");
    let receptionist = Receptionist::get(&system);
    let key = ServiceKey::<WorkerCommand>::new("workers");
    let worker = system.spawn_actor("worker", WorkerActor).unwrap();

    let first = receptionist
        .register(&key, worker.clone())
        .expect("first registration should succeed");
    let second = receptionist
        .register(&key, worker.clone())
        .expect("duplicate registration should succeed");

    assert_eq!(
        receptionist
            .find(&key)
            .expect("listing should resolve")
            .len(),
        1
    );

    drop(first);
    assert_eq!(
        receptionist
            .find(&key)
            .expect("listing should resolve")
            .len(),
        1
    );

    drop(second);
    assert!(receptionist
        .find(&key)
        .expect("listing should resolve")
        .is_empty());

    system.shutdown();
}

#[tokio::test]
async fn dropping_registration_handle_deregisters_service() {
    let system = ActorSystem::new("receptionist-handle-drop");
    let receptionist = Receptionist::get(&system);
    let key = ServiceKey::<WorkerCommand>::new("workers");
    let worker = system.spawn_actor("worker", WorkerActor).unwrap();
    let registration = receptionist
        .register(&key, worker.clone())
        .expect("worker should register");

    assert!(registration.is_active());
    drop(registration);

    assert!(receptionist
        .find(&key)
        .expect("listing should resolve")
        .is_empty());

    system.shutdown();
}

#[tokio::test]
async fn subscription_emits_initial_listing_and_updates() {
    let system = ActorSystem::new("receptionist-subscribe");
    let receptionist = Receptionist::get(&system);
    let key = ServiceKey::<WorkerCommand>::new("workers");
    let worker = system.spawn_actor("worker", WorkerActor).unwrap();
    let _registration = receptionist
        .register(&key, worker.clone())
        .expect("worker should register");
    let mut subscription = receptionist
        .subscribe(&key)
        .expect("subscription should start");

    let initial = subscription.recv().await.expect("initial listing");
    assert_eq!(initial.len(), 1);
    assert!(initial.contains(&worker));

    receptionist
        .deregister(&key, &worker)
        .expect("worker should deregister");
    let empty = tokio::time::timeout(Duration::from_secs(1), subscription.recv())
        .await
        .expect("deregister update should arrive")
        .expect("deregister listing should resolve");
    assert!(empty.is_empty());

    system.shutdown();
}

#[tokio::test]
async fn actor_termination_removes_registration_and_notifies_subscribers() {
    let system = ActorSystem::new("receptionist-termination");
    let receptionist = Receptionist::get(&system);
    let key = ServiceKey::<WorkerCommand>::new("workers");
    let worker = system.spawn_actor("worker", WorkerActor).unwrap();
    let mut subscription = receptionist
        .subscribe(&key)
        .expect("subscription should start");
    let initial = subscription.recv().await.expect("initial listing");
    assert!(initial.is_empty());
    let _registration = receptionist
        .register(&key, worker.clone())
        .expect("worker should register");

    let registered = tokio::time::timeout(Duration::from_secs(1), subscription.recv())
        .await
        .expect("register update should arrive")
        .expect("register listing should resolve");
    assert_eq!(registered.len(), 1);

    worker.tell(WorkerCommand::Stop).unwrap();
    let _terminated = worker.when_terminated().await;
    let empty = tokio::time::timeout(Duration::from_secs(1), subscription.recv())
        .await
        .expect("termination update should arrive")
        .expect("termination listing should resolve");
    assert!(empty.is_empty());
    assert!(receptionist
        .find(&key)
        .expect("listing should resolve")
        .is_empty());

    system.shutdown();
}

#[tokio::test]
async fn service_key_type_mismatch_fails_closed() {
    let system = ActorSystem::new("receptionist-type-mismatch");
    let receptionist = Receptionist::get(&system);
    let worker_key = ServiceKey::<WorkerCommand>::new("workers");
    let other_key = ServiceKey::<OtherCommand>::new("workers");
    let worker = system.spawn_actor("worker", WorkerActor).unwrap();
    let _registration = receptionist
        .register(&worker_key, worker)
        .expect("worker should register");

    let error = receptionist
        .find(&other_key)
        .expect_err("same service id with another protocol should fail");
    assert!(matches!(
        error,
        ReceptionistError::ServiceKeyTypeMismatch { .. }
    ));

    system.shutdown();
}
