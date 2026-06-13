//! Local typed receptionist and service discovery.

use std::any::Any;
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use crate::{ActorRef, ActorSystem, Message, RakkaError, SerializedActorRef};

const RECEPTIONIST_EVENT_CAPACITY: usize = 1024;

/// Convenient result alias for local receptionist operations.
pub type ReceptionistResult<T> = Result<T, ReceptionistError>;

/// Failure returned by local receptionist operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReceptionistError {
    /// A service id was already associated with another message protocol.
    ServiceKeyTypeMismatch {
        /// Service id.
        service_id: String,
        /// Message type already associated with the service id.
        expected: String,
        /// Message type requested by the caller.
        actual: String,
    },
    /// A subscription sender was dropped.
    SubscriptionClosed {
        /// Service id.
        service_id: String,
    },
    /// A subscriber lagged behind the bounded receptionist event buffer.
    SubscriptionLagged {
        /// Service id.
        service_id: String,
        /// Number of skipped listing-change signals.
        skipped: u64,
    },
}

impl Display for ReceptionistError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::ServiceKeyTypeMismatch {
                service_id,
                expected,
                actual,
            } => write!(
                f,
                "service key '{service_id}' is registered for '{expected}' but was used as '{actual}'"
            ),
            Self::SubscriptionClosed { service_id } => {
                write!(f, "receptionist subscription for '{service_id}' closed")
            }
            Self::SubscriptionLagged {
                service_id,
                skipped,
            } => write!(
                f,
                "receptionist subscription for '{service_id}' lagged by {skipped} updates"
            ),
        }
    }
}

impl Error for ReceptionistError {}

impl From<ReceptionistError> for RakkaError {
    fn from(error: ReceptionistError) -> Self {
        RakkaError::core("receptionist-error", error.to_string())
    }
}

/// Typed key used to register and discover local service actors.
pub struct ServiceKey<M>
where
    M: Message,
{
    id: String,
    message_type: &'static str,
    _message: PhantomData<fn(M)>,
}

impl<M> ServiceKey<M>
where
    M: Message,
{
    /// Creates a service key for message protocol `M`.
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            message_type: std::any::type_name::<M>(),
            _message: PhantomData,
        }
    }

    /// Stable service id.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Rust message type associated with this key.
    #[must_use]
    pub fn message_type(&self) -> &'static str {
        self.message_type
    }
}

impl<M> Clone for ServiceKey<M>
where
    M: Message,
{
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            message_type: self.message_type,
            _message: PhantomData,
        }
    }
}

impl<M> PartialEq for ServiceKey<M>
where
    M: Message,
{
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.message_type == other.message_type
    }
}

impl<M> Eq for ServiceKey<M> where M: Message {}

impl<M> Hash for ServiceKey<M>
where
    M: Message,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.message_type.hash(state);
    }
}

impl<M> Debug for ServiceKey<M>
where
    M: Message,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServiceKey")
            .field("id", &self.id)
            .field("message_type", &self.message_type)
            .finish()
    }
}

/// Current typed receptionist listing for a service key.
#[derive(Clone)]
pub struct Listing<M>
where
    M: Message,
{
    key: ServiceKey<M>,
    service_instances: Vec<ActorRef<M>>,
    revision: u64,
}

impl<M> Listing<M>
where
    M: Message,
{
    fn new(key: ServiceKey<M>, service_instances: Vec<ActorRef<M>>, revision: u64) -> Self {
        Self {
            key,
            service_instances,
            revision,
        }
    }

    /// Service key this listing describes.
    #[must_use]
    pub const fn key(&self) -> &ServiceKey<M> {
        &self.key
    }

    /// Typed service actor refs currently registered for the key.
    #[must_use]
    pub fn service_instances(&self) -> &[ActorRef<M>] {
        &self.service_instances
    }

    /// Monotonic receptionist revision for this service id.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// Alias for [`Listing::service_instances`].
    #[must_use]
    pub fn routees(&self) -> &[ActorRef<M>] {
        self.service_instances()
    }

    /// Number of registered service actors.
    #[must_use]
    pub fn len(&self) -> usize {
        self.service_instances.len()
    }

    /// Returns true when no service actor is currently registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.service_instances.is_empty()
    }

    /// Returns true when the listing contains the supplied actor incarnation.
    #[must_use]
    pub fn contains(&self, actor_ref: &ActorRef<M>) -> bool {
        self.service_instances.iter().any(|candidate| {
            candidate.path() == actor_ref.path() && candidate.uid() == actor_ref.uid()
        })
    }
}

impl<M> Debug for Listing<M>
where
    M: Message,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Listing")
            .field("key", &self.key)
            .field("service_instances", &self.service_instances)
            .field("revision", &self.revision)
            .finish()
    }
}

/// Local receptionist facade for typed service discovery.
#[derive(Clone)]
pub struct Receptionist {
    system: ActorSystem,
    registry: Arc<ReceptionistRegistry>,
}

impl Receptionist {
    /// Returns the local receptionist facade for an actor system.
    #[must_use]
    pub fn get(system: &ActorSystem) -> Self {
        Self {
            system: system.clone(),
            registry: system.receptionist_registry(),
        }
    }

    /// Registers an actor under a typed service key.
    pub fn register<M>(
        &self,
        key: &ServiceKey<M>,
        actor_ref: ActorRef<M>,
    ) -> ReceptionistResult<ReceptionistRegistration<M>>
    where
        M: Message,
    {
        let identity = ActorIdentity::from_actor_ref(&actor_ref);
        self.registry
            .register(key, identity.clone(), actor_ref.clone())?;

        let registry = self.registry.clone();
        let key_id = key.id().to_string();
        let cleanup_identity = identity.clone();
        let watched = actor_ref.clone();
        let cleanup_task = tokio::spawn(async move {
            let _terminated = watched.when_terminated().await;
            registry.remove_identity(&key_id, &cleanup_identity);
        });

        Ok(ReceptionistRegistration {
            registry: self.registry.clone(),
            key: key.clone(),
            actor_ref,
            identity,
            cleanup_task: Some(cleanup_task),
            active: true,
        })
    }

    /// Removes all registrations for an actor under a typed service key.
    pub fn deregister<M>(
        &self,
        key: &ServiceKey<M>,
        actor_ref: &ActorRef<M>,
    ) -> ReceptionistResult<bool>
    where
        M: Message,
    {
        self.registry
            .deregister(key, &ActorIdentity::from_actor_ref(actor_ref))
    }

    /// Finds the current typed listing for a service key.
    pub fn find<M>(&self, key: &ServiceKey<M>) -> ReceptionistResult<Listing<M>>
    where
        M: Message,
    {
        let (revision, entries) = self.registry.entries_for(key)?;
        self.listing_from_entries(key, entries, revision)
    }

    /// Finds only service actors registered directly with this local
    /// receptionist, excluding propagated clustered listings.
    pub fn find_local<M>(&self, key: &ServiceKey<M>) -> ReceptionistResult<Listing<M>>
    where
        M: Message,
    {
        let (revision, entries) = self.registry.local_entries_for(key)?;
        self.listing_from_entries(key, entries, revision)
    }

    /// Installs or replaces a propagated listing from another cluster node.
    ///
    /// Lower or equal versions are ignored so stale remote updates cannot
    /// overwrite a newer listing already applied for the same source node and
    /// service key.
    pub fn install_remote_listing<M>(
        &self,
        node_id: impl Into<String>,
        key: &ServiceKey<M>,
        routees: Vec<ActorRef<M>>,
        version: u64,
        observed_at_millis: u64,
    ) -> ReceptionistResult<bool>
    where
        M: Message,
    {
        self.registry.install_remote_listing(
            node_id.into(),
            key,
            routees,
            version,
            observed_at_millis,
        )
    }

    /// Removes all propagated listings for a remote cluster node.
    pub fn remove_remote_node(&self, node_id: &str) -> bool {
        self.registry.remove_remote_node(node_id)
    }

    /// Removes propagated listings last observed before `older_than_millis`.
    ///
    /// Returns the number of remote service listings removed.
    pub fn expire_remote_listings(&self, older_than_millis: u64) -> usize {
        self.registry.expire_remote_listings(older_than_millis)
    }

    /// Subscribes to current and future listings for a service key.
    pub fn subscribe<M>(
        &self,
        key: &ServiceKey<M>,
    ) -> ReceptionistResult<ReceptionistSubscription<M>>
    where
        M: Message,
    {
        let receiver = self.registry.subscribe(key)?;
        let initial = self.find(key)?;
        Ok(ReceptionistSubscription {
            receptionist: self.clone(),
            key: key.clone(),
            pending_initial: Some(initial),
            receiver,
        })
    }

    fn listing_from_entries<M>(
        &self,
        key: &ServiceKey<M>,
        entries: Vec<ServiceEntry>,
        revision: u64,
    ) -> ReceptionistResult<Listing<M>>
    where
        M: Message,
    {
        let resolver = self.system.actor_ref_resolver();
        let mut stale = Vec::new();
        let mut service_instances = Vec::new();
        for entry in entries {
            if let Some(actor_ref) = entry
                .typed_ref
                .as_ref()
                .and_then(|typed_ref| typed_ref.as_ref().downcast_ref::<ActorRef<M>>())
            {
                if actor_ref.is_terminated() {
                    stale.push(entry.key());
                } else {
                    service_instances.push(actor_ref.clone());
                }
                continue;
            }

            match resolver.resolve::<M>(&entry.serialized) {
                Ok(actor_ref) if !actor_ref.is_terminated() => service_instances.push(actor_ref),
                Ok(_) | Err(_) => stale.push(entry.key()),
            }
        }

        if !stale.is_empty() {
            self.registry.remove_entries(key.id(), &stale);
        }

        Ok(Listing::new(key.clone(), service_instances, revision))
    }
}

impl Debug for Receptionist {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receptionist")
            .field("system", &self.system.name())
            .finish_non_exhaustive()
    }
}

/// Active registration lease returned by [`Receptionist::register`].
pub struct ReceptionistRegistration<M>
where
    M: Message,
{
    registry: Arc<ReceptionistRegistry>,
    key: ServiceKey<M>,
    actor_ref: ActorRef<M>,
    identity: ActorIdentity,
    cleanup_task: Option<JoinHandle<()>>,
    active: bool,
}

impl<M> ReceptionistRegistration<M>
where
    M: Message,
{
    /// Service key this registration uses.
    #[must_use]
    pub const fn key(&self) -> &ServiceKey<M> {
        &self.key
    }

    /// Actor registered under the service key.
    #[must_use]
    pub const fn actor_ref(&self) -> &ActorRef<M> {
        &self.actor_ref
    }

    /// Returns true while this registration lease has not been released.
    #[must_use]
    pub const fn is_active(&self) -> bool {
        self.active
    }

    /// Explicitly releases this registration lease.
    pub fn deregister(mut self) -> ReceptionistResult<bool> {
        self.abort_cleanup_task();
        if self.active {
            self.active = false;
            self.registry.release(&self.key, &self.identity)
        } else {
            Ok(false)
        }
    }

    fn abort_cleanup_task(&mut self) {
        if let Some(cleanup_task) = self.cleanup_task.take() {
            cleanup_task.abort();
        }
    }
}

impl<M> Debug for ReceptionistRegistration<M>
where
    M: Message,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReceptionistRegistration")
            .field("key", &self.key)
            .field("actor_ref", &self.actor_ref)
            .field("active", &self.active)
            .finish()
    }
}

impl<M> Drop for ReceptionistRegistration<M>
where
    M: Message,
{
    fn drop(&mut self) {
        self.abort_cleanup_task();
        if self.active {
            self.active = false;
            let _ = self.registry.release(&self.key, &self.identity);
        }
    }
}

/// Active typed receptionist subscription.
pub struct ReceptionistSubscription<M>
where
    M: Message,
{
    receptionist: Receptionist,
    key: ServiceKey<M>,
    pending_initial: Option<Listing<M>>,
    receiver: broadcast::Receiver<u64>,
}

impl<M> ReceptionistSubscription<M>
where
    M: Message,
{
    /// Receives the initial listing or the next listing after a change.
    pub async fn recv(&mut self) -> ReceptionistResult<Listing<M>> {
        if let Some(initial) = self.pending_initial.take() {
            return Ok(initial);
        }

        match self.receiver.recv().await {
            Ok(_revision) => self.receptionist.find(&self.key),
            Err(broadcast::error::RecvError::Closed) => {
                Err(ReceptionistError::SubscriptionClosed {
                    service_id: self.key.id().to_string(),
                })
            }
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                Err(ReceptionistError::SubscriptionLagged {
                    service_id: self.key.id().to_string(),
                    skipped,
                })
            }
        }
    }
}

impl<M> Debug for ReceptionistSubscription<M>
where
    M: Message,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReceptionistSubscription")
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ActorIdentity {
    path: String,
    uid: u64,
}

impl ActorIdentity {
    fn from_actor_ref<M>(actor_ref: &ActorRef<M>) -> Self
    where
        M: Message,
    {
        Self {
            path: actor_ref.path().to_string(),
            uid: actor_ref.uid().value(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct ReceptionistRegistry {
    services: Mutex<BTreeMap<String, ServiceRegistry>>,
}

impl ReceptionistRegistry {
    pub(crate) fn new() -> Self {
        Self {
            services: Mutex::new(BTreeMap::new()),
        }
    }

    fn register<M>(
        &self,
        key: &ServiceKey<M>,
        identity: ActorIdentity,
        actor_ref: ActorRef<M>,
    ) -> ReceptionistResult<()>
    where
        M: Message,
    {
        let mut services = self.services.lock().expect("receptionist mutex poisoned");
        let service = services
            .entry(key.id().to_string())
            .or_insert_with(|| ServiceRegistry::new(key.message_type()));
        service.verify_type(key)?;
        let is_new = service.register(identity, actor_ref);
        if is_new {
            service.publish_change();
        }
        Ok(())
    }

    fn deregister<M>(
        &self,
        key: &ServiceKey<M>,
        identity: &ActorIdentity,
    ) -> ReceptionistResult<bool>
    where
        M: Message,
    {
        let mut services = self.services.lock().expect("receptionist mutex poisoned");
        let Some(service) = services.get_mut(key.id()) else {
            return Ok(false);
        };
        service.verify_type(key)?;
        let removed = service.remove(identity);
        if removed {
            service.publish_change();
        }
        Ok(removed)
    }

    fn release<M>(&self, key: &ServiceKey<M>, identity: &ActorIdentity) -> ReceptionistResult<bool>
    where
        M: Message,
    {
        let mut services = self.services.lock().expect("receptionist mutex poisoned");
        let Some(service) = services.get_mut(key.id()) else {
            return Ok(false);
        };
        service.verify_type(key)?;
        let removed = service.release(identity);
        if removed {
            service.publish_change();
        }
        Ok(removed)
    }

    fn entries_for<M>(&self, key: &ServiceKey<M>) -> ReceptionistResult<(u64, Vec<ServiceEntry>)>
    where
        M: Message,
    {
        let services = self.services.lock().expect("receptionist mutex poisoned");
        let Some(service) = services.get(key.id()) else {
            return Ok((0, Vec::new()));
        };
        service.verify_type(key)?;
        Ok((service.revision, service.entries()))
    }

    fn local_entries_for<M>(
        &self,
        key: &ServiceKey<M>,
    ) -> ReceptionistResult<(u64, Vec<ServiceEntry>)>
    where
        M: Message,
    {
        let services = self.services.lock().expect("receptionist mutex poisoned");
        let Some(service) = services.get(key.id()) else {
            return Ok((0, Vec::new()));
        };
        service.verify_type(key)?;
        Ok((service.revision, service.local_entries()))
    }

    fn subscribe<M>(&self, key: &ServiceKey<M>) -> ReceptionistResult<broadcast::Receiver<u64>>
    where
        M: Message,
    {
        let mut services = self.services.lock().expect("receptionist mutex poisoned");
        let service = services
            .entry(key.id().to_string())
            .or_insert_with(|| ServiceRegistry::new(key.message_type()));
        service.verify_type(key)?;
        Ok(service.sender.subscribe())
    }

    fn remove_identity(&self, key_id: &str, identity: &ActorIdentity) -> bool {
        let mut services = self.services.lock().expect("receptionist mutex poisoned");
        let Some(service) = services.get_mut(key_id) else {
            return false;
        };
        let removed = service.remove(identity);
        if removed {
            service.publish_change();
        }
        removed
    }

    fn remove_entries(&self, key_id: &str, entries: &[ServiceEntryKey]) -> bool {
        let mut services = self.services.lock().expect("receptionist mutex poisoned");
        let Some(service) = services.get_mut(key_id) else {
            return false;
        };
        let mut removed_any = false;
        for entry in entries {
            removed_any |= service.remove_entry(entry);
        }
        if removed_any {
            service.publish_change();
        }
        removed_any
    }

    fn install_remote_listing<M>(
        &self,
        node_id: String,
        key: &ServiceKey<M>,
        routees: Vec<ActorRef<M>>,
        version: u64,
        observed_at_millis: u64,
    ) -> ReceptionistResult<bool>
    where
        M: Message,
    {
        let mut services = self.services.lock().expect("receptionist mutex poisoned");
        let service = services
            .entry(key.id().to_string())
            .or_insert_with(|| ServiceRegistry::new(key.message_type()));
        service.verify_type(key)?;
        let changed = service.install_remote_listing(node_id, routees, version, observed_at_millis);
        if changed {
            service.publish_change();
        }
        Ok(changed)
    }

    fn remove_remote_node(&self, node_id: &str) -> bool {
        let mut services = self.services.lock().expect("receptionist mutex poisoned");
        let mut removed_any = false;
        for service in services.values_mut() {
            if service.remote_listings.remove(node_id).is_some() {
                service.publish_change();
                removed_any = true;
            }
        }
        removed_any
    }

    fn expire_remote_listings(&self, older_than_millis: u64) -> usize {
        let mut services = self.services.lock().expect("receptionist mutex poisoned");
        let mut removed = 0;
        for service in services.values_mut() {
            let before = service.remote_listings.len();
            service
                .remote_listings
                .retain(|_node_id, listing| listing.observed_at_millis >= older_than_millis);
            let service_removed = before.saturating_sub(service.remote_listings.len());
            if service_removed > 0 {
                removed += service_removed;
                service.publish_change();
            }
        }
        removed
    }
}

#[derive(Debug)]
struct ServiceRegistry {
    message_type: String,
    local_records: BTreeMap<ActorIdentity, ServiceRecord>,
    remote_listings: BTreeMap<String, RemoteListing>,
    revision: u64,
    sender: broadcast::Sender<u64>,
}

impl ServiceRegistry {
    fn new(message_type: &'static str) -> Self {
        let (sender, _) = broadcast::channel(RECEPTIONIST_EVENT_CAPACITY);
        Self {
            message_type: message_type.to_string(),
            local_records: BTreeMap::new(),
            remote_listings: BTreeMap::new(),
            revision: 0,
            sender,
        }
    }

    fn verify_type<M>(&self, key: &ServiceKey<M>) -> ReceptionistResult<()>
    where
        M: Message,
    {
        if self.message_type == key.message_type() {
            Ok(())
        } else {
            Err(ReceptionistError::ServiceKeyTypeMismatch {
                service_id: key.id().to_string(),
                expected: self.message_type.clone(),
                actual: key.message_type().to_string(),
            })
        }
    }

    fn register<M>(&mut self, identity: ActorIdentity, actor_ref: ActorRef<M>) -> bool
    where
        M: Message,
    {
        if let Some(record) = self.local_records.get_mut(&identity) {
            record.leases = record.leases.saturating_add(1);
            record.serialized = actor_ref.to_serialized_ref();
            record.typed_ref = Some(Arc::new(actor_ref));
            false
        } else {
            self.local_records.insert(
                identity,
                ServiceRecord {
                    serialized: actor_ref.to_serialized_ref(),
                    typed_ref: Some(Arc::new(actor_ref)),
                    leases: 1,
                },
            );
            true
        }
    }

    fn release(&mut self, identity: &ActorIdentity) -> bool {
        let Some(record) = self.local_records.get_mut(identity) else {
            return false;
        };
        record.leases = record.leases.saturating_sub(1);
        if record.leases == 0 {
            self.local_records.remove(identity);
            true
        } else {
            false
        }
    }

    fn remove(&mut self, identity: &ActorIdentity) -> bool {
        self.local_records.remove(identity).is_some()
    }

    fn entries(&self) -> Vec<ServiceEntry> {
        let mut entries = self.local_entries();
        entries.extend(self.remote_listings.iter().flat_map(|(node_id, listing)| {
            listing
                .records
                .iter()
                .map(|(identity, record)| ServiceEntry {
                    source: ServiceEntrySource::Remote {
                        node_id: node_id.clone(),
                    },
                    identity: identity.clone(),
                    serialized: record.serialized.clone(),
                    typed_ref: Some(record.typed_ref.clone()),
                })
        }));
        entries
    }

    fn local_entries(&self) -> Vec<ServiceEntry> {
        self.local_records
            .iter()
            .map(|(identity, record)| ServiceEntry {
                source: ServiceEntrySource::Local,
                identity: identity.clone(),
                serialized: record.serialized.clone(),
                typed_ref: record.typed_ref.clone(),
            })
            .collect()
    }

    fn install_remote_listing<M>(
        &mut self,
        node_id: String,
        routees: Vec<ActorRef<M>>,
        version: u64,
        observed_at_millis: u64,
    ) -> bool
    where
        M: Message,
    {
        if let Some(existing) = self.remote_listings.get(&node_id) {
            if version < existing.version {
                return false;
            }
        }
        if let Some(existing) = self.remote_listings.get_mut(&node_id) {
            if version == existing.version {
                existing.observed_at_millis = existing.observed_at_millis.max(observed_at_millis);
                return false;
            }
        } else if version == 0 && routees.is_empty() {
            return false;
        }

        let records = routees
            .into_iter()
            .filter(|routee| !routee.is_terminated())
            .map(|routee| {
                (
                    ActorIdentity::from_actor_ref(&routee),
                    RemoteServiceRecord {
                        serialized: routee.to_serialized_ref(),
                        typed_ref: Arc::new(routee),
                    },
                )
            })
            .collect();
        self.remote_listings.insert(
            node_id,
            RemoteListing {
                version,
                observed_at_millis,
                records,
            },
        );
        true
    }

    fn remove_entry(&mut self, entry: &ServiceEntryKey) -> bool {
        match entry {
            ServiceEntryKey::Local { identity } => self.local_records.remove(identity).is_some(),
            ServiceEntryKey::Remote { node_id, identity } => {
                let Some(listing) = self.remote_listings.get_mut(node_id) else {
                    return false;
                };
                let removed = listing.records.remove(identity).is_some();
                if listing.records.is_empty() {
                    self.remote_listings.remove(node_id);
                }
                removed
            }
        }
    }

    fn publish_change(&mut self) {
        self.revision = self.revision.saturating_add(1);
        let _ = self.sender.send(self.revision);
    }
}

type ErasedActorRef = Arc<dyn Any + Send + Sync>;

#[derive(Clone)]
struct ServiceEntry {
    source: ServiceEntrySource,
    identity: ActorIdentity,
    serialized: SerializedActorRef,
    typed_ref: Option<ErasedActorRef>,
}

impl ServiceEntry {
    fn key(&self) -> ServiceEntryKey {
        match &self.source {
            ServiceEntrySource::Local => ServiceEntryKey::Local {
                identity: self.identity.clone(),
            },
            ServiceEntrySource::Remote { node_id } => ServiceEntryKey::Remote {
                node_id: node_id.clone(),
                identity: self.identity.clone(),
            },
        }
    }
}

#[derive(Clone)]
enum ServiceEntrySource {
    Local,
    Remote { node_id: String },
}

#[derive(Clone)]
enum ServiceEntryKey {
    Local {
        identity: ActorIdentity,
    },
    Remote {
        node_id: String,
        identity: ActorIdentity,
    },
}

struct RemoteListing {
    version: u64,
    observed_at_millis: u64,
    records: BTreeMap<ActorIdentity, RemoteServiceRecord>,
}

impl Debug for RemoteListing {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteListing")
            .field("version", &self.version)
            .field("observed_at_millis", &self.observed_at_millis)
            .field("records", &self.records)
            .finish()
    }
}

struct RemoteServiceRecord {
    serialized: SerializedActorRef,
    typed_ref: ErasedActorRef,
}

impl Debug for RemoteServiceRecord {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteServiceRecord")
            .field("serialized", &self.serialized)
            .finish_non_exhaustive()
    }
}

struct ServiceRecord {
    serialized: SerializedActorRef,
    typed_ref: Option<ErasedActorRef>,
    leases: usize,
}

impl Debug for ServiceRecord {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServiceRecord")
            .field("serialized", &self.serialized)
            .field("leases", &self.leases)
            .finish_non_exhaustive()
    }
}
