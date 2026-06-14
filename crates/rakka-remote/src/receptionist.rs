//! Remote clustered receptionist wire-listing descriptors and proxy materialization.

use std::any::type_name;
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::marker::PhantomData;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use rakka_cluster::NodeId;
use rakka_core::{
    actor_future, Actor, ActorAction, ActorContext, ActorFuture, ActorOptions, ActorRef,
    ActorRefResolver, Listing, Message, RakkaError, Receptionist, ServiceKey, Subsystem,
    SupervisionStrategy,
};
use serde::{Deserialize, Serialize};

use crate::{
    RemoteActorRef, RemoteDestination, RemoteEnvelope, RemoteError, RemoteResult, RemoteTransport,
    SerializationRegistry,
};

/// Transport-facing descriptor for one service routee in a remote receptionist listing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteServiceRoutee {
    actor_ref: RemoteActorRef,
}

impl RemoteServiceRoutee {
    /// Creates a remote service routee from a concrete remote actor reference.
    #[must_use]
    pub const fn new(actor_ref: RemoteActorRef) -> Self {
        Self { actor_ref }
    }

    /// Creates a routee descriptor from a local actor ref.
    pub fn from_actor_ref<M>(
        source_node: NodeId,
        resolver: &ActorRefResolver,
        actor_ref: &ActorRef<M>,
    ) -> RemoteResult<Self>
    where
        M: Message,
    {
        let serialized = resolver.to_serialized_ref(actor_ref);
        RemoteActorRef::from_serialized(source_node, &serialized).map(Self::new)
    }

    /// Concrete actor incarnation descriptor for this service routee.
    #[must_use]
    pub const fn actor_ref(&self) -> &RemoteActorRef {
        &self.actor_ref
    }

    /// Rust message type associated with this service routee.
    #[must_use]
    pub fn message_type(&self) -> &str {
        self.actor_ref.message_type()
    }
}

/// Transport-facing receptionist listing for one service on one source node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteReceptionistListing {
    source_node: NodeId,
    service_id: String,
    service_message_type: String,
    routees: Vec<RemoteServiceRoutee>,
    version: u64,
    observed_at_millis: u64,
}

impl RemoteReceptionistListing {
    /// Creates a remote receptionist listing.
    pub fn new(
        source_node: NodeId,
        service_id: impl Into<String>,
        service_message_type: impl Into<String>,
        routees: Vec<RemoteServiceRoutee>,
        version: u64,
        observed_at_millis: u64,
    ) -> RemoteResult<Self> {
        Self::new_with_max_routees(
            source_node,
            service_id,
            service_message_type,
            routees,
            version,
            observed_at_millis,
            None,
        )
    }

    /// Creates a remote receptionist listing and enforces a routee-count limit.
    pub fn new_with_max_routees(
        source_node: NodeId,
        service_id: impl Into<String>,
        service_message_type: impl Into<String>,
        routees: Vec<RemoteServiceRoutee>,
        version: u64,
        observed_at_millis: u64,
        max_routees: Option<usize>,
    ) -> RemoteResult<Self> {
        let listing = Self {
            source_node,
            service_id: service_id.into(),
            service_message_type: service_message_type.into(),
            routees,
            version,
            observed_at_millis,
        };
        listing.validate_with_max_routees(max_routees)?;
        Ok(listing)
    }

    /// Converts a local typed receptionist listing into a remote wire listing.
    pub fn from_listing<M>(
        source_node: NodeId,
        resolver: &ActorRefResolver,
        listing: &Listing<M>,
        observed_at_millis: u64,
    ) -> RemoteResult<Self>
    where
        M: Message,
    {
        Self::from_listing_with_max_routees(
            source_node,
            resolver,
            listing,
            observed_at_millis,
            None,
        )
    }

    /// Converts a local typed listing and enforces a routee-count limit.
    pub fn from_listing_with_max_routees<M>(
        source_node: NodeId,
        resolver: &ActorRefResolver,
        listing: &Listing<M>,
        observed_at_millis: u64,
        max_routees: Option<usize>,
    ) -> RemoteResult<Self>
    where
        M: Message,
    {
        let routees = listing
            .routees()
            .iter()
            .map(|routee| {
                RemoteServiceRoutee::from_actor_ref(source_node.clone(), resolver, routee)
            })
            .collect::<RemoteResult<Vec<_>>>()?;

        Self::new_with_max_routees(
            source_node,
            listing.key().id(),
            listing.key().message_type(),
            routees,
            listing.revision(),
            observed_at_millis,
            max_routees,
        )
    }

    /// Source cluster node that published this listing.
    #[must_use]
    pub const fn source_node(&self) -> &NodeId {
        &self.source_node
    }

    /// Receptionist service id.
    #[must_use]
    pub fn service_id(&self) -> &str {
        &self.service_id
    }

    /// Rust message type associated with the service key.
    #[must_use]
    pub fn service_message_type(&self) -> &str {
        &self.service_message_type
    }

    /// Routees published by the source node.
    #[must_use]
    pub fn routees(&self) -> &[RemoteServiceRoutee] {
        &self.routees
    }

    /// Monotonic source receptionist revision.
    #[must_use]
    pub const fn version(&self) -> u64 {
        self.version
    }

    /// Source observation time in milliseconds.
    #[must_use]
    pub const fn observed_at_millis(&self) -> u64 {
        self.observed_at_millis
    }

    /// Number of routees in this listing.
    #[must_use]
    pub fn len(&self) -> usize {
        self.routees.len()
    }

    /// Returns true when the listing contains no routees.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.routees.is_empty()
    }

    /// Validates a remote receptionist listing after deserialization.
    pub fn validate(&self) -> RemoteResult<()> {
        self.validate_with_max_routees(None)
    }

    /// Validates a remote receptionist listing and enforces a routee-count limit.
    pub fn validate_with_max_routees(&self, max_routees: Option<usize>) -> RemoteResult<()> {
        validate_node_id(&self.source_node)?;
        require_non_empty("service_id", self.service_id.clone())?;
        require_non_empty("service_message_type", self.service_message_type.clone())?;
        if let Some(max) = max_routees {
            if self.routees.len() > max {
                return Err(RemoteError::InvalidEnvelope {
                    message: format!(
                        "remote receptionist listing for service {:?} has {} routees, max {max}",
                        self.service_id,
                        self.routees.len()
                    ),
                });
            }
        }
        for routee in &self.routees {
            if routee.actor_ref().node_id() != &self.source_node {
                return Err(RemoteError::InvalidEnvelope {
                    message: format!(
                        "routee node {} does not match listing source node {}",
                        routee.actor_ref().node_id(),
                        self.source_node
                    ),
                });
            }
            if routee.message_type() != self.service_message_type {
                return Err(RemoteError::InvalidEnvelope {
                    message: format!(
                        "routee message type {} does not match service message type {}",
                        routee.message_type(),
                        self.service_message_type
                    ),
                });
            }
        }

        Ok(())
    }
}

/// Stable key for one materialized remote service proxy.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RemoteServiceRouteeKey {
    source_node: NodeId,
    service_id: String,
    actor_path: String,
    actor_uid: u64,
    message_type: String,
}

impl RemoteServiceRouteeKey {
    /// Creates a routee key from explicit remote routee identity fields.
    #[must_use]
    pub fn new(
        source_node: NodeId,
        service_id: impl Into<String>,
        actor_path: impl Into<String>,
        actor_uid: u64,
        message_type: impl Into<String>,
    ) -> Self {
        Self {
            source_node,
            service_id: service_id.into(),
            actor_path: actor_path.into(),
            actor_uid,
            message_type: message_type.into(),
        }
    }

    /// Creates a routee key for a routee in a service listing.
    #[must_use]
    pub fn from_routee(service_id: impl Into<String>, routee: &RemoteServiceRoutee) -> Self {
        let actor_ref = routee.actor_ref();
        Self::new(
            actor_ref.node_id().clone(),
            service_id,
            actor_ref.path().to_string(),
            actor_ref.uid().value(),
            actor_ref.message_type(),
        )
    }

    /// Source cluster node that owns the real service actor.
    #[must_use]
    pub const fn source_node(&self) -> &NodeId {
        &self.source_node
    }

    /// Receptionist service id.
    #[must_use]
    pub fn service_id(&self) -> &str {
        &self.service_id
    }

    /// Remote actor path.
    #[must_use]
    pub fn actor_path(&self) -> &str {
        &self.actor_path
    }

    /// Remote actor incarnation uid.
    #[must_use]
    pub const fn actor_uid(&self) -> u64 {
        self.actor_uid
    }

    /// Rust message type accepted by the remote actor.
    #[must_use]
    pub fn message_type(&self) -> &str {
        &self.message_type
    }
}

impl Display for RemoteServiceRouteeKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:{}:{}#{}:{}",
            self.source_node, self.service_id, self.actor_path, self.actor_uid, self.message_type
        )
    }
}

/// Failure returned while materializing or applying remote receptionist proxies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteServiceProxyError {
    /// Listing validation failed.
    InvalidListing {
        /// Validation failure.
        error: RemoteError,
    },
    /// The requested Rust protocol did not match the listing service protocol.
    MessageTypeMismatch {
        /// Receptionist service id.
        service_id: String,
        /// Rust message type requested by the caller.
        expected: &'static str,
        /// Message type carried by the remote listing.
        actual: String,
    },
    /// A local proxy actor could not be spawned.
    SpawnProxy {
        /// Routee being materialized.
        routee: Box<RemoteServiceRouteeKey>,
        /// Spawn failure detail.
        message: String,
    },
    /// A materialized proxy could not be recovered as the requested type.
    ProxyTypeMismatch {
        /// Routee being materialized.
        routee: Box<RemoteServiceRouteeKey>,
        /// Rust message type requested by the caller.
        expected: &'static str,
    },
    /// Installing proxy refs into the local receptionist failed.
    Receptionist {
        /// Receptionist service id.
        service_id: String,
        /// Failure detail.
        message: String,
    },
}

impl Display for RemoteServiceProxyError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidListing { error } => {
                write!(f, "remote receptionist listing is invalid: {error}")
            }
            Self::MessageTypeMismatch {
                service_id,
                expected,
                actual,
            } => write!(
                f,
                "remote receptionist listing for '{service_id}' is for '{actual}' but was applied as '{expected}'"
            ),
            Self::SpawnProxy { routee, message } => {
                write!(f, "could not spawn remote service proxy for {routee}: {message}")
            }
            Self::ProxyTypeMismatch { routee, expected } => write!(
                f,
                "remote service proxy for {routee} could not be recovered as '{expected}'"
            ),
            Self::Receptionist {
                service_id,
                message,
            } => write!(
                f,
                "remote service proxy listing for '{service_id}' could not be installed: {message}"
            ),
        }
    }
}

impl Error for RemoteServiceProxyError {}

/// Convenient result alias for remote service proxy materialization.
pub type RemoteServiceProxyResult<T> = Result<T, RemoteServiceProxyError>;

/// Local proxy actor that forwards service messages to one remote actor ref.
pub struct RemoteServiceProxy<M>
where
    M: Message + Sync,
{
    target: RemoteActorRef,
    serialization: SerializationRegistry,
    transport: Arc<dyn RemoteTransport>,
    _message: PhantomData<fn() -> M>,
}

impl<M> RemoteServiceProxy<M>
where
    M: Message + Sync,
{
    /// Creates a proxy actor for one remote service routee.
    #[must_use]
    pub fn new(
        target: RemoteActorRef,
        serialization: SerializationRegistry,
        transport: Arc<dyn RemoteTransport>,
    ) -> Self {
        Self {
            target,
            serialization,
            transport,
            _message: PhantomData,
        }
    }

    /// Remote actor target this proxy forwards to.
    #[must_use]
    pub const fn target(&self) -> &RemoteActorRef {
        &self.target
    }

    /// Serialization registry used for outbound payloads.
    #[must_use]
    pub const fn serialization(&self) -> &SerializationRegistry {
        &self.serialization
    }

    fn forward(&self, message: &M) -> rakka_core::RakkaResult<()> {
        let encoded = self.serialization.encode(message).map_err(|error| {
            RakkaError::new(
                Subsystem::Remote,
                error.code(),
                format!("remote service proxy encode failed: {error}"),
            )
        })?;
        let envelope =
            RemoteEnvelope::new(RemoteDestination::actor_ref(self.target.clone()), encoded);
        self.transport
            .send(self.target.node_id(), envelope)
            .map_err(|error| {
                RakkaError::new(
                    Subsystem::Remote,
                    "remote-service-proxy-send",
                    format!("remote service proxy send failed: {error}"),
                )
            })
    }
}

impl<M> Actor for RemoteServiceProxy<M>
where
    M: Message + Sync,
{
    type Msg = M;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        let outcome = self.forward(&msg).map(|()| ActorAction::Continue);
        actor_future(async move { outcome })
    }
}

impl<M> Clone for RemoteServiceProxy<M>
where
    M: Message + Sync,
{
    fn clone(&self) -> Self {
        Self {
            target: self.target.clone(),
            serialization: self.serialization.clone(),
            transport: self.transport.clone(),
            _message: PhantomData,
        }
    }
}

impl<M> Debug for RemoteServiceProxy<M>
where
    M: Message + Sync,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteServiceProxy")
            .field("target", &self.target)
            .field("message_type", &type_name::<M>())
            .finish_non_exhaustive()
    }
}

/// Snapshot of a remote service proxy registry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteServiceProxyRegistrySnapshot {
    proxy_count: usize,
    listing_count: usize,
}

impl RemoteServiceProxyRegistrySnapshot {
    /// Creates a remote service proxy registry snapshot.
    #[must_use]
    pub const fn new(proxy_count: usize, listing_count: usize) -> Self {
        Self {
            proxy_count,
            listing_count,
        }
    }

    /// Number of live or tracked proxy actors.
    #[must_use]
    pub const fn proxy_count(&self) -> usize {
        self.proxy_count
    }

    /// Number of source-node service listings tracked by the registry.
    #[must_use]
    pub const fn listing_count(&self) -> usize {
        self.listing_count
    }
}

/// Registry that materializes remote receptionist routees as local proxy actors.
#[derive(Clone)]
pub struct RemoteServiceProxyRegistry {
    inner: Arc<RemoteServiceProxyRegistryInner>,
}

impl RemoteServiceProxyRegistry {
    /// Creates a proxy registry using the actor system's local receptionist.
    #[must_use]
    pub fn new(
        local_node_id: NodeId,
        system: rakka_core::ActorSystem,
        transport: Arc<dyn RemoteTransport>,
        serialization: SerializationRegistry,
    ) -> Self {
        let receptionist = Receptionist::get(&system);
        Self::with_receptionist(
            local_node_id,
            system,
            receptionist,
            transport,
            serialization,
        )
    }

    /// Creates a proxy registry from an owned transport implementation.
    #[must_use]
    pub fn with_transport<T>(
        local_node_id: NodeId,
        system: rakka_core::ActorSystem,
        transport: T,
        serialization: SerializationRegistry,
    ) -> Self
    where
        T: RemoteTransport,
    {
        Self::new(local_node_id, system, Arc::new(transport), serialization)
    }

    /// Creates a proxy registry from explicit parts.
    #[must_use]
    pub fn with_receptionist(
        local_node_id: NodeId,
        system: rakka_core::ActorSystem,
        receptionist: Receptionist,
        transport: Arc<dyn RemoteTransport>,
        serialization: SerializationRegistry,
    ) -> Self {
        Self {
            inner: Arc::new(RemoteServiceProxyRegistryInner {
                local_node_id,
                system,
                receptionist,
                transport,
                serialization,
                state: Mutex::new(RemoteServiceProxyState::default()),
            }),
        }
    }

    /// Local cluster node served by this proxy registry.
    #[must_use]
    pub fn local_node_id(&self) -> &NodeId {
        &self.inner.local_node_id
    }

    /// Applies one remote wire listing by materializing local service proxies.
    pub fn apply_listing<M>(
        &self,
        listing: RemoteReceptionistListing,
    ) -> RemoteServiceProxyResult<bool>
    where
        M: Message + Sync,
    {
        listing
            .validate()
            .map_err(|error| RemoteServiceProxyError::InvalidListing { error })?;
        self.check_message_type::<M>(&listing)?;
        if listing.source_node() == &self.inner.local_node_id {
            return Ok(false);
        }

        let listing_key = RemoteServiceListingKey::from_listing(&listing);
        let service_key = ServiceKey::<M>::new(listing.service_id().to_string());
        let service_id = listing.service_id().to_string();
        let source_node = listing.source_node().to_string();
        let version = listing.version();
        let observed_at_millis = listing.observed_at_millis();
        let routees = self.materialize_listing::<M>(&listing_key, &listing)?;

        self.inner
            .receptionist
            .install_remote_listing(
                source_node,
                &service_key,
                routees,
                version,
                observed_at_millis,
            )
            .map_err(|error| RemoteServiceProxyError::Receptionist {
                service_id,
                message: error.to_string(),
            })
    }

    /// Removes all proxies and receptionist entries for one remote source node.
    pub fn remove_remote_node(&self, node_id: &NodeId) -> usize {
        let proxy_keys = {
            let mut state = self
                .inner
                .state
                .lock()
                .expect("remote service proxy registry mutex poisoned");
            state
                .listings
                .retain(|listing_key, _listing| listing_key.source_node != *node_id);
            state
                .proxies
                .keys()
                .filter(|routee_key| routee_key.source_node() == node_id)
                .cloned()
                .collect::<Vec<_>>()
        };
        let removed = self.stop_proxies(proxy_keys);
        self.inner
            .receptionist
            .remove_remote_node(&node_id.to_string());
        removed
    }

    /// Removes stale listings and stops the proxies materialized from them.
    pub fn expire_stale_listings(&self, older_than_millis: u64) -> usize {
        let (expired_count, proxy_keys) = {
            let mut state = self
                .inner
                .state
                .lock()
                .expect("remote service proxy registry mutex poisoned");
            let expired_listing_keys = state
                .listings
                .iter()
                .filter(|(_key, listing)| listing.observed_at_millis < older_than_millis)
                .map(|(key, _listing)| key.clone())
                .collect::<Vec<_>>();
            let mut proxy_keys = BTreeSet::new();
            for listing_key in &expired_listing_keys {
                if let Some(listing) = state.listings.remove(listing_key) {
                    proxy_keys.extend(listing.routees);
                }
            }
            (expired_listing_keys.len(), proxy_keys.into_iter().collect())
        };

        self.stop_proxies(proxy_keys);
        self.inner
            .receptionist
            .expire_remote_listings(older_than_millis);
        expired_count
    }

    /// Number of currently tracked proxy actors.
    #[must_use]
    pub fn proxy_count(&self) -> usize {
        self.inner
            .state
            .lock()
            .expect("remote service proxy registry mutex poisoned")
            .proxies
            .len()
    }

    /// Number of source-node service listings currently tracked.
    #[must_use]
    pub fn listing_count(&self) -> usize {
        self.inner
            .state
            .lock()
            .expect("remote service proxy registry mutex poisoned")
            .listings
            .len()
    }

    /// Returns true when a proxy routee key is currently tracked.
    #[must_use]
    pub fn contains_proxy(&self, routee_key: &RemoteServiceRouteeKey) -> bool {
        self.inner
            .state
            .lock()
            .expect("remote service proxy registry mutex poisoned")
            .proxies
            .contains_key(routee_key)
    }

    /// Returns an observable registry snapshot.
    #[must_use]
    pub fn snapshot(&self) -> RemoteServiceProxyRegistrySnapshot {
        let state = self
            .inner
            .state
            .lock()
            .expect("remote service proxy registry mutex poisoned");
        RemoteServiceProxyRegistrySnapshot::new(state.proxies.len(), state.listings.len())
    }

    fn check_message_type<M>(
        &self,
        listing: &RemoteReceptionistListing,
    ) -> RemoteServiceProxyResult<()>
    where
        M: Message + Sync,
    {
        let expected = type_name::<M>();
        if listing.service_message_type() == expected {
            Ok(())
        } else {
            Err(RemoteServiceProxyError::MessageTypeMismatch {
                service_id: listing.service_id().to_string(),
                expected,
                actual: listing.service_message_type().to_string(),
            })
        }
    }

    fn materialize_listing<M>(
        &self,
        listing_key: &RemoteServiceListingKey,
        listing: &RemoteReceptionistListing,
    ) -> RemoteServiceProxyResult<Vec<ActorRef<M>>>
    where
        M: Message + Sync,
    {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("remote service proxy registry mutex poisoned");

        if let Some(existing) = state.listings.get(listing_key).cloned() {
            if listing.version() < existing.version {
                return existing_routees(&state, existing);
            }
            if listing.version() == existing.version {
                if let Some(existing) = state.listings.get_mut(listing_key) {
                    existing.observed_at_millis = existing
                        .observed_at_millis
                        .max(listing.observed_at_millis());
                }
                return existing_routees(&state, existing);
            }
        } else if listing.version() == 0 && listing.is_empty() {
            return Ok(Vec::new());
        }

        let new_routee_keys = listing
            .routees()
            .iter()
            .map(|routee| RemoteServiceRouteeKey::from_routee(listing.service_id(), routee))
            .collect::<BTreeSet<_>>();

        let old_routee_keys = state
            .listings
            .get(listing_key)
            .map(|existing| existing.routees.clone())
            .unwrap_or_default();
        let mut routees = Vec::with_capacity(listing.routees().len());

        for routee in listing.routees() {
            let routee_key = RemoteServiceRouteeKey::from_routee(listing.service_id(), routee);
            if let Some(actor_ref) = state
                .proxies
                .get(&routee_key)
                .and_then(ProxyEntry::live_actor_ref::<M>)
            {
                routees.push(actor_ref);
                continue;
            }

            if let Some(stale) = state.proxies.remove(&routee_key) {
                stale.stop();
            }
            let actor_ref = self.spawn_proxy::<M>(&routee_key, routee)?;
            state
                .proxies
                .insert(routee_key, ProxyEntry::new(actor_ref.clone()));
            routees.push(actor_ref);
        }

        for obsolete in old_routee_keys.difference(&new_routee_keys) {
            if let Some(proxy) = state.proxies.remove(obsolete) {
                proxy.stop();
            }
        }

        state.listings.insert(
            listing_key.clone(),
            RemoteProxyListingState {
                version: listing.version(),
                observed_at_millis: listing.observed_at_millis(),
                routees: new_routee_keys,
            },
        );

        Ok(routees)
    }

    fn spawn_proxy<M>(
        &self,
        routee_key: &RemoteServiceRouteeKey,
        routee: &RemoteServiceRoutee,
    ) -> RemoteServiceProxyResult<ActorRef<M>>
    where
        M: Message + Sync,
    {
        let target = routee.actor_ref().clone();
        let serialization = self.inner.serialization.clone();
        let transport = self.inner.transport.clone();
        self.inner
            .system
            .spawn_anonymous_with_options(
                move || {
                    RemoteServiceProxy::<M>::new(
                        target.clone(),
                        serialization.clone(),
                        transport.clone(),
                    )
                },
                ActorOptions::default().with_supervision(SupervisionStrategy::Resume),
            )
            .map_err(|error| RemoteServiceProxyError::SpawnProxy {
                routee: Box::new(routee_key.clone()),
                message: error.to_string(),
            })
    }

    fn stop_proxies(&self, proxy_keys: Vec<RemoteServiceRouteeKey>) -> usize {
        let mut removed = 0;
        let mut state = self
            .inner
            .state
            .lock()
            .expect("remote service proxy registry mutex poisoned");
        for proxy_key in proxy_keys {
            if let Some(proxy) = state.proxies.remove(&proxy_key) {
                proxy.stop();
                removed += 1;
            }
        }
        removed
    }
}

impl Debug for RemoteServiceProxyRegistry {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteServiceProxyRegistry")
            .field("local_node_id", &self.inner.local_node_id)
            .field("system", &self.inner.system.name())
            .field("snapshot", &self.snapshot())
            .finish_non_exhaustive()
    }
}

fn require_non_empty(field: &str, value: String) -> RemoteResult<String> {
    if value.is_empty() {
        Err(RemoteError::InvalidEnvelope {
            message: format!("missing {field}"),
        })
    } else {
        Ok(value)
    }
}

fn validate_node_id(node_id: &NodeId) -> RemoteResult<()> {
    NodeId::from_str(&node_id.to_string())
        .map(|_node_id| ())
        .map_err(|error| RemoteError::InvalidEnvelope {
            message: error.to_string(),
        })
}

struct RemoteServiceProxyRegistryInner {
    local_node_id: NodeId,
    system: rakka_core::ActorSystem,
    receptionist: Receptionist,
    transport: Arc<dyn RemoteTransport>,
    serialization: SerializationRegistry,
    state: Mutex<RemoteServiceProxyState>,
}

#[derive(Default)]
struct RemoteServiceProxyState {
    proxies: BTreeMap<RemoteServiceRouteeKey, ProxyEntry>,
    listings: BTreeMap<RemoteServiceListingKey, RemoteProxyListingState>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct RemoteServiceListingKey {
    source_node: NodeId,
    service_id: String,
    message_type: String,
}

impl RemoteServiceListingKey {
    fn from_listing(listing: &RemoteReceptionistListing) -> Self {
        Self {
            source_node: listing.source_node().clone(),
            service_id: listing.service_id().to_string(),
            message_type: listing.service_message_type().to_string(),
        }
    }
}

#[derive(Debug, Clone)]
struct RemoteProxyListingState {
    version: u64,
    observed_at_millis: u64,
    routees: BTreeSet<RemoteServiceRouteeKey>,
}

fn existing_routees<M>(
    state: &RemoteServiceProxyState,
    listing: RemoteProxyListingState,
) -> RemoteServiceProxyResult<Vec<ActorRef<M>>>
where
    M: Message + Sync,
{
    listing
        .routees
        .iter()
        .map(|routee_key| {
            state
                .proxies
                .get(routee_key)
                .and_then(ProxyEntry::live_actor_ref::<M>)
                .ok_or_else(|| RemoteServiceProxyError::ProxyTypeMismatch {
                    routee: Box::new(routee_key.clone()),
                    expected: type_name::<M>(),
                })
        })
        .collect()
}

struct ProxyEntry {
    proxy: Box<dyn ErasedProxyEntry>,
}

impl ProxyEntry {
    fn new<M>(actor_ref: ActorRef<M>) -> Self
    where
        M: Message + Sync,
    {
        Self {
            proxy: Box::new(TypedProxyEntry { actor_ref }),
        }
    }

    fn live_actor_ref<M>(&self) -> Option<ActorRef<M>>
    where
        M: Message + Sync,
    {
        if self.proxy.is_terminated() {
            return None;
        }
        self.proxy.as_any().downcast_ref::<ActorRef<M>>().cloned()
    }

    fn stop(&self) {
        self.proxy.stop();
    }
}

trait ErasedProxyEntry: Send + Sync {
    fn as_any(&self) -> &(dyn std::any::Any + Send + Sync);

    fn stop(&self);

    fn is_terminated(&self) -> bool;
}

struct TypedProxyEntry<M>
where
    M: Message + Sync,
{
    actor_ref: ActorRef<M>,
}

impl<M> ErasedProxyEntry for TypedProxyEntry<M>
where
    M: Message + Sync,
{
    fn as_any(&self) -> &(dyn std::any::Any + Send + Sync) {
        &self.actor_ref
    }

    fn stop(&self) {
        let _ = self.actor_ref.stop();
    }

    fn is_terminated(&self) -> bool {
        self.actor_ref.is_terminated()
    }
}
