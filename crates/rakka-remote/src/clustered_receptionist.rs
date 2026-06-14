//! Explicit remote clustered receptionist runtime helper.

use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::sync::Arc;
use std::time::Duration;

use rakka_cluster::{Cluster, ClusteredReceptionistSettings, MembershipState, NodeId};
use rakka_core::{ActorSystem, Message, RakkaError, Receptionist, ServiceKey, Subsystem};

use crate::{
    RemoteActorRefInbound, RemoteEndpoint, RemoteEndpointResult, RemoteError,
    RemoteReceptionistListing, RemoteServiceProxyError, RemoteServiceProxyRegistry,
    RemoteServiceProxyRegistrySnapshot, RemoteTransport, SerializationRegistry,
};

/// Failure returned by the explicit remote clustered receptionist helper.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteClusteredReceptionistError {
    /// Local receptionist lookup or install failed.
    Receptionist {
        /// Receptionist service id.
        service_id: String,
        /// Failure detail.
        message: String,
    },
    /// Wire listing conversion or validation failed.
    Remote {
        /// Remote failure detail.
        error: RemoteError,
    },
    /// Proxy materialization failed.
    Proxy {
        /// Proxy materialization failure.
        error: Box<RemoteServiceProxyError>,
    },
    /// A listing exceeded the configured routee limit.
    ListingTooLarge {
        /// Receptionist service id.
        service_id: String,
        /// Actual routee count.
        actual: usize,
        /// Configured maximum routee count.
        max: usize,
    },
}

impl RemoteClusteredReceptionistError {
    /// Stable machine-readable error code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Receptionist { .. } => "receptionist-error",
            Self::Remote { .. } => "remote-error",
            Self::Proxy { .. } => "remote-service-proxy-error",
            Self::ListingTooLarge { .. } => "remote-receptionist-listing-too-large",
        }
    }

    /// Converts this error to a framework error.
    #[must_use]
    pub fn into_rakka_error(self) -> RakkaError {
        RakkaError::new(Subsystem::Remote, self.code(), self.to_string())
    }
}

impl Display for RemoteClusteredReceptionistError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Receptionist {
                service_id,
                message,
            } => write!(
                f,
                "remote clustered receptionist operation for '{service_id}' failed: {message}"
            ),
            Self::Remote { error } => {
                write!(f, "remote clustered receptionist wire listing failed: {error}")
            }
            Self::Proxy { error } => {
                write!(f, "remote clustered receptionist proxy operation failed: {error}")
            }
            Self::ListingTooLarge {
                service_id,
                actual,
                max,
            } => write!(
                f,
                "remote clustered receptionist listing for '{service_id}' has {actual} routees, above configured maximum {max}"
            ),
        }
    }
}

impl Error for RemoteClusteredReceptionistError {}

/// Convenient result alias for remote clustered receptionist helper operations.
pub type RemoteClusteredReceptionistResult<T> = Result<T, RemoteClusteredReceptionistError>;

/// Explicit runtime helper that wires remote service listings to local proxies.
#[derive(Clone)]
pub struct RemoteClusteredReceptionist {
    system: ActorSystem,
    cluster: Cluster,
    endpoint: RemoteEndpoint,
    receptionist: Receptionist,
    proxy_registry: RemoteServiceProxyRegistry,
    transport: Arc<dyn RemoteTransport>,
    serialization: SerializationRegistry,
    settings: ClusteredReceptionistSettings,
}

impl RemoteClusteredReceptionist {
    /// Creates a remote clustered receptionist helper from explicit parts.
    #[must_use]
    pub fn new(
        system: ActorSystem,
        cluster: Cluster,
        endpoint: RemoteEndpoint,
        transport: Arc<dyn RemoteTransport>,
        serialization: SerializationRegistry,
        settings: ClusteredReceptionistSettings,
    ) -> Self {
        let receptionist = Receptionist::get(&system);
        let proxy_registry = RemoteServiceProxyRegistry::with_receptionist(
            cluster.local_node_id(),
            system.clone(),
            receptionist.clone(),
            transport.clone(),
            serialization.clone(),
        );
        Self {
            system,
            cluster,
            endpoint,
            receptionist,
            proxy_registry,
            transport,
            serialization,
            settings,
        }
    }

    /// Creates a helper from an owned transport implementation.
    #[must_use]
    pub fn with_transport<T>(
        system: ActorSystem,
        cluster: Cluster,
        endpoint: RemoteEndpoint,
        transport: T,
        serialization: SerializationRegistry,
        settings: ClusteredReceptionistSettings,
    ) -> Self
    where
        T: RemoteTransport,
    {
        Self::new(
            system,
            cluster,
            endpoint,
            Arc::new(transport),
            serialization,
            settings,
        )
    }

    /// Actor system used for local actor-ref resolution and proxy spawning.
    #[must_use]
    pub const fn system(&self) -> &ActorSystem {
        &self.system
    }

    /// Cluster facade used for local identity and reachability filtering.
    #[must_use]
    pub const fn cluster(&self) -> &Cluster {
        &self.cluster
    }

    /// Remote endpoint served by this helper.
    #[must_use]
    pub const fn endpoint(&self) -> &RemoteEndpoint {
        &self.endpoint
    }

    /// Local receptionist used by this helper.
    #[must_use]
    pub const fn receptionist(&self) -> &Receptionist {
        &self.receptionist
    }

    /// Proxy registry used for remote service routees.
    #[must_use]
    pub const fn proxy_registry(&self) -> &RemoteServiceProxyRegistry {
        &self.proxy_registry
    }

    /// Transport used by materialized service proxies.
    #[must_use]
    pub fn transport(&self) -> &Arc<dyn RemoteTransport> {
        &self.transport
    }

    /// Serialization registry used for inbound actor refs and outbound proxies.
    #[must_use]
    pub const fn serialization(&self) -> &SerializationRegistry {
        &self.serialization
    }

    /// Clustered receptionist settings used by this helper.
    #[must_use]
    pub const fn settings(&self) -> &ClusteredReceptionistSettings {
        &self.settings
    }

    /// Returns a clone with updated settings.
    #[must_use]
    pub fn with_settings(mut self, settings: ClusteredReceptionistSettings) -> Self {
        self.settings = settings;
        self
    }

    /// Registers inbound delivery for remote actor-ref envelopes of type `M`.
    pub fn register_actor_ref_handler<M>(&self) -> RemoteEndpointResult<()>
    where
        M: Message + Sync,
    {
        self.endpoint
            .register_actor_ref_handler::<M>(RemoteActorRefInbound::<M>::new(
                self.cluster.local_node_id(),
                self.system.clone(),
                self.serialization.clone(),
            ))
    }

    /// Captures a local-only typed receptionist listing as a remote wire listing.
    pub fn publish_once<M>(
        &self,
        key: &ServiceKey<M>,
        observed_at_millis: u64,
    ) -> RemoteClusteredReceptionistResult<Option<RemoteReceptionistListing>>
    where
        M: Message,
    {
        if !self.settings.enabled() {
            return Ok(None);
        }

        let listing = self.receptionist.find_local(key).map_err(|error| {
            RemoteClusteredReceptionistError::Receptionist {
                service_id: key.id().to_string(),
                message: error.to_string(),
            }
        })?;
        self.check_routee_limit(key.id(), listing.len())?;

        RemoteReceptionistListing::from_listing_with_max_routees(
            self.cluster.local_node_id(),
            &self.system.actor_ref_resolver(),
            &listing,
            observed_at_millis,
            self.settings.max_routees_per_listing(),
        )
        .map(Some)
        .map_err(|error| RemoteClusteredReceptionistError::Remote { error })
    }

    /// Applies a remote wire listing by materializing local proxy routees.
    pub fn apply_wire_listing<M>(
        &self,
        listing: RemoteReceptionistListing,
    ) -> RemoteClusteredReceptionistResult<bool>
    where
        M: Message + Sync,
    {
        if !self.settings.enabled() {
            return Ok(false);
        }

        listing
            .validate()
            .map_err(|error| RemoteClusteredReceptionistError::Remote { error })?;
        if listing.source_node() == &self.cluster.local_node_id() {
            return Ok(false);
        }

        if !self.source_node_is_up(listing.source_node()) {
            self.proxy_registry
                .remove_remote_node(listing.source_node());
            return Ok(false);
        }

        self.check_routee_limit(listing.service_id(), listing.len())?;
        self.proxy_registry
            .apply_listing::<M>(listing)
            .map_err(|error| RemoteClusteredReceptionistError::Proxy {
                error: Box::new(error),
            })
    }

    /// Removes proxy listings whose source member is no longer `Up`.
    ///
    /// Returns the number of source members that caused local state to change.
    pub fn prune_unreachable_members(&self) -> usize {
        let state = self.cluster.state();
        state
            .members()
            .iter()
            .filter(|member| member.node().id() != state.local_node_id())
            .filter(|member| member.state() != MembershipState::Up)
            .filter(|member| {
                let before = self.proxy_registry.snapshot();
                self.proxy_registry.remove_remote_node(member.node().id());
                self.proxy_registry.snapshot() != before
            })
            .count()
    }

    /// Expires remote proxy listings older than the configured TTL.
    ///
    /// Returns the number of expired source-node service listings.
    pub fn expire_stale_listings(&self, now_millis: u64) -> usize {
        let ttl_millis = duration_millis(self.settings.remote_listing_ttl());
        let older_than_millis = now_millis.saturating_sub(ttl_millis);
        self.proxy_registry.expire_stale_listings(older_than_millis)
    }

    /// Returns an observable proxy registry snapshot.
    #[must_use]
    pub fn proxy_snapshot(&self) -> RemoteServiceProxyRegistrySnapshot {
        self.proxy_registry.snapshot()
    }

    fn source_node_is_up(&self, source_node: &NodeId) -> bool {
        self.cluster
            .state()
            .member(source_node)
            .is_some_and(|member| member.state() == MembershipState::Up)
    }

    fn check_routee_limit(
        &self,
        service_id: &str,
        actual: usize,
    ) -> RemoteClusteredReceptionistResult<()> {
        let Some(max) = self.settings.max_routees_per_listing() else {
            return Ok(());
        };
        if actual <= max {
            Ok(())
        } else {
            Err(RemoteClusteredReceptionistError::ListingTooLarge {
                service_id: service_id.to_string(),
                actual,
                max,
            })
        }
    }
}

impl Debug for RemoteClusteredReceptionist {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteClusteredReceptionist")
            .field("system", &self.system.name())
            .field("cluster", &self.cluster)
            .field("endpoint", &self.endpoint)
            .field("settings", &self.settings)
            .field("proxy_snapshot", &self.proxy_snapshot())
            .finish_non_exhaustive()
    }
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}
