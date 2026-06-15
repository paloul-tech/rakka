//! Clustered receptionist propagation facade.

use std::fmt::{self, Formatter};
use std::time::Duration;

use rakka_core::{ActorRef, ActorSystem, Message, Receptionist, ServiceKey};

use crate::{Cluster, ClusterError, ClusterResult, MembershipState, NodeId};

/// Settings for clustered receptionist propagation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusteredReceptionistSettings {
    enabled: bool,
    publish_interval: Duration,
    remote_listing_ttl: Duration,
    max_routees_per_listing: Option<usize>,
}

impl ClusteredReceptionistSettings {
    /// Creates enabled settings with conservative defaults.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns true when clustered propagation is enabled.
    #[must_use]
    pub const fn enabled(&self) -> bool {
        self.enabled
    }

    /// Interval metadata for runtime-driven publication loops.
    #[must_use]
    pub const fn publish_interval(&self) -> Duration {
        self.publish_interval
    }

    /// Time-to-live for remote listings.
    #[must_use]
    pub const fn remote_listing_ttl(&self) -> Duration {
        self.remote_listing_ttl
    }

    /// Optional maximum routees accepted in one propagated listing.
    #[must_use]
    pub const fn max_routees_per_listing(&self) -> Option<usize> {
        self.max_routees_per_listing
    }

    /// Enables or disables clustered receptionist propagation.
    #[must_use]
    pub const fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Sets publication interval metadata.
    #[must_use]
    pub const fn with_publish_interval(mut self, interval: Duration) -> Self {
        self.publish_interval = interval;
        self
    }

    /// Sets remote listing TTL.
    #[must_use]
    pub const fn with_remote_listing_ttl(mut self, ttl: Duration) -> Self {
        self.remote_listing_ttl = ttl;
        self
    }

    /// Sets the maximum routees accepted in one propagated listing.
    #[must_use]
    pub const fn with_max_routees_per_listing(mut self, max: usize) -> Self {
        self.max_routees_per_listing = Some(max);
        self
    }

    /// Removes the propagated listing size limit.
    #[must_use]
    pub const fn without_max_routees_per_listing(mut self) -> Self {
        self.max_routees_per_listing = None;
        self
    }
}

impl Default for ClusteredReceptionistSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            publish_interval: Duration::from_secs(3),
            remote_listing_ttl: Duration::from_secs(30),
            max_routees_per_listing: None,
        }
    }
}

/// Versioned receptionist listing propagated from one cluster node.
pub struct ClusteredReceptionistListing<M>
where
    M: Message,
{
    source_node: NodeId,
    key: ServiceKey<M>,
    routees: Vec<ActorRef<M>>,
    version: u64,
    observed_at_millis: u64,
}

impl<M> Clone for ClusteredReceptionistListing<M>
where
    M: Message,
{
    fn clone(&self) -> Self {
        Self {
            source_node: self.source_node.clone(),
            key: self.key.clone(),
            routees: self.routees.clone(),
            version: self.version,
            observed_at_millis: self.observed_at_millis,
        }
    }
}

impl<M> ClusteredReceptionistListing<M>
where
    M: Message,
{
    /// Creates a propagated receptionist listing.
    #[must_use]
    pub fn new(
        source_node: NodeId,
        key: ServiceKey<M>,
        routees: Vec<ActorRef<M>>,
        version: u64,
        observed_at_millis: u64,
    ) -> Self {
        Self {
            source_node,
            key,
            routees,
            version,
            observed_at_millis,
        }
    }

    /// Source cluster node.
    #[must_use]
    pub const fn source_node(&self) -> &NodeId {
        &self.source_node
    }

    /// Service key this listing describes.
    #[must_use]
    pub const fn key(&self) -> &ServiceKey<M> {
        &self.key
    }

    /// Routees registered on the source node.
    #[must_use]
    pub fn routees(&self) -> &[ActorRef<M>] {
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
}

impl<M> fmt::Debug for ClusteredReceptionistListing<M>
where
    M: Message,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClusteredReceptionistListing")
            .field("source_node", &self.source_node)
            .field("key", &self.key)
            .field("routee_count", &self.routees.len())
            .field("version", &self.version)
            .field("observed_at_millis", &self.observed_at_millis)
            .finish()
    }
}

/// Facade that publishes and applies clustered receptionist listings.
#[derive(Clone)]
pub struct ClusteredReceptionist {
    cluster: Cluster,
    receptionist: Receptionist,
    settings: ClusteredReceptionistSettings,
}

impl ClusteredReceptionist {
    /// Creates a clustered receptionist facade for an actor system and cluster.
    #[must_use]
    pub fn get(system: &ActorSystem, cluster: Cluster) -> Self {
        Self::new(
            cluster,
            Receptionist::get(system),
            ClusteredReceptionistSettings::default(),
        )
    }

    /// Creates a clustered receptionist facade from explicit parts.
    #[must_use]
    pub const fn new(
        cluster: Cluster,
        receptionist: Receptionist,
        settings: ClusteredReceptionistSettings,
    ) -> Self {
        Self {
            cluster,
            receptionist,
            settings,
        }
    }

    /// Cluster facade used for local identity and reachability filtering.
    #[must_use]
    pub const fn cluster(&self) -> &Cluster {
        &self.cluster
    }

    /// Underlying local receptionist.
    #[must_use]
    pub const fn receptionist(&self) -> &Receptionist {
        &self.receptionist
    }

    /// Propagation settings.
    #[must_use]
    pub const fn settings(&self) -> &ClusteredReceptionistSettings {
        &self.settings
    }

    /// Returns a clone with different propagation settings.
    #[must_use]
    pub fn with_settings(mut self, settings: ClusteredReceptionistSettings) -> Self {
        self.settings = settings;
        self
    }

    /// Captures the local-only listing for `key` as a versioned publication.
    pub fn publish_local<M>(
        &self,
        key: &ServiceKey<M>,
        observed_at_millis: u64,
    ) -> ClusterResult<Option<ClusteredReceptionistListing<M>>>
    where
        M: Message,
    {
        if !self.settings.enabled {
            return Ok(None);
        }

        let listing = self
            .receptionist
            .find_local(key)
            .map_err(ClusterError::from_receptionist)?;
        self.check_routee_limit(key.id(), listing.len())?;

        Ok(Some(ClusteredReceptionistListing::new(
            self.cluster.local_node_id(),
            key.clone(),
            listing.service_instances().to_vec(),
            listing.revision(),
            observed_at_millis,
        )))
    }

    /// Applies a listing received from another cluster node.
    pub fn apply_remote<M>(&self, listing: ClusteredReceptionistListing<M>) -> ClusterResult<bool>
    where
        M: Message,
    {
        if !self.settings.enabled {
            return Ok(false);
        }

        if listing.source_node() == &self.cluster.local_node_id() {
            return Ok(false);
        }

        if !self.source_node_is_up(listing.source_node()) {
            self.receptionist
                .remove_remote_node(&listing.source_node().to_string());
            return Ok(false);
        }

        self.check_routee_limit(listing.key().id(), listing.routees().len())?;
        self.receptionist
            .install_remote_listing(
                listing.source_node().to_string(),
                listing.key(),
                listing.routees().to_vec(),
                listing.version(),
                listing.observed_at_millis(),
            )
            .map_err(ClusterError::from_receptionist)
    }

    /// Publishes a local listing directly to another clustered receptionist.
    pub fn propagate_to<M>(
        &self,
        destination: &ClusteredReceptionist,
        key: &ServiceKey<M>,
        observed_at_millis: u64,
    ) -> ClusterResult<bool>
    where
        M: Message,
    {
        let Some(listing) = self.publish_local(key, observed_at_millis)? else {
            return Ok(false);
        };
        destination.apply_remote(listing)
    }

    /// Removes propagated listings whose source member is no longer `Up`.
    ///
    /// Returns the number of source nodes removed from this local receptionist.
    pub fn prune_unreachable_members(&self) -> usize {
        let state = self.cluster.state();
        state
            .members()
            .iter()
            .filter(|member| member.node().id() != state.local_node_id())
            .filter(|member| member.state() != MembershipState::Up)
            .filter(|member| {
                self.receptionist
                    .remove_remote_node(&member.node().id().to_string())
            })
            .count()
    }

    /// Expires propagated listings older than the configured TTL.
    ///
    /// Returns the number of remote service listings removed.
    pub fn expire_stale_listings(&self, now_millis: u64) -> usize {
        let ttl_millis = duration_millis(self.settings.remote_listing_ttl);
        let older_than_millis = now_millis.saturating_sub(ttl_millis);
        self.receptionist.expire_remote_listings(older_than_millis)
    }

    fn source_node_is_up(&self, source_node: &NodeId) -> bool {
        self.cluster
            .state()
            .member(source_node)
            .is_some_and(|member| member.state() == MembershipState::Up)
    }

    fn check_routee_limit(&self, service_id: &str, actual: usize) -> ClusterResult<()> {
        let Some(max) = self.settings.max_routees_per_listing else {
            return Ok(());
        };
        if actual <= max {
            Ok(())
        } else {
            Err(ClusterError::ReceptionistListingTooLarge {
                service_id: service_id.to_string(),
                actual,
                max,
            })
        }
    }
}

impl fmt::Debug for ClusteredReceptionist {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClusteredReceptionist")
            .field("cluster", &self.cluster)
            .field("receptionist", &self.receptionist)
            .field("settings", &self.settings)
            .finish()
    }
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}
