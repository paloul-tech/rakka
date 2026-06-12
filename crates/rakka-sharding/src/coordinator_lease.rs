//! Coordinator leadership lease abstractions.

use std::collections::BTreeMap;
use std::fmt::{self, Debug, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rakka_cluster::NodeId;

use crate::error::{ShardingError, ShardingResult};
use crate::identity::EntityType;

/// Boxed future returned by asynchronous coordinator lease operations.
pub type CoordinatorLeaseFuture<'a, T> =
    Pin<Box<dyn Future<Output = ShardingResult<T>> + Send + 'a>>;

/// Durable authority token for one shard coordinator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseToken {
    namespace: String,
    entity_type: EntityType,
    holder_node: NodeId,
    fencing_token: u64,
    expires_at_millis: u64,
}

impl LeaseToken {
    /// Creates a lease token.
    #[must_use]
    pub fn new(
        namespace: impl Into<String>,
        entity_type: EntityType,
        holder_node: NodeId,
        fencing_token: u64,
        expires_at_millis: u64,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            entity_type,
            holder_node,
            fencing_token,
            expires_at_millis,
        }
    }

    /// Namespace isolating this lease.
    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Entity type whose coordinator authority is leased.
    #[must_use]
    pub const fn entity_type(&self) -> &EntityType {
        &self.entity_type
    }

    /// Cluster node holding this lease.
    #[must_use]
    pub const fn holder_node(&self) -> &NodeId {
        &self.holder_node
    }

    /// Monotonically increasing fencing token assigned by the lease backend.
    #[must_use]
    pub const fn fencing_token(&self) -> u64 {
        self.fencing_token
    }

    /// Wall-clock expiry timestamp in milliseconds since the Unix epoch.
    #[must_use]
    pub const fn expires_at_millis(&self) -> u64 {
        self.expires_at_millis
    }

    /// Returns true when this token has expired by `now_millis`.
    #[must_use]
    pub const fn is_expired_at(&self, now_millis: u64) -> bool {
        self.expires_at_millis <= now_millis
    }
}

/// Asynchronous coordinator leadership lease backend.
pub trait ShardCoordinatorLease: Debug + Send + Sync + 'static {
    /// Stable lease backend name used for diagnostics.
    fn lease_name(&self) -> &'static str;

    /// Acquires or refreshes coordinator authority for an entity type.
    fn acquire<'a>(
        &'a self,
        entity_type: &'a EntityType,
        holder: &'a NodeId,
    ) -> CoordinatorLeaseFuture<'a, LeaseToken>;

    /// Renews an existing lease token.
    fn renew<'a>(&'a self, token: &'a LeaseToken) -> CoordinatorLeaseFuture<'a, ()>;

    /// Releases an existing lease token.
    fn release<'a>(&'a self, token: LeaseToken) -> CoordinatorLeaseFuture<'a, ()>;
}

/// In-memory coordinator lease backend for tests and single-process deployments.
#[derive(Clone)]
pub struct InMemoryShardCoordinatorLease {
    namespace: Arc<str>,
    lease_duration: Duration,
    records: Arc<Mutex<BTreeMap<LeaseKey, LeaseRecord>>>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct LeaseKey {
    namespace: String,
    entity_type: EntityType,
}

impl LeaseKey {
    fn new(namespace: impl Into<String>, entity_type: EntityType) -> Self {
        Self {
            namespace: namespace.into(),
            entity_type,
        }
    }
}

#[derive(Debug, Clone)]
struct LeaseRecord {
    holder_node: NodeId,
    fencing_token: u64,
    expires_at_millis: u64,
}

impl Default for InMemoryShardCoordinatorLease {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryShardCoordinatorLease {
    /// Default namespace used by in-memory leases.
    pub const DEFAULT_NAMESPACE: &'static str = "default";

    /// Default in-memory lease duration.
    pub const DEFAULT_LEASE_DURATION: Duration = Duration::from_secs(15);

    /// Creates an in-memory coordinator lease backend.
    #[must_use]
    pub fn new() -> Self {
        Self::with_namespace(Self::DEFAULT_NAMESPACE)
    }

    /// Creates an in-memory coordinator lease backend in an explicit namespace.
    #[must_use]
    pub fn with_namespace(namespace: impl Into<String>) -> Self {
        Self {
            namespace: Arc::from(namespace.into()),
            lease_duration: Self::DEFAULT_LEASE_DURATION,
            records: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// Sets the lease duration used by new and renewed tokens.
    #[must_use]
    pub fn with_lease_duration(mut self, lease_duration: Duration) -> Self {
        self.lease_duration = lease_duration;
        self
    }

    /// Namespace used to isolate leases.
    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Returns the stored token for an entity type, if present.
    #[must_use]
    pub fn token(&self, entity_type: &EntityType) -> Option<LeaseToken> {
        let records = self
            .records
            .lock()
            .expect("in-memory shard coordinator lease mutex poisoned");
        records
            .get(&LeaseKey::new(
                self.namespace().to_string(),
                entity_type.clone(),
            ))
            .map(|record| self.token_from_record(entity_type, record))
    }

    fn token_from_record(&self, entity_type: &EntityType, record: &LeaseRecord) -> LeaseToken {
        LeaseToken::new(
            self.namespace().to_string(),
            entity_type.clone(),
            record.holder_node.clone(),
            record.fencing_token,
            record.expires_at_millis,
        )
    }

    fn next_expiry(&self, now_millis: u64) -> ShardingResult<u64> {
        let lease_millis = u64::try_from(self.lease_duration.as_millis()).map_err(|_overflow| {
            ShardingError::CoordinatorLease {
                lease: "in-memory".to_string(),
                message: "lease duration exceeds u64 milliseconds".to_string(),
            }
        })?;
        Ok(now_millis.saturating_add(lease_millis))
    }
}

impl Debug for InMemoryShardCoordinatorLease {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("InMemoryShardCoordinatorLease")
            .field("namespace", &self.namespace())
            .field("lease_duration", &self.lease_duration)
            .finish_non_exhaustive()
    }
}

impl ShardCoordinatorLease for InMemoryShardCoordinatorLease {
    fn lease_name(&self) -> &'static str {
        "in-memory"
    }

    fn acquire<'a>(
        &'a self,
        entity_type: &'a EntityType,
        holder: &'a NodeId,
    ) -> CoordinatorLeaseFuture<'a, LeaseToken> {
        Box::pin(async move {
            let now_millis = current_timestamp_millis();
            let expires_at_millis = self.next_expiry(now_millis)?;
            let key = LeaseKey::new(self.namespace().to_string(), entity_type.clone());
            let mut records = self
                .records
                .lock()
                .expect("in-memory shard coordinator lease mutex poisoned");

            match records.get_mut(&key) {
                Some(record)
                    if record.holder_node == *holder || record.expires_at_millis <= now_millis =>
                {
                    if record.holder_node != *holder {
                        record.fencing_token = record.fencing_token.saturating_add(1);
                    }
                    record.holder_node = holder.clone();
                    record.expires_at_millis = expires_at_millis;
                    Ok(self.token_from_record(entity_type, record))
                }
                Some(record) => Err(ShardingError::CoordinatorLeaseRejected {
                    lease: self.lease_name().to_string(),
                    entity_type: Box::new(entity_type.clone()),
                    holder_node: Box::new(holder.clone()),
                    current_holder_node: Some(Box::new(record.holder_node.clone())),
                    expires_at_millis: Some(record.expires_at_millis),
                }),
                None => {
                    let record = LeaseRecord {
                        holder_node: holder.clone(),
                        fencing_token: 1,
                        expires_at_millis,
                    };
                    let token = self.token_from_record(entity_type, &record);
                    records.insert(key, record);
                    Ok(token)
                }
            }
        })
    }

    fn renew<'a>(&'a self, token: &'a LeaseToken) -> CoordinatorLeaseFuture<'a, ()> {
        Box::pin(async move {
            let now_millis = current_timestamp_millis();
            let next_expiry = self.next_expiry(now_millis)?;
            let key = LeaseKey::new(token.namespace().to_string(), token.entity_type().clone());
            let mut records = self
                .records
                .lock()
                .expect("in-memory shard coordinator lease mutex poisoned");
            let Some(record) = records.get_mut(&key) else {
                return Err(lease_lost(self.lease_name(), token, None, None));
            };

            if record.holder_node != *token.holder_node()
                || record.fencing_token != token.fencing_token()
                || record.expires_at_millis <= now_millis
            {
                return Err(lease_lost(
                    self.lease_name(),
                    token,
                    Some(record.holder_node.clone()),
                    Some(record.fencing_token),
                ));
            }

            record.expires_at_millis = next_expiry;
            Ok(())
        })
    }

    fn release<'a>(&'a self, token: LeaseToken) -> CoordinatorLeaseFuture<'a, ()> {
        Box::pin(async move {
            let key = LeaseKey::new(token.namespace().to_string(), token.entity_type().clone());
            let mut records = self
                .records
                .lock()
                .expect("in-memory shard coordinator lease mutex poisoned");
            match records.get(&key) {
                Some(record)
                    if record.holder_node == *token.holder_node()
                        && record.fencing_token == token.fencing_token() =>
                {
                    records.remove(&key);
                    Ok(())
                }
                Some(record) => Err(lease_lost(
                    self.lease_name(),
                    &token,
                    Some(record.holder_node.clone()),
                    Some(record.fencing_token),
                )),
                None => Ok(()),
            }
        })
    }
}

pub(crate) fn lease_lost(
    lease: &str,
    token: &LeaseToken,
    actual_holder_node: Option<NodeId>,
    actual_fencing_token: Option<u64>,
) -> ShardingError {
    ShardingError::CoordinatorLeaseLost {
        lease: lease.to_string(),
        entity_type: Box::new(token.entity_type().clone()),
        holder_node: Box::new(token.holder_node().clone()),
        fencing_token: token.fencing_token(),
        actual_holder_node: actual_holder_node.map(Box::new),
        actual_fencing_token,
    }
}

fn current_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}
