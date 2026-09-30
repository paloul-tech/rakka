#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! PostgreSQL cluster sharding coordinator, lease, and remembered entity stores.

use std::error::Error;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use rakka_cluster::NodeId;
use rakka_core::Subsystem;
use rakka_sharding::{
    AsyncShardCoordinatorStore, CoordinatorLeaseFuture, CoordinatorStoreFuture, EntityId,
    EntityType, LeaseToken, PersistedShardCoordinatorState, RememberedEntityStore,
    RememberedStoreFuture, ShardCoordinatorLease, ShardKey, ShardingError, ShardingResult,
};
use tokio_postgres::Client;

/// Crate name used in diagnostics.
pub const CRATE_NAME: &str = "rakka-sharding-postgres";

/// Subsystem associated with the PostgreSQL sharding plugin.
#[must_use]
pub const fn subsystem() -> Subsystem {
    Subsystem::Sharding
}

/// Backend name for PostgreSQL sharding coordinator telemetry.
pub const BACKEND_NAME: &str = "postgres";

/// Default namespace for coordinator state when no explicit namespace is configured.
pub const DEFAULT_NAMESPACE: &str = "default";

/// Default shard coordinator state table name.
pub const COORDINATOR_TABLE_NAME: &str = "rakka_shard_coordinator_state";

/// Default shard coordinator lease table name.
pub const COORDINATOR_LEASE_TABLE_NAME: &str = "rakka_shard_coordinator_lease";

/// Default remembered entity table name.
pub const REMEMBERED_ENTITIES_TABLE_NAME: &str = "rakka_shard_remembered_entities";

/// PostgreSQL advisory lock id used while applying coordinator migrations.
pub const MIGRATION_LOCK_ID: i64 = 982_451_653;

/// SQL migration for the default shard coordinator state table.
pub const MIGRATION_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS rakka_shard_coordinator_state (
    namespace TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    revision BIGINT NOT NULL CHECK (revision >= 0),
    number_of_shards INTEGER NOT NULL CHECK (number_of_shards > 0),
    allocation_strategy TEXT NOT NULL,
    fencing_token BIGINT NOT NULL DEFAULT 0 CHECK (fencing_token >= 0),
    state_json JSONB NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace, entity_type)
);
ALTER TABLE rakka_shard_coordinator_state
    ADD COLUMN IF NOT EXISTS fencing_token BIGINT NOT NULL DEFAULT 0;
"#;

/// SQL migration for the default shard coordinator lease table.
pub const LEASE_MIGRATION_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS rakka_shard_coordinator_lease (
    namespace TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    holder_node TEXT NOT NULL,
    fencing_token BIGINT NOT NULL CHECK (fencing_token > 0),
    expires_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace, entity_type)
);
"#;

/// SQL migration for the default remembered entity table.
pub const REMEMBERED_ENTITIES_MIGRATION_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS rakka_shard_remembered_entities (
    namespace TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    shard_id INTEGER NOT NULL CHECK (shard_id >= 0),
    entity_id TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace, entity_type, shard_id, entity_id)
);
"#;

/// Builder for [`PostgresShardCoordinatorStore`].
pub struct PostgresShardCoordinatorStoreBuilder {
    client: Client,
    namespace: String,
}

/// Builder for [`PostgresShardCoordinatorLease`].
pub struct PostgresShardCoordinatorLeaseBuilder {
    client: Client,
    namespace: String,
    lease_duration: Duration,
}

/// Builder for [`PostgresRememberedEntityStore`].
pub struct PostgresRememberedEntityStoreBuilder {
    client: Client,
    namespace: String,
}

impl PostgresShardCoordinatorLeaseBuilder {
    /// Sets the namespace used to isolate coordinator leases.
    #[must_use]
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = namespace.into();
        self
    }

    /// Sets the coordinator lease duration.
    #[must_use]
    pub const fn with_lease_duration(mut self, lease_duration: Duration) -> Self {
        self.lease_duration = lease_duration;
        self
    }

    /// Builds a PostgreSQL coordinator lease backend.
    #[must_use]
    pub fn build(self) -> PostgresShardCoordinatorLease {
        PostgresShardCoordinatorLease {
            client: Arc::new(self.client),
            namespace: self.namespace.into(),
            lease_duration: self.lease_duration,
        }
    }

    /// Applies the default migration and returns the built lease backend.
    pub async fn migrate(self) -> ShardingResult<PostgresShardCoordinatorLease> {
        let lease = self.build();
        lease.migrate().await?;
        Ok(lease)
    }
}

impl PostgresRememberedEntityStoreBuilder {
    /// Sets the namespace used to isolate remembered entity ids.
    #[must_use]
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = namespace.into();
        self
    }

    /// Builds a PostgreSQL remembered entity store.
    #[must_use]
    pub fn build(self) -> PostgresRememberedEntityStore {
        PostgresRememberedEntityStore {
            client: Arc::new(self.client),
            namespace: self.namespace.into(),
        }
    }

    /// Applies the default migration and returns the built store.
    pub async fn migrate(self) -> ShardingResult<PostgresRememberedEntityStore> {
        let store = self.build();
        store.migrate().await?;
        Ok(store)
    }
}

impl PostgresShardCoordinatorStoreBuilder {
    /// Sets the namespace used to isolate coordinator state.
    #[must_use]
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = namespace.into();
        self
    }

    /// Builds a PostgreSQL shard coordinator store.
    #[must_use]
    pub fn build(self) -> PostgresShardCoordinatorStore {
        PostgresShardCoordinatorStore {
            client: Arc::new(self.client),
            namespace: self.namespace.into(),
        }
    }

    /// Applies the default migration and returns the built store.
    pub async fn migrate(self) -> ShardingResult<PostgresShardCoordinatorStore> {
        let store = self.build();
        store.migrate().await?;
        Ok(store)
    }
}

/// PostgreSQL durable shard coordinator store.
#[derive(Clone)]
pub struct PostgresShardCoordinatorStore {
    client: Arc<Client>,
    namespace: Arc<str>,
}

impl PostgresShardCoordinatorStore {
    /// Creates a PostgreSQL shard coordinator store in the default namespace.
    #[must_use]
    pub fn new(client: Client) -> Self {
        Self::builder(client).build()
    }

    /// Creates a builder for a PostgreSQL shard coordinator store.
    #[must_use]
    pub fn builder(client: Client) -> PostgresShardCoordinatorStoreBuilder {
        PostgresShardCoordinatorStoreBuilder {
            client,
            namespace: DEFAULT_NAMESPACE.to_string(),
        }
    }

    /// Creates a PostgreSQL shard coordinator store in an explicit namespace.
    #[must_use]
    pub fn with_namespace(client: Client, namespace: impl Into<String>) -> Self {
        Self::builder(client).with_namespace(namespace).build()
    }

    /// Namespace used to isolate coordinator state.
    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Applies the default table migration.
    pub async fn migrate(&self) -> ShardingResult<()> {
        acquire_migration_lock(&self.client)
            .await
            .map_err(map_postgres_error)?;
        let migration_result = self.client.batch_execute(MIGRATION_SQL).await;
        let unlock_result = release_migration_lock(&self.client).await;

        match (migration_result, unlock_result) {
            (Err(error), _) | (Ok(()), Err(error)) => Err(map_postgres_error(error)),
            (Ok(()), Ok(())) => Ok(()),
        }
    }
}

impl std::fmt::Debug for PostgresShardCoordinatorStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresShardCoordinatorStore")
            .field("namespace", &self.namespace())
            .finish_non_exhaustive()
    }
}

impl AsyncShardCoordinatorStore for PostgresShardCoordinatorStore {
    fn backend_name(&self) -> &'static str {
        BACKEND_NAME
    }

    fn load<'a>(
        &'a self,
        entity_type: &'a EntityType,
    ) -> CoordinatorStoreFuture<'a, Option<PersistedShardCoordinatorState>> {
        let client = self.client.clone();
        let namespace = self.namespace.clone();
        Box::pin(async move {
            let row = client
                .query_opt(
                    r#"
SELECT revision,
       number_of_shards,
       allocation_strategy,
       state_json::text AS state_json
FROM rakka_shard_coordinator_state
WHERE namespace = $1
  AND entity_type = $2
"#,
                    &[&namespace.as_ref(), &entity_type.as_str()],
                )
                .await
                .map_err(map_postgres_error)?;

            row.map(decode_state_row).transpose()
        })
    }

    fn compare_and_set<'a>(
        &'a self,
        entity_type: &'a EntityType,
        expected_revision: u64,
        state: PersistedShardCoordinatorState,
    ) -> CoordinatorStoreFuture<'a, PersistedShardCoordinatorState> {
        let client = self.client.clone();
        let namespace = self.namespace.clone();
        Box::pin(async move {
            compare_and_set_state(
                &client,
                namespace.as_ref(),
                entity_type,
                expected_revision,
                state,
                None,
            )
            .await
        })
    }

    fn compare_and_set_with_lease<'a>(
        &'a self,
        entity_type: &'a EntityType,
        expected_revision: u64,
        state: PersistedShardCoordinatorState,
        lease_token: Option<&'a LeaseToken>,
    ) -> CoordinatorStoreFuture<'a, PersistedShardCoordinatorState> {
        let client = self.client.clone();
        let namespace = self.namespace.clone();
        Box::pin(async move {
            compare_and_set_state(
                &client,
                namespace.as_ref(),
                entity_type,
                expected_revision,
                state,
                lease_token,
            )
            .await
        })
    }

    fn delete<'a>(
        &'a self,
        entity_type: &'a EntityType,
        expected_revision: u64,
    ) -> CoordinatorStoreFuture<'a, ()> {
        let client = self.client.clone();
        let namespace = self.namespace.clone();
        Box::pin(async move {
            let expected = u64_to_i64(expected_revision, "expected coordinator revision")?;
            let deleted = client
                .execute(
                    r#"
DELETE FROM rakka_shard_coordinator_state
WHERE namespace = $1
  AND entity_type = $2
  AND revision = $3::bigint
"#,
                    &[&namespace.as_ref(), &entity_type.as_str(), &expected],
                )
                .await
                .map_err(map_postgres_error)?;

            if deleted == 1 {
                return Ok(());
            }

            let actual_revision =
                load_actual_revision(&client, namespace.as_ref(), entity_type).await?;
            if actual_revision == expected_revision {
                Ok(())
            } else {
                Err(ShardingError::CoordinatorRevisionConflict {
                    entity_type: entity_type.clone(),
                    expected_revision,
                    actual_revision,
                })
            }
        })
    }
}

/// PostgreSQL coordinator leadership lease backend.
#[derive(Clone)]
pub struct PostgresShardCoordinatorLease {
    client: Arc<Client>,
    namespace: Arc<str>,
    lease_duration: Duration,
}

impl PostgresShardCoordinatorLease {
    /// Default PostgreSQL coordinator lease duration.
    pub const DEFAULT_LEASE_DURATION: Duration = Duration::from_secs(15);

    /// Creates a PostgreSQL coordinator lease backend in the default namespace.
    #[must_use]
    pub fn new(client: Client) -> Self {
        Self::builder(client).build()
    }

    /// Creates a builder for a PostgreSQL coordinator lease backend.
    #[must_use]
    pub fn builder(client: Client) -> PostgresShardCoordinatorLeaseBuilder {
        PostgresShardCoordinatorLeaseBuilder {
            client,
            namespace: DEFAULT_NAMESPACE.to_string(),
            lease_duration: Self::DEFAULT_LEASE_DURATION,
        }
    }

    /// Creates a PostgreSQL coordinator lease backend in an explicit namespace.
    #[must_use]
    pub fn with_namespace(client: Client, namespace: impl Into<String>) -> Self {
        Self::builder(client).with_namespace(namespace).build()
    }

    /// Namespace used to isolate coordinator leases.
    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Coordinator lease duration.
    #[must_use]
    pub const fn lease_duration(&self) -> Duration {
        self.lease_duration
    }

    /// Applies the default lease table migration.
    pub async fn migrate(&self) -> ShardingResult<()> {
        acquire_migration_lock(&self.client)
            .await
            .map_err(map_postgres_error)?;
        let migration_result = self.client.batch_execute(LEASE_MIGRATION_SQL).await;
        let unlock_result = release_migration_lock(&self.client).await;

        match (migration_result, unlock_result) {
            (Err(error), _) | (Ok(()), Err(error)) => Err(map_postgres_error(error)),
            (Ok(()), Ok(())) => Ok(()),
        }
    }

    fn lease_duration_millis(&self) -> ShardingResult<f64> {
        let millis = self.lease_duration.as_secs_f64() * 1000.0;
        if millis <= 0.0 {
            return Err(ShardingError::CoordinatorLease {
                lease: BACKEND_NAME.to_string(),
                message: "lease duration must be greater than zero".to_string(),
            });
        }
        if millis.is_finite() {
            Ok(millis)
        } else {
            Err(ShardingError::CoordinatorLease {
                lease: BACKEND_NAME.to_string(),
                message: "lease duration is not finite".to_string(),
            })
        }
    }
}

impl std::fmt::Debug for PostgresShardCoordinatorLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresShardCoordinatorLease")
            .field("namespace", &self.namespace())
            .field("lease_duration", &self.lease_duration)
            .finish_non_exhaustive()
    }
}

impl ShardCoordinatorLease for PostgresShardCoordinatorLease {
    fn lease_name(&self) -> &'static str {
        BACKEND_NAME
    }

    fn acquire<'a>(
        &'a self,
        entity_type: &'a EntityType,
        holder: &'a NodeId,
    ) -> CoordinatorLeaseFuture<'a, LeaseToken> {
        let client = self.client.clone();
        let namespace = self.namespace.clone();
        Box::pin(async move {
            let lease_millis = self.lease_duration_millis()?;
            let holder_node = holder.to_string();

            if let Some(row) = client
                .query_opt(
                    r#"
UPDATE rakka_shard_coordinator_lease
SET holder_node = $3,
    fencing_token = CASE
        WHEN holder_node = $3 THEN fencing_token
        ELSE fencing_token + 1
    END,
    expires_at = now() + ($4::double precision * INTERVAL '1 millisecond'),
    updated_at = now()
WHERE namespace = $1
  AND entity_type = $2
  AND (holder_node = $3 OR expires_at <= now())
RETURNING holder_node,
          fencing_token,
          ((EXTRACT(EPOCH FROM expires_at) * 1000)::bigint) AS expires_at_millis
"#,
                    &[
                        &namespace.as_ref(),
                        &entity_type.as_str(),
                        &holder_node,
                        &lease_millis,
                    ],
                )
                .await
                .map_err(map_postgres_error)?
            {
                return decode_lease_row(namespace.as_ref(), entity_type, row);
            }

            if let Some(row) = client
                .query_opt(
                    r#"
INSERT INTO rakka_shard_coordinator_lease
    (namespace, entity_type, holder_node, fencing_token, expires_at)
VALUES (
    $1,
    $2,
    $3,
    1,
    now() + ($4::double precision * INTERVAL '1 millisecond')
)
ON CONFLICT (namespace, entity_type) DO NOTHING
RETURNING holder_node,
          fencing_token,
          ((EXTRACT(EPOCH FROM expires_at) * 1000)::bigint) AS expires_at_millis
"#,
                    &[
                        &namespace.as_ref(),
                        &entity_type.as_str(),
                        &holder_node,
                        &lease_millis,
                    ],
                )
                .await
                .map_err(map_postgres_error)?
            {
                return decode_lease_row(namespace.as_ref(), entity_type, row);
            }

            let current = load_current_lease(&client, namespace.as_ref(), entity_type).await?;
            Err(lease_rejected(BACKEND_NAME, entity_type, holder, current))
        })
    }

    fn renew<'a>(&'a self, token: &'a LeaseToken) -> CoordinatorLeaseFuture<'a, ()> {
        let client = self.client.clone();
        Box::pin(async move {
            let lease_millis = self.lease_duration_millis()?;
            let fencing_token = u64_to_i64(token.fencing_token(), "lease fencing token")?;
            let holder_node = token.holder_node().to_string();
            let row = client
                .query_opt(
                    r#"
UPDATE rakka_shard_coordinator_lease
SET expires_at = now() + ($5::double precision * INTERVAL '1 millisecond'),
    updated_at = now()
WHERE namespace = $1
  AND entity_type = $2
  AND holder_node = $3
  AND fencing_token = $4::bigint
  AND expires_at > now()
RETURNING fencing_token
"#,
                    &[
                        &token.namespace(),
                        &token.entity_type().as_str(),
                        &holder_node,
                        &fencing_token,
                        &lease_millis,
                    ],
                )
                .await
                .map_err(map_postgres_error)?;

            if row.is_some() {
                return Ok(());
            }

            let current =
                load_current_lease(&client, token.namespace(), token.entity_type()).await?;
            Err(lease_lost_from_current(BACKEND_NAME, token, current))
        })
    }

    fn release<'a>(&'a self, token: LeaseToken) -> CoordinatorLeaseFuture<'a, ()> {
        let client = self.client.clone();
        Box::pin(async move {
            let fencing_token = u64_to_i64(token.fencing_token(), "lease fencing token")?;
            let holder_node = token.holder_node().to_string();
            let deleted = client
                .execute(
                    r#"
DELETE FROM rakka_shard_coordinator_lease
WHERE namespace = $1
  AND entity_type = $2
  AND holder_node = $3
  AND fencing_token = $4::bigint
"#,
                    &[
                        &token.namespace(),
                        &token.entity_type().as_str(),
                        &holder_node,
                        &fencing_token,
                    ],
                )
                .await
                .map_err(map_postgres_error)?;

            if deleted == 1 {
                return Ok(());
            }

            let current =
                load_current_lease(&client, token.namespace(), token.entity_type()).await?;
            if current.is_none() {
                Ok(())
            } else {
                Err(lease_lost_from_current(BACKEND_NAME, &token, current))
            }
        })
    }
}

/// PostgreSQL remembered entity store.
#[derive(Clone)]
pub struct PostgresRememberedEntityStore {
    client: Arc<Client>,
    namespace: Arc<str>,
}

impl PostgresRememberedEntityStore {
    /// Creates a PostgreSQL remembered entity store in the default namespace.
    #[must_use]
    pub fn new(client: Client) -> Self {
        Self::builder(client).build()
    }

    /// Creates a builder for a PostgreSQL remembered entity store.
    #[must_use]
    pub fn builder(client: Client) -> PostgresRememberedEntityStoreBuilder {
        PostgresRememberedEntityStoreBuilder {
            client,
            namespace: DEFAULT_NAMESPACE.to_string(),
        }
    }

    /// Creates a PostgreSQL remembered entity store in an explicit namespace.
    #[must_use]
    pub fn with_namespace(client: Client, namespace: impl Into<String>) -> Self {
        Self::builder(client).with_namespace(namespace).build()
    }

    /// Namespace used to isolate remembered entity ids.
    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Applies the default remembered entity table migration.
    pub async fn migrate(&self) -> ShardingResult<()> {
        acquire_migration_lock(&self.client)
            .await
            .map_err(map_remembered_postgres_error)?;
        let migration_result = self
            .client
            .batch_execute(REMEMBERED_ENTITIES_MIGRATION_SQL)
            .await;
        let unlock_result = release_migration_lock(&self.client).await;

        match (migration_result, unlock_result) {
            (Err(error), _) | (Ok(()), Err(error)) => Err(map_remembered_postgres_error(error)),
            (Ok(()), Ok(())) => Ok(()),
        }
    }
}

impl std::fmt::Debug for PostgresRememberedEntityStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresRememberedEntityStore")
            .field("namespace", &self.namespace())
            .finish_non_exhaustive()
    }
}

impl RememberedEntityStore for PostgresRememberedEntityStore {
    fn backend_name(&self) -> &'static str {
        BACKEND_NAME
    }

    fn remember<'a>(
        &'a self,
        shard: &'a ShardKey,
        entity_id: &'a EntityId,
    ) -> RememberedStoreFuture<'a, ()> {
        let client = self.client.clone();
        let namespace = self.namespace.clone();
        Box::pin(async move {
            let shard_id = shard_id_to_i32(shard)?;
            client
                .execute(
                    r#"
INSERT INTO rakka_shard_remembered_entities
    (namespace, entity_type, shard_id, entity_id)
VALUES ($1, $2, $3::integer, $4)
ON CONFLICT (namespace, entity_type, shard_id, entity_id)
DO UPDATE SET updated_at = now()
"#,
                    &[
                        &namespace.as_ref(),
                        &shard.entity_type().as_str(),
                        &shard_id,
                        &entity_id.as_str(),
                    ],
                )
                .await
                .map_err(map_remembered_postgres_error)?;
            Ok(())
        })
    }

    fn forget<'a>(
        &'a self,
        shard: &'a ShardKey,
        entity_id: &'a EntityId,
    ) -> RememberedStoreFuture<'a, bool> {
        let client = self.client.clone();
        let namespace = self.namespace.clone();
        Box::pin(async move {
            let shard_id = shard_id_to_i32(shard)?;
            let deleted = client
                .execute(
                    r#"
DELETE FROM rakka_shard_remembered_entities
WHERE namespace = $1
  AND entity_type = $2
  AND shard_id = $3::integer
  AND entity_id = $4
"#,
                    &[
                        &namespace.as_ref(),
                        &shard.entity_type().as_str(),
                        &shard_id,
                        &entity_id.as_str(),
                    ],
                )
                .await
                .map_err(map_remembered_postgres_error)?;
            Ok(deleted > 0)
        })
    }

    fn remembered_for_shard<'a>(
        &'a self,
        shard: &'a ShardKey,
    ) -> RememberedStoreFuture<'a, Vec<EntityId>> {
        let client = self.client.clone();
        let namespace = self.namespace.clone();
        Box::pin(async move {
            let shard_id = shard_id_to_i32(shard)?;
            let rows = client
                .query(
                    r#"
SELECT entity_id
FROM rakka_shard_remembered_entities
WHERE namespace = $1
  AND entity_type = $2
  AND shard_id = $3::integer
ORDER BY entity_id ASC
"#,
                    &[
                        &namespace.as_ref(),
                        &shard.entity_type().as_str(),
                        &shard_id,
                    ],
                )
                .await
                .map_err(map_remembered_postgres_error)?;
            Ok(rows
                .into_iter()
                .map(|row| EntityId::new(row.get::<_, String>("entity_id")))
                .collect())
        })
    }
}

async fn compare_and_set_state(
    client: &Client,
    namespace: &str,
    entity_type: &EntityType,
    expected_revision: u64,
    state: PersistedShardCoordinatorState,
    lease_token: Option<&LeaseToken>,
) -> ShardingResult<PersistedShardCoordinatorState> {
    validate_state_entity_type(entity_type, &state)?;
    let revision = u64_to_i64(state.snapshot().revision(), "coordinator revision")?;
    let expected = u64_to_i64(expected_revision, "expected coordinator revision")?;
    let number_of_shards = u32_to_i32(state.snapshot().number_of_shards())?;
    let allocation_strategy = state.allocation_strategy().to_string();
    let fencing_token = lease_token.map_or(Ok(0), |token| {
        u64_to_i64(token.fencing_token(), "lease fencing token")
    })?;
    let state_json =
        serde_json::to_string(&state).map_err(|error| ShardingError::CoordinatorStore {
            backend: BACKEND_NAME.to_string(),
            message: format!("failed to encode coordinator state: {error}"),
        })?;

    let row = if expected_revision == 0 {
        client
            .query_opt(
                r#"
INSERT INTO rakka_shard_coordinator_state
    (namespace, entity_type, revision, number_of_shards, allocation_strategy, fencing_token, state_json, schema_version)
VALUES ($1, $2, $3::bigint, $4::integer, $5, $6::bigint, $7::text::jsonb, 1)
ON CONFLICT (namespace, entity_type) DO NOTHING
RETURNING revision
"#,
                &[
                    &namespace,
                    &entity_type.as_str(),
                    &revision,
                    &number_of_shards,
                    &allocation_strategy,
                    &fencing_token,
                    &state_json,
                ],
            )
            .await
    } else {
        client
            .query_opt(
                r#"
UPDATE rakka_shard_coordinator_state
SET revision = $3::bigint,
    number_of_shards = $4::integer,
    allocation_strategy = $5,
    fencing_token = $6::bigint,
    state_json = $7::text::jsonb,
    schema_version = 1,
    updated_at = now()
WHERE namespace = $1
  AND entity_type = $2
  AND revision = $8::bigint
  AND fencing_token <= $6::bigint
RETURNING revision
"#,
                &[
                    &namespace,
                    &entity_type.as_str(),
                    &revision,
                    &number_of_shards,
                    &allocation_strategy,
                    &fencing_token,
                    &state_json,
                    &expected,
                ],
            )
            .await
    }
    .map_err(map_postgres_error)?;

    if row.is_some() {
        return Ok(state);
    }

    let actual = load_actual_revision_and_fencing(client, namespace, entity_type).await?;
    if let (Some(token), Some((actual_revision, actual_fencing_token))) = (lease_token, actual) {
        if actual_revision == expected_revision && actual_fencing_token > token.fencing_token() {
            return Err(ShardingError::CoordinatorLeaseLost {
                lease: BACKEND_NAME.to_string(),
                entity_type: Box::new(entity_type.clone()),
                holder_node: Box::new(token.holder_node().clone()),
                fencing_token: token.fencing_token(),
                actual_holder_node: None,
                actual_fencing_token: Some(actual_fencing_token),
            });
        }
    }

    Err(ShardingError::CoordinatorRevisionConflict {
        entity_type: entity_type.clone(),
        expected_revision,
        actual_revision: actual.map_or(0, |(revision, _fencing)| revision),
    })
}

fn decode_state_row(row: tokio_postgres::Row) -> ShardingResult<PersistedShardCoordinatorState> {
    let revision = i64_to_u64(row.get("revision"), "coordinator revision")?;
    let number_of_shards = i32_to_u32(row.get("number_of_shards"))?;
    let allocation_strategy: String = row.get("allocation_strategy");
    let state_json: String = row.get("state_json");
    let state =
        serde_json::from_str::<PersistedShardCoordinatorState>(&state_json).map_err(|error| {
            ShardingError::CoordinatorStore {
                backend: BACKEND_NAME.to_string(),
                message: format!("failed to decode coordinator state: {error}"),
            }
        })?;

    if state.snapshot().revision() != revision {
        return Err(ShardingError::CoordinatorStore {
            backend: BACKEND_NAME.to_string(),
            message: format!(
                "coordinator state revision {} did not match row revision {revision}",
                state.snapshot().revision()
            ),
        });
    }

    if state.snapshot().number_of_shards() != number_of_shards {
        return Err(ShardingError::CoordinatorStore {
            backend: BACKEND_NAME.to_string(),
            message: format!(
                "coordinator state shard count {} did not match row shard count {number_of_shards}",
                state.snapshot().number_of_shards()
            ),
        });
    }

    if state.allocation_strategy() != allocation_strategy {
        return Err(ShardingError::CoordinatorStore {
            backend: BACKEND_NAME.to_string(),
            message: format!(
                "coordinator state allocation strategy {} did not match row strategy {allocation_strategy}",
                state.allocation_strategy()
            ),
        });
    }

    Ok(state)
}

fn validate_state_entity_type(
    entity_type: &EntityType,
    state: &PersistedShardCoordinatorState,
) -> ShardingResult<()> {
    if state.snapshot().entity_type() == entity_type {
        Ok(())
    } else {
        Err(ShardingError::PersistedCoordinatorSnapshotMismatch {
            expected_entity_type: entity_type.clone(),
            actual_entity_type: state.snapshot().entity_type().clone(),
            expected_shards: state.snapshot().number_of_shards(),
            actual_shards: state.snapshot().number_of_shards(),
        })
    }
}

#[derive(Debug, Clone)]
struct CurrentLease {
    holder_node: NodeId,
    fencing_token: u64,
    expires_at_millis: u64,
}

fn decode_lease_row(
    namespace: &str,
    entity_type: &EntityType,
    row: tokio_postgres::Row,
) -> ShardingResult<LeaseToken> {
    let holder_node: String = row.get("holder_node");
    let holder_node =
        NodeId::from_str(&holder_node).map_err(|error| ShardingError::CoordinatorLease {
            lease: BACKEND_NAME.to_string(),
            message: format!("failed to decode lease holder node: {error}"),
        })?;
    let fencing_token = i64_to_u64(row.get("fencing_token"), "lease fencing token")?;
    let expires_at_millis = i64_to_u64(row.get("expires_at_millis"), "lease expiry")?;

    Ok(LeaseToken::new(
        namespace.to_string(),
        entity_type.clone(),
        holder_node,
        fencing_token,
        expires_at_millis,
    ))
}

async fn load_current_lease(
    client: &Client,
    namespace: &str,
    entity_type: &EntityType,
) -> ShardingResult<Option<CurrentLease>> {
    let row = client
        .query_opt(
            r#"
SELECT holder_node,
       fencing_token,
       ((EXTRACT(EPOCH FROM expires_at) * 1000)::bigint) AS expires_at_millis
FROM rakka_shard_coordinator_lease
WHERE namespace = $1
  AND entity_type = $2
"#,
            &[&namespace, &entity_type.as_str()],
        )
        .await
        .map_err(map_postgres_error)?;

    row.map(|row| {
        let holder_node: String = row.get("holder_node");
        let holder_node =
            NodeId::from_str(&holder_node).map_err(|error| ShardingError::CoordinatorLease {
                lease: BACKEND_NAME.to_string(),
                message: format!("failed to decode lease holder node: {error}"),
            })?;
        Ok(CurrentLease {
            holder_node,
            fencing_token: i64_to_u64(row.get("fencing_token"), "lease fencing token")?,
            expires_at_millis: i64_to_u64(row.get("expires_at_millis"), "lease expiry")?,
        })
    })
    .transpose()
}

fn lease_rejected(
    lease: &str,
    entity_type: &EntityType,
    holder: &NodeId,
    current: Option<CurrentLease>,
) -> ShardingError {
    ShardingError::CoordinatorLeaseRejected {
        lease: lease.to_string(),
        entity_type: Box::new(entity_type.clone()),
        holder_node: Box::new(holder.clone()),
        current_holder_node: current
            .as_ref()
            .map(|lease| Box::new(lease.holder_node.clone())),
        expires_at_millis: current.map(|lease| lease.expires_at_millis),
    }
}

fn lease_lost_from_current(
    lease: &str,
    token: &LeaseToken,
    current: Option<CurrentLease>,
) -> ShardingError {
    ShardingError::CoordinatorLeaseLost {
        lease: lease.to_string(),
        entity_type: Box::new(token.entity_type().clone()),
        holder_node: Box::new(token.holder_node().clone()),
        fencing_token: token.fencing_token(),
        actual_holder_node: current
            .as_ref()
            .map(|lease| Box::new(lease.holder_node.clone())),
        actual_fencing_token: current.map(|lease| lease.fencing_token),
    }
}

async fn load_actual_revision(
    client: &Client,
    namespace: &str,
    entity_type: &EntityType,
) -> ShardingResult<u64> {
    let row = client
        .query_opt(
            r#"
SELECT revision
FROM rakka_shard_coordinator_state
WHERE namespace = $1
  AND entity_type = $2
"#,
            &[&namespace, &entity_type.as_str()],
        )
        .await
        .map_err(map_postgres_error)?;

    row.map(|row| i64_to_u64(row.get("revision"), "coordinator revision"))
        .transpose()
        .map(|revision| revision.unwrap_or(0))
}

async fn load_actual_revision_and_fencing(
    client: &Client,
    namespace: &str,
    entity_type: &EntityType,
) -> ShardingResult<Option<(u64, u64)>> {
    let row = client
        .query_opt(
            r#"
SELECT revision,
       fencing_token
FROM rakka_shard_coordinator_state
WHERE namespace = $1
  AND entity_type = $2
"#,
            &[&namespace, &entity_type.as_str()],
        )
        .await
        .map_err(map_postgres_error)?;

    row.map(|row| {
        Ok((
            i64_to_u64(row.get("revision"), "coordinator revision")?,
            i64_to_u64(row.get("fencing_token"), "lease fencing token")?,
        ))
    })
    .transpose()
}

fn u64_to_i64(value: u64, label: &str) -> ShardingResult<i64> {
    i64::try_from(value).map_err(|_overflow| ShardingError::CoordinatorStore {
        backend: BACKEND_NAME.to_string(),
        message: format!("{label} {value} exceeds PostgreSQL bigint range"),
    })
}

fn i64_to_u64(value: i64, label: &str) -> ShardingResult<u64> {
    u64::try_from(value).map_err(|_negative| ShardingError::CoordinatorStore {
        backend: BACKEND_NAME.to_string(),
        message: format!("{label} {value} was negative"),
    })
}

fn u32_to_i32(value: u32) -> ShardingResult<i32> {
    i32::try_from(value).map_err(|_overflow| ShardingError::CoordinatorStore {
        backend: BACKEND_NAME.to_string(),
        message: format!("shard count {value} exceeds PostgreSQL integer range"),
    })
}

fn i32_to_u32(value: i32) -> ShardingResult<u32> {
    u32::try_from(value).map_err(|_negative| ShardingError::CoordinatorStore {
        backend: BACKEND_NAME.to_string(),
        message: format!("shard count {value} was negative"),
    })
}

fn shard_id_to_i32(shard: &ShardKey) -> ShardingResult<i32> {
    i32::try_from(shard.shard_id().as_u32()).map_err(|_overflow| {
        ShardingError::RememberedEntityStore {
            backend: BACKEND_NAME.to_string(),
            message: format!(
                "shard id {} exceeds PostgreSQL integer range",
                shard.shard_id()
            ),
        }
    })
}

async fn acquire_migration_lock(client: &Client) -> Result<(), tokio_postgres::Error> {
    let _row = client
        .query_one("SELECT pg_advisory_lock($1)", &[&MIGRATION_LOCK_ID])
        .await?;
    Ok(())
}

async fn release_migration_lock(client: &Client) -> Result<(), tokio_postgres::Error> {
    let _row = client
        .query_one("SELECT pg_advisory_unlock($1)", &[&MIGRATION_LOCK_ID])
        .await?;
    Ok(())
}

fn map_postgres_error(error: tokio_postgres::Error) -> ShardingError {
    let mut message = error.to_string();
    let mut source = error.source();
    while let Some(error) = source {
        message.push_str(": ");
        message.push_str(&error.to_string());
        source = error.source();
    }
    ShardingError::CoordinatorStore {
        backend: BACKEND_NAME.to_string(),
        message,
    }
}

fn map_remembered_postgres_error(error: tokio_postgres::Error) -> ShardingError {
    let mut message = error.to_string();
    let mut source = error.source();
    while let Some(error) = source {
        message.push_str(": ");
        message.push_str(&error.to_string());
        source = error.source();
    }
    ShardingError::RememberedEntityStore {
        backend: BACKEND_NAME.to_string(),
        message,
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use rakka_cluster::{
        ClusterMembership, ClusterNode, DiscoverySnapshot, MembershipConfig, NodeAddress, NodeId,
    };
    use rakka_sharding::{
        AsyncShardCoordinatorStore, ClusterShardingRuntime, EntityType, RoutedEntityMessage,
        ShardCoordinator, ShardCoordinatorLease, ShardId, ShardRegion, ShardingConfig,
        ShardingError,
    };
    use tokio_postgres::NoTls;

    use super::*;

    #[derive(Debug)]
    struct TestCommand;

    #[tokio::test]
    async fn postgres_store_round_trip_when_dsn_is_set() {
        let dsn = match std::env::var("RAKKA_POSTGRES_TEST_DSN") {
            Ok(dsn) => dsn,
            Err(_) => return,
        };
        let store = connect_store(&dsn, unique_namespace("round-trip")).await;
        let entity_type = EntityType::new("PostgresCart");
        let (first, second) = coordinator_states(entity_type.clone());

        assert_eq!(store.load(&entity_type).await.unwrap(), None);

        let inserted = store
            .compare_and_set(&entity_type, 0, first.clone())
            .await
            .unwrap();
        assert_eq!(inserted, first);
        assert_eq!(store.load(&entity_type).await.unwrap(), Some(first.clone()));

        let updated = store
            .compare_and_set(&entity_type, first.snapshot().revision(), second.clone())
            .await
            .unwrap();
        assert_eq!(updated, second);

        let conflict = store
            .compare_and_set(&entity_type, first.snapshot().revision(), first)
            .await
            .unwrap_err();
        assert!(matches!(
            conflict,
            ShardingError::CoordinatorRevisionConflict {
                expected_revision: 1,
                actual_revision: 2,
                ..
            }
        ));

        store
            .delete(&entity_type, second.snapshot().revision())
            .await
            .unwrap();
        assert_eq!(store.load(&entity_type).await.unwrap(), None);
        store.delete(&entity_type, 0).await.unwrap();
    }

    #[tokio::test]
    async fn postgres_store_isolates_namespaces_when_dsn_is_set() {
        let dsn = match std::env::var("RAKKA_POSTGRES_TEST_DSN") {
            Ok(dsn) => dsn,
            Err(_) => return,
        };
        let namespace_a = unique_namespace("namespace-a");
        let namespace_b = unique_namespace("namespace-b");
        let store_a = connect_store(&dsn, namespace_a).await;
        let store_b = connect_store(&dsn, namespace_b).await;
        let entity_type = EntityType::new("NamespaceCart");
        let (first, _second) = coordinator_states(entity_type.clone());

        store_a
            .compare_and_set(&entity_type, 0, first.clone())
            .await
            .unwrap();

        assert_eq!(store_a.load(&entity_type).await.unwrap(), Some(first));
        assert_eq!(store_b.load(&entity_type).await.unwrap(), None);
    }

    #[tokio::test]
    async fn postgres_store_rejects_stale_fencing_token_when_dsn_is_set() {
        let dsn = match std::env::var("RAKKA_POSTGRES_TEST_DSN") {
            Ok(dsn) => dsn,
            Err(_) => return,
        };
        let namespace = unique_namespace("store-fencing");
        let store = connect_store(&dsn, namespace.clone()).await;
        let entity_type = EntityType::new("FencedStoreCart");
        let (first, second) = coordinator_states(entity_type.clone());
        let holder_a = NodeId::new("rakka-0", "uid-a");
        let holder_b = NodeId::new("rakka-1", "uid-b");
        let stale_token = LeaseToken::new(namespace.clone(), entity_type.clone(), holder_a, 1, 1);
        let current_token = LeaseToken::new(namespace, entity_type.clone(), holder_b, 2, 2);

        store
            .compare_and_set_with_lease(&entity_type, 0, first, Some(&current_token))
            .await
            .unwrap();

        let lost = store
            .compare_and_set_with_lease(
                &entity_type,
                second.snapshot().revision() - 1,
                second,
                Some(&stale_token),
            )
            .await
            .unwrap_err();

        assert!(matches!(
            lost,
            ShardingError::CoordinatorLeaseLost {
                entity_type,
                holder_node,
                fencing_token: 1,
                actual_fencing_token: Some(2),
                ..
            } if *entity_type == EntityType::new("FencedStoreCart")
                && *holder_node == NodeId::new("rakka-0", "uid-a")
        ));
    }

    #[tokio::test]
    async fn postgres_runtime_recovers_without_rewriting_when_dsn_is_set() {
        let dsn = match std::env::var("RAKKA_POSTGRES_TEST_DSN") {
            Ok(dsn) => dsn,
            Err(_) => return,
        };
        let store = connect_store(&dsn, unique_namespace("runtime")).await;
        let local = node("rakka-0", "uid-a");
        let remote = node("rakka-1", "uid-b");
        let membership = membership_with_up_nodes(vec![local, remote]);
        let entity_type = EntityType::new("RuntimeCart");
        let config = ShardingConfig::new(4).unwrap();

        let mut first_runtime =
            ClusterShardingRuntime::with_async_coordinator_store(membership.clone(), store.clone());
        first_runtime
            .register_region_async(region(entity_type.clone(), config.clone()))
            .await
            .unwrap();
        let first_state = store.load(&entity_type).await.unwrap().unwrap();

        let mut recovered_runtime =
            ClusterShardingRuntime::with_async_coordinator_store(membership, store.clone());
        recovered_runtime
            .register_region_async(region(entity_type.clone(), config))
            .await
            .unwrap();
        let recovered_state = store.load(&entity_type).await.unwrap().unwrap();

        assert_eq!(recovered_state, first_state);
        assert_eq!(
            recovered_runtime
                .coordinator(&entity_type)
                .unwrap()
                .revision(),
            first_state.snapshot().revision()
        );
    }

    #[tokio::test]
    async fn postgres_lease_acquires_renews_and_releases_when_dsn_is_set() {
        let dsn = match std::env::var("RAKKA_POSTGRES_TEST_DSN") {
            Ok(dsn) => dsn,
            Err(_) => return,
        };
        // Nothing in the round trip depends on expiry, so the duration is
        // generous: a loaded CI runner pausing between the acquire and the
        // renew must not expire the lease out from under the test.
        let lease = connect_lease(
            &dsn,
            unique_namespace("lease-round-trip"),
            Duration::from_secs(30),
        )
        .await;
        let entity_type = EntityType::new("PostgresLeaseCart");
        let holder = NodeId::new("rakka-0", "uid-a");

        let token = lease.acquire(&entity_type, &holder).await.unwrap();
        assert_eq!(token.namespace(), lease.namespace());
        assert_eq!(token.entity_type(), &entity_type);
        assert_eq!(token.holder_node(), &holder);
        assert_eq!(token.fencing_token(), 1);

        lease.renew(&token).await.unwrap();
        lease.release(token).await.unwrap();

        let reacquired = lease.acquire(&entity_type, &holder).await.unwrap();
        assert_eq!(reacquired.fencing_token(), 1);
    }

    #[tokio::test]
    async fn postgres_lease_rejects_active_holder_and_fences_stale_token_when_dsn_is_set() {
        let dsn = match std::env::var("RAKKA_POSTGRES_TEST_DSN") {
            Ok(dsn) => dsn,
            Err(_) => return,
        };
        let namespace = unique_namespace("lease-conflict");
        let holder_a = NodeId::new("rakka-0", "uid-a");
        let holder_b = NodeId::new("rakka-1", "uid-b");

        // An unexpired holder rejects a contender. The holding lease's
        // duration is generous so the contender's acquire always lands
        // inside it, however slowly CI schedules the round trips.
        let held_type = EntityType::new("PostgresLeaseHeldCart");
        let lease_a_held = connect_lease(&dsn, namespace.clone(), Duration::from_secs(30)).await;
        let lease_b = connect_lease(&dsn, namespace.clone(), Duration::from_millis(100)).await;
        lease_a_held.acquire(&held_type, &holder_a).await.unwrap();
        let rejected = lease_b.acquire(&held_type, &holder_b).await.unwrap_err();
        assert!(matches!(
            rejected,
        ShardingError::CoordinatorLeaseRejected {
            entity_type,
            holder_node,
            current_holder_node: Some(current_holder_node),
            ..
        } if *entity_type == held_type
                && *holder_node == holder_b
                && *current_holder_node == holder_a
        ));

        // An expired holder is superseded with a bumped fencing token, and
        // its stale renew is fenced. The first holder's duration is short so
        // it expires; the contender polls until its acquire succeeds instead
        // of racing a fixed sleep against the wall clock.
        let entity_type = EntityType::new("PostgresLeaseConflictCart");
        let lease_a = connect_lease(&dsn, namespace.clone(), Duration::from_millis(10)).await;
        let token_a = lease_a.acquire(&entity_type, &holder_a).await.unwrap();
        assert_eq!(token_a.fencing_token(), 1);
        let mut acquired_b = None;
        for _attempt in 0..200 {
            match lease_b.acquire(&entity_type, &holder_b).await {
                Ok(token) => {
                    acquired_b = Some(token);
                    break;
                }
                Err(ShardingError::CoordinatorLeaseRejected { .. }) => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(error) => panic!("the contender's acquire failed: {error}"),
            }
        }
        let token_b = acquired_b.expect("the expired holder is eventually superseded");
        assert_eq!(token_b.fencing_token(), 2);

        let lost = lease_a.renew(&token_a).await.unwrap_err();
        assert!(matches!(
            lost,
        ShardingError::CoordinatorLeaseLost {
            entity_type,
            holder_node,
            fencing_token: 1,
            actual_holder_node: Some(actual_holder_node),
            actual_fencing_token: Some(2),
            ..
        } if *entity_type == EntityType::new("PostgresLeaseConflictCart")
                && *holder_node == holder_a
                && *actual_holder_node == holder_b
        ));
    }

    #[tokio::test]
    async fn postgres_remembered_entity_store_round_trips_when_dsn_is_set() {
        let dsn = match std::env::var("RAKKA_POSTGRES_TEST_DSN") {
            Ok(dsn) => dsn,
            Err(_) => return,
        };
        let namespace_a = unique_namespace("remembered-a");
        let namespace_b = unique_namespace("remembered-b");
        let store_a = connect_remembered_store(&dsn, namespace_a).await;
        let store_b = connect_remembered_store(&dsn, namespace_b).await;
        let shard = ShardKey::new(EntityType::new("PostgresRememberedCart"), ShardId::new(2));
        let cart_a = EntityId::new("cart-a");
        let cart_b = EntityId::new("cart-b");

        store_a.remember(&shard, &cart_b).await.unwrap();
        store_a.remember(&shard, &cart_a).await.unwrap();
        store_a.remember(&shard, &cart_a).await.unwrap();

        assert_eq!(
            store_a.remembered_for_shard(&shard).await.unwrap(),
            vec![cart_a.clone(), cart_b]
        );
        assert_eq!(store_b.remembered_for_shard(&shard).await.unwrap(), vec![]);
        assert!(store_a.forget(&shard, &cart_a).await.unwrap());
        assert!(!store_a.forget(&shard, &cart_a).await.unwrap());
        assert_eq!(
            store_a.remembered_for_shard(&shard).await.unwrap(),
            vec![EntityId::new("cart-b")]
        );
    }

    async fn connect_store(dsn: &str, namespace: String) -> PostgresShardCoordinatorStore {
        let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.unwrap();
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                eprintln!("postgres sharding connection error: {error}");
            }
        });
        PostgresShardCoordinatorStore::builder(client)
            .with_namespace(namespace)
            .migrate()
            .await
            .unwrap()
    }

    async fn connect_lease(
        dsn: &str,
        namespace: String,
        lease_duration: Duration,
    ) -> PostgresShardCoordinatorLease {
        let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.unwrap();
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                eprintln!("postgres sharding lease connection error: {error}");
            }
        });
        PostgresShardCoordinatorLease::builder(client)
            .with_namespace(namespace)
            .with_lease_duration(lease_duration)
            .migrate()
            .await
            .unwrap()
    }

    async fn connect_remembered_store(
        dsn: &str,
        namespace: String,
    ) -> PostgresRememberedEntityStore {
        let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.unwrap();
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                eprintln!("postgres remembered entity connection error: {error}");
            }
        });
        PostgresRememberedEntityStore::builder(client)
            .with_namespace(namespace)
            .migrate()
            .await
            .unwrap()
    }

    fn coordinator_states(
        entity_type: EntityType,
    ) -> (
        PersistedShardCoordinatorState,
        PersistedShardCoordinatorState,
    ) {
        let local = node("rakka-0", "uid-a");
        let remote = node("rakka-1", "uid-b");
        let remote_id = remote.id().clone();
        let mut membership = membership_with_up_nodes(vec![local, remote]);
        let config = ShardingConfig::new(4).unwrap();
        let mut coordinator = ShardCoordinator::new(entity_type.clone(), config);

        coordinator.reconcile(&membership);
        let first = PersistedShardCoordinatorState::now(
            coordinator.snapshot(),
            coordinator.allocation_strategy_name(),
        );

        membership.mark_down(&remote_id, 3).unwrap();
        coordinator.reconcile(&membership);
        let second = PersistedShardCoordinatorState::now(
            coordinator.snapshot(),
            coordinator.allocation_strategy_name(),
        );

        (first, second)
    }

    fn region(entity_type: EntityType, config: ShardingConfig) -> ShardRegion<TestCommand> {
        ShardRegion::new(
            entity_type,
            config,
            |_message: RoutedEntityMessage<TestCommand>| Ok(()),
        )
    }

    fn membership_with_up_nodes(nodes: Vec<ClusterNode>) -> ClusterMembership {
        let local = nodes[0].clone();
        let mut membership = ClusterMembership::new(
            local,
            MembershipConfig::new(1, Duration::from_millis(50), Duration::from_millis(100)),
        );

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

    fn node(logical_id: &str, incarnation: &str) -> ClusterNode {
        ClusterNode::new(
            NodeId::new(logical_id, incarnation),
            NodeAddress::new(format!("{logical_id}.rakka.default.svc"), 2552),
        )
    }

    fn unique_namespace(prefix: &str) -> String {
        format!(
            "{prefix}-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        )
    }
}
