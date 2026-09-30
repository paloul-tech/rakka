//! The communal knowledge-space scope: `(TenantId, KnowledgeSpaceId)`.
//!
//! Every claim, transition, and query in this crate is addressed through this
//! scope ([specification 13.4](../../../docs/plans/rakka-agent/spec.md)), and
//! the store contract makes a read under the wrong scope indistinguishable
//! from reading nothing (scenario 18). The scope resolves open decision 2
//! ([specification 21.3](../../../docs/plans/rakka-agent/spec.md)): the
//! default communal boundary is a tenant- or organization-scoped
//! [`KnowledgeSpaceId`], never an implicit cross-tenant graph — the tenant is
//! part of the key, so cross-tenant aliasing is unrepresentable, and
//! federation would be an explicit later design, not an accident of key
//! construction.

use std::fmt::{self, Display, Formatter};

use rakka_agent::{
    validate_tenant, AgentIdentityError, AgentIdentityResult, KnowledgeSpaceId, TenantId,
    AGENT_SCOPE_SEPARATOR,
};
use serde::de::Error as DeserializeError;
use serde::{Deserialize, Deserializer, Serialize};

/// Field name reported when a flattened knowledge-space key fails to parse.
const SCOPE_FIELD_KNOWLEDGE_SPACE: &str = "knowledge space scope";

/// Durable scope of one communal knowledge space:
/// `(TenantId, KnowledgeSpaceId)`
/// ([specification 6.8](../../../docs/plans/rakka-agent/spec.md)).
///
/// It serializes as its flattened key string, so a persisted scope is
/// re-validated and re-parsed on load rather than trusted field by field —
/// the same rule every `rakka-agent` composite scope follows.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KnowledgeSpaceScope {
    tenant: TenantId,
    space: KnowledgeSpaceId,
}

impl KnowledgeSpaceScope {
    /// Creates a knowledge-space scope, validating the tenant value.
    pub fn new(tenant: TenantId, space: KnowledgeSpaceId) -> AgentIdentityResult<Self> {
        validate_tenant(&tenant)?;
        Ok(Self { tenant, space })
    }

    /// Tenant boundary of this knowledge space.
    #[must_use]
    pub const fn tenant(&self) -> &TenantId {
        &self.tenant
    }

    /// Knowledge-space identity within the tenant.
    #[must_use]
    pub const fn space(&self) -> &KnowledgeSpaceId {
        &self.space
    }

    /// Flattened, injective key string for this scope.
    ///
    /// Injective because neither segment may contain the separator, which the
    /// identity validation of both components enforces.
    #[must_use]
    pub fn key(&self) -> String {
        let mut key = String::new();
        key.push_str(self.tenant.as_str());
        key.push(AGENT_SCOPE_SEPARATOR);
        key.push_str(self.space.as_str());
        key
    }

    /// Parses a flattened scope key, failing closed on a malformed value.
    pub fn parse(key: &str) -> AgentIdentityResult<Self> {
        let segments: Vec<&str> = key.split(AGENT_SCOPE_SEPARATOR).collect();
        let [tenant, space]: [&str; 2] =
            segments
                .as_slice()
                .try_into()
                .map_err(|_| AgentIdentityError::MalformedScopeKey {
                    field: SCOPE_FIELD_KNOWLEDGE_SPACE,
                    expected_segments: 2,
                    actual_segments: segments.len(),
                })?;
        Self::new(TenantId::new(tenant), KnowledgeSpaceId::new(space)?)
    }
}

impl Display for KnowledgeSpaceScope {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.key())
    }
}

impl Serialize for KnowledgeSpaceScope {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.key())
    }
}

impl<'de> Deserialize<'de> for KnowledgeSpaceScope {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let key = String::deserialize(deserializer)?;
        Self::parse(&key).map_err(DeserializeError::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope() -> KnowledgeSpaceScope {
        KnowledgeSpaceScope::new(
            TenantId::new("acme"),
            KnowledgeSpaceId::new("support-kb").expect("the space id is valid"),
        )
        .expect("the scope is valid")
    }

    #[test]
    fn the_key_round_trips_through_parse_and_serde() {
        let scope = scope();
        assert_eq!(scope.key(), "acme/support-kb");
        assert_eq!(
            KnowledgeSpaceScope::parse(&scope.key()).expect("the key parses"),
            scope
        );
        let json = serde_json::to_string(&scope).expect("the scope serializes");
        assert_eq!(json, "\"acme/support-kb\"");
        let restored: KnowledgeSpaceScope =
            serde_json::from_str(&json).expect("the scope deserializes");
        assert_eq!(restored, scope);
    }

    #[test]
    fn malformed_keys_fail_closed() {
        // Wrong segment count.
        assert_eq!(
            KnowledgeSpaceScope::parse("acme")
                .expect_err("one segment is refused")
                .code(),
            "malformed-scope-key"
        );
        assert_eq!(
            KnowledgeSpaceScope::parse("acme/kb/extra")
                .expect_err("three segments are refused")
                .code(),
            "malformed-scope-key"
        );
        // An invalid component is refused by its own validation, through serde
        // as well as parse.
        assert!(serde_json::from_str::<KnowledgeSpaceScope>("\"acme\"").is_err());
        assert!(serde_json::from_str::<KnowledgeSpaceScope>("\"/kb\"").is_err());
    }

    #[test]
    fn distinct_tenants_yield_distinct_keys_for_the_same_space_id() {
        let a = KnowledgeSpaceScope::new(
            TenantId::new("tenant-a"),
            KnowledgeSpaceId::new("shared").expect("the space id is valid"),
        )
        .expect("the scope is valid");
        let b = KnowledgeSpaceScope::new(
            TenantId::new("tenant-b"),
            KnowledgeSpaceId::new("shared").expect("the space id is valid"),
        )
        .expect("the scope is valid");
        assert_ne!(a.key(), b.key());
    }
}
