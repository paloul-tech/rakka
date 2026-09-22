//! The run scope, the tool intent, and the observable artifact store the
//! executor's proofs drive.
//!
//! The scope and the intent mirror `rakka-agent`'s own integration fixtures
//! (`crates/rakka-agent/tests/common/mod.rs`): the effect is built exactly as
//! a run commits one, from an [`AgentEffectSpec`], so the `idempotency_key`
//! and `effect_id` a proof reads back are the derived values a real dispatch
//! would carry.
//!
//! [`SharedArtifactStore`] is here rather than `rakka-agent-workflow`'s
//! `FakeArtifactStore` for one reason: that store's `Clone` copies its map, so
//! a test holding a clone cannot see what the executor wrote through the
//! `McpArtifactStore` handle. This one shares its state behind an `Arc`, which
//! is what makes "the large result reached the store" assertable at all.

// Each integration-test binary compiles this module independently; what one
// binary leaves unused is not dead code.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::Arc;

use rakka_agent::{
    AgentEffectSpec, AgentRevisionNumber, AgentRunEffect, AgentRunEffectRequest, AgentRunScope,
    AgentToolCallId, AgentToolCallRequest, AgentToolId,
};
use rakka_agent_workflow::{
    AgentArtifactError, AgentArtifactRead, AgentArtifactStore, AgentArtifactStoreFuture,
    AgentArtifactWriteRequest, AgentTimestampMillis, ArtifactRef,
};
use tokio::sync::Mutex;

/// The tenant every fixture runs under.
pub const TENANT: &str = "acme";
/// The agent every fixture runs as.
pub const AGENT: &str = "support-agent";
/// The task every fixture's run was assigned from.
pub const TASK: &str = "ticket-1";

/// The run scope one attempt is dispatched under.
///
/// # Panics
///
/// When the fixture's own identifiers are invalid, which would make every
/// proof in the file meaningless.
#[must_use]
pub fn run_scope() -> AgentRunScope {
    let task = rakka_agent::AgentTaskId::new(TASK).expect("the task id is valid");
    let run =
        rakka_agent::run_id_for_assignment(&task, rakka_agent::AgentAssignmentGeneration::new(1))
            .expect("the run id is derivable");
    AgentRunScope::new(
        rakka_agent::TenantId::new(TENANT),
        rakka_agent::AgentId::new(AGENT).expect("the agent id is valid"),
        run,
    )
    .expect("the run scope is valid")
}

/// The model's call for one tool, with empty arguments.
///
/// # Panics
///
/// When the tool id or the arguments are invalid.
#[must_use]
pub fn tool_call(tool: &str) -> AgentToolCallRequest {
    AgentToolCallRequest::new(
        AgentToolCallId::new("call-1").expect("the call id is valid"),
        AgentToolId::new(tool).expect("the tool id is valid"),
        serde_json::json!({}),
    )
    .expect("the call is bounded")
}

/// The tool intent one attempt dispatches under: one read-only attempt of the
/// named tool, bounded by `timeout_ms`, exactly as a run commits it.
///
/// # Panics
///
/// When the effect cannot be derived from the fixture's own scope.
#[must_use]
pub fn tool_intent_with_timeout(tool: &str, timeout_ms: Option<u64>) -> AgentRunEffect {
    let mut spec = AgentEffectSpec::read_only();
    if let Some(timeout_ms) = timeout_ms {
        spec = spec.with_timeout_ms(timeout_ms);
    }
    AgentRunEffect::new(
        &run_scope(),
        1,
        0,
        AgentRunEffectRequest::Tool {
            call: Box::new(tool_call(tool)),
        },
        &spec,
        AgentRevisionNumber::INITIAL,
        AgentTimestampMillis::new(1),
    )
    .expect("the effect derives")
}

/// One stored artifact: what the reference says, and the bytes behind it.
type StoredArtifact = (ArtifactRef, Vec<u8>);

/// An in-memory artifact store whose state is shared by every clone, so a
/// proof can read back what the executor wrote through its own handle.
#[derive(Debug, Clone, Default)]
pub struct SharedArtifactStore {
    artifacts: Arc<Mutex<BTreeMap<String, StoredArtifact>>>,
}

impl SharedArtifactStore {
    /// How many artifacts have been written.
    pub async fn len(&self) -> usize {
        self.artifacts.lock().await.len()
    }

    /// Whether nothing has been written.
    pub async fn is_empty(&self) -> bool {
        self.artifacts.lock().await.is_empty()
    }

    /// One artifact's stored bytes, by artifact id.
    pub async fn bytes(&self, artifact_id: &str) -> Option<Vec<u8>> {
        self.artifacts
            .lock()
            .await
            .get(artifact_id)
            .map(|(_, bytes)| bytes.clone())
    }
}

impl AgentArtifactStore for SharedArtifactStore {
    fn put_artifact<'a>(
        &'a mut self,
        request: AgentArtifactWriteRequest,
    ) -> AgentArtifactStoreFuture<'a, ArtifactRef> {
        let artifacts = Arc::clone(&self.artifacts);
        Box::pin(async move {
            let mut held = artifacts.lock().await;
            let artifact_id = request
                .artifact_id
                .unwrap_or_else(|| format!("artifact-{}", held.len() + 1));
            let byte_len = u64::try_from(request.bytes.len()).unwrap_or(u64::MAX);
            let reference = ArtifactRef {
                artifact_id: artifact_id.clone(),
                kind: request.kind,
                uri: format!("memory://mcp-fixture/{artifact_id}"),
                checksum: request.checksum,
                content_type: request.content_type,
                byte_len: Some(byte_len),
                retention_class: request.retention_class,
                encryption: request.encryption,
                redaction: request.redaction,
                created_at: request.created_at,
                metadata: request.metadata,
            };
            held.insert(artifact_id, (reference.clone(), request.bytes));
            Ok(reference)
        })
    }

    fn get_artifact<'a>(
        &'a self,
        reference: &'a ArtifactRef,
    ) -> AgentArtifactStoreFuture<'a, AgentArtifactRead> {
        let artifacts = Arc::clone(&self.artifacts);
        let artifact_id = reference.artifact_id.clone();
        Box::pin(async move {
            let held = artifacts.lock().await;
            held.get(&artifact_id)
                .map(|(reference, bytes)| AgentArtifactRead {
                    reference: reference.clone(),
                    bytes: bytes.clone(),
                })
                .ok_or(AgentArtifactError::ArtifactNotFound { artifact_id })
        })
    }
}
