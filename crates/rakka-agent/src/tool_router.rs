//! Composes tool executors behind the dispatcher's single executor slot.
//!
//! The dispatcher holds one [`AgentDispatchToolExecutor`]; a deployment with
//! function tools, process tools, and remote MCP tools routes each call to the
//! executor that owns it by identity — an exact tool id, a tool-id prefix
//! (`mcp.` for every MCP binding), or the kind the registry declares for the
//! tool — and everything else reaches the fallback. Arguments never influence
//! routing, so a call cannot steer itself to another executor.
//!
//! Routing is not authority: which executor performs a call decides nothing
//! about whether it may be performed. Every dispatch still passes the
//! [`crate::dispatch::AgentDispatchAuthority`] gate before it reaches this
//! router.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use rakka_agent_workflow::AgentEphemeralCredential;

use crate::definition::AgentToolId;
use crate::dispatch::{AgentDispatchFuture, AgentDispatchToolExecutor};
use crate::effect::AgentRunEffect;
use crate::identity::AgentRunScope;
use crate::model::AgentToolCallRequest;
use crate::task::AgentTaskContent;
use crate::tools::{AgentToolKind, AgentToolRegistry};

/// One executor behind a route.
type Executor = Arc<dyn AgentDispatchToolExecutor>;

/// Routes tool calls to executors by tool identity, with a fallback.
pub struct AgentToolExecutorRouter {
    fallback: Executor,
    exact: BTreeMap<AgentToolId, Executor>,
    /// Kept sorted by descending prefix length, so the first match is the
    /// longest one.
    prefixes: Vec<(String, Executor)>,
    kinds: Vec<(AgentToolKind, AgentToolRegistry, Executor)>,
}

impl fmt::Debug for AgentToolExecutorRouter {
    // Route counts only: a tool id is deployment data, and the executors
    // behind the routes are opaque.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentToolExecutorRouter")
            .field("exact_routes", &self.exact.len())
            .field("prefix_routes", &self.prefixes.len())
            .field("kind_routes", &self.kinds.len())
            .finish_non_exhaustive()
    }
}

impl AgentToolExecutorRouter {
    /// A router whose every unmatched call reaches `fallback`.
    #[must_use]
    pub fn new(fallback: Executor) -> Self {
        Self {
            fallback,
            exact: BTreeMap::new(),
            prefixes: Vec::new(),
            kinds: Vec::new(),
        }
    }

    /// Routes every tool whose id starts with `prefix`; the longest matching
    /// prefix wins.
    ///
    /// An empty prefix is ignored rather than installed: it matches every tool
    /// id, so it would be a second catch-all sitting in front of the real
    /// fallback — and one whose precedence against the fallback nothing in the
    /// wiring makes visible. The fallback is the catch-all.
    ///
    /// Registering the same prefix twice keeps the first route: equal-length
    /// prefixes hold their registration order, so the later one is
    /// unreachable.
    #[must_use]
    pub fn with_prefix_route(mut self, prefix: impl Into<String>, executor: Executor) -> Self {
        let prefix = prefix.into();
        if prefix.is_empty() {
            return self;
        }
        self.prefixes.push((prefix, executor));
        self.prefixes.sort_by(|left, right| {
            right
                .0
                .len()
                .cmp(&left.0.len())
                .then_with(|| left.0.cmp(&right.0))
        });
        self
    }

    /// Routes one tool id exactly; an exact route beats every prefix.
    ///
    /// Registering the same tool twice keeps the last route: one id has one
    /// executor, and the later registration replaces the earlier.
    #[must_use]
    pub fn with_tool_route(mut self, tool: AgentToolId, executor: Executor) -> Self {
        self.exact.insert(tool, executor);
        self
    }

    /// Routes every tool the given registry declares with `kind`.
    ///
    /// The registry is the authority on what a tool *is*, so a tool it does
    /// not hold — or holds under another kind — is not this route's, whatever
    /// its id looks like.
    #[must_use]
    pub fn with_kind_route(
        mut self,
        kind: AgentToolKind,
        registry: AgentToolRegistry,
        executor: Executor,
    ) -> Self {
        self.kinds.push((kind, registry, executor));
        self
    }

    /// The executor one call reaches: exact id, then longest prefix, then
    /// declared kind, then the fallback.
    fn resolve(&self, tool: &AgentToolId) -> &Executor {
        if let Some(executor) = self.exact.get(tool) {
            return executor;
        }
        let id = tool.as_str();
        if let Some((_, executor)) = self
            .prefixes
            .iter()
            .find(|(prefix, _)| id.starts_with(prefix.as_str()))
        {
            return executor;
        }
        for (kind, registry, executor) in &self.kinds {
            if registry
                .binding(tool)
                .is_some_and(|binding| binding.descriptor().kind == *kind)
            {
                return executor;
            }
        }
        &self.fallback
    }
}

impl AgentDispatchToolExecutor for AgentToolExecutorRouter {
    fn execute<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        intent: &'a AgentRunEffect,
        call: &'a AgentToolCallRequest,
        credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentDispatchFuture<'a, AgentTaskContent> {
        self.resolve(&call.tool)
            .execute(scope, intent, call, credential)
    }
}
