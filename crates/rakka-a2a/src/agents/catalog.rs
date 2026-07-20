//! Hosted agent targets an A2A send can resolve to.
//!
//! The catalog answers one question: which `(AgentId, AgentTaskDefinition)`
//! pair serves this request. It is the agents-surface analog of the workflow
//! catalog — an application-owned, versioned resolution surface — and it
//! never resolves credentials; a target carries logical identity and the
//! typed task contract only.

use rakka_agent::{AgentId, AgentTaskDefinition};

/// One hosted agent plus the typed task definition it accepts over A2A.
#[derive(Debug, Clone)]
pub struct A2AAgentTarget {
    /// The agent that will be assigned created tasks.
    pub agent: AgentId,
    /// The versioned typed task contract created tasks use.
    pub definition: AgentTaskDefinition,
}

impl A2AAgentTarget {
    /// Creates a target.
    #[must_use]
    pub const fn new(agent: AgentId, definition: AgentTaskDefinition) -> Self {
        Self { agent, definition }
    }
}

/// Selection extracted from one request's metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct A2AAgentSelector<'a> {
    /// Requested agent id, when the request named one.
    pub agent: Option<&'a str>,
    /// Requested task-definition id, when the request named one.
    pub task_definition: Option<&'a str>,
}

impl A2AAgentSelector<'_> {
    /// True when the selector accepts the given target.
    #[must_use]
    pub fn matches(&self, target: &A2AAgentTarget) -> bool {
        if let Some(agent) = self.agent {
            if target.agent.as_str() != agent {
                return false;
            }
        }
        if let Some(definition) = self.task_definition {
            if target.definition.definition_id.as_str() != definition {
                return false;
            }
        }
        true
    }
}

/// Resolves which hosted agent target serves one A2A request.
pub trait A2AAgentCatalog: Send + Sync + 'static {
    /// Resolves a selector onto exactly one target, or `None` when no target
    /// (or more than one ambiguous target) matches.
    fn resolve(&self, selector: &A2AAgentSelector<'_>) -> Option<A2AAgentTarget>;

    /// Every hosted target, for agent-card/skill production.
    fn targets(&self) -> Vec<A2AAgentTarget>;
}

/// A fixed in-memory catalog over a bounded target list.
#[derive(Debug, Clone, Default)]
pub struct A2AStaticAgentCatalog {
    targets: Vec<A2AAgentTarget>,
}

impl A2AStaticAgentCatalog {
    /// Creates an empty catalog.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            targets: Vec::new(),
        }
    }

    /// A catalog hosting exactly one target.
    #[must_use]
    pub fn single(target: A2AAgentTarget) -> Self {
        Self {
            targets: vec![target],
        }
    }

    /// Adds a hosted target.
    #[must_use]
    pub fn with_target(mut self, target: A2AAgentTarget) -> Self {
        self.targets.push(target);
        self
    }
}

impl A2AAgentCatalog for A2AStaticAgentCatalog {
    fn resolve(&self, selector: &A2AAgentSelector<'_>) -> Option<A2AAgentTarget> {
        let mut matches = self
            .targets
            .iter()
            .filter(|target| selector.matches(target));
        let first = matches.next()?;
        // An ambiguous selection resolves to nothing rather than guessing.
        if matches.next().is_some() {
            return None;
        }
        Some(first.clone())
    }

    fn targets(&self) -> Vec<A2AAgentTarget> {
        self.targets.clone()
    }
}
