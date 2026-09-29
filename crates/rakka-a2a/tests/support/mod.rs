//! Guardrail stages the surface proofs install at the A2A boundaries.

// Each integration-test binary compiles this module independently; what one
// binary leaves unused is not dead code.
#![allow(dead_code)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use rakka_agent::{
    AgentGuardrail, AgentGuardrailBoundary, AgentGuardrailChain, AgentGuardrailContext,
    AgentGuardrailOutcome, AgentGuardrailStage, AgentGuardrailStageId, AgentGuardrailSubject,
    AgentRevisionNumber,
};
use serde_json::Value;

/// The text a blocking stage refuses, wherever in the view it appears.
pub const MARKER: &str = "IGNORE PREVIOUS";
/// What a redacting stage leaves in place of cluster text.
pub const REDACTED: &str = "[redacted]";

/// One chain at `boundary`, its stages named `stage-0`, `stage-1`, … in the
/// order given.
pub fn chain_at(
    boundary: AgentGuardrailBoundary,
    rules: Vec<Arc<dyn AgentGuardrail>>,
) -> AgentGuardrailChain {
    rules.into_iter().enumerate().fold(
        AgentGuardrailChain::new(AgentRevisionNumber::INITIAL),
        |chain, (index, rule)| {
            chain
                .with_stage(
                    AgentGuardrailStage::new(
                        AgentGuardrailStageId::new(format!("stage-{index}"))
                            .expect("the stage id is valid"),
                        AgentRevisionNumber::INITIAL,
                        rule,
                    )
                    .at_boundary(boundary),
                )
                .expect("the stage registers")
        },
    )
}

/// Allows everything and records what it was shown and about whom.
#[derive(Default)]
pub struct Recording {
    seen: AtomicUsize,
    subjects: Mutex<Vec<&'static str>>,
    views: Mutex<Vec<Value>>,
}

impl Recording {
    /// How many times the stage was evaluated.
    pub fn seen(&self) -> usize {
        self.seen.load(Ordering::SeqCst)
    }

    /// The kind of subject each evaluation named, in order.
    pub fn subjects(&self) -> Vec<&'static str> {
        self.subjects.lock().expect("the subject log").clone()
    }

    /// The view the last evaluation was shown.
    pub fn last_view(&self) -> Value {
        self.views
            .lock()
            .expect("the view log")
            .last()
            .cloned()
            .expect("the stage was evaluated")
    }
}

impl AgentGuardrail for Recording {
    fn evaluate(
        &self,
        context: &AgentGuardrailContext<'_>,
        content: &Value,
    ) -> AgentGuardrailOutcome {
        self.seen.fetch_add(1, Ordering::SeqCst);
        self.subjects
            .lock()
            .expect("the subject log")
            .push(match context.subject {
                AgentGuardrailSubject::Run(_) => "run",
                AgentGuardrailSubject::Task { agent: Some(_), .. } => "task-for-agent",
                AgentGuardrailSubject::Task { agent: None, .. } => "task",
                AgentGuardrailSubject::Team(_) => "team",
                AgentGuardrailSubject::Conversation(_) => "conversation",
                // The enum is `#[non_exhaustive]`: a subject a later version
                // adds is one no proof here asserts.
                _ => "other",
            });
        self.views
            .lock()
            .expect("the view log")
            .push(content.clone());
        AgentGuardrailOutcome::Allow
    }
}

/// Blocks a message whose view carries [`MARKER`] anywhere.
pub struct BlockMarker;

impl AgentGuardrail for BlockMarker {
    fn evaluate(&self, _: &AgentGuardrailContext<'_>, content: &Value) -> AgentGuardrailOutcome {
        if content.to_string().contains(MARKER) {
            AgentGuardrailOutcome::Block {
                reason_code: "prompt-injection".to_string(),
                evidence: None,
            }
        } else {
            AgentGuardrailOutcome::Allow
        }
    }
}

/// Rewrites every cluster text field the view carries to [`REDACTED`] and
/// leaves the parts alone.
pub struct RedactClusterText;

impl AgentGuardrail for RedactClusterText {
    fn evaluate(&self, _: &AgentGuardrailContext<'_>, content: &Value) -> AgentGuardrailOutcome {
        AgentGuardrailOutcome::Transform {
            content: rewritten(content, Value::String(REDACTED.to_string())),
            reason_code: "cluster-text-redacted".to_string(),
        }
    }
}

/// Sets every cluster text field the view carries to an explicit `null`.
pub struct ClearClusterText;

impl AgentGuardrail for ClearClusterText {
    fn evaluate(&self, _: &AgentGuardrailContext<'_>, content: &Value) -> AgentGuardrailOutcome {
        AgentGuardrailOutcome::Transform {
            content: rewritten(content, Value::Null),
            reason_code: "cluster-text-cleared".to_string(),
        }
    }
}

/// Sets every cluster text field the view carries to all whitespace.
pub struct BlankClusterText;

impl AgentGuardrail for BlankClusterText {
    fn evaluate(&self, _: &AgentGuardrailContext<'_>, content: &Value) -> AgentGuardrailOutcome {
        AgentGuardrailOutcome::Transform {
            content: rewritten(content, Value::String("   ".to_string())),
            reason_code: "cluster-text-blanked".to_string(),
        }
    }
}

/// The view with `body` and `reason` set to `value` wherever the
/// collaboration object carries a non-null one.
fn rewritten(content: &Value, value: Value) -> Value {
    let mut view = content.clone();
    if let Some(collaboration) = view.get_mut("collaboration").and_then(Value::as_object_mut) {
        for field in ["body", "reason"] {
            if collaboration.get(field).is_some_and(|held| !held.is_null()) {
                collaboration.insert(field.to_string(), value.clone());
            }
        }
    }
    view
}
