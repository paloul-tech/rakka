//! The deciding identity of a failure.
//!
//! A failed effect is recorded under a **pipeline code**, which names the
//! dispatch step that failed: `guardrail-blocked`,
//! `credential-resolution-failed`, `dispatch-collaborator-failed`. That code
//! is what an application alerts on, and it does not say *which* decision
//! refused: which guardrail stage and under what reason, which resolver
//! condition, which executor or MCP code. [`AgentFailureReason`] is that
//! second fact, carried from the refusal to the outcome and recorded beside
//! the pipeline code wherever the run persists one.
//!
//! Three rules hold everywhere it appears:
//!
//! - **A code, never text.** The value is whatever the deciding party
//!   already answers under its own stable-code contract. No message, no
//!   detail, no evidence reference.
//! - **Observability, never correctness.** Nothing reads a reason to decide
//!   anything, which is why the records that gain one keep their schema
//!   versions: a record written before the field decodes with none, and a
//!   record without one serializes exactly as before.
//! - **Never a metric label.** A deployment's own codes are length-bounded,
//!   not value-bounded.

use serde::{Deserialize, Deserializer, Serialize};

use crate::definition::AgentGuardrailStageId;

/// Largest a reason code may be, in bytes.
///
/// The bound a guardrail reason and a dispatch failure code already have
/// (`AGENT_GUARDRAIL_REASON_MAX_LENGTH`,
/// `AGENT_DISPATCH_FAILURE_CODE_MAX_LENGTH`). A longer code is cut, which
/// leaves a string equal to no registered code — the honest outcome for
/// something that was never a stable identifier.
pub const AGENT_FAILURE_REASON_CODE_MAX_LENGTH: usize = 128;

/// Which decision failed an effect, beside the pipeline code that names the
/// step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentFailureReason {
    #[serde(deserialize_with = "bounded_on_decode")]
    code: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    stage: Option<AgentGuardrailStageId>,
}

impl AgentFailureReason {
    /// The reason a collaborator's own code gives, or `None` when the code
    /// is blank: a blank code identifies nothing.
    #[must_use]
    pub fn new(code: impl AsRef<str>) -> Option<Self> {
        let code = bounded(code.as_ref());
        if code.trim().is_empty() {
            return None;
        }
        Some(Self { code, stage: None })
    }

    /// The reason a guardrail stage gives. Always a reason, even under a
    /// blank code: the stage alone says which rule decided.
    #[must_use]
    pub fn guardrail(stage: AgentGuardrailStageId, reason_code: impl AsRef<str>) -> Self {
        Self {
            code: bounded(reason_code.as_ref()),
            stage: Some(stage),
        }
    }

    /// Names the guardrail stage that decided.
    #[must_use]
    pub fn with_stage(mut self, stage: AgentGuardrailStageId) -> Self {
        self.stage = Some(stage);
        self
    }

    /// The deciding party's own stable code.
    #[must_use]
    pub fn code(&self) -> &str {
        &self.code
    }

    /// The guardrail stage that decided, when a guardrail did.
    #[must_use]
    pub fn stage(&self) -> Option<&AgentGuardrailStageId> {
        self.stage.as_ref()
    }
}

/// One line, cut at [`AGENT_FAILURE_REASON_CODE_MAX_LENGTH`] on a character
/// boundary.
fn bounded(code: &str) -> String {
    let flat: String = code
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect();
    if flat.len() <= AGENT_FAILURE_REASON_CODE_MAX_LENGTH {
        return flat;
    }
    let mut end = AGENT_FAILURE_REASON_CODE_MAX_LENGTH;
    while end > 0 && !flat.is_char_boundary(end) {
        end -= 1;
    }
    flat[..end].to_string()
}

/// Bounds a decoded code exactly as a constructed one is bounded.
fn bounded_on_decode<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(bounded(&String::deserialize(deserializer)?))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{AgentFailureReason, AGENT_FAILURE_REASON_CODE_MAX_LENGTH};
    use crate::definition::AgentGuardrailStageId;

    fn stage() -> AgentGuardrailStageId {
        AgentGuardrailStageId::new("pii-filter").expect("id")
    }

    #[test]
    fn a_blank_code_is_no_reason() {
        assert_eq!(AgentFailureReason::new(""), None);
        assert_eq!(AgentFailureReason::new("  \t"), None);
    }

    #[test]
    fn a_code_is_bounded_on_a_character_boundary_and_kept_on_one_line() {
        let long = format!("{}é", "c".repeat(AGENT_FAILURE_REASON_CODE_MAX_LENGTH - 1));
        let reason = AgentFailureReason::new(&long).expect("a reason");
        assert_eq!(
            reason.code(),
            "c".repeat(AGENT_FAILURE_REASON_CODE_MAX_LENGTH - 1)
        );
        let broken = AgentFailureReason::new("vault\nlease").expect("a reason");
        assert_eq!(broken.code(), "vault lease");
    }

    #[test]
    fn a_collaborators_reason_serializes_without_a_stage() {
        let reason = AgentFailureReason::new("mcp-tool-error").expect("a reason");
        assert_eq!(reason.stage(), None);
        assert_eq!(
            serde_json::to_value(&reason).expect("encodes"),
            json!({"code": "mcp-tool-error"})
        );
    }

    #[test]
    fn a_guardrails_reason_carries_its_stage_and_round_trips() {
        let reason = AgentFailureReason::guardrail(stage(), "denied-substring");
        assert_eq!(reason.stage(), Some(&stage()));
        let encoded = serde_json::to_value(&reason).expect("encodes");
        assert_eq!(
            encoded,
            json!({"code": "denied-substring", "stage": "pii-filter"})
        );
        assert_eq!(
            serde_json::from_value::<AgentFailureReason>(encoded).expect("decodes"),
            reason
        );
    }

    #[test]
    fn a_decoded_code_is_bounded_too() {
        let long = "c".repeat(AGENT_FAILURE_REASON_CODE_MAX_LENGTH + 40);
        let decoded: AgentFailureReason =
            serde_json::from_value(json!({"code": long})).expect("decodes");
        assert_eq!(decoded.code().len(), AGENT_FAILURE_REASON_CODE_MAX_LENGTH);
    }
}
