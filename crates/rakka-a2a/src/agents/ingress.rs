//! Durable, deduplicated ingress normalization for the agents surface.
//!
//! Public A2A task identity equals [`AgentTaskId`] verbatim (specification
//! 14.2, resolved open decision 17): a send that names a task targets exactly
//! that entity, a send without one derives the id deterministically from the
//! tenant and the request's deduplication identity, and the tenant always
//! comes from the authenticated context — never parsed out of the id.
//!
//! Deduplication follows the substrate surface's derivation: the stable
//! operation id every entity command carries is built from the tenant, the
//! task id, and the request's deduplication discriminator (the explicit
//! `io.rakka.command.deduplication_key` metadata, or the A2A `message_id`).
//! Because a retried send reuses both its message id and therefore its
//! derived task id, the same durable operation reaches the same entity and
//! the entity's operation-id inbox answers with the original outcome — one
//! task, one run, one turn (specification 14.1, scenario 1).

use std::collections::HashMap;

use a2a::{Message, PartContent};
use a2a_server::ServiceParams;
use rakka_agent::{
    AgentGoalMode, AgentOperationId, AgentOperationKind, AgentTaskContent, AgentTaskCreation,
    AgentTaskEntityCommand, AgentTaskId, TenantId,
};
use rakka_agent_workflow::{
    extract_agent_trace_context, AgentAttributes, AgentTelemetryContext, PrincipalRef,
    TRACEPARENT_HEADER, TRACESTATE_HEADER,
};
use serde_json::Value;

use crate::mapping::{
    canonical_tenant, generated_task_id, metadata_string, principal_ref_from_value,
    require_non_blank, A2AMappingError, A2ATaskIntent, A2ATenantResolver, A2ATenantSource,
};

use super::catalog::{A2AAgentCatalog, A2AAgentSelector, A2AAgentTarget};
use super::error::{RakkaAgentA2AError, RakkaAgentA2AResult};

/// Metadata key selecting the target agent id.
pub const META_AGENT_ID: &str = "io.rakka.agent.id";

/// Metadata key selecting the typed task-definition id.
pub const META_TASK_DEFINITION: &str = "io.rakka.agent.task-definition";

/// Metadata key carrying an explicit durable deduplication key.
///
/// Same key as the substrate surface; when present it both deduplicates the
/// command and, for a send without a task id, derives the task id — so two
/// sends that share it converge on one task whatever their message ids.
pub const META_DEDUPLICATION_KEY: &str = crate::mapping::META_DEDUPLICATION_KEY;

/// Normalized identity for one agents-surface command request.
#[derive(Debug, Clone)]
pub struct NormalizedAgentCommand {
    /// Canonical tenant from the authenticated context.
    pub tenant: TenantId,
    /// Where the tenant came from.
    pub tenant_source: A2ATenantSource,
    /// The durable task identity; its value is the public A2A `Task.id`.
    pub task: AgentTaskId,
    /// Public grouping id. Opaque: defaults to the task's own id.
    pub context_id: String,
    /// New task versus continuation.
    pub intent: A2ATaskIntent,
    /// The stable operation id the entity command deduplicates on.
    pub operation_id: AgentOperationId,
    /// The request's deduplication discriminator (explicit metadata key or
    /// the A2A message id).
    pub discriminator: String,
    /// Authenticated principal reference, when supplied.
    pub principal: Option<PrincipalRef>,
    /// Requested agent selection, when the request named one.
    pub agent: Option<String>,
    /// Requested task-definition selection, when the request named one.
    pub task_definition: Option<String>,
    /// W3C trace context extracted from the request metadata *before* durable
    /// acceptance (specification 17.5, 17.6: the ingress `SERVER` span
    /// extracts context first). Sanitized at extraction: a malformed
    /// `traceparent` is dropped whole rather than entering durable state, and
    /// an untraced ingress starts a root.
    pub telemetry: AgentTelemetryContext,
    /// The validated collaboration engagement — delegation envelope or
    /// handoff cluster — when the send engaged the versioned collaboration
    /// extension (specification 14.4). `None` for an ordinary send.
    pub collaboration: Option<super::collaboration::AgentCollaborationEnvelope>,
}

impl NormalizedAgentCommand {
    /// The public A2A task id — the `AgentTaskId` value verbatim.
    #[must_use]
    pub fn public_task_id(&self) -> &str {
        self.task.as_str()
    }
}

/// Normalizes one `message/send` on the agents surface.
///
/// # Errors
///
/// Fails closed on a blank message id, an unresolved tenant, a task or
/// deduplication identity that cannot key a durable scope, or metadata with
/// the wrong shape. Nothing durable happens here.
pub fn normalize_agent_send(
    resolver: &dyn A2ATenantResolver,
    default_tenant: Option<&str>,
    params: &ServiceParams,
    request_tenant: Option<&str>,
    message: &Message,
    metadata: &HashMap<String, Value>,
) -> RakkaAgentA2AResult<NormalizedAgentCommand> {
    require_non_blank(&message.message_id, "message.message_id")
        .map_err(RakkaAgentA2AError::Mapping)?;
    let (tenant, tenant_source) =
        canonical_tenant(resolver, default_tenant, params, request_tenant)
            .map_err(RakkaAgentA2AError::Mapping)?;

    let discriminator = metadata_string(metadata, META_DEDUPLICATION_KEY)
        .map_err(RakkaAgentA2AError::Mapping)?
        .unwrap_or_else(|| message.message_id.clone());

    let (task, intent) = match message.task_id.as_deref() {
        Some(task_id) if !task_id.trim().is_empty() => {
            (AgentTaskId::new(task_id)?, A2ATaskIntent::ContinueTask)
        }
        Some(_) => {
            return Err(RakkaAgentA2AError::Mapping(A2AMappingError::MissingField {
                field: "message.task_id",
            }));
        }
        None => (
            AgentTaskId::new(generated_task_id(tenant.as_str(), &discriminator))?,
            A2ATaskIntent::NewTask,
        ),
    };

    let context_id = message
        .context_id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| task.as_str())
        .to_string();

    // The single chokepoint where the collaboration extension either parses
    // whole or fails the send closed (specification 14.4). Parsed before the
    // operation id derives, because a handoff is a distinct operation class:
    // it continues an existing task, never creates one, and deduplicates
    // under its own reserved kind.
    let collaboration = super::collaboration::parse_collaboration_envelope(message, metadata)?;
    let operation_kind = match collaboration.as_ref() {
        Some(super::collaboration::AgentCollaborationEnvelope::Handoff(cluster)) => {
            if !matches!(intent, A2ATaskIntent::ContinueTask) {
                return Err(RakkaAgentA2AError::Unsupported {
                    operation: "agent-collaboration",
                    reason: "a handoff send must name the task it transfers via message.task_id",
                });
            }
            // The cluster's handoff id doubles verbatim as the send's
            // deduplication identity — that binding is what makes one
            // transfer one durable operation. A send that deduplicates under
            // any other key could alias a different transfer onto a recorded
            // one (answering success for a transfer never recorded) or split
            // one transfer across operation ids, so the mismatch fails
            // closed before anything durable happens.
            if discriminator != cluster.handoff {
                return Err(RakkaAgentA2AError::Refused {
                    code: "handoff-identity-mismatch".to_string(),
                    message: format!(
                        "a handoff send must deduplicate under its handoff id {}, not {}",
                        cluster.handoff, discriminator
                    ),
                });
            }
            AgentOperationKind::Handoff
        }
        _ => AgentOperationKind::TaskCreation,
    };
    let operation_id = AgentOperationId::new(
        operation_kind,
        [tenant.as_str(), task.as_str(), discriminator.as_str()],
    )?;

    Ok(NormalizedAgentCommand {
        tenant,
        tenant_source,
        task,
        context_id,
        intent,
        operation_id,
        discriminator,
        principal: metadata
            .get(crate::mapping::META_PRINCIPAL_REF)
            .map(principal_ref_from_value)
            .transpose()
            .map_err(RakkaAgentA2AError::Mapping)?,
        agent: metadata_string(metadata, META_AGENT_ID).map_err(RakkaAgentA2AError::Mapping)?,
        task_definition: metadata_string(metadata, META_TASK_DEFINITION)
            .map_err(RakkaAgentA2AError::Mapping)?,
        telemetry: extract_ingress_telemetry(metadata),
        collaboration,
    })
}

/// Normalizes one `tasks/cancel` on the agents surface.
///
/// # Errors
///
/// Fails closed on an unresolved tenant or a task id that cannot key a
/// durable scope.
pub fn normalize_agent_cancel(
    resolver: &dyn A2ATenantResolver,
    default_tenant: Option<&str>,
    params: &ServiceParams,
    request_tenant: Option<&str>,
    task_id: &str,
    metadata: &HashMap<String, Value>,
) -> RakkaAgentA2AResult<NormalizedAgentCommand> {
    let (tenant, tenant_source) =
        canonical_tenant(resolver, default_tenant, params, request_tenant)
            .map_err(RakkaAgentA2AError::Mapping)?;
    let task = AgentTaskId::new(task_id)?;
    // Cancellation has no message id; it deduplicates on the explicit key
    // when one is supplied and is otherwise one logical operation per task —
    // the entity treats a repeated cancellation as the duplicate it is.
    let discriminator = metadata_string(metadata, META_DEDUPLICATION_KEY)
        .map_err(RakkaAgentA2AError::Mapping)?
        .unwrap_or_else(|| "a2a-cancel".to_string());
    let operation_id = AgentOperationId::new(
        AgentOperationKind::Cancellation,
        [tenant.as_str(), task.as_str(), discriminator.as_str()],
    )?;
    let context_id = task.as_str().to_string();

    Ok(NormalizedAgentCommand {
        tenant,
        tenant_source,
        task,
        context_id,
        intent: A2ATaskIntent::ContinueTask,
        operation_id,
        discriminator,
        principal: metadata
            .get(crate::mapping::META_PRINCIPAL_REF)
            .map(principal_ref_from_value)
            .transpose()
            .map_err(RakkaAgentA2AError::Mapping)?,
        agent: None,
        task_definition: None,
        telemetry: extract_ingress_telemetry(metadata),
        collaboration: None,
    })
}

/// Extracts the W3C trace context an ingress request carried, before anything
/// durable happens (specification 17.5).
///
/// The request metadata is the carrier: the standard lowercase `traceparent`
/// and `tracestate` keys, matched case-insensitively the way any W3C text-map
/// extraction is. Malformed context is dropped whole — the public protocol
/// policy is permissive acceptance, and a malformed value never enters
/// durable state — and the result is sanitized exactly as every durable
/// telemetry write is, so an untraced or untrusted ingress yields the empty
/// context and every segment it causes starts a root.
fn extract_ingress_telemetry(metadata: &HashMap<String, Value>) -> AgentTelemetryContext {
    let mut carrier = AgentAttributes::new();
    for key in [TRACEPARENT_HEADER, TRACESTATE_HEADER] {
        if let Some(value) = metadata
            .iter()
            .find(|(candidate, _)| candidate.eq_ignore_ascii_case(key))
            .and_then(|(_, value)| value.as_str())
        {
            carrier.insert(key.to_string(), value.to_string());
        }
    }
    match extract_agent_trace_context(&carrier) {
        Ok(Some(context)) => rakka_agent::sanitize_agent_telemetry_context(context),
        Ok(None) | Err(_) => AgentTelemetryContext::default(),
    }
}

/// The catalog selector this request expressed.
#[must_use]
pub fn agent_selector(normalized: &NormalizedAgentCommand) -> A2AAgentSelector<'_> {
    A2AAgentSelector {
        agent: normalized.agent.as_deref(),
        task_definition: normalized.task_definition.as_deref(),
    }
}

/// Maps the message body onto the typed task input.
///
/// Bounded and deliberately narrow for this surface: data parts pass their
/// JSON through, text parts become JSON strings, several parts become an
/// array. Binary and URL parts fail closed until the artifact strategy of
/// the substrate surface is adapted for agent tasks.
///
/// # Errors
///
/// Fails closed on an empty message body or a binary/URL part.
pub fn agent_task_input(message: &Message) -> RakkaAgentA2AResult<Value> {
    let mut values = Vec::with_capacity(message.parts.len());
    for part in &message.parts {
        match &part.content {
            PartContent::Text(text) => values.push(Value::String(text.clone())),
            PartContent::Data(data) => values.push(data.clone()),
            PartContent::Raw(_) | PartContent::Url(_) => {
                return Err(RakkaAgentA2AError::Unsupported {
                    operation: "send-message",
                    reason: "binary and url parts are not accepted by the agents surface",
                });
            }
        }
    }
    match values.len() {
        0 => Err(RakkaAgentA2AError::Mapping(A2AMappingError::MissingField {
            field: "message.parts",
        })),
        1 => Ok(values.remove(0)),
        _ => Ok(Value::Array(values)),
    }
}

/// Builds the deduplicated creation command for one normalized send.
///
/// # Errors
///
/// Fails closed when the input exceeds the bounded inline content limit.
pub fn agent_task_create_command(
    normalized: &NormalizedAgentCommand,
    target: &A2AAgentTarget,
    input: Value,
) -> RakkaAgentA2AResult<AgentTaskEntityCommand> {
    // A collaboration send binds the child to its delegation graph: the
    // validated envelope becomes the child's recorded provenance, its parent
    // binding, and its goal binding (specification 14.4). The escrow stays
    // `None` either way — a conserved budget grant cannot ride A2A, so the
    // envelope's budget is advisory provenance and the child's ledger builds
    // from its own definition ceilings.
    let (goal, parent, delegation) = match normalized.collaboration.as_ref() {
        Some(super::collaboration::AgentCollaborationEnvelope::Delegation(envelope)) => {
            let provenance = envelope.to_provenance()?;
            (
                envelope.goal_id()?,
                Some(provenance.parent_task.clone()),
                Some(Box::new(provenance)),
            )
        }
        // A handoff never creates a task: the same `AgentTaskId` is the whole
        // point. Reaching a creation with the handoff cluster is a routing
        // bug fail-closed, not a silent plain task.
        Some(super::collaboration::AgentCollaborationEnvelope::Handoff(_)) => {
            return Err(RakkaAgentA2AError::Unsupported {
                operation: "agent-collaboration",
                reason: "a handoff envelope cannot create a task",
            });
        }
        None => (None, None, None),
    };
    Ok(AgentTaskEntityCommand::Create {
        operation_id: normalized.operation_id.clone(),
        creation: Box::new(AgentTaskCreation {
            definition: target.definition.clone(),
            input: AgentTaskContent::inline(input)?,
            assignee: Some(target.agent.clone()),
            goal,
            // An A2A message creates a finite unit of work; a continuous root
            // control task is instituted by the goal surface, never by
            // ingress — and never an epoch, whose creation only the admitting
            // controller owes.
            goal_mode: AgentGoalMode::Finite,
            goal_spec: None,
            parent,
            dependencies: Vec::new(),
            escrow: None,
            wake: None,
            delegation,
            telemetry: normalized.telemetry.clone(),
        }),
    })
}

/// Builds the deduplicated handoff command for one normalized send carrying
/// the handoff cluster ([specification 8.9](../../../../docs/plans/rakka-agent/spec.md)).
///
/// # Errors
///
/// Fails closed when the normalized send carries no handoff cluster, or when
/// a cluster identity cannot key a durable scope. Every field is a claim the
/// task's transition re-validates against durable state.
pub fn agent_task_handoff_command(
    normalized: &NormalizedAgentCommand,
) -> RakkaAgentA2AResult<AgentTaskEntityCommand> {
    let Some(super::collaboration::AgentCollaborationEnvelope::Handoff(envelope)) =
        normalized.collaboration.as_ref()
    else {
        return Err(RakkaAgentA2AError::Unsupported {
            operation: "agent-collaboration",
            reason: "the send carries no handoff envelope",
        });
    };
    Ok(AgentTaskEntityCommand::RecordHandoff {
        operation_id: normalized.operation_id.clone(),
        request: Box::new(envelope.to_request()?),
    })
}

/// Builds the deduplicated cancellation command for one normalized cancel.
#[must_use]
pub fn agent_task_cancel_command(
    normalized: &NormalizedAgentCommand,
    reason: &str,
) -> AgentTaskEntityCommand {
    AgentTaskEntityCommand::Cancel {
        operation_id: normalized.operation_id.clone(),
        reason: reason.to_string(),
    }
}

/// Resolves the catalog target for one normalized send, failing closed when
/// nothing (or more than one thing) matches.
///
/// # Errors
///
/// Returns [`RakkaAgentA2AError::UnknownAgent`] when resolution fails.
pub fn resolve_agent_target(
    catalog: &dyn A2AAgentCatalog,
    normalized: &NormalizedAgentCommand,
) -> RakkaAgentA2AResult<A2AAgentTarget> {
    catalog
        .resolve(&agent_selector(normalized))
        .ok_or_else(|| RakkaAgentA2AError::UnknownAgent {
            agent: normalized.agent.clone(),
            task_definition: normalized.task_definition.clone(),
        })
}

/// Resolves the catalog target a handoff cluster names, failing closed when
/// this surface does not serve it — the same gate
/// [`resolve_agent_target`] places on a creation send, keyed off the
/// cluster's own target claim rather than the request's selection metadata.
///
/// # Errors
///
/// Returns [`RakkaAgentA2AError::UnknownAgent`] when the named target is not
/// a hosted agent of this surface.
pub fn resolve_handoff_target(
    catalog: &dyn A2AAgentCatalog,
    cluster: &super::collaboration::AgentHandoffCollaborationMetadata,
) -> RakkaAgentA2AResult<A2AAgentTarget> {
    catalog
        .resolve(&A2AAgentSelector {
            agent: Some(&cluster.target_agent),
            task_definition: Some(&cluster.target_task_definition),
        })
        .ok_or_else(|| RakkaAgentA2AError::UnknownAgent {
            agent: Some(cluster.target_agent.clone()),
            task_definition: Some(cluster.target_task_definition.clone()),
        })
}

#[cfg(test)]
mod tests {
    use a2a::{Part, Role};
    use serde_json::json;

    use crate::mapping::A2AHeaderTenantResolver;

    use super::*;

    const RESOLVER: A2AHeaderTenantResolver = A2AHeaderTenantResolver;

    fn normalize(message: &Message) -> RakkaAgentA2AResult<NormalizedAgentCommand> {
        normalize_agent_send(
            &RESOLVER,
            Some("tenant-a"),
            &ServiceParams::new(),
            None,
            message,
            &HashMap::new(),
        )
    }

    #[test]
    fn a_retried_send_derives_the_same_task_and_operation() {
        let mut message = Message::new(Role::User, vec![Part::text("hello")]);
        message.message_id = "msg-1".to_string();
        let first = normalize(&message).expect("normalize");
        let retry = normalize(&message).expect("normalize retry");
        assert_eq!(first.task, retry.task);
        assert_eq!(first.operation_id, retry.operation_id);
        assert!(matches!(first.intent, A2ATaskIntent::NewTask));
    }

    #[test]
    fn ingress_extracts_valid_trace_context_and_drops_malformed_whole() {
        let mut message = Message::new(Role::User, vec![Part::text("hello")]);
        message.message_id = "msg-1".to_string();

        let traced = HashMap::from([
            (
                // Case-insensitive, the way any W3C text-map extraction is.
                "TraceParent".to_string(),
                Value::String(
                    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string(),
                ),
            ),
            (
                "tracestate".to_string(),
                Value::String("vendor=value".to_string()),
            ),
        ]);
        let normalized = normalize_agent_send(
            &RESOLVER,
            Some("tenant-a"),
            &ServiceParams::new(),
            None,
            &message,
            &traced,
        )
        .expect("normalize");
        assert_eq!(
            normalized.telemetry.trace_parent.as_deref(),
            Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"),
        );
        assert_eq!(
            normalized.telemetry.trace_state.as_deref(),
            Some("vendor=value")
        );

        // The extracted context rides the creation command it derived.
        let definition = rakka_agent::AgentTaskDefinition::new(
            rakka_agent::AgentTaskDefinitionId::new("resolve-ticket").expect("definition id"),
            "Resolve one ticket.",
            rakka_agent::AgentSchemaRef::new(
                rakka_agent::AgentSchemaId::new("in").expect("schema id"),
                rakka_agent::AgentRevisionNumber::INITIAL,
            ),
            rakka_agent::AgentSchemaRef::new(
                rakka_agent::AgentSchemaId::new("out").expect("schema id"),
                rakka_agent::AgentRevisionNumber::INITIAL,
            ),
        )
        .expect("the definition is valid");
        let command = agent_task_create_command(
            &normalized,
            &A2AAgentTarget::new(
                rakka_agent::AgentId::new("support").expect("agent id"),
                definition,
            ),
            json!({ "ticket": 1 }),
        )
        .expect("the creation command builds");
        let AgentTaskEntityCommand::Create { creation, .. } = command else {
            panic!("a send builds a creation");
        };
        assert_eq!(creation.telemetry, normalized.telemetry);

        // Malformed context is dropped whole, and the send is not refused
        // over telemetry.
        let malformed = HashMap::from([(
            "traceparent".to_string(),
            Value::String("not-a-traceparent".to_string()),
        )]);
        let normalized = normalize_agent_send(
            &RESOLVER,
            Some("tenant-a"),
            &ServiceParams::new(),
            None,
            &message,
            &malformed,
        )
        .expect("a malformed traceparent never fails the send");
        assert!(normalized.telemetry.trace_parent.is_none());
    }

    #[test]
    fn different_messages_derive_different_tasks() {
        let mut first = Message::new(Role::User, vec![Part::text("hello")]);
        first.message_id = "msg-1".to_string();
        let mut second = Message::new(Role::User, vec![Part::text("hello")]);
        second.message_id = "msg-2".to_string();
        assert_ne!(
            normalize(&first).expect("normalize").task,
            normalize(&second).expect("normalize").task,
        );
    }

    #[test]
    fn an_explicit_deduplication_key_converges_across_message_ids() {
        let metadata = HashMap::from([(
            META_DEDUPLICATION_KEY.to_string(),
            Value::String("order-42".to_string()),
        )]);
        let mut first = Message::new(Role::User, vec![Part::text("hello")]);
        first.message_id = "msg-1".to_string();
        let mut second = Message::new(Role::User, vec![Part::text("hello")]);
        second.message_id = "msg-2".to_string();
        let normalize = |message: &Message| {
            normalize_agent_send(
                &RESOLVER,
                Some("tenant-a"),
                &ServiceParams::new(),
                None,
                message,
                &metadata,
            )
            .expect("normalize")
        };
        let first = normalize(&first);
        let second = normalize(&second);
        assert_eq!(first.task, second.task);
        assert_eq!(first.operation_id, second.operation_id);
    }

    #[test]
    fn an_explicit_task_id_is_the_agent_task_id_verbatim() {
        let mut message = Message::new(Role::User, vec![Part::text("hello")]);
        message.message_id = "msg-1".to_string();
        message.task_id = Some("task-abc".to_string());
        let normalized = normalize(&message).expect("normalize");
        assert_eq!(normalized.task.as_str(), "task-abc");
        assert_eq!(normalized.public_task_id(), "task-abc");
        assert!(matches!(normalized.intent, A2ATaskIntent::ContinueTask));
    }

    #[test]
    fn a_task_id_that_cannot_key_a_scope_fails_closed() {
        let mut message = Message::new(Role::User, vec![Part::text("hello")]);
        message.message_id = "msg-1".to_string();
        message.task_id = Some("bad/id".to_string());
        assert!(matches!(
            normalize(&message),
            Err(RakkaAgentA2AError::Identity(_))
        ));
    }

    #[test]
    fn context_id_defaults_to_the_task_id_and_stays_opaque() {
        let mut message = Message::new(Role::User, vec![Part::text("hello")]);
        message.message_id = "msg-1".to_string();
        let defaulted = normalize(&message).expect("normalize");
        assert_eq!(defaulted.context_id, defaulted.task.as_str());

        message.context_id = Some("grouping-7".to_string());
        let supplied = normalize(&message).expect("normalize");
        assert_eq!(supplied.context_id, "grouping-7");
        assert_eq!(supplied.task, defaulted.task);
    }

    #[test]
    fn message_parts_map_onto_typed_input() {
        let mut message = Message::new(Role::User, vec![Part::text("hello")]);
        message.message_id = "msg-1".to_string();
        assert_eq!(
            agent_task_input(&message).expect("text input"),
            Value::String("hello".to_string()),
        );

        let mut message = Message::new(
            Role::User,
            vec![Part {
                content: PartContent::Data(json!({"ticket": 1})),
                filename: None,
                media_type: None,
                metadata: None,
            }],
        );
        message.message_id = "msg-2".to_string();
        assert_eq!(
            agent_task_input(&message).expect("data input"),
            json!({"ticket": 1}),
        );

        let mut message = Message::new(Role::User, vec![Part::raw(vec![0xff])]);
        message.message_id = "msg-3".to_string();
        assert!(matches!(
            agent_task_input(&message),
            Err(RakkaAgentA2AError::Unsupported { .. })
        ));
    }
}
