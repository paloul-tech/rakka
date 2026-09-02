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

/// Metadata key carrying a typed-result submission's declared contract
/// (specification 8.12): one structured object naming the task definition,
/// its revision, and the result schema the submission claims to fulfill.
///
/// A `message/send` naming an existing `task_id` and carrying this key is
/// the authenticated completion of a human-owned task; the message parts
/// carry the typed result itself. The object parses whole or fails the send
/// closed — a field this build does not serve is refused, never silently
/// dropped — and the entity re-validates every claim against its durable
/// definition.
pub const META_AGENT_RESULT: &str = "io.rakka.agent.result";

/// The declared contract of one typed-result submission, parsed whole from
/// [`META_AGENT_RESULT`].
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct AgentTaskResultBinding {
    /// The task-definition id the submission claims to fulfill.
    pub definition: String,
    /// The claimed revision of that definition.
    pub definition_version: u64,
    /// The schema the result is expressed in.
    pub result_schema: String,
    /// The claimed revision of that schema.
    pub result_schema_version: u64,
    /// The claimed evidence digest, when the submission carries one.
    /// Advisory for the deployment authorizer; the surface accepts no
    /// evidence artifacts yet.
    #[serde(default)]
    pub evidence_digest: Option<String>,
}

/// Parses the result binding when the key is present: present means it
/// parses whole, the collaboration extension's rule.
fn parse_result_binding(
    metadata: &HashMap<String, Value>,
) -> RakkaAgentA2AResult<Option<AgentTaskResultBinding>> {
    let Some(value) = metadata.get(META_AGENT_RESULT) else {
        return Ok(None);
    };
    serde_json::from_value(value.clone())
        .map(Some)
        .map_err(|_| {
            RakkaAgentA2AError::Mapping(A2AMappingError::InvalidMetadata {
                field: META_AGENT_RESULT.to_string(),
                reason: "the result binding must be one object with definition, \
                     definition-version, result-schema, and result-schema-version",
            })
        })
}

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
    /// The typed-result submission's declared contract, when the send
    /// carried [`META_AGENT_RESULT`] (specification 8.12). `None` for every
    /// other command.
    pub result: Option<AgentTaskResultBinding>,
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
    // The result binding parses at the same chokepoint. Two engagements on
    // one send are refused whole: a submission riding a collaboration
    // cluster is a half-formed engagement, not a message to route by
    // precedence.
    let result = parse_result_binding(metadata)?;
    if result.is_some() && collaboration.is_some() {
        return Err(RakkaAgentA2AError::Refused {
            code: "result-binding-conflicts-with-collaboration".to_string(),
            message: "a typed-result submission cannot ride a collaboration engagement".to_string(),
        });
    }
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
        Some(super::collaboration::AgentCollaborationEnvelope::Team(cluster)) => {
            // A team command is not a task continuation: the board task it
            // touches rides the cluster's own `task` field, and a send that
            // names `message.task_id` is a half-formed engagement refused
            // where it enters rather than routed ambiguously.
            if !matches!(intent, A2ATaskIntent::NewTask) {
                return Err(RakkaAgentA2AError::Refused {
                    code: "team-send-names-task".to_string(),
                    message: "a team command must not name message.task_id; the board task \
                              rides the cluster's task field"
                        .to_string(),
                });
            }
            let team = rakka_agent::AgentTeamId::new(&cluster.team)?;
            let kind = match cluster.operation {
                super::collaboration::AgentTeamWireOperation::Claim
                | super::collaboration::AgentTeamWireOperation::Release
                | super::collaboration::AgentTeamWireOperation::Transfer => {
                    AgentOperationKind::TeamClaim
                }
                super::collaboration::AgentTeamWireOperation::Message => {
                    AgentOperationKind::TeamMessage
                }
                super::collaboration::AgentTeamWireOperation::PostTask
                | super::collaboration::AgentTeamWireOperation::Join
                | super::collaboration::AgentTeamWireOperation::Leave => {
                    AgentOperationKind::TeamOperation
                }
            };
            // The team command deduplicates under the team's own scope, not
            // the synthesized placeholder task id, so two retries of one
            // board decision converge on one durable operation at the team
            // entity's inbox. The verb label is part of the identity: the
            // kinds above are operation *classes* shared by opposing verbs,
            // and the team's operation log answers by id alone — without the
            // verb segment, a caller that keys its commands with a stable
            // per-member or per-task deduplication key would have its Leave
            // answered with the Join's memoized outcome, or its Release
            // absorbed by the prior Claim's, a success-shaped reply for a
            // decision the board never made. The cluster digest is the other
            // half of that identity: the verb alone still lets one reused
            // key alias two *different* decisions under the same verb —
            // Claim(task A) absorbed by Claim(task B)'s memo — so the
            // decision's own content (task, member, target, epoch, body)
            // rides the id the way the conversation arm's coordinates and
            // the handoff arm's forced discriminator do. A pure retry
            // re-serializes to the same canonical digest and converges; a
            // different decision derives a different operation, whatever
            // deduplication key the caller reused.
            let cluster_digest =
                rakka_agent::AgentContentDigest::of_json(&cluster.to_value()).value;
            let operation_id = AgentOperationId::new(
                kind,
                [
                    tenant.as_str(),
                    team.as_str(),
                    cluster.operation.as_label(),
                    cluster_digest.as_str(),
                    discriminator.as_str(),
                ],
            )?;
            return Ok(NormalizedAgentCommand {
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
                agent: metadata_string(metadata, META_AGENT_ID)
                    .map_err(RakkaAgentA2AError::Mapping)?,
                task_definition: metadata_string(metadata, META_TASK_DEFINITION)
                    .map_err(RakkaAgentA2AError::Mapping)?,
                telemetry: extract_ingress_telemetry(metadata),
                collaboration,
                result: None,
            });
        }
        Some(super::collaboration::AgentCollaborationEnvelope::Conversation(cluster)) => {
            // A conversation command is not a task continuation: the
            // governing task is bound at creation, and a send that names
            // `message.task_id` is a half-formed engagement refused where
            // it enters rather than routed ambiguously.
            if !matches!(intent, A2ATaskIntent::NewTask) {
                return Err(RakkaAgentA2AError::Refused {
                    code: "conversation-send-names-task".to_string(),
                    message: "a conversation command must not name message.task_id; the \
                              governing task is bound at creation"
                        .to_string(),
                });
            }
            let conversation = rakka_agent::AgentConversationId::new(&cluster.conversation)?;
            fn cluster_field<'a>(
                value: Option<&'a str>,
                field: &'static str,
            ) -> RakkaAgentA2AResult<&'a str> {
                value
                    .filter(|value| !value.trim().is_empty())
                    .ok_or(RakkaAgentA2AError::Mapping(A2AMappingError::MissingField {
                        field,
                    }))
            }
            // The operation id derives from the decision's own logical
            // coordinates — never the wire discriminator — so a retried
            // send re-derives the same durable operation whatever its
            // message id, while a regenerated submission with different
            // content derives a new one that the entity's turn ledger
            // refuses loudly instead of silently absorbing.
            let operation_id = match cluster.operation {
                super::collaboration::AgentConversationWireOperation::SubmitTurn => {
                    let participant = cluster_field(
                        cluster.participant.as_deref(),
                        "io.rakka.collaboration.participant",
                    )?;
                    let round = cluster.round.ok_or(RakkaAgentA2AError::Mapping(
                        A2AMappingError::MissingField {
                            field: "io.rakka.collaboration.round",
                        },
                    ))?;
                    let turn = cluster.turn.ok_or(RakkaAgentA2AError::Mapping(
                        A2AMappingError::MissingField {
                            field: "io.rakka.collaboration.turn",
                        },
                    ))?;
                    let body =
                        cluster_field(cluster.body.as_deref(), "io.rakka.collaboration.body")?;
                    // The direction is part of the decision's content, so it
                    // is part of its identity: two turns with the same words
                    // that steer the protocol differently must not share an
                    // operation id. Mapped by the same helper the command
                    // build uses, so the two can never drift.
                    let direction = conversation_direction(cluster)?;
                    rakka_agent::conversation_turn_operation_id(
                        &tenant,
                        &conversation,
                        round,
                        turn,
                        &rakka_agent::AgentId::new(participant)?,
                        &rakka_agent::conversation_turn_content_digest(body, direction.as_ref()),
                    )?
                }
                super::collaboration::AgentConversationWireOperation::End => {
                    let round = cluster.expected_round.ok_or(RakkaAgentA2AError::Mapping(
                        A2AMappingError::MissingField {
                            field: "io.rakka.collaboration.expected-round",
                        },
                    ))?;
                    // The reason is part of the decision, so it is part of
                    // its identity: an end regenerated with different
                    // reasoning must not be absorbed as a duplicate of the
                    // one already recorded.
                    let reason = cluster.reason.as_deref().unwrap_or_default();
                    rakka_agent::conversation_end_operation_id(
                        &tenant,
                        &conversation,
                        round,
                        reason,
                    )?
                }
            };
            return Ok(NormalizedAgentCommand {
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
                agent: metadata_string(metadata, META_AGENT_ID)
                    .map_err(RakkaAgentA2AError::Mapping)?,
                task_definition: metadata_string(metadata, META_TASK_DEFINITION)
                    .map_err(RakkaAgentA2AError::Mapping)?,
                telemetry: extract_ingress_telemetry(metadata),
                collaboration,
                result: None,
            });
        }
        // A delegation envelope creates a child task; a continuation naming
        // `message.task_id` under it is a half-formed engagement refused
        // where it enters — the accidental fall-through into the parked
        // input path is closed for good.
        Some(super::collaboration::AgentCollaborationEnvelope::Delegation(_))
            if matches!(intent, A2ATaskIntent::ContinueTask) =>
        {
            return Err(RakkaAgentA2AError::Refused {
                code: "delegation-send-names-task".to_string(),
                message: "a delegation creates a child task; it must not name message.task_id"
                    .to_string(),
            });
        }
        // One arm, one kind per intent: a continuation is a typed-result
        // submission (specification 8.12) — there is deliberately no
        // normalize-time discrimination between "result" and "plain input";
        // the task entity decides by ownership. The kind split keeps a
        // submission's operation id from ever aliasing a creation's over
        // identical segments. A declared contract on a task-creating send is
        // a half-formed engagement, refused rather than silently dropped.
        _ => match intent {
            A2ATaskIntent::NewTask => {
                if result.is_some() {
                    return Err(RakkaAgentA2AError::Refused {
                        code: "result-submission-requires-task".to_string(),
                        message: "a typed-result submission must name the task it completes \
                                  via message.task_id"
                            .to_string(),
                    });
                }
                AgentOperationKind::TaskCreation
            }
            A2ATaskIntent::ContinueTask => AgentOperationKind::ResultSubmission,
        },
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
        result,
    })
}

/// Resolves the canonical tenant for a read that names no task.
///
/// The scoped reads of specification 17.13 and 17.18 address an entity or a
/// goal, not a task, so they cannot borrow `normalize_agent_cancel`'s
/// task-shaped normalization — doing so would mint a cancellation operation id
/// for a read and force a task id the caller never supplied.
///
/// # Errors
///
/// Fails when no tenant can be resolved from the request, the resolver, or the
/// configured default.
pub fn resolve_agent_tenant(
    resolver: &dyn A2ATenantResolver,
    default_tenant: Option<&str>,
    params: &ServiceParams,
    request_tenant: Option<&str>,
) -> RakkaAgentA2AResult<TenantId> {
    let (tenant, _source) = canonical_tenant(resolver, default_tenant, params, request_tenant)
        .map_err(RakkaAgentA2AError::Mapping)?;
    Ok(tenant)
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
        result: None,
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
pub(crate) fn extract_ingress_telemetry(
    metadata: &HashMap<String, Value>,
) -> AgentTelemetryContext {
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

/// Extracts the W3C trace context an ingress request carried in its **header**
/// map.
///
/// [`ServiceParams`] is the lowercase HTTP header / gRPC metadata map, which is
/// where W3C trace context canonically travels. The message metadata is the
/// carrier the agent domain propagates through durable state, and it is the one
/// [`extract_ingress_telemetry`] reads; this is the carrier a request that
/// carries no payload metadata at all still has — every read method, and any
/// send refused before its metadata could be merged. It is used for the ingress
/// span and nothing else: a read writes no durable state, so this cannot change
/// what any record carries.
///
/// Malformed context is dropped whole and sanitized, exactly as the metadata
/// carrier is.
#[must_use]
pub(crate) fn extract_header_telemetry(params: &ServiceParams) -> AgentTelemetryContext {
    let mut carrier = AgentAttributes::new();
    for key in [TRACEPARENT_HEADER, TRACESTATE_HEADER] {
        if let Some(value) = params
            .iter()
            .find(|(candidate, _)| candidate.eq_ignore_ascii_case(key))
            .and_then(|(_, values)| values.first())
        {
            carrier.insert(key.to_string(), value.clone());
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
        // A team command drives a board decision, never a task creation;
        // reaching here with the team cluster is the same routing bug.
        Some(super::collaboration::AgentCollaborationEnvelope::Team(_)) => {
            return Err(RakkaAgentA2AError::Unsupported {
                operation: "agent-collaboration",
                reason: "a team envelope cannot create a task",
            });
        }
        // A conversation command drives a turn-protocol decision, never a
        // task creation; reaching here with the conversation cluster is the
        // same routing bug.
        Some(super::collaboration::AgentCollaborationEnvelope::Conversation(_)) => {
            return Err(RakkaAgentA2AError::Unsupported {
                operation: "agent-collaboration",
                reason: "a conversation envelope cannot create a task",
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
            team: None,
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

/// Builds the deduplicated typed-result submission command for one
/// normalized continuation carrying the result binding
/// ([specification 8.12](../../../../docs/plans/rakka-agent/spec.md)).
///
/// The deduplication contract the caller signs up to: the discriminator —
/// the explicit `io.rakka.command.deduplication_key`, or the message id —
/// identifies one logical submission, and a retry under it converges on the
/// original decision, a recorded *rejection* included. A corrected
/// resubmission after a rejection is a new decision and must carry a new
/// key.
///
/// # Errors
///
/// Fails closed when the binding is absent (`io.rakka.agent.result` — there
/// is no plain-input path to fall back to), when no authenticated principal
/// rides the send (specification 8.12: an *authenticated* human or
/// service), or when the input exceeds the bounded inline content limit.
/// Every binding field is a claim the task entity re-validates against its
/// durable definition.
pub fn agent_task_result_command(
    normalized: &NormalizedAgentCommand,
    input: Value,
    causation: &str,
    now: rakka_agent_workflow::AgentTimestampMillis,
) -> RakkaAgentA2AResult<AgentTaskEntityCommand> {
    let Some(binding) = normalized.result.as_ref() else {
        return Err(RakkaAgentA2AError::Mapping(A2AMappingError::MissingField {
            field: "io.rakka.agent.result",
        }));
    };
    let Some(principal) = normalized.principal.as_ref() else {
        return Err(RakkaAgentA2AError::Mapping(A2AMappingError::MissingField {
            field: crate::mapping::META_PRINCIPAL_REF,
        }));
    };
    let submission = rakka_agent::AgentHumanResultSubmission {
        // The durable provenance label must stay unambiguous for
        // colon-bearing ids: the compact join makes ("user:a", "b") and
        // ("user", "a:b") byte-identical, so those switch to the canonical
        // JSON object while every colon-free principal keeps the exact
        // string existing records carry.
        principal: crate::mapping::principal_provenance_label(principal),
        definition_id: rakka_agent::AgentTaskDefinitionId::new(&binding.definition)?,
        definition_version: rakka_agent::AgentRevisionNumber::new(binding.definition_version),
        result_schema: rakka_agent::AgentSchemaRef::new(
            rakka_agent::AgentSchemaId::new(&binding.result_schema)?,
            rakka_agent::AgentRevisionNumber::new(binding.result_schema_version),
        ),
        content: AgentTaskContent::inline(input)?,
        // Structurally empty, and safe to be: a human-owned task definition
        // may not declare `EvidenceRequired`, so no rule the entity evaluates
        // can depend on what this surface cannot carry. The binding's
        // `evidence_digest` stays advisory for the deployment authorizer.
        // When the artifact strategy lands, this is where the artifacts
        // arrive and that definition guard lifts together with it.
        evidence: Vec::new(),
        causation_id: rakka_agent_workflow::AgentCausationId::new(causation),
        submitted_at: now,
    };
    Ok(AgentTaskEntityCommand::SubmitHumanResult {
        operation_id: normalized.operation_id.clone(),
        submission: Box::new(submission),
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

/// Builds the deduplicated team entity command for one normalized team send
/// ([specification 8.10](../../../../docs/plans/rakka-agent/spec.md)).
///
/// Every cluster field is a claim the team entity's transition re-validates;
/// a field the named operation requires but the cluster omits fails closed
/// here, before anything durable happens. Membership changes require an
/// authenticated principal — the management-write precedent — because their
/// provenance records who accepted them.
pub fn agent_team_command(
    normalized: &NormalizedAgentCommand,
    now: rakka_agent_workflow::AgentTimestampMillis,
) -> RakkaAgentA2AResult<(
    rakka_agent::AgentTeamScope,
    rakka_agent::AgentTeamEntityCommand,
)> {
    use rakka_agent::{AgentTeamEntityCommand, AgentTeamId, AgentTeamScope};

    use super::collaboration::{AgentCollaborationEnvelope, AgentTeamWireOperation};

    let Some(AgentCollaborationEnvelope::Team(cluster)) = normalized.collaboration.as_ref() else {
        return Err(RakkaAgentA2AError::Unsupported {
            operation: "agent-collaboration",
            reason: "the send carries no team envelope",
        });
    };
    let scope = AgentTeamScope::new(normalized.tenant.clone(), AgentTeamId::new(&cluster.team)?)?;
    let operation_id = normalized.operation_id.clone();

    let required = |value: &Option<String>, field: &'static str| -> RakkaAgentA2AResult<String> {
        value
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .map(ToString::to_string)
            .ok_or(RakkaAgentA2AError::Mapping(A2AMappingError::MissingField {
                field,
            }))
    };
    let board_task = |value: &Option<String>| -> RakkaAgentA2AResult<AgentTaskId> {
        Ok(AgentTaskId::new(required(
            value,
            "io.rakka.collaboration.task",
        )?)?)
    };
    let member_id = |value: &Option<String>,
                     field: &'static str|
     -> RakkaAgentA2AResult<rakka_agent::AgentId> {
        Ok(rakka_agent::AgentId::new(required(value, field)?)?)
    };
    let expected_epoch =
        cluster
            .expected_epoch
            .ok_or(RakkaAgentA2AError::Mapping(A2AMappingError::MissingField {
                field: "io.rakka.collaboration.expected-epoch",
            }));
    let lifecycle_revision =
        cluster
            .expected_lifecycle_revision
            .ok_or(RakkaAgentA2AError::Mapping(A2AMappingError::MissingField {
                field: "io.rakka.collaboration.expected-lifecycle-revision",
            }));
    // A membership change records who accepted it (specification 7.2); an
    // unauthenticated one fails closed exactly as a management write does.
    let provenance = || -> RakkaAgentA2AResult<Box<rakka_agent::AgentRevisionProvenance>> {
        let principal = normalized
            .principal
            .clone()
            .ok_or(RakkaAgentA2AError::Mapping(A2AMappingError::MissingField {
                field: "io.rakka.principal.ref",
            }))?;
        Ok(Box::new(super::management::management_provenance(
            principal,
            &normalized.discriminator,
            None,
            now,
        )))
    };

    let command = match cluster.operation {
        AgentTeamWireOperation::Claim => AgentTeamEntityCommand::Claim {
            operation_id,
            task: board_task(&cluster.task)?,
            member: member_id(&cluster.member, "io.rakka.collaboration.member")?,
            expected_epoch: expected_epoch?,
        },
        AgentTeamWireOperation::Release => AgentTeamEntityCommand::Release {
            operation_id,
            task: board_task(&cluster.task)?,
            member: member_id(&cluster.member, "io.rakka.collaboration.member")?,
            expected_epoch: expected_epoch?,
        },
        AgentTeamWireOperation::Transfer => AgentTeamEntityCommand::Transfer {
            operation_id,
            task: board_task(&cluster.task)?,
            member: member_id(&cluster.member, "io.rakka.collaboration.member")?,
            target: member_id(
                &cluster.target_member,
                "io.rakka.collaboration.target-member",
            )?,
            expected_epoch: expected_epoch?,
        },
        AgentTeamWireOperation::PostTask => AgentTeamEntityCommand::PostTask {
            operation_id,
            task: board_task(&cluster.task)?,
            posted_by: member_id(&cluster.member, "io.rakka.collaboration.member")?,
        },
        AgentTeamWireOperation::Message => AgentTeamEntityCommand::AppendMessage {
            operation_id,
            from: member_id(&cluster.member, "io.rakka.collaboration.member")?,
            // An absent target is the broadcast spelling; a *present* one is
            // a directed message, and a blank value fails closed like every
            // other blank field here instead of silently widening to the
            // broadcast — the authorizer was shown the raw directed claim,
            // and what was authorized and what executes must not diverge.
            to: match cluster.target_member.as_deref() {
                None => None,
                Some(_) => Some(member_id(
                    &cluster.target_member,
                    "io.rakka.collaboration.target-member",
                )?),
            },
            body: required(&cluster.body, "io.rakka.collaboration.body")?,
        },
        AgentTeamWireOperation::Join => {
            let mut capability_scopes = std::collections::BTreeSet::new();
            for scope in &cluster.capability_scopes {
                capability_scopes.insert(rakka_agent::AgentCapabilityId::new(scope)?);
            }
            AgentTeamEntityCommand::AddMember {
                operation_id,
                member: member_id(&cluster.member, "io.rakka.collaboration.member")?,
                capability_scopes,
                expected_lifecycle_revision: rakka_agent::AgentRevisionNumber::new(
                    lifecycle_revision?,
                ),
                provenance: provenance()?,
            }
        }
        AgentTeamWireOperation::Leave => AgentTeamEntityCommand::RemoveMember {
            operation_id,
            member: member_id(&cluster.member, "io.rakka.collaboration.member")?,
            expected_lifecycle_revision: rakka_agent::AgentRevisionNumber::new(lifecycle_revision?),
            provenance: provenance()?,
        },
    };
    Ok((scope, command))
}

/// Maps the conversation cluster's two direction spellings to the entity's
/// direction, failing closed on a payload carrying both.
///
/// One mapping serves both the operation-id derivation in
/// [`normalize_agent_send`] and the command build below, because the two must
/// agree: the id covers the direction, so a spelling one side honored and the
/// other dropped would put a turn's identity out of step with the decision it
/// names.
fn conversation_direction(
    cluster: &super::collaboration::AgentConversationCollaborationMetadata,
) -> RakkaAgentA2AResult<Option<rakka_agent::AgentConversationDirection>> {
    use rakka_agent::AgentConversationDirection;

    // The two spellings are mutually exclusive: a payload carrying both is a
    // half-formed engagement refused whole.
    match (cluster.designate.as_deref(), cluster.close_round) {
        (Some(_), Some(true)) => Err(RakkaAgentA2AError::Unsupported {
            operation: "agent-collaboration",
            reason: "a moderator turn cannot both designate and close the round",
        }),
        (Some(designated), _) => Ok(Some(AgentConversationDirection::Designate(
            rakka_agent::AgentId::new(designated)?,
        ))),
        (None, Some(true)) => Ok(Some(AgentConversationDirection::CloseRound)),
        (None, _) => Ok(None),
    }
}

/// Builds the deduplicated conversation entity command for one normalized
/// conversation send
/// ([specification 8.11](../../../../docs/plans/rakka-agent/spec.md)).
///
/// Every cluster field is a claim the conversation entity's transition
/// re-validates — the roster gate, the cursor's derived owner fence, and the
/// early end's moderator fence decide, never the wire; a field the named
/// operation requires but the cluster omits fails closed here, before
/// anything durable happens. The early end requires an authenticated
/// principal — the management-write precedent — because its provenance
/// records who accepted it, and names its claimed agent because only the
/// moderator's end terminalizes a conversation.
pub fn agent_conversation_command(
    normalized: &NormalizedAgentCommand,
    now: rakka_agent_workflow::AgentTimestampMillis,
) -> RakkaAgentA2AResult<(
    rakka_agent::AgentConversationScope,
    rakka_agent::AgentConversationEntityCommand,
)> {
    use rakka_agent::{
        AgentConversationEntityCommand, AgentConversationId, AgentConversationScope,
        AgentConversationTurnSubmit,
    };

    use super::collaboration::{AgentCollaborationEnvelope, AgentConversationWireOperation};

    let Some(AgentCollaborationEnvelope::Conversation(cluster)) = normalized.collaboration.as_ref()
    else {
        return Err(RakkaAgentA2AError::Unsupported {
            operation: "agent-collaboration",
            reason: "the send carries no conversation envelope",
        });
    };
    let scope = AgentConversationScope::new(
        normalized.tenant.clone(),
        AgentConversationId::new(&cluster.conversation)?,
    )?;
    let operation_id = normalized.operation_id.clone();

    let required = |value: &Option<String>, field: &'static str| -> RakkaAgentA2AResult<String> {
        value
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .map(ToString::to_string)
            .ok_or(RakkaAgentA2AError::Mapping(A2AMappingError::MissingField {
                field,
            }))
    };

    let command = match cluster.operation {
        AgentConversationWireOperation::SubmitTurn => {
            let round = cluster.round.ok_or(RakkaAgentA2AError::Mapping(
                A2AMappingError::MissingField {
                    field: "io.rakka.collaboration.round",
                },
            ))?;
            let turn =
                cluster
                    .turn
                    .ok_or(RakkaAgentA2AError::Mapping(A2AMappingError::MissingField {
                        field: "io.rakka.collaboration.turn",
                    }))?;
            let participant = rakka_agent::AgentId::new(required(
                &cluster.participant,
                "io.rakka.collaboration.participant",
            )?)?;
            let body = required(&cluster.body, "io.rakka.collaboration.body")?;
            let direction = conversation_direction(cluster)?;
            let mut usage = rakka_agent::AgentBudgetConsumption::zero();
            usage.tokens = cluster.tokens_consumed.unwrap_or(0);
            AgentConversationEntityCommand::SubmitTurn {
                operation_id,
                submit: Box::new(AgentConversationTurnSubmit {
                    round,
                    turn,
                    participant,
                    body,
                    direction,
                    usage,
                }),
            }
        }
        AgentConversationWireOperation::End => {
            let expected_round = cluster.expected_round.ok_or(RakkaAgentA2AError::Mapping(
                A2AMappingError::MissingField {
                    field: "io.rakka.collaboration.expected-round",
                },
            ))?;
            // An early end records who accepted it (specification 7.2); an
            // unauthenticated one fails closed exactly as a management
            // write does.
            let principal = normalized
                .principal
                .clone()
                .ok_or(RakkaAgentA2AError::Mapping(A2AMappingError::MissingField {
                    field: "io.rakka.principal.ref",
                }))?;
            // The end names its claimed agent in the same field a turn names
            // its speaker; specification 8.11 grants the early end to the
            // moderator alone, and the entity fences the claim against the
            // durable moderator.
            let moderator = rakka_agent::AgentId::new(required(
                &cluster.participant,
                "io.rakka.collaboration.participant",
            )?)?;
            AgentConversationEntityCommand::EndEarly {
                operation_id,
                moderator,
                expected_round,
                reason: cluster.reason.clone().unwrap_or_default(),
                provenance: Box::new(super::management::management_provenance(
                    principal,
                    &normalized.discriminator,
                    None,
                    now,
                )),
            }
        }
    };
    Ok((scope, command))
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
