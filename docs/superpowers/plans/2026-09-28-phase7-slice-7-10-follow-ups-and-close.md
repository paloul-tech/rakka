# Phase 7 Slice 7.10: Follow-ups and Phase Close Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close Phase 7 after slice 7.7 and land what the three delivered slices left owing: MCP results over 2 KiB through a store a deployment can implement, a publish-time sync that refuses an echoed credential and is bounded in time, the deciding code and stage of a failed effect on the run's own records (issue #80), a checkpoint resolution that always leaves its trace, the guardrail transform and ingress residuals of slice 7.2, and the retry deadline proof slice 7.1 owes.

**Architecture:** Four independent parts on one branch. Part A changes only `rakka-agent-mcp`: a `&self` artifact sink that receives the run scope replaces the mutex-guarded store as the executor's write path, the sync refuses server text that carries the credential it sent, and both syncs run under a deadline. Part B follows the host's brief for issue #80: one small `AgentFailureReason { code, stage }` rides from the refusal to the outcome and lands, additively and unversioned, beside every pipeline code the run already persists; the collaborator's inner code becomes the reason of an exhausted outcome; and the `checkpoint-resolve` segment closes when the resolving transition commits rather than when the whole call returns. Part C tightens the model-response transform rule, refuses a transform that clears a required collaboration field, proves the four ingress leaves no test reaches, and adds the two-attempt deadline proof. Part D records everything and closes the phase.

**Tech Stack:** Rust 1.88 workspace; `rakka-agent`, `rakka-agent-workflow`, `rakka-agent-mcp` (`rmcp = "=3.4.0"`), `rakka-a2a` (feature `agents`); tokio; serde/serde_json. No new dependency.

**Spec:** `docs/superpowers/specs/2026-09-19-phase7-agent-surface-parity-design.md` — section 0 ("Phase cut (2026-09-28)"), 4.2 item 3 and 4.7 (Task 11), 5.2 and 5.3 (Tasks 1–3), 6.1 and 6.3 (Tasks 9–10), 11.1, 11.4, 12 (row 7.10, revised), 13 (revised). For Part B the requirement is the host's brief, filed as issue #80: `../paloul.rakka.host/docs/specs/2026-09-25-upstream-effect-failure-reason-code-brief.md` (read-only). Its own rule binds this plan: where the brief and the code at the tip disagree, the code wins and the difference goes in the report (Task 12 writes that report). For Part A the consumer's questions are in `../paloul.rakka.host/docs/specs/2026-09-24-upstream-slice-7-7-verification.md` section 5 (read-only).

## Global Constraints

- Base: `rakka-agents` at `ebc7147` (the merge of PR #79). Branch: `rakka-agents-phase7-slice-7-10`, created by Task 1's first commit. Never commit on `rakka-agents` itself.
- Every public item needs a doc comment (`missing_docs = "warn"`; validation runs clippy with `-D warnings`). `unsafe_code = "forbid"`. MSRV 1.88.
- Never persist or log a credential. A secret exists only as the `AgentEphemeralCredential` the dispatcher resolved for one attempt. No new field, message, or log line in this slice may carry credential material, tool arguments, or a response body. A reason **code** is a stable identifier; a reason is never free text.
- Stable codes are a compatibility surface. **No pipeline code changes**: `guardrail-blocked`, `checkpoint-required`, `credential-resolution-failed`, and `dispatch-collaborator-failed` stay what `code()` answers and what every record's `code` holds. New codes this slice registers (Task 12): `mcp-descriptor-credential-echoed`, plus the four guardrail reason codes that already exist as bare literals and become constants (`text-too-long`, `denied-substring`, `undeclared-tool-call`, `guardrail-transform-oversized`). No other new code.
- Every new durable field is `#[serde(default, skip_serializing_if = "Option::is_none")]`: a record written before the change decodes with `None`, and a record without the field serializes byte-identically to today. **No schema version is bumped** (decision D7).
- Every existing constructor and `with_*` builder keeps compiling at its current call sites. `McpDispatchToolExecutor::new` and `with_launcher` keep accepting an `McpArtifactStore`; `sync_mcp_descriptors` and `sync_mcp_descriptors_over` keep their argument lists.
- Bounds: a reason code is at most `AGENT_FAILURE_REASON_CODE_MAX_LENGTH = 128` bytes; a stage is an `AgentGuardrailStageId` (at most 256 bytes by construction); `MCP_SERVER_NAME_MAX_BYTES = 256`; `MCP_SYNC_TIMEOUT_DEFAULT_MS = 30_000`.
- The host repository `../paloul.rakka.host` is read-only. Never edit it.
- Test files: one concern per file under `crates/<crate>/tests/`; unit tests inline. Commit messages end with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`. If the harness adds a different co-author trailer, amend it before reporting. Never push or open a PR without the owner's go-ahead (Task 12 stops before that).
- On this machine: run tests per crate, give every cargo command an explicit 600000 ms timeout in the foreground, never end a turn with a cargo run in the background, redirect `scripts/validate.sh` to a file and read the real exit code from the file, and run `cargo clean` at every task boundary (the owner's rule; the controller runs it and records it in the ledger, never an implementer mid-task).

## Decisions this plan makes (confirm or overturn)

Each is the planner's ruling where the spec, the brief, and the tree left a choice. Tasks are independent enough that overturning one strikes or edits one task.

| # | Decision | Why | Cost if wrong |
| --- | --- | --- | --- |
| D1 | The MCP executor writes through a new `McpArtifactSink` (`&self`, `Send + Sync`, receives `&AgentRunScope`); an `McpArtifactStore` converts into it, so no existing call site changes | The host's store is `&self`, needs a tenant and a run key, and cannot implement upstream's `&mut self` trait | One trait and one newtype on an unpublished crate |
| D2 | The derived artifact id `mcp-<effect>-g<generation>-<call>` is a **request**; the sink may mint its own id. The returned reference is validated with `validate_artifact_ref` and the attempt fails under the artifact error's own code when it does not pass | The host's store is content-addressed and ignores a requested id; an unvalidated reference would be recorded and fail later on read | A store that returned a sloppy reference now fails the attempt it used to pass |
| D3 | Inline results and artifact references are measured as the run measures them (`AgentTaskContent::size_bytes()`, wrapper included) against the same 2 KiB | Today a result encoding to 2 038–2 048 bytes passes the executor and is then refused by the run as `effect-tool-result-too-large` | Eleven bytes of inline capacity |
| D4 | A sync whose server-chosen text carries the credential it sent is **refused** (`mcp-descriptor-credential-echoed`), not redacted | Redacting a schema breaks the SHA-256 digest the dispatch-time recheck and the release digest compare; a server echoing its bearer token into a listing is a fact the operator must see | A publish blocked by a misbehaving third-party server |
| D5 | `server_name` is cut at 256 bytes on a character boundary | It is server-chosen, stored as release data, and unbounded today | One constant |
| D6 | Both syncs run under a 30 s default; `_within` twins take an explicit bound. A timeout reads as the existing `mcp-descriptor-sync-failed` | No configuration may leave a network call unbounded (the 7.7 final review's rule for attempts); no new code | A very slow legitimate server needs the `_within` twin |
| D7 | Issue #80's fields land with **no schema version bump** | The brief asks for none; the fields are observability only and nothing reads them to decide anything, which is the `AgentRunEffect::telemetry` precedent. A downgrade re-persist may drop a reason, and the pipeline code beside it survives | A reason lost during a rolling downgrade |
| D8 | One struct, `AgentFailureReason { code, stage }`, as one field `reason` on each record, instead of two fields | The brief allows either; one field is one serde attribute and one pattern | Field naming differs from the brief's example |
| D9 | The handoff cell's `Failed` and `RakkaAgentA2AError::Refused` carry the reason too; the workflow-invocation cell does not | The code shows a handoff block lands on the handoff cell, never on `EffectFailed`, and an ingress block passes through `RakkaAgentA2AError`; the brief names neither. The workflow cell was not asked for | Two extra fields; one asymmetry |
| D10 | The A2A **wire** is unchanged: a remote caller still reads the pipeline code only | A guardrail stage id is a deployment's internal name | A federated peer cannot read the stage |
| D11 | The `checkpoint-resolve` segment closes when the transition committed **and dropped the checkpoint** | Today an `Escalate` answers `Applied`, leaves the checkpoint open, and closes a segment; the real resolution then closes a second one under the same span id | An escalation leaves no resolve segment (it resolved nothing) |
| D12 | A model-response transform may rewrite or drop a proposal; it may not **add** one (`guardrail-transform-invalid`), and a reference proposal survives only byte-identical (`guardrail-transform-unsupported`) | An invented proposal is an invented task result; a rewritten `uri` or `checksum` changes the digest the task fingerprints | A stage that relied on adding a proposal is refused |
| D13 | A transform that clears a **required** collaboration field with an explicit `null` is refused `guardrail-transform-invalid` | Today a cleared handoff reason silently keeps the original while the transform is logged as applied, and a cleared message body blames the caller with a mapping error | A stage that cleared one is refused instead of half-applied |
| D14 | The reduced acceptance statement (spec 13) is met by the three slices' own real-dispatcher proofs; no new acceptance example is built | The walk the spec named drives the mounted endpoint, SSE, the directory, and the builders, all cut | No single end-to-end walk across providers, MCP, and guardrails |

## Not built in this slice

Two items of the follow-up list need a design decision only the owner can make, because each adds durable state. Both are recorded in the spec as open (Task 12), with the facts a design starts from. Neither blocks any task here.

1. **MCP input-required mapped to a human checkpoint.** `AgentDispatchToolExecutor::execute` can answer only content or an error, and every error is a retryable attempt failure; a checkpoint decision carries approve or deny and no structured input; checkpoint ids collide across rounds of one generation; rmcp requires the server's `requestState` back byte-exact while Rakka scrubs the credential from every server-chosen text it keeps; a legacy (2025-11-25) server elicits on the live session, which a per-attempt client cannot hold open; and no public ingress sends `ResolveCheckpoint`. The mapping is a slice of its own: a new executor outcome, a new checkpoint kind with a structured answer, and a continuation record.
2. **`model-profile-revision-mismatch` enforced.** The durable model intent carries `profile: None`, the profile is re-selected from the agent's current settings on every attempt, and the profile digest is the FNV fingerprint a checkpoint binding refuses. Enforcing the code needs a durable pin. The alternative is to withdraw the code and state that the current catalog wins, with the envelope and revocation rechecked per attempt. Which is right depends on whether a release rollout should fail an in-flight model retry.

## Review Focus

The inputs the spec and the brief imply but never name, most likely to bite first. Each has its test in the task that owns the code.

1. **A store that is not a pass-through.** A content-addressed sink mints its own id, returns `redaction: Unknown`, or fails. Expected: the run records the sink's reference when it validates, and the attempt fails under the artifact error's own code when it does not. (Task 1, `artifact_sink.rs`.)
2. **A result on the 2 KiB edge.** A text result whose content encodes to exactly 2 048 bytes, and one byte more. Expected: the first is inline and the run accepts it; the second is refused or stored, never handed to a run that will refuse it. (Task 1.)
3. **A credential echoed where nobody looks.** The server puts the token inside a nested schema string, in its JSON-escaped spelling, or in its server name rather than a description. Expected: the sync refuses and its message carries neither the secret nor the server's text. (Task 2.)
4. **A record written before the change, and a downgrade.** An effect, a terminal reason, and a cell persisted without `reason`. Expected: each decodes to `None`, and a record with no reason serializes byte-identically to today. (Task 6.)
5. **A resolution that is not one.** An `Escalate` decision, a replayed resolve, and a resolve whose settle pass lost a compare-and-set. Expected: exactly one `checkpoint-resolve` segment per resolved checkpoint, on the call that committed it. (Task 8.)

## File structure

| File | Responsibility |
| --- | --- |
| `crates/rakka-agent-mcp/src/artifacts.rs` (new) | `McpArtifactSink`, `McpArtifactFuture`, `McpArtifacts`, the store-to-sink adapter |
| `crates/rakka-agent-mcp/src/executor.rs` | takes `impl Into<McpArtifacts>`; threads the scope to the write; kind, redaction, reference validation, the run's size rule |
| `crates/rakka-agent-mcp/src/sync.rs` | `CredentialEchoed`, the server-name bound, the deadline and the `_within` twins |
| `crates/rakka-agent-mcp/src/binding.rs` | `MCP_SERVER_NAME_MAX_BYTES`, `MCP_SYNC_TIMEOUT_DEFAULT_MS`; the inline bound's doc |
| `crates/rakka-agent-mcp/tests/artifact_sink.rs` (new) | Task 1's proofs |
| `crates/rakka-agent-mcp/tests/sync_credential_echo.rs` (new) | Task 2's proofs |
| `crates/rakka-agent-mcp/tests/sync_deadline.rs` (new) | Task 3's proofs |
| `crates/rakka-agent/src/failure.rs` (new) | `AgentFailureReason` and its bound |
| `crates/rakka-agent/src/effect.rs` | `reason` on `Failed`/`Exhausted`; `failed`/`exhausted`/`with_reason`; `last_error_reason` on the effect record |
| `crates/rakka-agent/src/tools.rs` | `AgentAuthorityRefusal.reason`; `refuse_guardrail_disposition` fills it; the proposal transform rule |
| `crates/rakka-agent/src/dispatch.rs` | findings carry the reason; every refusal-to-outcome mapping passes it on; the collaborator's inner code as the exhausted reason |
| `crates/rakka-agent/src/run.rs` | writes the reason beside each pipeline code; the resolve segment's commit rule |
| `crates/rakka-agent/src/delegation.rs`, `coordination.rs` | `reason` on the two cells' `Failed` |
| `crates/rakka-agent/src/guardrails/builtin.rs`, `guardrails.rs` | the four reason-code constants |
| `crates/rakka-agent/src/testkit.rs` | outcome literals become constructors; `CrashPoint::ConflictBeforeWrite` |
| `crates/rakka-a2a/src/agents/{error,service,delegation,handoff,guardrails}.rs` | the reason through the A2A findings; the required-field rule |
| `crates/rakka-a2a/Cargo.toml`, `scripts/validate.sh` | the four test files gated; the guard line |
| `crates/rakka-a2a/tests/support/mod.rs` (new) | the guardrail stages the surface proofs install |
| `crates/rakka-agent/src/conversation.rs`, `team.rs` | the operation-id docs |
| `crates/rakka-agent/src/query.rs`, `examples/durable-agent-acceptance/src/provider.rs` | one pattern; one comment |
| `crates/rakka-agent/tests/failure_reason_records.rs` (new) | Task 6's record proofs |
| `crates/rakka-agent/tests/secret_exclusion.rs`, `mcp_client_dispatch.rs` | Task 7's proofs |
| `crates/rakka-agent/tests/tool_authority.rs`, `delegation_dispatch.rs`, `handoff_record.rs` | Task 6's end-to-end proofs |
| `crates/rakka-agent/tests/checkpoint_resolve_segment.rs` (new), `checkpoint_reconciliation.rs` | Task 8's proofs |
| `crates/rakka-agent/tests/model_response_guardrails.rs` | Tasks 5, 6, and 9's proofs |
| `crates/rakka-a2a/tests/{team_surface,conversation_surface,handoff_surface,human_task_surface}.rs` | Task 10's proofs |
| `crates/rakka-a2a/tests/ingress_egress_guardrails.rs` | Task 5's finding-level proofs |
| `crates/rakka-agent/tests/model_provider_dispatch.rs` | Task 11's proof |
| `docs/*.md`, `CHANGELOG.md`, the spec, `docs/plans/rakka-agent/spec.md` | Task 12 |

---

# Part A — MCP follow-ups

### Task 1: The artifact sink, and results measured as the run measures them

**Files:**
- Create: `crates/rakka-agent-mcp/src/artifacts.rs`
- Modify: `crates/rakka-agent-mcp/src/executor.rs` (the struct field at :149, `new` :194, `with_launcher` :216, `build` :236, `attempt` :366, `call` :487, `content` :580, `bounded` :626, imports :52-57)
- Modify: `crates/rakka-agent-mcp/src/binding.rs:42-43` (the inline bound's doc)
- Modify: `crates/rakka-agent-mcp/src/lib.rs` (module map, re-exports)
- Create: `crates/rakka-agent-mcp/tests/artifact_sink.rs`

**Interfaces:**
- Consumes: `rakka_agent_workflow::{AgentArtifactError, AgentArtifactStore, AgentArtifactWriteRequest, ArtifactRef, ArtifactKind, RedactionStatus, validate_artifact_ref}`; `rakka_agent::{AgentRunScope, AgentTaskContent}`; the crate's own `McpArtifactStore` and `mcp_artifact_store`, which stay where they are in `executor.rs` (the reference launcher reads launch specifications through them).
- Produces, all re-exported from the crate root:
  - `pub type McpArtifactFuture<'a> = Pin<Box<dyn Future<Output = Result<ArtifactRef, AgentArtifactError>> + Send + 'a>>;`
  - `pub trait McpArtifactSink: Send + Sync { fn put_result<'a>(&'a self, scope: &'a AgentRunScope, request: AgentArtifactWriteRequest) -> McpArtifactFuture<'a>; }`
  - `pub struct McpArtifacts` with `pub fn sink<S: McpArtifactSink + 'static>(sink: S) -> Self`, `impl From<McpArtifactStore> for McpArtifacts`, `impl From<Arc<dyn McpArtifactSink>> for McpArtifacts`
  - `McpDispatchToolExecutor::new(descriptors, bindings, artifacts: impl Into<McpArtifacts>, http, egress)` and `with_launcher(.., artifacts: impl Into<McpArtifacts>, .., launcher)`. Every existing call site passes an `McpArtifactStore` and compiles unchanged.

- [ ] **Step 1: Create the branch**

```bash
git checkout -b rakka-agents-phase7-slice-7-10
git status --porcelain
```

Expected: the branch is created. `git status` lists exactly two paths, both left by the planner and both committed by Task 12, not by this task: the modified spec `docs/superpowers/specs/2026-09-19-phase7-agent-surface-parity-design.md` and this untracked plan. Do not stage either in Tasks 1–11.

- [ ] **Step 2: Write the failing tests**

Create `crates/rakka-agent-mcp/tests/artifact_sink.rs`:

```rust
//! The executor's artifact write path: a sink that is not a pass-through
//! store, the run scope it is handed, the reference it returns, and the
//! 2 KiB bound measured as the run itself measures it.
//!
//! The whole file is gated: it drives the in-process fake server, which only
//! exists under `testkit`.
#![cfg(feature = "testkit")]

use std::sync::{Arc, Mutex};

use rakka_agent::{
    AgentContentDigest, AgentDispatchToolExecutor, AgentEffectSafetyClass, AgentRunScope,
    AgentTaskContent, AgentToolCallId, AgentToolCallRequest, AgentToolDeclaration, AgentToolId,
    AgentToolResultBehavior, AGENT_TOOL_RESULT_MAX_BYTES,
};
use rakka_agent_mcp::testkit::{
    serve_fake, CountingClient, FakeMcpServer, FakeTool, FakeToolBehaviour,
};
use rakka_agent_mcp::{
    mcp_artifact_store, sync_mcp_descriptors, McpAllowAllEgress, McpArtifactFuture,
    McpArtifactSink, McpArtifacts, McpDispatchToolExecutor, McpServerBinding, McpServerId,
    McpToolPolicy, MCP_INLINE_RESULT_MAX_BYTES,
};
use rakka_agent_workflow::{
    AgentArtifactError, AgentArtifactWriteRequest, AgentAttributes, AgentEphemeralCredential,
    AgentTimestampMillis, ArtifactKind, ArtifactRef, RedactionStatus,
};
use serde_json::json;

mod support;
use support::{run_scope, tool_intent_with_timeout, SharedArtifactStore};

/// A credential value the fake server echoes into a large result.
const ECHOED_TOKEN: &str = "sink-token-sentinel";

/// How the recording sink answers a write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SinkMode {
    /// A content-addressed store: the SHA-256 of the bytes is the id, and the
    /// requested id is ignored.
    ContentAddressed,
    /// As above, but the reference says `redaction: Unknown`, which the
    /// default artifact policy refuses.
    UnknownRedaction,
    /// The store is down.
    Failing,
    /// A valid reference whose URI alone is larger than the run's bound.
    HugeUri,
}

/// One write, as the sink saw it.
#[derive(Debug, Clone)]
struct SeenWrite {
    scope_key: String,
    request: AgentArtifactWriteRequest,
}

/// A `&self` sink, as a deployment's own store is: it records what it was
/// handed and mints its own reference.
#[derive(Debug, Clone)]
struct RecordingSink {
    mode: SinkMode,
    seen: Arc<Mutex<Vec<SeenWrite>>>,
}

impl RecordingSink {
    fn new(mode: SinkMode) -> Self {
        Self {
            mode,
            seen: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn seen(&self) -> Vec<SeenWrite> {
        self.seen.lock().expect("the sink's log").clone()
    }
}

impl McpArtifactSink for RecordingSink {
    fn put_result<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        request: AgentArtifactWriteRequest,
    ) -> McpArtifactFuture<'a> {
        Box::pin(async move {
            self.seen.lock().expect("the sink's log").push(SeenWrite {
                scope_key: scope.key(),
                request: request.clone(),
            });
            if self.mode == SinkMode::Failing {
                return Err(AgentArtifactError::Store {
                    message: "the artifact store is unavailable".to_string(),
                });
            }
            let digest = AgentContentDigest::sha256_of_bytes(&request.bytes).value;
            let uri = if self.mode == SinkMode::HugeUri {
                format!("host://artifacts/{}/{digest}", "p".repeat(3_000))
            } else {
                format!("host://artifacts/{digest}")
            };
            Ok(ArtifactRef {
                artifact_id: digest.clone(),
                kind: request.kind,
                uri,
                checksum: Some(format!("sha256:{digest}")),
                content_type: request.content_type,
                byte_len: Some(u64::try_from(request.bytes.len()).unwrap_or(u64::MAX)),
                retention_class: request.retention_class,
                encryption: None,
                redaction: if self.mode == SinkMode::UnknownRedaction {
                    RedactionStatus::Unknown
                } else {
                    request.redaction
                },
                created_at: request.created_at,
                metadata: AgentAttributes::default(),
            })
        })
    }
}

fn server_id() -> McpServerId {
    McpServerId::new("crm").expect("id")
}

fn policy(behavior: AgentToolResultBehavior) -> McpToolPolicy {
    McpToolPolicy::new(AgentToolDeclaration::new(AgentEffectSafetyClass::ReadOnly))
        .with_result_behavior(behavior)
}

fn call(tool: &str) -> AgentToolCallRequest {
    AgentToolCallRequest::new(
        AgentToolCallId::new("call-1").expect("id"),
        AgentToolId::new(format!("mcp.crm.{tool}")).expect("id"),
        json!({}),
    )
    .expect("call")
}

/// The longest text a `{"text": …}` result may carry and still be inline:
/// derived from the run's own bound and the run's own measure, so the proof
/// holds whatever the wrapper costs.
fn longest_inline_text() -> usize {
    let empty = AgentTaskContent::inline(json!({"text": ""}))
        .expect("inline")
        .size_bytes();
    AGENT_TOOL_RESULT_MAX_BYTES - empty
}

fn text_tool(name: &str, text: String) -> FakeTool {
    FakeTool::new(
        name,
        "Answers with text.",
        json!({"type":"object"}),
        FakeToolBehaviour::Text(text),
    )
}

/// One tool per case. `big` and `leaky` are stored; the `edge-*` pair sits on
/// either side of the inline bound.
fn fake() -> FakeMcpServer {
    let edge = longest_inline_text();
    FakeMcpServer::new()
        .with_tool(text_tool("big", "x".repeat(5_000)))
        .with_tool(text_tool(
            "leaky",
            format!("{ECHOED_TOKEN} {}", "x".repeat(5_000)),
        ))
        .with_tool(text_tool("edge-in", "x".repeat(edge)))
        .with_tool(text_tool("edge-out", "x".repeat(edge + 1)))
        .with_tool(text_tool("edge-stored", "x".repeat(edge + 1)))
}

fn binding(url: &str) -> McpServerBinding {
    McpServerBinding::streamable_http(server_id(), url)
        .with_tool("big", policy(AgentToolResultBehavior::ArtifactReference))
        .expect("t")
        .with_tool("leaky", policy(AgentToolResultBehavior::ArtifactReference))
        .expect("t")
        .with_tool("edge-in", policy(AgentToolResultBehavior::InlineBounded))
        .expect("t")
        .with_tool("edge-out", policy(AgentToolResultBehavior::InlineBounded))
        .expect("t")
        .with_tool(
            "edge-stored",
            policy(AgentToolResultBehavior::ArtifactReference),
        )
        .expect("t")
}

async fn executor_over(
    url: &str,
    artifacts: impl Into<McpArtifacts>,
) -> McpDispatchToolExecutor<CountingClient> {
    let binding = binding(url);
    let http = CountingClient::new();
    let set = sync_mcp_descriptors(
        &http,
        &binding,
        None,
        AgentTimestampMillis::new(1),
        &McpAllowAllEgress,
    )
    .await
    .expect("syncs");
    McpDispatchToolExecutor::new(
        vec![set],
        vec![binding],
        artifacts,
        http,
        Arc::new(McpAllowAllEgress),
    )
    .expect("builds")
}

#[tokio::test]
async fn a_sink_that_mints_its_own_id_is_what_the_attempt_answers() {
    let endpoint = serve_fake(fake()).await;
    let sink = RecordingSink::new(SinkMode::ContentAddressed);
    let executor = executor_over(&endpoint.url, McpArtifacts::sink(sink.clone())).await;
    let intent = tool_intent_with_timeout("mcp.crm.big", Some(5_000));
    let content = executor
        .execute(&run_scope(), &intent, &call("big"), None)
        .await
        .expect("artifact");
    let AgentTaskContent::Artifact(reference) = content else {
        panic!("expected an artifact, got {content:?}")
    };

    let seen = sink.seen();
    assert_eq!(seen.len(), 1, "one write");
    assert_eq!(
        seen[0].scope_key,
        run_scope().key(),
        "the sink is handed the run the attempt belongs to"
    );
    assert_eq!(seen[0].request.kind, ArtifactKind::ToolOutput);
    assert_eq!(
        seen[0].request.redaction,
        RedactionStatus::ReferenceOnly,
        "nothing was scrubbed, so nothing is marked redacted"
    );
    assert_eq!(
        seen[0].request.artifact_id,
        Some(format!(
            "mcp-{}-g{}-call-1",
            intent.effect_id,
            intent.generation.get()
        )),
        "the derived id is still requested"
    );
    assert_eq!(
        reference.artifact_id,
        AgentContentDigest::sha256_of_bytes(&seen[0].request.bytes).value,
        "and the sink's own id is what the attempt answers"
    );
}

#[tokio::test]
async fn a_reference_the_default_policy_refuses_fails_the_attempt() {
    let endpoint = serve_fake(fake()).await;
    let sink = RecordingSink::new(SinkMode::UnknownRedaction);
    let executor = executor_over(&endpoint.url, McpArtifacts::sink(sink)).await;
    let error = executor
        .execute(
            &run_scope(),
            &tool_intent_with_timeout("mcp.crm.big", Some(5_000)),
            &call("big"),
            None,
        )
        .await
        .expect_err("an unknown redaction status is not a recordable reference");
    assert!(
        error.to_string().contains("invalid-artifact-reference"),
        "{error}"
    );
}

#[tokio::test]
async fn a_sink_failure_fails_the_attempt_under_the_stores_own_code() {
    let endpoint = serve_fake(fake()).await;
    let sink = RecordingSink::new(SinkMode::Failing);
    let executor = executor_over(&endpoint.url, McpArtifacts::sink(sink)).await;
    let error = executor
        .execute(
            &run_scope(),
            &tool_intent_with_timeout("mcp.crm.big", Some(5_000)),
            &call("big"),
            None,
        )
        .await
        .expect_err("the store is down");
    assert!(error.to_string().contains("artifact-store"), "{error}");
}

#[tokio::test]
async fn a_scrubbed_result_is_stored_marked_redacted() {
    let endpoint = serve_fake(fake()).await;
    let sink = RecordingSink::new(SinkMode::ContentAddressed);
    let executor = executor_over(&endpoint.url, McpArtifacts::sink(sink.clone())).await;
    let credential = AgentEphemeralCredential::bearer_token(ECHOED_TOKEN);
    executor
        .execute(
            &run_scope(),
            &tool_intent_with_timeout("mcp.crm.leaky", Some(5_000)),
            &call("leaky"),
            Some(&credential),
        )
        .await
        .expect("artifact");
    let seen = sink.seen();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].request.redaction, RedactionStatus::Redacted);
    let stored = String::from_utf8(seen[0].request.bytes.clone()).expect("json");
    assert!(!stored.contains(ECHOED_TOKEN), "the token was scrubbed");
    assert!(stored.contains("<redacted>"), "{stored}");
}

#[tokio::test]
async fn a_result_on_the_inline_edge_is_measured_as_the_run_measures_it() {
    let endpoint = serve_fake(fake()).await;
    let sink = RecordingSink::new(SinkMode::ContentAddressed);
    let executor = executor_over(&endpoint.url, McpArtifacts::sink(sink.clone())).await;

    let inline = executor
        .execute(
            &run_scope(),
            &tool_intent_with_timeout("mcp.crm.edge-in", Some(5_000)),
            &call("edge-in"),
            None,
        )
        .await
        .expect("the longest inline result");
    assert!(inline.inline_value().is_some(), "{inline:?}");
    assert_eq!(
        inline.size_bytes(),
        AGENT_TOOL_RESULT_MAX_BYTES,
        "exactly the bound the run enforces"
    );
    assert_eq!(MCP_INLINE_RESULT_MAX_BYTES, AGENT_TOOL_RESULT_MAX_BYTES);

    let error = executor
        .execute(
            &run_scope(),
            &tool_intent_with_timeout("mcp.crm.edge-out", Some(5_000)),
            &call("edge-out"),
            None,
        )
        .await
        .expect_err("one byte over, and the binding keeps results inline");
    assert!(
        error.to_string().contains("mcp-result-too-large"),
        "{error}"
    );

    let stored = executor
        .execute(
            &run_scope(),
            &tool_intent_with_timeout("mcp.crm.edge-stored", Some(5_000)),
            &call("edge-stored"),
            None,
        )
        .await
        .expect("one byte over, and the binding stores it");
    assert!(stored.artifact_ref().is_some(), "{stored:?}");
    assert!(stored.size_bytes() <= AGENT_TOOL_RESULT_MAX_BYTES);
    assert_eq!(sink.seen().len(), 1, "only the stored one was written");
}

#[tokio::test]
async fn a_reference_larger_than_the_runs_bound_is_refused() {
    let endpoint = serve_fake(fake()).await;
    let sink = RecordingSink::new(SinkMode::HugeUri);
    let executor = executor_over(&endpoint.url, McpArtifacts::sink(sink)).await;
    let error = executor
        .execute(
            &run_scope(),
            &tool_intent_with_timeout("mcp.crm.big", Some(5_000)),
            &call("big"),
            None,
        )
        .await
        .expect_err("the run would refuse this reference");
    assert!(
        error.to_string().contains("mcp-result-too-large"),
        "{error}"
    );
}

#[tokio::test]
async fn a_store_handle_still_builds_an_executor() {
    let endpoint = serve_fake(fake()).await;
    let store = SharedArtifactStore::default();
    let executor = executor_over(&endpoint.url, mcp_artifact_store(store.clone())).await;
    executor
        .execute(
            &run_scope(),
            &tool_intent_with_timeout("mcp.crm.big", Some(5_000)),
            &call("big"),
            None,
        )
        .await
        .expect("artifact");
    assert_eq!(store.len().await, 1, "the store behind the handle was written");
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cargo test -p rakka-agent-mcp --features testkit --test artifact_sink` (timeout 600000)
Expected: FAIL to compile with unresolved imports `McpArtifactFuture`, `McpArtifactSink`, `McpArtifacts`.

- [ ] **Step 4: Create the sink module**

Create `crates/rakka-agent-mcp/src/artifacts.rs`:

```rust
//! Where an executor writes a tool result too large to stay inline.
//!
//! [`McpArtifactSink`] is the write seam. It takes `&self` and is `Sync`, so
//! one sink is shared by every concurrent attempt without a lock of this
//! crate's making, and it is handed the run scope, because a deployment's
//! store files an artifact under a tenant and a run and the write request
//! names neither.
//!
//! [`AgentArtifactStore`](rakka_agent_workflow::AgentArtifactStore) cannot be
//! that seam: `put_artifact` takes `&mut self` and the trait is `Send` but
//! not `Sync`. A store of that shape still works — an
//! [`McpArtifactStore`](crate::McpArtifactStore) converts into
//! [`McpArtifacts`], behind the same async mutex as before.
//!
//! The executor **requests** an artifact id and does not require it back. A
//! content-addressed store mints its own and ignores the request; what the
//! run records is the reference the sink returned, once it passes
//! [`validate_artifact_ref`](rakka_agent_workflow::validate_artifact_ref).

use std::fmt::{self, Debug, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use rakka_agent::AgentRunScope;
use rakka_agent_workflow::{AgentArtifactError, AgentArtifactWriteRequest, ArtifactRef};

use crate::executor::McpArtifactStore;

/// What one artifact write answers.
pub type McpArtifactFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ArtifactRef, AgentArtifactError>> + Send + 'a>>;

/// The store an executor writes an over-large tool result to.
///
/// # Contract
///
/// - `scope` is the run the attempt belongs to. It is the only source of the
///   tenant and the run a store may file the artifact under; nothing in
///   `request` names them.
/// - `request.artifact_id` is a request. It is derived from the effect, its
///   generation, and the call, so a store that honors it rewrites one
///   artifact when an attempt is re-driven. A store that mints its own id
///   must be idempotent on the bytes instead.
/// - The reference returned must pass the default artifact policy: a
///   checksum, a content type, a byte length, a retention class, and a
///   redaction status other than `Unknown`. The executor refuses one that
///   does not, under the artifact error's own code.
/// - The write runs inside the attempt's deadline.
/// - An error's text is persisted, bounded, on the outbox row and the fleet
///   index. It must carry no credential and no content.
pub trait McpArtifactSink: Send + Sync {
    /// Writes one tool result and answers its durable reference.
    fn put_result<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        request: AgentArtifactWriteRequest,
    ) -> McpArtifactFuture<'a>;
}

/// The artifact write path an executor is built with.
///
/// Built from a sink with [`McpArtifacts::sink`], or converted from an
/// [`McpArtifactStore`] with `into()`.
#[derive(Clone)]
pub struct McpArtifacts(Arc<dyn McpArtifactSink>);

impl McpArtifacts {
    /// Wraps one deployment-owned sink.
    #[must_use]
    pub fn sink<S>(sink: S) -> Self
    where
        S: McpArtifactSink + 'static,
    {
        Self(Arc::new(sink))
    }

    /// Writes through the wrapped sink.
    pub(crate) fn put_result<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        request: AgentArtifactWriteRequest,
    ) -> McpArtifactFuture<'a> {
        self.0.put_result(scope, request)
    }
}

impl Debug for McpArtifacts {
    /// Opaque: a sink is a deployment's own type, and nothing about it
    /// belongs in a log line.
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("McpArtifacts(..)")
    }
}

impl From<Arc<dyn McpArtifactSink>> for McpArtifacts {
    fn from(sink: Arc<dyn McpArtifactSink>) -> Self {
        Self(sink)
    }
}

impl From<McpArtifactStore> for McpArtifacts {
    fn from(store: McpArtifactStore) -> Self {
        Self(Arc::new(StoreSink { store }))
    }
}

/// An [`McpArtifactStore`] as a sink: the scope is dropped, because
/// `put_artifact` has nowhere to take it, and writes serialize behind the
/// store's mutex.
struct StoreSink {
    store: McpArtifactStore,
}

impl McpArtifactSink for StoreSink {
    fn put_result<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        request: AgentArtifactWriteRequest,
    ) -> McpArtifactFuture<'a> {
        Box::pin(async move {
            let mut store = self.store.lock().await;
            store.put_artifact(request).await
        })
    }
}
```

In `crates/rakka-agent-mcp/src/lib.rs`, add `mod artifacts;` beside the other `mod` lines, add `pub use artifacts::{McpArtifactFuture, McpArtifactSink, McpArtifacts};` beside the `pub use executor::{…}` line, and add one line to the module map in the crate doc: "`artifacts` — the `&self` sink an executor writes an over-large result through".

- [ ] **Step 5: Thread the sink and the scope through the executor**

In `crates/rakka-agent-mcp/src/executor.rs`:

1. Imports. Replace the `rakka_agent_workflow` import with:

```rust
use rakka_agent_workflow::{
    validate_artifact_ref, AgentArtifactStore, AgentArtifactWriteRequest,
    AgentEphemeralCredential, ArtifactKind, RedactionStatus,
};
```

and add `use crate::artifacts::McpArtifacts;` beside the other `crate::` imports.

2. The alias's doc comment (:76-88). Replace its last paragraph ("The executor writes each result under a derived artifact id … must accept an artifact id with slashes in it.") with:

```rust
/// An executor converts this handle into an
/// [`McpArtifacts`](crate::McpArtifacts) with `into()`; a deployment whose
/// store is `&self` implements [`McpArtifactSink`](crate::McpArtifactSink)
/// instead and never builds one of these. The reference launcher reads launch
/// specifications through this handle.
```

3. The field at :149 becomes `artifacts: McpArtifacts,`.

4. `new`, `with_launcher`, and `build`. In `new` and `with_launcher` change the parameter to `artifacts: impl Into<McpArtifacts>,` and pass `artifacts.into()` to `build`; in `build` change the parameter to `artifacts: McpArtifacts,`. Nothing else in the three bodies changes. Add to both public constructors' docs: "`artifacts` is where an over-large result is written: an [`McpArtifacts`](crate::McpArtifacts), or an [`McpArtifactStore`] converted into one."

5. `attempt` (:366). Pass the scope to `call`:

```rust
        let outcome = within(
            deadline,
            self.call(
                &session, scope, server, descriptor, policy, intent, call, &secrets,
            ),
        )
        .await
        .and_then(std::convert::identity);
```

6. `call` (:487). Add `scope: &AgentRunScope,` as its second parameter after `session`, and change its last statement to:

```rust
        self.content(scope, policy, &call.tool, intent, call, answer, secrets)
            .await
```

7. `content` (:580). Add `#[allow(clippy::too_many_arguments)]` above it, add `scope: &AgentRunScope,` as its first parameter after `&self`, and change the `Complete` arm's last call to `self.bounded(scope, policy, tool, intent, call, &result, secrets).await`.

8. `bounded` (:626). Replace the whole function with:

```rust
    /// Inline when the result is wholly textual or structured and fits the
    /// run's own bound; an artifact when the binding says so; a refusal
    /// otherwise. Either way the kept content is scrubbed of `secrets` before
    /// it is measured.
    ///
    /// The measure is the run's: [`AgentTaskContent::size_bytes`], the
    /// content's own serialization with its `{"inline":…}` or
    /// `{"artifact":…}` wrapper, against
    /// [`MCP_INLINE_RESULT_MAX_BYTES`] — the same 2 KiB the run enforces on
    /// what a dispatcher delivers. Measuring the bare value would admit a
    /// result the run then refuses.
    #[allow(clippy::too_many_arguments)]
    async fn bounded(
        &self,
        scope: &AgentRunScope,
        policy: &McpToolPolicy,
        tool: &AgentToolId,
        intent: &AgentRunEffect,
        call: &AgentToolCallRequest,
        result: &CallToolResult,
        secrets: &[&str],
    ) -> Result<AgentTaskContent, AgentDispatchError> {
        let (mut candidate, overflow) = candidate_of(result);
        redact_value(&mut candidate, secrets);
        if !overflow {
            // A value over the task's own inline bound is not inline content
            // at all, and falls through to the binding's behavior with
            // everything else that is too large.
            if let Ok(content) = AgentTaskContent::inline(candidate) {
                if content.size_bytes() <= MCP_INLINE_RESULT_MAX_BYTES {
                    return Ok(content);
                }
            }
        }
        match policy.result_behavior {
            AgentToolResultBehavior::ArtifactReference => {
                let stored = serde_json::to_string(&StoredResult {
                    content: &result.content,
                    structured_content: &result.structured_content,
                })
                .map_err(encoding_refused)?;
                let scrubbed = redact_json_text(&stored, secrets);
                // Marked only when the scrub changed something: `Redacted`
                // is a statement about these bytes, not about the path.
                let redaction = if scrubbed == stored {
                    RedactionStatus::ReferenceOnly
                } else {
                    RedactionStatus::Redacted
                };
                let bytes = scrubbed.into_bytes();
                // The writer's checksum, not the store's: the default
                // artifact policy refuses a reference without one, and a
                // pass-through store only returns what it was given. SHA-256
                // over the exact bytes written, so a reader can tell a stored
                // result from a substituted one.
                let checksum = format!(
                    "sha256:{}",
                    AgentContentDigest::sha256_of_bytes(&bytes).value
                );
                // `new`'s defaults, retention class `standard` among them.
                // The executor holds no clock; the effect's own commit time
                // is durable and identical across a re-drive.
                let request = AgentArtifactWriteRequest::new(
                    ArtifactKind::ToolOutput,
                    "application/json",
                    bytes,
                    intent.created_at,
                )
                .checksum(checksum)
                .redaction(redaction)
                // Requested, not required: derived, so a store that honors
                // it rewrites one artifact when the generation is re-driven.
                .artifact_id(format!(
                    "mcp-{}-g{}-{}",
                    intent.effect_id,
                    intent.generation.get(),
                    call.call_id
                ));
                let refused = |error: rakka_agent_workflow::AgentArtifactError| {
                    AgentDispatchError::collaborator(error.code(), error.to_string())
                };
                let reference = self
                    .artifacts
                    .put_result(scope, request)
                    .await
                    .map_err(refused)?;
                // The store is the deployment's, and what it returns becomes
                // the run's record: a reference the default policy refuses
                // fails here, not on the read that would find it later.
                validate_artifact_ref(&reference).map_err(refused)?;
                let content = AgentTaskContent::artifact(reference);
                if content.size_bytes() > MCP_INLINE_RESULT_MAX_BYTES {
                    return Err(AgentDispatchError::collaborator(
                        "mcp-result-too-large",
                        format!(
                            "{tool}: the artifact reference the store returned is over \
                             {MCP_INLINE_RESULT_MAX_BYTES} bytes once encoded"
                        ),
                    ));
                }
                Ok(content)
            }
            // `InlineBounded`, and — since `AgentToolResultBehavior` is
            // `#[non_exhaustive]` — any behavior a later version adds that
            // this adapter does not know how to satisfy. Both keep the inline
            // bound, which refuses rather than widening what becomes state.
            _ => Err(AgentDispatchError::collaborator(
                "mcp-result-too-large",
                format!(
                    "{tool}: the result exceeds {MCP_INLINE_RESULT_MAX_BYTES} bytes and the \
                     binding keeps results inline"
                ),
            )),
        }
    }
```

If `encoding_refused` is now unused by anything but the artifact arm, it stays: that arm still uses it.

9. In `crates/rakka-agent-mcp/src/binding.rs:42`, replace the constant's doc line with:

```rust
/// Largest a tool result's content may be and stay inline, in bytes: the
/// content as the run measures it, its `{"inline":…}` wrapper included, and
/// equal to `rakka_agent::AGENT_TOOL_RESULT_MAX_BYTES`. An artifact reference
/// is held to the same bound.
```

- [ ] **Step 6: Run the new tests to verify they pass**

Run: `cargo test -p rakka-agent-mcp --features testkit --test artifact_sink` (timeout 600000)
Expected: PASS, 7 tests.

- [ ] **Step 7: Run the crate's existing suites**

Run: `cargo test -p rakka-agent-mcp --features testkit` and `cargo test -p rakka-agent-mcp --all-features` (timeout 600000 each)
Expected: PASS. If `client_dispatch.rs` or `child_process.rs` asserts `ArtifactKind::File` on a stored result, change that assertion to `ArtifactKind::ToolOutput`; change nothing else in an existing test.

Run: `cargo test -p rakka-agent --test mcp_client_dispatch` (timeout 600000)
Expected: PASS, 7 scenarios. Scenario 4 passes an `McpArtifactStore` and must compile unchanged.

- [ ] **Step 8: Lint and commit**

Run: `cargo fmt --all -- --check` and `cargo clippy -p rakka-agent-mcp --all-targets --all-features -- -D warnings` (timeout 600000)
Expected: both exit 0.

```bash
git add crates/rakka-agent-mcp/src/artifacts.rs crates/rakka-agent-mcp/src/executor.rs \
        crates/rakka-agent-mcp/src/binding.rs crates/rakka-agent-mcp/src/lib.rs \
        crates/rakka-agent-mcp/tests/artifact_sink.rs
git commit -m "Write an over-large MCP result through a sink a deployment can implement, and measure every result as the run measures it

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

### Task 2: A sync refuses server text that carries the credential it sent

**Files:**
- Modify: `crates/rakka-agent-mcp/src/binding.rs` (one constant, after `MCP_LIST_PAGES_MAX` at :40)
- Modify: `crates/rakka-agent-mcp/src/sync.rs` (imports :49-56, `McpSyncError` :207-315, `sync_mcp_descriptors` :340, `sync_mcp_descriptors_over` :375, `sync_over_session` :388, `synced_descriptors` :469, `synced_descriptor` :492, the module doc :20-31)
- Modify: `crates/rakka-agent-mcp/src/lib.rs` (re-export the constant)
- Create: `crates/rakka-agent-mcp/tests/sync_credential_echo.rs`

**Interfaces:**
- Consumes: `crate::client::credential_secrets(credential: Option<&AgentEphemeralCredential>) -> Vec<&str>` (`pub(crate)`, `client.rs:255`).
- Produces:
  - `pub const MCP_SERVER_NAME_MAX_BYTES: usize = 256;`
  - `McpSyncError::CredentialEchoed { server: String, tool: Option<String>, field: &'static str }`, code `mcp-descriptor-credential-echoed`. `field` is one of `"server name"`, `"description"`, `"input schema"`, `"output schema"`.
  - `sync_over_session(session, binding, server, synced_at, secrets: &[&str])` (private; Task 3 changes it again).
  - Both public sync signatures are unchanged.

- [ ] **Step 1: Write the failing tests**

Create `crates/rakka-agent-mcp/tests/sync_credential_echo.rs`:

```rust
//! A publish-time sync against a server that echoes the credential it was
//! sent: every place server-chosen text would become release data, and the
//! bound on the one that had none.
//!
//! The whole file is gated: it drives the in-process fake server, which only
//! exists under `testkit`.
#![cfg(feature = "testkit")]

use rakka_agent::{AgentEffectSafetyClass, AgentToolDeclaration};
use rakka_agent_mcp::testkit::{
    hardened_reqwest_client, serve_fake, FakeMcpServer, FakeTool, FakeToolBehaviour,
};
use rakka_agent_mcp::{
    sync_mcp_descriptors, McpAllowAllEgress, McpDescriptorSet, McpServerBinding, McpServerId,
    McpSyncError, McpToolPolicy, MCP_SERVER_NAME_MAX_BYTES,
};
use rakka_agent_workflow::{AgentEphemeralCredential, AgentTimestampMillis};
use serde_json::{json, Value};

/// The credential the sync sends, and a hostile server echoes.
const TOKEN: &str = "publish-token-sentinel";
/// One whose JSON spelling differs from its own: the quote is escaped on the
/// wire and decoded before the sync reads it.
const QUOTED_TOKEN: &str = "publish\"token";

fn binding(url: &str) -> McpServerBinding {
    McpServerBinding::streamable_http(McpServerId::new("crm").expect("id"), url)
        .with_tool(
            "search",
            McpToolPolicy::new(AgentToolDeclaration::new(AgentEffectSafetyClass::ReadOnly)),
        )
        .expect("tool")
}

fn search(description: &str, input_schema: Value) -> FakeTool {
    FakeTool::new(
        "search",
        description,
        input_schema,
        FakeToolBehaviour::Echo,
    )
}

async fn sync(
    server: FakeMcpServer,
    credential: Option<&AgentEphemeralCredential>,
) -> Result<McpDescriptorSet, McpSyncError> {
    let endpoint = serve_fake(server).await;
    sync_mcp_descriptors(
        &hardened_reqwest_client(),
        &binding(&endpoint.url),
        credential,
        AgentTimestampMillis::new(1),
        &McpAllowAllEgress,
    )
    .await
}

/// The refusal names the field and carries neither the secret nor the
/// server's own words.
#[track_caller]
fn assert_echo_refused(
    result: Result<McpDescriptorSet, McpSyncError>,
    field: &str,
    tool: Option<&str>,
    secret: &str,
) {
    let error = result.expect_err("an echoed credential stores nothing");
    assert_eq!(error.code(), "mcp-descriptor-credential-echoed", "{error}");
    let McpSyncError::CredentialEchoed {
        field: refused,
        tool: named,
        ..
    } = &error
    else {
        panic!("expected CredentialEchoed, got {error:?}")
    };
    assert_eq!(*refused, field);
    assert_eq!(named.as_deref(), tool);
    let text = format!("{error} {error:?}");
    assert!(!text.contains(secret), "the refusal leaks the secret: {text}");
    assert!(
        !text.contains("SERVER-WORDS"),
        "the refusal quotes the server: {text}"
    );
}

#[tokio::test]
async fn a_description_that_carries_the_credential_is_refused() {
    let credential = AgentEphemeralCredential::bearer_token(TOKEN);
    let server = FakeMcpServer::new().with_tool(search(
        &format!("SERVER-WORDS call me with {TOKEN}"),
        json!({"type":"object"}),
    ));
    assert_echo_refused(
        sync(server, Some(&credential)).await,
        "description",
        Some("search"),
        TOKEN,
    );
}

#[tokio::test]
async fn an_input_schema_that_carries_the_credential_is_refused_however_deep() {
    let credential = AgentEphemeralCredential::api_key("x-api-key", TOKEN);
    let server = FakeMcpServer::new().with_tool(search(
        "Searches.",
        json!({
            "type": "object",
            "properties": {
                "q": {
                    "type": "string",
                    "examples": [["SERVER-WORDS", { "nested": format!("key={TOKEN}") }]]
                }
            }
        }),
    ));
    assert_echo_refused(
        sync(server, Some(&credential)).await,
        "input schema",
        Some("search"),
        TOKEN,
    );
}

#[tokio::test]
async fn a_schema_key_and_an_escaped_spelling_are_both_caught() {
    let credential = AgentEphemeralCredential::bearer_token(QUOTED_TOKEN);
    let mut properties = serde_json::Map::new();
    properties.insert(
        format!("field-{QUOTED_TOKEN}"),
        json!({"type": "string"}),
    );
    let server = FakeMcpServer::new().with_tool(search(
        "Searches.",
        json!({"type": "object", "properties": properties}),
    ));
    assert_echo_refused(
        sync(server, Some(&credential)).await,
        "input schema",
        Some("search"),
        QUOTED_TOKEN,
    );
}

#[tokio::test]
async fn an_output_schema_that_carries_the_credential_is_refused() {
    let credential = AgentEphemeralCredential::bearer_token(TOKEN);
    let server = FakeMcpServer::new().with_tool(
        search("Searches.", json!({"type":"object"})).with_output_schema(json!({
            "type": "object",
            "description": format!("SERVER-WORDS {TOKEN}")
        })),
    );
    assert_echo_refused(
        sync(server, Some(&credential)).await,
        "output schema",
        Some("search"),
        TOKEN,
    );
}

#[tokio::test]
async fn a_server_name_that_carries_the_credential_is_refused() {
    let credential = AgentEphemeralCredential::bearer_token(TOKEN);
    let server = FakeMcpServer::new()
        .with_server_name(format!("crm-SERVER-WORDS-{TOKEN}"))
        .with_tool(search("Searches.", json!({"type":"object"})));
    assert_echo_refused(
        sync(server, Some(&credential)).await,
        "server name",
        None,
        TOKEN,
    );
}

#[tokio::test]
async fn the_same_text_is_stored_when_it_is_not_the_credential() {
    // The positive control: the refusal is about the credential this sync
    // sent, not about the words. With another credential, and with none, the
    // very same listing syncs.
    let listing = || {
        FakeMcpServer::new().with_tool(search(
            &format!("call me with {TOKEN}"),
            json!({"type":"object"}),
        ))
    };
    let other = AgentEphemeralCredential::bearer_token("another-token");
    let set = sync(listing(), Some(&other)).await.expect("syncs");
    assert!(set.descriptors[0]
        .binding
        .descriptor()
        .description
        .contains(TOKEN));
    sync(listing(), None).await.expect("syncs with no credential");
}

#[tokio::test]
async fn a_server_name_is_cut_at_its_bound_on_a_character_boundary() {
    // 255 ASCII bytes, then a two-byte character straddling the bound.
    let name = format!("{}é-and-then-some", "n".repeat(MCP_SERVER_NAME_MAX_BYTES - 1));
    let server = FakeMcpServer::new()
        .with_server_name(name)
        .with_tool(search("Searches.", json!({"type":"object"})));
    let set = sync(server, None).await.expect("syncs");
    assert_eq!(
        set.server_name,
        "n".repeat(MCP_SERVER_NAME_MAX_BYTES - 1),
        "cut before the character the bound would have split"
    );
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p rakka-agent-mcp --features testkit --test sync_credential_echo` (timeout 600000)
Expected: FAIL to compile: `MCP_SERVER_NAME_MAX_BYTES` is unresolved and `McpSyncError` has no variant `CredentialEchoed`.

- [ ] **Step 3: Add the constant**

In `crates/rakka-agent-mcp/src/binding.rs`, after `MCP_LIST_PAGES_MAX`:

```rust
/// Largest a server's self-reported implementation name may be as a
/// descriptor set stores it, in bytes. A longer name is cut at a character
/// boundary: the name is the server's to choose, and the set is release data.
pub const MCP_SERVER_NAME_MAX_BYTES: usize = 256;
```

Add `MCP_SERVER_NAME_MAX_BYTES` to the `pub use binding::{…}` list in `crates/rakka-agent-mcp/src/lib.rs`, in alphabetical position.

- [ ] **Step 4: Add the refusal**

In `crates/rakka-agent-mcp/src/sync.rs`:

1. Add the variant to `McpSyncError`, after `Descriptor`:

```rust
    /// The server's own text carries the credential this sync sent it.
    ///
    /// Nothing is stored. A descriptor set is release data and its
    /// description and schema reach the model, so a set built from such a
    /// listing would publish the secret; and a schema cannot be scrubbed
    /// instead, because its digest is what a dispatch compares with the live
    /// server's.
    CredentialEchoed {
        /// The server whose text carried it.
        server: String,
        /// The tool whose text carried it, or `None` for the server's name.
        tool: Option<String>,
        /// Which text: `server name`, `description`, `input schema`, or
        /// `output schema`.
        field: &'static str,
    },
```

2. Add its arm to `code()`: `Self::CredentialEchoed { .. } => "mcp-descriptor-credential-echoed",`.

3. Add its arm to `Display`:

```rust
            Self::CredentialEchoed {
                server,
                tool: Some(tool),
                field,
            } => write!(
                f,
                "the MCP server {server}'s tool {tool} {field} carries the credential the sync \
                 sent; nothing was stored"
            ),
            Self::CredentialEchoed {
                server,
                tool: None,
                field,
            } => write!(
                f,
                "the MCP server {server}'s {field} carries the credential the sync sent; \
                 nothing was stored"
            ),
```

4. Imports: add `credential_secrets` to the `use crate::client::{…}` list and `MCP_SERVER_NAME_MAX_BYTES` to the `use crate::binding::{…}` list.

5. Add the three helpers above `sync_over_session`:

```rust
/// Whether `text` carries any of `secrets`. An empty secret matches nothing.
fn carries(text: &str, secrets: &[&str]) -> bool {
    secrets
        .iter()
        .any(|secret| !secret.is_empty() && text.contains(secret))
}

/// Whether any string, key, or number in `value` carries any of `secrets`.
///
/// The value is walked decoded, so a secret whose JSON spelling differs from
/// its own — one with a quote or a backslash in it — is found as itself. A
/// number is compared whole rather than searched: a digit run inside a
/// larger number is not the secret.
fn value_carries(value: &Value, secrets: &[&str]) -> bool {
    match value {
        Value::String(text) => carries(text, secrets),
        Value::Array(items) => items.iter().any(|item| value_carries(item, secrets)),
        Value::Object(map) => map
            .iter()
            .any(|(key, nested)| carries(key, secrets) || value_carries(nested, secrets)),
        Value::Number(number) => {
            let spelled = number.to_string();
            secrets.iter().any(|secret| *secret == spelled)
        }
        Value::Null | Value::Bool(_) => false,
    }
}

/// The server's name as a set stores it: cut at
/// [`MCP_SERVER_NAME_MAX_BYTES`], on a character boundary.
fn bounded_server_name(name: &str) -> String {
    if name.len() <= MCP_SERVER_NAME_MAX_BYTES {
        return name.to_string();
    }
    let mut end = MCP_SERVER_NAME_MAX_BYTES;
    while end > 0 && !name.is_char_boundary(end) {
        end -= 1;
    }
    name[..end].to_string()
}
```

6. `sync_mcp_descriptors`: replace its last two statements with

```rust
    let session = connect(http, binding, credential, egress).await?;
    // The material the server now holds, and so could echo into anything it
    // lists.
    let secrets = credential_secrets(credential);
    sync_over_session(session, binding, &server, synced_at, &secrets).await
```

7. `sync_mcp_descriptors_over`: change its last statement to `sync_over_session(session, binding, &server, synced_at, &[]).await` and add to its doc: "No credential travels over a launcher's transport, so there is none for the listing to echo."

8. Replace `sync_over_session` with:

```rust
/// Everything after the handshake that both syncs share: one listing, the
/// session closed, then the answer judged.
async fn sync_over_session(
    session: McpClientSession,
    binding: &McpServerBinding,
    server: &str,
    synced_at: AgentTimestampMillis,
    secrets: &[&str],
) -> Result<McpDescriptorSet, McpSyncError> {
    let listed = session.list_all_tools().await;
    let protocol_version = session.negotiated_version().as_str().to_string();
    let reported_name = session.server_name().to_string();
    // The session is closed before the answer is judged: a refusal must not
    // leave a transport (and its credential-bearing client, or its child
    // process) alive.
    let built = listed
        .map_err(|error| McpSyncError::Client(service_error(server, &error)))
        .and_then(|listed| {
            listed.ok_or_else(|| {
                McpSyncError::Client(McpClientError::Protocol {
                    server: server.to_string(),
                    reason: format!("tools/list did not end within {MCP_LIST_PAGES_MAX} pages"),
                })
            })
        })
        .and_then(|listed| synced_descriptors(binding, server, &listed, secrets));
    session.close().await;
    // Judged first: a server that echoes the credential is refused for that,
    // whatever else its listing got wrong.
    if carries(&reported_name, secrets) {
        return Err(McpSyncError::CredentialEchoed {
            server: server.to_string(),
            tool: None,
            field: "server name",
        });
    }
    Ok(McpDescriptorSet {
        schema_version: MCP_DESCRIPTOR_SET_SCHEMA_VERSION,
        server_id: binding.server_id.clone(),
        server_name: bounded_server_name(&reported_name),
        protocol_version,
        synced_at,
        descriptors: built?,
    })
}
```

9. `synced_descriptors`: add `secrets: &[&str],` as its last parameter and pass it on: `synced_descriptor(binding, server, tool, policy, found, secrets)`.

10. `synced_descriptor`: add `secrets: &[&str],` as its last parameter. Insert this block directly after the `SchemaTooLarge` check and before the `if policy.honor_hints` block:

```rust
    // Before anything is derived from the server's text: what it says about
    // this tool must not carry the credential the sync sent it. The raw
    // description is judged, not the bounded one — a cut could leave part of
    // a secret behind and hide the rest.
    let echoed = |field: &'static str| McpSyncError::CredentialEchoed {
        server: server.to_string(),
        tool: Some(tool.to_string()),
        field,
    };
    if listed
        .description
        .as_deref()
        .is_some_and(|description| carries(description, secrets))
    {
        return Err(echoed("description"));
    }
    if value_carries(&input_schema, secrets) {
        return Err(echoed("input schema"));
    }
    if output_schema
        .as_ref()
        .is_some_and(|schema| value_carries(schema, secrets))
    {
        return Err(echoed("output schema"));
    }
```

11. Module doc (:20): change "Three bounds and one rule then guard what crosses in:" to "Four bounds and two rules then guard what crosses in:" and add two list items:

```rust
//! - The server's self-reported name is cut at [`MCP_SERVER_NAME_MAX_BYTES`].
//! - Text that carries the credential the sync sent — in the server's name,
//!   a description, or either schema — refuses the sync
//!   (`mcp-descriptor-credential-echoed`). It is refused rather than
//!   scrubbed because a scrubbed schema would no longer match the digest a
//!   dispatch compares with the live server's.
```

- [ ] **Step 5: Add the unit tests for the helpers**

In the `#[cfg(test)] mod tests` of `crates/rakka-agent-mcp/src/sync.rs`, add:

```rust
    #[test]
    fn an_empty_secret_matches_nothing() {
        assert!(!carries("anything at all", &[""]));
        assert!(!value_carries(&json!({"a": ["b", 1]}), &[""]));
        assert!(!value_carries(&json!({"a": "b"}), &[]));
    }

    #[test]
    fn a_number_is_compared_whole() {
        assert!(value_carries(&json!({"maximum": 424_242}), &["424242"]));
        assert!(!value_carries(&json!({"maximum": 1_424_242}), &["424242"]));
    }
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cargo test -p rakka-agent-mcp --features testkit --test sync_credential_echo` (timeout 600000)
Expected: PASS, 7 tests.

Run: `cargo test -p rakka-agent-mcp --all-features` (timeout 600000)
Expected: PASS. No existing sync test sends a credential a fake echoes, so none changes.

Run: `cargo test -p rakka-agent --test mcp_client_dispatch` (timeout 600000)
Expected: PASS, 7 scenarios.

- [ ] **Step 7: Lint and commit**

Run: `cargo fmt --all -- --check` and `cargo clippy -p rakka-agent-mcp --all-targets --all-features -- -D warnings` (timeout 600000)
Expected: both exit 0.

```bash
git add crates/rakka-agent-mcp/src/binding.rs crates/rakka-agent-mcp/src/sync.rs \
        crates/rakka-agent-mcp/src/lib.rs crates/rakka-agent-mcp/tests/sync_credential_echo.rs
git commit -m "Refuse a descriptor sync whose server text carries the credential it sent, and bound the server name a set stores

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

### Task 3: Both syncs run under a deadline

**Files:**
- Modify: `crates/rakka-agent-mcp/src/binding.rs` (one constant, after `MCP_ATTEMPT_TIMEOUT_DEFAULT_MS`)
- Modify: `crates/rakka-agent-mcp/src/sync.rs` (`sync_mcp_descriptors` :340, `sync_mcp_descriptors_over` :375, `sync_over_session` :388)
- Modify: `crates/rakka-agent-mcp/src/lib.rs` (re-exports)
- Create: `crates/rakka-agent-mcp/tests/sync_deadline.rs`

**Interfaces:**
- Consumes: Task 2's `sync_over_session(session, binding, server, synced_at, secrets)`.
- Produces:
  - `pub const MCP_SYNC_TIMEOUT_DEFAULT_MS: u64 = 30_000;`
  - `pub async fn sync_mcp_descriptors_within<C>(http: &C, binding: &McpServerBinding, credential: Option<&AgentEphemeralCredential>, synced_at: AgentTimestampMillis, egress: &dyn McpEgressCheck, timeout: Duration) -> Result<McpDescriptorSet, McpSyncError>`
  - `pub async fn sync_mcp_descriptors_over_within(transport: McpChildTransport, binding: &McpServerBinding, synced_at: AgentTimestampMillis, timeout: Duration) -> Result<McpDescriptorSet, McpSyncError>`
  - `sync_mcp_descriptors` and `sync_mcp_descriptors_over` keep their signatures and apply the default.
  - A sync that runs out of time answers `McpSyncError::Client(McpClientError::Transport { .. })`, code `mcp-descriptor-sync-failed`, whose reason is `the sync exceeded its <n> ms bound`.

- [ ] **Step 1: Write the failing tests**

Create `crates/rakka-agent-mcp/tests/sync_deadline.rs`:

```rust
//! A publish-time sync is bounded in time: a handshake that is never
//! answered, a listing that stalls, the default, and the session closed on
//! the way out.
//!
//! The whole file is gated: it drives the in-process fake server, which only
//! exists under `testkit`.
#![cfg(feature = "testkit")]

use std::time::{Duration, Instant};

use rakka_agent::{AgentEffectSafetyClass, AgentToolDeclaration};
use rakka_agent_mcp::testkit::{
    hardened_reqwest_client, serve_fake, FakeMcpServer, FakeTool, FakeToolBehaviour,
};
use rakka_agent_mcp::{
    sync_mcp_descriptors, sync_mcp_descriptors_over_within, sync_mcp_descriptors_within,
    McpAllowAllEgress, McpServerBinding, McpServerId, McpToolPolicy, MCP_SYNC_TIMEOUT_DEFAULT_MS,
};
use rakka_agent_workflow::AgentTimestampMillis;
use serde_json::json;

mod support;
use support::duplex_transport;

fn binding(url: &str) -> McpServerBinding {
    McpServerBinding::streamable_http(McpServerId::new("crm").expect("id"), url)
        .with_tool(
            "search",
            McpToolPolicy::new(AgentToolDeclaration::new(AgentEffectSafetyClass::ReadOnly)),
        )
        .expect("tool")
}

fn listing() -> FakeMcpServer {
    FakeMcpServer::new().with_tool(FakeTool::new(
        "search",
        "Searches.",
        json!({"type":"object"}),
        FakeToolBehaviour::Echo,
    ))
}

/// A socket that accepts every connection and never writes a byte.
async fn silent_endpoint() -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("the loopback socket binds");
    let address = listener.local_addr().expect("the address is readable");
    let silent = tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((socket, _)) = listener.accept().await {
            held.push(socket);
        }
    });
    (format!("http://{address}/mcp"), silent)
}

#[track_caller]
fn assert_timed_out(error: &rakka_agent_mcp::McpSyncError, bound_ms: u64) {
    assert_eq!(error.code(), "mcp-descriptor-sync-failed", "{error}");
    assert!(
        error
            .to_string()
            .contains(&format!("the sync exceeded its {bound_ms} ms bound")),
        "{error}"
    );
}

#[tokio::test]
async fn a_handshake_that_is_never_answered_ends_at_the_bound() {
    let (url, silent) = silent_endpoint().await;
    let started = Instant::now();
    let error = tokio::time::timeout(
        Duration::from_secs(10),
        sync_mcp_descriptors_within(
            &hardened_reqwest_client(),
            &binding(&url),
            None,
            AgentTimestampMillis::new(1),
            &McpAllowAllEgress,
            Duration::from_millis(200),
        ),
    )
    .await
    .expect("the sync returned on its own rather than waiting forever")
    .expect_err("the handshake was never answered");
    silent.abort();
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "the bound fired inside the handshake: {:?}",
        started.elapsed()
    );
    assert_timed_out(&error, 200);
}

#[tokio::test]
async fn a_listing_that_stalls_ends_at_the_bound_and_the_session_is_closed() {
    let endpoint = serve_fake(listing().with_list_delay(5_000)).await;
    let started = Instant::now();
    let error = sync_mcp_descriptors_within(
        &hardened_reqwest_client(),
        &binding(&endpoint.url),
        None,
        AgentTimestampMillis::new(1),
        &McpAllowAllEgress,
        Duration::from_millis(300),
    )
    .await
    .expect_err("the listing never answered inside the bound");
    // The bound, plus the session's own bounded close.
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );
    assert_timed_out(&error, 300);
    assert_eq!(
        endpoint.server.list_calls(),
        1,
        "the listing was reached, and asked for once"
    );
}

#[tokio::test]
async fn a_sync_inside_its_bound_is_unchanged() {
    let endpoint = serve_fake(listing()).await;
    let bounded = sync_mcp_descriptors_within(
        &hardened_reqwest_client(),
        &binding(&endpoint.url),
        None,
        AgentTimestampMillis::new(1),
        &McpAllowAllEgress,
        Duration::from_secs(5),
    )
    .await
    .expect("syncs");
    let defaulted = sync_mcp_descriptors(
        &hardened_reqwest_client(),
        &binding(&endpoint.url),
        None,
        AgentTimestampMillis::new(1),
        &McpAllowAllEgress,
    )
    .await
    .expect("syncs");
    assert_eq!(bounded, defaulted);
    assert_eq!(MCP_SYNC_TIMEOUT_DEFAULT_MS, 30_000);
}

#[tokio::test]
async fn a_launcher_transport_sync_is_bounded_too() {
    let server = listing().with_list_delay(5_000);
    let child = McpServerBinding::child_process(
        McpServerId::new("crm").expect("id"),
        support::spec_artifact_ref(),
    )
    .with_tool(
        "search",
        McpToolPolicy::new(AgentToolDeclaration::new(AgentEffectSafetyClass::ReadOnly)),
    )
    .expect("tool");
    let error = sync_mcp_descriptors_over_within(
        duplex_transport(server),
        &child,
        AgentTimestampMillis::new(1),
        Duration::from_millis(300),
    )
    .await
    .expect_err("the listing never answered inside the bound");
    assert_timed_out(&error, 300);
}
```

The two helpers are `tests/support/mod.rs`'s own: `duplex_transport(server: FakeMcpServer) -> McpChildTransport` (:224) and `spec_artifact_ref() -> ArtifactRef` (:202). Change nothing in `support/mod.rs`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p rakka-agent-mcp --features testkit --test sync_deadline` (timeout 600000)
Expected: FAIL to compile: `sync_mcp_descriptors_within`, `sync_mcp_descriptors_over_within`, and `MCP_SYNC_TIMEOUT_DEFAULT_MS` are unresolved.

- [ ] **Step 3: Add the constant**

In `crates/rakka-agent-mcp/src/binding.rs`, after `MCP_ATTEMPT_TIMEOUT_DEFAULT_MS`:

```rust
/// Default bound, in milliseconds, on one publish-time descriptor sync: the
/// handshake and the listing together.
///
/// [`sync_mcp_descriptors`](crate::sync_mcp_descriptors) and
/// [`sync_mcp_descriptors_over`](crate::sync_mcp_descriptors_over) apply it,
/// so no sync is unbounded; their `_within` twins take the caller's own.
/// Thirty seconds, like an attempt's default: rmcp's legacy fallback alone
/// can spend ten of them waiting for a `server/discover` a pre-2026-07-28
/// server never answers.
pub const MCP_SYNC_TIMEOUT_DEFAULT_MS: u64 = 30_000;
```

Add `MCP_SYNC_TIMEOUT_DEFAULT_MS` to the `pub use binding::{…}` list in `lib.rs`, and `sync_mcp_descriptors_over_within, sync_mcp_descriptors_within` to the `pub use sync::{…}` list.

- [ ] **Step 4: Bound both syncs**

In `crates/rakka-agent-mcp/src/sync.rs`:

1. Imports: add `use std::time::Duration;` and add `MCP_SYNC_TIMEOUT_DEFAULT_MS` to the `use crate::binding::{…}` list.

2. Add the two helpers above `sync_mcp_descriptors`:

```rust
/// The instant a sync must be over by. `checked_add` rather than `+`: a bound
/// the platform's clock cannot represent ends the sync at once, failing
/// closed rather than panicking or waiting forever.
fn sync_deadline(timeout: Duration) -> tokio::time::Instant {
    let now = tokio::time::Instant::now();
    now.checked_add(timeout).unwrap_or(now)
}

/// What a sync that ran out of time answers: one fact to the operator, the
/// descriptors could not be refreshed, under the code a transport failure of
/// the sync already has.
fn timed_out(server: &str, timeout: Duration) -> McpSyncError {
    McpSyncError::Client(McpClientError::Transport {
        server: server.to_string(),
        reason: format!("the sync exceeded its {} ms bound", timeout.as_millis()),
    })
}
```

3. Replace `sync_mcp_descriptors` (keep its doc comment, replacing the paragraph that begins "The sync sets no deadline of its own" with: "The whole sync — handshake and listing — is bounded by [`MCP_SYNC_TIMEOUT_DEFAULT_MS`]; [`sync_mcp_descriptors_within`] takes the caller's own bound. A sync that runs out of time is refused `mcp-descriptor-sync-failed`, with its session closed.") with:

```rust
pub async fn sync_mcp_descriptors<C>(
    http: &C,
    binding: &McpServerBinding,
    credential: Option<&AgentEphemeralCredential>,
    synced_at: AgentTimestampMillis,
    egress: &dyn McpEgressCheck,
) -> Result<McpDescriptorSet, McpSyncError>
where
    C: StreamableHttpClient + Sync,
{
    sync_mcp_descriptors_within(
        http,
        binding,
        credential,
        synced_at,
        egress,
        Duration::from_millis(MCP_SYNC_TIMEOUT_DEFAULT_MS),
    )
    .await
}

/// As [`sync_mcp_descriptors`], under the caller's own bound.
///
/// `timeout` covers the handshake and the listing together. The session's
/// close is owed on every path and is bounded on its own terms, so the call
/// returns within `timeout` plus that fixed few seconds.
///
/// # Errors
///
/// [`McpSyncError`] with its stable code; a sync that ran out of time is
/// `mcp-descriptor-sync-failed`.
pub async fn sync_mcp_descriptors_within<C>(
    http: &C,
    binding: &McpServerBinding,
    credential: Option<&AgentEphemeralCredential>,
    synced_at: AgentTimestampMillis,
    egress: &dyn McpEgressCheck,
    timeout: Duration,
) -> Result<McpDescriptorSet, McpSyncError>
where
    C: StreamableHttpClient + Sync,
{
    binding.validate()?;
    let server = binding.server_id.to_string();
    let deadline = sync_deadline(timeout);
    let session =
        tokio::time::timeout_at(deadline, connect(http, binding, credential, egress))
            .await
            .map_err(|_| timed_out(&server, timeout))??;
    // The material the server now holds, and so could echo into anything it
    // lists.
    let secrets = credential_secrets(credential);
    sync_over_session(
        session, binding, &server, synced_at, &secrets, deadline, timeout,
    )
    .await
}
```

4. Replace `sync_mcp_descriptors_over` (keep its doc comment, adding the same bound sentence) with:

```rust
pub async fn sync_mcp_descriptors_over(
    transport: McpChildTransport,
    binding: &McpServerBinding,
    synced_at: AgentTimestampMillis,
) -> Result<McpDescriptorSet, McpSyncError> {
    sync_mcp_descriptors_over_within(
        transport,
        binding,
        synced_at,
        Duration::from_millis(MCP_SYNC_TIMEOUT_DEFAULT_MS),
    )
    .await
}

/// As [`sync_mcp_descriptors_over`], under the caller's own bound.
///
/// # Errors
///
/// [`McpSyncError`] with its stable code; a sync that ran out of time is
/// `mcp-descriptor-sync-failed`.
pub async fn sync_mcp_descriptors_over_within(
    transport: McpChildTransport,
    binding: &McpServerBinding,
    synced_at: AgentTimestampMillis,
    timeout: Duration,
) -> Result<McpDescriptorSet, McpSyncError> {
    binding.validate()?;
    let server = binding.server_id.to_string();
    let deadline = sync_deadline(timeout);
    let session = tokio::time::timeout_at(deadline, connect_over(transport, binding))
        .await
        .map_err(|_| timed_out(&server, timeout))??;
    sync_over_session(session, binding, &server, synced_at, &[], deadline, timeout).await
}
```

5. In `sync_over_session`, add `deadline: tokio::time::Instant,` and `timeout: Duration,` as its last two parameters, add `#[allow(clippy::too_many_arguments)]` above it only if clippy asks for it (seven parameters is the threshold's edge), and replace its first statement `let listed = session.list_all_tools().await;` with:

```rust
    // Only the listing is awaited under the deadline. The close below is
    // owed whether or not it fired, and is bounded on its own terms.
    let listed = tokio::time::timeout_at(deadline, session.list_all_tools()).await;
```

and replace the start of the `built` chain so an elapsed deadline becomes the refusal:

```rust
    let built = listed
        .map_err(|_| timed_out(server, timeout))
        .and_then(|listed| {
            listed.map_err(|error| McpSyncError::Client(service_error(server, &error)))
        })
        .and_then(|listed| {
            listed.ok_or_else(|| {
                McpSyncError::Client(McpClientError::Protocol {
                    server: server.to_string(),
                    reason: format!("tools/list did not end within {MCP_LIST_PAGES_MAX} pages"),
                })
            })
        })
        .and_then(|listed| synced_descriptors(binding, server, &listed, secrets));
```

Everything after `built` stays as Task 2 left it.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p rakka-agent-mcp --features testkit --test sync_deadline` (timeout 600000)
Expected: PASS, 4 tests.

Run: `cargo test -p rakka-agent-mcp --all-features` (timeout 600000)
Expected: PASS.

- [ ] **Step 6: Lint and commit**

Run: `cargo fmt --all -- --check` and `cargo clippy -p rakka-agent-mcp --all-targets --all-features -- -D warnings` (timeout 600000)
Expected: both exit 0.

```bash
git add crates/rakka-agent-mcp/src/binding.rs crates/rakka-agent-mcp/src/sync.rs \
        crates/rakka-agent-mcp/src/lib.rs crates/rakka-agent-mcp/tests/sync_deadline.rs
git commit -m "Bound every descriptor sync: a thirty-second default over the handshake and the listing, and a twin that takes the caller's own

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

# Part B — Issue #80: the deciding code on the run's own records

The requirement is the host's brief (path in the header). Read its sections 4, 6, 7, and 9 before starting Task 4. Its section 10 asks for a report that quotes each proof's failure before the fix: every task of this part keeps the output of its own "verify they fail" step in its task report, and Task 12 assembles them. Eleven facts in the brief differ from the code at `ebc7147`; each is resolved in the task that meets it and listed for the report in Task 12. The two that change what is built: the run bounds a code at 512 bytes, not 128, so this slice bounds the **reason** at 128 and leaves every existing `code` exactly as it is bounded today; and a handoff block never reaches `EffectFailed`, so the handoff cell carries the reason too (decision D9).

### Task 4: `AgentFailureReason`, the outcome's constructors, and the reason-code constants

No behavior changes in this task. It adds the type and the field, turns every outcome struct literal into a constructor call so later tasks can set a reason without touching sixty sites, and names the four guardrail reason codes that exist only as bare literals.

**Files:**
- Create: `crates/rakka-agent/src/failure.rs`
- Modify: `crates/rakka-agent/src/effect.rs` (`AgentRunEffectOutcome` :2261, its `impl` :2354)
- Modify: `crates/rakka-agent/src/lib.rs` (module list :54-95, re-exports)
- Modify: `crates/rakka-agent/src/guardrails/builtin.rs` (:64, :139, :190, and its unit tests), `crates/rakka-agent/src/guardrails.rs` (:805)
- Modify, mechanically: `crates/rakka-agent/src/dispatch.rs` (23 sites), `crates/rakka-agent/src/testkit.rs` (32), `crates/rakka-agent/src/run.rs` (6), `crates/rakka-agent/tests/communal_claim_append.rs` (3), `crates/rakka-agent/tests/private_memory_promotion.rs` (1), `crates/rakka-agent/tests/fan_out_fan_in.rs` (1)

**Interfaces:**
- Produces, all re-exported from the crate root:
  - `pub const AGENT_FAILURE_REASON_CODE_MAX_LENGTH: usize = 128;`
  - `pub struct AgentFailureReason` (private fields; `Debug, Clone, PartialEq, Eq, Serialize, Deserialize`) with `pub fn new(code: impl AsRef<str>) -> Option<Self>` (`None` for a blank code), `pub fn guardrail(stage: AgentGuardrailStageId, reason_code: impl AsRef<str>) -> Self`, `pub fn with_stage(self, stage: AgentGuardrailStageId) -> Self`, `pub fn code(&self) -> &str`, `pub fn stage(&self) -> Option<&AgentGuardrailStageId>`
  - `AgentRunEffectOutcome::Failed { code, message, reason: Option<AgentFailureReason> }` and `Exhausted { code, message, reason }`
  - `AgentRunEffectOutcome::failed(code: impl Into<String>, message: impl Into<String>) -> Self`, `exhausted(code, message) -> Self`, `with_reason(self, reason: Option<AgentFailureReason>) -> Self`, `failure_reason(&self) -> Option<&AgentFailureReason>`
  - `pub const AGENT_GUARDRAIL_REASON_TEXT_TOO_LONG: &str = "text-too-long";`, `AGENT_GUARDRAIL_REASON_DENIED_SUBSTRING: &str = "denied-substring";`, `AGENT_GUARDRAIL_REASON_UNDECLARED_TOOL_CALL: &str = "undeclared-tool-call";` (in `guardrails::builtin`), and `AGENT_GUARDRAIL_REASON_TRANSFORM_OVERSIZED: &str = "guardrail-transform-oversized";` (in `guardrails`)

- [ ] **Step 1: Write the failing unit tests for the reason**

Create `crates/rakka-agent/src/failure.rs` with only its tests and the imports they need, so the first run fails on the missing type:

```rust
//! The deciding identity of a failure.

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{AgentFailureReason, AGENT_FAILURE_REASON_CODE_MAX_LENGTH};
    use crate::guardrails::AgentGuardrailStageId;

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
```

Add `pub mod failure;` to `crates/rakka-agent/src/lib.rs`, between `pub mod evaluation;` and `pub mod events;`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p rakka-agent --lib failure::` (timeout 600000)
Expected: FAIL to compile: `AgentFailureReason` and `AGENT_FAILURE_REASON_CODE_MAX_LENGTH` are unresolved.

- [ ] **Step 3: Write the type**

Put this above the `#[cfg(test)]` block in `crates/rakka-agent/src/failure.rs`, replacing the one-line module doc:

```rust
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

use crate::guardrails::AgentGuardrailStageId;

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
```

Add to `crates/rakka-agent/src/lib.rs`, in alphabetical position among the `pub use` lines: `pub use failure::{AgentFailureReason, AGENT_FAILURE_REASON_CODE_MAX_LENGTH};`.

- [ ] **Step 4: Run the reason's tests to verify they pass**

Run: `cargo test -p rakka-agent --lib failure::` (timeout 600000)
Expected: PASS, 5 tests.

- [ ] **Step 5: Write the failing unit tests for the outcome**

In the `#[cfg(test)] mod tests` of `crates/rakka-agent/src/effect.rs`, add:

```rust
    #[test]
    fn an_outcome_written_before_the_reason_decodes_with_none() {
        for (tag, outcome) in [
            ("failed", AgentRunEffectOutcome::failed("c", "m")),
            ("exhausted", AgentRunEffectOutcome::exhausted("c", "m")),
        ] {
            let old = serde_json::json!({ tag: { "code": "c", "message": "m" } });
            let decoded: AgentRunEffectOutcome =
                serde_json::from_value(old.clone()).expect("decodes");
            assert_eq!(decoded, outcome);
            assert_eq!(decoded.failure_reason(), None);
            assert_eq!(
                serde_json::to_value(&outcome).expect("encodes"),
                old,
                "an outcome without a reason serializes as it always did"
            );
        }
    }

    #[test]
    fn a_reason_rides_a_failed_and_an_exhausted_outcome_and_nothing_else() {
        let reason = crate::failure::AgentFailureReason::new("egress_denied");
        for outcome in [
            AgentRunEffectOutcome::failed("c", "m"),
            AgentRunEffectOutcome::exhausted("c", "m"),
        ] {
            let carried = outcome.with_reason(reason.clone());
            assert_eq!(carried.failure_reason(), reason.as_ref());
            assert_eq!(carried.failure_code(), Some("c"));
            let decoded: AgentRunEffectOutcome = serde_json::from_value(
                serde_json::to_value(&carried).expect("encodes"),
            )
            .expect("decodes");
            assert_eq!(decoded, carried);
        }
        let cancelled = AgentRunEffectOutcome::Cancelled {
            reason: "fenced".to_string(),
        };
        assert_eq!(cancelled.clone().with_reason(reason), cancelled);
    }
```

Run: `cargo test -p rakka-agent --lib effect::tests::an_outcome_written_before` (timeout 600000)
Expected: FAIL to compile: no function `failed`, `exhausted`, `with_reason`, or `failure_reason`.

- [ ] **Step 6: Add the field and the constructors**

In `crates/rakka-agent/src/effect.rs`, replace the `Failed` and `Exhausted` variants with:

```rust
    /// The generation failed definitively.
    Failed {
        /// Stable machine-readable code: the pipeline's.
        code: String,
        /// Human-readable detail.
        message: String,
        /// Which decision failed it, when one party decided: a guardrail's
        /// stage and reason code, a collaborator's own code. Observability
        /// only; see [`AgentFailureReason`](crate::failure::AgentFailureReason).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<crate::failure::AgentFailureReason>,
    },
    /// The generation's retry budget was spent without a result.
    Exhausted {
        /// Stable machine-readable code of the last failure: the pipeline's.
        code: String,
        /// Human-readable detail.
        message: String,
        /// Which decision failed the last attempt, when one party decided.
        /// Observability only.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<crate::failure::AgentFailureReason>,
    },
```

and add to `impl AgentRunEffectOutcome`, above `is_completed`:

```rust
    /// A definitive failure with no deciding identity beyond its code.
    #[must_use]
    pub fn failed(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Failed {
            code: code.into(),
            message: message.into(),
            reason: None,
        }
    }

    /// A spent retry budget with no deciding identity beyond its code.
    #[must_use]
    pub fn exhausted(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Exhausted {
            code: code.into(),
            message: message.into(),
            reason: None,
        }
    }

    /// Sets which decision failed a `Failed` or `Exhausted` outcome. Every
    /// other outcome is returned unchanged: nothing failed it.
    #[must_use]
    pub fn with_reason(mut self, deciding: Option<crate::failure::AgentFailureReason>) -> Self {
        if let Self::Failed { reason, .. } | Self::Exhausted { reason, .. } = &mut self {
            *reason = deciding;
        }
        self
    }

    /// Which decision failed the generation, when the outcome carries one.
    #[must_use]
    pub fn failure_reason(&self) -> Option<&crate::failure::AgentFailureReason> {
        match self {
            Self::Failed { reason, .. } | Self::Exhausted { reason, .. } => reason.as_ref(),
            _ => None,
        }
    }
```

- [ ] **Step 7: Turn every struct literal into a constructor call**

Run: `cargo check -p rakka-agent --all-targets --all-features 2>&1 | grep -E '^error' | sort | uniq -c` (timeout 600000)
Expected: only `E0063` (missing field `reason`) and `E0027` (pattern does not mention field `reason`). The compiler's list is the checklist; the counts per file are 32 in `src/testkit.rs`, 23 in `src/dispatch.rs`, 6 in `src/run.rs`, 3 in `tests/communal_claim_append.rs`, 1 in `tests/private_memory_promotion.rs`, 1 in `tests/fan_out_fan_in.rs`.

Apply exactly these three rewrites, and no other change, at every site:

| Site shape | Becomes |
| --- | --- |
| `AgentRunEffectOutcome::Failed { code: A, message: B }` | `AgentRunEffectOutcome::failed(A, B)` |
| `AgentRunEffectOutcome::Exhausted { code: A, message: B }` | `AgentRunEffectOutcome::exhausted(A, B)` |
| `AgentRunEffectOutcome::Failed { code, message }` (field shorthand, as a **constructor**) | `AgentRunEffectOutcome::failed(code, message)` |

`A` and `B` keep their expressions exactly, including `.to_string()` and `format!(…)`; the constructors take `impl Into<String>`, so a `String` and a `&str` both pass. A **pattern** that binds both fields and nothing else (`Failed { code, message } =>`) gains `..`. In `src/run.rs` the three constructors are the synthetic outcomes at :5247 (`denial`), :5282 (`abandonment`), and :5813 (`expiry`); the three patterns at :4443, :4444, and :4472 already end in `..` and do not change.

Two arms in `src/dispatch.rs` are left as struct-literal **patterns** for Task 5 to extend, and only their constructor half is rewritten now: the A2A send finding at :3691-3693 and the handoff finding at :3719-3721 become `Ok(AgentRunEffectOutcome::failed(code, message))`.

- [ ] **Step 8: Name the four reason codes**

In `crates/rakka-agent/src/guardrails/builtin.rs`, below `AGENT_BUILTIN_DENY_MAX_ENTRY_BYTES`:

```rust
/// The reason code [`MaxTextLength`] blocks under.
pub const AGENT_GUARDRAIL_REASON_TEXT_TOO_LONG: &str = "text-too-long";
/// The reason code [`DenySubstrings`] blocks under.
pub const AGENT_GUARDRAIL_REASON_DENIED_SUBSTRING: &str = "denied-substring";
/// The reason code [`RequireResultTool`] blocks under.
pub const AGENT_GUARDRAIL_REASON_UNDECLARED_TOOL_CALL: &str = "undeclared-tool-call";
```

Replace the three literals at :64, :139, and :190 with `AGENT_GUARDRAIL_REASON_TEXT_TOO_LONG.to_string()`, `AGENT_GUARDRAIL_REASON_DENIED_SUBSTRING.to_string()`, and `AGENT_GUARDRAIL_REASON_UNDECLARED_TOOL_CALL.to_string()`, and replace the same three strings in that file's own unit tests with the constants.

In `crates/rakka-agent/src/guardrails.rs`, below `AGENT_GUARDRAIL_REASON_MAX_LENGTH`:

```rust
/// The reason code the chain itself blocks under when a stage's transform
/// grew the content past the boundary's bound. The one reason code a chain
/// authors rather than a stage.
pub const AGENT_GUARDRAIL_REASON_TRANSFORM_OVERSIZED: &str = "guardrail-transform-oversized";
```

and replace the literal at :805 with `AGENT_GUARDRAIL_REASON_TRANSFORM_OVERSIZED.to_string()`.

Add the three to the `pub use guardrails::builtin::{…}` list in `lib.rs` and the fourth to the `pub use guardrails::{…}` list.

- [ ] **Step 9: Run the crate's tests**

Run: `cargo test -p rakka-agent --lib` (timeout 600000)
Expected: PASS, including the two outcome tests of Step 5 and the five of Step 1.

Run: `cargo test -p rakka-agent --all-features --test effect_dispatch --test communal_claim_append --test private_memory_promotion --test fan_out_fan_in --test model_response_guardrails` (timeout 600000)
Expected: PASS, with the counts these files had before the task.

Run: `cargo check --workspace --all-targets --all-features` (timeout 600000)
Expected: exit 0. An example or another crate that constructs the outcome by literal would fail here; none does at `ebc7147`.

- [ ] **Step 10: Lint and commit**

Run: `cargo fmt --all -- --check` and `cargo clippy -p rakka-agent --all-targets --all-features -- -D warnings` (timeout 600000)
Expected: both exit 0.

```bash
git add crates/rakka-agent/src/failure.rs crates/rakka-agent/src/effect.rs \
        crates/rakka-agent/src/lib.rs crates/rakka-agent/src/guardrails.rs \
        crates/rakka-agent/src/guardrails/builtin.rs crates/rakka-agent/src/dispatch.rs \
        crates/rakka-agent/src/testkit.rs crates/rakka-agent/src/run.rs \
        crates/rakka-agent/tests/communal_claim_append.rs \
        crates/rakka-agent/tests/private_memory_promotion.rs \
        crates/rakka-agent/tests/fan_out_fan_in.rs
git commit -m "Give a failed effect's outcome a place for the decision that failed it, and name the guardrail reason codes that were bare literals

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

### Task 5: The reason travels from the refusal to the outcome

**Files:**
- Modify: `crates/rakka-agent/src/tools.rs` (`AgentAuthorityRefusal` :1128-1158, `refuse_guardrail_disposition` :3260-3294)
- Modify: `crates/rakka-agent/src/dispatch.rs` (`AgentA2aSendFinding::Refused` :600, `AgentA2aHandoffFinding::Refused` :673, `reviewed_tool_outcome` :3452, `reviewed_model_outcome` :3525, the two finding arms :3691 and :3719, `refuse_dispatch` :3955)
- Modify: `crates/rakka-agent/src/testkit.rs` (the scripted dispatcher's two finding arms, :2533 and :2583)
- Modify: `crates/rakka-a2a/src/agents/error.rs` (:52, :123), `service.rs` (:465, :1248, :1789, :1792), `ingress.rs` (:211, :232, :248, :331, :436, :452), `client.rs` (:210), `delegation.rs` (:254, :274, :285, :294, :334, :355), `handoff.rs` (:376, :387, :393, :402, :471)
- Test: `crates/rakka-agent/tests/model_response_guardrails.rs`, `crates/rakka-agent/tests/tool_authority.rs`, `crates/rakka-a2a/tests/ingress_egress_guardrails.rs`; unit tests inline in `tools.rs`

**Interfaces:**
- Consumes: Task 4's `AgentFailureReason::guardrail(stage, reason_code)` and `AgentRunEffectOutcome::failed(..).with_reason(..)`.
- Produces:
  - `AgentAuthorityRefusal.reason: Option<AgentFailureReason>` (public field, serialized only when present) and `AgentAuthorityRefusal::with_reason(self, reason: Option<AgentFailureReason>) -> Self`. `of` and `transient` set `None`.
  - `refuse_guardrail_disposition` keeps its signature and fills `reason` for `Blocked` and for an unsatisfied `CheckpointRequired`. It has five call sites — `ToolResponse` (`tools.rs:1334`), `ModelResponse` (:1407), `ToolRequest` (:2281), `ModelRequest` (:2462), and the A2A pair (`rakka-a2a/src/agents/guardrails.rs:154`) — and none of them changes.
  - `AgentA2aSendFinding::Refused { code, message, reason: Option<AgentFailureReason> }`, `AgentA2aHandoffFinding::Refused { code, message, reason }`, `RakkaAgentA2AError::Refused { code, message, reason }`.
  - Every refusal-to-outcome mapping in the dispatcher passes the reason on.

- [ ] **Step 1: Write the failing tests**

In `crates/rakka-agent/tests/model_response_guardrails.rs`, append to the body of `a_blocking_stage_refuses_under_guardrail_blocked_with_the_stage_and_reason_in_the_message` (:284), after its two existing assertions:

```rust
    let reason = refusal
        .reason
        .as_ref()
        .expect("a guardrail block names its decision");
    assert_eq!(reason.stage(), Some(&stage_id("response-filter")));
    assert_eq!(
        reason.code(),
        "prompt-injection",
        "the stage's own reason code, not the pipeline's"
    );
```

In the same file, add after `a_checkpoint_requiring_stage_fails_closed_under_checkpoint_required` (:370):

```rust
#[test]
fn a_checkpoint_requiring_stage_names_its_decision_too() {
    let authority = authority_with(Arc::new(RequireHuman));
    let refusal = authority
        .review_model_response(&run_scope(), text_turn("hello"))
        .expect_err("no checkpoint can gate a response that already exists");
    assert_eq!(refusal.code, "checkpoint-required");
    let reason = refusal.reason.expect("the requiring stage is named");
    assert_eq!(reason.stage(), Some(&stage_id("response-filter")));
    assert!(!reason.code().is_empty());
}

#[test]
fn a_refusal_no_guardrail_decided_carries_no_reason() {
    let authority = authority_with(Arc::new(InventToolCall));
    let refusal = authority
        .review_model_response(&run_scope(), tool_calling_turn())
        .expect_err("an invented call id is refused");
    assert_eq!(refusal.code, "guardrail-transform-invalid");
    assert_eq!(
        refusal.reason, None,
        "the authority refused the transform; no stage blocked anything"
    );
}
```

In `crates/rakka-a2a/tests/ingress_egress_guardrails.rs`, add this helper below `chain_at` (:114):

```rust
/// The stage a finding or an error names, as text.
fn named_stage(reason: Option<&rakka_agent::AgentFailureReason>) -> Option<String> {
    reason
        .and_then(rakka_agent::AgentFailureReason::stage)
        .map(ToString::to_string)
}
```

Then add these assertions, keeping every assertion each test already makes.

In `an_ingress_block_reaches_an_in_process_executor_as_a_refused_finding` (:660) and in `an_egress_block_refuses_the_delegation_send_before_the_service_sees_it` (:685), directly after the existing `assert!(matches!(&finding, AgentA2aSendFinding::Refused { .. }))`:

```rust
    let AgentA2aSendFinding::Refused { reason, .. } = &finding else {
        unreachable!("asserted above")
    };
    assert_eq!(
        named_stage(reason.as_ref()).as_deref(),
        Some("a2a-filter"),
        "the blocking stage is named on the finding"
    );
    assert_eq!(
        reason.as_ref().map(rakka_agent::AgentFailureReason::code),
        Some("prompt-injection")
    );
```

In `an_egress_block_refuses_the_handoff_send` (:747), directly after its existing `assert!(matches!(&finding, AgentA2aHandoffFinding::Refused { .. }))`, the same block over `AgentA2aHandoffFinding::Refused`.

In `an_ingress_block_refuses_the_send_and_creates_nothing` (:399), inside the `for` loop, directly after the existing `assert!(matches!(&error, RakkaAgentA2AError::Refused { .. }))`:

```rust
        let RakkaAgentA2AError::Refused { reason, .. } = &error else {
            unreachable!("asserted above")
        };
        assert_eq!(
            named_stage(reason.as_ref()).as_deref(),
            Some("a2a-filter"),
            "the ingress block names its stage on the error an in-process caller reads"
        );
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p rakka-agent --test model_response_guardrails` (timeout 600000)
Expected: FAIL to compile: no field `reason` on `AgentAuthorityRefusal`.

- [ ] **Step 3: Carry the reason on the refusal**

In `crates/rakka-agent/src/tools.rs`, replace the struct and its `impl` (:1127-1158) with:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AgentAuthorityRefusal {
    /// Stable machine-readable reason code: the pipeline's.
    pub code: String,
    /// Human-readable detail.
    pub message: String,
    /// Whether the refusing condition may clear without a new definition,
    /// setup, or reconfiguration.
    pub retryable: bool,
    /// Which decision refused, when one party decided: a guardrail's stage
    /// and reason code. It reaches the failed effect's outcome and the run's
    /// records beside `code`; the message does not.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<crate::failure::AgentFailureReason>,
}

impl AgentAuthorityRefusal {
    /// A definitive refusal.
    #[must_use]
    pub fn of(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            retryable: false,
            reason: None,
        }
    }

    /// A refusal whose condition may clear.
    #[must_use]
    pub fn transient(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            retryable: true,
            reason: None,
        }
    }

    /// Names the decision that refused.
    #[must_use]
    pub fn with_reason(mut self, reason: Option<crate::failure::AgentFailureReason>) -> Self {
        self.reason = reason;
        self
    }
}
```

In `refuse_guardrail_disposition`, replace the two `Err(…)` expressions:

```rust
            Err(AgentAuthorityRefusal::of(
                "guardrail-blocked",
                format!("guardrail stage {stage} blocked {what}: {reason_code}{evidence}"),
            )
            .with_reason(Some(crate::failure::AgentFailureReason::guardrail(
                stage.clone(),
                reason_code,
            ))))
```

```rust
            Err(AgentAuthorityRefusal::of(
                "checkpoint-required",
                format!(
                    "guardrail stage {stage} requires a checkpoint grant, and none binds this \
                     intent: {reason_code}"
                ),
            )
            .with_reason(Some(crate::failure::AgentFailureReason::guardrail(
                stage.clone(),
                reason_code,
            ))))
```

and add to its doc comment, before `# Errors`: "The refusal names the deciding stage and its reason code in `reason`, so the identity survives to the failed effect's record; the message keeps them too, and the block's evidence reference stays in the message only."

- [ ] **Step 4: Carry the reason on the two findings and into the outcome**

In `crates/rakka-agent/src/dispatch.rs`:

1. Add to both `Refused` variants (`AgentA2aSendFinding` :600, `AgentA2aHandoffFinding` :673), after `message`:

```rust
        /// Which decision refused, when one party decided: the guardrail
        /// stage and reason code of an egress or ingress block.
        reason: Option<crate::failure::AgentFailureReason>,
```

2. The send finding's arm (:3691) becomes two arms:

```rust
                    AgentA2aSendFinding::Conflict { code, message } => {
                        Ok(AgentRunEffectOutcome::failed(code, message))
                    }
                    AgentA2aSendFinding::Refused {
                        code,
                        message,
                        reason,
                    } => Ok(AgentRunEffectOutcome::failed(code, message).with_reason(reason)),
```

and the handoff finding's arm (:3719) the same, over `AgentA2aHandoffFinding`.

3. In `reviewed_tool_outcome` (:3459), `reviewed_model_outcome` (:3532), and `refuse_dispatch` (:3972), the outcome becomes:

```rust
                AgentRunEffectOutcome::failed(
                    bounded_failure_code(&refusal.code),
                    bounded_failure_detail(&refusal.message),
                )
                .with_reason(refusal.reason.clone())
```

keeping the `Ok(…)` wrapper the first two have and the comment `refuse_dispatch` has.

4. `crates/rakka-agent/src/testkit.rs` maps the same two findings for the in-process `ScriptedDispatcher` (:2533-2534 and :2583-2584). Split each pair of arms exactly as items 2 above does, so a scripted executor's `Refused { reason }` reaches the outcome through the in-process driver as it does through the real one:

```rust
                        Ok(crate::dispatch::AgentA2aSendFinding::Conflict { code, message }) => {
                            AgentRunEffectOutcome::failed(code, message)
                        }
                        Ok(crate::dispatch::AgentA2aSendFinding::Refused {
                            code,
                            message,
                            reason,
                        }) => AgentRunEffectOutcome::failed(code, message).with_reason(reason),
```

Keep whatever each existing arm does besides building the outcome; only the outcome expression and the pattern change. The handoff pair is the same over `AgentA2aHandoffFinding`.

- [ ] **Step 5: Carry the reason through the A2A surface**

In `crates/rakka-a2a/src/agents/error.rs`, add to `Refused` (:52), after `message`:

```rust
        /// Which decision refused, when one party decided: the guardrail
        /// stage and reason code of an ingress block. In-process only; the
        /// wire carries the code and the message.
        reason: Option<rakka_agent::AgentFailureReason>,
```

and change the `Display` arm (:123) to `Self::Refused { code, message, .. } => …`.

Run: `cargo check -p rakka-a2a --features agents --all-targets 2>&1 | grep -E '^error' | sort | uniq -c` (timeout 600000)
Expected: `E0063` and `E0027` only. Resolve each by this table, and no other change:

| Site | Change |
| --- | --- |
| `service.rs:465`, the `map_err` in `admit_ingress` | add `reason: refusal.reason,` |
| `service.rs:1248`, `:1789`, `:1792`; `ingress.rs:211`, `:232`, `:248`, `:331`, `:436`, `:452` | add `reason: None,` (at `service.rs:1789`, the shorthand literal becomes `RakkaAgentA2AError::Refused { code, message, reason: None }`) |
| `client.rs:210`, a pattern | add `..` |
| `delegation.rs:285` and `handoff.rs:393`, the patterns that turn the service's error into a finding | bind `reason` and pass it: `RakkaAgentA2AError::Refused { code, message, reason } => Ok(AgentA2aSendFinding::Refused { code, message, reason })` (and the handoff twin) |
| `delegation.rs:355` and `handoff.rs:471`, the egress block | add `reason: refusal.reason,` |
| `delegation.rs:254`, `:274`, `:294`, `:334`; `handoff.rs:376`, `:387`, `:402` | add `reason: None,` |

`crates/rakka-a2a/src/agents/guardrails.rs:154` calls `refuse_guardrail_disposition` and returns its refusal unchanged; it needs no edit.

- [ ] **Step 6: Add the unit tests on the mapping**

In the `#[cfg(test)] mod tests` of `crates/rakka-agent/src/tools.rs`, add:

```rust
    #[test]
    fn a_disposition_maps_its_stage_and_reason_onto_the_refusal() {
        let stage = AgentGuardrailStageId::new("pii-filter").expect("id");
        let blocked = AgentGuardrailDisposition::Blocked {
            stage: stage.clone(),
            reason_code: "denied-substring".to_string(),
            evidence: None,
        };
        let refusal = refuse_guardrail_disposition(&blocked, "the call", false)
            .expect_err("a block refuses");
        assert_eq!(refusal.code, "guardrail-blocked");
        let reason = refusal.reason.expect("named");
        assert_eq!(reason.stage(), Some(&stage));
        assert_eq!(reason.code(), "denied-substring");

        let gated = AgentGuardrailDisposition::CheckpointRequired {
            stage: stage.clone(),
            reason_code: "needs-approval".to_string(),
        };
        let refusal = refuse_guardrail_disposition(&gated, "the call", false)
            .expect_err("no grant binds the intent");
        assert_eq!(refusal.code, "checkpoint-required");
        assert_eq!(
            refusal.reason.as_ref().map(|reason| reason.code()),
            Some("needs-approval")
        );
        assert!(refuse_guardrail_disposition(&gated, "the call", true).is_ok());
        assert!(
            refuse_guardrail_disposition(&AgentGuardrailDisposition::Allowed, "the call", false)
                .is_ok()
        );
    }

    #[test]
    fn a_refusal_without_a_reason_serializes_as_it_always_did() {
        let refusal = AgentAuthorityRefusal::of("tool-undeclared", "no such tool");
        assert_eq!(
            serde_json::to_value(&refusal).expect("encodes"),
            serde_json::json!({
                "code": "tool-undeclared",
                "message": "no such tool",
                "retryable": false
            })
        );
    }
```

If the test module does not already import `AgentGuardrailDisposition` and `AgentGuardrailStageId`, add `use crate::guardrails::{AgentGuardrailDisposition, AgentGuardrailStageId};` inside it.

- [ ] **Step 7: Run the tests to verify they pass**

Run: `cargo test -p rakka-agent --lib tools::tests` and `cargo test -p rakka-agent --test model_response_guardrails --test tool_authority` (timeout 600000 each)
Expected: PASS.

Run: `cargo test -p rakka-a2a --all-features --test ingress_egress_guardrails --test handoff_surface --test human_task_surface` (timeout 600000)
Expected: PASS.

Run: `cargo check --workspace --all-targets --all-features` (timeout 600000)
Expected: exit 0.

- [ ] **Step 8: Lint and commit**

Run: `cargo fmt --all -- --check` and `cargo clippy -p rakka-agent -p rakka-a2a --all-targets --all-features -- -D warnings` (timeout 600000)
Expected: both exit 0.

```bash
git add crates/rakka-agent/src/tools.rs crates/rakka-agent/src/dispatch.rs \
        crates/rakka-agent/src/testkit.rs \
        crates/rakka-agent/tests/model_response_guardrails.rs \
        crates/rakka-a2a/src/agents crates/rakka-a2a/tests/ingress_egress_guardrails.rs
git commit -m "Carry a guardrail's stage and reason code from the refusal to the failed effect's outcome, across the dispatcher and the A2A surface

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

### Task 6: The reason on the run's durable records

**Files:**
- Modify: `crates/rakka-agent/src/effect.rs` (`AgentRunEffect` :1833, `new` :1903, the new-generation reset :2005)
- Modify: `crates/rakka-agent/src/run.rs` (`AgentRunTerminalReason::EffectFailed` :474, `apply_effect_outcome` :4452, :4484, :4569, :4689; the unit-test literals at :10556 and :10586; `the_growth_reserve_covers_the_maximal_working_set` :10046)
- Modify: `crates/rakka-agent/src/delegation.rs` (`AgentDelegationStatus::Failed` :646, `settle_failed` :841)
- Modify: `crates/rakka-agent/src/coordination.rs` (`AgentHandoffStatus::Failed` :1510, `settle_failed` :1646)
- Modify: `crates/rakka-agent/src/query.rs:564` (a pattern)
- Modify: `crates/rakka-agent/tests/common/mod.rs` (one fixture method beside `terminal_failure_code` :3237)
- Create: `crates/rakka-agent/tests/failure_reason_records.rs`
- Test: `crates/rakka-agent/tests/model_response_guardrails.rs`, `tool_authority.rs`, `delegation_dispatch.rs`, `handoff_record.rs`; pattern fixes in `goal_view.rs:806`, `handoff_cancellation.rs:255`

**Interfaces:**
- Consumes: Task 4's `AgentRunEffectOutcome::failure_reason()`; Task 5's reason on the outcome.
- Produces:
  - `AgentRunEffect.last_error_reason: Option<AgentFailureReason>` (public, beside `last_error_code`)
  - `AgentRunTerminalReason::EffectFailed { effect_id, code, reason: Option<AgentFailureReason> }`
  - `AgentDelegationStatus::Failed { code, reason }` and `AgentHandoffStatus::Failed { code, reason }`
  - `AgentDelegationCell::settle_failed_because(&mut self, code: impl Into<String>, reason: Option<AgentFailureReason>, now: AgentTimestampMillis)` and the same method on the handoff cell. `settle_failed(code, now)` stays on both and means "no deciding identity".
  - Test fixture: `AuthorityFixture::terminal_failure_reason(&self) -> Option<AgentFailureReason>`.
  - Every new field is `#[serde(default, skip_serializing_if = "Option::is_none")]`. No schema version changes. The workflow-invocation cell and the `Indeterminate` outcome are not changed.

- [ ] **Step 1: Write the failing record-shape tests**

Create `crates/rakka-agent/tests/failure_reason_records.rs`:

```rust
//! The durable shape of a failure's deciding identity: every record that
//! gains a reason decodes without one, serializes without one exactly as it
//! did before the field existed, and round-trips with one.

mod common;

use common::run_scope;
use rakka_agent::{
    AgentDelegationStatus, AgentEffectSpec, AgentFailureReason, AgentGuardrailStageId,
    AgentHandoffStatus, AgentRevisionNumber, AgentRunEffect, AgentRunEffectRequest,
    AgentRunTerminalReason, AgentToolCallId, AgentToolCallRequest, AgentToolId,
    AGENT_FAILURE_REASON_CODE_MAX_LENGTH, AGENT_IDENTITY_MAX_LENGTH,
};
use rakka_agent_workflow::AgentTimestampMillis;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{json, Value};

fn reason() -> AgentFailureReason {
    AgentFailureReason::guardrail(
        AgentGuardrailStageId::new("response-filter").expect("id"),
        "prompt-injection",
    )
}

fn effect() -> AgentRunEffect {
    AgentRunEffect::new(
        &run_scope(),
        1,
        1,
        AgentRunEffectRequest::Tool {
            call: Box::new(
                AgentToolCallRequest::new(
                    AgentToolCallId::new("call-1").expect("id"),
                    AgentToolId::new("charge-card").expect("id"),
                    json!({}),
                )
                .expect("call"),
            ),
        },
        &AgentEffectSpec::read_only(),
        AgentRevisionNumber::INITIAL,
        AgentTimestampMillis::new(1),
    )
    .expect("the effect derives")
}

/// The object a record's variant serializes to: `{"<tag>": {…}}`.
fn body<'a>(encoded: &'a Value, tag: &str) -> &'a serde_json::Map<String, Value> {
    encoded
        .get(tag)
        .and_then(Value::as_object)
        .unwrap_or_else(|| panic!("expected {tag} in {encoded}"))
}

/// Without a reason: no key is written, and what was written decodes back.
/// With one: it round-trips. Together: a record persisted before the field
/// existed is indistinguishable from one persisted without a reason now.
#[track_caller]
fn assert_additive<T>(without: &T, with: &T, tag: &str, field: &str)
where
    T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let encoded = serde_json::to_value(without).expect("encodes");
    assert!(
        !body(&encoded, tag).contains_key(field),
        "{field} is not written when it is absent: {encoded}"
    );
    assert_eq!(
        &serde_json::from_value::<T>(encoded).expect("decodes"),
        without
    );
    let encoded = serde_json::to_value(with).expect("encodes");
    assert_eq!(
        body(&encoded, tag).get(field),
        Some(&json!({"code": "prompt-injection", "stage": "response-filter"}))
    );
    assert_eq!(&serde_json::from_value::<T>(encoded).expect("decodes"), with);
}

#[test]
fn the_terminal_reason_is_additive() {
    let effect_id = effect().effect_id;
    assert_additive(
        &AgentRunTerminalReason::EffectFailed {
            effect_id: effect_id.clone(),
            code: "guardrail-blocked".to_string(),
            reason: None,
        },
        &AgentRunTerminalReason::EffectFailed {
            effect_id,
            code: "guardrail-blocked".to_string(),
            reason: Some(reason()),
        },
        "effect-failed",
        "reason",
    );
}

#[test]
fn the_two_cells_are_additive() {
    assert_additive(
        &AgentDelegationStatus::Failed {
            code: "guardrail-blocked".to_string(),
            reason: None,
        },
        &AgentDelegationStatus::Failed {
            code: "guardrail-blocked".to_string(),
            reason: Some(reason()),
        },
        "failed",
        "reason",
    );
    assert_additive(
        &AgentHandoffStatus::Failed {
            code: "guardrail-blocked".to_string(),
            reason: None,
        },
        &AgentHandoffStatus::Failed {
            code: "guardrail-blocked".to_string(),
            reason: Some(reason()),
        },
        "failed",
        "reason",
    );
}

#[test]
fn the_effect_record_is_additive() {
    let plain = effect();
    let encoded = serde_json::to_value(&plain).expect("encodes");
    assert!(
        !encoded
            .as_object()
            .expect("an object")
            .contains_key("last_error_reason"),
        "{encoded}"
    );
    assert_eq!(
        serde_json::from_value::<AgentRunEffect>(encoded).expect("decodes"),
        plain
    );

    let mut failed = effect();
    failed.last_error_code = Some("guardrail-blocked".to_string());
    failed.last_error_reason = Some(reason());
    let encoded = serde_json::to_value(&failed).expect("encodes");
    assert_eq!(
        encoded.get("last_error_reason"),
        Some(&json!({"code": "prompt-injection", "stage": "response-filter"}))
    );
    assert_eq!(
        serde_json::from_value::<AgentRunEffect>(encoded).expect("decodes"),
        failed
    );
}

#[test]
fn a_maximal_reason_is_bounded() {
    let maximal = AgentFailureReason::guardrail(
        AgentGuardrailStageId::new("s".repeat(AGENT_IDENTITY_MAX_LENGTH)).expect("id"),
        "c".repeat(AGENT_FAILURE_REASON_CODE_MAX_LENGTH * 4),
    );
    let bytes = serde_json::to_vec(&maximal).expect("encodes").len();
    assert!(
        bytes <= AGENT_IDENTITY_MAX_LENGTH + AGENT_FAILURE_REASON_CODE_MAX_LENGTH + 32,
        "a reason costs a record at most its two bounds and its keys: {bytes}"
    );
}
```

`run_scope` is `tests/common/mod.rs`'s. Every other name is re-exported at the crate root.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p rakka-agent --test failure_reason_records` (timeout 600000)
Expected: FAIL to compile: the three variants have no field `reason`, and `AgentRunEffect` has no field `last_error_reason`.

- [ ] **Step 3: Add the fields**

1. `crates/rakka-agent/src/effect.rs`, after `last_error_code` (:1833):

```rust
    /// Which decision failed the last dispatch or execution, when one party
    /// decided: a guardrail's stage and reason code, a collaborator's own
    /// code. Beside [`Self::last_error_code`], which stays the pipeline's.
    /// Observability only, never correctness: an effect persisted before
    /// this field decodes with none, and no dispatch decision reads it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error_reason: Option<crate::failure::AgentFailureReason>,
```

In `AgentRunEffect::new` (:1903) add `last_error_reason: None,` after `last_error_code: None,`. At the new-generation reset (:2005) add `self.last_error_reason = None;` after `self.last_error_code = None;`.

2. `crates/rakka-agent/src/run.rs`, `EffectFailed` (:474):

```rust
    /// A durable effect failed and the run could not continue.
    EffectFailed {
        /// The effect that failed.
        effect_id: AgentEffectId,
        /// Its stable failure code: the pipeline's.
        code: String,
        /// Which decision failed it, when one party decided. Observability
        /// only.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<crate::failure::AgentFailureReason>,
    },
```

3. `crates/rakka-agent/src/delegation.rs`, `Failed` (:646):

```rust
    /// The send failed definitively without creating a child.
    Failed {
        /// Stable machine-readable failure code.
        code: String,
        /// Which decision failed the send, when one party decided: the
        /// guardrail stage and reason code of an egress or ingress block.
        /// Observability only.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<crate::failure::AgentFailureReason>,
    },
```

and replace `settle_failed` (:840-847) with:

```rust
    /// Settles the cell with a definitive failure no single party decided.
    pub fn settle_failed(&mut self, code: impl Into<String>, now: AgentTimestampMillis) {
        self.settle_failed_because(code, None, now);
    }

    /// Settles the cell with a definitive failure and the decision behind
    /// it, first-writer-wins.
    pub fn settle_failed_because(
        &mut self,
        code: impl Into<String>,
        reason: Option<crate::failure::AgentFailureReason>,
        now: AgentTimestampMillis,
    ) {
        if self.status.is_settled() {
            return;
        }
        self.status = AgentDelegationStatus::Failed {
            code: code.into(),
            reason,
        };
        self.settled_at = Some(now);
    }
```

4. `crates/rakka-agent/src/coordination.rs`, `AgentHandoffStatus::Failed` (:1510) and its `settle_failed` (:1646): the same two edits, over `AgentHandoffStatus::Failed`. The variant's new field doc reads "Which decision failed the send, when one party decided. Observability only."

- [ ] **Step 4: Write the reason where the pipeline code is written**

In `apply_effect_outcome`, `crates/rakka-agent/src/run.rs`. `outcome` is the function's own parameter and stays readable throughout the arm.

1. After `effect.last_error_code = Some(bounded_detail(code.clone()));` (:4452):

```rust
            effect.last_error_reason = outcome.failure_reason().cloned();
```

2. The handoff cell (:4484) becomes:

```rust
                        cell.settle_failed_because(
                            bounded_detail(code.clone()),
                            outcome.failure_reason().cloned(),
                            now,
                        );
```

3. The delegation cell (:4569) becomes:

```rust
                            cell.settle_failed_because(
                                code.clone(),
                                outcome.failure_reason().cloned(),
                                now,
                            );
```

4. The terminal reason (:4689) becomes:

```rust
                    run.terminal_reason = Some(AgentRunTerminalReason::EffectFailed {
                        effect_id: effect_id.clone(),
                        code: bounded_detail(code),
                        reason: outcome.failure_reason().cloned(),
                    });
```

No `code` anywhere is bounded differently than it is today.

- [ ] **Step 5: Resolve the compile errors the fields cause**

Run: `cargo check -p rakka-agent --all-targets --all-features 2>&1 | grep -E '^error' | sort | uniq -c` (timeout 600000)
Expected: `E0063` and `E0027` only, at these sites:

| Site | Change |
| --- | --- |
| `src/query.rs:564`, `AgentHandoffStatus::Refused { code } \| AgentHandoffStatus::Failed { code }` | the `Failed` half becomes `AgentHandoffStatus::Failed { code, .. }` |
| `src/run.rs:10556`, `:10586`, literals in unit tests | add `reason: None,` |
| `tests/goal_view.rs:806`, `AgentDelegationStatus::Failed { code } if …` | `{ code, .. }` |
| `tests/handoff_cancellation.rs:255`, `AgentHandoffStatus::Failed { code } if …` | `{ code, .. }` |
| `tests/delegation_dispatch.rs:100` and `:186`, equality literals | add `reason: None,` |

Any other site the compiler names takes the same two rules: a literal gains `reason: None,` and a pattern gains `..`.

- [ ] **Step 6: Run the record-shape tests to verify they pass**

Run: `cargo test -p rakka-agent --test failure_reason_records` (timeout 600000)
Expected: PASS, 4 tests.

- [ ] **Step 7: Write the failing end-to-end assertions**

1. In `crates/rakka-agent/tests/common/mod.rs`, after `terminal_failure_code` (:3237):

```rust
    /// Which decision failed the effect that stopped the run, when the
    /// record names one.
    pub async fn terminal_failure_reason(&self) -> Option<rakka_agent::AgentFailureReason> {
        let run = self.fx.run_snapshot().await.expect("the run exists");
        match run.terminal_reason {
            Some(AgentRunTerminalReason::EffectFailed { reason, .. }) => reason,
            other => panic!("expected an effect failure, found {other:?}"),
        }
    }
```

2. `crates/rakka-agent/tests/model_response_guardrails.rs`, in `a_blocked_model_response_ends_the_run_once_and_never_reaches_memory` (:393), after the `terminal_failure_code` assertion:

```rust
    let reason = fx
        .terminal_failure_reason()
        .await
        .expect("the run's record names the decision");
    assert_eq!(reason.stage(), Some(&stage_id("response-filter")));
    assert_eq!(reason.code(), "prompt-injection");
    let model_effect = fx.effect_at(0).await.expect("the model effect");
    assert_eq!(
        model_effect.last_error_code.as_deref(),
        Some("guardrail-blocked"),
        "the pipeline code is unchanged"
    );
    assert_eq!(model_effect.last_error_reason.as_ref(), Some(&reason));
```

and add after that test the brief's own first proof, with a built-in stage and its exported reason code:

```rust
/// A built-in stage is named on the run's records under its exported reason
/// code, and the refusal's words are in no record.
#[tokio::test]
async fn a_built_in_stage_is_named_on_the_runs_records_and_its_message_is_not() {
    let rule = rakka_agent::DenySubstrings::new(["ignore previous"]).expect("the rule is valid");
    let fx = AuthorityFixture::new(
        DeterministicModelAdapter::new().with_turn_for(1, proposing_turn(MARKER, "done")),
        authority_with(Arc::new(rule)),
        None,
    );
    fx.start().await;
    fx.pump().await;

    assert_eq!(fx.terminal_failure_code().await, "guardrail-blocked");
    let reason = fx
        .terminal_failure_reason()
        .await
        .expect("the run's record names the decision");
    assert_eq!(
        reason.code(),
        rakka_agent::AGENT_GUARDRAIL_REASON_DENIED_SUBSTRING
    );
    assert_eq!(reason.stage(), Some(&stage_id("response-filter")));

    let state = rakka_agent::load_agent_run_state(
        &fx.fx.runs,
        &run_scope(),
        &rakka_agent::AgentSchemaPolicy::default(),
    )
    .await
    .expect("the run state loads")
    .expect("the run exists");
    let encoded = serde_json::to_string(&state).expect("the run state encodes");
    assert!(encoded.contains("denied-substring"), "{encoded}");
    assert!(encoded.contains("response-filter"), "{encoded}");
    assert!(
        !encoded.contains("blocked the model response"),
        "the refusal's message reaches no record: {encoded}"
    );
}
```

Run this one test **before** Step 3's fields exist is impossible (it would not compile), so its pre-fix evidence is taken differently: on the commit before this task, run the existing `a_blocked_model_response_ends_the_run_once_and_never_reaches_memory` with one added line, `panic!("{}", serde_json::to_string(&state).unwrap())` over the loaded run state, and keep the printed record for Task 12's report. It contains `guardrail-blocked` and neither `prompt-injection` nor `response-filter`. Remove the line again.

3. `crates/rakka-agent/tests/tool_authority.rs`, in `a_blocked_tool_response_never_reaches_the_run` (:903), after the `effect.last_error_code` assertion:

```rust
    let reason = effect
        .last_error_reason
        .as_ref()
        .expect("the effect's record names the decision");
    assert_eq!(reason.stage(), Some(&stage_id("response-filter")));
    assert_eq!(reason.code(), "prompt-injection");
    assert_eq!(fx.terminal_failure_reason().await.as_ref(), Some(reason));
```

and in `a_guardrail_block_keeps_a_tool_call_undispatchable` (:1096), after the `invocation_count` assertion — the `ToolRequest` boundary, which settles through `refuse_dispatch`:

```rust
    let reason = fx
        .terminal_failure_reason()
        .await
        .expect("a refused dispatch names the decision too");
    assert_eq!(reason.stage(), Some(&stage_id("amount-limit")));
    assert_eq!(reason.code(), "amount-over-limit");
    let tool_effect = fx.effect_at(1).await.expect("the tool effect");
    assert_eq!(tool_effect.last_error_reason.as_ref(), Some(&reason));
```

and in `a_missing_mandatory_guardrail_stage_fails_closed_at_dispatch` (:739), after its last assertion — a refusal no guardrail stage decided:

```rust
    assert_eq!(
        fx.terminal_failure_reason().await,
        None,
        "the authority refused; no stage blocked, so no reason is invented"
    );
```

4. `crates/rakka-agent/tests/delegation_dispatch.rs`, below `ConflictExecutor`:

```rust
/// Refuses every send as an egress guardrail would: the pipeline code, and
/// the stage and reason code that decided.
struct BlockedExecutor;

fn blocking_reason() -> rakka_agent::AgentFailureReason {
    rakka_agent::AgentFailureReason::guardrail(
        rakka_agent::AgentGuardrailStageId::new("a2a-filter").expect("id"),
        "prompt-injection",
    )
}

impl AgentA2aSendExecutor for BlockedExecutor {
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _intent: &'a AgentRunEffect,
        _delegation: &'a AgentDelegationRecord,
        _credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentDispatchFuture<'a, AgentA2aSendFinding> {
        Box::pin(async move {
            Ok(AgentA2aSendFinding::Refused {
                code: "guardrail-blocked".to_string(),
                message: "guardrail stage a2a-filter blocked the outbound A2A message"
                    .to_string(),
                reason: Some(blocking_reason()),
            })
        })
    }
}

/// A send a guardrail blocked settles its cell under the pipeline code, with
/// the stage and reason code beside it.
#[tokio::test]
async fn a_blocked_send_records_the_deciding_stage_on_its_cell() {
    let fixture = Fixture::new(
        ScriptedDispatcher::with_adapter(
            DeterministicModelAdapter::new().with_turn(delegating_turn()),
        )
        .with_a2a_send_executor(Arc::new(BlockedExecutor)),
    )
    .with_delegation(delegation_config());

    let (status, run_status) = drive(&fixture).await;
    assert_eq!(
        status,
        AgentDelegationStatus::Failed {
            code: "guardrail-blocked".to_string(),
            reason: Some(blocking_reason()),
        }
    );
    assert_eq!(run_status, Some(AgentRunStatus::Failed));
}
```

5. `crates/rakka-agent/tests/handoff_record.rs`. Add a constructor to `StubHandoffExecutor`, beside `recorded()`:

```rust
    /// Refuses every send as an egress guardrail would.
    fn blocked() -> Arc<Self> {
        Arc::new(Self {
            finding: AgentA2aHandoffFinding::Refused {
                code: "guardrail-blocked".to_string(),
                message: "guardrail stage a2a-filter blocked the outbound handoff message"
                    .to_string(),
                reason: Some(rakka_agent::AgentFailureReason::guardrail(
                    rakka_agent::AgentGuardrailStageId::new("a2a-filter").expect("id"),
                    "prompt-injection",
                )),
            },
            seen: Mutex::new(Vec::new()),
        })
    }
```

and this test, after `a_refused_target_restores_the_source_and_the_run_resumes`:

```rust
/// A handoff a guardrail blocked never reaches a terminal reason: the run
/// survives it. The deciding stage is on the two records that do exist, the
/// handoff cell and the send's effect.
#[tokio::test]
async fn a_blocked_handoff_records_the_deciding_stage_and_the_run_survives() {
    let fixture = handoff_fixture(
        StubHandoffExecutor::blocked(),
        vec![handoff_turn(handoff_arguments()), proposing_turn()],
    );
    create_goal_task(&fixture).await;
    fixture.pump().await.expect("the loop should converge");

    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let state = run.state().expect("state");
    assert_eq!(
        state.status(),
        Some(AgentRunStatus::Completed),
        "the source resumed past the refusal"
    );
    let source = state.run().expect("the record survives");
    let cell = source.loop_state.handoff().expect("the cell survives");
    let AgentHandoffStatus::Failed { code, reason } = &cell.status else {
        panic!("the cell settles failed, got {:?}", cell.status)
    };
    assert_eq!(code, "guardrail-blocked");
    let reason = reason.as_ref().expect("the deciding stage is recorded");
    assert_eq!(reason.code(), "prompt-injection");
    assert_eq!(
        reason.stage().map(ToString::to_string).as_deref(),
        Some("a2a-filter")
    );
    let send = source
        .loop_state
        .effects()
        .iter()
        .find(|effect| effect.effect_id == cell.record.effect)
        .expect("the send's effect");
    assert_eq!(send.last_error_code.as_deref(), Some("guardrail-blocked"));
    assert_eq!(send.last_error_reason.as_ref(), Some(reason));
    assert_eq!(source.terminal_reason, Some(AgentRunTerminalReason::ResultAccepted));
}
```

Import `AgentRunTerminalReason` in that file if it is not already imported. If the fixture's run ends under a different terminal reason than `ResultAccepted` for a completed source (read the assertion `a_refused_target_restores_the_source_and_the_run_resumes` makes about its own completed run), assert `source.status == AgentRunStatus::Completed` alone and drop the last line.

- [ ] **Step 8: Run the end-to-end tests to verify they pass**

Run: `cargo test -p rakka-agent --all-features --test model_response_guardrails --test tool_authority --test delegation_dispatch --test handoff_record --test goal_view --test handoff_cancellation` (timeout 600000)
Expected: PASS.

If an assertion of Step 7 fails with `reason` `None` where a stage is expected, the mapping of Task 5 missed that route: find which of `reviewed_tool_outcome`, `reviewed_model_outcome`, `refuse_dispatch`, or the scripted dispatcher's arm built the outcome, and add `.with_reason(..)` there. Do not change the assertion.

- [ ] **Step 9: Hold the growth reserve to the new fields**

In `the_growth_reserve_covers_the_maximal_working_set` (`crates/rakka-agent/src/run.rs:10046`), build one maximal reason after `let now = …;`:

```rust
        let maximal_reason = crate::failure::AgentFailureReason::guardrail(
            crate::guardrails::AgentGuardrailStageId::new(
                "s".repeat(crate::identity::AGENT_IDENTITY_MAX_LENGTH),
            )
            .expect("the stage id is valid"),
            "c".repeat(crate::failure::AGENT_FAILURE_REASON_CODE_MAX_LENGTH),
        );
```

and stamp it on every effect the test records, immediately before each `run.loop_state.record_effect(…)` call (the model effect and the tool effect in the loop):

```rust
        model_effect.last_error_code = Some("c".repeat(AGENT_RUN_DETAIL_MAX_LENGTH));
        model_effect.last_error_reason = Some(maximal_reason.clone());
```

(the tool effect's binding in the loop is `effect`; make it `let mut effect` and stamp it the same way).

Run: `cargo test -p rakka-agent --lib the_growth_reserve_covers_the_maximal_working_set` (timeout 600000)
Expected: PASS. If it fails, stop and report BLOCKED with the measured growth and the reserve. Do not raise `AGENT_RUN_STATE_GROWTH_RESERVE_BYTES`: that is the owner's decision.

- [ ] **Step 10: Run the crate's suites, lint, and commit**

Run: `cargo test -p rakka-agent --lib` and `cargo test -p rakka-agent --all-features --test run_entity --test effect_dispatch --test checkpoint_run --test checkpoint_reconciliation --test schema_compatibility --test secret_exclusion` (timeout 600000 each)
Expected: PASS.

Run: `cargo check --workspace --all-targets --all-features`, `cargo fmt --all -- --check`, and `cargo clippy -p rakka-agent --all-targets --all-features -- -D warnings` (timeout 600000 each)
Expected: all exit 0.

```bash
git add crates/rakka-agent/src/effect.rs crates/rakka-agent/src/run.rs \
        crates/rakka-agent/src/delegation.rs crates/rakka-agent/src/coordination.rs \
        crates/rakka-agent/src/query.rs crates/rakka-agent/tests/common/mod.rs \
        crates/rakka-agent/tests/failure_reason_records.rs \
        crates/rakka-agent/tests/model_response_guardrails.rs \
        crates/rakka-agent/tests/tool_authority.rs \
        crates/rakka-agent/tests/delegation_dispatch.rs \
        crates/rakka-agent/tests/handoff_record.rs \
        crates/rakka-agent/tests/goal_view.rs \
        crates/rakka-agent/tests/handoff_cancellation.rs
git commit -m "Record the decision that failed an effect beside its pipeline code: on the effect, the terminal reason, and the delegation and handoff cells

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

### Task 7: A collaborator's own code is the reason of an exhausted outcome

**Files:**
- Modify: `crates/rakka-agent/src/dispatch.rs` (`collaborator_code` :232, the resolution failure :2728-2768, the invocation failure :2840-2854, `record_attempt_failure` :3338-3384, the resolver trait's doc :1348-1356)
- Modify: `examples/durable-agent-acceptance/src/provider.rs:91-92` (a comment)
- Test: `crates/rakka-agent/tests/secret_exclusion.rs`, `crates/rakka-agent/tests/mcp_client_dispatch.rs`

**Interfaces:**
- Consumes: Task 4's `AgentFailureReason::new(code) -> Option<Self>` and `AgentRunEffectOutcome::exhausted(..).with_reason(..)`; Task 6's records, which already copy the reason of a `Failed` **or** an `Exhausted` outcome.
- Produces: no new public item. `record_attempt_failure` (private) gains a last parameter before `pass`: `reason: Option<AgentFailureReason>`. The run's record reads, for example, `credential-resolution-failed` / `vault-unreachable`, `dispatch-collaborator-failed` / `mcp-tool-error`, `dispatch-collaborator-failed` / `egress-denied-by-policy`.

What is persisted is a **code**. The resolver's detail stays unpersisted, exactly as the comment at `dispatch.rs:2732-2745` draws the line: the dispatcher already writes this very code on its own log line, so it has already been judged non-secret.

- [ ] **Step 1: Write the failing tests**

In `crates/rakka-agent/tests/secret_exclusion.rs`, after `a_resolver_failure_persists_its_stable_code_and_never_the_resolvers_detail` (:504):

```rust
/// The resolver's own code is recorded beside the pipeline's once the
/// retry budget is spent, and its detail still is not.
#[tokio::test]
async fn an_exhausted_resolution_records_the_resolvers_code_and_never_its_detail() {
    let fx = credentialed_fixture().with_failing_credential_resolver(
        "vault-unreachable",
        "vault said: token=RAKKA-SECRET-VAULT-DETAIL",
    );
    fx.start().await;
    fx.pump().await;

    assert_eq!(
        fx.terminal_failure_code().await,
        "credential-resolution-failed",
        "the pipeline code is unchanged"
    );
    let reason = fx
        .terminal_failure_reason()
        .await
        .expect("the resolver's own code is recorded");
    assert_eq!(reason.code(), "vault-unreachable");
    assert_eq!(reason.stage(), None, "no guardrail decided this");

    let tool_effect = fx.effect_at(1).await.expect("the tool effect");
    assert_eq!(
        tool_effect.last_error_code.as_deref(),
        Some("credential-resolution-failed")
    );
    assert_eq!(tool_effect.last_error_reason.as_ref(), Some(&reason));

    for (label, encoded) in fx.durable_surfaces().await {
        assert!(
            !encoded.contains("RAKKA-SECRET-VAULT-DETAIL"),
            "durable surface {label:?} carries the resolver's own failure text: {encoded}"
        );
    }
}
```

and after `an_executor_failure_detail_is_bounded_before_it_reaches_a_durable_row` (:549):

```rust
/// An executor's own code is recorded beside the pipeline's, bounded as a
/// code is, whatever the executor put in its message.
#[tokio::test]
async fn an_exhausted_invocation_records_the_executors_code_as_a_bounded_field() {
    let long_code = format!("executor-exploded-{}", "x".repeat(400));
    let spec = credentialed_spec();
    let adapter = DeterministicModelAdapter::new()
        .with_turn_for(1, tool_calling_turn())
        .with_turn_for(2, proposing_turn());
    let mut fx = AuthorityFixture::over(adapter, tool_registry_for_spec(TOOL, &spec), None)
        .with_credential_resolver("RAKKA-SECRET-BEARER");
    fx.tools = RecordingToolExecutor::new().with_failure(
        TOOL,
        &long_code,
        "RAKKA-SECRET-EXECUTOR-DETAIL in the message",
    );
    fx.start().await;
    fx.pump().await;

    assert_eq!(
        fx.terminal_failure_code().await,
        "dispatch-collaborator-failed"
    );
    let reason = fx
        .terminal_failure_reason()
        .await
        .expect("the executor's own code is recorded");
    assert_eq!(
        reason.code().len(),
        rakka_agent::AGENT_FAILURE_REASON_CODE_MAX_LENGTH,
        "a verbose code is cut at the code bound"
    );
    assert!(reason.code().starts_with("executor-exploded-"));
    assert!(
        !serde_json::to_string(&reason)
            .expect("encodes")
            .contains("RAKKA-SECRET-EXECUTOR-DETAIL"),
        "a reason is a code, never the message"
    );
}
```

In `crates/rakka-agent/tests/mcp_client_dispatch.rs`, in the egress scenario (`an_egress_refusal_fails_the_attempt_under_the_deployments_code_…`, :662), directly after `assert_eq!(effect.last_error_code.as_deref(), Some(COLLABORATOR_FAILED));`:

```rust
    assert_eq!(
        effect
            .last_error_reason
            .as_ref()
            .map(rakka_agent::AgentFailureReason::code),
        Some(EGRESS_DENIED),
        "the deployment's own code is a field on the record, not a substring of a line"
    );
    assert_eq!(
        fx.terminal_failure_reason()
            .await
            .as_ref()
            .map(rakka_agent::AgentFailureReason::code),
        Some(EGRESS_DENIED)
    );
```

and in the echo scenario (`a_credential_the_server_echoes_into_its_error_text_is_redacted_before_it_is_persisted`, :1009), directly after its own `assert_eq!(effect.last_error_code.as_deref(), Some(COLLABORATOR_FAILED));`:

```rust
    assert_eq!(
        effect
            .last_error_reason
            .as_ref()
            .map(rakka_agent::AgentFailureReason::code),
        Some("mcp-tool-error"),
        "the MCP code is the reason; the server's text is not part of it"
    );
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p rakka-agent --test secret_exclusion an_exhausted_` (timeout 600000)
Expected: FAIL, both tests, at `expect("… own code is recorded")`: the reason is `None`.

- [ ] **Step 3: Pass the inner code through**

In `crates/rakka-agent/src/dispatch.rs`:

1. `collaborator_code` (:232) bounds a code as a code:

```rust
fn collaborator_code(error: &AgentDispatchError) -> String {
    match error {
        AgentDispatchError::Collaborator { code, .. } => bounded_failure_code(code),
        other => other.code().to_string(),
    }
}
```

2. Add below it:

```rust
/// Which decision failed an attempt, when a collaborator decided it: the
/// collaborator's own code. `None` for every other failure, whose pipeline
/// code already is the whole identity.
fn collaborator_reason(error: &AgentDispatchError) -> Option<crate::failure::AgentFailureReason> {
    match error {
        AgentDispatchError::Collaborator { .. } => {
            crate::failure::AgentFailureReason::new(collaborator_code(error))
        }
        _ => None,
    }
}
```

3. `record_attempt_failure` (:3338): add `reason: Option<crate::failure::AgentFailureReason>,` between `message: &str,` and `pass: &mut AgentDispatchPass,`, and the outcome it delivers becomes:

```rust
                AgentRunEffectOutcome::exhausted(code.to_string(), detail).with_reason(reason),
```

Add to its doc comment: "`reason` is the collaborator's own code, when a collaborator failed the attempt; it rides the `Exhausted` word to the run's record and is written nowhere else."

4. The resolution failure (:2759): pass the resolver's code. It persists a code, not a detail:

```rust
                        return self
                            .record_attempt_failure(
                                scope,
                                &claim,
                                intent,
                                attempt,
                                "credential-resolution-failed",
                                &detail,
                                // The resolver's own code, which the line
                                // above already logs: a stable identifier,
                                // never the resolver's words.
                                crate::failure::AgentFailureReason::new(collaborator_code(&error)),
                                pass,
                            )
                            .await;
```

5. The invocation failure (:2844):

```rust
            Err(error) => {
                return self
                    .record_attempt_failure(
                        scope,
                        &claim,
                        intent,
                        attempt,
                        error.code(),
                        &error.to_string(),
                        collaborator_reason(&error),
                        pass,
                    )
                    .await;
            }
```

Run `grep -n 'record_attempt_failure(' crates/rakka-agent/src/dispatch.rs`. At `ebc7147` these two are the only call sites; any other takes `None`.

6. The resolver trait's doc (:1348-1356). Replace the section "# The error text this returns becomes durable state" and its paragraph with:

```rust
/// # The error's code becomes durable state; its text does not
///
/// A failing resolution burns the attempt under the pipeline's
/// `credential-resolution-failed` and a detail the dispatcher authors itself.
/// The error's text is never persisted and never logged. Its **code** is: it
/// is written on the dispatcher's log line and, once the retry budget is
/// spent, recorded beside the pipeline code as the failure's reason, bounded
/// at `AGENT_FAILURE_REASON_CODE_MAX_LENGTH`. A code is a stable identifier
/// — `vault-unreachable`, `lease-too-short` — and MUST carry no credential,
/// argument, or content material
/// ([specification 16](../../../docs/plans/rakka-agent/spec.md)).
```

7. In `examples/durable-agent-acceptance/src/provider.rs` (:91-92), replace the comment "The error text a failing resolution returns becomes durable state, so it names the variable and never what it held." with "A failing resolution's code becomes durable state and its text never does; the text still names the variable and never what it held."

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p rakka-agent --test secret_exclusion` and `cargo test -p rakka-agent --test mcp_client_dispatch` (timeout 600000 each)
Expected: PASS.

Run: `cargo test -p rakka-agent --all-features --test effect_dispatch --test model_provider_dispatch --test tool_authority` (timeout 600000)
Expected: PASS.

- [ ] **Step 5: Lint and commit**

Run: `cargo fmt --all -- --check` and `cargo clippy -p rakka-agent -p rakka-example-durable-agent-acceptance --all-targets --all-features -- -D warnings` (timeout 600000)
Expected: both exit 0.

```bash
git add crates/rakka-agent/src/dispatch.rs crates/rakka-agent/tests/secret_exclusion.rs \
        crates/rakka-agent/tests/mcp_client_dispatch.rs \
        examples/durable-agent-acceptance/src/provider.rs
git commit -m "Record a collaborator's own code as the reason of an exhausted effect: the resolver's, the executor's, the MCP server's

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

### Task 8: The `checkpoint-resolve` segment closes when the resolution commits

Today the segment closes only when the whole call — transition plus settle pass — answers `Applied`. Three things follow, and this task changes all three. A resolution whose settle pass lost a compare-and-set is durable and leaves no trace at all, on either call. A resolution whose settle pass failed in a way `apply` retries answers `Duplicate` for its own commit, and closes nothing. And an `Escalate` decision answers `Applied` with the checkpoint still open, so it closes a segment for a resolution that did not happen, and the real one later closes a second under the same span id.

**Files:**
- Modify: `crates/rakka-agent/src/run.rs` (`AgentRunEntityStore` fields :6405-6470, `apply` :6681-6773, `apply_command` directly above its settle call at :7038, `close_segment_linked` :7722, `resolving_checkpoint` :7813, `ResolvingCheckpoint` :9246)
- Modify: `crates/rakka-agent/src/testkit.rs` (`CrashPoint` :3763, `CrashingStateStore` :3808-3975)
- Create: `crates/rakka-agent/tests/checkpoint_resolve_segment.rs`
- Test: `crates/rakka-agent/tests/checkpoint_reconciliation.rs`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `CrashPoint::ConflictBeforeWrite` (the enum is `#[non_exhaustive]`): at the armed write a second writer moves the record first, so the armed write loses with a genuine `DurableError::RevisionConflict`.
  - No public API change in `run.rs`. The rule: a `checkpoint-resolve` segment closes on the call whose transition committed and left the named checkpoint no longer open; a `run-resume` segment closes on the call that first observes the wait ended. Both commands that resolve are covered, `ResolveCheckpoint` and `ResolveIndeterminateEffect`.

- [ ] **Step 1: Add the conflict crash point**

In `crates/rakka-agent/src/testkit.rs`:

1. Add the variant to `CrashPoint`, after `AfterWrite`:

```rust
    /// The owner did not die: a second writer moved the durable record first,
    /// so the owner's write loses its compare-and-set with a genuine
    /// `RevisionConflict`. Unlike the two crashes, this is the failure an
    /// entity with two writers meets in production, and the one that makes a
    /// resident entity drop its cached record.
    ConflictBeforeWrite,
```

2. Add a field to `CrashingStateStore`, after `crash_after`: `conflict: Arc<std::sync::atomic::AtomicBool>,`. Initialize it `Arc::new(std::sync::atomic::AtomicBool::new(false))` in `new` and clone it in `Clone::clone`, each beside `crash_after`.

3. In `crash_at`, after the `crash_after` store:

```rust
        self.conflict.store(
            matches!(point, CrashPoint::ConflictBeforeWrite),
            Ordering::SeqCst,
        );
```

4. In `compare_and_set`, read the flag beside `after` — `let conflict = self.conflict.load(Ordering::SeqCst);` — and make the first statement of the `async move` block:

```rust
            if crash_at != 0 && write == crash_at && conflict {
                // The second writer: the record as it stands, written back
                // at its own revision. Nothing about the state changes; the
                // revision moves, which is all a lost compare-and-set is.
                if let Some(record) = self.inner.load(persistence_id).await? {
                    self.inner
                        .compare_and_set(persistence_id, record.revision, record.state)
                        .await?;
                }
                return self
                    .inner
                    .compare_and_set(persistence_id, expected_revision, state)
                    .await;
            }
```

`BeforeWrite` must not also fire for this point: change the existing first check to `if crash_at != 0 && write == crash_at && !after && !conflict`. `delete` takes the same guard on its own `BeforeWrite` check and gets no conflict arm: no flow deletes.

5. The point's own proof is the first test of Step 2's file, `a_conflict_point_loses_the_armed_write_with_a_revision_conflict`. It drives the run store, which every fixture wraps in a `CrashingStateStore` (`pub type RunStore = CrashingStateStore<AgentRunState>`, `tests/common/mod.rs:69`).

- [ ] **Step 2: Write the failing segment tests**

Create `crates/rakka-agent/tests/checkpoint_resolve_segment.rs`:

```rust
//! When a checkpoint's resolution leaves its trace: on the call whose
//! transition committed it, exactly once, whatever the settle pass after it
//! did.

use std::sync::Arc;

use rakka_agent::testkit::{CrashPoint, ScriptedDispatcher};
use rakka_agent::{
    load_agent_run_state, AgentApprovalDecision, AgentCheckpoint, AgentCheckpointDecision,
    AgentEffectPolicies, AgentEffectSpec, AgentModelTurn, AgentOperationId, AgentOperationKind,
    AgentRunEffect, AgentRunEntityCommand, AgentRunEntityReply, AgentSchemaPolicy,
    AgentSegmentOperation, AgentTaskContent, AgentTelemetrySegment, AgentToolCallId,
    AgentToolCallRequest, AgentToolId, InMemoryAgentRunEffectSink, InMemoryAgentSegmentSink,
    ATTR_AGENT_TELEMETRY_LINK_KIND, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
    LINK_KIND_PARKED_CHECKPOINT, LINK_KIND_RESUME_REQUEST,
};
use rakka_agent_workflow::{AgentTelemetryContext, AgentTimestampMillis, PrincipalRef};

mod common;

use common::*;

const INGRESS_PARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
const REQUEST_PARENT: &str = "00-1bf7651916cd43dd8448eb211c80319d-c7ad6b7169203332-01";

fn context(trace_parent: &str) -> AgentTelemetryContext {
    AgentTelemetryContext {
        trace_parent: Some(trace_parent.to_string()),
        ..AgentTelemetryContext::default()
    }
}

/// A fixture whose one tool is checkpoint-required, so the run parks on an
/// approval before anything dispatches.
fn checkpointed_fixture() -> Fixture {
    use std::sync::atomic::AtomicU64;

    let tool = AgentToolId::new("charge-card").expect("tool id");
    let policies = AgentEffectPolicies::new()
        .with_tool_spec(
            tool.clone(),
            AgentEffectSpec::non_idempotent().with_checkpoint_required(),
        )
        .expect("the checkpoint-required tool spec is valid");
    Fixture::with_sink(
        ScriptedDispatcher::new()
            .with_turn(
                AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
                    .with_text("Charging the card.")
                    .with_tool_call(
                        AgentToolCallRequest::new(
                            AgentToolCallId::new("call-1").expect("call id"),
                            tool,
                            serde_json::json!({ "amount": 42 }),
                        )
                        .expect("the tool call is bounded"),
                    ),
            )
            .with_turn(
                AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
                    .with_text("Done.")
                    .with_proposal(
                        AgentTaskContent::inline(serde_json::json!({ "answer": "charged" }))
                            .expect("the proposal is inline-bounded"),
                    ),
            ),
        InMemoryAgentRunEffectSink::new(),
        policies,
        Arc::new(AtomicU64::new(1)),
    )
}

/// Parks a traced run on its approval checkpoint and answers the checkpoint
/// and the effect it gates.
async fn park(fx: &Fixture) -> (rakka_agent::AgentCheckpoint, AgentRunEffect) {
    fx.instantiate_agent().await;
    fx.create_task_traced(context(INGRESS_PARENT)).await;
    fx.pump().await.expect("the run parks on its checkpoint");
    let state = load_agent_run_state(&fx.runs, &run_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the run state loads")
        .expect("the run exists");
    let loop_state = state.loop_state().expect("the loop exists");
    let checkpoint = loop_state
        .open_checkpoints()
        .first()
        .expect("the approval checkpoint is open")
        .clone();
    let effect = loop_state
        .effects()
        .iter()
        .find(|effect| effect.effect_id == checkpoint.bound_effect.effect_id)
        .expect("the gated effect is on the loop")
        .clone();
    (checkpoint, effect)
}

fn approve(checkpoint: &rakka_agent::AgentCheckpoint, key: &str) -> AgentRunEntityCommand {
    AgentRunEntityCommand::ResolveCheckpoint {
        operation_id: AgentOperationId::for_agent(
            AgentOperationKind::CheckpointResolution,
            &agent_scope(),
            key,
        )
        .expect("the decision key derives"),
        checkpoint_id: checkpoint.checkpoint_id.clone(),
        resolver: PrincipalRef {
            principal_type: "user".to_string(),
            principal_id: "approver".to_string(),
            display_name: None,
        },
        decision: Box::new(AgentCheckpointDecision::Approval(
            AgentApprovalDecision::Approve {
                credential_binding: None,
                expires_at: AgentTimestampMillis::new(1_000_000),
                allowed_use_count: 1,
            },
        )),
        telemetry: context(REQUEST_PARENT),
    }
}

fn closed(sink: &InMemoryAgentSegmentSink, wanted: &str) -> Vec<AgentTelemetrySegment> {
    sink.segments()
        .into_iter()
        .filter(|segment| match wanted {
            "resolve" => matches!(segment.operation, AgentSegmentOperation::CheckpointResolve),
            "resume" => matches!(segment.operation, AgentSegmentOperation::RunResume),
            other => panic!("unknown segment class {other}"),
        })
        .collect()
}

/// The resolution of one clean call: how many resolve and resume segments a
/// resolution closes when nothing goes wrong. The measure every other arm is
/// held to.
async fn clean_resolution() -> (usize, usize) {
    let sink = Arc::new(InMemoryAgentSegmentSink::new());
    let fx = checkpointed_fixture().with_segments(sink.clone());
    let (checkpoint, _) = park(&fx).await;
    let resumes_before = closed(&sink, "resume").len();
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    let reply = run
        .apply(approve(&checkpoint, "d1"), &fx.router, fx.now())
        .await
        .expect("the decision applies");
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
    (
        closed(&sink, "resolve").len(),
        closed(&sink, "resume").len() - resumes_before,
    )
}

#[tokio::test]
async fn a_conflict_point_loses_the_armed_write_with_a_revision_conflict() {
    use rakka_persistence::{DurableError, DurableStateStore};

    let fx = checkpointed_fixture();
    park(&fx).await;
    let id = run_scope().persistence_id();
    let before = fx
        .runs
        .load(&id)
        .await
        .expect("loads")
        .expect("the run exists");
    fx.runs.crash_at(1, CrashPoint::ConflictBeforeWrite);
    let lost = fx
        .runs
        .compare_and_set(&id, before.revision, before.state)
        .await
        .expect_err("a second writer moved the record first");
    fx.runs.assert_crash_fired(1, CrashPoint::ConflictBeforeWrite);
    fx.runs.survive();
    assert!(
        matches!(lost, DurableError::RevisionConflict { .. }),
        "{lost:?}"
    );
    let after = fx
        .runs
        .load(&id)
        .await
        .expect("loads")
        .expect("the run exists");
    assert_ne!(
        after.revision, before.revision,
        "the second writer's write is what moved the record"
    );
}

#[tokio::test]
async fn a_resolution_whose_settle_pass_loses_a_write_still_leaves_its_trace_once() {
    let (clean_resolves, clean_resumes) = clean_resolution().await;
    assert_eq!(clean_resolves, 1, "the measure itself");

    let sink = Arc::new(InMemoryAgentSegmentSink::new());
    let fx = checkpointed_fixture().with_segments(sink.clone());
    let (checkpoint, effect) = park(&fx).await;
    let resumes_before = closed(&sink, "resume").len();

    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    // Write 1 is the resolving transition. Write 2 is the settle pass's
    // first compare-and-set, which a second writer beats.
    fx.runs.crash_at(2, CrashPoint::ConflictBeforeWrite);
    let first = run
        .apply(approve(&checkpoint, "d1"), &fx.router, fx.now())
        .await;
    fx.runs.assert_crash_fired(2, CrashPoint::ConflictBeforeWrite);
    fx.runs.survive();
    assert!(
        first.is_err(),
        "the settle pass lost its write, and the call says so: {first:?}"
    );

    // The resolution is durable, so its trace exists: once, under the
    // identity the park linked forward to, a child of the run's ingress, and
    // linking the parked span and the request.
    let resolved = closed(&sink, "resolve");
    assert_eq!(resolved.len(), 1, "{:?}", sink.operations());
    let identity =
        AgentCheckpoint::resolve_span_identity(&effect.telemetry, &checkpoint.checkpoint_id)
            .expect("the resolve identity derives");
    assert_eq!(
        resolved[0].span_id.as_deref(),
        Some(identity.span_id.as_str())
    );
    assert_eq!(
        resolved[0].telemetry.trace_parent.as_deref(),
        Some(INGRESS_PARENT),
        "the run's context was read before the write that dropped the cached record"
    );
    for kind in [LINK_KIND_PARKED_CHECKPOINT, LINK_KIND_RESUME_REQUEST] {
        assert!(
            resolved[0].telemetry.span_links.iter().any(|link| {
                link.attributes.get(ATTR_AGENT_TELEMETRY_LINK_KIND) == Some(&kind.to_string())
            }),
            "a `{kind}` link: {:?}",
            resolved[0].telemetry.span_links
        );
    }

    // The re-drive finds the operation applied, resolves nothing, and closes
    // no second resolve segment.
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    let replay = run
        .apply(approve(&checkpoint, "d1"), &fx.router, fx.now())
        .await
        .expect("the replay answers");
    assert!(
        matches!(replay, AgentRunEntityReply::Duplicate { .. }),
        "{replay:?}"
    );
    assert_eq!(
        closed(&sink, "resolve").len(),
        clean_resolves,
        "a duplicate resolved nothing and closes nothing"
    );
    assert_eq!(
        closed(&sink, "resume").len() - resumes_before,
        clean_resumes,
        "across the errored call and its re-drive, the wait's end is recorded as often as a \
         clean call records it"
    );
}

#[tokio::test]
async fn a_replayed_resolution_closes_nothing() {
    let sink = Arc::new(InMemoryAgentSegmentSink::new());
    let fx = checkpointed_fixture().with_segments(sink.clone());
    let (checkpoint, _) = park(&fx).await;
    for expected in ["applied", "duplicate"] {
        let mut run = fx.run();
        run.recover(fx.now()).await.expect("the run recovers");
        let reply = run
            .apply(approve(&checkpoint, "d1"), &fx.router, fx.now())
            .await
            .expect("the decision answers");
        match expected {
            "applied" => assert!(matches!(reply, AgentRunEntityReply::Applied { .. })),
            _ => assert!(matches!(reply, AgentRunEntityReply::Duplicate { .. })),
        }
        assert_eq!(closed(&sink, "resolve").len(), 1, "after the {expected} call");
    }
}

#[tokio::test]
async fn a_refused_resolution_closes_nothing() {
    let sink = Arc::new(InMemoryAgentSegmentSink::new());
    let fx = checkpointed_fixture().with_segments(sink.clone());
    let (checkpoint, _) = park(&fx).await;
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    // A lost first write: the transition itself never committed.
    fx.runs.crash_at(1, CrashPoint::ConflictBeforeWrite);
    let refused = run
        .apply(approve(&checkpoint, "d1"), &fx.router, fx.now())
        .await;
    fx.runs.assert_crash_fired(1, CrashPoint::ConflictBeforeWrite);
    fx.runs.survive();
    assert!(refused.is_err(), "{refused:?}");
    assert!(
        closed(&sink, "resolve").is_empty(),
        "nothing committed, so nothing was resolved: {:?}",
        sink.operations()
    );
}
```

In `crates/rakka-agent/tests/checkpoint_reconciliation.rs`, add after `an_escalate_decision_keeps_the_wait_nonterminal_until_a_resolving_decision` (:400):

```rust
/// An escalation is not a resolution, so it leaves no resolve segment; the
/// decision that does resolve leaves exactly one.
#[tokio::test]
async fn an_escalation_closes_no_resolve_segment_and_the_resolution_closes_one() {
    use rakka_agent::{AgentSegmentOperation, InMemoryAgentSegmentSink};

    let sink = Arc::new(InMemoryAgentSegmentSink::new());
    let fx = fixture().with_segments(sink.clone());
    let (_effect_id, _generation, checkpoint_id) = park_indeterminate(&fx).await;
    let resolves = |sink: &InMemoryAgentSegmentSink| {
        sink.segments()
            .into_iter()
            .filter(|segment| {
                matches!(segment.operation, AgentSegmentOperation::CheckpointResolve)
            })
            .count()
    };

    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    run.apply(
        resolve_command(
            checkpoint_id.clone(),
            "d1",
            reconcile(AgentReconciliationDecision::Escalate),
        ),
        &fx.router,
        fx.now(),
    )
    .await
    .expect("the escalation applies");
    assert_eq!(
        resolves(&sink),
        0,
        "the checkpoint is still open: {:?}",
        sink.operations()
    );

    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    run.apply(
        resolve_command(checkpoint_id, "d2", confirmed_completed()),
        &fx.router,
        fx.now(),
    )
    .await
    .expect("the resolution applies");
    assert_eq!(resolves(&sink), 1, "{:?}", sink.operations());
}
```

If that file does not import `Arc`, add `use std::sync::Arc;`.

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cargo test -p rakka-agent --test checkpoint_resolve_segment --test checkpoint_reconciliation` (timeout 600000)
Expected: `a_conflict_point_loses_the_armed_write_with_a_revision_conflict` PASSES (Step 1 built it). `a_resolution_whose_settle_pass_loses_a_write_still_leaves_its_trace_once` FAILS with zero resolve segments; `an_escalation_closes_no_resolve_segment_and_the_resolution_closes_one` FAILS with one segment after the escalation. `a_replayed_resolution_closes_nothing` and `a_refused_resolution_closes_nothing` PASS already: they pin what must not change.

- [ ] **Step 4: Decide the resolution at the commit**

In `crates/rakka-agent/src/run.rs`:

1. Add above `struct ResolvingCheckpoint`:

```rust
/// What the command in flight had committed when its transition returned,
/// read before the settle pass that follows it.
///
/// The settle pass is the run's next piece of work, and a failure there is
/// reported by the call's `Err`. It can also lose a compare-and-set, which
/// drops the cached record — after which nothing about the transition can
/// be read back. So the two facts a segment is decided on are read here,
/// while the record the transition wrote is still the cached one.
#[derive(Debug, Clone, Copy, Default)]
struct CommittedTransition {
    /// The checkpoint the command named is no longer open: it was resolved.
    /// An escalation commits and leaves it open, and resolved nothing.
    resolved: bool,
    /// The run was not waiting once the transition committed.
    resumed: bool,
}
```

2. Add to `ResolvingCheckpoint` a field, after `request`:

```rust
    /// The run's own context, read with the rest: a settle pass that loses a
    /// write drops the cached record, and the context with it.
    run: AgentTelemetryContext,
```

and fill it in `resolving_checkpoint` (:7813): add `run: loop_state.telemetry().clone(),` to the `ResolvingCheckpoint { … }` literal.

3. Add two fields to `AgentRunEntityStore` (:6405), after `segments`, and initialize both to `None` in its constructor (:6464). If a second struct in the file carries the same field list and builds the store (the entity's own builder at :8769 does not hold these; only the store does), leave it alone:

```rust
    /// The checkpoint the command in flight would resolve, when it is a
    /// resolution command. Set by `apply`, read by `apply_command`.
    resolving: Option<HumanCheckpointId>,
    /// What the command in flight committed. Set by `apply_command` between
    /// the transition and the settle pass, taken by `apply`.
    committed: Option<CommittedTransition>,
```

4. Add a method beside `loop_telemetry` (:7708):

```rust
    /// Records what the transition just committed, before the settle pass
    /// can drop the record it is read from.
    fn note_committed_transition(&mut self) {
        let committed = self
            .state()
            .ok()
            .and_then(|state| state.loop_state())
            .map(|loop_state| CommittedTransition {
                resolved: self.resolving.as_ref().is_some_and(|checkpoint_id| {
                    !loop_state
                        .open_checkpoints()
                        .iter()
                        .any(|checkpoint| checkpoint.checkpoint_id == *checkpoint_id)
                }),
                resumed: !loop_state.phase().is_waiting(),
            });
        self.committed = committed;
    }
```

5. In `apply_command`, directly above the comment "// The inner pass: `apply`'s own sampling scope wraps this call." (:7037):

```rust
        // The command's transition committed. Whether it resolved a
        // checkpoint, and whether the wait ended, is decided here.
        self.note_committed_transition();
```

The replay path returns earlier (:6796) and never reaches this line, which is what keeps a duplicate from closing anything.

6. Split `close_segment_linked` (:7722) so a caller can supply the context:

```rust
    fn close_segment_linked(
        &self,
        segment: crate::observability::AgentTelemetrySegment,
        links: Vec<AgentSpanLink>,
    ) {
        self.close_segment_under(segment, self.loop_telemetry(), links);
    }

    /// [`Self::close_segment_linked`] under a context the caller read
    /// earlier, for a segment closed after the cached record may be gone.
    fn close_segment_under(
        &self,
        segment: crate::observability::AgentTelemetrySegment,
        telemetry: AgentTelemetryContext,
        links: Vec<AgentSpanLink>,
    ) {
        let Some(sink) = self.segments.as_ref() else {
            return;
        };
        let telemetry = if links.is_empty() {
            telemetry
        } else {
            agent_linked_telemetry_context(&telemetry, links)
        };
        sink.record(
            &segment
                .identity(AgentSegmentIdentity::of_run(&self.scope))
                .telemetry(telemetry),
        );
    }
```

7. In `apply` (:6681). Read the run's context with the resume timer, arm the watch before the command, and decide both segments from what committed. Replace from `let resolving = self.resolving_checkpoint(&command);` through the end of the function with:

```rust
        let resolving = self.resolving_checkpoint(&command);
        let resolve_timer = resolving.as_ref().map(|_| AgentSegmentTimer::start(now));
        // Read now, for a segment that may close after the cached record is
        // gone.
        let context_before = self.loop_telemetry();
        self.resolving = resolving
            .as_ref()
            .map(|resolving| resolving.checkpoint_id.clone());
        self.committed = None;
        let reverify = command.clone();
        let reply = match self.apply_command(command, router, now).await {
            // (the existing comment block stays here, unchanged)
            Err(error) if run_refusal_may_be_stale(&error) => {
                self.rematerialize(now).await?;
                self.apply_command(reverify, router, now).await
            }
            other => other,
        };
        let committed = self.committed.take().unwrap_or_default();
        self.resolving = None;
        self.record_fan_in_resolution(resolution_before);
        // The resolution segment links the parked span and the incoming
        // request ([specification 17.11]), and exports under the identity the
        // park linked forward to ([17.9]). Its subject is the resolution, and
        // the resolution is the transition: it closes on the call whose
        // transition committed and left the checkpoint no longer open,
        // whatever the settle pass after it answered. A duplicate, a refusal,
        // and an escalation resolved nothing and close nothing.
        let mut links = Vec::new();
        if let (Some(resolving), Some(timer), true) =
            (resolving, resolve_timer, committed.resolved)
        {
            links = resolving.links();
            let mut segment = timer
                .close(AgentSegmentOperation::CheckpointResolve)
                .attribute(SEGMENT_ATTR_CHECKPOINT_KIND, resolving.kind.as_label());
            if let Some(identity) = AgentCheckpoint::resolve_span_identity(
                &resolving.effect_telemetry,
                &resolving.checkpoint_id,
            ) {
                segment = segment.span_id(identity.span_id);
            }
            self.close_segment_under(segment.ok(), resolving.run.clone(), links.clone());
        }
        // Closed only when the wait actually ended. A command that arrives at
        // a waiting run and leaves it waiting — a duplicate, a refusal, a
        // partial fan-in — discharged nothing, and a resume segment for it
        // would claim a transition that did not happen. The record after the
        // settle pass answers when it can be read; when the settle pass lost
        // a write and dropped it, what the transition itself committed
        // answers, under the context read before the command.
        if let Some(timer) = resume_timer {
            let after = self
                .state()
                .ok()
                .and_then(|state| state.loop_state().map(AgentLoopState::phase));
            let (resumed, context) = match after {
                Some(phase) => (!phase.is_waiting(), self.loop_telemetry()),
                None => (committed.resumed, context_before),
            };
            if resumed {
                self.close_segment_under(
                    timer.close(AgentSegmentOperation::RunResume).ok(),
                    context,
                    links,
                );
            }
        }
        reply
    }
```

Keep the long comment that sits inside the `match` today (:6714-6728) exactly where it is; the placeholder line above stands for it. The first call's `self.rematerialize(now).await?` can return early with the watch still armed; that is harmless, because the next `apply` re-arms both fields before it reads either.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p rakka-agent --test checkpoint_resolve_segment --test checkpoint_reconciliation` (timeout 600000)
Expected: PASS.

Run: `cargo test -p rakka-agent --all-features --test trace_scenarios --test effect_dispatch --test telemetry_segments --test checkpoint_run --test otel_span_mapping` (timeout 600000)
Expected: PASS. `a_parked_checkpoint_carries_the_segment_a_resume_doubly_links` and `an_indeterminate_transition_links_the_ambiguous_attempt_and_the_reconciliation_decision` assert exactly one resolve segment with both links, and must still.

Run: `cargo test -p rakka-agent --lib` (timeout 600000)
Expected: PASS.

- [ ] **Step 6: Lint and commit**

Run: `cargo fmt --all -- --check` and `cargo clippy -p rakka-agent --all-targets --all-features -- -D warnings` (timeout 600000)
Expected: both exit 0.

```bash
git add crates/rakka-agent/src/run.rs crates/rakka-agent/src/testkit.rs \
        crates/rakka-agent/tests/checkpoint_resolve_segment.rs \
        crates/rakka-agent/tests/checkpoint_reconciliation.rs
git commit -m "Close a checkpoint's resolve segment when the resolution commits, not when the whole call returns: once, and never for an escalation

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

# Part C — Guardrail and provider residuals

### Task 9: A model-response transform cannot add a proposal or rewrite a reference

**Files:**
- Modify: `crates/rakka-agent/src/tools.rs` (`review_model_response`: the doc :1360-1388 and the proposal check :1475-1500)
- Test: `crates/rakka-agent/tests/model_response_guardrails.rs`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: no new public item and no new code. The rule `review_model_response` enforces on a transformed turn's proposal becomes:

| Original | Transformed | Verdict |
| --- | --- | --- |
| none | none | accepted |
| none | any | refused `guardrail-transform-invalid`: the model proposed nothing |
| inline | inline | accepted: a stage may rewrite an inline proposal |
| inline | reference, or reference → inline | refused `guardrail-transform-unsupported` (unchanged) |
| reference | the same reference, every field equal | accepted |
| reference | a reference differing in any field | refused `guardrail-transform-unsupported` |
| any | none | accepted: a stage may drop a proposal, as it may drop a tool call |

- [ ] **Step 1: Write the failing tests**

In `crates/rakka-agent/tests/model_response_guardrails.rs`, add below `ReferenceTheProposal`:

```rust
/// The reference a model's own turn proposed, in the tests that need one.
fn proposed_reference() -> ArtifactRef {
    ArtifactRef {
        artifact_id: "result-1".to_string(),
        kind: ArtifactKind::File,
        uri: "s3://results/result-1".to_string(),
        checksum: Some("sha256:result-1".to_string()),
        content_type: Some("application/json".to_string()),
        byte_len: Some(32),
        retention_class: Some("standard".to_string()),
        encryption: None,
        redaction: RedactionStatus::Unredacted,
        created_at: AgentTimestampMillis::new(1),
        metadata: AgentAttributes::default(),
    }
}

fn referencing_turn(text: &str) -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text(text)
        .with_proposal(AgentTaskContent::artifact(proposed_reference()))
}

/// A transform that gives a turn an inline proposal, whatever it had.
struct InventInlineProposal;

impl AgentGuardrail for InventInlineProposal {
    fn evaluate(&self, _: &AgentGuardrailContext<'_>, content: &Value) -> AgentGuardrailOutcome {
        let mut altered = content.clone();
        altered["proposal"] = serde_json::to_value(
            AgentTaskContent::inline(json!({ "answer": "invented" })).expect("inline"),
        )
        .expect("the inline content encodes");
        AgentGuardrailOutcome::Transform {
            content: altered,
            reason_code: "proposal-invented".to_string(),
        }
    }
}

/// A transform that keeps a reference's id and rewrites where it points and
/// what it promises to hold.
struct RepointTheReference;

impl AgentGuardrail for RepointTheReference {
    fn evaluate(&self, _: &AgentGuardrailContext<'_>, content: &Value) -> AgentGuardrailOutcome {
        let mut repointed = proposed_reference();
        repointed.uri = "s3://elsewhere/result-1".to_string();
        repointed.checksum = Some("sha256:something-else".to_string());
        let mut altered = content.clone();
        altered["proposal"] = serde_json::to_value(AgentTaskContent::artifact(repointed))
            .expect("the artifact content encodes");
        AgentGuardrailOutcome::Transform {
            content: altered,
            reason_code: "reference-repointed".to_string(),
        }
    }
}

/// A transform that removes the proposal and nothing else.
struct DropTheProposal;

impl AgentGuardrail for DropTheProposal {
    fn evaluate(&self, _: &AgentGuardrailContext<'_>, content: &Value) -> AgentGuardrailOutcome {
        let mut altered = content.clone();
        altered["proposal"] = Value::Null;
        AgentGuardrailOutcome::Transform {
            content: altered,
            reason_code: "proposal-dropped".to_string(),
        }
    }
}
```

and add after `a_transform_that_changes_the_proposal_to_a_reference_is_refused_as_unsupported` (:362):

```rust
/// A reference where the model proposed nothing fabricates a task result out
/// of an artifact nothing in the turn produced.
#[test]
fn a_transform_that_adds_a_reference_proposal_is_refused_as_invalid() {
    for turn in [text_turn("hello"), tool_calling_turn()] {
        let refusal = authority_with(Arc::new(ReferenceTheProposal))
            .review_model_response(&run_scope(), turn)
            .expect_err("the model proposed nothing");
        assert_eq!(refusal.code, "guardrail-transform-invalid");
    }
}

/// An inline proposal the model never made is an invented result too.
#[test]
fn a_transform_that_adds_an_inline_proposal_is_refused_as_invalid() {
    let refusal = authority_with(Arc::new(InventInlineProposal))
        .review_model_response(&run_scope(), text_turn("hello"))
        .expect_err("the model proposed nothing");
    assert_eq!(refusal.code, "guardrail-transform-invalid");

    // The positive control: the same stage over a turn that did propose is a
    // rewrite of an inline proposal, which a stage may make.
    let review = authority_with(Arc::new(InventInlineProposal))
        .review_model_response(&run_scope(), proposing_turn("all good", "done"))
        .expect("rewriting an inline proposal is permitted");
    assert!(review.transformed);
    assert_eq!(
        review.turn.proposal.and_then(|proposal| proposal.inline_value().cloned()),
        Some(json!({ "answer": "invented" }))
    );
}

/// The id is not the reference: the `uri` and the `checksum` are what the
/// task fingerprints, and every other field is what a reader is promised.
#[test]
fn a_transform_that_rewrites_a_reference_under_its_own_id_is_refused_as_unsupported() {
    let refusal = authority_with(Arc::new(RepointTheReference))
        .review_model_response(&run_scope(), referencing_turn("stored"))
        .expect_err("a reference is not a stage's to rewrite");
    assert_eq!(refusal.code, "guardrail-transform-unsupported");
}

/// A stage that leaves the reference alone may still rewrite the rest.
#[test]
fn a_transform_that_keeps_a_reference_whole_passes() {
    let review = authority_with(Arc::new(RedactText))
        .review_model_response(&run_scope(), referencing_turn("SENSITIVE"))
        .expect("the reference is untouched");
    assert!(review.transformed);
    assert_eq!(review.turn.text.as_deref(), Some("[redacted]"));
    assert_eq!(
        review.turn.proposal.as_ref().and_then(AgentTaskContent::artifact_ref),
        Some(&proposed_reference())
    );
}

/// A stage may drop a proposal, inline or reference, as it may drop a tool
/// call: nothing is fabricated by proposing less.
#[test]
fn a_transform_may_drop_a_proposal() {
    for turn in [
        proposing_turn("all good", "done"),
        referencing_turn("stored"),
    ] {
        let review = authority_with(Arc::new(DropTheProposal))
            .review_model_response(&run_scope(), turn)
            .expect("dropping a proposal is permitted");
        assert!(review.transformed);
        assert_eq!(review.turn.proposal, None);
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p rakka-agent --test model_response_guardrails a_transform` (timeout 600000)
Expected: three FAIL — `a_transform_that_adds_a_reference_proposal_is_refused_as_invalid`, `a_transform_that_adds_an_inline_proposal_is_refused_as_invalid`, and `a_transform_that_rewrites_a_reference_under_its_own_id_is_refused_as_unsupported` — each because the review accepted the turn. `a_transform_that_keeps_a_reference_whole_passes` and `a_transform_may_drop_a_proposal` PASS already: they pin what the rule must keep permitting.

- [ ] **Step 3: Replace the proposal check**

In `crates/rakka-agent/src/tools.rs`, replace the block from the comment "The proposal's *form* is not a stage's to change" through the closing brace of its `if let` (:1475-1500, ending directly before `review.turn = transformed;`) with:

```rust
            // The proposal is the model's, and a stage may only make it say
            // less. It may rewrite an inline proposal or drop any proposal.
            // It may not add one where the model proposed nothing — that is
            // an invented task result, the twin of an invented tool call. And
            // a reference is not a stage's to write at all, for the reason a
            // reference-held tool result cannot be rewritten
            // ([`Self::review_tool_response`]): it names an immutable
            // artifact the run never loads here, so turning an inline
            // proposal into one fabricates an artifact, turning one into
            // inline invents the bytes it stood for, and changing any field
            // of one — its `uri` and `checksum` are what the task
            // fingerprints — proposes content nothing in this turn produced.
            // A reference survives a transform only whole.
            match (&review.turn.proposal, &transformed.proposal) {
                (None, Some(_)) => {
                    return Err(AgentAuthorityRefusal::of(
                        "guardrail-transform-invalid",
                        "a guardrail transform may rewrite or drop the proposal the model made; \
                         it may not add one to a turn that proposed nothing",
                    ));
                }
                (Some(original), Some(proposal))
                    if original.artifact_ref() != proposal.artifact_ref() =>
                {
                    return Err(AgentAuthorityRefusal::of(
                        "guardrail-transform-unsupported",
                        "a guardrail transform may rewrite an inline proposal; it may not change \
                         the proposal's form between inline and an artifact reference, nor \
                         change any field of a reference",
                    ));
                }
                _ => {}
            }
```

`artifact_ref()` answers `None` for an inline proposal, so two inline proposals compare equal there whatever their values, and an inline against a reference compares unequal. `ArtifactRef` derives `Eq`, so two references compare field by field.

In the doc comment of `review_model_response` (:1360-1388), replace the sentence that begins "A transform that changes the proposal's form" and ends "a reference cannot be rewritten." with:

```rust
    /// A stage may drop the proposal. A transform that adds a proposal to a
    /// turn that made none is refused (`guardrail-transform-invalid`), and
    /// one that changes the proposal's form — inline to artifact reference,
    /// reference to inline — or any field of a reference is refused
    /// (`guardrail-transform-unsupported`), the `ToolResponse` precedent
    /// that a reference cannot be rewritten: a reference survives a
    /// transform only whole.
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p rakka-agent --test model_response_guardrails` (timeout 600000)
Expected: PASS, every test in the file.

Run: `cargo test -p rakka-agent --lib tools::tests` (timeout 600000)
Expected: PASS.

- [ ] **Step 5: Lint and commit**

Run: `cargo fmt --all -- --check` and `cargo clippy -p rakka-agent --all-targets --all-features -- -D warnings` (timeout 600000)
Expected: both exit 0.

```bash
git add crates/rakka-agent/src/tools.rs crates/rakka-agent/tests/model_response_guardrails.rs
git commit -m "Refuse a model-response transform that adds a proposal or rewrites any field of a reference

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

### Task 10: A required collaboration field cannot be cleared, and every ingress leaf is proven with a chain installed

Only the creation leaf of the A2A service has a test with a guardrail chain installed. The team leaf, the conversation leaf, the handoff branch, and the typed-result branch have none, and neither handoff text arm — ingress or egress — is reached by any test. Reading them found one defect: a stage that clears a required field with an explicit `null` is half-applied. A cleared handoff reason keeps the original while the transform is logged as applied; a cleared message body fails the re-derived command with a mapping error that blames the caller.

**Files:**
- Modify: `crates/rakka-a2a/src/agents/guardrails.rs` (`A2aCollaborationText` :25, `collaboration_text` :56, `evaluate_a2a_content` :118 and its doc, the unit tests' `original()` :311)
- Modify: `crates/rakka-a2a/src/agents/handoff.rs:441-445` (the egress text literal)
- Modify: `crates/rakka-agent/src/conversation.rs` (the `operation_id` docs at :1752 and :1760), `crates/rakka-agent/src/team.rs` (the `AppendMessage` command's `operation_id` doc)
- Modify: `crates/rakka-a2a/Cargo.toml` (four `[[test]]` entries), `scripts/validate.sh` (one line)
- Create: `crates/rakka-a2a/tests/support/mod.rs`
- Test: `crates/rakka-a2a/tests/team_surface.rs`, `conversation_surface.rs`, `handoff_surface.rs`, `human_task_surface.rs`

**Interfaces:**
- Consumes: Task 5's `RakkaAgentA2AError::Refused { code, message, reason }`.
- Produces: no new public item and no new code. `A2aCollaborationText` (crate-private) gains `body_required: bool` and `reason_required: bool`. A transform whose view sets a required field to `null` is refused `guardrail-transform-invalid`. An optional field is still cleared by `null`, and an omitted key still means unchanged.

| Envelope | `body` | `reason` | `context` |
| --- | --- | --- | --- |
| Team | required when the command carries one | none | none |
| Conversation | required when the command carries one | optional | none |
| Handoff | none | required | optional |

- [ ] **Step 1: Gate the four test files**

In `crates/rakka-a2a/Cargo.toml`, after the `human_task_surface` entry:

```toml
[[test]]
name = "conversation_surface"
required-features = ["agents"]

[[test]]
name = "coordination_surface"
required-features = ["agents"]

[[test]]
name = "handoff_surface"
required-features = ["agents"]

[[test]]
name = "team_surface"
required-features = ["agents"]
```

In `scripts/validate.sh`, directly after the existing `cargo check -p rakka-a2a --no-default-features` line (:14):

```sh
cargo check -p rakka-a2a --no-default-features --tests
```

Run: `cargo check -p rakka-a2a --no-default-features --tests` (timeout 600000)
Expected: exit 0. Before the four entries it fails with unresolved imports of `rakka_a2a::agents` and `rakka_agent`; run it once with the entries removed to see that, then restore them.

- [ ] **Step 2: Write the failing unit tests for the required-field rule**

In the `mod tests` of `crates/rakka-a2a/src/agents/guardrails.rs`, change `original()` to end with `..A2aCollaborationText::default()` after its three fields, and add:

```rust
    fn review_of(
        text: &A2aCollaborationText,
        collaboration: Value,
    ) -> Result<A2aContentReview, AgentAuthorityRefusal> {
        let scope = AgentTaskScope::new(
            TenantId::new("acme"),
            AgentTaskId::new("task-1").expect("the task id is valid"),
        )
        .expect("the task scope is valid");
        evaluate_a2a_content(
            &chain(collaboration),
            AgentGuardrailBoundary::A2aIngress,
            AgentGuardrailSubject::Task {
                scope: &scope,
                agent: None,
            },
            &parts(),
            Some(text),
        )
    }

    #[test]
    fn a_required_field_may_be_rewritten_and_may_not_be_cleared() {
        let required = A2aCollaborationText {
            body_required: true,
            reason_required: true,
            ..original()
        };
        let rewritten = review_of(&required, json!({ "body": "[b]", "reason": "[r]" }))
            .expect("rewriting is permitted")
            .text
            .expect("the review carries the cluster text");
        assert_eq!(rewritten.body.as_deref(), Some("[b]"));
        assert_eq!(rewritten.reason.as_deref(), Some("[r]"));
        assert!(rewritten.body_required && rewritten.reason_required);

        for cleared in [json!({ "body": null }), json!({ "reason": null })] {
            let refusal = review_of(&required, cleared.clone())
                .expect_err("a required field cannot be cleared");
            assert_eq!(refusal.code, "guardrail-transform-invalid", "{cleared}");
        }
    }

    #[test]
    fn the_envelope_says_which_fields_are_required() {
        use super::super::collaboration::parse_collaboration_envelope;
        use super::super::{
            AGENT_COLLABORATION_EXTENSION_URI, AGENT_COLLABORATION_SCHEMA_VERSION,
            META_COLLABORATION,
        };

        // Parsed from the wire shape, so the proof holds to whatever the
        // cluster types are.
        let text_of = |cluster: Value| {
            let mut message = a2a::Message::new(a2a::Role::User, parts());
            message.extensions = Some(vec![AGENT_COLLABORATION_EXTENSION_URI.to_string()]);
            let metadata: std::collections::HashMap<String, Value> =
                [(META_COLLABORATION.to_string(), cluster)]
                    .into_iter()
                    .collect();
            let envelope = parse_collaboration_envelope(&message, &metadata)
                .expect("the cluster parses")
                .expect("the message carries an envelope");
            collaboration_text(Some(&envelope)).expect("the envelope carries text")
        };

        let handoff = text_of(json!({
            "schema": AGENT_COLLABORATION_SCHEMA_VERSION,
            "handoff": "h-1",
            "source-agent": "a",
            "source-run": "r",
            "source-generation": 1,
            "target-agent": "b",
            "target-task-definition": "d",
            "reason": "needs billing authority",
            "policy-revision": 1,
        }));
        assert!(handoff.reason_required && !handoff.body_required);

        let message = text_of(json!({
            "schema": AGENT_COLLABORATION_SCHEMA_VERSION,
            "team": "t",
            "operation": "message",
            "member": "a",
            "body": "who owns this ticket?",
        }));
        assert!(message.body_required && !message.reason_required);

        let end = text_of(json!({
            "schema": AGENT_COLLABORATION_SCHEMA_VERSION,
            "conversation": "c",
            "operation": "end",
            "participant": "a",
            "expected-round": 0,
            "reason": "consensus",
        }));
        assert!(
            !end.body_required && !end.reason_required,
            "an end carries no body, and its reason is the one field a stage may clear"
        );
    }
```

Run: `cargo test -p rakka-a2a --features agents --lib agents::guardrails` (timeout 600000)
Expected: FAIL to compile: `A2aCollaborationText` has no field `body_required`.

- [ ] **Step 3: Implement the rule**

In `crates/rakka-a2a/src/agents/guardrails.rs`:

1. Add to `A2aCollaborationText`, after `context`:

```rust
    /// Whether the command requires the body it carries: a stage may rewrite
    /// it and may not clear it.
    pub(crate) body_required: bool,
    /// Whether the command requires the reason it carries.
    pub(crate) reason_required: bool,
```

2. `collaboration_text` becomes:

```rust
pub(crate) fn collaboration_text(
    envelope: Option<&AgentCollaborationEnvelope>,
) -> Option<A2aCollaborationText> {
    match envelope? {
        // A body is carried only by the verb that needs one, so a carried
        // body is a required one.
        AgentCollaborationEnvelope::Team(cluster) => Some(A2aCollaborationText {
            body: cluster.body.clone(),
            body_required: cluster.body.is_some(),
            ..A2aCollaborationText::default()
        }),
        AgentCollaborationEnvelope::Conversation(cluster) => Some(A2aCollaborationText {
            body: cluster.body.clone(),
            reason: cluster.reason.clone(),
            body_required: cluster.body.is_some(),
            ..A2aCollaborationText::default()
        }),
        AgentCollaborationEnvelope::Handoff(cluster) => Some(A2aCollaborationText {
            reason: Some(cluster.reason.clone()),
            context: cluster.context.clone(),
            reason_required: true,
            ..A2aCollaborationText::default()
        }),
        AgentCollaborationEnvelope::Delegation(_) => None,
    }
}
```

3. In `evaluate_a2a_content`, the `new_text` literal gains `body_required: text.body_required,` and `reason_required: text.reason_required,`, and directly after the existing "it adds a collaboration field the message did not carry" check:

```rust
        // Clearing is for a field the command can do without. A required
        // one cleared here would be half-applied: a handoff's reason has no
        // cleared form, so the original would survive a transform logged as
        // applied, and a cleared body would fail the command downstream as a
        // missing field and blame the caller for the stage's decision.
        if (text.body_required && new_text.body.is_none())
            || (text.reason_required && new_text.reason.is_none())
        {
            return Err(invalid(
                "it clears a collaboration field the command requires",
            ));
        }
```

4. In the doc comment of `evaluate_a2a_content`, replace "an explicit `null` clears it" with "an explicit `null` clears it, unless the command requires the field, which a stage may rewrite and may not clear".

In `crates/rakka-a2a/src/agents/handoff.rs` (:441-445), the text the egress evaluation is handed becomes:

```rust
                let text = super::guardrails::A2aCollaborationText {
                    reason: Some(handoff.reason.clone()),
                    context: handoff.context.clone(),
                    reason_required: true,
                    ..super::guardrails::A2aCollaborationText::default()
                };
```

Keep the binding name that site already uses.

Run: `cargo test -p rakka-a2a --features agents --lib agents::guardrails` (timeout 600000)
Expected: PASS, including the six tests that were already there.

- [ ] **Step 4: Add the shared stages**

Create `crates/rakka-a2a/tests/support/mod.rs`:

```rust
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
```

- [ ] **Step 5: Give each fixture a chain parameter**

The four fixtures build their service the same way. In each of `team_surface.rs` (:142-202), `conversation_surface.rs` (:131-205), and `human_task_surface.rs` (:130-196), replace `with_authorizer` with these three functions, moving its whole existing body into `build` unchanged except for the service:

```rust
    fn with_authorizer(authorizer: Arc<dyn A2AAuthorizer>) -> Self {
        Self::build(authorizer, None)
    }

    /// A fixture whose service evaluates `chain` at the `A2aIngress`
    /// boundary.
    fn with_ingress_chain(chain: rakka_agent::AgentGuardrailChain) -> Self {
        Self::build(Arc::new(AllowAllAuthorizer), Some(chain))
    }

    fn build(
        authorizer: Arc<dyn A2AAuthorizer>,
        ingress: Option<rakka_agent::AgentGuardrailChain>,
    ) -> Self {
```

Inside `build`, the expression that is `Arc::new(Service::new(…).with_clock(…).with_default_tenant(TENANT))` today (the conversation fixture's also ends in `.with_metrics(metrics.clone())`) becomes:

```rust
        let mut service = Service::new(/* the arguments it has today, unchanged */)
            .with_clock(Arc::new(TestClock(clock.clone())))
            .with_default_tenant(TENANT);
        if let Some(chain) = ingress {
            service = service.with_ingress_guardrails(Arc::new(chain));
        }
        let service = Arc::new(service);
```

keeping every builder call the fixture's own expression has, in its own order.

`handoff_surface.rs` (:167-262) takes two chains, because its handoff executor is built inside the fixture:

```rust
    fn with_authorizer(
        adapter: DeterministicModelAdapter,
        authorizer: Arc<dyn A2AAuthorizer>,
    ) -> Self {
        Self::build(adapter, authorizer, None, None)
    }

    /// A fixture whose service evaluates `ingress` at the `A2aIngress`
    /// boundary and whose handoff executor evaluates `egress` at `A2aEgress`.
    fn with_chains(
        adapter: DeterministicModelAdapter,
        ingress: Option<rakka_agent::AgentGuardrailChain>,
        egress: Option<rakka_agent::AgentGuardrailChain>,
    ) -> Self {
        Self::build(adapter, Arc::new(AllowAllAuthorizer), ingress, egress)
    }

    fn build(
        adapter: DeterministicModelAdapter,
        authorizer: Arc<dyn A2AAuthorizer>,
        ingress: Option<rakka_agent::AgentGuardrailChain>,
        egress: Option<rakka_agent::AgentGuardrailChain>,
    ) -> Self {
```

with the same service change, and the dispatcher line (:251) becoming:

```rust
        let mut executor = A2AAgentHandoffSendExecutor::new(service.clone());
        if let Some(chain) = egress {
            executor = executor.with_egress_guardrails(Arc::new(chain));
        }
        let dispatcher =
            ScriptedDispatcher::with_adapter(adapter).with_a2a_handoff_executor(Arc::new(executor));
```

Add `mod support;` to each of the four files, below its `use` block.

Run: `cargo test -p rakka-a2a --all-features --test team_surface --test conversation_surface --test handoff_surface --test human_task_surface` (timeout 600000)
Expected: PASS, with the counts these files had before: the refactor changes no behavior.

- [ ] **Step 6: Write the team and conversation proofs**

In `crates/rakka-a2a/tests/team_surface.rs`, add:

```rust
fn message_cluster(member: &str, body: &str) -> Value {
    json!({
        "schema": AGENT_COLLABORATION_SCHEMA_VERSION,
        "team": TEAM,
        "operation": "message",
        "member": member,
        "body": body,
    })
}

fn ingress(rules: Vec<Arc<dyn rakka_agent::AgentGuardrail>>) -> rakka_agent::AgentGuardrailChain {
    support::chain_at(rakka_agent::AgentGuardrailBoundary::A2aIngress, rules)
}

/// The team leaf evaluates ingress once per command, names the team as its
/// subject, and what a stage rewrote is what the board records.
#[tokio::test]
async fn an_ingress_chain_reviews_a_team_message_and_its_transform_is_recorded() {
    let recording = Arc::new(support::Recording::default());
    let fixture = Fixture::with_ingress_chain(ingress(vec![
        recording.clone(),
        Arc::new(support::RedactClusterText),
    ]));
    fixture.board_world().await;

    let response = fixture
        .service
        .send(
            &params(),
            &send_request(team_message(
                "m-1",
                message_cluster(MEMBER_A, "SENSITIVE who owns this ticket?"),
            )),
        )
        .await
        .expect("the message is served");
    assert!(
        response_payload(&response).get("Applied").is_some(),
        "{:?}",
        response_payload(&response)
    );
    assert_eq!(recording.seen(), 1, "once per command");
    assert_eq!(recording.subjects(), vec!["team"]);

    let snapshot = fixture.team_snapshot().await;
    let bodies: Vec<&str> = snapshot
        .messages
        .iter()
        .map(|message| message.body.as_str())
        .collect();
    assert_eq!(bodies, vec![support::REDACTED], "the board holds the rewrite");
}

/// A block refuses the command before the board sees it.
#[tokio::test]
async fn an_ingress_block_refuses_a_team_message_and_the_board_is_unchanged() {
    let fixture = Fixture::with_ingress_chain(ingress(vec![Arc::new(support::BlockMarker)]));
    fixture.board_world().await;
    let error = fixture
        .service
        .send(
            &params(),
            &send_request(team_message(
                "m-1",
                message_cluster(MEMBER_A, support::MARKER),
            )),
        )
        .await
        .expect_err("the marker is blocked");
    let RakkaAgentA2AError::Refused { code, reason, .. } = &error else {
        panic!("expected a refusal, got {error:?}")
    };
    assert_eq!(code, "guardrail-blocked");
    assert_eq!(
        reason
            .as_ref()
            .and_then(|reason| reason.stage())
            .map(ToString::to_string)
            .as_deref(),
        Some("stage-0")
    );
    assert!(fixture.team_snapshot().await.messages.is_empty());
}

/// A message's body is required: a stage that clears it is refused as a
/// stage's mistake, never passed on as the caller's missing field.
#[tokio::test]
async fn a_transform_that_clears_a_team_message_body_is_refused_as_the_stages_own() {
    let fixture = Fixture::with_ingress_chain(ingress(vec![Arc::new(support::ClearClusterText)]));
    fixture.board_world().await;
    let error = fixture
        .service
        .send(
            &params(),
            &send_request(team_message(
                "m-1",
                message_cluster(MEMBER_A, "who owns this ticket?"),
            )),
        )
        .await
        .expect_err("a required body cannot be cleared");
    assert!(
        matches!(
            &error,
            RakkaAgentA2AError::Refused { code, .. } if code == "guardrail-transform-invalid"
        ),
        "got {error:?}"
    );
    assert!(fixture.team_snapshot().await.messages.is_empty());
}
```

In `crates/rakka-a2a/tests/conversation_surface.rs`, add:

```rust
fn ingress(rules: Vec<Arc<dyn rakka_agent::AgentGuardrail>>) -> rakka_agent::AgentGuardrailChain {
    support::chain_at(rakka_agent::AgentGuardrailBoundary::A2aIngress, rules)
}

/// The conversation leaf evaluates ingress once per command, names the
/// conversation as its subject, and what a stage rewrote is the turn the
/// conversation records.
#[tokio::test]
async fn an_ingress_chain_reviews_a_turn_and_its_transform_is_recorded() {
    let recording = Arc::new(support::Recording::default());
    let fixture = Fixture::with_ingress_chain(ingress(vec![
        recording.clone(),
        Arc::new(support::RedactClusterText),
    ]));
    fixture.conversation_world().await;

    let response = fixture
        .service
        .send(
            &params(),
            &send_request(conversation_message(
                "turn-1",
                submit_cluster(MEMBER_A, 0, 0, "SENSITIVE proposal", 25),
            )),
        )
        .await
        .expect("the turn is served");
    assert!(
        response_payload(&response).get("Applied").is_some(),
        "{:?}",
        response_payload(&response)
    );
    assert_eq!(recording.seen(), 1, "once per command");
    assert_eq!(recording.subjects(), vec!["conversation"]);

    let recorded = serde_json::to_string(&fixture.conversation_snapshot().await)
        .expect("the snapshot encodes");
    assert!(recorded.contains(support::REDACTED), "{recorded}");
    assert!(!recorded.contains("SENSITIVE"), "{recorded}");
}

/// A block refuses the turn before the conversation sees it.
#[tokio::test]
async fn an_ingress_block_refuses_a_turn_and_the_conversation_is_unchanged() {
    let fixture = Fixture::with_ingress_chain(ingress(vec![Arc::new(support::BlockMarker)]));
    fixture.conversation_world().await;
    let before = serde_json::to_string(&fixture.conversation_snapshot().await)
        .expect("the snapshot encodes");
    let error = fixture
        .service
        .send(
            &params(),
            &send_request(conversation_message(
                "turn-1",
                submit_cluster(MEMBER_A, 0, 0, support::MARKER, 25),
            )),
        )
        .await
        .expect_err("the marker is blocked");
    assert!(
        matches!(
            &error,
            RakkaAgentA2AError::Refused { code, .. } if code == "guardrail-blocked"
        ),
        "got {error:?}"
    );
    // The snapshot read itself ticks the fixture's clock, so the comparison
    // is of what the conversation recorded, not of when it was read.
    let after = fixture.conversation_snapshot().await;
    assert!(
        !serde_json::to_string(&after)
            .expect("the snapshot encodes")
            .contains(support::MARKER),
        "nothing of the blocked turn was recorded"
    );
    assert!(!before.contains(support::MARKER));
}

/// A turn's body is required; an end's reason is not.
#[tokio::test]
async fn a_cleared_turn_body_is_refused_and_a_cleared_end_reason_is_applied() {
    let fixture = Fixture::with_ingress_chain(ingress(vec![Arc::new(support::ClearClusterText)]));
    fixture.conversation_world().await;
    let error = fixture
        .service
        .send(
            &params(),
            &send_request(conversation_message(
                "turn-1",
                submit_cluster(MEMBER_A, 0, 0, "the proposal", 25),
            )),
        )
        .await
        .expect_err("a required body cannot be cleared");
    assert!(
        matches!(
            &error,
            RakkaAgentA2AError::Refused { code, .. } if code == "guardrail-transform-invalid"
        ),
        "got {error:?}"
    );

    // An end is a governed decision: it needs an authenticated principal.
    let mut end = conversation_message("end-1", end_cluster(MODERATOR, 0, "SENSITIVE reason"));
    end.metadata
        .as_mut()
        .expect("the message carries metadata")
        .insert(META_PRINCIPAL_REF.to_string(), json!("user:operator-7"));
    let ended = fixture
        .service
        .send(&params(), &send_request(end))
        .await
        .expect("an end whose reason a stage cleared is still an end");
    assert!(
        response_payload(&ended).get("Applied").is_some(),
        "{:?}",
        response_payload(&ended)
    );
    let recorded = serde_json::to_string(&fixture.conversation_snapshot().await)
        .expect("the snapshot encodes");
    assert!(!recorded.contains("SENSITIVE"), "{recorded}");
}
```

If `RakkaAgentA2AError` is not already imported in `conversation_surface.rs`, add it to the `rakka_a2a::agents` import. The end mirrors the file's own accepting end (`end-auth`, :688): the moderator, round 0, an authenticated principal.

- [ ] **Step 7: Write the handoff and typed-result proofs**

In `crates/rakka-a2a/tests/handoff_surface.rs`, add:

```rust
/// Drives the source's turn, which commits the transfer and sends it through
/// the service, and answers the task's recorded handoff reason, when the
/// transfer recorded one.
async fn handed_off_reason(fixture: &Fixture) -> Option<String> {
    fixture.instantiate(&source()).await;
    fixture.instantiate(&target()).await;
    fixture.create_task().await;
    fixture
        .service
        .get_task(&params(), Some(TENANT), TASK, None, None)
        .await
        .expect("the projection bootstraps");
    fixture.pump(&source(), 1).await;
    fixture.pump(&source(), 1).await;

    let mut task = AgentTaskEntityStore::new(
        fixture.task_scope(),
        fixture.tasks.clone(),
        fixture.agents.clone(),
        fixture.history.clone(),
    );
    task.recover(fixture.now())
        .await
        .expect("the task recovers");
    task.snapshot()
        .expect("the snapshot reads")
        .expect("the task exists")
        .handoff
        .as_deref()
        .map(|provenance| provenance.reason.clone())
}

fn both_turns() -> DeterministicModelAdapter {
    DeterministicModelAdapter::new()
        .with_turn(handoff_turn())
        .with_turn(proposing_turn())
}

fn chain(
    boundary: rakka_agent::AgentGuardrailBoundary,
    rule: Arc<dyn rakka_agent::AgentGuardrail>,
) -> rakka_agent::AgentGuardrailChain {
    support::chain_at(boundary, vec![rule])
}

/// The egress text arm: what the executor's stage rewrote is what the task
/// records.
#[tokio::test]
async fn an_egress_transform_of_the_handoff_reason_is_what_the_task_records() {
    let fixture = Fixture::with_chains(
        both_turns(),
        None,
        Some(chain(
            rakka_agent::AgentGuardrailBoundary::A2aEgress,
            Arc::new(support::RedactClusterText),
        )),
    );
    assert_eq!(
        handed_off_reason(&fixture).await.as_deref(),
        Some(support::REDACTED)
    );
}

/// The ingress text arm: what the service's stage rewrote is what the task
/// records, and the stage was shown the transfer's target.
#[tokio::test]
async fn an_ingress_transform_of_the_handoff_reason_is_what_the_task_records() {
    let recording = Arc::new(support::Recording::default());
    let fixture = Fixture::with_chains(
        both_turns(),
        Some(support::chain_at(
            rakka_agent::AgentGuardrailBoundary::A2aIngress,
            vec![recording.clone(), Arc::new(support::RedactClusterText)],
        )),
        None,
    );
    assert_eq!(
        handed_off_reason(&fixture).await.as_deref(),
        Some(support::REDACTED)
    );
    assert!(
        recording.subjects().contains(&"task-for-agent"),
        "the handoff leaf names the task and the agent it transfers to: {:?}",
        recording.subjects()
    );
    assert_eq!(
        recording
            .last_view()
            .pointer("/collaboration/reason")
            .and_then(Value::as_str),
        Some("needs billing authority"),
        "the first stage saw the reason the model gave"
    );
}

/// A handoff's reason is required at both boundaries: a stage that clears it
/// refuses the transfer, and no transfer is recorded under the original.
#[tokio::test]
async fn a_cleared_handoff_reason_refuses_the_transfer_at_either_boundary() {
    for boundary in [
        rakka_agent::AgentGuardrailBoundary::A2aEgress,
        rakka_agent::AgentGuardrailBoundary::A2aIngress,
    ] {
        let cleared = Some(chain(boundary, Arc::new(support::ClearClusterText)));
        let fixture = if boundary == rakka_agent::AgentGuardrailBoundary::A2aEgress {
            Fixture::with_chains(both_turns(), None, cleared)
        } else {
            Fixture::with_chains(both_turns(), cleared, None)
        };
        assert_eq!(
            handed_off_reason(&fixture).await,
            None,
            "no transfer was recorded at {boundary:?}"
        );

        let mut run = run_entity(
            &fixture.run_scope(&source(), 1),
            &fixture.runs,
            &fixture.effects,
        );
        run.recover(fixture.now())
            .await
            .expect("the source recovers");
        let state = run.state().expect("state");
        let cell = state
            .loop_state()
            .expect("the loop exists")
            .handoff()
            .expect("the cell survives");
        assert!(
            matches!(
                &cell.status,
                rakka_agent::AgentHandoffStatus::Failed { code, .. }
                    if code == "guardrail-transform-invalid"
            ),
            "at {boundary:?} the cell settles under the stage's own refusal, got {:?}",
            cell.status
        );
    }
}
```

In `crates/rakka-a2a/tests/human_task_surface.rs`, add:

```rust
/// The typed-result leaf evaluates ingress too: a poisoned submission is
/// refused before the task sees it, and the stage is shown the task with no
/// agent, because a human's result names none.
#[tokio::test]
async fn an_ingress_chain_reviews_a_result_submission() {
    let recording = Arc::new(support::Recording::default());
    let fixture = Fixture::with_ingress_chain(support::chain_at(
        rakka_agent::AgentGuardrailBoundary::A2aIngress,
        vec![recording.clone(), Arc::new(support::BlockMarker)],
    ));
    fixture.create_human_task().await;
    let before = fixture.snapshot().await;

    let error = fixture
        .service
        .send_message(
            &params(),
            &send_request(submission_message(
                "poisoned",
                json!({ "answer": support::MARKER }),
            )),
        )
        .await
        .expect_err("the marker is blocked");
    assert!(
        matches!(
            &error,
            RakkaAgentA2AError::Refused { code, .. } if code == "guardrail-blocked"
        ),
        "got {error:?}"
    );
    assert_eq!(recording.seen(), 1);
    assert_eq!(recording.subjects(), vec!["task"]);

    let after = fixture.snapshot().await;
    assert_eq!(after.status, before.status);
    assert!(after.accepted_result.is_none());
    assert_eq!(
        fixture
            .history_count(AgentTaskHistoryKind::ResultProposed)
            .await,
        0,
        "nothing of the blocked submission was recorded"
    );

    let accepted = fixture
        .service
        .send_message(
            &params(),
            &send_request(submission_message("clean", json!({ "answer": "approved" }))),
        )
        .await;
    assert!(accepted.is_ok(), "a clean submission still lands: {accepted:?}");
    assert_eq!(recording.seen(), 2);
}
```

The clean answer is the one `an_authenticated_submission_completes_the_human_task` (:325) submits.

- [ ] **Step 8: Run the proofs**

Run: `cargo test -p rakka-a2a --all-features --test team_surface --test conversation_surface --test handoff_surface --test human_task_surface --test ingress_egress_guardrails` (timeout 600000)
Expected: PASS. If a proof of Step 6 or 7 fails because the fixture's world differs from what the test assumes, fix the test's use of the fixture, never the assertion about the guardrail: once per command, the subject named, the rewrite recorded, the required field refused.

- [ ] **Step 9: State the operation-id drift where a reader will meet it**

In `crates/rakka-agent/src/conversation.rs`, the `SubmitTurn.operation_id` doc (:1752-1754) becomes:

```rust
        /// Stable dedup identity of this turn, derived over the coordinate
        /// and the body digest by
        /// [`crate::coordination::conversation_turn_operation_id`].
        ///
        /// Over the body **as the caller sent it**. An `A2aIngress` stage
        /// that rewrites the body does so after the id is derived, so the id
        /// names the wire body while [`AgentConversationTurnSubmit::body`]
        /// and the turn ledger hold the admitted one. A retried send under
        /// the same chain re-derives the same id and the same rewrite and
        /// converges. Under a chain whose revision changed between the send
        /// and its retry, the operation log answers `Duplicate` inside its
        /// window and the ledger answers `conversation-turn-content-mismatch`
        /// past it.
```

and the `EndEarly.operation_id` doc (:1760-1762) gains: "Over the reason as the caller sent it; an ingress stage that rewrites or clears the reason does not change the id."

In `crates/rakka-agent/src/team.rs`, find the `AppendMessage` variant of `AgentTeamEntityCommand` and add to its `operation_id` field doc: "Derived over the cluster as the caller sent it, body included; an ingress stage that rewrites the body does not change the id."

- [ ] **Step 10: Lint and commit**

Run: `cargo fmt --all -- --check`, `cargo clippy -p rakka-a2a -p rakka-agent --all-targets --all-features -- -D warnings`, and `cargo doc -p rakka-agent --no-deps` (timeout 600000 each)
Expected: all exit 0, the doc build with no broken intra-doc link.

```bash
git add crates/rakka-a2a/src/agents/guardrails.rs crates/rakka-a2a/src/agents/handoff.rs \
        crates/rakka-a2a/Cargo.toml crates/rakka-a2a/tests/support/mod.rs \
        crates/rakka-a2a/tests/team_surface.rs crates/rakka-a2a/tests/conversation_surface.rs \
        crates/rakka-a2a/tests/handoff_surface.rs crates/rakka-a2a/tests/human_task_surface.rs \
        crates/rakka-agent/src/conversation.rs crates/rakka-agent/src/team.rs scripts/validate.sh
git commit -m "Refuse a transform that clears a required collaboration field, and prove every A2A ingress leaf and both handoff text arms with a chain installed

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

### Task 11: The deadline is recomputed on the retry

Spec 4.7 names this proof and slice 7.1 left it owed: the one existing test drives a single attempt and pins the deadline only to a range. The deadline is observable in one place. `call_with` receives an `AgentModelRequest`, which has no deadline field; the per-attempt intent reaches the credential resolver and the tool executors. So the proof reads the resolver.

**Files:**
- Test: `crates/rakka-agent/tests/model_provider_dispatch.rs`

**Interfaces:**
- Consumes: `AuthorityFixture::{with_model_adapter, one_pass, pump, wf_clock, credentials}` (`tests/common/mod.rs`), `ScriptedCredentialResolver::deadlines() -> Vec<Option<AgentTimestampMillis>>` (`testkit.rs:3486`), `SharedAtomicWorkflowClock::advance(millis)`; the file's own `profiled_fixture_with`, `select_profile`, `committed_model_bound`, `proposing_turn`.
- Produces: one test. No production change.

- [ ] **Step 1: Write the test**

In `crates/rakka-agent/tests/model_provider_dispatch.rs`, after `the_profile_credential_reaches_call_with_under_the_attempt_deadline` (:325):

```rust
/// The deadline is the attempt's, not the effect's: a retry resolves its
/// credential under a deadline recomputed from the retry's own start, and
/// the durable record holds neither.
#[tokio::test]
async fn a_retried_model_call_resolves_under_a_deadline_recomputed_from_its_own_start() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use rakka_agent::{AgentModelAdapter, AgentModelError, AgentModelFuture, AgentModelRequest};

    const TIMEOUT_MS: u64 = 30_000;
    const BETWEEN_ATTEMPTS_MS: u64 = 10_000;

    /// Fails its first call the way a provider outage does, and answers the
    /// second. It declares the two attempts it needs: the dispatcher reads an
    /// adapter's declaration as a ceiling and refuses a spec that asks for
    /// more (`model-policy-conflict`).
    struct FailsOnce {
        calls: AtomicUsize,
    }

    impl AgentModelAdapter for FailsOnce {
        fn adapter_version(&self) -> AgentRevisionNumber {
            CURRENT_AGENT_LOOP_ADAPTER_VERSION
        }

        fn retry_policy(&self) -> AgentModelRetryPolicy {
            AgentModelRetryPolicy::read_only(2).expect("two read-only attempts is a valid policy")
        }

        fn call<'a>(&'a self, _request: &'a AgentModelRequest) -> AgentModelFuture<'a> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                if call == 0 {
                    Err(AgentModelError::Provider {
                        message: "the provider is unavailable".to_string(),
                    })
                } else {
                    Ok(proposing_turn())
                }
            })
        }
    }

    let spec = AgentEffectSpec::read_only()
        .with_timeout_ms(TIMEOUT_MS)
        .with_max_attempts(2)
        .expect("two attempts is a valid bound");
    let adapter = Arc::new(FailsOnce {
        calls: AtomicUsize::new(0),
    });
    let fx = profiled_fixture_with(DeterministicModelAdapter::new(), true, spec)
        .with_model_adapter(adapter.clone());
    fx.start().await;
    select_profile(&fx).await;
    fx.settle().await;
    assert_eq!(
        committed_model_bound(&fx).await,
        (Some(TIMEOUT_MS), None),
        "the committed effect carries the bound and no deadline"
    );

    // The first attempt: the credential resolves, the provider fails, the
    // attempt is recorded as failed and the ticket stays claimable.
    let first = fx.one_pass().await;
    assert_eq!(first.failed_attempts, 1, "{first:?}");
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 1);
    let resolver = fx.credentials.as_ref().expect("resolver");
    let after_first = resolver.deadlines();
    assert_eq!(after_first.len(), 1);
    let first_deadline = after_first[0].expect("the first attempt carries a deadline");
    assert!(
        (TIMEOUT_MS..=fx.fx.now().as_millis() + TIMEOUT_MS).contains(&first_deadline.as_millis()),
        "the first deadline is the first attempt's start plus the bound: {first_deadline:?}"
    );

    // Time passes between the attempts, and only the dispatcher's clock
    // knows it.
    fx.wf_clock.advance(BETWEEN_ATTEMPTS_MS);
    fx.pump().await;

    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
    assert_eq!(
        adapter.calls.load(Ordering::SeqCst),
        2,
        "the second attempt answered"
    );
    let deadlines = resolver.deadlines();
    assert_eq!(deadlines.len(), 2, "one resolution per attempt");
    let second_deadline = deadlines[1].expect("the retry carries a deadline too");
    assert!(
        second_deadline.as_millis() >= first_deadline.as_millis() + BETWEEN_ATTEMPTS_MS,
        "recomputed from the retry's own start, not carried over: \
         {first_deadline:?} then {second_deadline:?}"
    );
    assert!(
        second_deadline.as_millis() <= fx.fx.now().as_millis() + TIMEOUT_MS,
        "and still the retry's start plus the bound: {second_deadline:?}"
    );
    assert_eq!(
        committed_model_bound(&fx).await,
        (Some(TIMEOUT_MS), None),
        "neither deadline was ever persisted"
    );
}
```

- [ ] **Step 2: Run the test**

Run: `cargo test -p rakka-agent --test model_provider_dispatch a_retried_model_call` (timeout 600000)
Expected: PASS. This is a proof of behavior slice 7.1 already shipped, so it passes on its first run; Step 3 shows it can fail.

If it fails with `model-policy-conflict`, the fixture's pipeline is not reading the installed adapter's `retry_policy`: confirm `with_model_adapter` is what the pipeline builds from (`tests/common/mod.rs:2972-2981`). If the first pass reports `failed_attempts == 0`, the model ticket was not yet claimable: replace `fx.one_pass()` with the same two-step the file's first test uses, `fx.settle().await` then one `fx.one_pass().await`, and assert on that pass.

- [ ] **Step 3: Falsify it**

In `crates/rakka-agent/src/dispatch.rs`, at the per-attempt stamp (:2689-2693), temporarily replace `attempt_started_at.as_millis().saturating_add(timeout_ms)` with `intent.created_at.as_millis().saturating_add(timeout_ms)`, a deadline computed once from the effect rather than per attempt.

Run: `cargo test -p rakka-agent --test model_provider_dispatch a_retried_model_call` (timeout 600000)
Expected: FAIL at "recomputed from the retry's own start, not carried over". Keep the failure output for the task report, then restore the line and confirm `git diff --stat crates/rakka-agent/src/dispatch.rs` prints nothing.

- [ ] **Step 4: Run the file, lint, and commit**

Run: `cargo test -p rakka-agent --test model_provider_dispatch` (timeout 600000)
Expected: PASS, every test in the file.

Run: `cargo fmt --all -- --check` and `cargo clippy -p rakka-agent --all-targets --all-features -- -D warnings` (timeout 600000)
Expected: both exit 0.

```bash
git add crates/rakka-agent/tests/model_provider_dispatch.rs
git commit -m "Prove a retried model call resolves its credential under a deadline recomputed from the retry's own start

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

# Part D — Records and phase close

### Task 12: Records, the report for issue #80, validation, and the phase close

**Files:**
- Modify: `docs/rakka-compatibility.md` (a new bullet after the slice 7.7 bullet at :93; two sentences inside the 7.7 bullet)
- Modify: `CHANGELOG.md` (`### Added` :9, `### Changed` :485, `### Fixed` :539)
- Modify: `docs/rakka-agent-security-validation-matrix.md` (the MCP credential row :131; two new rows)
- Modify: `docs/rakka-agent-telemetry-validation-matrix.md` (the 17.11 checkpoint row :49)
- Modify: `docs/rakka-agents.md` (the guardrail paragraph :267-273; the MCP paragraph)
- Modify: `docs/rakka-api-boundary-inventory.md` (the `rakka-agent-mcp` row :70 and the `rakka-agent` row)
- Modify: `docs/plans/rakka-agent/spec.md` (11.3 :1174, 16 :1747, 17.11 :2066)
- Modify: `docs/superpowers/specs/2026-09-19-phase7-agent-surface-parity-design.md` (inline marks; the planner's phase-cut amendment is already in the working tree)
- Create, never commit: `.superpowers/sdd/2026-09-28-phase7-slice-7-10-follow-ups-and-close/issue-80-report.md`
- Commit: this plan and the spec

**Interfaces:**
- Consumes: every earlier task's commits and task reports.
- Produces: the records, the report, a green validation on the final tree, and a branch ready for the owner's integration decision. Nothing is pushed.

- [ ] **Step 1: The compatibility bullet**

In `docs/rakka-compatibility.md`, add one bullet directly after the slice 7.7 bullet (:93). Write it as one paragraph, in the register of the bullets above it, stating exactly these facts and nothing the code does not do:

1. Phase 7 slice 7.10 ("follow-ups and phase close") adds one stable refusal code, one record type, five additive durable fields, and four exported constants for reason codes that already existed; it bumps no schema version and changes no pipeline code.
2. `mcp-descriptor-credential-echoed`: a publish-time sync whose server-chosen text — the server's name, a tool's description, its input schema, or its output schema — carries the credential the sync sent. The sync stores nothing. It is a refusal and not a redaction because a schema's SHA-256 digest is what the dispatch-time recheck and the release digest compare. `McpSyncError` reaches a caller directly, never through `AgentDispatchError::Collaborator`. This is the sixteenth MCP code.
3. Both syncs are bounded: `MCP_SYNC_TIMEOUT_DEFAULT_MS` (30 000) over the handshake and the listing; `sync_mcp_descriptors_within` and `sync_mcp_descriptors_over_within` take the caller's bound; a sync that runs out of time is `mcp-descriptor-sync-failed`. `McpDescriptorSet.server_name` is cut at `MCP_SERVER_NAME_MAX_BYTES` (256).
4. `McpArtifactSink` is the executor's write path (`&self`, `Send + Sync`, handed the run scope); `McpDispatchToolExecutor::new` and `with_launcher` take `impl Into<McpArtifacts>`, which an `McpArtifactStore` converts into, so existing call sites are unchanged. The artifact id `mcp-<effect id>-g<generation>-<call id>` is requested, not required: a store may mint its own. The reference a sink returns must pass `validate_artifact_ref`, and one that does not fails the attempt under the artifact error's own code (`invalid-artifact-reference`). A stored result is written as `ArtifactKind::ToolOutput`, marked `RedactionStatus::Redacted` when the credential scrub changed its bytes. An inline result and an artifact reference are both measured as the run measures them, wrapper included, against `AGENT_TOOL_RESULT_MAX_BYTES`.
5. `AgentFailureReason { code, stage }` records which decision failed an effect, beside the pipeline code: a guardrail's stage id and reason code, or a collaborator's own code. `code` is bounded at `AGENT_FAILURE_REASON_CODE_MAX_LENGTH` (128) on construction and on decode; `stage` is an `AgentGuardrailStageId`. It is a code, never text, and never a metric label.
6. The field is `reason: Option<AgentFailureReason>`, `#[serde(default, skip_serializing_if = "Option::is_none")]`, on `AgentRunEffectOutcome::Failed` and `Exhausted`, `AgentRunTerminalReason::EffectFailed`, `AgentDelegationStatus::Failed`, and `AgentHandoffStatus::Failed`; and `last_error_reason`, same attributes, on `AgentRunEffect`. A record written before the change decodes with `None`; a record without a reason serializes byte-identically to before. **No schema version is bumped**, on the `AgentRunEffect::telemetry` precedent: the field is observability only and no decision reads it. The cost is stated: a binary that predates the field drops it when it re-persists a record during a rolling downgrade, and the pipeline code beside it survives.
7. **Source-breaking, all compile-time.** Each of those five variants gains a field, so an exhaustive struct pattern or a struct literal over one stops compiling; a pattern takes `..` and a literal takes `reason: None`, or the literal becomes `AgentRunEffectOutcome::failed(code, message)` / `exhausted(code, message)`. The same holds for `AgentA2aSendFinding::Refused`, `AgentA2aHandoffFinding::Refused`, and `RakkaAgentA2AError::Refused`. `AgentAuthorityRefusal` gains the public field `reason`; nothing in the tree builds it by literal.
8. The A2A wire is unchanged: a remote caller reads the pipeline code and the message. The reason is in-process.
9. Four guardrail reason codes that were bare literals are exported and registered: `text-too-long` (`AGENT_GUARDRAIL_REASON_TEXT_TOO_LONG`), `denied-substring` (`AGENT_GUARDRAIL_REASON_DENIED_SUBSTRING`), `undeclared-tool-call` (`AGENT_GUARDRAIL_REASON_UNDECLARED_TOOL_CALL`), and the chain's own `guardrail-transform-oversized` (`AGENT_GUARDRAIL_REASON_TRANSFORM_OVERSIZED`). No registered code changed meaning.
10. A `checkpoint-resolve` segment closes on the call whose transition committed the resolution and left the checkpoint no longer open, whatever the settle pass after it answered. An escalation, a duplicate, and a refusal close none. `run-resume` closes on the call that first observes the wait ended.
11. `guardrail-transform-invalid` and `guardrail-transform-unsupported` each cover one more case and neither changes meaning: a model-response transform that adds a proposal to a turn that made none is invalid, one that changes any field of a reference proposal is unsupported, and an A2A transform that clears a collaboration field the command requires is invalid.

Inside the slice 7.7 bullet, replace the sentence that ends "so the `AgentArtifactStore` behind an `McpArtifactStore` must accept keys with slashes." with "and that id is a request: see the slice 7.10 bullet." and replace "Every code above but `mcp-transport-failed` reaches a caller through `AgentDispatchError::Collaborator`" with "Every dispatch-time code above but `mcp-transport-failed` reaches a caller through `AgentDispatchError::Collaborator` (the sync's own codes reach one through `McpSyncError`)".

- [ ] **Step 2: The changelog**

In `CHANGELOG.md` under `## Unreleased`:

Under `### Added`, one entry: "`rakka-agent` and `rakka-agent-mcp`, Phase 7 slice 7.10 (follow-ups and phase close)", stating items 2 through 6 and 9 of Step 1 in the changelog's own register, naming every new public item: `McpArtifactSink`, `McpArtifactFuture`, `McpArtifacts`, `McpSyncError::CredentialEchoed`, `MCP_SERVER_NAME_MAX_BYTES`, `MCP_SYNC_TIMEOUT_DEFAULT_MS`, `sync_mcp_descriptors_within`, `sync_mcp_descriptors_over_within`, `AgentFailureReason`, `AGENT_FAILURE_REASON_CODE_MAX_LENGTH`, `AgentRunEffectOutcome::{failed, exhausted, with_reason, failure_reason}`, `AgentRunEffect::last_error_reason`, `AgentAuthorityRefusal::with_reason`, the two cells' `settle_failed_because`, the four reason-code constants, and the testkit's `CrashPoint::ConflictBeforeWrite`.

Under `### Changed`, one entry stating item 7 of Step 1 as a migration note a consumer can act on, with the two rewrites spelled out.

Under `### Fixed`, five entries, each one sentence of what was wrong and one of what holds now:

1. An MCP result whose content encoded to 2 038–2 048 bytes passed the executor and was then refused by the run as `effect-tool-result-too-large`.
2. A publish-time sync stored server-chosen text that carried the credential it had sent.
3. A checkpoint resolution whose settle pass lost a compare-and-set, or failed in a way the call retried, left no `checkpoint-resolve` segment on any call; and an escalation closed one for a resolution that had not happened.
4. A model-response transform could add a proposal the model never made, or rewrite any field of a reference proposal under its own id.
5. A guardrail stage that cleared a required collaboration field was half-applied: a handoff kept its original reason under a transform logged as applied, and a cleared message body failed the command with a mapping error blaming the caller. Four `rakka-a2a` test files did not compile without the `agents` feature.

Under `### Validation`, one line: `scripts/validate.sh` now compiles `rakka-a2a`'s tests with no features.

- [ ] **Step 3: The matrices, the product doc, and the inventory**

1. `docs/rakka-agent-security-validation-matrix.md`. In the MCP credential row (:131), after "has the credential's material replaced by `<redacted>`;", add: "a publish-time sync whose server-chosen text carries the credential it sent stores nothing (`mcp-descriptor-credential-echoed`);" and add to its proof cell `crates/rakka-agent-mcp/tests/sync_credential_echo.rs` (seven tests). Add two rows to the same table:

| Clause | Enforced where | Proof | Status |
| --- | --- | --- | --- |
| A stored MCP result is filed under the run that produced it and recorded only behind a reference the default policy accepts | `McpArtifactSink::put_result` is handed the run scope; the executor validates the returned reference before it becomes the run's record | `crates/rakka-agent-mcp/tests/artifact_sink.rs` | Met |
| A publish-time sync is bounded in time and leaves no session open | `sync_mcp_descriptors` and `sync_mcp_descriptors_over` apply `MCP_SYNC_TIMEOUT_DEFAULT_MS`; the session is closed on every path | `crates/rakka-agent-mcp/tests/sync_deadline.rs` | Met |

and one row to the guardrail table the `ModelResponse` row sits in: clause "A failure's deciding identity is a bounded code and a stage id, never text", enforced where "`AgentFailureReason`, bounded on construction and on decode; the refusal's message and the collaborator's detail reach no record", proof "`failure_reason_records.rs`, `secret_exclusion.rs::an_exhausted_resolution_records_the_resolvers_code_and_never_its_detail`, `model_response_guardrails.rs::a_built_in_stage_is_named_on_the_runs_records_and_its_message_is_not`", status Met.

2. `docs/rakka-agent-telemetry-validation-matrix.md`, the 17.11 checkpoint row (:49): add to its "enforced where" cell "the resolve segment closes when the resolving transition commits and drops the checkpoint, under the run context read before the command" and to its proof cell `checkpoint_resolve_segment.rs` and `checkpoint_reconciliation.rs::an_escalation_closes_no_resolve_segment_and_the_resolution_closes_one`. Leave the "A timer or child wait closes no park segment" item (:214) exactly as it is: issue #70's third item is still owed.

3. `docs/rakka-agents.md`. In the guardrail paragraph (:267-273), after "A blocked model response fails the effect once under `guardrail-blocked`;" add "the run's record names the stage and the reason code that decided beside that code;". In the MCP paragraph, state in two sentences that an over-large result is written through a sink a deployment implements, and that a sync refuses a listing that echoes its credential and is bounded in time.

4. `docs/rakka-api-boundary-inventory.md`. In the `rakka-agent-mcp` row (:70) add "an artifact sink seam"; in the `rakka-agent` row add "a failure's deciding identity (`AgentFailureReason`)".

Run: `cargo test -p rakka-agent --features otel --test compatibility_currency` and every doc-holding test the repository has — `grep -rln 'rakka-compatibility.md\|rakka-agents.md\|validation-matrix.md' crates/*/tests` lists them; run each file it names (timeout 600000 each).
Expected: PASS. A doc-holding test that fails names the claim it could not hold; fix the document, never the test.

- [ ] **Step 4: The normative specification**

In `docs/plans/rakka-agent/spec.md`:

1. 11.3, after the effect state model's closing paragraph, add:

```markdown
A failed or exhausted effect is recorded under the stable code of the
dispatch step that failed. Where one party decided the failure — a guardrail
stage, a credential resolver, an executor — the record SHOULD also carry that
party's own stable code, and for a guardrail its stage identity, as a bounded
field beside the step's code. The field is observability: no dispatch,
recovery, or resolution decision MAY read it, and its absence MUST NOT change
any outcome.
```

2. 16, after the guardrail outcome bullet (:1747-1749), add:

```markdown
- A durable record MAY carry a guardrail's stage identity and stable reason
  code. It MUST NOT carry the refusal's message, the evaluated content, or a
  protected evidence reference outside the bounded failure detail that
  already holds one.
```

3. 17.11, after "The later resolution/resume span MUST link to the parked span and the incoming human/service request span." add:

```markdown
The resolution span's subject is the resolving transition. It MUST be closed
on the call whose transition committed the resolution, whether or not the
work that followed the commit succeeded, and it MUST NOT be closed for a
decision that left the checkpoint open, for a replay of an applied
resolution, or for a refused one.
```

- [ ] **Step 5: Mark the design spec**

In `docs/superpowers/specs/2026-09-19-phase7-agent-surface-parity-design.md`, add one paragraph to section 0, after "**Phase cut (2026-09-28).**":

```markdown
**Slice 7.10 (2026-09-28).** What the follow-up slice built, each marked
inline as "slice 7.10": the MCP executor writes an over-large result through
`McpArtifactSink`, a `&self` seam handed the run scope, and measures every
result as the run does (5.3); a sync refuses server text that carries the
credential it sent, bounds the server name, and runs under a deadline (5.2);
a failure's deciding identity, `AgentFailureReason`, rides from the refusal
to the run's records, and a collaborator's own code is the reason of an
exhausted outcome (11.1, and issue #80); a `checkpoint-resolve` segment
closes when the resolution commits; a model-response transform may not add a
proposal or rewrite a reference (6.1); an A2A transform may not clear a field
the command requires, and every ingress leaf is proven with a chain
installed (6.3); and the two-attempt deadline proof 4.7 named exists. One
statement of 4.2 item 3 is corrected: the per-attempt deadline reaches the
credential resolver and the tool executors on the intent they are handed; a
model adapter's `call_with` receives the request, which carries none. Plan:
`docs/superpowers/plans/2026-09-28-phase7-slice-7-10-follow-ups-and-close.md`.
```

and place these inline marks, each a parenthesis opening "(slice 7.10:" at the sentence it corrects:

| Section | Sentence | Mark |
| --- | --- | --- |
| 4.2 item 3 | "on the intent it hands to `resolve`, to `execute`, and to `call_with`" | `call_with` receives the request, which has no deadline field; the stamp is observable at the resolver and the executors |
| 4.7 | "recomputed on the retry" | proven by `a_retried_model_call_resolves_under_a_deadline_recomputed_from_its_own_start` |
| 5.2 | the sentence that says the sync has no deadline of its own | both syncs apply `MCP_SYNC_TIMEOUT_DEFAULT_MS`; the `_within` twins take the caller's bound |
| 5.2 | the first sentence describing what the sync stores | server text that carries the sync's credential refuses the sync; the server name is cut at 256 bytes |
| 5.3 | "`artifacts` is `McpArtifactStore`" | `artifacts` is `impl Into<McpArtifacts>`; the write seam is `McpArtifactSink` |
| 5.3 | the Rust sketch that shows `with_child_process_launcher` | the constructor is `with_launcher`, as the prose below says |
| 6.1 | the transform rule | a transform may not add a proposal, and a reference survives only whole |
| 6.3 | "explicit null clears" | unless the command requires the field |
| 11.1 | the MCP code list | `mcp-descriptor-credential-echoed` is the sixteenth |

Find each sentence with `grep -n`; if a sentence is worded differently from the table, mark the sentence that makes the same claim.

- [ ] **Step 6: The report for issue #80**

Create `.superpowers/sdd/2026-09-28-phase7-slice-7-10-follow-ups-and-close/issue-80-report.md`. Confirm it is ignored with `git check-ignore -v` on that path; if it is not, write it to the session scratch directory the controller names instead. Never commit it. It contains, under these headings:

1. **What to pin.** The branch and its head commit; the merge commit is added by the controller when the PR merges.
2. **Each new field.** A table: field, type, every record it lands on, serde attributes. Copy it from Step 1 item 6.
3. **The bound at each site.** The reason's `code` at 128 bytes on construction and on decode; `stage` at 256 by construction; every existing `code` exactly as it was bounded before — state each one as the code has it: `EffectFailed.code` and `last_error_code` through `bounded_detail` (512), the dispatcher's delivered codes through `bounded_failure_code` (128) on the four routes that apply it, the delegation cell's `code` unbounded at the run, the handoff cell's through `bounded_detail`.
4. **The reason codes.** The three built-in codes and the chain's own, with their constant names.
5. **Ask 3.** On commit. What `run-resume` does on the errored call, as Task 8's test measured it: quote the two counts `clean_resolution()` returned and say on which call the errored arm closed its resume segment.
6. **The tests, with each pre-fix failure.** One row per proof of the brief's section 6, the test that is that proof, and the failure output its task kept.
7. **Codes that changed meaning.** None. Two codes cover one more case each; say which.
8. **Section 8 of the brief.** The comment at `run.rs:6736-6739` was rewritten with the rule, since Ask 3 landed.
9. **Where the code differed from the brief.** The eleven differences, each in one line: the function at the drop site is `apply_effect_outcome`; `dispatch.rs:647` is the enum header; the run bounds a code at 512; `collaborator_code` bounded at 512 and now bounds at 128; several routes deliver a code with no 128-byte bound; the delegation cell's code is unbounded; a handoff block never reaches `EffectFailed`, so the handoff cell carries the reason; an ingress block passes through `RakkaAgentA2AError::Refused`, which carries it too; `refuse_guardrail_disposition` has five call sites; the resolve segment covers `ResolveIndeterminateEffect` as well; and the host's line numbers moved.
10. **What breaks at the consumer's bump.** The host sites by path: `crates/rakka-host-gateway/tests/trace_context_etcd.rs:379`, `guardrails_end_to_end.rs:677` and `:826`, `crates/rakka-host-runtime/tests/entities_etcd.rs:499`, and the two struct literals at `crates/rakka-host-runtime/src/fleet.rs:1768` and `:1772`. Re-read each line in the host repository before writing it down; the host moves.
11. **Validation.** The results of Step 7.

- [ ] **Step 7: Validate the final tree**

Run, each in the foreground with a 600000 ms timeout, per crate because the one-shot workspace test run is killed on this machine:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test -p rakka-agent-mcp --all-features
cargo test -p rakka-agent --all-features
cargo test -p rakka-a2a --all-features
cargo test -p rakka-agent-workflow --all-features
cargo test -p rakka
cargo check -p rakka-a2a --no-default-features --tests
cargo check -p rakka-agent-mcp --no-default-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features
scripts/package-check.sh
```

Expected: every command exits 0. `cargo test -p rakka-agent --all-features` alone takes thirty to fifty minutes on a cold target; if the harness cannot hold one foreground command that long, run it file by file with `--test <name>` over `ls crates/rakka-agent/tests/*.rs`.

Then the script itself, to a file:

```bash
scripts/validate.sh > /tmp/validate-7-10.log 2>&1; echo "exit=$?" >> /tmp/validate-7-10.log
grep -n '^exit=' /tmp/validate-7-10.log
grep -c 'test result: ok' /tmp/validate-7-10.log
grep -E 'test result: FAILED' /tmp/validate-7-10.log
```

Use the session's scratch directory in place of `/tmp`. Expected: `exit=0`, no `FAILED` line. If the workspace phase is killed, say so in the report with the per-crate results above; do not report the script as passed.

The gated suites, when their service is up (`docker ps` shows the pgvector container on port 5433):

```bash
RAKKA_POSTGRES_TEST_DSN=postgres://postgres:postgres@localhost:5433/postgres cargo test -p rakka-agent-postgres
```

Expected: PASS. If no Postgres is running, record "not run: no database" in the report; never start or stop a container.

- [ ] **Step 8: Commit the records, the plan, and the spec**

```bash
git add docs/rakka-compatibility.md CHANGELOG.md \
        docs/rakka-agent-security-validation-matrix.md \
        docs/rakka-agent-telemetry-validation-matrix.md \
        docs/rakka-agents.md docs/rakka-api-boundary-inventory.md \
        docs/plans/rakka-agent/spec.md
git commit -m "Record what slice 7.10 shipped: the artifact sink, the bounded and credential-refusing sync, the failure's deciding identity, and the resolve segment's commit rule

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"

git add docs/superpowers/specs/2026-09-19-phase7-agent-surface-parity-design.md \
        docs/superpowers/plans/2026-09-28-phase7-slice-7-10-follow-ups-and-close.md
git commit -m "Close Phase 7 after slice 7.7: the cut, the deferred slices with their seams, the two open decisions, and the slice 7.10 plan

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
git status --porcelain
```

Expected: the working tree is clean.

- [ ] **Step 9: Stop**

Do not push and do not open a pull request. Report the branch, its head, the commit count above `ebc7147`, the validation results, and the report's path. The owner decides the integration. Issues #80 and #70 stay open until the owner closes them: #80 is answered by this branch, and #70's third item, wait segments for timer and child waits, is not.

---

## Self-review record

- **Spec coverage.** Follow-up items 1, 2, and 5 are Tasks 1 to 3. Item 3, issue #80, is Tasks 4 to 8: Ask 1 in Tasks 4, 5, and 6, Ask 2 in Task 7, Ask 3 in Task 8, the companion ask for exported reason codes in Task 4, the brief's section 10 report in Task 12. Item 6 is Tasks 9 and 10. Item 7's deadline proof is Task 11. Items 4 and the enforcement half of 7 are not built; "Not built in this slice" says why, and the spec records both as open decisions. The phase close is Task 12 and the planner's spec amendment.
- **The brief's proofs.** Proof 1 is `a_built_in_stage_is_named_on_the_runs_records_and_its_message_is_not` and the `ToolResponse` assertions added to `a_blocked_tool_response_never_reaches_the_run` (Task 6). Proof 2 is `a_blocked_send_records_the_deciding_stage_on_its_cell` over a scripted executor in `rakka-agent`, and the finding-level assertions over the real executor in `rakka-a2a` (Tasks 5 and 6): `rakka-agent` has no dev-dependency on `rakka-a2a`, so no single test holds both halves. Proof 3 is the two `an_exhausted_…` tests (Task 7). Proof 4 is `a_resolution_whose_settle_pass_loses_a_write_still_leaves_its_trace_once` (Task 8). Proof 5 is `a_replayed_resolution_closes_nothing`, `a_refused_resolution_closes_nothing`, and `an_outcome_written_before_the_reason_decodes_with_none`.
- **Type consistency.** `AgentFailureReason::new` answers `Option<Self>` and `guardrail` answers `Self` everywhere they are called. `with_reason` takes `Option<AgentFailureReason>` on both `AgentRunEffectOutcome` and `AgentAuthorityRefusal`. `settle_failed_because(code, reason, now)` has one argument order on both cells. `McpArtifacts::sink` and `impl From<McpArtifactStore>` are the only two ways Task 1's tests build the write path. `sync_over_session` has five parameters after Task 2 and seven after Task 3, and Task 3 shows the seven.
- **Review Focus.** Each of the five has its test: `artifact_sink.rs` (1 and 2), `sync_credential_echo.rs` (3), `failure_reason_records.rs` (4), `checkpoint_resolve_segment.rs` and the escalation test (5).
- **Known soft spots, stated.** Two steps tell the implementer what to do if a fixture behaves differently from what the planner read: the completed source run's terminal reason in Task 6's handoff proof, and whether the first dispatch pass of Task 11 finds the model ticket claimable. Task 8 Step 4 rests on one fact the planner did not trace to its end — at which write the run's phase stops waiting — and its test is written to hold either way, by measuring a clean call and holding the errored one to it. None changes what a test asserts.
