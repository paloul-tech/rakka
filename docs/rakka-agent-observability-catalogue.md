# Agent Domain Observability Catalogue

Status: implemented (slice 6.3a).

[Specification 17.12](plans/rakka-agent/spec.md) requires agent metric labels
to be bounded **and documented**. Before this slice nothing documented them.
The only agent-domain metric table in the repository —
[`plans/rakka-agent/technical-guidance.md`](plans/rakka-agent/technical-guidance.md)'s
— was aspirational: fifteen of its eighteen rows named metrics that did not
exist, and the three that matched by name did not match by label. Meanwhile
the in-crate list that was supposed to keep the label vocabulary honest had
gone stale on four keys already in use.

So this document is not the source of truth. `AGENT_DOMAIN_METRIC_INSTRUMENTS`
in `crates/rakka-agent/src/observability.rs` is, and
`crates/rakka-agent/tests/metric_catalogue.rs` scans the crate's own sources to
prove the two agree in both directions: a metric the code declares but the
catalogue omits fails the suite, and a catalogue entry naming a metric no
constant binds fails it too. What follows is that data, rendered.

The companion documents are
[`rakka-agent-security-validation-matrix.md`](rakka-agent-security-validation-matrix.md)
and [`rakka-agent-fault-injection-matrix.md`](rakka-agent-fault-injection-matrix.md).
The telemetry validation matrix that records which telemetry claims are
enforced, delegated, or inferred belongs to slice 6.3b and will cite this page.

## Metrics

Bucket column: *latency* is `AGENT_LATENCY_BUCKETS_MS` (1 ms to 15 minutes),
*count* is `AGENT_COUNT_BUCKETS`. One ladder rather than a bespoke set per
instrument, because the agent domain's durations span the same range for the
same reasons — a bounded in-process segment at the bottom, a durable round trip
in the middle, a human or an external system at the top — and a shared ladder
is what lets a dashboard compare a model call against the wait it caused
without re-bucketing.

| Metric | Instrument | Unit | Bounded labels | Buckets | What it measures |
| --- | --- | --- | --- | --- | --- |
| `rakka.agent.decisions` | Counter | `{decision}` | `decision_kind`, `decision_source` | — | Agent-loop decisions durably retained by a decision sink. |
| `rakka.agent.run.transitions` | Counter | `{transition}` | `phase` | — | Committed loop transitions, by the phase they advanced from. |
| `rakka.agent.effect.outcomes` | Counter | `{effect}` | `effect_kind`, `safety_class`, `outcome` | — | Resolved effect generations, including indeterminate outcomes. |
| `rakka.agent.effect.outstanding.duration` | Histogram | `ms` | `effect_kind`, `outcome` | latency | Durable acceptance to durable result, as the run observed it. |
| `rakka.agent.turn.duration` | Histogram | `ms` | `outcome` | latency | One bounded active turn, across its durable model round trip. |
| `rakka.agent.model.tokens` | Histogram | `{token}` | `direction` | count | Provider-reported token usage per recorded turn, by direction. |
| `rakka.agent.recovery.events` | Counter | `{recovery}` | `outcome` | — | Run recoveries after restart, passivation, or shard movement. |
| `rakka.agent.recovery.duration` | Histogram | `ms` | `outcome` | latency | One run recovery, measured in the process that performed it. |
| `rakka.agent.decision.drops` | Gauge | `{decision}` | — | — | Decision events a run's bounded outbox ring dropped. |
| `rakka.agent.telemetry.flush.failures` | Counter | `{failure}` | `signal` | — | Telemetry flush attempts a sink refused, by signal. |
| `rakka.agent.memory.retrievals` | Counter | `{retrieval}` | `backend`, `outcome` | — | Private-memory retrievals run by context-snapshot assembly. |
| `rakka.agent.memory.ingress.outcomes` | Counter | `{record}` | `outcome` | — | Memory-ingress guardrail outcomes on retrieved records. |
| `rakka.agent.wake.dispositions` | Counter | `{wake}` | `outcome`, `trigger` | — | Wake dispositions the continuous controller durably recorded. |
| `rakka.agent.wakes` | Counter | `{wake}` | `outcome`, `trigger` | — | Wake delivery attempts made by the shared scanner. |
| `rakka.agent.epochs` | Counter | `{epoch}` | `outcome` | — | Continuous-goal epoch admissions and results. |
| `rakka.agent.goal.lifecycle` | Counter | `{transition}` | `transition` | — | Continuous-goal lifecycle transitions at the admission gate. |
| `rakka.agent.goal.status` | Counter | `{transition}` | `transition` | — | Goal-contract status transitions, by the status arrived at. |
| `rakka.agent.goal.stagnation` | Counter | `{trip}` | `trigger` | — | Stagnation-threshold trips the wake controller detected. |
| `rakka.agent.delegation.results` | Counter | `{result}` | `outcome` | — | Delegated children's terminal results decided at the parent's door. |
| `rakka.agent.handoff.results` | Counter | `{result}` | `outcome` | — | Handoff resolutions decided at the source run's door. |
| `rakka.agent.fan_in.resolutions` | Counter | `{group}` | `outcome` | — | Fan-in groups resolved, by bounded resolution code. |
| `rakka.agent.workflow.results` | Counter | `{result}` | `outcome` | — | Child workflow runs' terminal results accepted at the parent's door. |
| `rakka.agent.team.operations` | Counter | `{operation}` | `operation`, `outcome` | — | Team board and lifecycle operations committed at the team entity. |
| `rakka.agent.moderation.turns` | Counter | `{operation}` | `operation`, `outcome` | — | Moderated-conversation turn and lifecycle operations. |
| `rakka.agent.human.results` | Counter | `{result}` | `outcome` | — | Authenticated human-result submissions decided at the task entity. |
| `rakka.agent.dependency.outcomes` | Counter | `{edge}` | `outcome` | — | Dependency outcomes durably applied at the dependent task. |
| `rakka.agent.exchange.unsettleable` | Counter | `{refusal}` | `operation`, `error_code` | — | Exchange replies a settle pass could not settle, per pass. |

Every label *value* comes from a closed `as_label()` vocabulary. Raw
`TenantId`, `AgentId`, `AgentGoalId`, `AgentTaskId`, `AgentRunId`,
coordination, effect, checkpoint, memory, claim, and workflow-run identifiers,
provider response ids, prompts, tool arguments and results, URLs, user values,
and full error messages never label a metric — enforced by
`validate_agent_domain_metric_attributes`, which reuses the substrate's
forbidden-key guard and adds a 96-byte single-line bound on every value.

### Where a duration's endpoints come from

Two rules, and the difference is not stylistic.

- **A duration spanning a durable boundary uses persisted timestamps.**
  `rakka.agent.effect.outstanding.duration` and `rakka.agent.turn.duration`
  measure across a passivation: the run is not resident while its effect is
  outstanding, so there is no live segment to time, and the endpoints are the
  effect's own `created_at` and the transition that resolved it. A run that
  passivated, recovered, or moved shard mid-effect therefore reports the same
  figure a resident one would. `record_agent_domain_duration` is the helper,
  and a backwards clock records nothing rather than a negative sample.
- **A duration inside one process measures itself.**
  `rakka.agent.recovery.duration` is a durable load happening now, with exactly
  one injected timestamp available, so it is measured by its segment's
  monotonic width (`AgentSegmentTimer`) and *anchored* at the injected
  timestamp. Deriving a width from a single injected value would report every
  live operation as instantaneous.

### Rows 17.12 asks for that the substrate already publishes

These are deliberately absent from `rakka.agent.*`. A second name for one
number is two catalogues that drift, which is the failure this slice exists to
correct.

| 17.12 clause | Provided by |
| --- | --- |
| Currently resident entities | `rakka.agent_workflow.run.active` |
| Durable inbox backlog | `rakka.agent_workflow.inbox.pending_commands` |
| Durable outbox backlog | `rakka.agent_workflow.outbox.due_effects` |
| Dispatcher in-flight and dispatch latency | `rakka.agent_workflow.dispatcher.latency_ms` |
| Timer lateness | `rakka.agent_workflow.timers.late_by_ms` |
| Shard ownership distribution | `rakka.agent_workflow.shard.owned`, `rakka.sharding.shards_owned` |
| Mailbox and stream pressure | `rakka.agent_workflow.runtime.mailbox_depth`, `rakka.agent_workflow.stream.pressure`, `rakka.actor.mailbox.depth`, `rakka.stream.pressure` |
| Human checkpoint wait latency and waiting runs | `rakka.agent_workflow.human.wait.latency_ms`, `rakka.agent_workflow.human.waiting_runs` |

### Owed, and named here so it is not assumed covered

17.12's remaining clauses have no instrument yet, and this is the list rather
than a silence: logically active and waiting goals and runs by bounded status
class; activation/passivation rate and cold-activation latency; durable
trigger and timer backlog with oldest age; wait duration and current age by
wait or checkpoint kind; delegation, workflow-tool, task-operation, wake
admission, epoch, autonomy admission, budget, team, and moderation *durations*
(their counters exist); memory operation and retrieval latency and returned
record count; context snapshot size and count; and the telemetry export queue
depth and Collector/exporter health, which belong to the deployment-owned
exporter that slice 6.3b wires.

The fleet-scoped gauges among these need enumeration Rakka does not index —
the same constraint slice 6.2 hit with `terminal_at` — so they will arrive as a
bounded, deployment-invoked sweep in the shape of `AgentMemoryRetentionSweep`,
not as something an entity can record about itself.

## Bounded operation segments

`AgentSegmentOperation` is Rakka's own stable vocabulary for the span rows of
[17.6](plans/rakka-agent/spec.md).
[17.20](plans/rakka-agent/spec.md) requires exactly this: the agent domain
keeps an internal vocabulary and the OpenTelemetry GenAI mapping lives behind
the `otel` feature. So the loop, the entities, and the dispatcher close these
unconditionally — a `--no-default-features` build measures the same operations
— and `crates/rakka-agent/src/otel.rs` is the only place they become
`invoke_agent`, `execute_tool`, and the rest.

A segment is *closed*, never open. The runtime never holds a span object across
a durable wait ([17.4](plans/rakka-agent/spec.md)), and the boundaries are
never persisted: stamping them into a durable record would make a telemetry
change a state migration, which [17.20](plans/rakka-agent/spec.md) forbids.
What *is* persisted is the trace context a segment carries, which is what lets
a resume link back to the operation that parked.

| Segment class | 17.6 row |
| --- | --- |
| `a2a-ingress` | A2A ingress |
| `wake-admit` | Continuous wake/epoch admission |
| `autonomy-admit` | Autonomy admission |
| `budget-reserve`, `budget-settle` | Budget operation |
| `invoke-agent` | Active turn/invocation |
| `decide` | General decision |
| `model-inference` | Model inference |
| `effect-schedule` | Effect schedule |
| `effect-dispatch` | Effect dispatch |
| `tool-authorize` | Tool dispatch grant |
| `execute-tool` | Tool execution |
| `delegate-to-peer` | Outbound A2A call |
| `validate-task-result` | Task result validation |
| `handoff` | Handoff |
| `team-operation` | Team operation |
| `moderation-turn` | Moderated turn |
| `workflow-invoke` | Workflow tool invocation |
| `goal-evaluate` | Goal evaluation |
| `memory-operation`, `retrieval` | Memory operation, retrieval |
| `checkpoint-open` | Checkpoint open |
| `run-resume`, `run-recover` | Run resume/recovery |

Two rows of 17.6 are absent because the operations do not exist: `embeddings
{model}` and `plan {agent.name}`. 17.6 asks for the planning span "only when
planning is reliably distinguishable", and it is not.

### Which classes have a production call site today

The vocabulary and the convention mapping are complete over the shipped
milestones; the emission is not, and this is the list rather than a silence.

| Closed by | Classes |
| --- | --- |
| The run entity | `decide`, `invoke-agent`, `effect-schedule`, `checkpoint-open`, `run-resume`, `run-recover` |
| The dispatcher | `tool-authorize`, `effect-dispatch`, `model-inference`, `execute-tool` |
| The A2A service | `a2a-ingress` |

The remaining classes — `wake-admit`, `autonomy-admit`, `budget-reserve`,
`budget-settle`, `validate-task-result`, `handoff`, `team-operation`,
`moderation-turn`, `workflow-invoke`, `goal-evaluate`, `delegate-to-peer`,
`memory-operation`, and `retrieval` — map to spans today but are closed by
nobody. Wiring them means threading the sink
through the task, team, and conversation entities, which is the same shape as
the run entity's and is owed rather than blocked. `delegate-to-peer`,
`workflow-invoke`, and `goal-evaluate` are additionally *covered in interval*
by `effect-dispatch`, whose `effect_kind` label distinguishes an A2A send from
a workflow start from a goal evaluation — what they lack is the convention's
own name and kind, not a measurement.

### What a retention policy can select on

[Specification 17.16](plans/rakka-agent/spec.md) asks a sampling policy to
retain eight classes of trace. A policy can only express a class if a span
carries something to match, so the mapping is recorded here rather than left
for the deployment slice to discover:

| 17.16 retention class | Selectable on |
| --- | --- |
| `ERROR` status or stable failure code | span status, `error.type`, `rakka.error.code` |
| Indeterminate effect or reconciliation | `rakka.agent.effect.status` = `indeterminate`, on an error-status dispatch span closed at the park |
| Security denial, policy override, revocation | the refusal code on a failed `rakka.agent.tool.authorize` span |
| Checkpoint escalation or timeout | `rakka.agent.checkpoint.kind` on `rakka.agent.checkpoint.open` |
| Recovery failure | error status on `rakka.agent.run.recover` |
| Configured high latency | the span's own duration |
| Excessive retry | `rakka.agent.effect.attempt` |
| Newly deployed version under investigation | `rakka.agent.settings_revision` |

Stale-owner conflict is the one 17.16 case with no dedicated attribute: it
surfaces today as a failed transition's stable error code rather than as a
class of its own.

### Two vocabularies, and why the log one is wider

`AGENT_SPAN_ATTRIBUTE_KEYS` is the closed set a span may carry.
`AGENT_LOG_ATTRIBUTE_KEYS` is a **superset**, adding the substrate's durable
correlation identities — `run_id`, `correlation_id`, `causation_id`,
`audit_event_id`, and the rest. That is not a weaker rule, it is a different
one: [17.13](plans/rakka-agent/spec.md) asks a structured log to carry exactly
the identities [17.12](plans/rakka-agent/spec.md) forbids on a *metric*, so
applying the span list to logs would strip the audit trail while claiming to
redact it. Content, credentials, prompts, completions, tool payloads, and
memory records are on neither list and reach neither surface.

A class with no call site is not a silent gap in the suite:
`tests/telemetry_segments.rs` asserts the classes above against a real run and
a real dispatch pipeline, and `tests/otel_span_mapping.rs` maps every class,
so a row added to the emission is already covered by the mapping proof.

### Why the agent's name is not in the span name

`invoke_agent` carries no name. 17.6 forbids a span name to embed an agent
identifier, and `AgentId` is an identifier however bounded it is — so the
identity rides `AgentSegmentIdentity` and reaches the export as
`gen_ai.agent.id`, an access-controlled attribute
([17.3](plans/rakka-agent/spec.md)), never a name. A deployment that configures
a bounded telemetry or template name can supply it; Rakka does not invent one
from the id it happens to hold.

## Repeatable commands

```sh
cargo test -p rakka-agent --test metric_catalogue
cargo test -p rakka-agent --test agent_metrics
cargo test -p rakka-agent --test telemetry_segments
cargo test -p rakka-core --test observability_exporters
```
