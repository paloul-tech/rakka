# Agent-domain OpenTelemetry Collector topology

Reference manifests for exporting Rakka Agent traces, metrics, and logs over
OTLP, with the allowlisting and tail sampling
[specification 17.14](spec.md#1714-content-capture-and-redaction) and
[17.16](spec.md#1716-sampling) require, and the Collector's own health
telemetry [17.17](spec.md#1717-otlp-and-collector-boundary) asks for.

They are a **reference**, not an evergreen security or compatibility
guarantee — 17.17 says so explicitly, and the revalidation procedure below is
what keeps them honest across upgrades.

## Files

| File | What it is |
| --- | --- |
| `kubernetes-agent-otel-collector-topology.yaml` | Twelve documents: namespace, RBAC, two Collector configurations, three services, a DaemonSet, a Deployment, a PodDisruptionBudget. |
| this document | The runtime contract, the sizing arithmetic, and the revalidation procedure. |

## Why this is not the workflow domain's topology

The workflow domain ships its own at
[`../agentic-workflow/kubernetes-otel-collector-topology.yaml`](../agentic-workflow/kubernetes-otel-collector-topology.yaml),
and it is a different artifact for a different vocabulary:

- its `transform/redact` is a **denylist** of six content keys
  (`prompt_text`, `completion_text`, `tool_arguments`, `tool_output`,
  `artifact_uri`, `authorization`) — none of which the GenAI vocabulary uses,
  so applied to agent telemetry it deletes nothing and permits everything;
- its metric rules drop workflow identifiers (`workflow_id`, `run_id`,
  `command_id`, `effect_id`, `correlation_id`), which are not the agent
  domain's metric labels; and
- it runs a head `probabilistic_sampler` at 100%, which retains nothing
  selectively and has no notion of the eight retention classes 17.16 names.

This one is an **allowlist** keyed on `gen_ai.*` and `rakka.agent.*`, and a
tail sampler. 17.14 asks for an allowlist, and the reason is stated there: the
application knows exactly which keys it writes, while a denylist is a guess
about what content will be called next time.

## Runtime shape

Two tiers, because tail sampling requires two.

**Agent (DaemonSet, node-local).** Receives OTLP on the node's `hostPort`
4317/4318, enriches with `k8sattributes`, batches, and forwards. Traces and
logs leave through a `loadbalancing` exporter with `routing_key: traceID`,
resolving gateway **pod** addresses from the headless service; metrics leave
through a plain `otlp/gateway` exporter.

**Gateway (Deployment, 2 replicas).** Runs `transform/allowlist`, then
`tail_sampling`, then `batch`, and exports to the operator's backend with a
bounded queue and retry.

Neither tier runs `kubeletstats` or `hostmetrics`, which the workflow domain's
topology does. That is deliberate rather than an omission: this topology
carries **agent** telemetry, and node and container infrastructure metrics
belong to whatever already collects them for the cluster — a second collector
for them duplicates every series. The `nodes/stats` RBAC grant those receivers
need is therefore absent from the ClusterRole too.

### The two configuration decisions that are not cosmetic

- **The routed service is headless.** 17.16 requires every span of one trace to
  reach the same tail-sampling instance. The `loadbalancing` exporter's `k8s`
  resolver reads pod addresses from a service; a `ClusterIP` service returns
  one virtual address, kube-proxy spreads the spans of a single trace across
  replicas, and each replica then samples a partial trace. Nothing fails, and
  the sampling silently becomes wrong. `rakka-agent-otel-gateway-headless` is
  `clusterIP: None` for this reason, and the contract test asserts it.
- **Metrics do not use the trace router.** The `loadbalancing` exporter refuses
  `routing_key: traceID` for metrics: the pinned distribution rejects the
  configuration at startup. This was found by running the distribution's own
  `validate`, not by review.

## Sampling, sized together

17.16 requires decision wait, trace buffers, memory limiter, queues, and
exporter retry to be sized **together**. The shipped numbers and their
arithmetic:

| Setting | Value | Why |
| --- | --- | --- |
| `expected_new_traces_per_sec` | 200 | The rate the gateway pair is sized for. |
| `decision_wait` | 30s | 200/s × 30s ≈ 6,000 traces undecided at any moment. |
| `num_traces` | 50,000 | The LRU of traces held: an order of magnitude above the undecided set, so late spans of a decided trace still land in it. |
| gateway memory limit | 4Gi | One full agent run produced 38 spans in the export walk; 50,000 traces is ~1.9M spans worst case. |
| `memory_limiter` | 80% / 20% spike | Bounds the above rather than trusting the estimate. |
| backend `sending_queue` | 4,096 | Sized to drain a decision burst, not a steady rate: tail sampling emits in bursts as decisions land. |
| backend `retry_on_failure` | 1s → 10s, 60s cap | Bounded, so a backend outage sheds load rather than growing the queue without limit. |

### The eight retention classes

Each selects on an attribute a mapping function actually writes — checked by
`crates/rakka-agent/tests/collector_allowlist.rs`, because a policy keyed on an
attribute nothing emits matches nothing in production while passing every
string assertion about this file.

| 17.16 class | Policy | Selector |
| --- | --- | --- |
| `ERROR` status | `error-status` | span status |
| Stable failure code | `stable-failure-code` | `rakka.error.code` present |
| Security denial, override, revocation | `security-denial-or-revocation` | `error.type = rakka.agent.authority` |
| Indeterminate effect or reconciliation | `indeterminate-effect-or-reconciliation` | `rakka.agent.effect.status` |
| Checkpoint escalation or timeout | `checkpoint-escalation-or-timeout` | `rakka.agent.checkpoint.kind` |
| Recovery failure or stale-owner conflict | `recovery-failure-or-stale-owner` | `error.type` |
| Configured high latency | `configured-high-latency` | span duration |
| Excessive retry | `excessive-retry` | `rakka.agent.effect.attempt` |
| New version under investigation | `version-under-investigation` | `rakka.agent.settings_revision` |
| Routine successful turns | `routine-successful-turns` | 5% probabilistic |

**Every policy is a string, status, or latency policy — never a
`numeric_attribute` one.** `AgentAttributes` is a `BTreeMap<String, String>`,
so every agent-domain span attribute reaches the Collector as a string; a
numeric policy over `rakka.agent.effect.attempt` would match nothing while
looking correct. `excessive-retry` uses a regex for attempt three and above.

### The limit tail sampling has here, stated rather than assumed

A tail-sampling decision is made `decision_wait` after a trace's first span. An
agent trace can outlive that by a wide margin: a run parked on a human approval
checkpoint resumes minutes or hours later, and the segments it closes then
belong to the same trace. Those spans arrive after the decision and are
governed by it.

This is inherent to tail sampling, not a defect in this configuration, and it
is the reason the retention classes select on attributes that appear **early**
in a trace wherever possible. It is recorded as an inferred claim in
[`../../rakka-agent-telemetry-validation-matrix.md`](../../rakka-agent-telemetry-validation-matrix.md).

## Redaction, as an allowlist

`transform/allowlist` runs `keep_keys` over three vocabularies:

| Context | Keys | Source of truth |
| --- | --- | --- |
| span | 30 | `AGENT_SPAN_ATTRIBUTE_KEYS` |
| log | 44 | that set unioned with `AGENT_LOG_ATTRIBUTE_KEYS` — a log legitimately carries the durable correlation identities 17.13 asks for and 17.12 forbids on a metric |
| datapoint | 35 | `AGENT_METRIC_FIELDS` ∪ the substrate's bounded metric vocabulary |

The application minimises **before** export — 17.14 puts the Collector second,
as defence in depth — so these lists are the second of two layers, not the
only one.

## Local validation

```sh
# The Kubernetes objects.
kubectl apply --dry-run=client -f docs/plans/rakka-agent/kubernetes-agent-otel-collector-topology.yaml

# The contract tests (always run, no cluster or runtime needed).
cargo test -p rakka-k8s --test agent_otel_collector_topology
cargo test -p rakka-agent --all-features --test collector_allowlist

# The gated arms.
RAKKA_AGENT_OTEL_VALIDATE_MANIFESTS=1 \
  cargo test -p rakka-k8s --test agent_otel_collector_topology -- --nocapture
RAKKA_AGENT_OTEL_VALIDATE_COLLECTOR_CONFIG=1 \
  cargo test -p rakka-k8s --test agent_otel_collector_topology -- --nocapture
```

The second gated arm runs the pinned distribution's own `validate` against both
ConfigMap payloads. It is the only check that knows what a Collector
configuration means: it found that `container.name` is no longer a
`k8sattributes` metadata field at this distribution, and that the
`loadbalancing` exporter refuses `routing_key: traceID` for metrics. Neither
was visible to `kubectl` or to any string assertion.

## Pinned versions, and how to revalidate them

| Component | Pin | Where |
| --- | --- | --- |
| Collector distribution | `otel/opentelemetry-collector-contrib:0.159.0` | this topology, both tiers |
| GenAI semantic conventions | `1.36.0` | `AGENT_GENAI_CONVENTION_REVISION` |
| OpenTelemetry Rust SDK | `0.29` | `[workspace.dependencies]` |

The workflow domain's topology remains pinned at
`otel/opentelemetry-collector-contrib:0.107.0`. That is deliberate: it is a
different domain's shipped artifact with its own gate and its own plan, and
moving it is not this slice's change. The spread is recorded in the telemetry
validation matrix rather than left implicit.

**To revalidate on an upgrade**, in this order:

1. Change the image tag in both tiers of the YAML and in `COLLECTOR_IMAGE` in
   `crates/rakka-k8s/tests/agent_otel_collector_topology.rs`.
2. Run the gated config arm. It is the step that catches a renamed field, a
   removed component, or a routing key a component no longer accepts.
3. Run the gated `kubectl` arm for the object shapes.
4. Re-read [17.20](spec.md#1720-semantic-convention-compatibility)'s upgrade
   list against the diff: span names and kinds, metric names, units and
   buckets, required attributes, operation values, content-capture guidance,
   and **Collector rules** — the last of which is this file.
5. Update the pinned-version table above and the matrix entry.

A convention-revision upgrade never requires a durable agent-state migration
(17.20). A distribution upgrade never requires either.

## Open items

- Backend credentials and TLS are the deployment's: the shipped exporter uses
  `tls.insecure: true` against an in-cluster backend. mTLS, authentication, and
  network isolation are named as delegated in the telemetry validation matrix.
- NetworkPolicies for the agent-domain lanes, matching the workflow domain's
  `kubernetes-security-policy.yaml`.
- A `ServiceMonitor`/`PodMonitor` for the `service.telemetry` endpoint both
  tiers now expose on port 8888.
