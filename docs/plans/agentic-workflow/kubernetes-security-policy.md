# Rakka Agent Workflow Kubernetes Security Policy

Status: Slice 6.6 reference artifact.

This guide defines the security and policy envelope for the Rakka agent
workflow Kubernetes topology. It is deliberately scoped to deployment
boundaries: public API ingress, human approval submissions, internal remoting,
database access, Collector access, secrets, service accounts, pod security, and
least-privilege tool execution.

Rakka should not become the whole authentication, authorization, ingress, or
secret-management platform. Applications and operators still own identity
provider integration, tenant policy, external secret systems, TLS/mTLS
material, and production network controls.

## Files

- `kubernetes-security-policy.yaml`: reference NetworkPolicy envelope for
  `rakka-system`.
- `kubernetes-reference-topology.yaml`: app Deployment defaults that keep the
  Rakka service account token unmounted and process/tool execution locked down
  by default.
- `kubernetes-otel-collector-topology.yaml`: Collector pod security defaults
  and the RBAC exception required for Kubernetes metadata enrichment.

## Boundary Model

| Boundary | Allowed | Not Allowed By Default |
| --- | --- | --- |
| Public workflow API | Authenticated ingress to HTTP `8080` and gRPC `50051` through a trusted ingress controller. | Direct public access to internal remoting, `/drain`, raw OTLP, PostgreSQL, or object storage. |
| Human checkpoints | Authenticated, authorized, idempotent approval submissions that enter through the durable inbox. | Anonymous approval callbacks, expired decisions, replayed decisions without deduplication, or approvals that bypass audit. |
| Internal remoting | Rakka runtime pods talking to other Rakka runtime pods on TCP `2552`. | Public ingress, Collector pods, database pods, or arbitrary pods initiating remoting. |
| PostgreSQL | Rakka runtime pods and migration/backfill jobs using scoped credentials. | Public database access, DSNs in ConfigMaps, or broad credentials shared with tool processes. |
| Collector | Rakka runtime pods and node agents sending OTLP to the gateway Collector. | Public OTLP intake, backend tokens in ConfigMaps, or unredacted prompt/tool payloads in telemetry. |
| Tool/process execution | Explicit allowlists, no inherited environment, bounded resources, scoped credentials, and declared egress. | Arbitrary shell execution, inherited secrets, broad filesystem access, or unrestricted network egress. |
| Operational endpoints | Kubelet/internal probes and operator-only access to readiness, liveness, metrics, snapshots, and drain. | Public exposure of `/drain`, `/metrics`, or `/snapshots` without an admin boundary. |

## Public API And Human Approval

The public API boundary starts at the ingress controller or API gateway, not at
Rakka internal remoting. Public workflow commands and human approval
submissions must provide:

- authenticated principal identity;
- authorization scope for tenant, workflow type, workflow id, checkpoint id,
  and decision action;
- idempotency key or message id;
- request expiry or signed callback expiry;
- input validation before durable inbox acceptance;
- audit evidence linking principal, checkpoint, decision, correlation id, and
  trace context.

Human decisions should be accepted as durable inbox commands. This preserves
pause/resume recovery and prevents approval handlers from mutating live actor
state directly.

Ingress rules are expected to route only the application API surface. Path
level restrictions for `/drain`, `/metrics`, and `/snapshots` must be enforced
by the application, ingress controller, service mesh, or an admin-only route;
native Kubernetes NetworkPolicy cannot inspect HTTP paths.

## Internal Remoting

`Service/rakka-agent-internal` remains headless and cluster-internal. It
exposes only TCP `2552` for Rakka node-to-node remoting. NetworkPolicy
`rakka-agent-remoting` allows that port only between pods labeled:

```text
app.kubernetes.io/name=rakka-agent-workflow
app.kubernetes.io/component=agent-runtime
```

The public service must never expose the `remoting` port. The public ingress
controller must never route to `rakka-agent-internal`.

## NetworkPolicy Envelope

The reference policy manifest starts with default-deny ingress and egress for
all pods in `rakka-system`, then opens only named lanes:

- DNS egress to CoreDNS on TCP/UDP `53`.
- Public API ingress from namespaces labeled `rakka.rs/ingress=public`.
- Runtime remoting between Rakka runtime pods on TCP `2552`.
- Runtime egress to PostgreSQL on TCP `5432`.
- Runtime egress to object storage on TCP `9000` or managed HTTPS `443`.
- Runtime egress to the gateway Collector on TCP `4317` and `4318`.
- Collector gateway ingress from Rakka runtime pods and Collector agents.
- Collector egress to backend OTLP and Kubernetes API endpoints.
- Collector agent egress to kubelet stats on TCP `10250`.

Native Kubernetes NetworkPolicy cannot select an `ExternalName` Service or a
DNS hostname. For local Docker Desktop PostgreSQL, the policy allows TCP `5432`
to `0.0.0.0/0` as a local-development compromise. Production deployments
should replace that with managed database CIDRs, private endpoint ranges,
service mesh policy, cloud security groups, or CNI-specific FQDN policy.

Docker Desktop may validate NetworkPolicy objects without enforcing them. A
production cluster needs a CNI that implements NetworkPolicy.

## Service Accounts And RBAC

`ServiceAccount/rakka-agent-workflow` should not need Kubernetes API access for
the reference topology. The manifest sets:

```yaml
automountServiceAccountToken: false
```

on both the ServiceAccount and the Pod spec. If a future runtime feature needs
API access, add a scoped Role/RoleBinding for that feature instead of mounting
the default token broadly.

`ServiceAccount/rakka-otel-collector` is the intentional exception. The
Collector needs Kubernetes API and kubelet access for `k8sattributes` and
`kubeletstats`. Its ClusterRole is limited to pods, namespaces, nodes,
`nodes/stats`, deployments, and replicasets.

## Secrets

Local Docker Desktop examples keep simple credentials in raw Secrets so the
topology is runnable. Production deployments should:

- source database, object-store, model-provider, tool, and OTLP backend
  credentials from an external secret manager or sealed secret workflow;
- avoid DSNs, tokens, and private keys in ConfigMaps;
- avoid passing broad credentials to child process/tool adapters;
- rotate credentials without rebuilding images;
- enable Kubernetes encryption at rest for Secrets;
- prefer mounted files or workload identity for high-value credentials when
  the application supports them.

The reference topology passes PostgreSQL and artifact credentials through
`secretKeyRef`. That is acceptable for local and near-production validation,
but not a full production secret-management story.

## Pod Security

Rakka runtime pods and Collector pods should run with restricted container
defaults where compatible:

- `runAsNonRoot: true`;
- `allowPrivilegeEscalation: false`;
- `capabilities.drop: ["ALL"]`;
- `readOnlyRootFilesystem: true`;
- `seccompProfile.type: RuntimeDefault`;
- bounded CPU and memory requests/limits.

The Collector DaemonSet currently exposes OTLP host ports for optional
node-local export. Treat host ports as a local topology convenience or a
controlled production exception. If enforcing the Kubernetes Restricted Pod
Security Standard namespace-wide, disable those host ports or place the
Collector agent in a namespace/policy profile that explicitly permits them.

## Tool And Process Policy

Agent workflows can execute tools, child processes, model calls, and external
APIs. That makes tool policy part of the deployment boundary.

Required defaults:

- `RAKKA_PROCESS_ALLOWLIST_REQUIRED=true`;
- `RAKKA_PROCESS_INHERIT_ENVIRONMENT=false`;
- workflow-owned allowlists for executable paths, arguments, working
  directories, and target classes;
- no ambient database, object-store, model-provider, or OTLP credentials in
  child process environments;
- per-tool timeout, retry, output-size, and artifact policies;
- explicit egress policy for each external tool/provider class;
- audit records for tool request, approval, execution, output artifact, and
  policy overrides.

The reference NetworkPolicy intentionally does not open general model or tool
egress. Application deployments should add narrow egress policies for approved
providers.

## Telemetry And Audit

Telemetry is security-sensitive for agent workflows. The gateway Collector
redacts prompt, completion, tool, artifact, authorization, and high-cardinality
identifier attributes before backend export. Application code must still avoid
emitting secrets, raw credentials, or large payloads into traces, metrics, or
logs.

Durable audit remains separate from logs. Human decisions, policy overrides,
tool execution, artifact handoff, and admin operations need audit records even
when log sampling or retention changes.

## Local Validation

Validate the manifests:

```sh
kubectl apply --dry-run=client -f docs/plans/agentic-workflow/kubernetes-reference-topology.yaml
kubectl apply --dry-run=client -f docs/plans/agentic-workflow/kubernetes-otel-collector-topology.yaml
kubectl apply --dry-run=client -f docs/plans/agentic-workflow/kubernetes-security-policy.yaml
```

Run the contract tests:

```sh
cargo test -p rakka-k8s --test agent_workflow_security_policy
cargo test -p rakka-k8s --test agent_workflow_topology
cargo test -p rakka-k8s --test agent_workflow_otel_collector_topology
```

## Policy Checklist

- Public ingress reaches only the public API service.
- Internal remoting is reachable only between Rakka runtime pods.
- PostgreSQL is not publicly exposed and production DSNs are not in ConfigMaps.
- Collector OTLP intake is not public.
- Backend OTLP credentials are stored in Secrets or external secret systems.
- Rakka runtime service account token is not mounted by default.
- Collector RBAC is limited to metadata and kubelet stats needs.
- Runtime and Collector containers run non-root and disallow privilege
  escalation.
- Tool/process execution uses allowlists and does not inherit ambient
  environment secrets.
- Model/tool/provider egress is opened explicitly per deployment.
- `/drain`, `/metrics`, and `/snapshots` stay behind admin or internal
  boundaries.
- Human approval submissions are authenticated, authorized, idempotent,
  expiry-bound, durable, and audited.
- NetworkPolicy enforcement is verified with the production CNI, not only
  client-side manifest validation.

## References

- Kubernetes NetworkPolicy:
  <https://kubernetes.io/docs/concepts/services-networking/network-policies/>.
- Kubernetes Pod Security Standards:
  <https://kubernetes.io/docs/concepts/security/pod-security-standards/>.
- Kubernetes Secrets:
  <https://kubernetes.io/docs/concepts/configuration/secret/>.
- Kubernetes Service Accounts:
  <https://kubernetes.io/docs/concepts/security/service-accounts/>.
