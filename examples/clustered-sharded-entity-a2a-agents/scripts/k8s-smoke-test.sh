#!/usr/bin/env sh
# Kubernetes smoke test for the clustered sharded-entity A2A example.
#
# Applies the demo stack, waits for it to become Ready, sends an A2A task,
# registers a push notification config, then force-deletes an agent pod and
# asserts the task and its push config still resolve — proving durable recovery
# and cross-node routing over the shared PostgreSQL store.
#
# The container image (`rakka-clustered-a2a-agents:0.1.0` by default) must
# already be built and loaded into the cluster; see doc/kubernetes-testing.md.
#
# Requires: kubectl (with a working context), curl, jq.
#
# Usage:
#   examples/clustered-sharded-entity-a2a-agents/scripts/k8s-smoke-test.sh
#
# Config (environment overrides):
#   SMOKE_NAMESPACE   (default rakka-a2a-agents)
#   SMOKE_STATEFULSET (default rakka-a2a-agent)
#   SMOKE_PUBLIC_SVC  (default rakka-a2a-public)
#   SMOKE_KILL_POD    (default <statefulset>-0)
#   SMOKE_TENANT      (default tenant-a)
#   SMOKE_LOCAL_PORT  (default 18080)
#   SMOKE_TIMEOUT     (kubectl rollout/wait timeout, default 180s)
#   SMOKE_READY_SECS  (HTTP readiness poll budget, default 150)
#   SMOKE_RETRIES     (task-read retry attempts, default 40)
#   SMOKE_APPLY       (apply manifests first, default 1)
#   SMOKE_CLEANUP     (delete the stack on success, default 0)
#   SMOKE_DRY_RUN     (print the plan and exit, default 0)

set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
EXAMPLE_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
K8S_DIR="$EXAMPLE_DIR/k8s"

NAMESPACE="${SMOKE_NAMESPACE:-rakka-a2a-agents}"
STATEFULSET="${SMOKE_STATEFULSET:-rakka-a2a-agent}"
PUBLIC_SVC="${SMOKE_PUBLIC_SVC:-rakka-a2a-public}"
KILL_POD="${SMOKE_KILL_POD:-${STATEFULSET}-0}"
TENANT="${SMOKE_TENANT:-tenant-a}"
LOCAL_PORT="${SMOKE_LOCAL_PORT:-18080}"
TIMEOUT="${SMOKE_TIMEOUT:-180s}"
READY_SECS="${SMOKE_READY_SECS:-150}"
RETRIES="${SMOKE_RETRIES:-40}"
RETRY_SLEEP="${SMOKE_RETRY_SLEEP:-3}"
APPLY="${SMOKE_APPLY:-1}"
CLEANUP="${SMOKE_CLEANUP:-0}"
DRY_RUN="${SMOKE_DRY_RUN:-0}"

BASE="http://127.0.0.1:${LOCAL_PORT}"
PF_PID=""
RESP_FILE=""

log()  { printf '==> %s\n' "$*"; }
info() { printf '    %s\n' "$*"; }
fail() { printf 'SMOKE FAIL: %s\n' "$*" >&2; exit 1; }
now()  { date +%s; }

need() {
  command -v "$1" >/dev/null 2>&1 || fail "required command not found: $1"
}

kc() { kubectl -n "$NAMESPACE" "$@"; }

cleanup() {
  stop_forward
  [ -n "$RESP_FILE" ] && rm -f "$RESP_FILE" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

stop_forward() {
  if [ -n "$PF_PID" ]; then
    kill "$PF_PID" >/dev/null 2>&1 || true
    wait "$PF_PID" 2>/dev/null || true
    PF_PID=""
  fi
}

start_forward() {
  stop_forward
  kubectl -n "$NAMESPACE" port-forward "svc/${PUBLIC_SVC}" "${LOCAL_PORT}:80" \
    >/dev/null 2>&1 &
  PF_PID=$!
}

# Polls GET /readyz through the port-forward until it answers 200.
wait_ready() {
  deadline=$(( $(now) + READY_SECS ))
  while [ "$(now)" -lt "$deadline" ]; do
    code=$(curl -sS -o /dev/null -w '%{http_code}' "${BASE}/readyz" 2>/dev/null || echo 000)
    [ "$code" = "200" ] && return 0
    sleep 2
  done
  return 1
}

# read_task <task-id>: writes the body to RESP_FILE, succeeds on HTTP 200.
read_task() {
  code=$(curl -sS -o "$RESP_FILE" -w '%{http_code}' \
    -H "x-rakka-tenant: ${TENANT}" "${BASE}/a2a/tasks/$1" 2>/dev/null || echo 000)
  [ "$code" = "200" ]
}

# retry <attempts> <cmd...>: runs cmd until it succeeds or attempts run out.
retry() {
  attempts="$1"
  shift
  i=0
  while [ "$i" -lt "$attempts" ]; do
    if "$@"; then
      return 0
    fi
    i=$(( i + 1 ))
    sleep "$RETRY_SLEEP"
  done
  return 1
}

if [ "$DRY_RUN" = "1" ]; then
  log "DRY RUN — planned steps (no cluster changes):"
  [ "$APPLY" = "1" ] && info "kubectl apply -f $K8S_DIR"
  info "kubectl -n $NAMESPACE rollout status deploy/rakka-a2a-etcd --timeout=$TIMEOUT"
  info "kubectl -n $NAMESPACE rollout status deploy/rakka-a2a-postgres --timeout=$TIMEOUT"
  info "kubectl -n $NAMESPACE rollout status statefulset/$STATEFULSET --timeout=$TIMEOUT"
  info "kubectl -n $NAMESPACE port-forward svc/$PUBLIC_SVC $LOCAL_PORT:80"
  info "GET  $BASE/readyz  (wait for 200)"
  info "POST $BASE/a2a/message:send  (tenant=$TENANT) -> capture task id"
  info "GET  $BASE/a2a/tasks/<id>  (assert 200)"
  info "POST $BASE/a2a/tasks/<id>/pushNotificationConfigs  (assert stored)"
  info "kubectl -n $NAMESPACE delete pod $KILL_POD --grace-period=0 --force"
  info "GET  $BASE/a2a/tasks/<id>  (retry; assert recovers after pod kill)"
  info "GET  $BASE/a2a/tasks/<id>/pushNotificationConfigs  (assert config survived)"
  info "kubectl -n $NAMESPACE rollout status statefulset/$STATEFULSET --timeout=$TIMEOUT"
  [ "$CLEANUP" = "1" ] && info "kubectl delete -f $K8S_DIR"
  exit 0
fi

need kubectl
need curl
need jq
[ -d "$K8S_DIR" ] || fail "manifest directory not found: $K8S_DIR"
RESP_FILE=$(mktemp)

if [ "$APPLY" = "1" ]; then
  log "Applying manifests from $K8S_DIR"
  kubectl apply -f "$K8S_DIR" >/dev/null
fi

log "Waiting for etcd, PostgreSQL, and the agent StatefulSet to become Ready"
kc rollout status deploy/rakka-a2a-etcd --timeout="$TIMEOUT"
kc rollout status deploy/rakka-a2a-postgres --timeout="$TIMEOUT"
kc rollout status "statefulset/${STATEFULSET}" --timeout="$TIMEOUT"

log "Opening port-forward to svc/${PUBLIC_SVC} on 127.0.0.1:${LOCAL_PORT}"
start_forward
wait_ready || fail "public endpoint did not become ready within ${READY_SECS}s"
info "public endpoint is ready"

log "Sending a new A2A task (tenant=${TENANT})"
SEND_BODY=$(cat <<JSON
{
  "message": {
    "messageId": "smoke-$(now)",
    "contextId": "smoke-ctx-1",
    "role": "ROLE_USER",
    "parts": [{ "text": "kubernetes smoke test" }]
  },
  "tenant": "${TENANT}"
}
JSON
)
SEND_OUT=$(curl -sS -X POST "${BASE}/a2a/message:send" \
  -H 'content-type: application/json' -H "x-rakka-tenant: ${TENANT}" \
  -d "$SEND_BODY") || fail "message:send request failed"
TASK=$(printf '%s' "$SEND_OUT" | jq -r '.task.id // .id // empty' 2>/dev/null || true)
[ -n "$TASK" ] || fail "no task id in message:send response: $SEND_OUT"
info "task id: $TASK"

log "Reading the task back"
read_task "$TASK" || fail "task $TASK not readable after send"
info "status: $(jq -r '.status.state // .status // "unknown"' "$RESP_FILE")"

log "Registering a push notification config"
# The body is a flat TaskPushNotificationConfig (ProtoJSON); task_id also comes
# from the path but is included for clarity.
CREATE_CODE=$(curl -sS -o "$RESP_FILE" -w '%{http_code}' \
  -X POST "${BASE}/a2a/tasks/${TASK}/pushNotificationConfigs" \
  -H 'content-type: application/json' -H "x-rakka-tenant: ${TENANT}" \
  -d "{\"url\":\"https://example.com/smoke-hook\",\"task_id\":\"${TASK}\"}" \
  2>/dev/null || echo 000)
[ "$CREATE_CODE" = "200" ] \
  || fail "push config create failed (HTTP $CREATE_CODE): $(cat "$RESP_FILE" 2>/dev/null)"
CFG_COUNT=$(curl -sS -H "x-rakka-tenant: ${TENANT}" \
  "${BASE}/a2a/tasks/${TASK}/pushNotificationConfigs" \
  | jq '(.configs // []) | length' 2>/dev/null || echo 0)
[ "${CFG_COUNT:-0}" -ge 1 ] || fail "push config not stored (count=$CFG_COUNT)"
info "push config count: $CFG_COUNT"

log "Force-deleting pod ${KILL_POD} to exercise failover and durable recovery"
kc delete pod "$KILL_POD" --grace-period=0 --force >/dev/null 2>&1 || true

# The deleted pod may have been the port-forward endpoint; rebind to a live one.
start_forward
log "Asserting the task survives the pod kill (durable recovery)"
retry "$RETRIES" read_task "$TASK" \
  || fail "task $TASK did not recover within $((RETRIES * RETRY_SLEEP))s of the pod kill"
info "recovered status: $(jq -r '.status.state // .status // "unknown"' "$RESP_FILE")"

log "Asserting the push config survived the pod kill"
CFG_AFTER=$(curl -sS -H "x-rakka-tenant: ${TENANT}" \
  "${BASE}/a2a/tasks/${TASK}/pushNotificationConfigs" \
  | jq '(.configs // []) | length' 2>/dev/null || echo 0)
[ "${CFG_AFTER:-0}" -ge 1 ] || fail "push config lost after pod kill (count=$CFG_AFTER)"
info "push config count after recovery: $CFG_AFTER"

log "Waiting for the StatefulSet to return to full readiness"
kc rollout status "statefulset/${STATEFULSET}" --timeout="$TIMEOUT"

if [ "$CLEANUP" = "1" ]; then
  log "Cleaning up the stack (SMOKE_CLEANUP=1)"
  kubectl delete -f "$K8S_DIR" >/dev/null 2>&1 || true
fi

log "SMOKE PASS: task $TASK and its push config survived the ${KILL_POD} kill"
