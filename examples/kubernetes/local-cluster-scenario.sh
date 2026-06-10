#!/usr/bin/env sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
MANIFEST="$ROOT_DIR/examples/kubernetes/rakka-node.yaml"
NAMESPACE="${RAKKA_K8S_NAMESPACE:-rakka-system}"
STATEFULSET="${RAKKA_K8S_STATEFULSET:-rakka-node}"
IMAGE="${RAKKA_K8S_IMAGE:-ghcr.io/rakka-rs/rakka-node:0.1.0}"
NEXT_IMAGE="${RAKKA_K8S_NEXT_IMAGE:-}"
TIMEOUT="${RAKKA_K8S_TIMEOUT:-180s}"
DRY_RUN="${RAKKA_K8S_SCENARIO_DRY_RUN:-0}"
REPLICAS="${RAKKA_K8S_REPLICAS:-3}"
HTTP_PORT="${RAKKA_K8S_HTTP_PORT:-8080}"
READY_PATH="${RAKKA_K8S_READY_PATH:-/ready}"
LIVE_PATH="${RAKKA_K8S_LIVE_PATH:-/live}"
DRAIN_PATH="${RAKKA_K8S_DRAIN_PATH:-/drain}"
METRICS_PATH="${RAKKA_K8S_METRICS_PATH:-/metrics}"
SNAPSHOTS_PATH="${RAKKA_K8S_SNAPSHOTS_PATH:-/snapshots}"
ROUTE_PATH="${RAKKA_K8S_ROUTE_PATH:-/scenario/sharding/route-remote}"
ROUTE_ENTITY_ID="${RAKKA_K8S_ROUTE_ENTITY_ID:-cart-v1g}"
ROUTE_ITEM="${RAKKA_K8S_ROUTE_ITEM:-apple}"
ROUTE_EXPECT="${RAKKA_K8S_ROUTE_EXPECT:-rakka-node-}"
METRICS_EXPECT="${RAKKA_K8S_METRICS_EXPECT:-rakka_http_request_latency_ms}"
SNAPSHOTS_EXPECT="${RAKKA_K8S_SNAPSHOTS_EXPECT:-kubernetes_health}"
POD0="${STATEFULSET}-0"
POD1="${STATEFULSET}-1"

url_for() {
  path="$1"
  printf 'http://127.0.0.1:%s%s' "$HTTP_PORT" "$path"
}

scenario_url() {
  printf '%s?entity_id=%s&item=%s&expect_remote=1' \
    "$(url_for "$ROUTE_PATH")" "$ROUTE_ENTITY_ID" "$ROUTE_ITEM"
}

dry_run_line() {
  echo "$*"
}

if [ "$DRY_RUN" = "1" ]; then
  dry_run_line "kubectl apply -f <manifest with image $IMAGE>"
  dry_run_line "kubectl -n $NAMESPACE rollout status statefulset/$STATEFULSET --timeout=$TIMEOUT"
  dry_run_line "kubectl -n $NAMESPACE wait --for=condition=Ready pod -l app.kubernetes.io/name=rakka-node --timeout=$TIMEOUT"
  dry_run_line "kubectl -n $NAMESPACE get pod -l app.kubernetes.io/name=rakka-node -o wide"
  dry_run_line "kubectl -n $NAMESPACE exec $POD0 -- GET $(url_for "$READY_PATH")"
  dry_run_line "kubectl -n $NAMESPACE exec $POD0 -- GET $(url_for "$LIVE_PATH")"
  dry_run_line "kubectl -n $NAMESPACE exec $POD0 -- GET $(url_for "$METRICS_PATH") | grep $METRICS_EXPECT"
  dry_run_line "kubectl -n $NAMESPACE exec $POD0 -- GET $(url_for "$SNAPSHOTS_PATH") | grep $SNAPSHOTS_EXPECT"
  dry_run_line "kubectl -n $NAMESPACE exec $POD0 -- GET $(scenario_url) | grep $ROUTE_EXPECT"
  dry_run_line "kubectl -n $NAMESPACE exec $POD1 -- GET $(url_for "$DRAIN_PATH")"
  dry_run_line "kubectl -n $NAMESPACE exec $POD1 -- expect GET $(url_for "$READY_PATH") to fail after drain"
  dry_run_line "kubectl -n $NAMESPACE get pod/$POD1 -o jsonpath={.metadata.uid}"
  dry_run_line "kubectl -n $NAMESPACE delete pod/$POD1 --wait=true --timeout=$TIMEOUT"
  dry_run_line "kubectl -n $NAMESPACE rollout status statefulset/$STATEFULSET --timeout=$TIMEOUT"
  dry_run_line "kubectl -n $NAMESPACE wait --for=condition=Ready pod/$POD1 --timeout=$TIMEOUT"
  dry_run_line "kubectl -n $NAMESPACE get pod/$POD1 -o jsonpath={.metadata.uid}"
  dry_run_line "kubectl -n $NAMESPACE exec $POD0 -- GET $(scenario_url) | grep $ROUTE_EXPECT"
  if [ -n "$NEXT_IMAGE" ]; then
    PARTITION=$((REPLICAS - 1))
    dry_run_line "kubectl -n $NAMESPACE patch statefulset/$STATEFULSET --type=merge -p '{\"spec\":{\"updateStrategy\":{\"type\":\"RollingUpdate\",\"rollingUpdate\":{\"partition\":$PARTITION}}}}'"
    dry_run_line "kubectl -n $NAMESPACE set image statefulset/$STATEFULSET rakka-node=$NEXT_IMAGE"
    dry_run_line "kubectl -n $NAMESPACE rollout status statefulset/$STATEFULSET --timeout=$TIMEOUT"
    dry_run_line "kubectl -n $NAMESPACE exec $POD0 -- GET $(scenario_url) | grep $ROUTE_EXPECT"
    dry_run_line "kubectl -n $NAMESPACE patch statefulset/$STATEFULSET --type=merge -p '{\"spec\":{\"updateStrategy\":{\"type\":\"RollingUpdate\",\"rollingUpdate\":{\"partition\":0}}}}'"
    dry_run_line "kubectl -n $NAMESPACE rollout status statefulset/$STATEFULSET --timeout=$TIMEOUT"
  fi
  exit 0
fi

pod_http_get() {
  pod="$1"
  url="$2"
  kubectl -n "$NAMESPACE" exec "$pod" -- sh -c \
    'url="$1"; if command -v wget >/dev/null 2>&1; then wget -qO- "$url"; else curl -fsS "$url"; fi' \
    sh "$url"
}

pod_http_expect_failure() {
  pod="$1"
  url="$2"
  if pod_http_get "$pod" "$url" >/dev/null 2>&1; then
    echo "expected $url on $pod to fail" >&2
    exit 1
  fi
}

assert_http_contains() {
  pod="$1"
  url="$2"
  expected="$3"
  body="$(pod_http_get "$pod" "$url")"
  if ! printf '%s' "$body" | grep "$expected" >/dev/null 2>&1; then
    echo "expected $url on $pod to contain $expected" >&2
    echo "$body" >&2
    exit 1
  fi
}

pod_uid() {
  pod="$1"
  kubectl -n "$NAMESPACE" get "pod/$pod" -o 'jsonpath={.metadata.uid}'
}

wait_ready() {
  kubectl -n "$NAMESPACE" rollout status "statefulset/$STATEFULSET" --timeout="$TIMEOUT"
  kubectl -n "$NAMESPACE" wait --for=condition=Ready pod -l app.kubernetes.io/name=rakka-node --timeout="$TIMEOUT"
}

verify_cluster_paths() {
  pod_http_get "$POD0" "$(url_for "$READY_PATH")" >/dev/null
  pod_http_get "$POD0" "$(url_for "$LIVE_PATH")" >/dev/null
  assert_http_contains "$POD0" "$(url_for "$METRICS_PATH")" "$METRICS_EXPECT"
  assert_http_contains "$POD0" "$(url_for "$SNAPSHOTS_PATH")" "$SNAPSHOTS_EXPECT"
  assert_http_contains "$POD0" "$(scenario_url)" "$ROUTE_EXPECT"
}

command -v kubectl >/dev/null 2>&1 || {
  echo "kubectl is required for the local cluster scenario" >&2
  exit 127
}

TMP_MANIFEST="$(mktemp "${TMPDIR:-/tmp}/rakka-k8s.XXXXXX.yaml")"
trap 'rm -f "$TMP_MANIFEST"' EXIT

sed "s#ghcr.io/rakka-rs/rakka-node:0.1.0#$IMAGE#g" "$MANIFEST" > "$TMP_MANIFEST"

kubectl apply -f "$TMP_MANIFEST"
wait_ready
kubectl -n "$NAMESPACE" get pod -l app.kubernetes.io/name=rakka-node -o wide
verify_cluster_paths

pod_http_get "$POD1" "$(url_for "$DRAIN_PATH")" >/dev/null
pod_http_expect_failure "$POD1" "$(url_for "$READY_PATH")"

OLD_UID="$(pod_uid "$POD1")"
kubectl -n "$NAMESPACE" delete "pod/$POD1" --wait=true --timeout="$TIMEOUT"
kubectl -n "$NAMESPACE" wait --for=condition=Ready "pod/$POD1" --timeout="$TIMEOUT"
wait_ready
NEW_UID="$(pod_uid "$POD1")"
if [ "$OLD_UID" = "$NEW_UID" ]; then
  echo "expected $POD1 to be replaced with a new pod uid" >&2
  exit 1
fi
verify_cluster_paths

if [ -n "$NEXT_IMAGE" ]; then
  PARTITION=$((REPLICAS - 1))
  kubectl -n "$NAMESPACE" patch "statefulset/$STATEFULSET" --type=merge \
    -p "{\"spec\":{\"updateStrategy\":{\"type\":\"RollingUpdate\",\"rollingUpdate\":{\"partition\":$PARTITION}}}}"
  kubectl -n "$NAMESPACE" set image "statefulset/$STATEFULSET" "rakka-node=$NEXT_IMAGE"
  wait_ready
  verify_cluster_paths
  kubectl -n "$NAMESPACE" patch "statefulset/$STATEFULSET" --type=merge \
    -p '{"spec":{"updateStrategy":{"type":"RollingUpdate","rollingUpdate":{"partition":0}}}}'
  wait_ready
  verify_cluster_paths
fi

echo "Rakka local Kubernetes multi-node scenario completed."
