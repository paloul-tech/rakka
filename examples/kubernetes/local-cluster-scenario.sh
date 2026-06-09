#!/usr/bin/env sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
MANIFEST="$ROOT_DIR/examples/kubernetes/rakka-node.yaml"
NAMESPACE="${RAKKA_K8S_NAMESPACE:-rakka-system}"
IMAGE="${RAKKA_K8S_IMAGE:-ghcr.io/rakka-rs/rakka-node:0.1.0}"
NEXT_IMAGE="${RAKKA_K8S_NEXT_IMAGE:-}"
TIMEOUT="${RAKKA_K8S_TIMEOUT:-180s}"
DRY_RUN="${RAKKA_K8S_SCENARIO_DRY_RUN:-0}"

if [ "$DRY_RUN" = "1" ]; then
  echo "kubectl apply -f <manifest with image $IMAGE>"
  echo "kubectl -n $NAMESPACE rollout status statefulset/rakka-node --timeout=$TIMEOUT"
  echo "kubectl -n $NAMESPACE wait --for=condition=Ready pod -l app.kubernetes.io/name=rakka-node --timeout=$TIMEOUT"
  echo "kubectl -n $NAMESPACE exec rakka-node-0 -- wget -qO- http://127.0.0.1:8080/ready"
  echo "kubectl -n $NAMESPACE exec rakka-node-0 -- wget -qO- http://127.0.0.1:8080/live"
  echo "kubectl -n $NAMESPACE exec rakka-node-1 -- wget -qO- http://127.0.0.1:8080/drain"
  echo "kubectl -n $NAMESPACE delete pod/rakka-node-1 --wait=false"
  echo "kubectl -n $NAMESPACE rollout status statefulset/rakka-node --timeout=$TIMEOUT"
  if [ -n "$NEXT_IMAGE" ]; then
    echo "kubectl -n $NAMESPACE set image statefulset/rakka-node rakka-node=$NEXT_IMAGE"
    echo "kubectl -n $NAMESPACE rollout status statefulset/rakka-node --timeout=$TIMEOUT"
  fi
  exit 0
fi

pod_http_get() {
  pod="$1"
  url="$2"
  kubectl -n "$NAMESPACE" exec "$pod" -- sh -c \
    'if command -v wget >/dev/null 2>&1; then wget -qO- "$1"; else curl -fsS "$1"; fi' \
    sh "$url"
}

command -v kubectl >/dev/null 2>&1 || {
  echo "kubectl is required for the local cluster scenario" >&2
  exit 127
}

TMP_MANIFEST="$(mktemp "${TMPDIR:-/tmp}/rakka-k8s.XXXXXX.yaml")"
trap 'rm -f "$TMP_MANIFEST"' EXIT

sed "s#ghcr.io/rakka-rs/rakka-node:0.1.0#$IMAGE#g" "$MANIFEST" > "$TMP_MANIFEST"

kubectl apply -f "$TMP_MANIFEST"
kubectl -n "$NAMESPACE" rollout status statefulset/rakka-node --timeout="$TIMEOUT"
kubectl -n "$NAMESPACE" wait --for=condition=Ready pod -l app.kubernetes.io/name=rakka-node --timeout="$TIMEOUT"

pod_http_get rakka-node-0 http://127.0.0.1:8080/ready
pod_http_get rakka-node-0 http://127.0.0.1:8080/live
pod_http_get rakka-node-1 http://127.0.0.1:8080/drain

kubectl -n "$NAMESPACE" delete pod/rakka-node-1 --wait=false
kubectl -n "$NAMESPACE" rollout status statefulset/rakka-node --timeout="$TIMEOUT"
kubectl -n "$NAMESPACE" wait --for=condition=Ready pod -l app.kubernetes.io/name=rakka-node --timeout="$TIMEOUT"

if [ -n "$NEXT_IMAGE" ]; then
  kubectl -n "$NAMESPACE" set image statefulset/rakka-node rakka-node="$NEXT_IMAGE"
  kubectl -n "$NAMESPACE" rollout status statefulset/rakka-node --timeout="$TIMEOUT"
  kubectl -n "$NAMESPACE" wait --for=condition=Ready pod -l app.kubernetes.io/name=rakka-node --timeout="$TIMEOUT"
fi

echo "Rakka local Kubernetes scenario completed."
