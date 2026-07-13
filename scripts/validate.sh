#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd "$(dirname "$0")" && pwd)
ROOT_DIR=$(CDPATH= cd "$SCRIPT_DIR/.." && pwd)

cd "$ROOT_DIR"

cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo check -p rakka-stream --no-default-features
cargo check -p rakka-process --no-default-features
cargo check -p rakka-a2a --no-default-features
cargo check -p rakka-agent --no-default-features
cargo test -p rakka-agent --no-default-features
cargo doc --workspace --all-features --no-deps

RAKKA_K8S_SCENARIO_DRY_RUN=1 \
RAKKA_K8S_NEXT_IMAGE=example/rakka-node:next \
    examples/kubernetes/local-cluster-scenario.sh >/dev/null
