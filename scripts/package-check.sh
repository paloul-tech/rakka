#!/usr/bin/env sh
set -eu

# Local packaging validation only. This script must never publish crates,
# upload artifacts to any registry, or access crates.io. Package checks always
# run in Cargo offline mode.

SCRIPT_DIR=$(CDPATH= cd "$(dirname "$0")" && pwd)
ROOT_DIR=$(CDPATH= cd "$SCRIPT_DIR/.." && pwd)

cd "$ROOT_DIR"

PUBLISHABLE_CRATES="
rakka-core
rakka-persistence
rakka-persistence-postgres
rakka-remote
rakka-cluster
rakka-sharding
rakka-workflow
rakka-stream
rakka-process
rakka-http
rakka-grpc
rakka-k8s
rakka-testkit
"

# Cargo cannot fully package unpublished crates with versioned internal Rakka
# dependencies in offline mode because it tries to resolve those dependency
# versions from the registry. Keep package checks offline: validate package file
# lists for every crate and fully package crates with no unpublished internal
# Rakka dependency.
FULL_PACKAGE_CRATES="
rakka-core
"

for crate in $PUBLISHABLE_CRATES; do
    cargo package -p "$crate" --allow-dirty --offline --no-verify --list >/dev/null
done

for crate in $FULL_PACKAGE_CRATES; do
    cargo package -p "$crate" --allow-dirty --offline >/dev/null
done

for manifest in examples/*/Cargo.toml; do
    if ! grep -q '^publish = false$' "$manifest"; then
        echo "example manifest must set publish = false: $manifest" >&2
        exit 1
    fi
done
